import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { runBridge } from "../src/mesh-bridge.js";
import { BRIDGE_PROTOCOL_VERSION, BRIDGE_START_DEADLINE_MS, BridgeStaleIncarnationError, BridgeStartDeadlineError, bridgeStartDeadlineMs,
  MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { COMMIT_OUTBOX_PREFIX, CommitOutbox } from "../src/mesh/commit-outbox.js";
import { isMeshRetryableBusy } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshBatchView, type MeshIdentity } from "../src/mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantFileCustodyTimeoutError, withParticipantFileDirectoryCustody, writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#13645, independent re-audit of WIP 4 (AUDIT-mirror-wip4-RESULT.md): regression tests for
// P1-1, P1-2, P2-1 and P2-3, built from the audit's probe (zz-audit-wip4.test.ts).
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  for (const s of stores.splice(0)) s.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const store = (stateBackend: "sqlite" | "file" = "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "reaudit-wip5-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend });
  expect(hub.stateBackend).toBe(stateBackend);
  stores.push(hub);
  return hub;
};
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const NOW = Date.UTC(2100, 0, 1);
const identity: MeshIdentity = { id: "session:audit-00000000", name: "main", kind: "main", sessionId: "audit" };
const host: FabricHostRecord = { format: 1, id: identity.id, rootId: identity.id, identity,
  incarnationToken: "opaque-z-predecessor", startedAt: NOW - 2_000, updatedAt: NOW, expiresAt: NOW + 86_400_000 };
const participant: FabricParticipantRecord = {
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  ownerIncarnation: "fixture:old-owner", name: "audit", label: "AUDIT", status: "idle", runner: "pi",
  transport: "host", capabilities: ["steer", "followUp"], sessionId: "audit", startedAt: host.startedAt, updatedAt: NOW, controlProtocol: "v1",
};
const presence = { hosts: [{ record: host, expiresAt: host.expiresAt }], participants: [participant] };
const successorHost = { ...host, incarnationToken: "opaque-a-successor", startedAt: host.startedAt - 120_000 };
const successor = { hosts: [{ record: successorHost, expiresAt: host.expiresAt }], participants: [{ ...participant, name: "successor-B" }] };
const outcome = (error: unknown) => error instanceof BridgeStaleIncarnationError ? "STALE"
  : `${(error as Error)?.constructor?.name}:${(error as { code?: string })?.code ?? "nocode"}:retryable=${isMeshRetryableBusy(error)}`;
const settle = async (run: () => Promise<unknown>): Promise<string> => {
  try { await run(); return "resolved"; } catch (error) { return outcome(error); }
};
const RETRYABLE_TIMEOUT = "retryable=true";

/** Holds one lease gate (as B's custody pass or renewal sweep would) until release(). */
const holdGate = async (hub: MeshStore, file: string) => {
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let entered!: () => void;
  const inGate = new Promise<void>(resolve => { entered = resolve; });
  const gate = hub.leaseCustody(file, async () => { entered(); await held; });
  await inGate;
  return async () => { release(); await gate; };
};

describe("mirror custody re-audit regressions (smarty-dev#13645 WIP 5)", () => {
  it("P1-1: a second link's mirror that waits out the participant-lock bound behind a held namespace backs off (retryable), then succeeds", async () => {
    const hub = store();
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const inCustody = new Promise<void>(resolve => { entered = resolve; });
    // Another link's state-changing mirror pass holds the participant namespace across its batches.
    const other = withParticipantFileDirectoryCustody(hub, async () => { entered(); await held; }, new Set(["session:other-link"]));
    await inCustody;
    const side = new StoreBridgeSide(hub, "ryzen3", () => NOW);
    let failure: unknown;
    const started = Date.now();
    try { await side.mirror(presence); } catch (error) { failure = error; }
    const waitedMs = Date.now() - started;
    release();
    await other;
    // The link's loop backs off on isBridgeRetryable = BridgeMirrorDeferredError || isMeshRetryableBusy.
    expect(failure).toBeInstanceOf(MeshLockTimeoutError);
    expect(isMeshRetryableBusy(failure)).toBe(true);
    expect((failure as Error).message).toContain("participant file lock");
    expect(waitedMs).toBeGreaterThanOrEqual(4_900);
    expect(hub.get(hostKey(identity.id), { fresh: true })).toBeUndefined();
    // The backed-off retry, with the namespace free, mirrors normally.
    await side.mirror(presence);
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "opaque-z-predecessor" });
    expect(readHostLeaseCurrent(hub.root, identity.id)).toMatchObject({ incarnationToken: "opaque-z-predecessor" });
  }, 20_000);

  it("P1-2: a stale A is fenced on its next pass while B holds A's relevant gate (probe skipped: B's outbox rows are unconfirmed for A)", async () => {
    const hub = store();
    const a = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await a.mirror(presence);
    await a.mirror(presence);
    const b = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await b.mirror(successor);
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "opaque-a-successor" });
    // Precondition: the shared bridge/ryzen2 scope holds B's recordings, so A's lease-only probe is off.
    expect(hub.listAll(COMMIT_OUTBOX_PREFIX, { fresh: true }).length).toBeGreaterThan(0);
    const releaseGate = await holdGate(hub, hostLeasePath(hub.root, identity.id));
    const first = await settle(() => a.mirror(presence));
    await releaseGate();
    const second = await settle(() => a.mirror(presence));
    // Main: first=STALE second=STALE. WIP 4: first=MeshLockTimeoutError (unfenced until A wins custody).
    expect({ first, second }).toEqual({ first: "STALE", second: "STALE" });
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "opaque-a-successor" });
  });

  it("P1-2: a stale A is fenced on its next pass while B holds A's relevant gate (A's lease-only probe plans and sees the stale refusal)", async () => {
    const hub = store();
    const a = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await a.mirror(presence);
    await a.mirror(presence);
    const b = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await b.mirror(successor);
    // B's next state-changing pass (a participant change) retires its ran rows: nothing in the scope
    // is unconfirmed for A, so A's next pass takes the lock-free probe.
    await b.mirror({ ...successor, participants: [{ ...successor.participants[0]!, label: "B2" }] });
    expect(hub.listAll(COMMIT_OUTBOX_PREFIX, { fresh: true })).toEqual([]);
    const releaseGate = await holdGate(hub, hostLeasePath(hub.root, identity.id));
    const first = await settle(() => a.mirror(presence));
    await releaseGate();
    const second = await settle(() => a.mirror(presence));
    expect({ first, second }).toEqual({ first: "STALE", second: "STALE" });
  });

  it("P2-1: a first-install lease effect whose additions gate is busy rejects the pass (readiness holds); the retry installs the lease", async () => {
    const hub = store();
    const leaseCustody = hub.leaseCustody.bind(hub);
    const additions = path.join(hub.root, "host-leases", ".additions");
    let calls = 0;
    let holder: Promise<void> | undefined;
    let release: (() => void) | undefined;
    (hub as unknown as { leaseCustody: typeof hub.leaseCustody }).leaseCustody = (async (file: string, operation: () => unknown, timeoutMs?: number, options?: object) => {
      if (file === additions && ++calls === 2 && !holder) {
        // The effect phase, after directory custody released: another writer (another link's pass,
        // the reaper or a native creating its lease) holds the additions gate at this instant.
        let entered!: () => void;
        const inGate = new Promise<void>(resolve => { entered = resolve; });
        holder = leaseCustody(additions, async () => { entered(); await new Promise<void>(resolve => { release = resolve; }); });
        await inGate;
      }
      return leaseCustody(file, operation as () => unknown, timeoutMs, options as never);
    }) as typeof hub.leaseCustody;
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    const first = await settle(() => side.mirror(presence));
    release?.();
    await holder;
    // WIP 4: first=resolved with lease=MISSING (the first mirror reported ready without its lease).
    expect(first).toContain(RETRYABLE_TIMEOUT);
    expect(hub.get(hostKey(identity.id), { fresh: true })).toBeDefined();
    expect(readHostLeaseCurrent(hub.root, identity.id)).toBeUndefined();
    await side.mirror(presence);
    expect(readHostLeaseCurrent(hub.root, identity.id)).toMatchObject({ id: identity.id, incarnationToken: "opaque-z-predecessor" });
  });

  it("P2-1: failed outbox rows keep a retry floor when a run stages 64 or more effects", async () => {
    const hub = store();
    const log: string[] = [];
    let busy = true;
    const outbox = new CommitOutbox(hub, "scope-floor", identity, {
      touch: (payload: { n: string }, _view: unknown, replay: boolean) => { log.push(`${payload.n}${replay ? ":replay" : ""}`); },
    }, {
      withEffectCustody: async (_kind, payload, operation) => {
        if (busy && (payload as { n: string }).n === "x") throw new MeshLockTimeoutError(" test gate", 1, 0);
        operation();
      },
    });
    const durable = outbox.plan();
    let view!: MeshBatchView;
    await hub.writeBatch({ identity, ops: [], prepare: () => durable.stage([{ kind: "put", key: "subject/x", value: 1 }],
      [{ kind: "touch", key: "x", payload: { n: "x" } }]), commitOutbox: ({ view: committed }) => { view = committed; } });
    // The busy custody leaves the row pending (failed, retried on a later run).
    await durable.runAsync(view);
    expect(log).toEqual([]);
    expect(outbox.hasUnconfirmed()).toBe(true);
    busy = false;
    const steady = outbox.plan();
    const many = Array.from({ length: 64 }, (_, n) => ({ kind: "touch", key: `y${n}`, payload: { n: `y${n}` } }));
    expect(steady.stage([], many)).toEqual([]);
    expect(await steady.runAsync(hub.stateView()!.view)).toBe(64);
    // WIP 4: the retry budget was 64 - 64 = 0, so the failed row never ran or retired before a restart.
    expect(log).toContain("x:replay");
    expect(outbox.hasUnconfirmed()).toBe(false);
  });

  it("P2-1: a caller that asks for it sees a busy effect custody as a retryable rejection, after every staged effect ran", async () => {
    const hub = store();
    const log: string[] = [];
    const outbox = new CommitOutbox(hub, "scope-surface", identity, {
      touch: (payload: { n: string }) => { log.push(payload.n); },
    }, {
      withEffectCustody: async (_kind, payload, operation) => {
        if ((payload as { n: string }).n === "a") throw new MeshLockTimeoutError(" test gate", 1, 0);
        operation();
      },
    });
    const plan = outbox.plan();
    let view!: MeshBatchView;
    await hub.writeBatch({ identity, ops: [], prepare: () => plan.stage([{ kind: "put", key: "subject/a", value: 1 }],
      [{ kind: "touch", key: "a", payload: { n: "a" } }, { kind: "touch", key: "b", payload: { n: "b" } }]),
    commitOutbox: ({ view: committed }) => { view = committed; } });
    const failure = await plan.runAsync(view, undefined, { surfaceBusyCustody: true }).then(() => undefined, (error: unknown) => error);
    expect(failure).toBeInstanceOf(MeshLockTimeoutError);
    expect(isMeshRetryableBusy(failure)).toBe(true);
    expect(log).toEqual(["b"]);
    expect(outbox.hasUnconfirmed()).toBe(true);
  });

  it("P2-3: a native that claims its lease file before its host row keeps it against a mirror renewal", async () => {
    const hub = store();
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await side.mirror(presence);
    await side.mirror(presence);
    expect(readHostLeaseCurrent(hub.root, identity.id)).toMatchObject({ incarnationToken: "opaque-z-predecessor" });
    const file = hostLeasePath(hub.root, identity.id);
    const leaseCustody = hub.leaseCustody.bind(hub);
    let claimed = false;
    (hub as unknown as { leaseCustody: typeof hub.leaseCustody }).leaseCustody = (async (gate: string, operation: () => unknown, timeoutMs?: number, options?: object) => {
      if (gate === file && !claimed) {
        claimed = true;
        // A moved Main (same root host id) claims its lease under its own gate, before its host row.
        await leaseCustody(file, () => writeHostLease(hub.root, {
          id: identity.id, rootId: identity.id, identityId: identity.id, updatedAt: NOW, expiresAt: NOW + 60_000,
          startedAt: NOW - 1_000, incarnationToken: "native-claim-token",
          session: { id: "native-session", startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 15_000 },
        }));
      }
      return leaseCustody(gate, operation as () => unknown, timeoutMs, options as never);
    }) as typeof hub.leaseCustody;
    // The host row still reads mirror-owned, so the renewal's row checks pass; the lease is native.
    await settle(() => side.mirror(presence));
    expect(claimed).toBe(true);
    expect(readHostLeaseCurrent(hub.root, identity.id)).toMatchObject({ incarnationToken: "native-claim-token",
      session: { id: "native-session" } });
  });
});

// smarty-dev#13645, independent delta audit of WIP 5 (AUDIT-mirror-wip5-RESULT.md): F1 and F2, built from the
// audit's probe (zz-audit-wip5.test.ts). Main's results: NONROOT lease=T2 and live; C1B/C1B-FRESH STALE/STALE.
describe("mirror custody delta-audit regressions (smarty-dev#13645 WIP 5 audit)", () => {
  it.each(["sqlite", "file"] as const)("F1: a resident host that restarts behind the bridge with a new token keeps a renewed mirror lease (%s)", async backend => {
    const hub = store(backend);
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    // A resident keeps its id across restarts (resident:<digest(rootId)>) and gets a new incarnation token.
    const residentId = "resident:" + "c".repeat(24);
    const resident = (token: string, startedAt: number): FabricHostRecord => ({ format: 1, id: residentId, rootId: identity.id,
      identity: { id: residentId, name: "resident", kind: "agent" }, startedAt, updatedAt: NOW, expiresAt: NOW + 60_000,
      incarnationToken: token });
    // Sampled, as a real far side's presence() reports each host, so the mirror rows carry mirrorLease: 1.
    const withResident = (record: FabricHostRecord) => ({ hosts: [{ record: host, expiresAt: host.expiresAt, observedAt: NOW },
      { record, expiresAt: record.expiresAt, observedAt: NOW }], participants: [participant] });
    await side.mirror(withResident(resident("child-T1", NOW - 5_000)));
    await side.mirror(withResident(resident("child-T1", NOW - 5_000)));
    expect(readHostLeaseCurrent(hub.root, residentId)).toMatchObject({ incarnationToken: "child-T1" });
    // T1 -> T2 directly (a crash restart within the lease TTL): the row and the lease both move to T2.
    await side.mirror(withResident(resident("child-T2", NOW - 1_000)));
    await side.mirror(withResident(resident("child-T2", NOW - 1_000)));
    const entry = hub.get(hostKey(residentId), { fresh: true });
    expect(entry?.value).toMatchObject({ incarnationToken: "child-T2", mirrorLease: 1 });
    expect(readHostLeaseCurrent(hub.root, residentId)).toMatchObject({ incarnationToken: "child-T2", startedAt: NOW - 1_000 });
    // Readers match the lease by token: the host reads live on this side, not expired.
    expect(hostEntryLiveness(entry!, readHostLeases(hub.root)).expiresAt).toBeGreaterThan(NOW);
  });

  // F2: main ran the stale check before any custody on every backend and side. The file backend (the
  // default) has no revision view, and a fresh side has completed no pass; both must still fence A.
  it("F2: a recovered stale A is fenced on its next pass while B holds A's host gate (file)", async () => {
    const hub = store("file");
    const a = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await a.mirror(presence);
    await a.mirror(presence);
    await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(successor);
    const releaseGate = await holdGate(hub, hostLeasePath(hub.root, identity.id));
    const first = await settle(() => a.mirror(presence));
    await releaseGate();
    const second = await settle(() => a.mirror(presence));
    expect({ first, second }).toEqual({ first: "STALE", second: "STALE" });
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "opaque-a-successor" });
  });

  it.each(["sqlite", "file"] as const)("F2: an unrecovered (fresh) stale A2 is fenced on its first pass while B holds the host gate (%s)", async backend => {
    const hub = store(backend);
    const a = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await a.mirror(presence);
    await a.mirror(presence);
    const b = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await b.mirror(successor);
    await b.mirror(successor);
    const leaseB = fs.readFileSync(hostLeasePath(hub.root, identity.id));
    const a2 = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    const releaseGate = await holdGate(hub, hostLeasePath(hub.root, identity.id));
    const first = await settle(() => a2.mirror(presence));
    await releaseGate();
    const second = await settle(() => a2.mirror(presence));
    expect({ first, second }).toEqual({ first: "STALE", second: "STALE" });
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "opaque-a-successor" });
    expect(fs.readFileSync(hostLeasePath(hub.root, identity.id))).toEqual(leaseB);
  });
});

// The hosted mesh-bridge-relay-cli failure: two link CLIs start together on one hub. Link A's first pass writes its
// lease right after it releases its directory custody, which is the moment link B takes the hub's shared host-lease
// additions gate for B's own pass (or the reverse). The zero-wait effect gate is busy and the pass rejects retryably
// (P2-1 above). The CLI's readiness barrier stopped the link on that rejection:
// "mesh-bridge: stopped: FABRIC_MESH_LOCK_TIMEOUT: ... host-lease-commits/5f43c55dca0091e6/custody.lock ... after 1 attempts".
describe("the CLI's readiness barrier backs off a retryable first pass (hosted mesh-bridge-relay-cli, smarty-dev#13645)", () => {
  /** runBridge (the CLI's code, in process) against a scripted far peer whose presence is one native root. The hub's
   * first pass meets a busy additions gate in its effect phase; the gate is released on the retry log line if asked. */
  const cliWithBusyFirstPass = async (releaseOnRetry: boolean) => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "reaudit-cli-"));
    roots.push(base);
    const hubRoot = path.join(base, "hub");
    const runtime = path.join(base, "x");
    fs.mkdirSync(runtime, { mode: 0o700 });
    const far = new MeshStore(path.join(base, "far"), 64 * 1024, 100, { stateBackend: "file" });
    stores.push(far);
    const farIdentity: MeshIdentity = { id: "session:far-00000000", name: "main", kind: "main", sessionId: "far" };
    const id = farIdentity.id;
    const now = Date.now();
    await far.put({ key: hostKey(id), identity: farIdentity, value: { format: 1, id, rootId: id, identity: farIdentity,
      startedAt: now, updatedAt: now, expiresAt: now + 120_000 } });
    writeHostLease(far.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
    writeParticipantFile(far.root, { key: key("topology/participants/", id), version: 1, updatedAt: now, updatedBy: farIdentity, value: {
      ...participant, id, rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: `fixture:${id}`, name: "far",
      label: "FAR", sessionId: "far", startedAt: now, updatedAt: now } });
    const farPresence = { ...await new StoreBridgeSide(far, "hub", Date.now, true).presence(), reserved: [] };
    expect(farPresence.hosts).toHaveLength(1);
    const peer = `
      const presence = ${JSON.stringify(farPresence)};
      require("node:readline").createInterface({ input: process.stdin }).on("line", line => {
        const request = JSON.parse(line);
        const result = request.op === "hello" ? { version: ${BRIDGE_PROTOCOL_VERSION}, changes: true, independentRpc: true, partialPresence: true, nonRootHosts: true }
          : request.op === "presence" ? presence
          : request.op === "latestSequence" ? 0
          : request.op === "latestCursor" ? { through: 0 }
          : request.op === "bridgedIds" ? { ids: [], through: 0, done: true }
          : request.op === "read" ? { events: [], through: 0, done: true } : {};
        process.stdout.write(JSON.stringify({ id: request.id, ok: true, result }) + "\\n");
      });
    `;
    // The effect phase of the hub's first pass (its host row committed, its lease not yet written, directory
    // custody released): another link's pass takes the hub's additions gate at this instant.
    const additions = path.join(hubRoot, "host-leases", ".additions");
    const leaseCustody = MeshStore.prototype.leaseCustody;
    let release!: () => void;
    const released = new Promise<void>(resolve => { release = resolve; });
    let holder: Promise<unknown> | undefined;
    vi.spyOn(MeshStore.prototype, "leaseCustody").mockImplementation((async function (this: MeshStore, file: string,
      operation: () => unknown, timeoutMs?: number, options?: { ownIncarnation?: string | undefined }) {
      if (!holder && file === additions && this.get(hostKey(id), { fresh: true }) && !readHostLeaseCurrent(hubRoot, id)) {
        let entered!: () => void;
        const inGate = new Promise<void>(resolve => { entered = resolve; });
        holder = leaseCustody.call(this, additions, async () => { entered(); await released; });
        await inGate;
      }
      return leaseCustody.call(this, file, operation, timeoutMs, options);
    }) as never);
    const logs: string[] = [];
    let linked!: () => void, retrying!: () => void;
    const isLinked = new Promise<void>(resolve => { linked = resolve; });
    const isRetrying = new Promise<void>(resolve => { retrying = resolve; });
    vi.spyOn(process.stderr, "write").mockImplementation(chunk => {
      const line = String(chunk);
      logs.push(line);
      if (line.includes("retrying in")) { retrying(); if (releaseOnRetry) release(); }
      if (line.includes("linked hub <-> far")) linked();
      return true;
    });
    // The relay's socket stays in this test's own runtime directory, never the real user's.
    const runtimeBefore = process.env.XDG_RUNTIME_DIR;
    process.env.XDG_RUNTIME_DIR = runtime;
    const controller = new AbortController();
    const running = runBridge(new Map([["mesh", hubRoot], ["name", "hub"], ["remote", "far"],
      ["cursor", path.join(base, "far.json")], ["call-timeout-ms", "5000"]]), [process.execPath, "-e", peer], controller.signal);
    const cleanup = async (): Promise<void> => {
      controller.abort();
      release();
      await holder;
      await running.catch(() => undefined);
      vi.restoreAllMocks();
      if (runtimeBefore === undefined) delete process.env.XDG_RUNTIME_DIR;
      else process.env.XDG_RUNTIME_DIR = runtimeBefore;
    };
    return { id, hubRoot, logs, running, controller, isLinked, isRetrying, cleanup, held: () => holder !== undefined };
  };
  const stoppedLines = (logs: string[]) => logs.filter(line => line.includes("stopped:"));
  // The lock path uses the platform separator (backslashes on Windows).
  const RETRY_LINE = /start: mesh lock timeout; retrying in 100 ms: FABRIC_MESH_LOCK_TIMEOUT: .*host-lease-commits[\\/]5f43c55dca0091e6[\\/]custody\.lock.* after 1 attempts/;

  it("a first pass whose lease effect meets a busy additions gate backs off and links, instead of stopping the link", async () => {
    const cli = await cliWithBusyFirstPass(true);
    try {
      const outcome = await Promise.race([cli.isLinked.then(() => "linked"), cli.running.then(code => `exited ${code}`)]);
      // 347f75c30: { outcome: "exited 1", stopped: ["mesh-bridge: stopped: FABRIC_MESH_LOCK_TIMEOUT: ...custody lock
      // <hub>/host-lease-commits/5f43c55dca0091e6/custody.lock, held by pid <self>) after 1 attempts, ..."] }.
      expect({ outcome, stopped: stoppedLines(cli.logs) }).toEqual({ outcome: "linked", stopped: [] });
      // Not vacuous: the first pass did meet the busy gate, and was refused with the hosted failure's error.
      expect(cli.held()).toBe(true);
      expect(cli.logs.join("")).toMatch(RETRY_LINE);
      // The backed-off start installed the lease the refused pass could not write.
      expect(readHostLeaseCurrent(cli.hubRoot, cli.id)).toMatchObject({ id: cli.id, rootId: cli.id });
      cli.controller.abort();
      await expect(cli.running).resolves.toBe(0);
    } finally { await cli.cleanup(); }
  }, 30_000);

  it("a stop during that backoff ends the link at once, without a stop error or a lease", async () => {
    const cli = await cliWithBusyFirstPass(false);
    try {
      await Promise.race([cli.isRetrying, cli.running]);
      expect(cli.logs.join("")).toMatch(RETRY_LINE);
      const stoppedAt = Date.now();
      cli.controller.abort();
      await expect(cli.running).resolves.toBe(0);
      // The backoff wait is woken by stop(); the CLI's own stop bound is 5 s.
      expect(Date.now() - stoppedAt).toBeLessThan(4_000);
      expect(stoppedLines(cli.logs)).toEqual([]);
      expect(cli.logs.join("")).not.toContain("linked hub <-> far");
      expect(readHostLeaseCurrent(cli.hubRoot, cli.id)).toBeUndefined();
    } finally { await cli.cleanup(); }
  }, 30_000);

  // fabric-store review of pi-fabric#1562, P2: the backoff had no overall deadline and nothing pings the watchdog
  // before run() links, so under WatchdogSec=90 systemd killed a start that never linked, with no reason written.
  it("a first pass still busy at the start deadline ends the link: exit 1 with a reason line, no lease", async () => {
    // A 4.5 s watchdog bounds the start at 3 s (two thirds), from before hello; the documented 90 s one keeps
    // the 60 s default.
    const watchdogBefore = process.env.WATCHDOG_USEC;
    process.env.WATCHDOG_USEC = "4500000";
    const cli = await cliWithBusyFirstPass(false);
    try {
      const startedAt = Date.now();
      // 2dde3878: { outcome: "still waiting", stopped: [] } (the start retried until the 10 s probe bound).
      const outcome = await Promise.race([cli.running.then(code => `exited ${code}`),
        new Promise<string>(resolve => setTimeout(() => resolve("still waiting"), 10_000))]);
      const stopped = stoppedLines(cli.logs);
      expect({ outcome, stopped: stopped.length }).toEqual({ outcome: "exited 1", stopped: 1 });
      expect(stopped[0]).toMatch(/^mesh-bridge: stopped: Bridge start still refused after \d+ ms \(deadline \d+ ms, \d+ attempts\); last: mesh lock timeout: FABRIC_MESH_LOCK_TIMEOUT: .*host-lease-commits[\\/]5f43c55dca0091e6[\\/]custody\.lock/);
      expect(Date.now() - startedAt).toBeLessThan(8_000);
      expect(cli.logs.join("")).toMatch(RETRY_LINE);
      expect(cli.logs.join("")).not.toContain("linked hub <-> far");
      expect(readHostLeaseCurrent(cli.hubRoot, cli.id)).toBeUndefined();
    } finally {
      await cli.cleanup();
      if (watchdogBefore === undefined) delete process.env.WATCHDOG_USEC;
      else process.env.WATCHDOG_USEC = watchdogBefore;
    }
  }, 30_000);

  it("the start deadline is two thirds of WATCHDOG_USEC, never above the 60 s default", () => {
    expect(BRIDGE_START_DEADLINE_MS).toBe(60_000);
    expect(bridgeStartDeadlineMs({})).toBe(60_000);
    expect(bridgeStartDeadlineMs({ WATCHDOG_USEC: "90000000" })).toBe(60_000); // the documented WatchdogSec=90
    expect(bridgeStartDeadlineMs({ WATCHDOG_USEC: "30000000" })).toBe(20_000);
    expect(bridgeStartDeadlineMs({ WATCHDOG_USEC: "600000000" })).toBe(60_000);
    expect(bridgeStartDeadlineMs({ WATCHDOG_USEC: "junk" })).toBe(60_000);
  });

  it("a waiting start logs its first refusal, each change of refusal type and once a minute, and makes its last attempt at the deadline", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    try {
      const hub = store("file"), far = store("file");
      const logs: Array<[number, string]> = [];
      const t0 = Date.now();
      const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: new StoreBridgeSide(hub, "far"),
        remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(path.dirname(hub.root), "cursor.json"),
        log: line => logs.push([Date.now() - t0, line]) });
      const attempts: number[] = [];
      vi.spyOn(bridge, "start").mockImplementation(async () => {
        const at = Date.now() - t0;
        attempts.push(at);
        // From 30 s on, a different refusal type: another link's custody outwaited, not a busy gate.
        throw at < 30_000 ? new MeshLockTimeoutError(" host lease lock gate", 1, 0)
          : new ParticipantFileCustodyTimeoutError("namespace", 3, 5);
      });
      const failure = bridge.startWithBackoff(150_000).catch(error => error);
      await vi.advanceTimersByTimeAsync(151_000);
      const error = await failure;
      expect(error).toBeInstanceOf(BridgeStartDeadlineError);
      expect(error).toMatchObject({ waitedMs: 150_000, deadlineMs: 150_000, attempts: attempts.length });
      // 100 ms doubling to 2 s, and the last wait is clamped so the last attempt is made at the deadline.
      expect(attempts.slice(0, 7)).toEqual([0, 100, 300, 700, 1_500, 3_100, 5_100]);
      expect(attempts.at(-2)).toBe(149_100);
      expect(attempts.at(-1)).toBe(150_000);
      // First refusal; the type change at 31.1 s; a minute later, 91.1 s. Not one line per attempt.
      expect(logs.map(([at]) => at)).toEqual([0, 31_100, 91_100]);
      expect(logs[0]![1]).toMatch(/^start: mesh lock timeout; retrying in 100 ms: FABRIC_MESH_LOCK_TIMEOUT/);
      expect(logs[1]![1]).toMatch(/^start: mesh lock timeout; retrying in 2000 ms \(waiting 31100 ms of 150000 ms, 20 attempts\): /);
      expect(logs[2]![1]).toContain("(waiting 91100 ms of 150000 ms, 50 attempts)");
      expect(attempts).toHaveLength(80);
      expect(error.message).toMatch(/^Bridge start still refused after 150000 ms \(deadline 150000 ms, 80 attempts\); last: mesh lock timeout: /);
      vi.useRealTimers();
      await bridge.stop();
    } finally { vi.useRealTimers(); }
  });
});

// pi-fabric#1724, review blocker 1: exercise real start/recovery/presence, not a mocked start().
// The original CLI custody regression above remains the operator-visible reason-line proof.
describe("startup recovery budgets do not replace the last busy refusal", () => {
  afterEach(() => vi.restoreAllMocks());

  const recoveringStartup = async (options: {
    budgetMs?: number;
    mark?: "at" | "beyond";
    clock?: () => number;
    log?: (line: string) => void;
  } = {}) => {
    const hub = store("file"), far = store("file");
    const now = Date.now();
    const nativeHost = { ...host, incarnationToken: host.incarnationToken!,
      startedAt: now - 2_000, updatedAt: now, expiresAt: now + 120_000 };
    await far.put({ key: hostKey(identity.id), identity, value: nativeHost });
    writeHostLease(far.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
      incarnationToken: nativeHost.incarnationToken, startedAt: nativeHost.startedAt, updatedAt: now, expiresAt: nativeHost.expiresAt });
    writeParticipantFile(far.root, { key: key("topology/participants/", identity.id), version: 1, updatedAt: now,
      updatedBy: identity, value: { ...participant, startedAt: nativeHost.startedAt, updatedAt: now } });
    const local = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
    const original = await far.publish({ topic: "fleet.work.startup.1", kind: "ask", from: identity,
      to: "factory", text: "recovered once" });
    // An append survived before the source checkpoint. Recovery must retain this real ID.
    const recovered = await local.publish({ topic: original.topic, kind: original.kind, from: identity,
      to: "factory", text: "recovered once", data: { bridge: { from: "far", id: original.id } } });
    const cursorPath = path.join(path.dirname(hub.root), "cursor.json");
    const saved = JSON.stringify({ format: 1, local: "hub", remote: "far",
      toRemote: { after: recovered.sequence, mark: original.sequence + (options.mark === "beyond" ? 1 : 0) },
      toLocal: { after: 0, mark: options.mark === undefined ? 0 : recovered.sequence + (options.mark === "beyond" ? 1 : 0) } });
    fs.writeFileSync(cursorPath, saved);
    const logs: string[] = [];
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
      ...(options.budgetMs === undefined ? {} : { startDeadlineMs: options.budgetMs }),
      ...(options.clock ? { clock: options.clock } : {}),
      log: line => { logs.push(line); options.log?.(line); } });
    return { bridge, local, remote, hub, logs, original, cursorPath, saved };
  };

  it.each(["busy", "success"] as const)("a real final attempt at the deadline can end in %s after recovering an uncheckpointed ID", async outcome => {
    let now = 0;
    const b = await recoveringStartup({ clock: () => now, log: line => {
      // Model the retry waking at its deadline without faking the native reader's I/O or clock.
      if (line.startsWith("start:")) now = 3_000;
    } });
    const first = new MeshLockTimeoutError(" first custody gate", 1, 0);
    const last = new MeshLockTimeoutError(" final custody gate", 1, 0);
    const mirror = b.local.mirror.bind(b.local);
    const attempts: number[] = [];
    vi.spyOn(b.local, "mirror").mockImplementation(async (...args) => {
      attempts.push(now);
      if (attempts.length === 1) throw first;
      if (outcome === "busy") throw last;
      return mirror(...args);
    });
    const read = vi.spyOn(b.local, "bridgedIds");
    try {
      const error = await b.bridge.startWithBackoff(3_000).then(() => undefined, (failure: unknown) => failure);
      expect(attempts).toEqual([0, 3_000]);
      expect(read).toHaveBeenCalledTimes(2);
      expect(b.local.recoveryReadCost?.events).toBe(1);
      expect(b.logs.filter(line => line.startsWith("recovery (toLocal): ") &&
        line.includes("matchingIds=1") && line.includes("completed=true"))).toHaveLength(2);
      if (outcome === "busy") {
        expect(error).toBeInstanceOf(BridgeStartDeadlineError);
        expect(error).toMatchObject({ waitedMs: 3_000, deadlineMs: 3_000, attempts: 2 });
        expect((error as BridgeStartDeadlineError).lastError).toBe(last);
        expect((error as Error).message).toMatch(/last: mesh lock timeout: .*final custody gate/);
        expect(readHostLeaseCurrent(b.hub.root, identity.id)).toBeUndefined();
        expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
      } else {
        expect(error).toBeUndefined();
        expect(readHostLeaseCurrent(b.hub.root, identity.id)).toMatchObject({ id: identity.id,
          incarnationToken: host.incarnationToken });
        // Readiness includes real custody; the recovered ID then prevents a duplicate append.
        expect(await b.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
        const received = b.hub.read({ after: 0, limit: 100 }).filter(event => event.topic === b.original.topic);
        expect(received).toHaveLength(1);
        expect(received[0]).toMatchObject({ data: { bridge: { id: b.original.id } } });
        expect(JSON.parse(fs.readFileSync(b.cursorPath, "utf8")).toLocal.after).toBe(b.original.sequence);
      }
    } finally { await b.bridge.stop(); }
  });

  it.each([0, 20])("still enforces a finite %i ms per-attempt budget when real recovery remains", async budgetMs => {
    const b = await recoveringStartup({ budgetMs });
    const read = b.local.bridgedIds.bind(b.local);
    vi.spyOn(b.local, "bridgedIds").mockImplementation(async after => {
      const page = await read(after);
      await new Promise(resolve => setTimeout(resolve, 25));
      return page;
    });
    const presenceRead = vi.spyOn(b.local, "presence");
    try {
      await expect(b.bridge.startWithBackoff(300)).rejects.toThrow(`Bridge recovery exceeded ${budgetMs} ms startup budget`);
      expect(presenceRead).not.toHaveBeenCalled();
      expect(readHostLeaseCurrent(b.hub.root, identity.id)).toBeUndefined();
      expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
      expect(b.logs.join("\n")).toMatch(/recovery \(toLocal\): .*completed=false cancelled=true/);
      expect(b.logs.some(line => line.startsWith("start:"))).toBe(false);
    } finally { await b.bridge.stop(); }
  });

  it("caps real recovery by the caller's start bound even without an options.startDeadlineMs (the CLI path)", async () => {
    const b = await recoveringStartup();
    expect(b.bridge.options.startDeadlineMs).toBeUndefined();
    const read = b.local.bridgedIds.bind(b.local);
    vi.spyOn(b.local, "bridgedIds").mockImplementation(async after => {
      const page = await read(after);
      await new Promise(resolve => setTimeout(resolve, 25));
      return page;
    });
    const presenceRead = vi.spyOn(b.local, "presence");
    try {
      await expect(b.bridge.startWithBackoff(20)).rejects.toThrow("Bridge recovery exceeded 20 ms startup budget");
      expect(presenceRead).not.toHaveBeenCalled();
      expect(readHostLeaseCurrent(b.hub.root, identity.id)).toBeUndefined();
      expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
    } finally { await b.bridge.stop(); }
  });

  it.each(["at", "beyond"] as const)("allows zero-budget no-op recovery with marks %s the real horizons, then installs presence", async mark => {
    const b = await recoveringStartup({ budgetMs: 0, mark });
    const localRead = vi.spyOn(b.local, "bridgedIds"), remoteRead = vi.spyOn(b.remote, "bridgedIds");
    try {
      await b.bridge.start(0);
      expect(localRead).not.toHaveBeenCalled();
      expect(remoteRead).not.toHaveBeenCalled();
      expect(readHostLeaseCurrent(b.hub.root, identity.id)).toMatchObject({ id: identity.id,
        incarnationToken: host.incarnationToken });
      expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
    } finally { await b.bridge.stop(); }
  });

  it("still cancels a zero-budget no-op while its horizon read is in flight", async () => {
    const b = await recoveringStartup({ budgetMs: 0, mark: "at" });
    const head = b.local.latestCursor.bind(b.local);
    vi.spyOn(b.local, "latestCursor").mockImplementation(async () => {
      await b.bridge.stop();
      return head();
    });
    const presenceRead = vi.spyOn(b.local, "presence");
    try {
      await expect(b.bridge.start(0)).rejects.toThrow(/cancelled|fenced/);
      expect(presenceRead).not.toHaveBeenCalled();
      expect(readHostLeaseCurrent(b.hub.root, identity.id)).toBeUndefined();
      expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
    } finally { await b.bridge.stop(); }
  });
});
