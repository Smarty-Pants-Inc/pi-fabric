import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { runBridge } from "../src/mesh-bridge.js";
import { BRIDGE_PROTOCOL_VERSION, BridgeStaleIncarnationError, StoreBridgeSide } from "../src/mesh/bridge.js";
import { COMMIT_OUTBOX_PREFIX, CommitOutbox } from "../src/mesh/commit-outbox.js";
import { isMeshRetryableBusy } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshBatchView, type MeshIdentity } from "../src/mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { withParticipantFileDirectoryCustody, writeParticipantFile } from "../src/topology/participant-files.js";
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
});
