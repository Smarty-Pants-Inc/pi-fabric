// expected-skip: no NATS_BIN/PATH executable: a real authenticated loopback broker is required.
// Keep reason: #17194 lost root AND live child presence after local lease loss.
// Real runtime, KV CAS, control/ACK and child process; inference is a deterministic worker fixture.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { readLocalHostLeaseProcessProof, TransportHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import { openFabricTransport } from "../src/transport/index.js";
import type { TransportKv } from "../src/transport/types.js";
import { NatsKvStore } from "../src/transport/nats/kv.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { createPathBHarness, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: lease recovery requires NATS_BIN");
let broker: PathBHarness;
beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
  try { expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0); }
  finally { vi.unstubAllEnvs(); }
}, 30_000);
afterAll(async () => { await broker?.stop(); vi.restoreAllMocks(); vi.unstubAllEnvs(); });

function main(root: string, sessionId: string, machine: "machine-a" | "machine-b") {
  const model = { provider: "fixture", id: "lease-recovery", name: "lease recovery fixture", api: "openai-completions",
    contextWindow: 100_000, maxTokens: 1_000, reasoning: false, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => sessionId, appendEntry() {}, sendMessage() {},
  } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    // The root remains in an OPEN turn throughout loss and automatic recovery.
    isIdle: () => false, hasPendingMessages: () => false, model,
    modelRegistry: { getAvailable: () => [model], getAll: () => [model], find: () => model,
      getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "fixture" }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("src/index.ts"), worker: path.join(root, "live-worker.mjs"),
    residentHost: path.join(root, "unused-resident.mjs"), skills: root,
  } });
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(root, machine, "mesh"),
      nats: { ...broker.runtimeOptions(machine === "machine-a" ? broker.users.machineA : broker.users.machineB), operatorMachine: "fabric-ops" } },
    agents: { enabled: true, model: "fixture/lease-recovery", deniedModels: [], nice: 0 },
    residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
  const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
    cwd: root, signal: AbortSignal.timeout(15_000), parentToolCallId: "lease-recovery", nestedToolCallId: ref, extensionContext: context,
    update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
  });
  return { runtime, context, config, invoke };
}

const key = (kind: "p" | "h", id: string) => `machine-a.${kind}.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;

/** Main's broker-expiry barrier: wait for the actual TTL marker, never delete a lease. */
function armLeaseGone(kv: TransportKv, leaseKey: string) {
  const stop = new AbortController();
  let readyResolve!: () => void;
  let readyReject!: (error: unknown) => void;
  const ready = new Promise<void>((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  const gone = (async () => {
    let seen = false;
    try {
      const signal = AbortSignal.any([stop.signal, AbortSignal.timeout(30_000)]);
      for await (const event of kv.watch(">", signal)) {
        if (event.type === "ready") readyResolve();
        if (event.type === "gone" && event.key === leaseKey) {
          expect(event.reason).toBe("expired");
          seen = true;
          readyResolve();
          return;
        }
      }
      if (!seen) throw new Error(`lease expiry watch ended before ${leaseKey}`);
    } catch (error) {
      readyReject(error);
      throw error;
    } finally { stop.abort(); }
  })();
  return { ready, gone };
}

describe.skipIf(!binary || process.platform !== "linux")("NATS root lease recovery with a live child", () => {
  it("automatically re-acquires and retains live presence during an open turn, then delivers a remote child followUp", async () => {
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-lease-recovery-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
    // Native child stays alive and consumes the real manager's steer channel. No model/network call.
    const received = path.join(root, "child-received.jsonl");
    fs.writeFileSync(path.join(root, "live-worker.mjs"), `
import fs from "node:fs";
import path from "node:path";
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index].slice(2), process.argv[index + 1]);
const now = Date.now(), status = args.get("status-file"), steer = args.get("steer-file");
const record = { id: args.get("id"), name: args.get("name"), task: fs.readFileSync(args.get("task-file"), "utf8"),
  status: "running", runner: "pi", transport: "process", sessionId: String(process.pid), cwd: args.get("cwd"),
  startedAt: now, updatedAt: now, turns: 1, toolCalls: 1, text: "", exitCode: null,
  usage: { input: 1, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
fs.writeFileSync(status + ".tmp", JSON.stringify(record)); fs.renameSync(status + ".tmp", status);
fs.writeFileSync(${JSON.stringify(received)}, JSON.stringify({ type: "prompt", pid: process.pid }) + "\\n");
let seen = 0;
const read = () => {
  if (!fs.existsSync(steer)) return;
  const lines = fs.readFileSync(steer, "utf8").split("\\n").filter(Boolean);
  for (const line of lines.slice(seen)) fs.appendFileSync(${JSON.stringify(received)}, line + "\\n");
  seen = lines.length;
};
fs.watch(path.dirname(steer), read); read();
process.on("SIGTERM", () => process.exit(0));
`);
    const ownerSession = "17194-owner", id = `session:${ownerSession}`;
    const owner = main(root, ownerSession, "machine-a"), sender = main(root, "17194-sender", "machine-b");
    let lease: TransportHostLease | undefined, senderLease: TransportHostLease | undefined;
    const acquire = TransportHostLease.prototype.acquire;
    vi.spyOn(TransportHostLease.prototype, "acquire").mockImplementation(function (this: TransportHostLease, signal) {
      if (this.self.hostId === id) lease = this;
      if (this.self.hostId === "session:17194-sender") senderLease = this;
      return acquire.call(this, signal);
    });
    // Observe actual broker-confirmed revisions; no KV read/write/cleanup is substituted.
    const publications = new Map<string, { revision: number; value: unknown }>();
    const put = NatsKvStore.prototype.put;
    vi.spyOn(NatsKvStore.prototype, "put").mockImplementation(async function <V>(this: NatsKvStore, entryKey: string,
      ownerToken: string, value: V, ifRevision: number, signal?: AbortSignal) {
      const revision = await put.call(this, entryKey, ownerToken, value, ifRevision, signal);
      publications.set(entryKey, { revision, value });
      return revision;
    });
    let clock: ReturnType<typeof vi.spyOn> | undefined, clockOffset = 13_001;
    try {
      await owner.runtime.initialize(owner.context, owner.config);
      await sender.runtime.initialize(sender.context, sender.config);
      const child = await owner.runtime.agents.spawn({ task: "STEER_ME", transport: "process", model: "fixture/lease-recovery" });
      await vi.waitFor(() => {
        const status = owner.runtime.agents.status(child.id);
        expect(status.status, JSON.stringify(status)).toBe("running");
        expect(fs.existsSync(received), JSON.stringify(status)).toBe(true);
        expect(fs.readFileSync(received, "utf8")).toContain('"type":"prompt"');
        expect(sender.runtime.participantInfos().find(row => row.id === child.id)).toMatchObject({ stale: false });
      }, { timeout: 10_000 });
      await owner.runtime.publishHostLifecycle("pi.agent_start", {});
      const beforeHost = publications.get(key("h", id))!.revision;
      const beforeRoot = publications.get(key("p", id))!.revision;
      const beforeChild = publications.get(key("p", child.id))!.revision;
      expect(lease).toBeDefined();
      // Inject the REAL local cutoff without blocking this test/broker for 13 s.
      // Keep the monotonic offset thereafter: restoring it early would mean clock rollback.
      const now = performance.now.bind(performance);
      clock = vi.spyOn(performance, "now").mockImplementation(() => now() + clockOffset);
      expect(lease!.live()).toBe(false);
      // No start/resume/refresh/reacquire call below: the existing heartbeat owns recovery.
      await vi.waitFor(() => {
        expect(publications.get(key("h", id))!.revision).toBeGreaterThan(beforeHost);
        // Main retains unchanged presence after same-owner CAS recovery; the renewed lease,
        // live remote routes and actual followUp below prove recovery, not redundant writes.
        expect(publications.get(key("p", id))!.revision).toBeGreaterThanOrEqual(beforeRoot);
        expect(publications.get(key("p", child.id))!.revision).toBeGreaterThanOrEqual(beforeChild);
        expect(sender.runtime.participantInfos().find(row => row.id === id)).toMatchObject({ stale: false });
        expect(sender.runtime.participantInfos().find(row => row.id === child.id)).toMatchObject({ stale: false, ownerHostId: id });
        expect(lease!.live()).toBe(true);
        // performance.now is process-wide: the sender must also finish its automatic recovery.
        expect(senderLease!.live()).toBe(true);
      }, { timeout: 12_000 });
      expect(owner.context.isIdle()).toBe(false);
      expect(owner.runtime.agents.status(child.id).status).toBe("running");
      const receipt = await sender.invoke("agents.followUp", { id: child.id, message: "CHILD_AFTER_LEASE_RECOVERY" });
      expect(receipt).toMatchObject({ acknowledged: true });
      await vi.waitFor(() => expect(fs.readFileSync(received, "utf8")).toContain("CHILD_AFTER_LEASE_RECOVERY"), { timeout: 5_000 });
      expect((publications.get(key("p", child.id))!.value as FabricParticipantRecord).ownerIncarnation)
        .toBe((publications.get(key("p", id))!.value as FabricParticipantRecord).ownerIncarnation);
      await owner.runtime.agents.stop(child.id);
      await vi.waitFor(() => {
        expect(sender.runtime.participantInfos().find(row => row.id === child.id)).toBeUndefined();
        expect(sender.runtime.participantInfos().find(row => row.id === id)).toMatchObject({ stale: false });
      }, { timeout: 5_000 });
      // Closing during a second suspension must settle cleanly, not restart a retired owner.
      clockOffset += 13_001;
      expect(lease!.live()).toBe(false);
      await expect(Promise.all([owner.runtime.shutdown("exit"), sender.runtime.shutdown("exit")])).resolves.toBeDefined();
      expect(lease!.signal.aborted).toBe(true);
    } finally {
      // Restore only after the owners have stopped; no artificial backwards clock during cleanup.
      await Promise.allSettled([owner.runtime.shutdown("exit"), sender.runtime.shutdown("exit")]);
      clock?.mockRestore();
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it.each(["renewal", "root check", "retry cap"] as const)(
    "bounds repeated final-renewal expiry and closes without republication (%s)", async (stopAt) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-recheck-close-"));
      const id = `session:recheck-close-${stopAt.replaceAll(" ", "-")}`, childId = `${id}:child`;
      const leaseKey = key("h", id), rootKey = key("p", id), childKey = key("p", childId);
      const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
      const config = normalizeFabricConfig({ mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: mesh.root,
        nats: { ...broker.runtimeOptions(broker.users.machineA), operatorMachine: "fabric-ops" } } });
      const transport = await openFabricTransport({ config: config.mesh, hostId: id, mesh });
      const stateDir = path.join(root, "transport"); fs.mkdirSync(stateDir, { mode: 0o700 });
      const directory = new ParticipantDirectory(mesh, { enabled: true, transport, transportStateDir: stateDir,
        hostId: id, rootId: id, identity: { id, name: "recheck-close", kind: "main", sessionId: id.slice(8) } });
      const record = (participantId: string, kind: "root" | "agent"): FabricParticipantRecord => ({
        format: 1, id: participantId, rootId: id, kind, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "source",
        sessionId: id.slice(8), name: "recheck-close", status: "running", runner: "pi", transport: kind === "root" ? "host" : "process",
        capabilities: ["followUp"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
      });
      directory.registerSource(() => [record(id, "root"), record(childId, "agent")]);
      let releaseRenew!: () => void, releaseStop!: () => void;
      const renewHeld = new Promise<void>(resolve => { releaseRenew = resolve; });
      const stopHeld = new Promise<void>(resolve => { releaseStop = resolve; });
      const holdUntilReleasedOrAborted = async (signal?: AbortSignal) => {
        signal?.throwIfAborted();
        let abort!: () => void;
        try {
          await Promise.race([stopHeld, new Promise<never>((_, reject) => {
            abort = () => reject(signal!.reason);
            signal?.addEventListener("abort", abort, { once: true });
          })]);
        } finally { signal?.removeEventListener("abort", abort); }
      };
      const expiryTarget = stopAt === "retry cap" ? 3 : 2;
      let expiredFinals = 0, checks = 0, stopped = false, nextPass = false;
      const writes: string[] = [];
      const rawRenew = TransportHostLease.prototype.renew;
      vi.spyOn(TransportHostLease.prototype, "renew").mockImplementation(async function (this: TransportHostLease, signal) {
        if (this.key !== leaseKey) return rawRenew.call(this, signal);
        await renewHeld;
        if (stopAt === "retry cap" && expiredFinals === expiryTarget && this.rootRecheck) {
          nextPass = true; // Only a later heartbeat can get here after the capped recovery promise settles.
          await holdUntilReleasedOrAborted(signal);
        }
        // As in the second-expiry fixture, remove ONLY the early renewal budgets so real 15s
        // broker TTLs can elapse. The operation held during close keeps its actual caller signal.
        return rawRenew.call(this, expiredFinals < expiryTarget ? undefined : signal);
      });
      const rawConfirm = KvParticipantDirectory.prototype.confirmSoleRoot;
      vi.spyOn(KvParticipantDirectory.prototype, "confirmSoleRoot").mockImplementation(async function (this: KvParticipantDirectory, value, revision, ...signals) {
        if (value.id !== id) return rawConfirm.call(this, value, revision, ...signals);
        checks++;
        if (expiredFinals === expiryTarget && stopAt !== "renewal") {
          stopped = true;
          await holdUntilReleasedOrAborted(signals[0]);
        }
        return rawConfirm.call(this, value, revision, ...signals);
      });
      const rawPut = NatsKvStore.prototype.put;
      vi.spyOn(NatsKvStore.prototype, "put").mockImplementation(async function <V>(this: NatsKvStore, entryKey: string,
        ownerToken: string, value: V, ifRevision: number, signal?: AbortSignal) {
        if (entryKey === leaseKey && checks > 0 && !(value as FabricKvHostLease).rootRecheck) {
          if (expiredFinals < expiryTarget) {
            const expired = armLeaseGone(transport.kv("leases"), leaseKey);
            await expired.ready;
            await expired.gone; // EVERY final renewal so far crosses a real KV expiry and re-acquires.
            expiredFinals++;
          } else if (stopAt === "renewal") {
            stopped = true;
            await holdUntilReleasedOrAborted(signal);
          }
        }
        const revision = await rawPut.call(this, entryKey, ownerToken, value, ifRevision, signal);
        writes.push(entryKey);
        return revision;
      });
      let closing: Promise<void> | undefined;
      try {
        await directory.start();
        const initialExpiry = armLeaseGone(transport.kv("leases"), leaseKey);
        await initialExpiry.ready;
        await initialExpiry.gone;
        writes.length = 0;
        releaseRenew();
        await vi.waitFor(() => {
          expect(expiredFinals).toBe(expiryTarget);
          if (stopAt === "retry cap") {
            expect(nextPass, "capped work must settle for the next heartbeat, not enter a fourth root check").toBe(true);
            expect(checks).toBe(3);
          } else expect(stopped).toBe(true);
        }, { timeout: 75_000 });
        expect(directory.canConsumeMesh()).toBe(false);
        directory.scheduleRefresh();
        await expect(directory.refreshPresence()).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
        expect(writes.filter(entryKey => entryKey === rootKey || entryKey === childKey)).toEqual([]);
        const start = Date.now();
        let closed = false;
        closing = directory.close().then(() => { closed = true; });
        // Much tighter than the old 5s operation budget: close must CANCEL the current wait,
        // not wait for its timeout (or for another broker TTL in an unbounded recovery loop).
        await vi.waitFor(() => expect(closed, "directory close must cancel pending recovery").toBe(true), { timeout: 2_000 });
        expect(Date.now() - start).toBeLessThan(2_000);
        expect(directory.heartbeatArmed).toBe(false);
        expect(directory.canConsumeMesh()).toBe(false);
        expect(writes.filter(entryKey => entryKey === rootKey || entryKey === childKey)).toEqual([]);
        expect(await transport.kv("presence").get(rootKey)).toBeUndefined();
        expect(await transport.kv("presence").get(childKey)).toBeUndefined();
      } finally {
        // The unfixed baseline can finish after the assertion times out; never leave its
        // deliberately stalled KV call/root check alive during cleanup.
        releaseRenew(); releaseStop();
        await (closing ?? directory.close()).catch(() => undefined);
        await transport.close().catch(() => undefined);
        vi.restoreAllMocks();
        fs.rmSync(root, { recursive: true, force: true });
      }
    }, 110_000);

  // Same real broker fence as main's #15310 regression, extended to retained child presence.
  it.each([
    { foreignOwner: false, secondExpiry: false }, { foreignOwner: true, secondExpiry: false },
    { foreignOwner: false, secondExpiry: true }, { foreignOwner: true, secondExpiry: true },
  ])("does not republish an expired incarnation's children before root re-verification (foreign owner: $foreignOwner, second expiry: $secondExpiry)", async ({ foreignOwner, secondExpiry }) => {
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-root-recheck-"));
    const id = `session:lease-recheck-${foreignOwner}-${secondExpiry}`, childId = `${id}:child`;
    const identity = { id, name: "recheck-owner", kind: "main" as const, sessionId: id.slice(8) };
    const leaseKey = key("h", id), rootKey = key("p", id), childKey = key("p", childId);
    const transportFor = async (machine: "machine-a" | "machine-b", hostId: string) => {
      const mesh = new MeshStore(path.join(root, machine, hostId), 65_536, 1_000);
      const config = normalizeFabricConfig({ mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: mesh.root,
        nats: { ...broker.runtimeOptions(machine === "machine-a" ? broker.users.machineA : broker.users.machineB), operatorMachine: "fabric-ops" } } });
      return { mesh, transport: await openFabricTransport({ config: config.mesh, hostId, mesh }) };
    };
    let releaseRenew!: () => void, releaseCheck!: () => void, releaseSecondPut!: () => void, releaseSecondCheck!: () => void;
    const renewHeld = new Promise<void>(resolve => { releaseRenew = resolve; });
    const checkHeld = new Promise<void>(resolve => { releaseCheck = resolve; });
    const secondPutHeld = new Promise<void>(resolve => { releaseSecondPut = resolve; });
    const secondCheckHeld = new Promise<void>(resolve => { releaseSecondCheck = resolve; });
    const checksEntered: number[] = [], checksPassed: number[] = [];
    let secondPutEntered = false;
    const rawRenew = TransportHostLease.prototype.renew;
    vi.spyOn(TransportHostLease.prototype, "renew").mockImplementation(function (this: TransportHostLease, signal?: AbortSignal) {
      if (this.key !== leaseKey) return rawRenew.call(this, signal);
      // Let the final unflagged CAS remain in flight through a real broker TTL, without
      // the directory's 5s request budget ending it first. No lease/KV result is replaced.
      return renewHeld.then(() => rawRenew.call(this,
        secondExpiry && checksPassed.length === 1 ? undefined : AbortSignal.timeout(5_000)));
    });
    // The old source has no re-check seam. Let it execute normally so the SAME test bytes
    // fail on missing recovery/publication fencing, not on spy setup or a missing method.
    if ("confirmSoleRoot" in KvParticipantDirectory.prototype) {
      const rawConfirm = KvParticipantDirectory.prototype.confirmSoleRoot;
      vi.spyOn(KvParticipantDirectory.prototype, "confirmSoleRoot").mockImplementation(async function (this: KvParticipantDirectory, record, revision) {
        checksEntered.push(revision);
        await (checksEntered.length === 1 ? checkHeld : secondCheckHeld);
        await rawConfirm.call(this, record, revision);
        checksPassed.push(revision);
      });
    }
    const writes: string[] = [];
    const rawPut = NatsKvStore.prototype.put;
    vi.spyOn(NatsKvStore.prototype, "put").mockImplementation(async function <V>(this: NatsKvStore, entryKey: string,
      ownerToken: string, value: V, ifRevision: number, signal?: AbortSignal) {
      if (secondExpiry && entryKey === leaseKey && checksPassed.length === 1 && !secondPutEntered &&
        !(value as FabricKvHostLease).rootRecheck) {
        secondPutEntered = true;
        await secondPutHeld;
      }
      const revision = await rawPut.call(this, entryKey, ownerToken, value, ifRevision, signal);
      writes.push(entryKey);
      return revision;
    });
    const a = await transportFor("machine-a", id), b = await transportFor("machine-b", "recheck-observer");
    const stateDir = path.join(root, "transport-a"); fs.mkdirSync(stateDir, { mode: 0o700 });
    const directory = new ParticipantDirectory(a.mesh, { enabled: true, transport: a.transport, transportStateDir: stateDir,
      hostId: id, rootId: id, identity });
    const observer = new KvParticipantDirectory({ transport: b.transport, lease: { live: () => true, signal: new AbortController().signal },
      identityId: "recheck-observer", rootId: "recheck-observer", ownerIncarnation: "observer" });
    let name = "recheck-owner";
    const record = (participantId: string, kind: "root" | "agent"): FabricParticipantRecord => ({
      format: 1, id: participantId, rootId: id, kind, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "source",
      sessionId: identity.sessionId, name, status: "running", runner: "pi", transport: kind === "root" ? "host" : "process",
      capabilities: ["followUp"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
    });
    directory.registerSource(() => [record(id, "root"), record(childId, "agent")]);
    const registerForeignOwner = async () => {
      // Another authenticated machine acquires the same root during the broker gap.
      const foreignKey = leaseKey.replace("machine-a.", "machine-b.");
      const at = Date.now();
      await b.transport.kv("leases").put(foreignKey, `${id}#taker`, {
        id, rootId: id, identityId: id, incarnation: "taker", incarnationToken: "taker", ...readLocalHostLeaseProcessProof(),
        updatedAt: at, expiresAt: at + 15_000,
      }, 0);
      await b.transport.kv("presence").put(rootKey.replace("machine-a.", "machine-b."), `${id}#taker`,
        { ...record(id, "root"), ownerIncarnation: "taker" }, 0);
    };
    try {
      observer.start();
      await directory.start();
      await vi.waitFor(() => expect(observer.resolve(childId)?.live).toBe(true), { timeout: 5_000 });
      const beforeRoot = await a.transport.kv("presence").get(rootKey);
      const beforeChild = await a.transport.kv("presence").get(childKey);
      const expired = armLeaseGone(b.transport.kv("leases"), leaseKey);
      await expired.ready;
      await expired.gone; // A real 15s broker expiry, not a local-clock injection or KV deletion.
      expect(await b.transport.kv("leases").get(leaseKey)).toBeUndefined();
      await vi.waitFor(() => expect(observer.resolve(childId)?.live).toBe(false), { timeout: 5_000 });
      if (foreignOwner && !secondExpiry) await registerForeignOwner();
      writes.length = 0;
      releaseRenew(); // Only release the stalled heartbeat; no explicit resume/reacquire call.
      await vi.waitFor(async () => expect(await a.transport.kv("leases").get(leaseKey)).toBeDefined(), { timeout: 8_000 });
      name = "changed-while-root-recheck-pending";
      directory.scheduleRefresh();
      const refresh = await Promise.allSettled([directory.refreshPresence()]);
      await new Promise(resolve => setTimeout(resolve, 1_500)); // Main's bound for the scheduled change refresh.
      expect(writes.filter(entryKey => entryKey === rootKey || entryKey === childKey),
        "neither retained root nor child may republish before root re-verification").toEqual([]);
      expect(checksEntered).toHaveLength(1);
      expect(refresh[0]).toMatchObject({ status: "rejected", reason: { code: "FABRIC_TRANSPORT_UNAVAILABLE" } });
      expect(directory.canConsumeMesh()).toBe(false);
      const recreated = await a.transport.kv("leases").get<FabricKvHostLease>(leaseKey);
      expect(recreated?.value).toMatchObject({ rootRecheck: true });
      expect((await a.transport.kv("presence").get(rootKey))?.revision).toBe(beforeRoot!.revision);
      expect((await a.transport.kv("presence").get(childKey))?.revision).toBe(beforeChild!.revision);
      expect(observer.resolve(childId)?.live).toBe(false);
      if (secondExpiry) {
        const expiredAgain = armLeaseGone(b.transport.kv("leases"), leaseKey);
        await expiredAgain.ready;
        releaseCheck(); // First check passes, then the final unflagged renewal stalls at its real CAS.
        await vi.waitFor(() => expect(secondPutEntered).toBe(true), { timeout: 5_000 });
        expect(checksPassed).toEqual([recreated!.revision]);
        await expiredAgain.gone;
        expect(await b.transport.kv("leases").get(leaseKey)).toBeUndefined();
        if (foreignOwner) await registerForeignOwner();
        releaseSecondPut(); // CAS sees the absent row and the real lease code re-acquires again.
        await vi.waitFor(async () => {
          const latest = await a.transport.kv("leases").get<FabricKvHostLease>(leaseKey);
          expect(latest?.revision).toBeGreaterThan(recreated!.revision);
          expect(latest?.value).toMatchObject({ rootRecheck: true });
        }, { timeout: 8_000 });
        name = "changed-after-second-expiry";
        directory.scheduleRefresh();
        const secondRefresh = await Promise.allSettled([directory.refreshPresence()]);
        await new Promise(resolve => setTimeout(resolve, 1_500)); // Same scheduled-refresh bound as the first gap.
        expect(writes.filter(entryKey => entryKey === rootKey || entryKey === childKey),
          "neither retained root nor child may republish before re-verification after the second expiry").toEqual([]);
        expect(checksEntered).toHaveLength(2);
        const latest = await a.transport.kv("leases").get<FabricKvHostLease>(leaseKey);
        expect(checksEntered[1]).toBe(latest!.revision);
        expect(checksEntered[1]).toBeGreaterThan(checksEntered[0]!);
        expect(checksPassed).toHaveLength(1);
        expect(secondRefresh[0]).toMatchObject({ status: "rejected", reason: { code: "FABRIC_TRANSPORT_UNAVAILABLE" } });
        expect(directory.canConsumeMesh()).toBe(false);
        expect((await a.transport.kv("presence").get(rootKey))?.revision).toBe(beforeRoot!.revision);
        expect((await a.transport.kv("presence").get(childKey))?.revision).toBe(beforeChild!.revision);
        expect(observer.resolve(childId)?.live).toBe(false);
        releaseSecondCheck(); // Real ordered-watch check runs against the second recreated revision.
      } else {
        releaseCheck(); // Run the real ordered-watch revision/other-owner check, not a stub verdict.
      }
      if (foreignOwner) {
        await vi.waitFor(() => expect(directory.heartbeatArmed).toBe(false), { timeout: 5_000 });
        expect(directory.canConsumeMesh()).toBe(false);
        await vi.waitFor(async () => expect(await a.transport.kv("presence").get(childKey)).toBeUndefined(), { timeout: 5_000 });
        expect(writes.filter(entryKey => entryKey === rootKey || entryKey === childKey)).toEqual([]);
        expect(await b.transport.kv("leases").get(leaseKey.replace("machine-a.", "machine-b."))).toBeDefined();
      } else {
        await vi.waitFor(async () => {
          expect((await a.transport.kv("presence").get(childKey))!.revision).toBeGreaterThan(beforeChild!.revision);
          expect(observer.resolve(childId)?.live).toBe(true);
          expect(directory.canConsumeMesh()).toBe(true);
        }, { timeout: 8_000 });
        expect((await a.transport.kv("leases").get(leaseKey))?.value).not.toHaveProperty("rootRecheck");
      }
    } finally {
      releaseRenew(); releaseCheck(); releaseSecondPut(); releaseSecondCheck();
      await directory.close().catch(() => undefined);
      await observer.close().catch(() => undefined);
      await Promise.allSettled([a.transport.close(), b.transport.close()]);
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 90_000);
});
