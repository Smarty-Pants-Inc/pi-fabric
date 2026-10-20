// expected-skip: no NATS_BIN/PATH executable: authenticated loopback broker is required.
// Keep reason: smarty-dev#14115 crossed broker CAS, bootstrap ordering and Main routing.
// No hosted runner/worker is started. Only the Pi host UI/model boundary is a fixture.
// ALL KV deletes are record-only; lease expiry is real (15s), presence TTL is real (600s).
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { Bucket } from "@nats-io/kv";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { openFabricTransport } from "../src/transport/index.js";
import { NatsKvStore } from "../src/transport/nats/kv.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import { readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { isOwnResidentActor } from "../src/residency/actor-ownership.js";
import { residentHostId } from "../src/residency/protocol.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport, TransportKv, TransportKvEntry } from "../src/transport/types.js";
import { createPathBHarness, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: succession needs an executable NATS_BIN");
const key = (machine: string, kind: "p" | "h", id: string) =>
  `${machine}.${kind}.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;
let broker: PathBHarness;
const deletes: Array<{ key: string; revision: number }> = [];

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
}, 30_000);
// vitest restoreMocks:true restores beforeAll spies before each test. Install the
// deletion guard in beforeEach, before ANY runtime/transport is created.
beforeEach(async () => {
  // Independent SDK backstop: an unwrapped or duplicate product adapter cannot
  // reach a broker delete/purge even if its outer record-only guard is bypassed.
  vi.spyOn(Bucket.prototype, "delete").mockImplementation(async () => { throw new Error("Real broker KV delete forbidden by succession fixture"); });
  vi.spyOn(Bucket.prototype, "purge").mockImplementation(async () => { throw new Error("Real broker KV purge forbidden by succession fixture"); });
  vi.spyOn(NatsKvStore.prototype, "delete").mockImplementation(async (entryKey, revision) => {
    deletes.push({ key: entryKey, revision }); return false;
  });
  // Behavioral guard preflight, before any runtime exists; no real store needed.
  expect(await NatsKvStore.prototype.delete("machine-a.p.guard-preflight", 1)).toBe(false);
  await expect(Bucket.prototype.delete("guard-preflight")).rejects.toThrow("Real broker KV delete forbidden");
  await expect(Bucket.prototype.purge("guard-preflight")).rejects.toThrow("Real broker KV purge forbidden");
  deletes.length = 0;
});
afterAll(async () => {
  try { await broker?.stop(); } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); }
});

function configFor(root: string, machine: "machine-a" | "machine-b"): FabricConfig {
  return normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(root, "mesh"),
      nats: { ...broker.runtimeOptions(machine === "machine-a" ? broker.users.machineA : broker.users.machineB), operatorMachine: "fabric-ops" } },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
}
function main(root: string, sessionId: string, label: string) {
  const messages: Array<{ message: unknown; options: unknown }> = [];
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => label, appendEntry() {}, sendMessage(message: unknown, options: unknown) { messages.push({ message, options }); },
  } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.join(root, "unused-worker.mjs"),
    residentHost: path.join(root, "unused-resident.mjs"), skills: root,
  } });
  const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
    cwd: root, signal: undefined, parentToolCallId: "presence-succession", nestedToolCallId: ref, extensionContext: context,
    update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
  });
  return { runtime, context, messages, invoke };
}
async function routeReady(view: KvParticipantDirectory, id: string) {
  const signal = AbortSignal.timeout(3_000);
  while (!view.ready) await view.whenChanged(id, signal);
  while (!view.resolve(id)?.live) await view.whenChanged(id, signal);
  return view.resolve(id)!;
}

type ParticipantRoute = NonNullable<ReturnType<KvParticipantDirectory["resolve"]>>;
type KvGetCall = { key: string; entry: TransportKvEntry<unknown> | undefined; store: NatsKvStore };
type KvPutCall = { key: string; owner: string; value: unknown; ifRevision: number; store: NatsKvStore };
type KvPutHook = (call: KvPutCall) => Promise<void> | void;
let successionFixtureNumber = 0;

async function directoryReady(view: KvParticipantDirectory, id: string, timeoutMs = 5_000): Promise<void> {
  const signal = AbortSignal.timeout(timeoutMs);
  while (!view.ready) await view.whenChanged(id, signal);
}

async function routeEventually(view: KvParticipantDirectory, id: string,
  predicate: (route: ParticipantRoute) => boolean, timeoutMs = 5_000): Promise<ParticipantRoute> {
  const signal = AbortSignal.timeout(timeoutMs);
  for (;;) {
    if (!view.ready) { await view.whenChanged(id, signal); continue; }
    const route = view.resolve(id);
    if (route && predicate(route)) return route;
    await view.whenChanged(id, signal);
  }
}

/** A real KV watch barrier for broker expiry; no timer stands in for the 15s TTL. */
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

function installKvHooks(onPut: KvPutHook) {
  const gets: KvGetCall[] = [], puts: KvPutCall[] = [];
  const rawGet = NatsKvStore.prototype.get;
  const rawPut = NatsKvStore.prototype.put;
  vi.spyOn(NatsKvStore.prototype, "get").mockImplementation(async function <V>(this: NatsKvStore, entryKey: string, signal?: AbortSignal) {
    const entry = await rawGet.call(this, entryKey, signal);
    gets.push({ key: entryKey, entry: entry as TransportKvEntry<unknown> | undefined, store: this });
    return entry as TransportKvEntry<V> | undefined;
  });
  vi.spyOn(NatsKvStore.prototype, "put").mockImplementation(async function <V>(this: NatsKvStore,
    entryKey: string, owner: string, value: V, ifRevision: number, signal?: AbortSignal) {
    const call: KvPutCall = { key: entryKey, owner, value, ifRevision, store: this };
    puts.push(call);
    await onPut(call);
    return rawPut.call(this, entryKey, owner, value, ifRevision, signal);
  });
  return { gets, puts, rawPut };
}

function participant(id: string, incarnation: string, name: string, now: number): FabricParticipantRecord {
  return { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: incarnation, sessionId: id, name, status: "idle", runner: "pi", transport: "host",
    capabilities: ["followUp"], controlProtocol: "v1", startedAt: now, updatedAt: now + 1 };
}
function hostLease(id: string, incarnation: string, now: number): FabricKvHostLease {
  return { id, identityId: id, rootId: id, incarnation, pid: process.pid, pidStartTime: "12345",
    bootId: "tls-succession-fixture", updatedAt: now, expiresAt: now + 15_000 };
}

async function successionFixture(label: string, incarnation = `successor-${label}`) {
  const serial = ++successionFixtureNumber;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `presence-boundary-${label}-${serial}-`));
  const id = `session:presence-boundary-${label}-${serial}`;
  const transport = await openFabricTransport({ config: configFor(root, "machine-a").mesh, hostId: id,
    mesh: new MeshStore(path.join(root, "mesh"), 65_536, 1_000) });
  const presence = transport.kv("presence"), leases = transport.kv("leases");
  const now = Date.now();
  const predecessor = participant(id, `predecessor-${label}-${serial}`, "predecessor", now - 2_000);
  const successor = participant(id, incarnation, "successor", now);
  const pkey = key("machine-a", "p", id), hkey = key("machine-a", "h", id);
  const predecessorOwner = `${id}#${predecessor.ownerIncarnation}`;
  const successorOwner = `${id}#${successor.ownerIncarnation}`;
  const oldRevision = await presence.put(pkey, predecessorOwner, predecessor, 0);
  const initialLease = hostLease(id, incarnation, now);
  const initialLeaseRevision = await leases.put(hkey, successorOwner, initialLease, 0);
  const leaseAbort = new AbortController();
  const gate = { live: () => true, signal: leaseAbort.signal };
  const view = new KvParticipantDirectory({ transport, lease: gate, identityId: id, rootId: id,
    ownerIncarnation: incarnation, operationTimeoutMs: 30_000 });
  // Only the observer identity differs: every KV operation still uses this real,
  // authenticated transport (class methods must retain their original receiver).
  const observerTransport: FabricTransport = { kind: transport.kind,
    self: { machine: transport.self.machine, hostId: `${id}-observer` },
    kv: transport.kv.bind(transport), send: transport.send.bind(transport), inbox: transport.inbox.bind(transport),
    drainState: transport.drainState.bind(transport), close: transport.close.bind(transport) };
  const observer = new KvParticipantDirectory({ transport: observerTransport, lease: gate, identityId: "observer",
    rootId: "observer", ownerIncarnation: `observer-${serial}` });
  view.start(); observer.start();
  await Promise.all([directoryReady(view, id), directoryReady(observer, id)]);
  return { root, id, transport, presence, leases, pkey, hkey, predecessor, successor, initialLease,
    predecessorOwner, successorOwner, oldRevision, initialLeaseRevision, view, observer, leaseAbort };
}

// This first test is FULL runtime bootstrap/receiver coverage, not a hand-built receiver:
// FabricRuntimeState.initialize -> ParticipantDirectory.start -> final refreshPresence ->
// control.start -> AgentsProvider.acceptControl -> Main.deliverAgent -> Pi sendMessage.
// AgentCompletionInbox is created by initialize but remote completion followUps enter the
// Main control dispatcher, not AgentCompletionInbox.enqueue. No fake receiver is substituted.
describe.skipIf(!binary || process.platform !== "linux")("#14115 real authenticated NATS succession", () => {
  it("projects this Path B Main's NATS presence and lease into the local store", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-local-projection-"));
    const sessionId = `projection-${createHash("sha256").update(String(Date.now())).digest("hex").slice(0, 12)}`;
    const id = `session:${sessionId}`;
    const config = configFor(root, "machine-a");
    const owner = main(root, sessionId, "presence-projection");
    const localKey = `topology/participants/${createHash("sha256").update(id).digest("hex")}`;
    try {
      await owner.runtime.initialize(owner.context, config);
      const projectRoot = process.env.PI_FABRIC_PROJECT_ROOT ?? owner.context.cwd;
      const meshRoot = process.env.PI_FABRIC_MESH_ROOT ?? path.resolve(projectRoot,
        owner.runtime.config.mesh.root ?? path.join(".pi", "fabric", "mesh"));
      await vi.waitFor(() => expect(readParticipantFile(meshRoot, localKey)).toMatchObject({
        key: localKey, updatedBy: { id }, value: { id, rootId: id, kind: "root", ownerIdentityId: id },
      }), { timeout: 5_000 });
      await vi.waitFor(() => expect(readHostLease(meshRoot, id)).toMatchObject({
        id, rootId: id, identityId: id, incarnationToken: expect.any(String),
      }), { timeout: 5_000 });
    } finally {
      try { await owner.runtime.shutdown("exit"); }
      finally { fs.rmSync(root, { recursive: true, force: true }); }
    }
  }, 20_000);

  it("stops and resumes the same native Main inside the presence TTL, then receives a real completion followUp within seconds", async () => {
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-main-resume-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent")); vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    const sessionId = "14115aaa-bbbb-4000-8000-000000000001", id = `session:${sessionId}`;
    const ownerConfig = configFor(root, "machine-a"), senderConfig = configFor(root, "machine-b");
    const first = main(root, sessionId, "presence-owner");
    const resumed = main(root, sessionId, "presence-owner");
    const resumedB = main(root, sessionId, "presence-owner");
    const liveDuplicate = main(root, sessionId, "presence-owner");
    const sender = main(root, "14115bbb-bbbb-4000-8000-000000000002", "presence-sender");
    let inspector: FabricTransport | undefined;
    try {
      // The real public factory authenticates with scoped agent creds, verifies USER.INFO,
      // opens fixed-policy buckets and provisions only this host's inbox consumer.
      inspector = await openFabricTransport({ config: ownerConfig.mesh, hostId: "presence-inspector",
        mesh: new MeshStore(path.join(root, "inspector"), 65_536, 1_000) });
      await first.runtime.initialize(first.context, ownerConfig);
      await sender.runtime.initialize(sender.context, senderConfig);
      await vi.waitFor(() => expect(sender.runtime.participantInfos().find(row => row.id === id)).toMatchObject({ stale: false }), { timeout: 3_000 });
      const fresh = await inspector.kv("presence").get<FabricParticipantRecord>(key("machine-a", "p", id));
      expect(fresh?.value).toMatchObject({ id, rootId: id, sessionId, ownerIdentityId: id, kind: "root" });
      // Fresh-create and receiver canary before reproducing the stopped/resumed path.
      await expect(sender.invoke("agents.followUp", { id, message: "FRESH_MAIN_RECEIVER_CANARY" }))
        .resolves.toMatchObject({ acknowledged: true });
      await vi.waitFor(() => expect(JSON.stringify(first.messages)).toContain("FRESH_MAIN_RECEIVER_CANARY"), { timeout: 3_000 });
      const beforeDuplicate = await inspector.kv("presence").get(key("machine-a", "p", id));
      await expect(liveDuplicate.runtime.initialize(liveDuplicate.context, ownerConfig)).rejects.toBeDefined();
      expect(await inspector.kv("presence").get(key("machine-a", "p", id))).toEqual(beforeDuplicate);
      expect(liveDuplicate.messages).toEqual([]);
      await liveDuplicate.runtime.shutdown("exit");
      expect(await inspector.kv("presence").get(key("machine-a", "p", id))).toEqual(beforeDuplicate);
      await first.runtime.shutdown("exit");
      const retained = await inspector.kv("presence").get<FabricParticipantRecord>(key("machine-a", "p", id));
      expect(retained, "record-only cleanup must leave the predecessor presence in the real broker").toBeDefined();
      expect(retained!.value.ownerIncarnation).toBe(fresh!.value.ownerIncarnation);
      expect(deletes.some(item => item.key === key("machine-a", "p", id))).toBe(true);
      // Normal path is broker expiry, not polling or a delete. One bounded safety wait
      // after the stopped lease's last possible renewal lets the 15s lease TTL elapse.
      await new Promise(resolve => setTimeout(resolve, 15_100));
      expect(await inspector.kv("leases").get(key("machine-a", "h", id))).toBeUndefined();
      expect(await inspector.kv("presence").get(key("machine-a", "p", id))).toEqual(retained);
      const started = performance.now();
      const candidates = [resumed, resumedB];
      const activations = await Promise.allSettled(candidates.map(candidate => candidate.runtime.initialize(candidate.context, ownerConfig)));
      expect(activations.filter(result => result.status === "fulfilled"), JSON.stringify(activations)).toHaveLength(1);
      expect(activations.filter(result => result.status === "rejected")).toHaveLength(1);
      const winnerIndex = activations.findIndex(result => result.status === "fulfilled");
      const winner = candidates[winnerIndex]!, loser = candidates[1 - winnerIndex]!;
      expect(winner.runtime.initialized).toBe(true);
      const successor = await inspector.kv("presence").get<FabricParticipantRecord>(key("machine-a", "p", id));
      expect(successor!.revision).toBeGreaterThan(retained!.revision);
      expect(successor!.value.ownerIncarnation).not.toBe(retained!.value.ownerIncarnation);
      expect(successor!.value).toMatchObject({ id, rootId: id, ownerIdentityId: id, sessionId, kind: "root" });
      await vi.waitFor(() => expect(sender.runtime.participantInfos().find(row => row.id === id))
        .toMatchObject({ stale: false, ownerIncarnation: successor!.value.ownerIncarnation }), { timeout: 3_000 });
      const receipt = await sender.invoke("agents.followUp", { id, message: "REAL_CHILD_COMPLETION_AFTER_RESUME",
        data: { childId: "completed-child-fixture", status: "completed" } });
      expect(receipt).toMatchObject({ acknowledged: true });
      await vi.waitFor(() => expect(JSON.stringify(winner.messages)).toContain("REAL_CHILD_COMPLETION_AFTER_RESUME"), { timeout: 3_000 });
      expect(winner.messages).toEqual(expect.arrayContaining([expect.objectContaining({ options: expect.objectContaining({ deliverAs: "followUp", triggerTurn: true }) })]));
      expect(loser.messages).toEqual([]);
      expect(JSON.stringify(first.messages)).not.toContain("REAL_CHILD_COMPLETION_AFTER_RESUME");
      expect(performance.now() - started).toBeLessThan(8_000);
    } finally {
      await Promise.allSettled([sender.runtime.shutdown("exit"), resumed.runtime.shutdown("exit"), resumedB.runtime.shutdown("exit"),
        liveDuplicate.runtime.shutdown("exit"), first.runtime.shutdown("exit")]);
      await inspector?.close(); vi.unstubAllEnvs();
    }
  }, 60_000);

  it("moves a live same-lineage durable actor row to the resident owner by broker CAS", async () => {
    const f = await successionFixture("resident-actor");
    const actorId = createHash("sha256").update(f.id).digest("hex").slice(0, 32);
    const resident = residentHostId(f.id), residentIncarnation = `resident-${f.id.slice(-12)}`;
    const actor: FabricParticipantRecord = { ...f.predecessor, id: actorId, kind: "actor", rootId: f.id, residency: "durable",
      ownerHostId: f.id, ownerIdentityId: f.id, ownerIncarnation: f.initialLease.incarnation, parentId: f.id };
    const actorKey = key("machine-a", "p", actorId);
    const oldRevision = await f.presence.put(actorKey, `${f.id}#${actor.ownerIncarnation}`, actor, 0);
    const residentOwner = `${resident}#${residentIncarnation}`;
    const residentLease: FabricKvHostLease = { id: resident, identityId: resident, rootId: f.id, incarnation: residentIncarnation,
      pid: process.pid, pidStartTime: "12345", bootId: "resident-actor-fixture", updatedAt: Date.now(), expiresAt: Date.now() + 15_000 };
    await f.leases.put(key("machine-a", "h", resident), residentOwner, residentLease, 0);
    const residentTransport: FabricTransport = { kind: f.transport.kind,
      self: { machine: f.transport.self.machine, hostId: resident },
      kv: f.transport.kv.bind(f.transport), send: f.transport.send.bind(f.transport), inbox: f.transport.inbox.bind(f.transport),
      drainState: f.transport.drainState.bind(f.transport), close: f.transport.close.bind(f.transport) };
    const residentView = new KvParticipantDirectory({ transport: residentTransport,
      lease: { live: () => true, signal: new AbortController().signal }, identityId: resident, rootId: f.id,
      ownerIncarnation: residentIncarnation, operationTimeoutMs: 30_000 });
    residentView.start();
    try {
      await directoryReady(residentView, actorId);
      const successor = { ...actor, ownerHostId: resident, ownerIdentityId: resident, ownerIncarnation: residentIncarnation,
        name: "dev-supervisor", updatedAt: Date.now() };
      const publication = await residentView.publish(successor);
      expect(publication.revision).toBeGreaterThan(oldRevision);
      expect(await f.presence.get<FabricParticipantRecord>(actorKey)).toMatchObject({
        key: actorKey, owner: residentOwner, revision: publication.revision, value: successor,
      });
      await routeEventually(f.view, actorId, route => route.live && route.ownerHostId === resident);
      // Main's existing actor source is listOwned(), whose owner check sees the
      // resident host after this directory watch update; the resident-specific view agrees.
      expect(isOwnResidentActor(f.view, actorId, f.id)).toBe(true);
      expect(deletes.filter(item => item.key === actorKey)).toEqual([]);
    } finally {
      await residentView.close();
      f.leaseAbort.abort();
      await Promise.allSettled([f.view.close(), f.observer.close()]);
      await f.transport.close();
    }
  }, 25_000);

  it("real broker CAS admits exactly one concurrent own-root resumer and leaves the winner live", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-broker-cas-"));
    const config = configFor(root, "machine-a");
    const id = "session:broker-cas-resume", incarnation = "current-host-lease";
    const transport = await openFabricTransport({ config: config.mesh, hostId: id, mesh: new MeshStore(path.join(root, "mesh"), 65_536, 1_000) });
    const gate = { live: () => true, signal: new AbortController().signal };
    const views = [0, 1].map(() => new KvParticipantDirectory({ transport, lease: gate, identityId: id, rootId: id, ownerIncarnation: incarnation }));
    try {
      const presence = transport.kv("presence"), leases = transport.kv("leases");
      const predecessor: FabricParticipantRecord = { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
        ownerIncarnation: "old-host-lease", sessionId: "broker-cas-resume", name: "old", status: "idle", runner: "pi", transport: "host",
        capabilities: ["followUp"], controlProtocol: "v1", startedAt: 1_000, updatedAt: 1_100 };
      const oldRevision = await presence.put(key("machine-a", "p", id), `${id}#old-host-lease`, predecessor, 0);
      const lease: FabricKvHostLease = { id, identityId: id, rootId: id, incarnation, pid: process.pid,
        pidStartTime: "12345", bootId: "cas-proof", updatedAt: Date.now(), expiresAt: Date.now() + 15_000 };
      await leases.put(key("machine-a", "h", id), `${id}#${incarnation}`, lease, 0);
      views.forEach(view => view.start());
      const records = ["resume-a", "resume-b"].map(name => ({ ...predecessor, ownerIncarnation: incarnation, name, startedAt: 2_000, updatedAt: 2_100 }));
      // Hold the two actual broker read responses, not CAS or publication: both
      // callers must compete using the same stale revision. The broker picks the winner.
      let reads = 0, release!: () => void;
      const readBarrier = new Promise<void>(resolve => { release = resolve; });
      const originalGet = presence.get.bind(presence);
      vi.spyOn(presence, "get").mockImplementation(async <V>(entryKey: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> => {
        const entry = await originalGet<V>(entryKey, signal);
        if (entryKey === key("machine-a", "p", id) && reads < 2) {
          if (++reads === 2) release();
          await readBarrier;
        }
        return entry;
      });
      const results = await Promise.allSettled(views.map((view, index) => view.publish(records[index]!)));
      expect(results.filter(result => result.status === "fulfilled"), JSON.stringify(results)).toHaveLength(1);
      expect(results.filter(result => result.status === "rejected")).toHaveLength(1);
      expect(results.find(result => result.status === "rejected")).toMatchObject({ status: "rejected", reason: { code: "FABRIC_KV_CONFLICT" } });
      const winner = results.findIndex(result => result.status === "fulfilled");
      const stored = await presence.get<FabricParticipantRecord>(key("machine-a", "p", id));
      expect(stored!.revision).toBeGreaterThan(oldRevision);
      expect(stored!.value).toEqual(records[winner]);
      expect(await routeReady(views[winner]!, id)).toMatchObject({ live: true, ownerIncarnation: incarnation });
    } finally { await Promise.all(views.map(view => view.close())); await transport.close(); }
  }, 15_000);

  it("rechecks a replaced lease after the succession CAS and restores the predecessor without deleting", async () => {
    const f = await successionFixture("replacement");
    const replacementIncarnation = "rightful-replacement";
    const replacementLease = hostLease(f.id, replacementIncarnation, Date.now());
    const replacementOwner = `${f.id}#${replacementIncarnation}`;
    let raceSeen = false, initialReadSeen = false, replacementLeaseRevision = 0;
    const hooks = installKvHooks(async call => {
      if (call.key !== f.pkey || call.ifRevision <= 0 || raceSeen) return;
      raceSeen = true;
      initialReadSeen = hooks.gets.some(read => read.key === f.hkey && read.entry?.revision === f.initialLeaseRevision);
      replacementLeaseRevision = await hooks.rawPut.call(hooks.gets.find(read => read.key === f.hkey)!.store, f.hkey, replacementOwner, replacementLease, f.initialLeaseRevision);
    });
    try {
      await expect(f.view.publish(f.successor)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
      expect(raceSeen).toBe(true);
      expect(initialReadSeen, "the CAS hook must run after the direct initial lease read").toBe(true);
      expect(replacementLeaseRevision).toBeGreaterThan(f.initialLeaseRevision);
      const stored = await f.presence.get<FabricParticipantRecord>(f.pkey);
      expect(stored).toMatchObject({ key: f.pkey, owner: f.predecessorOwner, value: f.predecessor });
      expect(stored!.revision).toBeGreaterThan(f.oldRevision);
      const presencePuts = hooks.puts.filter(call => call.key === f.pkey && call.ifRevision > 0);
      expect(presencePuts).toHaveLength(2);
      expect(presencePuts[0]).toMatchObject({ owner: f.successorOwner, ifRevision: f.oldRevision, value: f.successor });
      expect(presencePuts[1]).toMatchObject({ owner: f.predecessorOwner, value: f.predecessor });
      expect(presencePuts[1]!.ifRevision).toBeGreaterThan(f.oldRevision);
      expect(deletes.filter(item => item.key === f.pkey)).toEqual([]);
      expect(await routeEventually(f.observer, f.id, route => !route.live && route.ownerIncarnation === f.predecessor.ownerIncarnation))
        .toMatchObject({ live: false, ownerIncarnation: f.predecessor.ownerIncarnation });
    } finally {
      f.leaseAbort.abort();
      await Promise.allSettled([f.view.close(), f.observer.close()]);
      await f.transport.close();
    }
  }, 25_000);

  it("waits for the real 15-second lease expiry, then undoes the fenced succession CAS", async () => {
    const f = await successionFixture("expiry");
    const expiry = armLeaseGone(f.leases, f.hkey);
    await expiry.ready;
    let raceSeen = false, initialReadSeen = false;
    const hooks = installKvHooks(async call => {
      if (call.key !== f.pkey || call.ifRevision <= 0 || raceSeen) return;
      raceSeen = true;
      initialReadSeen = hooks.gets.some(read => read.key === f.hkey && read.entry?.revision === f.initialLeaseRevision);
      await expiry.gone;
    });
    try {
      await expect(f.view.publish(f.successor)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
      expect(raceSeen).toBe(true);
      expect(initialReadSeen).toBe(true);
      expect(await f.leases.get(f.hkey)).toBeUndefined();
      expect(hooks.gets.some(read => read.key === f.hkey && read.entry === undefined),
        "the post-CAS lease read must observe broker expiry").toBe(true);
      const stored = await f.presence.get<FabricParticipantRecord>(f.pkey);
      expect(stored).toMatchObject({ key: f.pkey, owner: f.predecessorOwner, value: f.predecessor });
      const presencePuts = hooks.puts.filter(call => call.key === f.pkey && call.ifRevision > 0);
      expect(presencePuts).toHaveLength(2);
      expect(presencePuts[0]).toMatchObject({ owner: f.successorOwner, ifRevision: f.oldRevision, value: f.successor });
      expect(presencePuts[1]).toMatchObject({ owner: f.predecessorOwner, value: f.predecessor });
      expect(deletes.filter(item => item.key === f.pkey)).toEqual([]);
      expect(await routeEventually(f.observer, f.id, route => !route.live && route.ownerIncarnation === f.predecessor.ownerIncarnation))
        .toMatchObject({ live: false, ownerIncarnation: f.predecessor.ownerIncarnation });
    } finally {
      f.leaseAbort.abort();
      await Promise.allSettled([f.view.close(), f.observer.close()]);
      await f.transport.close();
    }
  }, 40_000);

  it("leaves a rightful new lease holder untouched when the predecessor undo loses its presence CAS", async () => {
    const f = await successionFixture("undo-loss");
    const rightfulIncarnation = "rightful-cas-winner";
    const rightfulOwner = `${f.id}#${rightfulIncarnation}`;
    const rightfulLease = hostLease(f.id, rightfulIncarnation, Date.now());
    const rightfulRecord = participant(f.id, rightfulIncarnation, "rightful-winner", Date.now());
    let positiveSeen = false, undoRaceSeen = false, initialReadSeen = false;
    let rightfulPresenceRevision = 0, replacementLeaseRevision = 0;
    const hooks = installKvHooks(async call => {
      if (call.key !== f.pkey || call.ifRevision <= 0) return;
      if (!positiveSeen) {
        positiveSeen = true;
        initialReadSeen = hooks.gets.some(read => read.key === f.hkey && read.entry?.revision === f.initialLeaseRevision);
        replacementLeaseRevision = await hooks.rawPut.call(hooks.gets.find(read => read.key === f.hkey)!.store, f.hkey, rightfulOwner, rightfulLease, f.initialLeaseRevision);
        return;
      }
      if (!undoRaceSeen && call.owner === f.predecessorOwner) {
        undoRaceSeen = true;
        rightfulPresenceRevision = await hooks.rawPut.call(call.store, f.pkey, rightfulOwner, rightfulRecord, call.ifRevision);
      }
    });
    try {
      await expect(f.view.publish(f.successor)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
      expect(positiveSeen).toBe(true);
      expect(undoRaceSeen).toBe(true);
      expect(initialReadSeen).toBe(true);
      expect(replacementLeaseRevision).toBeGreaterThan(f.initialLeaseRevision);
      expect(rightfulPresenceRevision).toBeGreaterThan(f.oldRevision);
      const stored = await f.presence.get<FabricParticipantRecord>(f.pkey);
      expect(stored).toMatchObject({ key: f.pkey, owner: rightfulOwner, value: rightfulRecord });
      expect(stored!.revision).toBe(rightfulPresenceRevision);
      const presencePuts = hooks.puts.filter(call => call.key === f.pkey && call.ifRevision > 0);
      expect(presencePuts).toHaveLength(2);
      expect(presencePuts[0]).toMatchObject({ owner: f.successorOwner, ifRevision: f.oldRevision, value: f.successor });
      expect(presencePuts[1]).toMatchObject({ owner: f.predecessorOwner, value: f.predecessor });
      expect(deletes.filter(item => item.key === f.pkey)).toEqual([]);
      expect(await routeEventually(f.observer, f.id, route => route.live && route.ownerIncarnation === rightfulIncarnation))
        .toMatchObject({ live: true, ownerIncarnation: rightfulIncarnation });
    } finally {
      f.leaseAbort.abort();
      await Promise.allSettled([f.view.close(), f.observer.close()]);
      await f.transport.close();
    }
  }, 25_000);

  it("refuses a same-owner lease renewal because the direct lease revision changed", async () => {
    const f = await successionFixture("renewal");
    // Identical JSON and owner isolate the revision fence from all field checks:
    // a real broker rewrite still renews its TTL and must invalidate this attempt.
    const renewedLease = { ...f.initialLease };
    let raceSeen = false, initialReadSeen = false, renewedLeaseRevision = 0;
    const hooks = installKvHooks(async call => {
      if (call.key !== f.pkey || call.ifRevision <= 0 || raceSeen) return;
      raceSeen = true;
      initialReadSeen = hooks.gets.some(read => read.key === f.hkey && read.entry?.revision === f.initialLeaseRevision);
      renewedLeaseRevision = await hooks.rawPut.call(hooks.gets.find(read => read.key === f.hkey)!.store, f.hkey, f.successorOwner, renewedLease, f.initialLeaseRevision);
    });
    try {
      await expect(f.view.publish(f.successor)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
      expect(raceSeen).toBe(true);
      expect(initialReadSeen).toBe(true);
      expect(renewedLeaseRevision).toBeGreaterThan(f.initialLeaseRevision);
      const stored = await f.presence.get<FabricParticipantRecord>(f.pkey);
      expect(stored).toMatchObject({ key: f.pkey, owner: f.predecessorOwner, value: f.predecessor });
      const presencePuts = hooks.puts.filter(call => call.key === f.pkey && call.ifRevision > 0);
      expect(presencePuts).toHaveLength(2);
      expect(presencePuts[0]).toMatchObject({ owner: f.successorOwner, ifRevision: f.oldRevision, value: f.successor });
      expect(presencePuts[1]).toMatchObject({ owner: f.predecessorOwner, value: f.predecessor });
      expect(deletes.filter(item => item.key === f.pkey)).toEqual([]);
      expect(await routeEventually(f.observer, f.id, route => !route.live && route.ownerIncarnation === f.predecessor.ownerIncarnation))
        .toMatchObject({ live: false, ownerIncarnation: f.predecessor.ownerIncarnation });
    } finally {
      f.leaseAbort.abort();
      await Promise.allSettled([f.view.close(), f.observer.close()]);
      await f.transport.close();
    }
  }, 25_000);

  it("routes only a matching presence and lease pair after watches catch up", async () => {
    const f = await successionFixture("reader");
    try {
      const stale = await routeEventually(f.observer, f.id, route => !route.live && route.ownerIncarnation === f.predecessor.ownerIncarnation);
      expect(stale).toMatchObject({ live: false, ownerIncarnation: f.predecessor.ownerIncarnation });
      const revision = await f.presence.put(f.pkey, f.successorOwner, f.successor, f.oldRevision);
      expect(revision).toBeGreaterThan(f.oldRevision);
      const live = await routeEventually(f.observer, f.id, route => route.live && route.ownerIncarnation === f.successor.ownerIncarnation);
      expect(live).toMatchObject({ live: true, ownerHostId: f.id, ownerIdentityId: f.id, ownerIncarnation: f.successor.ownerIncarnation });
      const rightfulIncarnation = "reader-rightful-holder", rightfulOwner = `${f.id}#${rightfulIncarnation}`;
      await f.leases.put(f.hkey, rightfulOwner, hostLease(f.id, rightfulIncarnation, Date.now()), f.initialLeaseRevision);
      // This transition is driven by the lease watch, not a delay or presence write:
      // the previously live takeover row becomes non-live once that watch catches up.
      expect(await routeEventually(f.observer, f.id, route => !route.live && route.ownerIncarnation === f.successor.ownerIncarnation))
        .toMatchObject({ live: false, ownerIncarnation: f.successor.ownerIncarnation });
      const rightfulRecord = participant(f.id, rightfulIncarnation, "reader-rightful", Date.now());
      await f.presence.put(f.pkey, rightfulOwner, rightfulRecord, revision);
      expect(await routeEventually(f.observer, f.id, route => route.live && route.ownerIncarnation === rightfulIncarnation))
        .toMatchObject({ live: true, ownerIncarnation: rightfulIncarnation });
      expect(deletes.filter(item => item.key === f.pkey)).toEqual([]);
    } finally {
      f.leaseAbort.abort();
      await Promise.allSettled([f.view.close(), f.observer.close()]);
      await f.transport.close();
    }
  }, 15_000);
});
