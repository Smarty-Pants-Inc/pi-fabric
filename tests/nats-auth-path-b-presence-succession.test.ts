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
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport, TransportKvEntry } from "../src/transport/types.js";
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

// This first test is FULL runtime bootstrap/receiver coverage, not a hand-built receiver:
// FabricRuntimeState.initialize -> ParticipantDirectory.start -> final refreshPresence ->
// control.start -> AgentsProvider.acceptControl -> Main.deliverAgent -> Pi sendMessage.
// AgentCompletionInbox is created by initialize but remote completion followUps enter the
// Main control dispatcher, not AgentCompletionInbox.enqueue. No fake receiver is substituted.
describe.skipIf(!binary || process.platform !== "linux")("#14115 real authenticated NATS succession", () => {
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
});
