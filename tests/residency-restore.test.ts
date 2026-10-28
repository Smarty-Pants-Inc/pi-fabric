// expected-skip: no NATS_BIN/PATH executable or non-Linux: native residency recovery needs Linux/nats-server.
// Keep reason: L8(d), smarty-dev#16188: exact restored bytes must recover the original actor, not a replacement.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { fauxProvider } from "@earendil-works/pi-ai";
import { ModelRegistry, ModelRuntime, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { openFabricTransport } from "../src/transport/index.js";
import type { FabricTransport } from "../src/transport/types.js";
import { leaseValue, presenceValue } from "../src/topology/participant-view.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { mainGenerationPath, type ResidentMainGeneration } from "../src/residency/handover.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { residentRoot, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { FabricActorInfo, FabricActorMessage } from "../src/actors/types.js";
import { createPathBHarness, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { launchLog, same, stopAllOwned } from "./helpers/owned-processes.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("L8 restore requires NATS_BIN");
const repository = path.resolve(import.meta.dirname, "..");
const sessionId = "16188aaa-bbbb-4000-8000-000000000008";
const rootId = `session:${sessionId}`;
const ack = "L8_ORIGINAL_ACTOR_ACK";
const read = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const manifest = (root: string): Record<string, string> => {
  const files: Record<string, string> = {};
  const walk = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name), stat = fs.lstatSync(file);
      if (stat.isDirectory()) walk(file);
      else if (stat.isFile()) files[path.relative(root, file)] = `${stat.mode & 0o777}:${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}`;
      else throw new Error(`Unexpected checkpoint entry: ${file}`);
    }
  };
  walk(root); return files;
};

async function main(root: string, config: FabricConfig) {
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "agent/auth.json") });
  const faux = fauxProvider({ provider: "l8-restore-faux", models: [{ id: "one-turn" }] });
  modelRuntime.registerNativeProvider(faux.provider);
  await modelRuntime.getAvailable("l8-restore-faux");
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => "l8-restored-main", appendEntry() {}, sendMessage() {} } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false, model: faux.getModel(), modelRegistry: new ModelRegistry(modelRuntime),
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} } } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(repository, "dist/index.js"), worker: path.join(repository, "dist/worker.js"),
    residentHost: path.join(repository, "dist/residency/launcher.js"), skills: root,
  } });
  const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
    cwd: root, signal: AbortSignal.timeout(45_000), parentToolCallId: "l8-restore", nestedToolCallId: ref, extensionContext: context,
    update() {}, approve: async () => {}, audits: [], maxResultChars: 50_000,
  });
  const initializeStartedAt = performance.now();
  await runtime.initialize(context, config);
  console.log("L8_MAIN_STARTUP_MS", JSON.stringify({ elapsedMs: performance.now() - initializeStartedAt }));
  return { runtime, invoke };
}

describe.skipIf(!binary || process.platform !== "linux")("checkpoint/erase/restore native resident recovery", () => {
  it.each(["dead-owner", "reused-pid"] as const)("republishes and asks the original actor with closed Main generation and %s", async stale => {
    if (!binary) throw new Error("L8 restore requires nats-server");
    expect(fs.existsSync(path.join(repository, "dist/residency/launcher.js")), "build the candidate before native residency tests").toBe(true);
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "l8-residency-restore-")));
    const work = path.join(root, "work"); fs.mkdirSync(work);
    const launches = launchLog(root);
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(work, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", work);
    vi.stubEnv("PI_FABRIC_PI_BINARY", path.join(repository, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    for (const [name, value] of Object.entries(launches.env)) vi.stubEnv(name, value);
    let broker: PathBHarness | undefined;
    let inspector: FabricTransport | undefined;
    let inspectorMesh: MeshStore | undefined;
    let first: Awaited<ReturnType<typeof main>> | undefined;
    let restored: Awaited<ReturnType<typeof main>> | undefined;
    try {
      vi.stubEnv("TMPDIR", root);
      broker = await createPathBHarness({ binary, tls: {} });
      for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
      expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      for (const name of Object.keys(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, undefined);
      const config = normalizeFabricConfig({ fullCodeMode: false,
        mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(work, "mesh"), actorPollMs: 20,
          nats: { ...broker.runtimeOptions(broker.users.machineA), operatorMachine: "fabric-ops" } },
        agents: { enabled: true, transport: "process", model: "l8-restore-faux/one-turn", maxConcurrent: 1, timeoutMs: 30_000, nice: 0 },
        mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
      });
      const agentDir = path.join(work, "agent"); fs.mkdirSync(agentDir, { recursive: true });
      const provider = path.join(work, "faux.ts");
      // Real Pi worker and native model provider. Only inference is deterministic;
      // launcher, host fence, registry, NATS presence, control ACK and history are production.
      fs.writeFileSync(provider, `import { fauxProvider, fauxAssistantMessage } from ${JSON.stringify(path.join(repository, "node_modules/@earendil-works/pi-ai/dist/index.js"))};
export default function(pi) { const faux = fauxProvider({ provider: "l8-restore-faux", models: [{ id: "one-turn" }] });
faux.setResponses([fauxAssistantMessage(${JSON.stringify(ack)})]); pi.registerProvider(faux.provider); }\n`);
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ extensions: [provider], compaction: { enabled: false }, retry: { enabled: false } }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify(config));
      first = await main(work, config);
      const actor = await first.invoke("agents.create", { name: "l8-original", scope: "session", residency: "durable", runner: "pi",
        model: "l8-restore-faux/one-turn", instructions: "Reply ACK", responseMode: "text", coalesce: false, tools: [] }) as FabricActorInfo;
      if (!actor.sessionFile) throw new Error("Original actor must retain a session transcript");
      const sessionFile = actor.sessionFile;
      console.log("L8_PHASE", JSON.stringify({ phase: "created", actorId: actor.id }));
      const before = await first.invoke("agents.ask", { id: actor.id, message: "before checkpoint" }) as FabricActorMessage;
      expect(before).toMatchObject({ actorId: actor.id, direction: "out", text: ack });
      let oldHistory: FabricActorMessage[] = [];
      // The correlated reply can precede durable history publication. Observe
      // that existing commit before checkpointing, not a racing passive cache.
      await vi.waitFor(async () => {
        oldHistory = await first!.invoke("agents.messages", { id: actor.id, limit: 100 }) as FabricActorMessage[];
        expect(oldHistory).toHaveLength(2);
        expect(oldHistory.some(message => message.id === before.id)).toBe(true);
      });
      const meshRoot = config.mesh.root!;
      const resident = residentRoot(meshRoot, rootId);
      const ownerPath = path.join(resident, "owner.json");
      const oldOwner = read<ResidentHostOwner>(ownerPath);
      // Inspect the real authenticated KV rows, not Main's passive projection.
      // This transport never starts a participant directory or claims a host lease.
      inspectorMesh = new MeshStore(path.join(root, "inspector-mesh"), 65_536, 1_000);
      inspector = await openFabricTransport({ config: config.mesh, hostId: "l8-restore-inspector", mesh: inspectorMesh });
      const kvKey = (kind: "p" | "h", id: string) =>
        `${inspector!.self.machine}.${kind}.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;
      const presence = inspector.kv("presence"), leases = inspector.kv("leases");
      const actorKey = kvKey("p", actor.id), leaseKey = kvKey("h", oldOwner.hostId);
      const originalPresence = await presence.get<FabricParticipantRecord>(actorKey);
      const originalLease = await leases.get<FabricKvHostLease>(leaseKey);
      if (!originalPresence || !originalLease) throw new Error("Original resident must publish both NATS presence and lease");
      expect(presenceValue(originalPresence)).toMatchObject({ id: actor.id, kind: "actor", rootId,
        residency: "durable", ownerHostId: oldOwner.hostId, capabilities: expect.arrayContaining(["ask"]) });
      expect(leaseValue(originalLease)).toMatchObject({ id: oldOwner.hostId, rootId,
        identityId: originalPresence.value.ownerIdentityId, incarnation: originalPresence.value.ownerIncarnation,
        pid: oldOwner.pid });
      expect(originalLease.owner).toBe(originalPresence.owner);
      const lockPath = path.join(resident, "host.lock");
      const oldFence = read<{ dev: string; ino: string }>(path.join(resident, "host-fence.json"));
      // Counterexample: a new Main runtime attaches to a genuine live owner.
      // It refreshes its own generation, but must not replace the host/actor.
      await first.runtime.shutdown("exit"); first = undefined;
      first = await main(work, config);
      expect(read<ResidentHostOwner>(ownerPath).token).toBe(oldOwner.token);
      const currentMain = read<ResidentMainGeneration>(mainGenerationPath(resident));
      expect(currentMain.nonce).not.toMatch(/^closed:/);
      expect(await first.invoke("agents.messages", { id: actor.id, limit: 100 })).toEqual(oldHistory);
      expect(read(mainGenerationPath(resident))).toEqual(currentMain);
      expect(read<ResidentHostOwner>(ownerPath).token).toBe(oldOwner.token);
      await first.runtime.shutdown("exit"); first = undefined;
      const closedMain = read<ResidentMainGeneration>(mainGenerationPath(resident));
      expect(closedMain.nonce).toMatch(/^closed:/);
      // The checkpoint deliberately retains stale runtime ownership, exactly like
      // the L8 archive. Stop all fixture-owned native processes before erase.
      const backup = path.join(root, "checkpoint"); fs.cpSync(meshRoot, backup, { recursive: true });
      await stopAllOwned(launches.owned().reverse(), 5_000, 5_000);
      expect(launches.owned().some(same)).toBe(false);
      fs.rmSync(meshRoot, { recursive: true });
      fs.cpSync(backup, meshRoot, { recursive: true });
      expect(manifest(meshRoot)).toEqual(manifest(backup));
      expect(read<ResidentHostOwner>(ownerPath)).toEqual(oldOwner);
      expect(read(mainGenerationPath(resident))).toEqual(closedMain);
      expect(`${fs.statSync(lockPath).dev}:${fs.statSync(lockPath).ino}`).not.toBe(`${oldFence.dev}:${oldFence.ino}`);
      const transcript = fs.readFileSync(sessionFile, "utf8");
      if (stale === "reused-pid") {
        // A currently live PID with a different birth is NOT the old owner.
        // Seed both the restored owner and lock diagnostic, never a real process.
        for (const file of [ownerPath, lockPath]) {
          const previous = read<Record<string, unknown>>(file);
          fs.writeFileSync(file, JSON.stringify({ ...previous, pid: process.pid, processStartTime: `${BigInt(processStartTime(process.pid)!) + 1n}` }));
        }
      }
      // Graceful fixture cleanup can withdraw the old broker rows. Reproduce the
      // CI counterexample deterministically: retain the ORIGINAL incarnation's
      // quiescing presence and matching lease, as if shutdown stopped after its
      // quiesce publication. Keep the dead owner's process identity, not ours.
      // The lease is essential: without it, stopping presence is not routable.
      const stoppingPresence: FabricParticipantRecord = { ...originalPresence.value, status: "stopping", capabilities: [] };
      const remainingPresence = await presence.get(actorKey), remainingLease = await leases.get(leaseKey);
      const stoppingRevision = await presence.put(actorKey, originalPresence.owner, stoppingPresence, remainingPresence?.revision ?? 0);
      const retainedLeaseRevision = await leases.put(leaseKey, originalLease.owner, originalLease.value, remainingLease?.revision ?? 0);
      const retainedPresence = await presence.get<FabricParticipantRecord>(actorKey);
      const retainedLease = await leases.get<FabricKvHostLease>(leaseKey);
      expect(retainedPresence).toMatchObject({ revision: stoppingRevision, owner: originalPresence.owner, value: stoppingPresence });
      expect(retainedLease).toMatchObject({ revision: retainedLeaseRevision, owner: originalLease.owner, value: originalLease.value });
      expect(presenceValue(retainedPresence!)).toEqual(stoppingPresence);
      expect(leaseValue(retainedLease!)).toEqual(originalLease.value);
      console.log("L8_PHASE", JSON.stringify({ phase: "restored-files", actorId: actor.id, stale, oldOwner: oldOwner.pid,
        closedMain: closedMain.nonce, retainedStatus: stoppingPresence.status, retainedCapabilities: stoppingPresence.capabilities,
        retainedIncarnation: stoppingPresence.ownerIncarnation, stoppingRevision, retainedLeaseRevision }));
      restored = await main(work, config);
      // Startup recovery must publish before the ordinary ask can resolve its
      // original NATS actor route. No create/import/register/ensureActor repair.
      const after = await restored.invoke("agents.ask", { id: actor.id, message: "after restore" }) as FabricActorMessage;
      expect(after).toMatchObject({ actorId: actor.id, direction: "out", text: ack });
      let history: FabricActorMessage[] = [];
      await vi.waitFor(async () => {
        history = await restored!.invoke("agents.messages", { id: actor.id, limit: 100 }) as FabricActorMessage[];
        expect(history).toHaveLength(oldHistory.length + 2);
      });
      expect(history.slice(0, oldHistory.length)).toEqual(oldHistory);
      // Direct asks are persisted as their #enqueue payload in FabricActorMessage.data, not text.
      expect(history.slice(oldHistory.length)).toMatchObject([
        { actorId: actor.id, direction: "in", source: "direct", data: { message: "after restore" } },
        { id: after.id, actorId: actor.id, direction: "out", text: ack },
      ]);
      expect(fs.readFileSync(sessionFile, "utf8").startsWith(transcript)).toBe(true);
      expect(restored.runtime.participantInfos().find(row => row.id === actor.id)).toMatchObject({ id: actor.id, rootId, ownerHostId: oldOwner.hostId });
      const newOwner = read<ResidentHostOwner>(ownerPath);
      expect(newOwner.token).not.toBe(oldOwner.token);
      expect(read<ResidentMainGeneration>(mainGenerationPath(resident))).toMatchObject({ pid: process.pid, processStartTime: processStartTime(process.pid), rootId, sessionId });
      expect(read<ResidentMainGeneration>(mainGenerationPath(resident)).nonce).not.toBe(closedMain.nonce);
      const registry = read<{ actors: Array<{ id: string }> }>(path.join(meshRoot, "actors", sessionId, "actors.json"));
      expect(registry.actors.map(row => row.id)).toEqual([actor.id]);
      console.log("L8_RESTORE_ACK", JSON.stringify({ stale, actorId: actor.id, historyBefore: oldHistory.length, historyAfter: history.length, oldOwner: oldOwner.pid, newOwner: newOwner.pid }));
    } catch (error) {
      const resident = residentRoot(path.join(work, "mesh"), rootId);
      for (const name of ["launcher.log", "child-stderr.log", "handover-deferred.json", ...(
        fs.existsSync(path.join(resident, "generations")) ? fs.readdirSync(path.join(resident, "generations"))
          .filter(name => name.endsWith(".error.json")).map(name => `generations/${name}`) : [])]) {
        const file = path.join(resident, name);
        if (fs.existsSync(file)) console.log("L8_DIAGNOSTIC", name, fs.readFileSync(file, "utf8").slice(-12_000));
      }
      throw error;
    } finally {
      try { await first?.runtime.shutdown("exit"); await restored?.runtime.shutdown("exit"); }
      finally {
        try { await stopAllOwned(launches.owned().reverse(), 5_000, 5_000); }
        finally {
          try { await inspector?.close(); }
          finally {
            inspectorMesh?.closeState();
            await broker?.stop(); vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
          }
        }
      }
    }
  }, 180_000);
});
