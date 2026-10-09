// Isolated seeded PR #740 / smarty-dev#7791 before/after harness.
// See registry-dashboard.md for same-base commands and measurement limitations.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { isolateTestFleetEnvironment } from "../scripts/test-temp.ts";
isolateTestFleetEnvironment();
const { ActorManager } = await import("../src/actors/manager.ts");
const { ActorRegistryStore } = await import("../src/actors/registry-store.ts");
const { AgentManager } = await import("../src/agents/manager.ts");
const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.ts");
const { MeshStore } = await import("../src/mesh/store.ts");
const { FabricUiController } = await import("../src/ui/controller.ts");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "registry-dashboard-bench-"));
const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
  workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
});
const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
const managers: InstanceType<typeof ActorManager>[] = [];
const stores: InstanceType<typeof ActorRegistryStore>[] = [];
const seeds: Record<string, unknown>[][] = [];
const sizes: number[] = [];
let parses = 0, readCalls = 0, readCpuUs = 0, uiSnapshotReads = 0;
const parse = JSON.parse;
JSON.parse = ((text: string, ...args: unknown[]) => {
  if (typeof text === "string" && text.slice(0, 120).includes('"format"') && text.slice(0, 120).includes('"actors"')) parses++;
  return Reflect.apply(parse, JSON, [text, ...args]);
}) as typeof JSON.parse;
const read = ActorRegistryStore.prototype.read;
ActorRegistryStore.prototype.read = function () {
  readCalls++;
  const cpu = process.cpuUsage();
  try { return read.call(this); }
  finally { const used = process.cpuUsage(cpu); readCpuUs += used.user + used.system; }
};
let controller: InstanceType<typeof FabricUiController> | undefined;
try {
  for (let i = 0; i < 35; i++) {
    const actorRoot = path.join(root, `registry-${i}`);
    fs.mkdirSync(actorRoot, { recursive: true });
    const bytes = i === 0 ? 1_800_000 : 244_118;
    const records = Array.from({ length: 8 }, (_, j) => ({
      id: (i * 8 + j + 1).toString(16).padStart(32, "0"), name: `actor-${i}-${j}`,
      rootId: `session:bench-${i}`, instructions: "x".repeat(Math.floor(bytes / 8) - 800),
      status: "stopped", events: [], topics: [], residency: "session", delivery: "mailbox",
      runner: "pi", responseMode: "text", requirements: [], messages: [], createdAt: Date.now(), updatedAt: Date.now(),
    }));
    let serialized = JSON.stringify({ format: 1, actors: records });
    records[7]!.instructions += "x".repeat(bytes - Buffer.byteLength(serialized));
    serialized = JSON.stringify({ format: 1, actors: records });
    fs.writeFileSync(path.join(actorRoot, "actors.json"), serialized);
    sizes.push(Buffer.byteLength(serialized));
    const identity = { id: `session:bench-${i}`, name: "main", kind: "main" as const, sessionId: `bench-${i}` };
    managers.push(new ActorManager(`bench-${i}`, identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600_000 }, agents, () => {}, {
        actorRoot, persistent: true, rootId: identity.id, claimResidency: "session", closeGraceMs: 0,
        canConsumeMesh: () => false, reapDeadSessionPresence: false, meshRetentionSweepPath: false,
        presencePublisher: { refresh: async () => {}, schedule: () => {} },
      }));
    stores.push(new ActorRegistryStore(actorRoot)); seeds.push(records);
  }
  const noopSubscribe = () => () => {};
  const state = {
    initialized: true, widgetDismissedAt: 0,
    config: { ui: { ...DEFAULT_FABRIC_CONFIG.ui, refreshMs: 500, widget: "hidden" }, mesh: { enabled: false } },
    activity: { revision: () => 0, runs: () => [], subscribe: noopSubscribe },
    agents: { list: () => [], subscribeUi: noopSubscribe },
    actors: { list: () => managers.flatMap(manager => manager.list()), subscribe: noopSubscribe,
      instructions: (id: string) => seeds[Math.floor((parseInt(id, 16) - 1) / 8)]![(parseInt(id, 16) - 1) % 8]!.instructions,
      messages: () => [],
    },
    globalActors: { list: () => [] }, mesh,
    mainAgentInfo: () => { uiSnapshotReads++; return { id: "main", name: "Main", kind: "main", status: "running",
      runner: "pi", transport: "host", cwd: root, startedAt: 1, updatedAt: 1, local: true, pendingMessages: false }; },
  };
  const warnings: string[] = [];
  controller = new FabricUiController(state as never);
  controller.start({ mode: "tui", ui: { setWidget: () => {}, notify: (message: string) => warnings.push(message) } } as never);
  if (warnings.length) throw new Error(warnings.join("\n"));
  const startupParses = parses;
  parses = 0; readCalls = 0; readCpuUs = 0; uiSnapshotReads = 0;
  const durationMs = Number(process.argv[2] ?? 60_000);
  if (!Number.isFinite(durationMs) || durationMs < 1_000) throw new Error("duration must be finite and at least 1000 ms");
  const targetRounds = Math.ceil(durationMs / 250);
  const started = performance.now(), cpu = process.cpuUsage();
  let rounds = 0, writes = 0, sink = 0;
  while (rounds < targetRounds) {
    const elapsed = performance.now() - started;
    if (elapsed >= (writes + 1) * 10_000) {
      const index = writes % stores.length;
      seeds[index]![0]!.updatedAt = Date.now();
      await stores[index]!.withLock(() => stores[index]!.write(seeds[index]!));
      writes++;
    }
    for (const manager of managers) sink += manager.list().length;
    rounds++;
    const next = Math.min(durationMs, rounds * 250);
    const wait = Math.max(0, next - (performance.now() - started));
    if (wait) await new Promise(resolve => setTimeout(resolve, wait));
  }
  const wallMs = performance.now() - started, used = process.cpuUsage(cpu);
  console.log(JSON.stringify({ node: process.version, registries: sizes.length, actors: 280,
    totalBytes: sizes.reduce((a, b) => a + b, 0), largestBytes: Math.max(...sizes), durationMs, wallMs,
    rounds, writes, startupRegistryParses: startupParses, registryParses: parses, registryReadCalls: readCalls,
    registryReadCpuMs: readCpuUs / 1000, registryReadCores: readCpuUs / (wallMs * 1000),
    cpuUserMs: used.user / 1000, cpuSystemMs: used.system / 1000,
    cpuCores: (used.user + used.system) / (wallMs * 1000), hiddenUiSnapshotReads: uiSnapshotReads, sink }, null, 2));
} finally {
  controller?.stop();
  JSON.parse = parse; ActorRegistryStore.prototype.read = read;
  await Promise.all(managers.map(manager => manager.close()));
  await agents.close();
  fs.rmSync(root, { recursive: true, force: true });
}
