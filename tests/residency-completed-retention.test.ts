import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { RESIDENT_REQUEST_RETENTION_MS, residentRequestGeneration } from "../src/residency/request-expiry.js";
import { readResidentRequestDecision, residentRoot, residentResultPath, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";

// A native session Main owns the caller binding independently of the resident executor.
const mainParticipants = (config: ResidentHostConfig) => {
  const identity = { id: config.rootId, name: "live Main", kind: "main" as const, sessionId: config.sessionId };
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
  });
  participants.registerSource(() => [{
    format: 1, id: identity.id, rootId: identity.id, kind: "root", name: identity.name, status: "idle",
    ownerHostId: identity.id, ownerIdentityId: identity.id, sessionId: identity.sessionId,
    runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1",
    startedAt: Date.now(), updatedAt: Date.now(),
  }]);
  return participants;
};

const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
  expect(predicate()).toBe(true);
};
const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-completed-retention-"));
  const rootId = "session:completed-retention";
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "completed-retention", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const host = new ResidentHost(config);
  let now: number | undefined;
  let scans = 0;
  const nativeSweep = ResidentRequestRetention.prototype.sweep;
  const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep").mockImplementation(function (this: ResidentRequestRetention, time, live, _budget, gone) {
    nativeSweep.call(this, now ?? time, live, 10_000, gone);
    if (now !== undefined) scans++;
  });
  let due: ReturnType<typeof vi.spyOn> | undefined;
  const participants = mainParticipants(config);
  await participants.start();
  await host.start();
  const client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
  return {
    config, host, client,
    creationIds: (operation: "spawnBound" | "createActor") => fs.readdirSync(path.join(config.residencyRoot, "decisions")).map(name => name.slice(0, -5))
      .filter(id => readResidentRequestDecision(config.residencyRoot, id)?.operation === operation),
    ack: (id: string) => JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "acknowledgements", `${id}.json`), "utf8")),
    retained: (id: string) => fs.existsSync(path.join(config.residencyRoot, "decisions", `${id}.json`)),
    scan: async (time: number, until?: () => boolean) => {
      const before = scans; now = time;
      // Expiry is asynchronous maintenance, not a promise that the first 2-ms
      // tick finishes all reference preparation. Keep the existing wait deadline
      // and assert the actual public collection outcome, without racing a
      // conservative incomplete snapshot or transient targeted-proof timeout.
      host.agents.retentionReferences({ now: time, budgetMs: 100 });
      due ??= vi.spyOn(ResidentRequestRetention.prototype, "due"); due.mockReturnValue(true);
      await waitFor(() => scans > before && (!until || until())); due.mockReturnValue(false);
    },
    close: async () => {
      due?.mockRestore(); sweep.mockRestore();
      await client.close(); await host.close(); await participants.close(); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
};

it.each(["native", "win32"])("completed-but-not-cleaned durable agents release acknowledged capacity debt using saved results, not stale running handles (%s)", async platform => {
  const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
  if (platform === "win32") Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
  const fixture = await setup();
  try {
    const handles = [];
    for (let index = 0; index < 3; index++) {
      const handle = await fixture.client.spawnAgent({ task: `completed task ${index}`, transport: "process", extensions: false }, AbortSignal.timeout(5_000));
      handles.push(handle);
      // The public custody assertion is count/correctness coverage, not a
      // 2-ms filesystem-speed assertion. Deadline retry behavior is separately
      // covered at the production budget in residency-legacy-archive.test.ts.
      await waitFor(() => fs.existsSync(residentResultPath(fixture.config.residencyRoot, handle.id)) && !fixture.host.agents.retentionReferences({ budgetMs: 100 }).has(handle.id));
      expect(fixture.client.statusAgent(handle.id)).toMatchObject({ status: "completed" });
      const metadata = JSON.parse(fs.readFileSync(path.join(fixture.config.residencyRoot, "agents", `${handle.id}.json`), "utf8"));
      expect(metadata.handle.status).toBe("running");
    }
    const ids = fixture.creationIds("spawnBound");
    expect(ids).toHaveLength(3);
    const now = Math.max(...ids.map(id => fixture.ack(id).acknowledgedAt)) + RESIDENT_REQUEST_RETENTION_MS + 1;
    await fixture.scan(now, () => ids.every(id => !fixture.retained(id)));
    expect(ids.filter(fixture.retained)).toEqual([]);
    for (const handle of handles) {
      // Collection releases exchange capacity, NOT result/output ownership.
      expect(fixture.client.hasAgent(handle.id)).toBe(true);
      expect(fixture.client.statusAgent(handle.id)).toMatchObject({ status: "completed" });
      expect(fs.existsSync(residentResultPath(fixture.config.residencyRoot, handle.id))).toBe(true);
    }
  } finally {
    try { await fixture.close(); } finally { Object.defineProperty(process, "platform", nativePlatform); }
  }
}, 30_000);

it.each(["missing", "stub", "running", "wrong id", "wrong transport", "unreadable", "unresolved", "unknown descendant"])("completed spawn keeps uncertain capacity debt: %s", async kind => {
  const fixture = await setup();
  try {
    const handle = await fixture.client.spawnAgent({ task: "completed negative control", transport: "process", extensions: false }, AbortSignal.timeout(5_000));
    const saved = residentResultPath(fixture.config.residencyRoot, handle.id);
    await waitFor(() => fs.existsSync(saved) && !fixture.host.agents.retentionReferences({ budgetMs: 100 }).has(handle.id));
    const original = fs.readFileSync(saved, "utf8");
    const result = JSON.parse(original);
    const run = fixture.host.agents.runDirectory(handle.id)!;
    if (kind === "missing") fs.rmSync(saved);
    else if (kind === "stub") fs.writeFileSync(saved, JSON.stringify({ id: handle.id, status: "completed" }));
    else if (kind === "running") fs.writeFileSync(saved, JSON.stringify({ ...result, status: "running" }));
    else if (kind === "wrong id") fs.writeFileSync(saved, JSON.stringify({ ...result, id: "different" }));
    else if (kind === "wrong transport") fs.writeFileSync(saved, JSON.stringify({ ...result, transport: "tmux" }));
    else if (kind === "unreadable") fs.writeFileSync(saved, "{");
    else if (kind === "unresolved") fs.writeFileSync(path.join(run, "unresolved-worker.json"), "{}");
    else {
      const child = path.join(run, "nested", "unknown"); fs.mkdirSync(child, { recursive: true });
      fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "completed", transport: "process" }));
    }
    const id = fixture.creationIds("spawnBound")[0]!;
    const now = fixture.ack(id).acknowledgedAt + RESIDENT_REQUEST_RETENTION_MS + 1;
    await fixture.scan(now);
    expect(fixture.retained(id)).toBe(true);
    expect(fs.existsSync(path.join(fixture.config.residencyRoot, "acknowledgements", `${id}.json`))).toBe(true);
    fs.writeFileSync(saved, original);
    fs.rmSync(path.join(run, "unresolved-worker.json"), { force: true });
    fs.rmSync(path.join(run, "nested"), { recursive: true, force: true });
    await fixture.scan(now + 60_001, () => !fixture.retained(id));
    expect(fixture.retained(id)).toBe(false);
    expect(fixture.client.statusAgent(handle.id)).toMatchObject({ status: "completed" });
  } finally { await fixture.close(); }
}, 15_000);

it.each(["spawnBound", "createActor"] as const)("later cached %s retries acknowledge their own generation, preserve one entity and collect both exchanges", async operation => {
  const fixture = await setup();
  try {
    const create = operation === "spawnBound" ? vi.spyOn(fixture.host.agents, "spawn") : vi.spyOn(fixture.host.actors, "create");
    const request = { transport: "process" as const, idempotencyKey: "later-retry" };
    const invoke = () => operation === "spawnBound"
      ? fixture.client.spawnAgent({ ...request, task: "completed cached task", extensions: false }, AbortSignal.timeout(5_000))
      : fixture.client.createActor({ ...request, name: "cached actor", instructions: "Reply.", residency: "durable", scope: "project" }, AbortSignal.timeout(5_000));
    const first = await invoke();
    const originalId = fixture.creationIds(operation)[0]!;
    const firstAck = fixture.ack(originalId);
    await waitFor(() => Date.now() > firstAck.completedAt);
    const retry = await invoke();
    expect(retry.id).toBe(first.id);
    expect(create).toHaveBeenCalledTimes(1);
    const ids = fixture.creationIds(operation);
    expect(ids).toHaveLength(2);
    const retryId = ids.find(id => id !== originalId)!;
    expect(residentRequestGeneration(retryId)!).toBeGreaterThan(firstAck.completedAt);
    for (const id of ids) {
      const ack = fixture.ack(id);
      expect(ack.requestFormat).toBe(3);
      expect(ack.completedAt).toBeGreaterThanOrEqual(residentRequestGeneration(id)!);
      expect(ack.acknowledgedAt).toBeGreaterThanOrEqual(ack.completedAt);
      expect(readResidentRequestDecision(fixture.config.residencyRoot, id)?.id).toBe(first.id);
    }
    if (operation === "spawnBound") {
      await waitFor(() => fs.existsSync(residentResultPath(fixture.config.residencyRoot, first.id)));
      await fixture.client.cleanupAgent(first.id);
    } else await fixture.client.removeActor(first.id);
    await fixture.scan(Math.max(...ids.map(id => fixture.ack(id).acknowledgedAt)) + RESIDENT_REQUEST_RETENTION_MS + 1, () => ids.every(id => !fixture.retained(id)));
    for (const id of ids) {
      expect(fixture.retained(id)).toBe(false);
      expect(fs.existsSync(path.join(fixture.config.residencyRoot, "acknowledgements", `${id}.json`))).toBe(false);
    }
    expect(create).toHaveBeenCalledTimes(1);
  } finally { await fixture.close(); }
}, 30_000);
