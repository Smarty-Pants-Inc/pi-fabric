import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshBatchOperation } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#6729 / #6477: an idle resident heartbeat must not rewrite the presence of
// every (stopped) actor it owns inside the locked shared-state write.
const STOPPED = 200;
const PERIOD_MS = 5_000;
const roots: string[] = [], closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0)) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const presenceKey = (id: string) => "actors/absent-main/" + id;
const actorId = (index: number) => (index + 1).toString(16).padStart(32, "0");

const fixture = async (files: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stopped-presence-")); roots.push(root);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:absent-main", sessionId: "absent-main", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), "session:absent-main"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const created = Date.now() - 60_000;
  // Row 0 is live (idle); rows 1..STOPPED are stopped durable actors this host owns.
  const records = Array.from({ length: STOPPED + 1 }, (_, index) => ({
    id: actorId(index), name: (index === 0 ? "live" : "stopped-") + index, instructions: "wait",
    createdAt: created, updatedAt: created, rootId: config.rootId, residency: "durable", runner: "pi",
    status: index === 0 ? "idle" : "stopped", events: [], topics: [], messages: [],
  }));
  fs.mkdirSync(config.actorRoot, { recursive: true });
  fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: records }));
  if (files) await new MeshStore(config.meshRoot, 256 * 1024, 500).put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, hostLeases: "files", participants: "files" }, identity: { id: "policy", name: "policy", kind: "agent" } });
  const observer = new ParticipantDirectory(new MeshStore(config.meshRoot, 256 * 1024, 1_000), {
    enabled: true, hostId: "observer", rootId: "session:observer",
    identity: { id: "observer", name: "observer", kind: "agent" }, reapDeadHosts: false,
  });
  closers.push(() => observer.close());
  const start = async (): Promise<ResidentHost> => {
    const host = new ResidentHost(config);
    closers.unshift(() => host.close());
    await host.start();
    await host.participants.refresh();
    return host;
  };
  return { config, observer, start };
};

/** Every shared batch op of each full heartbeat, by key. */
const recordBatches = (host: ResidentHost) => {
  const batches: string[][] = [];
  const original = host.mesh.writeBatch.bind(host.mesh);
  vi.spyOn(host.mesh, "writeBatch").mockImplementation((input: Parameters<MeshStore["writeBatch"]>[0]) => {
    batches.push(input.ops.map((op: MeshBatchOperation) => op.key));
    return original(input);
  });
  return batches;
};
const stoppedKeys = new Set(Array.from({ length: STOPPED }, (_, index) =>
  [participantKey(actorId(index + 1)), presenceKey(actorId(index + 1))]).flat());
const fileVersion = (config: ResidentHostConfig, id: string) => readParticipantFile(config.meshRoot, participantKey(id))?.version;

describe("resident heartbeat with stopped actors (smarty-dev#6729)", () => {
  it.each([false, true])("writes O(1) records per idle heartbeat, not one per stopped actor (files=%s)", async (files) => {
    const { config, observer, start } = await fixture(files);
    const host = await start();
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    // The first heartbeat of this instance published every stopped actor once.
    const sample = actorId(7);
    expect(host.mesh.get(presenceKey(sample), { fresh: true })?.value).toMatchObject({ id: sample, status: "stopped" });
    const presenceBefore = host.mesh.get(presenceKey(sample), { fresh: true })!.version;
    const fileBefore = fileVersion(config, sample);
    const batches = recordBatches(host);
    for (let period = 1; period <= 3; period++) {
      now += PERIOD_MS;
      await host.participants.refresh();
    }
    const written = batches.flat();
    expect(written.filter(key => stoppedKeys.has(key))).toEqual([]);
    for (const batch of batches) expect(batch.length).toBeLessThanOrEqual(4);
    expect(host.mesh.get(presenceKey(sample), { fresh: true })!.version).toBe(presenceBefore);
    expect(fileVersion(config, sample)).toBe(fileBefore);
    // Readers still see the stopped actor as stopped and owned, well past an envelope lease.
    now += 60_000;
    await host.participants.refresh();
    expect(observer.get(sample, undefined, { fresh: true })).toMatchObject({ kind: "actor", status: "stopped", stale: false, ownerHostId: host.hostId });
    expect(observer.list({ scope: "project", kinds: ["actor"], fresh: true }).filter(actor => actor.status === "stopped")).toHaveLength(STOPPED);
    expect(host.actors.list().filter(actor => actor.status === "stopped")).toHaveLength(STOPPED);
    // Live and stopped actor rows remain unchanged: the host lease carries liveness.
    expect(batches.flat().filter(key => key === participantKey(actorId(0)))).toEqual([]);
    expect(batches.flat().filter(key => key === presenceKey(actorId(0)))).toEqual([]);
    clock.mockRestore();
  }, 120_000);

  it.each([false, true])("expires unchanged actor rows when the host lease stops renewing (files=%s)", async files => {
    const { config, observer, start } = await fixture(files);
    const host = await start();
    const id = actorId(0);
    const presence = host.mesh.get(presenceKey(id), { fresh: true })!;
    const participant = host.mesh.get(participantKey(id), { fresh: true });
    const file = fileVersion(config, id);
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    try {
      host.participants.suspend();
      now += 16_000;
      expect(observer.get(id, undefined, { fresh: true })).toBeUndefined();
      expect(host.mesh.get(presenceKey(id), { fresh: true })!.version).toBe(presence.version);
      expect(host.mesh.get(participantKey(id), { fresh: true })?.version).toBe(participant?.version);
      expect(fileVersion(config, id)).toBe(file);
    } finally { clock.mockRestore(); }
  });

  it.each([false, true])("publishes one changed actor in the same pass (files=%s)", async files => {
    const { config, start } = await fixture(files);
    const host = await start();
    const id = actorId(0);
    const beforePresence = host.mesh.get(presenceKey(id), { fresh: true })!;
    const beforeParticipant = host.mesh.get(participantKey(id), { fresh: true });
    const beforeFile = fileVersion(config, id);
    await host.actors.setInstructions(id, "changed in this pass");
    expect(host.mesh.get(presenceKey(id), { fresh: true })!.version).toBeGreaterThan(beforePresence.version);
    // Actor presence is flushed by the setter. The participant envelope carries
    // ownership/status, not private instructions, so it remains unchanged.
    if (files) {
      expect(fileVersion(config, id)).toBe(beforeFile);
      if (beforeParticipant) expect(host.mesh.get(participantKey(id), { fresh: true })!.version).toBe(beforeParticipant.version);
    } else {
      expect(host.mesh.get(participantKey(id), { fresh: true })!.version).toBe(beforeParticipant?.version);
    }
  });

  it.each([false, true])("publishes a public actor change without rewriting siblings (files=%s)", async files => {
    const { config, observer, start } = await fixture(files);
    const host = await start();
    const id = actorId(0), sibling = actorId(7);
    const before = host.mesh.get(presenceKey(id), { fresh: true })!;
    const beforeFile = fileVersion(config, id)!;
    const siblingPresence = host.mesh.get(presenceKey(sibling), { fresh: true });
    const siblingFile = readParticipantFile(config.meshRoot, participantKey(sibling));
    await host.actors.stop(id);
    expect(host.mesh.get(presenceKey(id), { fresh: true })!.version).toBeGreaterThan(before.version);
    expect(fileVersion(config, id)).toBeGreaterThan(beforeFile);
    expect(observer.get(id, undefined, { fresh: true })).toMatchObject({ status: "stopped", stale: false });
    expect(host.mesh.get(presenceKey(sibling), { fresh: true })).toEqual(siblingPresence);
    expect(readParticipantFile(config.meshRoot, participantKey(sibling))).toEqual(siblingFile);
  });

  // off Linux #1031 refuses removal (accepted gap smarty-dev#11335); pi-fabric#1079 unskips
  it.skipIf(process.platform !== "linux")("publishes a stop and a removal at once, renews stopped records rarely, and a restart republishes", async () => {
    const { config, observer, start } = await fixture(false);
    let host = await start();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const live = actorId(0), removed = actorId(3), sample = actorId(9);
    await host.actors.stop(live);
    now += PERIOD_MS;
    await host.participants.refresh();
    expect(host.mesh.get(presenceKey(live), { fresh: true })?.value).toMatchObject({ status: "stopped" });
    expect(observer.get(live, undefined, { fresh: true })).toMatchObject({ status: "stopped", stale: false });
    const removedPresence = host.mesh.get(presenceKey(removed), { fresh: true })!;
    const removedDir = path.join(config.actorRoot, removed);
    const sentinel = path.join(removedDir, "kept-11335.txt");
    if (process.platform !== "linux" && process.platform !== "darwin" && process.platform !== "win32") {
      fs.mkdirSync(removedDir, { recursive: true });
      fs.writeFileSync(sentinel, "history\n");
    }
    const removal = await host.actors.remove(removed);
    now += PERIOD_MS;
    await host.participants.refresh();
    if (process.platform === "linux" || process.platform === "darwin" || process.platform === "win32") {
      expect(host.mesh.get(presenceKey(removed), { fresh: true })).toBeUndefined();
    } else {
      // smarty-dev#8090 / follow-up smarty-dev#11335: on unsupported platforms tree refusal keeps legacy presence;
      // participant publication is independently withdrawn from the revoked actor list.
      expect(removal).toEqual({ removed: true, cleaned: false,
        pending: `removal cleanup failed: Root actor tree deletion needs the Main publication fence, which this platform (${process.platform}) lacks (smarty-dev#8090); follow-up smarty-dev#11335; the root ${config.rootId} actor tree is kept (a later remove or host start finishes it)` });
      expect(fs.readFileSync(sentinel, "utf8")).toBe("history\n");
      expect(host.mesh.get(presenceKey(removed), { fresh: true })).toEqual(removedPresence);
    }
    expect(host.mesh.get(participantKey(removed), { fresh: true })).toBeUndefined();
    expect(observer.get(removed, undefined, { fresh: true })).toBeUndefined();

    // Unchanged stopped records remain stable even after the old envelope-renewal floor.
    const presence = host.mesh.get(presenceKey(sample), { fresh: true })!;
    const envelope = host.mesh.get(participantKey(sample), { fresh: true });
    now += 60 * 60 * 1_000;
    await host.participants.refresh();
    expect(host.mesh.get(presenceKey(sample), { fresh: true })!.version).toBe(presence.version);
    if (envelope) expect(host.mesh.get(participantKey(sample), { fresh: true })!.version).toBe(envelope.version);

    // A restarted host (new instance) republishes its stopped actors' presence once.
    await host.close();
    now += PERIOD_MS;
    const restartedAt = now;
    host = await start();
    const republished = host.mesh.get(presenceKey(sample), { fresh: true })!;
    expect(republished.value).toMatchObject({ id: sample, status: "stopped" });
    expect(republished.updatedAt).toBeGreaterThanOrEqual(restartedAt);
    expect(republished.updatedBy.id).toBe(host.identity.id);
    expect(observer.get(sample, undefined, { fresh: true })).toMatchObject({ status: "stopped", stale: false, ownerHostId: host.hostId });
    expect(host.mesh.get(presenceKey(removed), { fresh: true })).toBeUndefined();
    expect(fs.existsSync(config.actorRoot)).toBe(true);
  }, 120_000);
});
