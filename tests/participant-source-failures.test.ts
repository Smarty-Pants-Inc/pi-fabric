import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory, type ParticipantSnapshotSource } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { kvWorld } from "./helpers/bridge-nats-kv.js";

// Keep: #17018's recorded NATS retirement regression and participant-ownership
// trust boundary. Real directory/lease/view, existing KV fixture; no broker/worker.
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.restoreAllMocks();
});
const ROOT = "session:4ddfc58b-d782-480d-88b7-51a7cbbd61ec";
const record = (id: string, kind: FabricParticipantRecord["kind"]): FabricParticipantRecord => ({
  format: 1, id, kind, rootId: ROOT, ownerHostId: ROOT, ownerIdentityId: ROOT, parentId: ROOT,
  name: id, status: kind === "root" ? "idle" : "running", runner: "pi",
  transport: kind === "agent" ? "process" : "host", capabilities: ["steer", "followUp", "stop"],
  startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
});
const setup = async (source: ParticipantSnapshotSource, actorRenewalAllowed?: (record: FabricParticipantRecord) => boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-source-failures-"));
  fs.mkdirSync(path.join(root, "local"), { mode: 0o700 });
  const identity: MeshIdentity = { id: ROOT, name: "Main", kind: "main" };
  const world = kvWorld();
  const directory = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), {
    enabled: true, hostId: ROOT, rootId: ROOT, identity, heartbeatMs: 60_000, leaseMs: 180_000,
    reapDeadHosts: false, random: () => 0.5, transport: world.transport("source-machine", ROOT),
    transportStateDir: path.join(root, "local"), ...(actorRenewalAllowed ? { actorRenewalAllowed } : {}),
  });
  directories.push(directory);
  directory.registerSource(() => [record(ROOT, "root")]);
  const removeSource = directory.registerSource(source, { isolateErrors: true, name: "operational" });
  await directory.start();
  const live = (id: string) => directory.participantView?.resolve(id)?.live;
  return { directory, live, removeSource };
};

for (const kind of ["agent", "actor"] as const) describe(`${kind} source identity retention`, () => {
  it("recovers after repeated whole-source failures without replaying stale payloads", async () => {
    const id = `${kind}:failed`, healthy = `${kind}:healthy`;
    let broken = false, name = "before";
    const s = await setup(() => {
      if (broken) throw new Error("source failed");
      return [{ ...record(id, kind), name }, record(healthy, kind)];
    });
    expect(s.live(id)).toBe(true);
    broken = true;
    for (let i = 0; i < 3; i++) await s.directory.refresh();
    expect(s.live(id)).toBeUndefined();
    expect(s.live(healthy)).toBeUndefined();
    expect(s.directory.canConsumeMesh()).toBe(true);
    broken = false; name = "fresh recovery";
    await s.directory.refresh();
    expect(s.live(id)).toBe(true);
    expect(s.directory.get(id)?.name).toBe("fresh recovery");
    expect(s.directory.isolatedSourceFailures()).toEqual([]);
  });

  it("retains omitted failed item ids while publishing healthy items and observing their genuine absence", async () => {
    const id = `${kind}:failed`, healthy = `${kind}:healthy`;
    let broken = false, includeHealthy = true;
    const s = await setup(reportFailure => {
      const records = includeHealthy ? [record(healthy, kind)] : [];
      if (broken) reportFailure(new Error("single item unavailable"), id);
      else records.push(record(id, kind));
      return records;
    });
    broken = true;
    await s.directory.refresh();
    await s.directory.refresh();
    expect(s.live(id)).toBeUndefined();
    expect(s.live(healthy)).toBe(true);
    expect(s.directory.isolatedSourceFailures()).toEqual([{ source: "operational", since: expect.any(Number),
      reason: "participant snapshot source failed", participantId: id }]);
    includeHealthy = false;
    await s.directory.refresh(); // Absence of the healthy id is authoritative, unlike the failed id.
    expect(s.live(healthy)).toBeUndefined();
    broken = false;
    await s.directory.refresh();
    expect(s.live(id)).toBe(true);
    expect(s.live(healthy)).toBeUndefined();
  });
});

it.each(["terminal", "stop-reason", "successful-absence", "partial-successful-absence", "withdrawal", "unregister"] as const)(
  "does not resurrect a task after genuine %s, including while previously omitted by failure", async transition => {
    const id = "agent:must-retire";
    let mode: "live" | "failed" | "absent" | "terminal" | "stop-reason" | "partial-absence" = "live";
    const s = await setup(reportFailure => {
      if (mode === "failed") throw new Error("transient source failure");
      if (mode === "partial-absence") { reportFailure(new Error("different item failed"), "agent:other"); return []; }
      if (mode === "absent") return [];
      return [{ ...record(id, "agent"), ...(mode === "terminal" ? { status: "completed" } : {}),
        ...(mode === "stop-reason" ? { stopReason: "owner-root-gone" as const } : {}) }];
    });
    expect(s.live(id)).toBe(true);
    mode = "failed";
    await s.directory.refresh();
    expect(s.live(id)).toBeUndefined();
    if (transition === "withdrawal") await s.directory.withdrawBeforeLaunch(id);
    else if (transition === "unregister") s.removeSource();
    else {
      mode = transition === "successful-absence" ? "absent"
        : transition === "partial-successful-absence" ? "partial-absence" : transition;
      await s.directory.refresh();
    }
    mode = "live";
    if (transition === "unregister") s.directory.registerSource(() => [record(id, "agent")], { isolateErrors: true, name: "replacement" });
    await s.directory.refresh();
    await s.directory.refresh();
    expect(s.live(id)).toBeUndefined();
    // Only an explicit, successfully fenced new launch can clear task retirement.
    await s.directory.publishBeforeLaunch(record(id, "agent"));
    await s.directory.refresh();
    expect(s.live(id)).toBe(true);
  },
);

it.each(["terminal", "successful-absence"] as const)("preserves authorized same-id actor reactivation after %s, but never a rejected ownership token", async transition => {
  const id = "actor:reactivated";
  let mode: "active" | "stopped" | "absent" = "active", token = "first";
  let authorized = "first";
  const s = await setup(() => mode === "absent" ? [] : [{ ...record(id, "actor"),
    status: mode === "stopped" ? "stopped" : "idle", actorOwnershipToken: token }],
  candidate => candidate.actorOwnershipToken === authorized);
  expect(s.live(id)).toBe(true);
  mode = transition === "terminal" ? "stopped" : "absent";
  await s.directory.refresh();
  expect(s.live(id)).toBeUndefined();
  mode = "active";
  authorized = "second"; // Native reset/recreation rotates the authoritative lineage token.
  await s.directory.refresh();
  expect(s.live(id)).toBeUndefined(); // Cached predecessor must not revive it.
  token = "second";
  await s.directory.refresh();
  expect(s.live(id)).toBe(true); // No task publishBeforeLaunch hook is needed for actor activation.
});
