import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { kvWorld } from "./helpers/bridge-nats-kv.js";

// AUDIT ONLY (ORG-2395 / smarty-dev#17018): does a transient isolated agent-source
// failure on NATS permanently retire that host's live task agents?
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.restoreAllMocks();
});

const ROOT = "session:4ddfc58b-d782-480d-88b7-51a7cbbd61ec";
const rootRecord = (): FabricParticipantRecord => ({ format: 1, id: ROOT, kind: "root", rootId: ROOT,
  ownerHostId: ROOT, ownerIdentityId: ROOT, name: "Main", status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" });

const setup = async (kind: "agent" | "actor") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "audit-17018-"));
  const identity: MeshIdentity = { id: ROOT, name: "Main", kind: "main" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
  const world = kvWorld();
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: ROOT, rootId: ROOT, identity, heartbeatMs: 60_000, leaseMs: 180_000,
    reapDeadHosts: false, random: () => 0.5,
    transport: world.transport("audit-machine", ROOT),
    transportStateDir: fs.mkdtempSync(path.join(os.tmpdir(), "audit-17018-local-")),
  });
  directories.push(directory);
  directory.registerSource(() => [rootRecord()]);
  let broken = false;
  const id = `${kind}:live-task`;
  directory.registerSource(() => {
    if (broken) throw new Error("transient isolated source failure");
    return [{ format: 1, id, kind, rootId: ROOT, ownerHostId: ROOT, ownerIdentityId: ROOT, parentId: ROOT,
      name: "live task", status: "running", runner: "pi", transport: kind === "agent" ? "process" : "host",
      capabilities: ["steer", "followUp", "stop"], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }];
  }, { isolateErrors: true });
  await directory.start();
  return { directory, id, setBroken: (value: boolean) => { broken = value; } };
};

for (const kind of ["agent", "actor"] as const) {
  it.skipIf(process.platform !== "linux")(`NATS: a live ${kind} returns after a transient isolated source failure`, async () => {
    const s = await setup(kind);
    await s.directory.refresh();
    expect(s.directory.participantView?.resolve(s.id)?.live).toBe(true);
    s.setBroken(true);
    await expect(s.directory.refresh()).resolves.toBeUndefined();
    const during = s.directory.participantView?.resolve(s.id);
    s.setBroken(false);
    await s.directory.refresh();
    await s.directory.refresh();
    const after = s.directory.participantView?.resolve(s.id);
    console.info(`AUDIT ${kind}`, JSON.stringify({ during: during ?? null, afterLive: after?.live ?? null }));
    expect(during).toBeUndefined();
    expect(after?.live, `${kind} must be republished once its source recovers`).toBe(true);
  });
}
