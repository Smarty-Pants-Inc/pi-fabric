import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  for (const directory of directories.splice(0).reverse()) await directory.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = async (filesOnly: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-registration-participants-"));
  roots.push(root);
  const identity = { id: "session:synthetic-retirement", sessionId: "synthetic-retirement", name: "main", kind: "main" as const };
  const mesh = new MeshStore(root, 64 * 1024, 100);
  if (filesOnly) await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, participants: "files", hostLeases: "files" } });
  const directory = (ownerId: string, interactive = true) => {
    const result = new ParticipantDirectory(mesh, { enabled: true, identity, hostId: identity.id, rootId: identity.id });
    result.registerSource(() => [result.root({ id: identity.id, sessionId: identity.sessionId, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", updatedAt: Date.now(), pendingMessages: false, local: true }, interactive, "fixture-owner", ownerId)]);
    directories.push(result);
    return result;
  };
  const published = () => readParticipantFiles(root, { maxAgeMs: 0 }).map(entry => entry.value);
  return { mesh, directory, published };
};

describe("root registration on main participant safety rules", () => {
  it("preserves noninteractive capabilities with admitted names and ownership tokens", async () => {
    const f = await fixture(false);
    const owner = f.directory("synthetic-owner", false);
    await owner.start();
    expect(f.published()).toEqual([expect.objectContaining({ name: "fixture-owner", rootRegistrationOwnerId: "synthetic-owner", interactive: false, capabilities: ["fabric"] })]);
  });

  it.each([false, true])("retirement deletes only the exact old reload root (files=%s)", async filesOnly => {
    const f = await fixture(filesOnly);
    const old = f.directory("synthetic-old-owner");
    await old.start();
    await old.quiesce("reload");
    await old.close();
    expect(f.published()).toEqual([expect.objectContaining({ status: "reloading" })]);
    await old.retireReloadRoot("synthetic-wrong-owner");
    expect(f.published()).toHaveLength(1);
    await old.retireReloadRoot("synthetic-old-owner");
    expect(f.published()).toHaveLength(0);
    expect(f.mesh.listAll("topology/participants/")).toHaveLength(0);
  });

  it.each([false, true])("old retirement cannot delete a resumed same-owner publication (files=%s)", async filesOnly => {
    const f = await fixture(filesOnly);
    const old = f.directory("synthetic-owner");
    await old.start();
    await old.quiesce("reload");
    await old.close();
    const replacement = f.directory("synthetic-owner");
    await replacement.start();
    await old.retireReloadRoot("synthetic-owner");
    expect(f.published()).toEqual([expect.objectContaining({ status: "idle", rootRegistrationOwnerId: "synthetic-owner" })]);
    if (!filesOnly) expect(f.mesh.listAll("topology/participants/")).toHaveLength(1);
  });
});
