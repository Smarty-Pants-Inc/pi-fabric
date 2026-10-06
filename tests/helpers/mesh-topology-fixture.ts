import { createHash } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../../src/mesh/store.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { readParticipantFile } from "../../src/topology/participant-files.js";

/** Seed from the real Main -> directory -> state/file publication path, with its rightful writer. */
export const publishTopologyFixture = async (root: string, id: string) => {
  const identity: MeshIdentity = { id, name: id, kind: "main", sessionId: id };
  const pi = { getThinkingLevel: () => "off" } as ExtensionAPI;
  const main = new MainAgentController(pi, id, true, root, id);
  const mesh = new MeshStore(root, 64 * 1024, 100);
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: id, rootId: id, identity, heartbeatMs: 100, leaseMs: 200, reapDeadHosts: false,
  });
  directory.registerSource(() => [directory.root(main.info())]);
  await directory.refresh();
  const hash = createHash("sha256").update(id).digest("hex");
  const host = mesh.get("topology/hosts/" + hash)!;
  const participant = mesh.get("topology/participants/" + hash) ?? readParticipantFile(root, "topology/participants/" + hash)!;
  await directory.close();
  return { host, participant };
};
