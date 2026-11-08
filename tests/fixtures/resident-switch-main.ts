// Native Main for the release-switch flow. Bundled by the test with the same
// native-digest layout as resident-drain-exit-native; no host/worker is replaced.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { FabricControlPlane, controlOwnerIncarnation } from "../../src/topology/control-plane.js";
import { ResidencyClient } from "../../src/residency/client.js";
import { ResidentActorClient } from "../../src/residency/actor-client.js";
import { processStartTime } from "../../src/residency/process-identity.js";
import { MeshStore, type MeshIdentity } from "../../src/mesh/store.js";
import type { ResidentHostConfig } from "../../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../../src/main-agent.js";

const { config, identity, oldRelease, targetRelease } = JSON.parse(fs.readFileSync(process.argv[2]!, "utf8")) as {
  config: ResidentHostConfig; identity: MeshIdentity; oldRelease: string; targetRelease: string;
};
// Static import and ResidencyClient's bundled dynamic import share this class.
// Call through unmodified; precommit refusals are replies, NOT durable drains.
const releaseDrain = ResidentActorClient.prototype.releaseDrain;
ResidentActorClient.prototype.releaseDrain = async function (...args) {
  const response = await releaseDrain.apply(this, args);
  fs.appendFileSync(path.join(config.cwd, "main-drain-replies.jsonl"), JSON.stringify({ pid: process.pid, response }) + "\n");
  return response;
};
const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
const startedAt = Date.now();
const birth = processStartTime(process.pid);
if (!birth) throw new Error("Native Main requires a verifiable process birth");
participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
  ownerHostId: identity.id, ownerIdentityId: identity.id, name: identity.name, status: "idle", residency: "session",
  runner: "pi", transport: "host", capabilities: ["fabric", "followUp"], cwd: config.cwd, sessionId: config.sessionId,
  startedAt, updatedAt: Date.now(), controlProtocol: "v1",
  mainProcess: { pid: process.pid, host: os.hostname(), startTime: birth } }]);
const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20,
  acknowledgementTimeoutMs: 30_000 });
const makeClient = (desired: ResidentHostConfig, release: string) => new ResidencyClient({ config: desired, mesh, participants,
  mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget, hostPath: path.join(release, "dist/residency/launcher.js") });
const clientA = makeClient(config, oldRelease);
const clientSame = makeClient(structuredClone(config), oldRelease);
const clientB = makeClient({ ...structuredClone(config), workerPath: path.join(targetRelease, "dist/worker.js"),
  fabricExtensionPath: path.join(targetRelease, "dist/index.js"),
  piBinary: path.join(targetRelease, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js") }, targetRelease);
const generation = () => JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8"));
await participants.start();
control.start(() => ({ accepted: false }));
const owner = await clientA.ensureHost();
await clientA.reconcileRelease();
process.send!({ ready: true, owner, main: generation() });
process.on("message", async (request: { id: string; operation: string; input: any }) => {
  try {
    let result: unknown;
    if (request.operation === "same-release") {
      clientSame.start(); result = generation(); await clientSame.close();
    } else if (request.operation === "create") result = await clientA.createActor(request.input);
    else if (request.operation === "followup") {
      const target = participants.get(request.input.id, undefined, { fresh: true });
      const incarnation = target && controlOwnerIncarnation(target);
      if (!incarnation) throw new Error("Actor has no published fenced control owner");
      result = await control.request(clientA.hostId, request.input.id, "followUp", {
        message: request.input.message, ownerIncarnation: incarnation,
      }, clientA.hostId);
    } else if (request.operation === "close-a") result = await clientA.close();
    else if (request.operation === "switch") { clientB.start(); result = generation(); }
    else if (request.operation === "status") result = await clientB.actorStatus(request.input.id);
    else if (request.operation === "batching") result = await clientB.setActor({ operation: "setBatching", id: request.input.id, batchWindowMs: 0 });
    else if (request.operation === "close") {
      await Promise.allSettled([clientA.close(), clientSame.close(), clientB.close()]);
      await control.close(); await participants.close(); mesh.closeState();
      process.send!({ id: request.id, result: { closed: true } }, () => process.exit(0)); return;
    } else throw new Error("Unknown release-switch Main operation");
    process.send!({ id: request.id, result });
  } catch (error) { process.send!({ id: request.id, error: error instanceof Error ? error.stack : String(error) }); }
});
