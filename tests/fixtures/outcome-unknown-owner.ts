import fs from "node:fs";
import path from "node:path";
import { MeshStore } from "../../src/mesh/store.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";

// Real cross-process owner: admission, claim, deadline, seen ledger and ACK are
// production code. Hold a committed delivery at its return boundary, not a fake
// response or a dead process. Resuming it must publish the actual late outcome.
const config = JSON.parse(fs.readFileSync(process.argv[2]!, "utf8")) as {
  meshRoot: string; ownerId: string; root: string;
};
const identity = { id: config.ownerId, name: "unknown-owner", kind: "main" as const };
const mesh = new MeshStore(config.meshRoot, 64 * 1024, 100);
let deliveries = 0;
let resolved = false;
const participants = new ParticipantDirectory(mesh, {
  enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100,
});
participants.registerSource(() => [{
  format: 1, id: identity.id, kind: "root", rootId: identity.id,
  ownerHostId: identity.id, ownerIdentityId: identity.id, name: identity.name,
  status: "idle", residency: "session", runner: "pi", transport: "host",
  capabilities: ["fabric", "followUp"], cwd: config.root,
  startedAt: Date.now(), updatedAt: Date.now(), turns: resolved ? deliveries : 0,
  pendingMessages: resolved, controlProtocol: "v1",
}]);
const control = new FabricControlPlane(mesh, identity, {
  enabled: true, hostId: identity.id, ownerIncarnation: participants.ownerIncarnation, pollMs: 20, acknowledgementTimeoutMs: 500,
});
let stop!: () => void;
const stopped = new Promise<void>(resolve => { stop = resolve; });
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
try {
  await participants.start();
  control.start(async command => {
    if (command.operation !== "followUp") return { accepted: false };
    deliveries++;
    fs.writeFileSync(path.join(config.root, "admitted.json"), JSON.stringify({ command, pid: process.pid, deliveries }));
    const deadline = Date.now() + 20_000;
    while (!fs.existsSync(path.join(config.root, "resume"))) {
      if (Date.now() > deadline) throw new Error("Test did not resume the admitted owner");
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    resolved = true;
    await participants.refresh();
    return { accepted: true, messageId: command.commandId, triggered: false };
  });
  fs.writeFileSync(path.join(config.root, "ready"), String(process.pid));
  await stopped;
} finally {
  // Release before closing: close awaits the real active handler/drain.
  fs.writeFileSync(path.join(config.root, "resume"), "cleanup");
  await control.close();
  await participants.close();
}
