import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../../src/main-agent.js";
import { MeshStore } from "../../src/mesh/store.js";
import { MainInboxMaintenance, mainInboxActive, mainInboxOwns, registerMainInbox } from "../../src/topology/stall-alarms.js";
import type { FabricParticipantSource } from "../../src/topology/types.js";
const mesh = new MeshStore(process.argv[2]!, 64 * 1024, 500);
const identity = { id: "session:C", sessionId: "C", name: "C", kind: "main" as const };
const pi = { on: () => () => {}, sendMessage: () => { throw new Error("busy inbox must not send"); }, getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
const context = { isIdle: () => false, hasPendingMessages: () => false, sessionManager: { getEntries: () => [] } } as unknown as ExtensionContext;
const main = new MainAgentController(pi, identity.id, true, mesh.root, "C");
const activation = registerMainInbox(mesh.root, identity, "C");
if (process.argv[3] === "before-admission") main.receiveInboxItem = () => { process.kill(process.pid, "SIGKILL"); };
main.attachFollowUpDrain(context, 60_000, path.join(mesh.root, "main-followups", "C.json"), 600,
  { owns: id => mainInboxOwns(mesh.root, identity.id, id), active: () => mainInboxActive(mesh.root, identity.id, activation) });
const publish = mesh.publish.bind(mesh);
mesh.publish = async input => {
  if (input.topic === "fleet.work.inbox-receipts") process.kill(process.pid, "SIGKILL");
  return publish(input);
};
await new MainInboxMaintenance(mesh, identity, { list: () => [] } as unknown as FabricParticipantSource, main,
  { rootPresenceAlarmMs: 900000, undeliveredAlarmMs: 1800000, rootGoneTtlMs: 7200000 }).run();
throw new Error("crash checkpoint was not reached");
