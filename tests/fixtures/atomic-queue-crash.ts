// Producer for the real process-restart queue durability regression. No worker
// subprocess is launched: the direct caller occupies the actor's activation slot.
import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";

const root = process.argv[2]!;
const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
agents.run = async (_request, signal) => new Promise<never>((_resolve, reject) => {
  signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
});
const actors = new ActorManager("round4", { id: "session:round4", name: "main", kind: "main" }, mesh,
  { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
    meshCursorPath: path.join(root, "actors", "mesh-cursor.json"), closeGraceMs: 50,
  });
const actor = await actors.create({ name: "restored", instructions: "Work", topics: ["round4.work"], responseMode: "text", coalesce: false });
// A confirmed seed cursor predates the safe ignored prefix and failed work.
actors.pauseForRelease();
await actors.checkpointForRelease();
actors.resumeAfterRelease();
void actors.ask(actor.id, "blocker").catch(() => {});
const actorDir = path.join(root, "actors", actor.id);
const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
const descriptors = new Map<number, string>();
let renamed = false, failures = 0;
fs.openSync = ((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.renameSync = (from, to) => { rename(from, to); if (String(to).startsWith(path.join(actorDir, "queue-"))) renamed = true; };
fs.fsyncSync = fd => {
  // Retain the confirmed seed so the restart has ignored-only progress to
  // checkpoint. Cursor storage is healthy again in the restarted process.
  if (descriptors.get(fd)?.includes("mesh-cursor.json.")) throw new Error("producer cursor checkpoint delayed");
  if (renamed && descriptors.get(fd) === actorDir) { failures++; throw new Error("queue post-rename directory barrier unavailable"); }
  sync(fd);
};
const from = { id: "peer", name: "peer", kind: "actor" as const };
const prefix = await mesh.publish({ topic: "ignored-prefix", from, text: "safe" });
const failed = await mesh.publish({ topic: "round4.work", from, text: "restoration retry" });
while (!failures) await new Promise(resolve => setTimeout(resolve, 20));
console.log(JSON.stringify({ actorId: actor.id, prefix: prefix.sequence, failed: failed.sequence, failures, renamed }));
// Parent SIGKILLs and waits for this process; no graceful queue rewrite can hide
// the visible-but-unconfirmed admission image.
setInterval(() => {}, 1000);
