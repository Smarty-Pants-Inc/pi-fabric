// Producer for ordinary completion -> failed queue replacement -> SIGKILL.
// B is durably admitted and the mesh replay cursor is beyond its source before A finishes.
import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";

const root = process.argv[2]!, actorRoot = path.join(root, "actors");
const cursorPath = path.join(actorRoot, "mesh-cursor.json");
const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
  workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
});
const actors = new ActorManager("review-r1", { id: "session:review-r1", name: "main", kind: "main" }, mesh,
  { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot, persistent: true, meshCursorPath: cursorPath, closeGraceMs: 50,
  });
const actor = await actors.create({ name: "ordinary-crash", instructions: "Work", topics: ["review.work"], responseMode: "text", coalesce: false });
const actorDir = path.join(actorRoot, actor.id);
let finished = false, releaseA!: () => void;
const held = new Promise<void>(resolve => { releaseA = resolve; });
const run = agents.run.bind(agents);
agents.run = async (...args) => { const result = await run(...args); finished = true; await held; return result; };
const until = async (predicate: () => boolean) => { for (let n = 0; !predicate(); n++) { if (n > 1000) throw new Error("completion producer timed out"); await new Promise(resolve => setTimeout(resolve, 20)); } };
actors.tell(actor.id, "ordinary A");
await until(() => finished);
const event = await mesh.publish({ topic: "review.work", from: { id: "peer", name: "peer", kind: "actor" }, text: "accepted crash B" });
await until(() => {
  try { return actors.status(actor.id).queued === 1 && JSON.parse(fs.readFileSync(cursorPath, "utf8")).last.sequence >= event.sequence; } catch { return false; }
});
const queue = path.join(actorDir, fs.readdirSync(actorDir).find(file => /^queue-.+\.json$/.test(file))!);
const files = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
const events: string[] = [];
fs.openSync = ((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.renameSync = (from, to) => { rename(from, to); if (String(to) === queue) events.push("rename"); };
fs.fsyncSync = fd => {
  const file = files.get(fd) ?? "";
  if (file.startsWith(queue + ".") && file.endsWith(".tmp")) events.push("file");
  if (file === actorDir) { events.push("namespace-failed"); throw new Error("completion replacement namespace unavailable"); }
  sync(fd);
};
const put = mesh.put.bind(mesh);
mesh.put = async request => {
  if (request.key === `actors/review-r1/${actor.id}`) {
    console.log(JSON.stringify({ actorId: actor.id, sequence: event.sequence, cursor: JSON.parse(fs.readFileSync(cursorPath, "utf8")).last.sequence, events }));
    // Keep B before launch until the parent kills this producer. No graceful repair.
    await new Promise<void>(() => {});
  }
  return put(request);
};
releaseA();
setInterval(() => {}, 1000);
