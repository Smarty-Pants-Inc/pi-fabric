import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";

const root = process.argv[2]!;
process.env.FAKE_CLAUDE_LOG = path.join(root, "claude.jsonl");
const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
  workerPath: path.resolve("src/worker.ts"), claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"), runRoot: path.join(root, "runs"),
});
const actors = new ActorManager("claude-crash", { id: "session:claude-crash", name: "main", kind: "main" },
  new MeshStore(path.join(root, "mesh"), 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
  { actorRoot: path.join(root, "actors"), persistent: true, maxSessionBytes: 64, preparationRetryMs: 25 });
const actor = await actors.create({ name: "native process reset", instructions: "Work.", residency: "durable", runner: "claude", model: "claude/haiku", transport: "process" });
await actors.ask(actor.id, "complete native process context");
while (actors.inFlightCount()) await new Promise(resolve => setTimeout(resolve, 10));
actors.pauseForRelease(); actors.tell(actor.id, "accepted native continuation");
const files = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs), lstat = fs.lstatSync.bind(fs);
let archived: string | undefined, failures = 0;
fs.openSync = ((file: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.renameSync = (from, to) => { rename(from, to); if (from === actor.sessionFile && String(to).endsWith(".bak")) archived = String(to); };
fs.fsyncSync = fd => { if (archived && files.get(fd) === path.dirname(actor.sessionFile!)) { failures++; throw new Error("native reset namespace unavailable"); } sync(fd); };
fs.lstatSync = ((file: fs.PathLike, options?: fs.StatOptions) => {
  if (process.platform === "win32" && archived && String(file) === archived) { failures++; throw new Error("native reset namespace unavailable"); }
  return lstat(file, options);
}) as typeof fs.lstatSync;
actors.resumeAfterRelease();
for (let n = 0; failures === 0; n++) { if (n > 1500) throw new Error("native reset fault did not fire"); await new Promise(resolve => setTimeout(resolve, 10)); }
if (!fs.existsSync(`${actor.sessionFile}.archive-pending.json`)) throw new Error("native reset journal missing");
process.stdout.write(JSON.stringify({ actorId: actor.id, file: actor.sessionFile, archived, failures }) + "\n", () => process.exit(0));
