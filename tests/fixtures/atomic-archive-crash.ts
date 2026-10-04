import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";

const root = process.argv[2]!;
if (process.argv[3] === "win32") Object.defineProperty(process, "platform", { value: "win32" });
const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
  workerPath: path.resolve("tests/fixtures/session-worker.mjs"), runRoot: path.join(root, "runs"),
});
const actors = new ActorManager("archive-crash", { id: "session:archive-crash", name: "main", kind: "main" }, mesh,
  { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, { actorRoot: path.join(root, "actors"), persistent: true });
const actor = await actors.create({ name: "archive process replacement", instructions: "Work.", residency: "durable", transport: "process" });
await actors.ask(actor.id, "complete process transcript");
while (actors.inFlightCount()) await new Promise(resolve => setTimeout(resolve, 10));
const contents = fs.readFileSync(actor.sessionFile!, "utf8");
const prior = ["20000101T000000000Z", "20000102T000000000Z", "20000103T000000000Z"].map(stamp => `${actor.sessionFile}.${stamp}.bak`);
for (const file of prior) fs.copyFileSync(actor.sessionFile!, file);
actors.pauseForRelease();
actors.tell(actor.id, "accepted process continuation");
const files = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
let archived: string | undefined, failures = 0;
fs.openSync = ((file: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.renameSync = (from, to) => { rename(from, to); if (from === actor.sessionFile && String(to).endsWith(".bak")) archived = String(to); };
fs.fsyncSync = fd => { if (archived && files.get(fd) === path.dirname(actor.sessionFile!)) { failures++; throw new Error("archive process namespace unavailable"); } sync(fd); };
if (process.platform === "win32") {
  // Directory fsync is unsupported there; fail the post-rename endpoint walk instead.
  const lstat = fs.lstatSync.bind(fs);
  fs.lstatSync = ((file: fs.PathLike, options?: fs.StatOptions) => {
    if (archived && String(file) === archived) { failures++; throw new Error("archive process namespace unavailable"); }
    return lstat(file, options);
  }) as typeof fs.lstatSync;
}
try { await actors.resetSession(actor.id); throw new Error("expected archive failure"); }
catch (error) { if (!(error instanceof Error) || !error.message.includes("namespace unavailable")) throw error; }
if (!failures || !fs.existsSync(`${actor.sessionFile}.archive-pending.json`)) throw new Error("archive fault did not retain its journal");
// Exit without close/save/healthy I/O: the successor must recover the obligation from disk.
process.stdout.write(JSON.stringify({ actorId: actor.id, file: actor.sessionFile, archived, contents, prior, failures }) + "\n", () => process.exit(0));
