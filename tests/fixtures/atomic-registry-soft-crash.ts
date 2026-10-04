// F36 producer: ordinary completion/setInstructions still request a soft save,
// but each whole-registry replacement must keep its actor-definition receipt.
// No worker subprocess runs; the parent kills this host without graceful close.
import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { ActorRegistryStore } from "../../src/actors/registry-store.js";
import { AgentManager } from "../../src/agents/manager.js";
import type { AgentRunResult } from "../../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";

const [root, mode, barrier] = process.argv.slice(2) as [string, "completion" | "setter", "file" | "directory"];
const until = async (predicate: () => boolean) => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("F36 producer timed out");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
let finishA!: () => void;
const aFinished = new Promise<void>(resolve => { finishA = resolve; });
agents.run = async (request, _signal, onStart) => {
  onStart?.({ id: "a".repeat(32), name: "A", status: "running", runner: "pi", transport: "process", cwd: root });
  await aFinished;
  return { id: "a".repeat(32), name: "A", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: root,
    startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, text: "A completed", exitCode: 0,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
};
const actors = new ActorManager("f36", { id: "session:f36", name: "main", kind: "main" }, mesh,
  { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
    meshCursorPath: path.join(root, "actors", "mesh-cursor.json"), closeGraceMs: 50,
  });
const instructions = mode === "setter" ? "Acknowledged updated definition — F36." : "Exact accepted definition — F36.";
const actor = await actors.create({ name: "F36", instructions: "Exact accepted definition — F36.", topics: ["f36.work"], responseMode: "text", coalesce: false });
let accepted: number | undefined, cursor: number | undefined;
if (mode === "completion") {
  void actors.ask(actor.id, "ordinary activation A").catch(() => {});
  await until(() => actors.status(actor.id).status === "running");
  const from = { id: "peer", name: "peer", kind: "actor" as const };
  accepted = (await mesh.publish({ topic: "f36.work", from, text: "accepted activation B" })).sequence;
  const prefix = await mesh.publish({ topic: "f36.ignored", from, text: "beyond B" });
  const cursorFile = path.join(root, "actors", "mesh-cursor.json");
  await until(() => {
    try { cursor = JSON.parse(fs.readFileSync(cursorFile, "utf8")).last.sequence; return cursor! >= prefix.sequence && actors.status(actor.id).queued === 1; }
    catch { return false; }
  });
}
actors.pauseForRelease(); // B cannot start and accidentally establish a prelaunch receipt.
const registry = path.join(root, "actors", "actors.json"), directory = path.dirname(registry);
const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
const files = new Map<number, string>();
const attempts: Array<{ durable: boolean; events: string[]; failed: boolean }> = [];
let current: typeof attempts[number] | undefined, fail = true;
fs.openSync = ((file, flags, perms) => { const fd = open(file, flags, perms); files.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.renameSync = (from, to) => { rename(from, to); if (current && String(to) === registry) current.events.push("rename"); };
fs.fsyncSync = fd => {
  const file = files.get(fd);
  const event = file?.startsWith(`${registry}.`) ? "file" : file === directory ? "directory" : undefined;
  if (current && event) {
    current.events.push(event);
    if (fail && event === barrier) { current.failed = true; throw new Error("F36 registry barrier unavailable"); }
  }
  sync(fd);
};
const write = ActorRegistryStore.prototype.write;
ActorRegistryStore.prototype.write = function(rows, options) {
  const row = rows.find(row => row.id === actor.id);
  if (mode === "setter" || row?.status === "queued") {
    current = { durable: options?.durable === true, events: [], failed: false };
    attempts.push(current);
  }
  try { return write.call(this, rows, options); } finally { current = undefined; }
};
if (mode === "completion") {
  finishA();
  await until(() => !!actors.status(actor.id).lastError?.includes("F36 registry barrier unavailable"));
} else {
  try { await actors.setInstructions(actor.id, instructions); throw new Error("F36 setter incorrectly acknowledged a failed barrier"); }
  catch (error) { if (!(error instanceof Error) || !error.message.includes("F36 registry barrier unavailable")) throw error; }
}
fail = false;
// Retry through the normal public soft setter path, without a prelaunch or
// explicit durable checkpoint. The failed replacement must not discharge debt.
await actors.setInstructions(actor.id, instructions);
console.log(JSON.stringify({ actorId: actor.id, instructions, accepted, cursor, attempts }));
setInterval(() => {}, 1000);
