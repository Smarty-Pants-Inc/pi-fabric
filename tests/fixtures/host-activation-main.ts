// Three independent Main host processes share ONLY an isolated mesh and tokens.
import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";

const [root, indexText, limitText, workerPath, piBinary] = process.argv.slice(2) as [string, string, string, string, string];
const index = Number(indexText);
const limit = Number(limitText);
const ownRoot = path.join(root, `main-${index}`);
fs.mkdirSync(ownRoot, { recursive: true });
const agents = new AgentManager(ownRoot, { ...DEFAULT_FABRIC_CONFIG.agents, hostActivationLimit: limit, maxConcurrent: 8,
  budgetUsd: 0, deniedModels: [], timeoutMs: 30_000, extensions: false, sessionExport: false }, {
  workerPath, piBinary, hostActivationDirectory: path.join(root, "tokens"), runRoot: path.join(ownRoot, "runs"),
  mainAgentId: `proof-main-${index}`, fabricSessionId: `proof-${index}`, fullCodeMode: false,
});
const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
const identity = { id: `session:proof-${index}`, name: `main-${index}`, kind: "main" as const, sessionId: `proof-${index}` };
const actors = new ActorManager(`proof-${index}`, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
  () => {}, { actorRoot: path.join(ownRoot, "actors"), persistent: false });
process.once("SIGTERM", () => {
  void (async () => { await actors.close(); await agents.close(); process.exit(0); })();
});
const wait = async (predicate: () => boolean) => {
  const until = Date.now() + 45_000;
  while (!predicate()) { if (Date.now() > until) throw new Error("Main fixture timeout"); await new Promise(resolve => setTimeout(resolve, 15)); }
};
try {
  const created = await Promise.all(Array.from({ length: 4 }, (_, slot) => actors.create({ name: `main-${index}-actor-${slot}`,
    instructions: "Local deterministic activation proof.", responseMode: "text", extensions: false })));
  fs.writeFileSync(path.join(ownRoot, "ready.json"), JSON.stringify({ pid: process.pid, identity, actors: created.map(actor => actor.id) }));
  await wait(() => fs.existsSync(path.join(root, "go")));
  const requests = created.map(actor => actors.ask(actor.id, "host-cap proof"));
  // Observe waiting through precisely the actorStatus backing method.
  const statusTimer = setInterval(() => {
    const temporary = path.join(ownRoot, "status.tmp");
    fs.writeFileSync(temporary, JSON.stringify(created.map(actor => actors.status(actor.id))));
    fs.renameSync(temporary, path.join(ownRoot, "status.json"));
  }, 25);
  try {
    const messages = await Promise.all(requests);
    fs.writeFileSync(path.join(ownRoot, "results.json"), JSON.stringify(messages));
  } finally { clearInterval(statusTimer); }
} finally {
  await actors.close();
  await agents.close();
}
