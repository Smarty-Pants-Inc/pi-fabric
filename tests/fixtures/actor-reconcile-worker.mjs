// Only the descendant case differs from the ordinary fake worker. It retains a
// real session writer after the leader dies, so PID absence cannot admit a run.
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

if (process.argv[2] === "--owner") {
  // A separate real owner, killed at the status rename BEFORE the actor's run
  // continuation can checkpoint anything. The test then starts a fresh owner.
  const [, , , bundle, root, mode] = process.argv;
  const { ActorManager, AgentManager, ProcessTransport, DEFAULT_FABRIC_CONFIG, MeshStore } = await import(pathToFileURL(bundle).href);
  if (process.env.ACTOR_LOSS_FENCE_RED_SEED === "1") {
    // Deliberate RED seed: reproduce the old publish-before-queue-fence window.
    const run = AgentManager.prototype.run;
    AgentManager.prototype.run = function (...args) { args[7] = undefined; return run.apply(this, args); };
  }
  const realNow = Date.now.bind(Date);
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async check => {
    const deadline = realNow() + 12_000;
    while (!check()) {
      if (realNow() > deadline) throw new Error(`Owner fixture timed out (${mode})`);
      await sleep(10);
    }
  };
  const identity = { id: mode === "adopt" ? "session:reconcile-adopter" : "session:reconcile-crash",
    name: "main", kind: "main", sessionId: "reconcile-crash" };
  const restarting = mode === "restart" || mode === "adopt";
  const blocked = mode === "loss-fallback" || mode === "interrupt";
  if (blocked) {
    const launch = ProcessTransport.prototype.launch;
    let first = true;
    ProcessTransport.prototype.launch = async function (request) {
      const native = await launch.call(this, request);
      if (!first) return native;
      first = false;
      let held = false;
      return { ...native, isAlive: async () => {
        if (!held) { held = true; await new Promise(() => {}); }
        return native.isAlive();
      } };
    };
  }
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, sessionExport: false }, {
    runRoot: path.join(root, "runs"), workerPath: fileURLToPath(import.meta.url),
  });
  const actors = new ActorManager("reconcile-crash", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 0, meshRetentionSweepPath: false,
    });
  const stateFile = path.join(root, "state.json");
  let state = fs.existsSync(stateFile) ? JSON.parse(fs.readFileSync(stateFile, "utf8")) : undefined;
  if (restarting) {
    await until(() => actors.status(state.actorId).status === "idle" && actors.status(state.actorId).queued === 0 &&
      fs.readFileSync(path.join(root, "worker-launches.jsonl"), "utf8").includes("NEXT queued item"));
    fs.writeFileSync(path.join(root, "restored.json"), JSON.stringify({ status: actors.status(state.actorId), messages: actors.messages(state.actorId) }));
    await actors.close();
    await agents.close();
  } else {
    if (!state) {
      const actor = await actors.create({ name: "crash-serial", instructions: "Respond.", responseMode: "text", coalesce: false });
      state = { actorId: actor.id };
      actors.tell(actor.id, "HANG", { replayId: "lost-original", reconcileCrashRoot: root });
    }
    await until(() => !!actors.status(state.actorId).inFlightRun);
    const runId = actors.status(state.actorId).inFlightRun.id;
    const statusFile = path.join(agents.runDirectory(runId), "status.json");
    await until(() => fs.existsSync(statusFile));
    if (!fs.existsSync(stateFile)) actors.tell(state.actorId, "NEXT queued item", { reconcileCrashRoot: root });
    const actorDir = path.join(root, "actors", state.actorId);
    const queueFile = path.join(actorDir, fs.readdirSync(actorDir).find(name => /^queue-.*\.json$/.test(name)));
    state = { ...state, runId, statusFile, queueFile, worker: agents.workerIdentity(runId) };
    fs.writeFileSync(stateFile, JSON.stringify(state));
    fs.copyFileSync(queueFile, path.join(root, "before.json"));
    const rename = fs.renameSync;
    fs.renameSync = function (from, to) {
      const result = rename.call(this, from, to);
      if (String(to) === statusFile && fs.readFileSync(statusFile, "utf8").includes("Actor worker is proven gone")) {
        fs.writeFileSync(path.join(root, "published.json"), JSON.stringify({
          verdict: JSON.parse(fs.readFileSync(statusFile, "utf8")), queue: JSON.parse(fs.readFileSync(queueFile, "utf8")),
        }));
        process.kill(process.pid, "SIGKILL");
      }
      return result;
    };
    process.kill(state.worker.pid, "SIGKILL");
    await until(() => !agents.workerIdentity(runId)?.open);
    if (mode === "interrupt") {
      // No verdict: this is the deliberately retained bounded restart policy.
      process.kill(process.pid, "SIGKILL");
    } else if (mode === "loss-fallback") {
      Date.now = () => realNow() + 31_000;
      await mesh.publish({ topic: "reconcile.pulse", from: identity });
    }
    await until(() => false); // the publication hook must kill this owner
  }
} else if (process.argv[2] === "--descendant") {
  const [, , , root, session] = process.argv;
  fs.writeFileSync(path.join(root, "descendant-ready"), String(process.pid));
  const timer = setInterval(() => {
    if (!fs.existsSync(path.join(root, "release-descendant"))) return;
    fs.appendFileSync(session, JSON.stringify({ reconcileDescendant: "last write" }) + "\n");
    clearInterval(timer);
  }, 20);
} else {
  const args = new Map();
  for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
  const task = fs.readFileSync(args.get("task-file"), "utf8");
  const crashRoot = task.match(/"reconcileCrashRoot":\s*("(?:\\.|[^"\\])*")/);
  if (crashRoot) {
    const root = JSON.parse(crashRoot[1]);
    fs.appendFileSync(path.join(root, "worker-launches.jsonl"), JSON.stringify({ id: args.get("id"), task }) + "\n");
  }
  const match = task.match(/"reconcileDescendantRoot":\s*("(?:\\.|[^"\\])*")/);
  if (crashRoot && fs.existsSync(path.join(JSON.parse(crashRoot[1]), "restarted"))) {
    const now = Date.now();
    fs.writeFileSync(args.get("status-file"), JSON.stringify({
      id: args.get("id"), name: args.get("name"), task, status: "completed", runner: "pi", transport: "process",
      sessionId: String(process.pid), cwd: args.get("cwd"), startedAt: now, updatedAt: now, finishedAt: now,
      turns: 1, toolCalls: 0, inferenceStarted: true, text: "restored activation completed", exitCode: 0,
      usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }));
  } else if (!match) await import("./fake-worker.mjs");
  else {
    const root = JSON.parse(match[1]);
    const now = Date.now();
    fs.writeFileSync(args.get("status-file"), JSON.stringify({
      id: args.get("id"), name: args.get("name"), task, status: "running", runner: "pi", transport: "process",
      sessionId: String(process.pid), cwd: args.get("cwd"), startedAt: now, updatedAt: now,
      turns: 1, toolCalls: 1, inferenceStarted: true, text: "", exitCode: null,
      usage: { input: 1, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }));
    spawn(process.execPath, [fileURLToPath(import.meta.url), "--descendant", root, args.get("session-file")], { stdio: "ignore" }).unref();
    setInterval(() => {}, 1_000);
  }
}
