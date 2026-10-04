import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { acquireHostActivation } from "../../src/agents/transports/host-activation.js";

interface Entry { child: ChildProcess; closed: Promise<number | null>; settled: number; output: string; stderr: string }
interface Event { type: string; runId?: string; workerPid: number; pid: number; held?: boolean; event?: { isError?: boolean; toolName?: string }; [key: string]: unknown }
const readEvents = (file: string): Event[] => {
  try { return fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Event); } catch { return []; }
};
const alive = (pid: number): boolean => {
  try { const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8"); return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!); } catch { return false; }
};
export const runRealPiHostProof = async (root: string, limit: number, tasks: number, nested = false,
  piBinary = process.env.PI_FABRIC_TEST_PI_BINARY ?? path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js")) => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const home = path.join(root, "home"); const log = path.join(root, "native-events.jsonl");
  const tokens = path.join(home, ".local/share/smarty-dev/fabric-host-tokens");
  const extension = path.resolve("dist/index.js");
  if (!fs.existsSync(extension)) throw new Error("Build Fabric before real-Pi acceptance");
  const entries: Entry[] = []; const blockers: number[] = [];
  let maxLive = 0; let maxHeldSlots = 0; let observations = 0;
  const sample = () => {
    // Discover native workers by their inherited scratch-token descriptor,
    // including the interval before Pi session_start. No worker wrapper or
    // launch substitution is involved; unrelated processes are never recorded.
    const discovered: number[] = [];
    for (const name of fs.readdirSync("/proc")) {
      if (!/^\d+$/.test(name)) continue;
      try {
        if (!fs.readlinkSync(`/proc/${name}/fd/3`).startsWith(tokens + path.sep)) continue;
        if (fs.readFileSync(`/proc/${name}/cmdline`, "utf8").split("\0").some(arg => arg === path.resolve("dist/worker.js"))) discovered.push(Number(name));
      } catch { /* process exited during observation */ }
    }
    const workers = [...new Set([...discovered, ...readEvents(log).filter(event => event.runId && event.type === "session-start").map(event => event.workerPid)])].filter(alive);
    const held = workers.filter(pid => { try { return fs.readFileSync(`/proc/${pid}/fdinfo/3`, "utf8").includes("lock:"); } catch { return false; } });
    maxLive = Math.max(maxLive, workers.length); maxHeldSlots = Math.max(maxHeldSlots, held.length); observations++;
    fs.appendFileSync(path.join(root, "concurrency.jsonl"), JSON.stringify({ at: Date.now(), workers, held }) + "\n");
  };
  let timer: ReturnType<typeof setInterval> | undefined;
  const wait = async (predicate: () => boolean, label: string) => {
    const deadline = Date.now() + 90_000;
    while (!predicate()) {
      const exited = entries.find(entry => entry.child.exitCode !== null || entry.child.signalCode !== null);
      if (exited || Date.now() > deadline) throw new Error(`Real Pi ${label} failed: ${entries.map(entry => entry.stderr + "\n" + entry.output.slice(-12000)).join("\n")}`);
      const errors = readEvents(log).filter(event => event.type === "tool-result" && event.event?.isError);
      if (errors.length) throw new Error(`Real Pi tool failure: ${JSON.stringify(errors)}`);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  };
  const prompt = (entry: Entry, code: string) => entry.child.stdin!.write(JSON.stringify({ type: "prompt", message: "HOST_CAP_MAIN_CODE:\n" + code }) + "\n");
  const outputFile = (index: number, file: string) => path.join(root, `main-${index}`, file);
  const write = (file: string, value: string) => `await pi.write({path:${JSON.stringify(file)},text:JSON.stringify(${value})});`;
  try {
    if (!nested) for (let slot = 0; slot < limit; slot++) blockers.push((await acquireHostActivation({ limit, directory: tokens }, { id: `proof-block-${slot}` })).fd);
    for (let index = 0; index < 3; index++) {
      const cwd = path.join(root, `main-${index}`); const profile = path.join(home, `profile-${index}`);
      fs.mkdirSync(cwd, { recursive: true }); fs.mkdirSync(profile, { recursive: true });
      fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ packages: [path.resolve(".")],
        extensions: [path.resolve("tests/fixtures/host-activation-cli-provider.ts")], defaultProjectTrust: "always", enableInstallTelemetry: false,
        compaction: { enabled: false }, retry: { enabled: false } }));
      fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ fullCodeMode: true, executor: { timeoutMs: 90_000 },
        agents: { hostActivationLimit: limit, hostActivationLimitScope: "all", maxConcurrent: 16, maxDepth: 8, timeoutMs: 60_000,
          budgetUsd: 0, deniedModels: [], extensions: true, sessionExport: false, retainRuns: true, nice: 19 },
        residency: { enabled: false }, memory: { enabled: false }, entropy: { enabled: false }, prewalk: { enabled: false }, mcp: { enabled: false }, mesh: { actorPollMs: 25 } }));
      const child = spawn(piBinary, ["--mode", "rpc", "--session-dir", path.join(cwd, "sessions"), "--model", "host-cap-proof/offline", "--thinking", "off", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files"], {
        cwd, stdio: ["pipe", "pipe", "pipe"], env: { PATH: process.env.PATH, HOME: home, TMPDIR: process.env.TMPDIR,
          PI_CODING_AGENT_DIR: profile, PI_OFFLINE: "1", PI_FABRIC_PI_BINARY: piBinary, PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
          PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), HOST_CAP_PROOF_LOG: log },
      });
      const entry: Entry = { child, closed: new Promise((resolve, reject) => { child.once("error", reject); child.once("close", resolve); }), settled: 0, output: "", stderr: "" };
      let buffer = "";
      child.stdout!.on("data", chunk => {
        entry.output += String(chunk); buffer += String(chunk);
        for (;;) { const lf = buffer.indexOf("\n"); if (lf < 0) break; const line = buffer.slice(0, lf); buffer = buffer.slice(lf + 1);
          try { if (JSON.parse(line).type === "agent_settled") entry.settled++; } catch { /* retain non-JSON diagnostics */ } }
      });
      child.stderr!.on("data", chunk => { entry.stderr += String(chunk); }); entries.push(entry);
      const count = Array.from({ length: tasks }, (_, task) => task).filter(task => task % 3 === index).length;
      const create = `const actor = await agents.create({scope:"session",name:"proof-leaf-${index}",instructions:"HOST_CAP_LEAF actor",model:"host-cap-proof/offline",responseMode:"text"}); ${write(outputFile(index, "ready.json"), "actor")}`;
      if (nested) prompt(entry, create);
      else prompt(entry, `${create}\nconst runs = Array.from({length:${count}},(_,i)=>agents.run({task:"HOST_CAP_LEAF independent ${index}:"+i,model:"host-cap-proof/offline",transport:"process"}));
const ask = agents.ask({id:actor.id,message:"HOST_CAP_LEAF queue visibility"});
let status; for(let i=0;i<1000;i++){status=await agents.actorStatus({id:actor.id});if(status.hostQueue)break;await new Promise(resolve=>setTimeout(resolve,25));}
if(!status.hostQueue)throw new Error("Missing actor hostQueue"); ${write(outputFile(index, "queued.json"), "status")}
const results=await Promise.all(runs); const message=await ask;
if(results.some(r=>r.status!=="completed")||message.error)throw new Error(JSON.stringify({results,message})); ${write(outputFile(index, "results.json"), "{results,message}")}`);
    }
    timer = setInterval(sample, 20);
    await wait(() => [0,1,2].every(index => fs.existsSync(outputFile(index, "ready.json"))), "root startup");
    if (nested) {
      await wait(() => entries.every(entry => entry.settled >= 1), "setup settlement");
      const target = JSON.parse(fs.readFileSync(outputFile(1, "ready.json"), "utf8")).id;
      prompt(entries[0]!, `const parent=await agents.create({scope:"session",name:"proof-parent",instructions:"Parent awaits cross-root actor.",model:"host-cap-proof/offline",responseMode:"text"});
const actor=await agents.ask({id:parent.id,message:${JSON.stringify("HOST_CAP_PARENT_ASK:" + target)}});
const joined=await agents.run({task:"HOST_CAP_PARENT_JOIN",model:"host-cap-proof/offline",transport:"process"});
const run=await agents.run({task:"HOST_CAP_PARENT_RUN",model:"host-cap-proof/offline",transport:"process"});
if(actor.error||joined.status!=="completed"||run.status!=="completed")throw new Error(JSON.stringify({actor,joined,run})); ${write(outputFile(0, "nested.json"), "{actor,joined,run}")}`);
      await wait(() => fs.existsSync(outputFile(0, "nested.json")) && entries[0]!.settled >= 2, "cap-one nested completion");
    } else {
      await wait(() => [0,1,2].every(index => fs.existsSync(outputFile(index, "queued.json"))), "public actorStatus queue");
      for (const fd of blockers.splice(0)) fs.closeSync(fd);
      await wait(() => [0,1,2].every(index => fs.existsSync(outputFile(index, "results.json"))) && entries.every(entry => entry.settled >= 1), "task completion");
    }
    sample();
    for (const entry of entries) entry.child.stdin!.end();
    const codes = await Promise.all(entries.map(entry => entry.closed));
    if (codes.some(code => code !== 0)) throw new Error(`Real Pi shutdown failed: ${JSON.stringify(codes)}`);
    const events = readEvents(log); const runs = new Set(events.filter(event => event.type === "session-start" && event.runId).map(event => event.runId));
    const completed = nested ? 3 : [0,1,2].reduce((sum,index) => sum + JSON.parse(fs.readFileSync(outputFile(index,"results.json"),"utf8")).results.length, 0);
    const builtArtifacts = Object.fromEntries(["dist/index.js", "dist/worker.js", "dist/agents/transports/host-activation-yield.js"].map(file => [file, createHash("sha256").update(fs.readFileSync(file)).digest("hex")]));
    const result = { piBinary: fs.realpathSync(piBinary), extension, builtArtifacts, roots: 3, limit, tasks, completed, activations: runs.size, maxLive,
      maxHeldSlots, observations, queuedActorStatuses: nested ? 0 : 3, nested, shutdownCodes: codes,
      nativeToolCalls: events.filter(event => event.type === "tool-call" && event.event?.toolName === "fabric_exec").length,
      inferenceAlwaysAdmitted: events.filter(event => event.type === "provider-call" && event.runId).every(event => event.held),
      errors: events.filter(event => event.type === "tool-result" && event.event?.isError) };
    if (result.errors.length || !result.inferenceAlwaysAdmitted || maxHeldSlots > limit || (!nested && (maxLive > limit || completed !== tasks))) throw new Error(`Real Pi oracle failed: ${JSON.stringify(result)}`);
    fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(result, null, 2));
    return result;
  } finally {
    if (timer) clearInterval(timer);
    for (const fd of blockers.splice(0)) fs.closeSync(fd);
    for (const entry of entries) if (entry.child.exitCode === null && entry.child.signalCode === null) { entry.child.stdin!.end(); entry.child.kill("SIGTERM"); }
    await Promise.all(entries.map(entry => entry.closed));
    for (let index = 0; index < entries.length; index++) {
      fs.writeFileSync(outputFile(index, "rpc.jsonl"), entries[index]!.output);
      fs.writeFileSync(outputFile(index, "stderr.log"), entries[index]!.stderr);
    }
  }
};
