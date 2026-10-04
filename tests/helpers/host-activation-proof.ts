import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { acquireHostActivation } from "../../src/agents/transports/host-activation.js";
import { resolveScriptRuntime } from "../../src/agents/transports/process-utils.js";

export interface ProofEvent { kind: "start" | "end"; id: string; actor: string; pid: number; at: number; token: string }
interface QueueTicket { activationId: string; sequence: number; pid: number }
export interface HostProof {
  processes: number; activations: number; limit: number; maxConcurrent: number;
  completed: number; queuedBeforeRelease: number; hostQueueStatuses: number;
  fifo: boolean; ticketOrder: string[]; startOrder: string[]; events: ProofEvent[];
}

export const runHostActivationProof = async (root: string, limit: number, realWorker = false): Promise<HostProof> => {
  const tokenDirectory = path.join(root, "tokens");
  fs.mkdirSync(root, { recursive: true });
  const log = path.join(root, "events.jsonl");
  const wrapper = path.join(root, "worker.mjs");
  const target = path.resolve(realWorker ? "dist/worker.js" : "tests/fixtures/fake-worker.mjs");
  if (!fs.existsSync(target)) throw new Error(`Missing proof worker: ${target}`);
  fs.writeFileSync(wrapper, `import fs from "node:fs";
const args = new Map(); for(let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const record = kind => fs.appendFileSync(${JSON.stringify(log)}, JSON.stringify({kind,id:args.get("--id"),actor:args.get("--actor-id"),pid:process.pid,at:Date.now(),token:fs.readlinkSync("/proc/self/fd/3")})+"\\n");
record("start"); process.on("exit",()=>record("end"));
${realWorker ? "" : "await new Promise(resolve=>setTimeout(resolve,250));"}
await import(${JSON.stringify(target)});
`);
  const piBinary = path.join(root, "local-pi.mjs");
  fs.writeFileSync(piBinary, `#!/usr/bin/env node
process.stdin.resume();
setTimeout(()=>{ process.stdout.write(JSON.stringify({type:"message_end",message:{role:"assistant",content:"local proof complete"}})+"\\n");process.stdout.write(JSON.stringify({type:"agent_settled"})+"\\n");process.exit(0);},400);
`, { mode: 0o700 });
  // Occupy all slots first, so every process must obtain an observable ticket.
  const blockers: number[] = [];
  const children: { child: ChildProcess; closed: Promise<number | null>; output: () => string }[] = [];
  const wait = async (predicate: () => boolean): Promise<void> => {
    const deadline = Date.now() + 45_000;
    while (!predicate()) {
      const dead = children.find(entry => entry.child.exitCode !== null);
      if (dead) throw new Error(`Main exited early: ${dead.child.exitCode}\n${dead.output()}`);
      if (Date.now() > deadline) throw new Error("Proof wait timed out");
      await new Promise(resolve => setTimeout(resolve, 15));
    }
  };
  let tickets: QueueTicket[] = [];
  let hostQueueStatuses = 0;
  try {
    for (let slot = 0; slot < limit; slot++) blockers.push((await acquireHostActivation({ limit, directory: tokenDirectory }, { id: `block-${slot}` })).fd);
    const runtime = await resolveScriptRuntime({ requireBun: true });
    for (let index = 0; index < 3; index++) {
      const child = spawn(runtime, [path.resolve("tests/fixtures/host-activation-main.ts"), root, String(index), String(limit), wrapper, piBinary], {
        stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, PI_FABRIC_DEPTH: "0", PI_CODING_AGENT_DIR: path.join(root, "profile"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh") },
      });
      let output = "";
      child.stdout?.on("data", data => { output += String(data); });
      child.stderr?.on("data", data => { output += String(data); });
      const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
      children.push({ child, closed, output: () => output });
    }
    await wait(() => [0, 1, 2].every(index => fs.existsSync(path.join(root, `main-${index}`, "ready.json"))));
    fs.writeFileSync(path.join(root, "go"), "go");
    await wait(() => {
      tickets = JSON.parse(fs.readFileSync(path.join(tokenDirectory, "queue.json"), "utf8")) as QueueTicket[];
      return tickets.length === 12;
    });
    await wait(() => {
      hostQueueStatuses = 0;
      for (const index of [0, 1, 2]) {
        try {
          const statuses = JSON.parse(fs.readFileSync(path.join(root, `main-${index}`, "status.json"), "utf8")) as { status: string; hostQueue?: { position: number; waitingSince: number; limit: number } }[];
          hostQueueStatuses += statuses.filter(status => status.status === "waiting" && status.hostQueue?.limit === limit && status.hostQueue.position > 0 && status.hostQueue.waitingSince > 0).length;
        } catch { return false; }
      }
      return hostQueueStatuses === 12;
    });
    fs.writeFileSync(path.join(root, "queue-before-release.json"), JSON.stringify(tickets, null, 2));
    fs.writeFileSync(path.join(root, "statuses-before-release.json"), JSON.stringify([0, 1, 2].map(index => ({
      main: index, actors: JSON.parse(fs.readFileSync(path.join(root, `main-${index}`, "status.json"), "utf8")),
    })), null, 2));
    for (const fd of blockers.splice(0)) fs.closeSync(fd);
    const codes = await Promise.all(children.map(entry => entry.closed));
    for (let index = 0; index < codes.length; index++) if (codes[index] !== 0) throw new Error(`Main ${index} exited ${codes[index]}: ${children[index]!.output()}`);
    const events = fs.readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line) as ProofEvent);
    const active = new Set<string>();
    let maxConcurrent = 0;
    for (const event of events) {
      if (event.kind === "start") { if (active.has(event.id)) throw new Error("Duplicate activation"); active.add(event.id); }
      else { if (!active.delete(event.id)) throw new Error("Unexpected activation exit"); }
      maxConcurrent = Math.max(maxConcurrent, active.size);
      if (!event.token.startsWith(tokenDirectory + path.sep)) throw new Error("Worker did not retain scratch token FD");
    }
    if (active.size) throw new Error("Proof left live activations");
    const startOrder = events.filter(event => event.kind === "start").map(event => event.id);
    const ticketOrder = tickets.map(ticket => ticket.activationId);
    const completed = [0, 1, 2].reduce((sum, index) => sum + (JSON.parse(fs.readFileSync(path.join(root, `main-${index}`, "results.json"), "utf8")) as unknown[]).length, 0);
    const proof = { processes: 3, activations: startOrder.length, limit, maxConcurrent, completed, queuedBeforeRelease: tickets.length,
      hostQueueStatuses, fifo: ticketOrder.every((id, index) => id === startOrder[index]), ticketOrder, startOrder, events };
    fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(proof, null, 2));
    return proof;
  } finally {
    for (const fd of blockers.splice(0)) fs.closeSync(fd);
    for (const entry of children) if (entry.child.exitCode === null && entry.child.signalCode === null) entry.child.kill("SIGTERM");
    await Promise.all(children.map(entry => entry.closed));
    fs.writeFileSync(path.join(root, "main-processes.log"), children.map((entry, index) => `MAIN ${index}\n${entry.output()}`).join("\n"));
  }
};
