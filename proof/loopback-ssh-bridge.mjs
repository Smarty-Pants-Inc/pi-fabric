// smarty-dev#2045 lane C: two real Pis (RPC mode) on two scratch meshes, "dev1" (hub) and "forge",
// linked by pi-fabric#135's bin/mesh-bridge over REAL ssh (a private loopback sshd; see
// loopback-ssh-bridge.sh). Every agent step is a model turn that calls fabric_exec.
// Based on lane B's two-mesh-proof.mjs (pi-fabric#132 round-1 proof).
// usage: node loopback-ssh-bridge.mjs SCRATCH PI_FABRIC_DIST BRIDGE_BIN SSH_HOST SSH_KEY SHIM_PATH
import { spawn, execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

const [scratch, fabricDist, bridgeBin, sshHost, sshKey, shimPath] = process.argv.slice(2);
const { MeshStore } = await import(path.join(path.dirname(fabricDist), "../src/mesh/store.ts"));
const fleetAgent = process.env.PI_CODING_AGENT_DIR;
const MODEL = process.env.PROOF_MODEL ?? "cliproxyapi-anthropic/claude-opus-5-5";
const log = (...a) => console.log(new Date().toISOString(), ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = { startedAt: new Date().toISOString(), fabricDist, bridgeBin, sshHost };
const children = new Set();
process.on("exit", () => { for (const child of children) { try { process.kill(-child.pid, "SIGKILL"); } catch {} try { child.kill("SIGKILL"); } catch {} } });
const fail = (error) => { results.failed = String(error?.stack ?? error); save(); console.error("FAILED", error); process.exit(1); };
process.on("unhandledRejection", fail);
const save = () => fs.writeFileSync(path.join(scratch, "results.json"), JSON.stringify(results, null, 2));

const side = (name) => {
  const root = path.join(scratch, name);
  const agentDir = path.join(root, "agent");
  const cwd = path.join(root, `${name}-work`);
  const mesh = path.join(root, "mesh");
  fs.mkdirSync(agentDir, { recursive: true });
  fs.mkdirSync(cwd, { recursive: true });
  // Credentials stay in the fleet agent dir: link, never copy.
  for (const file of ["auth.json", "models.json"]) fs.symlinkSync(path.join(fleetAgent, file), path.join(agentDir, file));
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ packages: [], extensions: [] }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { root: mesh } }));
  return { name, root, agentDir, cwd, mesh, store: new MeshStore(mesh, 256 * 1024, 500) };
};

const startPi = (s) => {
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(PI_|HERDR|FABRIC)/.test(k)));
  env.PI_CODING_AGENT_DIR = s.agentDir;
  const child = spawn("pi", ["--mode", "rpc", "-ne", "-e", fabricDist, "--model", MODEL, "--thinking", "low", "--session-dir", path.join(s.root, "sessions")],
    { cwd: s.cwd, env, stdio: ["pipe", "pipe", "pipe"] });
  children.add(child);
  const file = path.join(s.root, "rpc-stdout.jsonl");
  const out = fs.createWriteStream(file);
  child.stderr.pipe(fs.createWriteStream(path.join(s.root, "rpc-stderr.log")));
  const listeners = new Set();
  let buffer = "";
  child.stdout.on("data", (chunk) => {
    out.write(chunk);
    buffer += chunk.toString("utf8");
    let at;
    while ((at = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, at).replace(/\r$/, "");
      buffer = buffer.slice(at + 1);
      let record;
      try { record = JSON.parse(line); } catch { continue; }
      for (const listener of listeners) listener(record);
    }
  });
  const prompt = (message, timeoutMs = 600_000) => new Promise((resolve, reject) => {
    const tools = [];
    let text = "";
    const timer = setTimeout(() => { listeners.delete(on); reject(new Error(`${s.name}: prompt timed out`)); }, timeoutMs);
    const on = (record) => {
      if (record.type === "tool_execution_end") tools.push(record.result?.content?.map((c) => c.text).join("") ?? record.result);
      if (record.type === "message_end" && record.message?.role === "assistant") {
        text = (record.message.content ?? []).filter((c) => c.type === "text").map((c) => c.text).join("");
      }
      if (record.type === "agent_settled") { clearTimeout(timer); listeners.delete(on); resolve({ tools, text }); }
    };
    listeners.add(on);
    child.stdin.write(JSON.stringify({ id: randomUUID(), type: "prompt", message }) + "\n");
  });
  // The <fabric-agent-message ... delivery="..."> (or inbox) block that carried a needle into this session.
  const received = (needle) => {
    const raw = fs.readFileSync(file, "utf8");
    for (const line of raw.split("\n")) {
      if (!line.includes(needle)) continue;
      let record;
      try { record = JSON.parse(line); } catch { continue; }
      if (record.type !== "message_end" || !record.message || record.message.role === "assistant" || record.message.role === "toolResult") continue;
      const { content } = record.message;
      const text = typeof content === "string" ? content : (content ?? []).map((c) => c.text ?? "").join("");
      if (!text.includes(needle)) continue;
      const header = /<fabric-agent-message[^>]*>/.exec(text)?.[0] ?? text.slice(0, 300);
      return { at: new Date(record.message.timestamp ?? Date.now()).toISOString(), role: record.message.role, customType: record.message.customType, header };
    }
    return undefined;
  };
  return { child, prompt, received };
};

const roots = (s, mirrored) => s.store.listAll("topology/participants/", { fresh: true })
  .map((e) => e.value).filter((v) => v?.kind === "root" && (mirrored ? v.remoteHost : !v.remoteHost));
const waitFor = async (what, check, ms = 120_000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) { const v = check(); if (v) return v; await sleep(250); }
  throw new Error(`timed out waiting for ${what}`);
};
const run = (code) =>
  `Call the fabric_exec tool exactly once with exactly this code, and nothing else. Then reply with only the tool's raw result.\n\n${code}`;

const A = side("dev1"), B = side("forge");
const piA = startPi(A), piB = startPi(B);
await sleep(5_000);
results.bootstrap = await Promise.all([piA, piB].map((pi) => pi.prompt(run("const main = await agents.main();\nreturn { id: main.id, local: main.local };"))));
const [rootA] = await waitFor("dev1 root", () => roots(A, false)[0] && roots(A, false));
const [rootB] = await waitFor("forge root", () => roots(B, false)[0] && roots(B, false));
results.roots = { dev1: rootA.id, forge: rootB.id };
log("roots", rootA.id, rootB.id);

// The bridge: the hub side on dev1's mesh; the transport is ssh to the loopback sshd, whose forced
// command runs `mesh-bridge agent --mesh <forge mesh> --peer dev1`. The shim only adds `-F <private
// ssh_config>` so nothing in ~/.ssh is touched; the bridge's own ssh argv is unchanged.
const bridgeLog = path.join(scratch, "bridge.log");
const bridge = spawn("node", [bridgeBin, "run", "--mesh", A.mesh, "--name", "dev1", "--remote", "forge",
  "--cursor", path.join(scratch, "bridge-cursor.json"), "--ssh", sshHost, "--ssh-key", sshKey],
  { env: { ...process.env, PATH: `${shimPath}:${process.env.PATH}` }, stdio: ["ignore", "inherit", fs.openSync(bridgeLog, "a")], detached: true });
children.add(bridge);
await waitFor("forge root mirrored into dev1", () => roots(A, true).find((r) => r.id === rootB.id && r.remoteHost === "forge"));
await waitFor("dev1 root mirrored into forge", () => roots(B, true).find((r) => r.id === rootA.id && r.remoteHost === "dev1"));
results.transport = execFileSync("ps", ["-o", "pid,args", "--ppid", String(bridge.pid)], { encoding: "utf8" }).trim();
log("mirrors present both ways; transport:", results.transport);

// 1. Discovery with host set, then steer and followUp dev1 -> forge.
const tag = () => randomUUID().slice(0, 8);
const n = { s1: `LOOP-STEER-D2F-${tag()}`, f1: `LOOP-FOLLOWUP-D2F-${tag()}`, s2: `LOOP-STEER-F2D-${tag()}`, f2: `LOOP-FOLLOWUP-F2D-${tag()}` };
results.dev1Turn = await piA.prompt(run(
  `const peers = await agents.peers();\nconst steer = await agents.steer(${JSON.stringify(rootB.id)}, ${JSON.stringify(`${n.s1}: reply with just OK`)});\nconst followUp = await agents.followUp(${JSON.stringify(rootB.id)}, ${JSON.stringify(`${n.f1}: reply with just OK`)});\nreturn { peers: peers.filter((p) => p.host !== undefined).map((p) => ({ id: p.id, host: p.host, label: p.label })), steer, followUp };`));
results.forgeReceived = { steer: await waitFor("forge got the steer", () => piB.received(n.s1), 180_000), followUp: await waitFor("forge got the followUp", () => piB.received(n.f1), 240_000) };
log("forge received", JSON.stringify(results.forgeReceived));
await sleep(5_000);

// 2. The other direction: forge discovers dev1, steers and followUps it.
results.forgeTurn = await piB.prompt(run(
  `const peers = await agents.peers();\nconst steer = await agents.steer(${JSON.stringify(rootA.id)}, ${JSON.stringify(`${n.s2}: reply with just OK`)});\nconst followUp = await agents.followUp(${JSON.stringify(rootA.id)}, ${JSON.stringify(`${n.f2}: reply with just OK`)});\nreturn { peers: peers.filter((p) => p.host !== undefined).map((p) => ({ id: p.id, host: p.host, label: p.label })), steer, followUp };`));
results.dev1Received = { steer: await waitFor("dev1 got the steer", () => piA.received(n.s2), 180_000), followUp: await waitFor("dev1 got the followUp", () => piA.received(n.f2), 240_000) };
log("dev1 received", JSON.stringify(results.dev1Received));
await sleep(5_000);

// 3. Factory wakes, published on the dev1 hub mesh exactly as factory_host.py owner_wake does.
const pr = `LOOP-PRWAKE-${tag()}`, lane = `LOOP-LANEWAKE-${tag()}`, work = `LOOP-WORK-${tag()}`;
const now = Date.now();
// 3a. ops.owner pr.wake (factory-host:owner) addressed to the forge Main.
const prWake = await A.store.publish({ topic: "ops.owner", kind: "pr.wake", from: { id: "factory-host:owner", name: "factory-owner", kind: "main" },
  to: rootB.id, text: `${pr}: PR smarty-dev#2045 needs its owner`, data: { rootId: rootB.id, repo: "Smarty-Pants-Inc/smarty-dev", pr: 2045 } });
// 3b. The factory's lane wake: the followUp control command owner_wake publishes for a lane session.
const laneWake = await A.store.publish({ topic: "fabric.control.command", kind: "followUp", from: { id: "factory-host:stuck-work", name: "factory-stuck-work", kind: "main" },
  to: rootB.id, text: `${lane}: reply with just OK`, data: { version: 1, commandId: randomUUID().replaceAll("-", ""), targetId: rootB.id, operation: "followUp",
    replyTo: "factory-host:stuck-work", message: `${lane}: reply with just OK`, data: { ref: "smarty-dev#2045" }, requestedAt: now, deadlineAt: now + 120_000 } });
// 3c. A fleet.work event (the #754 shadow record / work inbox), kind p0, from the dev1 Main.
const workEvent = await A.store.publish({ topic: "fleet.work.smarty-dev.2045", kind: "p0", from: { id: rootA.id, name: "main", kind: "main" },
  to: rootB.id, text: `${work}: reply with just OK`, data: { ref: "smarty-dev#2045", key: work } });
results.wakesPublished = { prWake: prWake.id, laneWake: laneWake.id, work: workEvent.id };
const bridged = (id) => B.store.read({ after: 0, limit: 5_000 }).find((e) => e.data?.bridge?.id === id);
const onForge = {
  prWake: await waitFor("pr.wake on forge", () => bridged(prWake.id), 30_000),
  laneWake: await waitFor("lane wake on forge", () => bridged(laneWake.id), 30_000),
  work: await waitFor("fleet.work on forge", () => bridged(workEvent.id), 30_000),
};
results.wakesOnForgeMesh = Object.fromEntries(Object.entries(onForge).map(([k, e]) => [k, { sequence: e.sequence, topic: e.topic, kind: e.kind, from: e.from.id, to: e.to, bridge: e.data.bridge }]));
log("wakes on forge mesh", JSON.stringify(results.wakesOnForgeMesh));
results.forgeWokeByLaneWake = await waitFor("forge Main woken by the lane followUp", () => piB.received(lane), 180_000);
log("lane wake received", JSON.stringify(results.forgeWokeByLaneWake));
results.forgeWokeByWorkInbox = await waitFor("forge Main woken by fleet.work (root inbox)", () => piB.received(work), 240_000).catch((e) => ({ error: String(e.message) }));
log("work inbox", JSON.stringify(results.forgeWokeByWorkInbox));
await sleep(5_000);
// The forge Main reads the pr.wake addressed to it (ops.owner has no Main handler; it is a supervisor's input).
results.forgeReadsPrWake = await piB.prompt(run(
  `const self = (await agents.main()).id;\nconst events = await mesh.read({ topic: "ops.owner", to: self, limit: 50 });\nreturn events.map((e) => ({ kind: e.kind, from: e.from.id, to: e.to, text: e.text, bridge: e.data?.bridge }));`));
log("forge read pr.wake", JSON.stringify(results.forgeReadsPrWake.tools));
save();

// 4. Kill the bridge (SIGKILL: no clean withdrawal). Both Mains then try to reach the other side,
// once a second, and record every attempt with its duration until the named lapse error.
bridge.kill("SIGKILL");
const killedAt = Date.now();
results.bridgeKilledAt = new Date(killedAt).toISOString();
const probe = (target) => run(
  `const killedAt = ${killedAt};\nconst attempts = [];\nfor (let i = 0; i < 60; i++) {\n  const t = Date.now();\n  try { const r = await agents.steer(${JSON.stringify(target)}, "after the bridge died"); attempts.push({ sAfterKill: (t - killedAt) / 1000, ms: Date.now() - t, delivered: r }); }\n  catch (error) { const message = String(error?.message ?? error); attempts.push({ sAfterKill: (t - killedAt) / 1000, ms: Date.now() - t, error: message }); if (/lapsed/.test(message)) break; }\n  await new Promise((r) => setTimeout(r, 1000));\n}\nreturn attempts;`);
const [afterA, afterB] = await Promise.all([piA.prompt(probe(rootB.id)), piB.prompt(probe(rootA.id))]);
results.afterKill = { dev1ToForge: afterA, forgeToDev1: afterB };
await sleep(3_000);
// pgrep exits 1 when nothing matches; any other failure is an error, not "none".
results.processesAfterKill = Object.fromEntries([["bridge or agent", `mesh-bridge (run|agent) --mesh ${A.mesh}|agent --mesh ${B.mesh}`], ["ssh transport", `-i ${sshKey} ${sshHost}`]].map(([what, pattern]) => {
  try { return [what, execFileSync("pgrep", ["-af", "--", pattern], { encoding: "utf8" }).trim()]; }
  catch (error) { if (error.status === 1) return [what, "none"]; throw error; }
}));
results.bridgeLog = fs.readFileSync(bridgeLog, "utf8");
results.finishedAt = new Date().toISOString();
save();
log("RESULTS", JSON.stringify(results.afterKill, null, 2), results.processesAfterKill);
for (const pi of [piA, piB]) pi.child.stdin.end();
await sleep(3_000);
process.exit(0);
