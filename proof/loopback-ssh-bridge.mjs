// smarty-dev#2045 lane C: two real Pis (RPC mode) on two scratch meshes, "dev1" (hub) and "forge",
// linked by pi-fabric#135's bin/mesh-bridge over REAL ssh (a private loopback sshd; see
// loopback-ssh-bridge.sh). Every agent step is a model turn that calls fabric_exec.
// Based on lane B's two-mesh-proof.mjs (pi-fabric#132 round-1 proof).
// usage: bun loopback-ssh-bridge.mjs SCRATCH PI_FABRIC_DIST BRIDGE_BIN SSH_HOST SSH_KEY SSH_PORT KNOWN_HOSTS
import { spawn, execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

// BEGIN LOOPBACK VALIDATORS (the scratch negative probe evaluates this exact block).
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const toolEvidence = (code, token) => {
  let start, end, value;
  const records = [];
  return {
    records,
    accept(record) {
      records.push(record);
      if (record.type === "tool_execution_start") {
        assert(!start && record.toolName === "fabric_exec" && record.args?.code === code && record.toolCallId,
          "missing/extra/wrong exact-program fabric_exec selection");
        start = record;
      }
      if (record.type !== "tool_execution_end") return;
      assert(start && !end && record.toolCallId === start.toolCallId && record.toolName === "fabric_exec", "wrong toolCallId/tool result");
      assert(!record.isError && !record.result?.isError && record.result?.details?.success !== false, "fabric_exec error");
      end = record;
      const text = (record.result?.content ?? []).filter((c) => c.type === "text").map((c) => c.text).join("\n");
      let parsed;
      for (let a = text.indexOf("{"); a >= 0 && !parsed; a = text.indexOf("{", a + 1)) {
        for (let b = text.lastIndexOf("}"); b > a; b = text.lastIndexOf("}", b - 1)) {
          try { const candidate = JSON.parse(text.slice(a, b + 1)); if (candidate.proofToken === token && Object.hasOwn(candidate, "value")) { parsed = candidate; break; } } catch {}
        }
      }
      assert(parsed, "actual tool result missing result token/value");
      value = parsed.value;
    },
    finish() { assert(start && end, "missing actual fabric_exec result"); return { value, toolCallId: start.toolCallId, token, records }; },
  };
};
const promptEvidence = (code, token, message) => {
  const evidence = toolEvidence(code, token);
  let sequence = 0, activeRun, owningRun, ended = false, settled = false;
  return {
    records: evidence.records,
    accept(record) {
      assert(!settled, "event after requested run settled");
      if (record.type === "agent_start") activeRun = ++sequence;
      if ((record.type === "message_start" || record.type === "message_end") && record.message?.role === "user") {
        const content = record.message.content;
        const text = typeof content === "string" ? content : Array.isArray(content) && content.every((c) => c.type === "text")
          ? content.map((c) => c.text).join("") : undefined;
        if (text === message) {
          assert(activeRun !== undefined && (owningRun === undefined || owningRun === activeRun), "prompt run identity mismatch");
          owningRun = activeRun;
        }
      }
      // Prior settlement and unrelated native no-tool replies do not own this prompt.
      if (owningRun === undefined) {
        if (record.type === "agent_settled") activeRun = undefined;
        return;
      }
      assert(activeRun === owningRun, "owning run changed before settlement");
      if (record.type === "tool_execution_start" || record.type === "tool_execution_end") assert(!ended, "tool execution after owning agent_end");
      evidence.accept(record);
      if (record.type === "agent_end") ended = true;
      if (record.type === "agent_settled") {
        assert(ended && record.outcome === "completed", "owning run settled without completed agent_end");
        const value = evidence.finish();
        settled = true;
        activeRun = undefined;
        return value;
      }
    },
  };
};
const assertTurn = (turn, target, host) => {
  assert(turn?.value?.peers?.some((p) => p.id === target && p.host === host), `missing native peer ${target} on ${host}`);
  for (const name of ["steer", "followUp"]) {
    const ack = turn.value[name];
    assert(ack?.queued === true && ack.routed === "mesh" && ack.acknowledged === true && typeof ack.messageId === "string" && ack.messageId.trim(), `${name}: missing mesh acknowledged/queued messageId`);
  }
  assert(turn.value.steer.messageId !== turn.value.followUp.messageId, "send IDs must be distinct");
};
const assertWake = (event, original, recipient) => {
  assert(event && event.topic === original.topic && event.kind === original.kind && event.to === recipient &&
    event.from?.id === original.from.id && event.text === original.text && event.data?.bridge?.id === original.id,
  `missing/wrong original wake ${original.id} for ${recipient}`);
};
const assertNative = (message, needle, customType, id, sender, delivery) => {
  const content = typeof message?.content === "string" ? message.content : (message?.content ?? []).map((c) => c.text ?? "").join("");
  assert(message?.role === "custom" && message.customType === customType && content.includes(needle), "missing native receiver message");
  if (customType === "pi-fabric-inbox") assert(message.details?.ids?.includes(id) && content.includes(`id="${id}"`), "missing native work inbox event ID");
  else {
    const items = message.details?.items ?? [message.details];
    const blocks = [...content.matchAll(/<fabric-agent-message\s+([^>]*)>([\s\S]*?)<\/fabric-agent-message>/g)];
    const xml = (text) => String(text).replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    assert(blocks.length === items.length, "native item/block count mismatch");
    assert(items.some((item, index) => {
      const [, attrs, body] = blocks[index];
      return typeof item?.id === "string" && item.id.trim() && (id === undefined || item.id === id) &&
        item.from?.id === sender && (delivery === undefined || item.delivery === delivery) &&
        attrs.split(/\s+/).includes(`from_id="${xml(item.from.id)}"`) && attrs.split(/\s+/).includes(`delivery="${xml(item.delivery)}"`) &&
        body.split("\n<data>")[0].includes(xml(needle));
    }), "wrong native same-item text/ID/sender/delivery");
  }
  return { customType, id, sender, content, details: message.details };
};
const assertLapse = (turn, target, host, boundS = 180) => {
  const attempts = turn?.value;
  assert(Array.isArray(attempts) && attempts.length > 0, "missing post-kill attempts");
  const last = attempts.at(-1);
  assert(typeof last.error === "string" && last.error.includes(`Unknown Fabric participant: ${target}`) &&
    last.error.includes(`lease mirrored from remote host ${host} lapsed`) && last.error.includes("mesh bridge") &&
    Number.isFinite(last.sAfterKill) && Number.isFinite(last.ms) && last.ms >= 0 && last.sAfterKill >= 0 && last.sAfterKill + last.ms / 1000 <= boundS,
  `missing/beyond-bound named ${host} lease-lapsed error for ${target}`);
};
const assertNoSurvivors = (processes) => { assert(Object.keys(processes).length === 2 && Object.values(processes).every((v) => v === "none"), "bridge/SSH transport survivors"); };
// END LOOPBACK VALIDATORS

const [scratch, fabricDist, bridgeBin, sshHost, sshKey, sshPort, knownHosts] = process.argv.slice(2);
let MeshStore;
const fleetAgent = process.env.PI_CODING_AGENT_DIR;
const MODEL = process.env.PROOF_MODEL ?? "cliproxyapi/gpt-6.1-sol"; // smarty-dev#2236: Claude P0
const log = (...a) => console.log(new Date().toISOString(), ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = { status: "RUNNING", startedAt: new Date().toISOString(), fabricDist, bridgeBin, sshHost };
const children = new Set();
// BEGIN LOOPBACK LIFECYCLE (also extracted by the owner-only probe).
const processIdentity = (pid) => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    return { pid: Number(pid), state: fields[0], group: Number(fields[2]), session: Number(fields[3]), tick: fields[19] };
  } catch (error) { if (error.code === "ENOENT" || error.code === "ESRCH") return undefined; throw error; }
};
const ownChild = (child, label, requireCleanExit) => {
  const identity = processIdentity(child.pid);
  assert(identity && identity.group === child.pid && identity.session === child.pid, `${label}: not an owned detached group`);
  const owner = { child, label, identity, requireCleanExit, known: new Map([[child.pid, identity.tick]]), closed: false };
  child.once("error", (error) => { owner.error = String(error); });
  child.stdin?.on?.("error", (error) => { owner.error = `stdin: ${error}`; });
  child.once("close", (code, signal) => { owner.closed = true; owner.code = code; owner.signal = signal; });
  children.add(owner);
  return owner;
};
const ownedMembers = (owner) => {
  const leader = processIdentity(owner.identity.pid);
  assert(!leader || leader.tick === owner.identity.tick, `${owner.label}: owner PID reused`);
  const members = fs.readdirSync("/proc").filter((name) => /^\d+$/.test(name)).map(processIdentity)
    .filter((p) => p && p.group === owner.identity.group && p.session === owner.identity.session);
  for (const p of members) {
    assert(BigInt(p.tick) >= BigInt(owner.identity.tick), `${owner.label}: pre-existing group member`);
    assert(leader || owner.known.get(p.pid) === p.tick || members.some((m) => owner.known.get(m.pid) === m.tick), `${owner.label}: unauthenticated orphan group`);
    if (owner.known.has(p.pid)) assert(owner.known.get(p.pid) === p.tick, `${owner.label}: member PID reused`);
    owner.known.set(p.pid, p.tick);
  }
  return members;
};
const cleanupChild = async (owner, graceMs = 5_000, termMs = 3_000, killMs = 2_000) => {
  const receipt = { label: owner.label, pid: owner.identity.pid, startTick: owner.identity.tick, errors: [], signals: [] };
  const waitDead = async (ms) => {
    const until = Date.now() + ms;
    do {
      if (ownedMembers(owner).length === 0 && owner.closed) return true;
      await sleep(Math.min(50, Math.max(1, until - Date.now())));
    } while (Date.now() < until);
    return ownedMembers(owner).length === 0 && owner.closed;
  };
  try {
    ownedMembers(owner); // Capture descendants before EOF can reap the group leader.
    if (!owner.child.stdin?.destroyed && !owner.child.stdin?.writableEnded) owner.child.stdin?.end();
    let dead = await waitDead(graceMs);
    for (const [signal, ms] of [["SIGTERM", termMs], ["SIGKILL", killMs]]) {
      if (dead) break;
      const members = ownedMembers(owner);
      for (const member of members) {
        const current = processIdentity(member.pid);
        if (!current) continue;
        assert(current.tick === member.tick && current.group === owner.identity.group && current.session === owner.identity.session, "cleanup identity changed");
        try { process.kill(member.pid, signal); receipt.signals.push({ pid: member.pid, signal }); }
        catch (error) { if (error.code !== "ESRCH") throw error; }
      }
      dead = await waitDead(ms);
    }
    receipt.dead = dead;
    assert(dead, `${owner.label}: actual process/group survived cleanup`);
    assert(!owner.error, `${owner.label}: ${owner.error}`);
    if (owner.requireCleanExit) assert(owner.code === 0 && !owner.signal, `${owner.label}: unclean Pi exit ${owner.code}/${owner.signal}`);
  } catch (error) { receipt.errors.push(String(error?.stack ?? error)); }
  receipt.observed = [...owner.known].map(([pid, startTick]) => ({ pid, startTick }));
  receipt.code = owner.code; receipt.signal = owner.signal;
  return receipt;
};
const finalStatus = (proofPassed, cleanup) => proofPassed && cleanup.length > 0 && cleanup.every((r) => r.dead === true && r.errors.length === 0) ? "PASS" : "FAIL";
const rpcReady = (request) => request("get_state", 120_000);
// END LOOPBACK LIFECYCLE
const saveAfterKill = () => { if (results.bridgeKilledAt) fs.writeFileSync(path.join(scratch, "after-kill.json"), JSON.stringify(results, null, 2)); };
let finishing;
const finish = (error) => finishing ??= (async () => {
  if (error) results.failed = String(error?.stack ?? error);
  results.cleanup = await Promise.all([...children].map((owner) => cleanupChild(owner)));
  results.status = finalStatus(results.proofPassed === true && !results.failed, results.cleanup);
  results.finishedAt = new Date().toISOString();
  save(); saveAfterKill();
  if (results.status === "FAIL") console.error("FAILED", results.failed, results.cleanup);
  process.exit(results.status === "PASS" ? 0 : 1);
})();
const fail = (error) => { results.failed = String(error?.stack ?? error); return finish(error); };
process.on("unhandledRejection", fail);
process.on("uncaughtException", fail);
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
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(PI_|HERDR|FABRIC|SMARTY_ROLE$)/.test(k)));
  env.PI_CODING_AGENT_DIR = s.agentDir;
  // ponytail: proof calls perform one explicit native provider operation, not
  // general work-agent work. Retain context/profile/trust/mesh; allowlist the
  // extension tool and suppress optional skill discovery, not context files.
  const child = spawn("timeout", ["--kill-after=10s", "900s", "nice", "-n", "19", "pi", "--mode", "rpc", "-ne", "-e", fabricDist, "--tools", "fabric_exec", "--no-skills", "--model", MODEL, "--thinking", "low", "--session-dir", path.join(s.root, "sessions")],
    { cwd: s.cwd, env, stdio: ["pipe", "pipe", "pipe"], detached: true });
  const owner = ownChild(child, `${s.name} Pi`, true);
  const file = path.join(s.root, "rpc-stdout.jsonl");
  const out = fs.createWriteStream(file);
  child.stderr.pipe(fs.createWriteStream(path.join(s.root, "rpc-stderr.log")));
  const listeners = new Set();
  let buffer = "";
  child.stdout.on("data", (chunk) => {
    ownedMembers(owner); // Track actual Pi and descendants, not only the timeout wrapper.
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
  const request = (type, timeoutMs = 30_000) => new Promise((resolve, reject) => {
    const id = randomUUID();
    const done = (error, value) => { clearTimeout(timer); listeners.delete(on); child.off("close", dead); child.off("error", dead); error ? reject(error) : resolve(value); };
    const dead = () => done(new Error(`${s.name}: child died during ${type}`));
    const on = (record) => { if (record.type === "response" && record.id === id) done(record.success === false ? new Error(record.error) : null, record.data); };
    const timer = setTimeout(() => done(new Error(`${s.name}: ${type} timed out`)), timeoutMs);
    listeners.add(on);
    child.once("close", dead); child.once("error", dead);
    if (owner.closed || owner.error) return dead();
    child.stdin.write(JSON.stringify({ id, type }) + "\n", (error) => { if (error) done(error); });
  });
  const ready = rpcReady(request);
  const prompt = async (program, timeoutMs = 600_000) => {
    await ready; // Explicit cold extension binding budget, not a fixed sleep/30s guess.
    const deadline = Date.now() + timeoutMs;
    const remaining = () => { const ms = deadline - Date.now(); assert(ms > 0, `${s.name}: operation deadline`); return ms; };
    await waitFor(`${s.name} idle`, async () => !(await request("get_state", Math.min(30_000, remaining()))).isStreaming, Math.min(60_000, remaining()));
    const token = randomUUID();
    const code = `const value = await (async () => { ${program}\n})(); return { proofToken: ${JSON.stringify(token)}, value };`;
    const message = `Call fabric_exec exactly once with this exact code, resultFormat "json", and timeoutMs ${Math.min(timeoutMs, 180_000)}. Do not call any other tool. Then reply only OK.\n\n${code}`;
    const evidence = promptEvidence(code, token, message);
    results.toolExecutions ??= [];
    results.toolExecutions.push({ side: s.name, code, token, records: evidence.records });
    return new Promise((resolve, reject) => {
      const id = randomUUID();
      let completed = false;
      const done = (error, value) => { if (completed) return; completed = true; clearTimeout(timer); listeners.delete(on); child.off("close", dead); child.off("error", dead); save(); error ? reject(error) : resolve(value); };
      const dead = () => done(new Error(`${s.name}: child died during requested run`));
      const on = (record) => {
        try {
          if (record.type === "response" && record.id === id && record.success === false) throw new Error(record.error);
          const value = evidence.accept(record);
          if (value !== undefined) done(null, value);
        } catch (error) { done(error); }
      };
      const timer = setTimeout(() => done(new Error(`${s.name}: prompt/tool timed out`)), remaining());
      listeners.add(on);
      child.once("close", dead); child.once("error", dead);
      if (owner.closed || owner.error) return dead();
      child.stdin.write(JSON.stringify({ id, type: "prompt", message }) + "\n", (error) => { if (error) done(error); });
    });
  };
  // The <fabric-agent-message ... delivery="..."> (or inbox) block that carried a needle into this session.
  const received = async (needle, customType, id, sender, delivery) => {
    const raw = fs.readFileSync(file, "utf8");
    const messages = (await request("get_messages"))?.messages ?? [];
    for (const message of messages) {
      try { return assertNative(message, needle, customType, id, sender, delivery); } catch {}
    }
    for (const line of raw.split("\n")) {
      if (!line.includes(needle)) continue;
      let record;
      try { record = JSON.parse(line); } catch { continue; }
      if (record.type !== "message_end" || !record.message || record.message.role === "assistant" || record.message.role === "toolResult") continue;
      const { content } = record.message;
      const text = typeof content === "string" ? content : (content ?? []).map((c) => c.text ?? "").join("");
      if (!text.includes(needle)) continue;
      try { return assertNative(record.message, needle, customType, id, sender, delivery); } catch {}
    }
    return undefined;
  };
  return { child, prompt, received, ready };
};

const roots = (s, mirrored) => s.store.listAll("topology/participants/", { fresh: true })
  .map((e) => e.value).filter((v) => v?.kind === "root" && (mirrored ? v.remoteHost : !v.remoteHost));
const waitFor = async (what, check, ms = 120_000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) { const v = await check(); if (v) return v; await sleep(250); }
  throw new Error(`timed out waiting for ${what}`);
};
const run = (code) => code;

try {
({ MeshStore } = await import(path.join(path.dirname(fabricDist), "../src/mesh/store.ts")));
const A = side("dev1"), B = side("forge");
const piA = startPi(A), piB = startPi(B);
await Promise.all([piA.ready, piB.ready]);
results.bootstrap = await Promise.all([piA, piB].map((pi) => pi.prompt(run("const main = await agents.main();\nreturn { id: main.id, local: main.local };"))));
const [rootA] = await waitFor("dev1 root", () => roots(A, false)[0] && roots(A, false));
const [rootB] = await waitFor("forge root", () => roots(B, false)[0] && roots(B, false));
results.roots = { dev1: rootA.id, forge: rootB.id };
for (const [index, root] of [rootA, rootB].entries()) assert(results.bootstrap[index]?.value?.id === root.id && results.bootstrap[index].value.local === true, "native Main bootstrap mismatch");
log("roots", rootA.id, rootB.id);

// The bridge: the hub side on dev1's mesh; the transport is the bridge's own ssh argv to the loopback
// sshd, whose forced command runs `mesh-bridge agent --mesh <forge mesh> --peer dev1`.
const bridgeLog = path.join(scratch, "bridge.log");
const bridge = spawn("node", [bridgeBin, "run", "--mesh", A.mesh, "--name", "dev1", "--remote", "forge",
  "--cursor", path.join(scratch, "bridge-cursor.json"), "--ssh", sshHost, "--ssh-key", sshKey, "--ssh-port", sshPort, "--ssh-known-hosts", knownHosts],
  { stdio: ["ignore", "inherit", fs.openSync(bridgeLog, "a")], detached: true });
const bridgeOwner = ownChild(bridge, "bridge", false);
await waitFor("forge root mirrored into dev1", () => roots(A, true).find((r) => r.id === rootB.id && r.remoteHost === "forge"));
await waitFor("dev1 root mirrored into forge", () => roots(B, true).find((r) => r.id === rootA.id && r.remoteHost === "dev1"));
results.transport = execFileSync("ps", ["-o", "pid,args", "--ppid", String(bridge.pid)], { encoding: "utf8" }).trim();
log("mirrors present both ways; transport:", results.transport);

// 1. Discovery with host set, then steer and followUp dev1 -> forge.
const tag = () => randomUUID().slice(0, 8);
const n = { s1: `LOOP-STEER-D2F-${tag()}`, f1: `LOOP-FOLLOWUP-D2F-${tag()}`, s2: `LOOP-STEER-F2D-${tag()}`, f2: `LOOP-FOLLOWUP-F2D-${tag()}` };
results.dev1Turn = await piA.prompt(run(
  `const peers = await agents.peers();\nconst steer = await agents.steer(${JSON.stringify(rootB.id)}, ${JSON.stringify(`${n.s1}: reply with just OK`)});\nconst followUp = await agents.followUp(${JSON.stringify(rootB.id)}, ${JSON.stringify(`${n.f1}: reply with just OK`)});\nreturn { peers: peers.filter((p) => p.host !== undefined).map((p) => ({ id: p.id, host: p.host, label: p.label })), steer, followUp };`));
assertTurn(results.dev1Turn, rootB.id, "forge");
results.forgeReceived = { steer: await waitFor("forge got the steer", () => piB.received(n.s1, "pi-fabric-agent-message", results.dev1Turn.value.steer.messageId, rootA.id, "steer"), 180_000), followUp: await waitFor("forge got the followUp", () => piB.received(n.f1, "pi-fabric-agent-message", results.dev1Turn.value.followUp.messageId, rootA.id, "followUp"), 240_000) };
log("forge received", JSON.stringify(results.forgeReceived));
await sleep(5_000);

// 2. The other direction: forge discovers dev1, steers and followUps it.
results.forgeTurn = await piB.prompt(run(
  `const peers = await agents.peers();\nconst steer = await agents.steer(${JSON.stringify(rootA.id)}, ${JSON.stringify(`${n.s2}: reply with just OK`)});\nconst followUp = await agents.followUp(${JSON.stringify(rootA.id)}, ${JSON.stringify(`${n.f2}: reply with just OK`)});\nreturn { peers: peers.filter((p) => p.host !== undefined).map((p) => ({ id: p.id, host: p.host, label: p.label })), steer, followUp };`));
assertTurn(results.forgeTurn, rootA.id, "dev1");
results.dev1Received = { steer: await waitFor("dev1 got the steer", () => piA.received(n.s2, "pi-fabric-agent-message", results.forgeTurn.value.steer.messageId, rootB.id, "steer"), 180_000), followUp: await waitFor("dev1 got the followUp", () => piA.received(n.f2, "pi-fabric-agent-message", results.forgeTurn.value.followUp.messageId, rootB.id, "followUp"), 240_000) };
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
for (const [key, original] of Object.entries({ prWake, laneWake, work: workEvent })) assertWake(onForge[key], original, rootB.id);
results.wakesOnForgeMesh = Object.fromEntries(Object.entries(onForge).map(([k, e]) => [k, { sequence: e.sequence, topic: e.topic, kind: e.kind, from: e.from.id, to: e.to, bridge: e.data.bridge }]));
log("wakes on forge mesh", JSON.stringify(results.wakesOnForgeMesh));
results.forgeWokeByLaneWake = await waitFor("forge Main woken by the lane followUp", () => piB.received(lane, "pi-fabric-agent-message", undefined, "factory-host:stuck-work", "followUp"), 180_000);
log("lane wake received", JSON.stringify(results.forgeWokeByLaneWake));
results.forgeWokeByWorkInbox = await waitFor("forge Main woken by fleet.work (root inbox)", () => piB.received(work, "pi-fabric-inbox", onForge.work.id), 240_000);
log("work inbox", JSON.stringify(results.forgeWokeByWorkInbox));
await sleep(5_000);
// The forge Main reads the pr.wake addressed to it (ops.owner has no Main handler; it is a supervisor's input).
results.forgeReadsPrWake = await piB.prompt(run(
  `const self = (await agents.main()).id;\nconst events = await mesh.read({ topic: "ops.owner", to: self, limit: 50 });\nreturn { self, events }; `));
assert(results.forgeReadsPrWake.value?.self === rootB.id, "pr.wake read wrong Main");
const readWake = results.forgeReadsPrWake.value?.events?.find((e) => e.data?.bridge?.id === prWake.id);
assertWake(readWake, prWake, rootB.id);
log("forge read pr.wake", JSON.stringify(readWake));
save();

// 4. Kill the bridge (SIGKILL: no clean withdrawal). Both Mains then try to reach the other side,
// once a second, and record every attempt with its duration until the named lapse error.
ownedMembers(bridgeOwner);
assert(processIdentity(bridge.pid)?.tick === bridgeOwner.identity.tick, "bridge identity changed before kill");
assert(bridge.kill("SIGKILL"), "bridge SIGKILL failed");
const killedAt = Date.now();
results.bridgeKilledAt = new Date(killedAt).toISOString();
const probe = (target) => run(
  `const killedAt = ${killedAt};\nconst attempts = [];\nfor (let i = 0; i < 60 && Date.now() - killedAt < 150_000; i++) {\n  const t = Date.now();\n  try { const r = await agents.steer(${JSON.stringify(target)}, "after the bridge died"); attempts.push({ sAfterKill: (t - killedAt) / 1000, ms: Date.now() - t, delivered: r }); }\n  catch (error) { const message = String(error?.message ?? error); attempts.push({ sAfterKill: (t - killedAt) / 1000, ms: Date.now() - t, error: message }); if (/lapsed/.test(message)) break; }\n  await new Promise((r) => setTimeout(r, 1000));\n}\nreturn attempts;`);
const after = await Promise.allSettled([piA.prompt(probe(rootB.id), 180_000), piB.prompt(probe(rootA.id), 180_000)]);
results.afterKill = Object.fromEntries(after.map((entry, i) => [["dev1ToForge", "forgeToDev1"][i], entry.status === "fulfilled" ? entry.value : { error: String(entry.reason?.stack ?? entry.reason) }]));
save(); saveAfterKill();
await sleep(3_000);
// pgrep exits 1 when nothing matches; any other failure is an error, not "none".
const regexEscape = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
results.processesAfterKill = Object.fromEntries([["bridge or agent", `mesh-bridge (run|agent) --mesh ${regexEscape(A.mesh)}|agent --mesh ${regexEscape(B.mesh)}`], ["ssh transport", `(^|/)ssh .*${regexEscape(sshKey)}`]].map(([what, pattern]) => {
  try { return [what, execFileSync("pgrep", ["-af", "--", pattern], { encoding: "utf8" }).trim()]; }
  catch (error) { if (error.status === 1) return [what, "none"]; throw error; }
}));
results.bridgeLog = fs.readFileSync(bridgeLog, "utf8");
save(); saveAfterKill();
assert(after.every((entry) => entry.status === "fulfilled"), "required post-kill prompt failed");
assertLapse(results.afterKill.dev1ToForge, rootB.id, "forge");
assertLapse(results.afterKill.forgeToDev1, rootA.id, "dev1");
assertNoSurvivors(results.processesAfterKill);
log("RESULTS", JSON.stringify(results.afterKill, null, 2), results.processesAfterKill);
results.proofPassed = true;
} catch (error) { results.failed = String(error?.stack ?? error); }
finally { await finish(); }
