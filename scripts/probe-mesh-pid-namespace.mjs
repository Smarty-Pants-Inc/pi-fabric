#!/usr/bin/env node
// Real kernel PID-namespace acceptance probe through the compiled public mesh entry.
// No fabricated receipt/namespace identity, production roots, or external credentials.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MeshStore } from "../dist/mesh.js";

const script = fileURLToPath(import.meta.url);
const nativeNow = Date.now.bind(Date);
const namespace = () => fs.readlinkSync("/proc/self/ns/pid");
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const write = (file, value) => {
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(value, null, 2));
  fs.renameSync(`${file}.tmp`, file);
};
const waitFile = async file => {
  const deadline = nativeNow() + 20_000;
  while (!fs.existsSync(file)) {
    assert(nativeNow() < deadline, `Timed out awaiting ${file}`);
    await sleep(10);
  }
  return JSON.parse(fs.readFileSync(file, "utf8"));
};
const identity = id => ({ id, name: id, kind: "main" });
const mesh = c => new MeshStore(c.root, 65536, 100, { lockProtocol: c.protocol, lockTimeoutMs: 400 });
const owned = new Set();
const stop = child => {
  if (child.process.exitCode === null && child.process.signalCode === null) {
    try { process.kill(child.group ? -child.process.pid : child.process.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
  }
};
const start = (c, isolated = false) => {
  const args = [script, "--worker", JSON.stringify(c)];
  let binary = process.execPath, argv = args;
  if (isolated && !c.selfTest) {
    if (c.launcher === "bwrap") {
      binary = "bwrap";
      argv = ["--unshare-pid", "--dev-bind", "/", "/", "--proc", "/proc", process.execPath, ...args];
    } else {
      binary = "unshare";
      argv = ["--user", "--map-root-user", "--pid", "--fork", "--kill-child=SIGKILL", "--mount-proc", process.execPath, ...args];
    }
  }
  const fd = fs.openSync(`${c.result}.log`, "w", 0o600);
  // Nested crash holders stay in their owned supervisor's process group, so
  // top-level cleanup also kills them if the supervisor itself is terminated.
  const group = process.argv[2] !== "--worker";
  const processChild = spawn(binary, argv, { detached: group, stdio: ["ignore", fd, fd] });
  fs.closeSync(fd);
  const child = { process: processChild, group, done: undefined };
  child.done = new Promise((resolve, reject) => {
    processChild.once("error", reject);
    processChild.once("close", (code, signal) => resolve({ code, signal }));
  });
  child.done.catch(() => undefined);
  owned.add(child);
  return child;
};
const finish = async child => {
  const result = await child.done;
  owned.delete(child);
  assert.equal(result.code, 0, `Worker failed: ${JSON.stringify(result)}`);
};

const worker = async c => {
  const store = mesh(c);
  if (c.action === "hold") {
    const rename = fs.renameSync.bind(fs);
    let armed = true;
    fs.renameSync = (from, to) => {
      if (armed && String(to) === path.join(c.root, "state.json")) {
        armed = false;
        const owner = fs.readFileSync(path.join(c.root, ".lock", "owner"), "utf8");
        assert.equal(owner.split("\n")[1], String(process.pid));
        assert.equal(owner.split("\n")[4], namespace());
        write(c.ready, { namespace: namespace(), pid: process.pid, owner, procStatus: fs.readFileSync("/proc/self/status", "utf8") });
        const deadline = nativeNow() + 25_000;
        while (!fs.existsSync(c.release)) {
          assert(nativeNow() < deadline, "Holder release deadline exceeded");
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
        }
      }
      return rename(from, to);
    };
    await store.put({ key: `probe/${c.id}`, value: "committed", identity: identity(c.id) });
  } else if (c.action === "timeout") {
    // Advance only the waiter's clock past the deleted expiry bound, never alter
    // kernel identity or the holder's real receipt. The holder is actually stopped.
    Date.now = () => nativeNow() + 121_000;
    let error;
    try { await store.put({ key: `probe/${c.id}`, value: "must not commit", identity: identity(c.id) }); }
    catch (caught) { error = caught; }
    Date.now = nativeNow;
    assert.equal(error?.code, "FABRIC_MESH_LOCK_TIMEOUT");
    assert.match(error.message, c.selfTest ? /held by pid/ : /foreign pid namespace/);
    assert.equal(store.get(`probe/${c.id}`), undefined);
    write(c.result, { namespace: namespace(), timeout: error.code, diagnostic: error.message });
    return;
  } else if (c.action === "dead-recovery") {
    const holderConfig = { ...c, id: `${c.id}-dead`, action: "hold", ready: `${c.result}.ready`, release: `${c.result}.release`, result: `${c.result}.holder` };
    const child = start(holderConfig); // stays in THIS actual PID namespace
    try {
      const held = await waitFile(holderConfig.ready);
      assert.equal(held.namespace, namespace());
      stop(child); await child.done; owned.delete(child);
      await store.put({ key: `probe/${c.id}`, value: "recovered", identity: identity(c.id) });
      assert.equal(store.get(`probe/${c.id}`)?.value, "recovered");
    } finally { stop(child); await child.done; owned.delete(child); }
  } else if (c.action === "contend") {
    for (let i = 0; i < 40; i++) {
      await store.exclusive(() => {
        const sentinel = path.join(c.root, "probe-critical-section");
        fs.writeFileSync(sentinel, c.id, { flag: "wx" }); // EEXIST means overlap
        try {
          fs.appendFileSync(path.join(c.root, "probe-journal.jsonl"), JSON.stringify({ id: c.id, index: i, namespace: namespace() }) + "\n");
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2);
        } finally { fs.unlinkSync(sentinel); }
      }, 400);
      await store.put({ key: `probe/${c.id}`, value: i, identity: identity(c.id) });
      await store.publish({ topic: "probe.contention", kind: "probe", from: identity(c.id), data: { i } });
    }
  } else {
    await store.put({ key: `probe/${c.id}`, value: "committed", identity: identity(c.id) });
  }
  write(c.result, { namespace: namespace(), action: c.action, completed: true });
};

const main = async () => {
  assert.equal(process.platform, "linux", "Linux with procfs required");
  const outIndex = process.argv.indexOf("--out");
  assert(outIndex >= 0 && process.argv[outIndex + 1], "Usage: node scripts/probe-mesh-pid-namespace.mjs --out ABSOLUTE_DIR [--launcher unshare|bwrap] [--self-test]");
  const out = path.resolve(process.argv[outIndex + 1]); fs.mkdirSync(out, { recursive: true });
  assert.equal(fs.readdirSync(out).length, 0, "Output directory must be empty: stale handshake/result files are not proof");
  const launcherIndex = process.argv.indexOf("--launcher");
  const launcher = launcherIndex < 0 ? "unshare" : process.argv[launcherIndex + 1];
  assert(["unshare", "bwrap"].includes(launcher), "Unsupported launcher");
  const selfTest = process.argv.includes("--self-test");
  const evidence = { realPidNamespaceProof: false, requestedRealPidNamespace: !selfTest, launcher, hostNamespace: namespace(), protocols: [], completed: false };
  const roots = [];
  try {
    for (const protocol of [1, 2]) {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nspid-proof-")); roots.push(root);
      const c = (id, action, extra = {}) => ({ root, protocol, launcher, selfTest, id, action, result: path.join(out, `p${protocol}-${id}.json`), ...extra });
      const run = async (config, isolated = false) => {
        const child = start(config, isolated);
        try { await finish(child); return await waitFile(config.result); }
        finally { stop(child); await child.done; owned.delete(child); }
      };
      const store = mesh(c("parent", "write"));
      const proof = { protocol, directions: [], contention: undefined, deadRecovery: undefined };
      evidence.protocols.push(proof);
      for (const sandboxHolder of [true, false]) {
        const id = sandboxHolder ? "sandbox-holder" : "host-holder";
        const hc = c(id, "hold", { ready: path.join(out, `p${protocol}-${id}.ready.json`), release: path.join(root, `${id}.release`) });
        const holder = start(hc, sandboxHolder);
        try {
          const ready = await waitFile(hc.ready);
          assert.equal(ready.namespace === namespace(), selfTest || !sandboxHolder);
          process.kill(-holder.process.pid, "SIGSTOP");
          const record = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8");
          const timeout = await run(c(`${id}-waiter`, "timeout"), !sandboxHolder);
          assert.equal(timeout.namespace === ready.namespace, selfTest);
          assert.equal(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8"), record);
          fs.writeFileSync(hc.release, ""); process.kill(-holder.process.pid, "SIGCONT");
          await finish(holder);
          const successor = await run(c(`${id}-successor`, "write"), !sandboxHolder);
          assert.equal(store.get(`probe/${id}`)?.value, "committed");
          assert.equal(store.get(`probe/${id}-successor`)?.value, "committed");
          proof.directions.push({ holder: ready, timeout, successor, acknowledgedStateSurvived: true });
        } finally { stop(holder); await holder.done; owned.delete(holder); }
      }
      const hostConfig = c("host-contention", "contend"), sandboxConfig = c("sandbox-contention", "contend");
      const host = start(hostConfig), sandbox = start(sandboxConfig, true);
      try { await Promise.all([finish(host), finish(sandbox)]); }
      finally { stop(host); stop(sandbox); await Promise.all([host.done, sandbox.done]); owned.delete(host); owned.delete(sandbox); }
      const hostResult = await waitFile(hostConfig.result), sandboxResult = await waitFile(sandboxConfig.result);
      assert.equal(hostResult.namespace === sandboxResult.namespace, selfTest);
      const journal = fs.readFileSync(path.join(root, "probe-journal.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
      const events = fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
      assert.equal(journal.length, 80); assert.equal(events.length, 80);
      assert.equal(new Set(events.map(event => event.sequence)).size, 80);
      assert(events.every((event, index) => index === 0 || event.sequence > events[index - 1].sequence));
      assert.equal(store.get("probe/host-contention")?.value, 39);
      assert.equal(store.get("probe/sandbox-contention")?.value, 39);
      write(path.join(out, `p${protocol}-journal.json`), journal);
      write(path.join(out, `p${protocol}-events.json`), events);
      proof.contention = { hostResult, sandboxResult, acquisitions: 80, events: 80, overlaps: 0 };
      proof.deadRecovery = { host: await run(c("host-dead-recovery", "dead-recovery")), sandbox: await run(c("sandbox-dead-recovery", "dead-recovery"), true) };
      // Killing a foreign holder alone is NOT a proof available to MeshStore. Verify
      // the policy does not silently reintroduce unsafe foreign age recovery.
      const dc = c("foreign-dead", "hold", { ready: path.join(out, `p${protocol}-foreign-dead.ready.json`), release: path.join(root, "foreign-dead.release") });
      const dead = start(dc, true);
      try {
        const held = await waitFile(dc.ready); stop(dead); await dead.done; owned.delete(dead);
        assert.equal(held.namespace === namespace(), selfTest);
        if (!selfTest) proof.foreignDeadRemainsProtected = await run(c("foreign-dead-waiter", "timeout"));
        else await store.put({ key: "probe/self-test-recovery", value: true, identity: identity("parent") });
      } finally { stop(dead); await dead.done; owned.delete(dead); }
      write(path.join(out, `p${protocol}-state.json`), JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8")));
    }
    evidence.completed = true;
    evidence.realPidNamespaceProof = !selfTest;
  } catch (error) {
    evidence.failure = String(error?.stack ?? error);
    process.exitCode = 1;
  } finally {
    for (const child of owned) stop(child);
    await Promise.all([...owned].map(child => child.done.catch(() => undefined)));
    for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
    evidence.childrenJoined = true; evidence.isolatedRootsRemoved = true;
    write(path.join(out, "probe-result.json"), evidence);
  }
  console.log(JSON.stringify({ out, ...evidence }, null, 2));
};

if (process.argv[2] === "--worker") {
  try { await worker(JSON.parse(process.argv[3])); }
  finally {
    for (const child of owned) stop(child);
    await Promise.all([...owned].map(child => child.done.catch(() => undefined)));
  }
} else await main();
