// #9569 T11: real built baseline/candidate public APIs, plain Node, hosted Windows only.
// expected-skip: non-Windows/non-hosted: actual NTFS old/new destructive schedule is hosted-only.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { pathToFileURL } from "node:url";

if (process.platform !== "win32" || process.env.GITHUB_ACTIONS !== "true" || process.versions.bun) {
  throw new Error("T11 fixture requires plain Node on hosted Windows; never run locally");
}
const [projectArg, rootArg, controlArg, role, protocolArg] = process.argv.slice(2);
if (!["old-release", "current-release", "holder", "contender"].includes(role) || !["1", "2"].includes(protocolArg)) {
  throw new Error("invalid T11 fixture role/protocol");
}
const project = fs.realpathSync.native(projectArg);
const root = fs.realpathSync.native(rootArg);
const control = fs.realpathSync.native(controlArg);
const lock = path.join(root, ".lock"), owner = path.join(lock, "owner");
const read = fs.readFileSync, rename = fs.renameSync;
const execFile = childProcess.execFile, spawnSync = childProcess.spawnSync;
const asyncChildren = [], nativeTerminals = [];
// Observe, never substitute, the real incarnation-reader children and synchronous native helpers. Even if the
// public reader returns at its evidence deadline, finally awaits the actual close of every owned async child.
childProcess.execFile = function (file) {
  const child = execFile.apply(this, arguments);
  let error;
  child.once("error", caught => { error = caught.message; });
  asyncChildren.push(new Promise(resolve => child.once("close", (code, signal) => {
    nativeTerminals.push({ executable: String(file), pid: child.pid, code, signal, error, joinedAtUtc: new Date().toISOString(), mode: "async-close" });
    resolve();
  })));
  return child;
};
childProcess.spawnSync = function (file) {
  const terminal = spawnSync.apply(this, arguments); // returns only after the actual native process has ended
  nativeTerminals.push({ executable: String(file), pid: terminal.pid, code: terminal.status, signal: terminal.signal,
    error: terminal.error?.message, joinedAtUtc: new Date().toISOString(), mode: terminal.pid > 0 ? "sync-joined" : "not-started" });
  return terminal;
};
syncBuiltinESMExports();
const digest = file => createHash("sha256").update(read(file)).digest("hex");
const identity = file => {
  const s = fs.lstatSync(file, { bigint: true });
  return { dev: String(s.dev), ino: String(s.ino), size: String(s.size), mtimeNs: String(s.mtimeNs) };
};
const snapshot = () => ({ directory: identity(lock), owner: identity(owner), record: read(owner, "utf8") });
const publish = (name, value) => {
  const ready = path.join(control, `${name}.ready`);
  fs.writeFileSync(ready, JSON.stringify(value), { flag: "wx" });
  rename(ready, path.join(control, `${name}.json`));
};
// The real operation is synchronous, so its owned child cannot service IPC while paused. The closed go file is
// the event source; this test-only 10ms bounded observation loop is the safety net. No production timer is added.
const pause = (name, detail) => {
  publish(name, { pid: process.pid, role, ...detail });
  const end = performance.now() + 45_000;
  const clock = new Int32Array(new SharedArrayBuffer(4));
  while (!fs.existsSync(path.join(control, "go"))) {
    if (performance.now() >= end) throw new Error(`T11 ${name} checkpoint deadline`);
    Atomics.wait(clock, 0, 0, 10);
  }
};

let store, hooks, armed = false, paused = false;
let entered = 0;
const samples = [];
let result = { pid: process.pid, role, protocol: Number(protocolArg), project, entered: 0, status: "failed" };
try {
  const meshEntry = path.join(project, "dist", "mesh.js");
  const helper = path.join(project, "dist", "native", "fabric-win-remove.exe");
  result.artifacts = { meshEntry, meshSha256: digest(meshEntry), packagedHelperSha256: digest(helper),
    node: process.execPath, nodeSha256: digest(process.execPath), nodeVersion: process.version };
  const { MeshStore } = await import(pathToFileURL(meshEntry).href);
  const hookEntry = path.join(project, "dist", "storage", "windows-mesh-release.js");
  if (role !== "old-release" && fs.existsSync(hookEntry)) {
    hooks = (await import(pathToFileURL(hookEntry).href)).windowsMeshReleaseTestHooks;
    // This is the real compiled cache observer, not an outcome override or a stand-in helper.
    hooks.cacheRoot = path.join(control, "cache");
    hooks.afterChild = sample => {
      samples.push({ ...sample, observedHelperSha256: digest(sample.absolutePath) });
    };
  }
  if (role !== "old-release" && process.env.FABRIC_9569_ROLLOUT_CHILD_CURRENT === "true" && !hooks) {
    throw new Error("candidate stable compiled release observer missing");
  }
  if (role === "old-release") {
    if (fs.existsSync(hookEntry)) throw new Error("old role must use the actual pre-fix pinned baseline, not current dist");
    // All old release checks HAVE run before reaching its real canonical-source rename. Pass every call through,
    // including this rename after go. No source patch, fake record/result, helper switch or hand-written old release.
    fs.renameSync = function (from, to) {
      if (armed && !paused && String(from) === lock && String(to).startsWith(`${lock}.released.`)) {
        paused = true;
        pause("release-paused", { boundary: "old-after-real-check-before-path-rename", from: String(from), to: String(to), snapshot: snapshot() });
      }
      return rename.apply(this, arguments);
    };
  } else if (role === "current-release") {
    fs.readFileSync = function (file) {
      const bytes = read.apply(this, arguments);
      if (armed && !paused && String(file).startsWith(path.join(control, "cache") + path.sep) && path.basename(String(file)) === "fabric-win-remove.exe") {
        paused = true;
        // The retained-helper rehash follows the real W1 snapshot/full-record check. Return the real executable
        // bytes unchanged, then let the actual native guard judge the now-stale original identity/record.
        pause("release-paused", { boundary: "current-after-real-check-before-native-guard", helper: String(file), snapshot: snapshot() });
      }
      return bytes; // exact real bytes; native identity/full-record verification remains in charge
    };
  }
  store = new MeshStore(root, 64 * 1024, 500, { lockProtocol: Number(protocolArg), lockTimeoutMs: role === "contender" ? 800 : 5_000 });
  publish("started", { pid: process.pid, role, artifacts: result.artifacts });
  try {
    await store.exclusive(() => {
      entered++;
      result.entry = snapshot();
      if (role === "holder") pause("holding", { snapshot: result.entry });
      armed = true;
    });
    if (role === "contender") throw new Error("contender entered a held canonical lock");
    if (["old-release", "current-release"].includes(role) && !paused) throw new Error("real public release checkpoint was not reached");
    result.status = "completed";
  } catch (error) {
    // A genuine public timeout is the only admitted contender outcome, not an injected refusal.
    if (role === "contender" && entered === 0 && error?.code === "FABRIC_MESH_LOCK_TIMEOUT") {
      result.status = "excluded";
      result.refusal = { name: error.name, code: error.code, message: error.message };
    } else throw error;
  }
} catch (error) {
  result.error = String(error?.stack ?? error);
  process.exitCode = 1;
} finally {
  fs.readFileSync = read; fs.renameSync = rename;
  try { store?.closeState(); } catch (error) { result.error = `${result.error ?? ""}\ncloseState: ${error}`; process.exitCode = 1; }
  if (hooks) { delete hooks.afterChild; delete hooks.cacheRoot; }
  await Promise.all(asyncChildren); // no fail-fast escape, and no PID-scan substitute for process-handle close
  childProcess.execFile = execFile; childProcess.spawnSync = spawnSync; syncBuiltinESMExports();
  result = { ...result, entered, samples, nativeTerminals, finishedAtUtc: new Date().toISOString() };
  publish("result", result); // result is diagnostic only: the parent STILL waits for actual process close
}
