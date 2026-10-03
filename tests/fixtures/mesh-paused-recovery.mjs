import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, role, phase, order] = process.argv.slice(2);
const lock = path.join(root, ".lock");
const ownerPath = path.join(lock, "owner");
const write = fs.writeFileSync.bind(fs);
const rename = fs.renameSync.bind(fs);
const rmdir = fs.rmdirSync.bind(fs);
const timer = globalThis.setTimeout;
const wait = (file) => {
  // Only the recovery-first test owns cancellation and joins both children.
  // Its handshake must not race a separate fixture clock while the test parent
  // is descheduled. Keep the existing guard for the other fixture consumers.
  const ownedRecovery = order === "recovery-first" && (role === "initializer" || role === "successor");
  const deadline = ownedRecovery ? Infinity : Date.now() + 10_000;
  const parent = process.ppid;
  while (!fs.existsSync(file)) {
    if (Date.now() >= deadline) throw new Error(`Timed out awaiting owned ${role} fixture signal: ${file}`);
    try { process.kill(parent, 0); } catch { throw new Error(`Owned ${role} fixture lost its parent`); }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
};
let armed = true;
let ran = false;
let boundary;
let replacement;
if (role === "initializer") {
  fs.writeFileSync = (file, data, options) => {
    if (!armed || String(file) !== ownerPath) return write(file, data, options);
    armed = false;
    // "opened" holds the actual exclusive-create descriptor before any owner bytes.
    const fd = phase === "opened" ? fs.openSync(file, options.flag, options.mode) : undefined;
    try {
      write(path.join(root, "initializer.ready"), "");
      wait(path.join(root, "initializer.go"));
      return write(fd ?? file, data, options);
    } finally { if (fd !== undefined) fs.closeSync(fd); }
  };
} else if (role === "successor") {
  fs.writeFileSync = (file, data, options) => {
    if (armed && String(file) === ownerPath) {
      armed = false;
      // Recovery has acquired the empty successor. Join the original's rejected
      // publication/cleanup before attempting our own exclusive owner create.
      const stat = fs.lstatSync(lock);
      replacement = { dev: stat.dev, ino: stat.ino };
      write(path.join(root, "initializer.go"), "");
      wait(path.join(root, "initializer.finished"));
    }
    return write(file, data, options);
  };
} else {
  const pause = (at) => {
    armed = false;
    boundary = at;
    write(path.join(root, "recoverer.ready"), JSON.stringify({ boundary }));
    wait(path.join(root, "recoverer.go"));
  };
  fs.rmdirSync = (file, options) => {
    if (armed && String(file) === lock) pause("last-comparison-before-detach");
    return rmdir(file, options);
  };
  fs.renameSync = (from, to) => {
    if (armed && String(from) === lock && String(to).startsWith(`${lock}.dead.`)) {
      // Baseline: every owner/inode comparison has finished. Pause the ACTUAL detach,
      // not an earlier owner read, then let the original initializer enter first.
      pause("last-comparison-before-detach");
    }
    return rename(from, to);
  };
  globalThis.setTimeout = (...args) => {
    if (armed) {
      // An opened (empty/torn) owner file fails closed before removal. Pause that
      // denied retry so both phases exercise the same initializer-first order.
      pause("refused-before-detach");
      const now = Date.now.bind(Date);
      Date.now = () => now() + 5_001;
    }
    return timer(...args);
  };
}
// Intentionally omit lockProtocol: these children exercise the public default-v1 path.
const store = new MeshStore(root, 65536, 100, { lockTimeoutMs: 1_000 });
const result = await store.exclusive(() => {
  ran = true;
  if (role === "initializer") {
    const stat = fs.lstatSync(lock);
    write(path.join(root, "initializer.entered"), JSON.stringify({ owner: fs.readFileSync(ownerPath, "utf8"), ino: stat.ino, dev: stat.dev }));
    wait(path.join(root, "initializer.release"));
  }
}).catch(error => error);
const summary = { role, phase, boundary, ran, replacement, timeout: result?.code === "FABRIC_MESH_LOCK_TIMEOUT", code: result?.code };
if (role === "initializer" && order === "recovery-first") {
  // Keep the rejected holder genuinely alive while the parent checks immediate progress.
  write(path.join(root, "initializer.finished"), JSON.stringify(summary));
  wait(path.join(root, "initializer.release"));
}
console.log(JSON.stringify(summary));
