import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, role, phase] = process.argv.slice(2);
const lock = path.join(root, ".lock");
const ownerPath = path.join(lock, "owner");
const write = fs.writeFileSync.bind(fs);
const rename = fs.renameSync.bind(fs);
const timer = globalThis.setTimeout;
const wait = (file) => {
  const deadline = Date.now() + 10_000;
  while (!fs.existsSync(file) && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  if (!fs.existsSync(file)) throw new Error(`Timed out awaiting owned ${role} fixture signal: ${file}`);
};
let armed = true;
let ran = false;
let boundary;
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
} else {
  const pause = (at) => {
    armed = false;
    boundary = at;
    write(path.join(root, "recoverer.ready"), JSON.stringify({ boundary }));
    wait(path.join(root, "recoverer.go"));
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
      // Fixed policy has no ownerless detach boundary: pause its first denied retry
      // instead, after recovery returned false, so the same initializer-first order runs.
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
console.log(JSON.stringify({ role, phase, boundary, ran, timeout: result?.code === "FABRIC_MESH_LOCK_TIMEOUT", code: result?.code }));
