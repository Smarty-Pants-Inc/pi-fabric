import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, phase, protocol = "2"] = process.argv.slice(2);
const lockProtocol = Number(protocol);
const lock = path.join(root, ".lock");
const ownerPath = path.join(lock, "owner");
const ready = path.join(root, "paused.ready");
const go = path.join(root, "paused.go");
const resumed = path.join(root, "paused.resumed");
const write = fs.writeFileSync.bind(fs);
const rename = fs.renameSync.bind(fs);
let armed = true;
let ran = false;
let refused = false;
const pause = (resume) => {
  armed = false;
  write(ready, "");
  const deadline = Date.now() + 10_000;
  while (!fs.existsSync(go) && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  if (!fs.existsSync(go)) throw new Error("Timed out awaiting owned fixture resume");
  const owner = fs.readFileSync(ownerPath, "utf8");
  const inode = fs.statSync(lock).ino;
  if (phase === "rename") {
    const now = Date.now.bind(Date);
    Date.now = () => now() + 30_001; // the resumed waiter's deadline has elapsed
  }
  try { resume(); }
  catch (error) { refused = true; throw error; }
  finally {
    const changed = fs.readFileSync(ownerPath, "utf8") !== owner || fs.statSync(lock).ino !== inode || ran;
    write(resumed, "");
    if (changed) throw new Error("Paused writer overwrote or entered a live successor");
  }
};
if (phase === "write" || phase === "opened") {
  fs.writeFileSync = (file, data, options) => {
    if (!armed || path.basename(String(file)) !== "owner") return write(file, data, options);
    if (phase === "opened") {
      // Pause with an empty owner already opened. Recovery detaches this directory;
      // the resumed write lands through the original fd, not the successor's name.
      const fd = fs.openSync(file, options?.flag ?? "w", options?.mode);
      try { return pause(() => write(fd, data, options)); }
      finally { fs.closeSync(fd); }
    }
    return pause(() => write(file, data, options));
  };
} else {
  fs.renameSync = (from, to) => {
    if (!armed || String(to) !== lock) return rename(from, to);
    return pause(() => rename(from, to));
  };
}
const store = new MeshStore(root, 65536, 100, { lockTimeoutMs: 5000, ...(lockProtocol === 2 ? { lockProtocol } : {}) });
const result = await store.exclusive(() => { ran = true; }).catch(error => error);
console.log(JSON.stringify({ ran, refused, timeout: result?.code === "FABRIC_MESH_LOCK_TIMEOUT",
  ownershipLost: result?.code === "FABRIC_MESH_LOCK_OWNERSHIP_LOST" }));
