// pi-fabric#649 review round 3: SIGKILL a process during compaction's deferred off-lock settle
// of a no-archive dedupe intent. Phase "barrier": at the live-log barrier (before the receipt);
// "unlink": after the durable receipt, before the intent unlink.
import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, phase] = process.argv.slice(2);
const events = path.join(root, "events.jsonl");
const held = () => fs.existsSync(path.join(root, ".lock"));
const die = () => { process.kill(process.pid, "SIGKILL"); };
const sameFile = (fd, file) => {
  try { const a = fs.fstatSync(fd), b = fs.statSync(file); return a.dev === b.dev && a.ino === b.ino; }
  catch { return false; }
};
if (phase === "barrier") {
  const fsync = fs.fsyncSync;
  fs.fsyncSync = fd => { if (!held() && sameFile(fd, events)) die(); return fsync(fd); };
} else {
  const rm = fs.rmSync;
  fs.rmSync = (file, options) => { if (!held() && String(file).endsWith(".pending.json")) die(); return rm(file, options); };
}
const store = new MeshStore(root, 1024, 100, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 });
await store.publish({ topic: "mesh.fsync", from: { id: "session:crash", name: "crash", kind: "main" }, text: "trigger" });
throw new Error("The deferred settle did not reach its crash point");
