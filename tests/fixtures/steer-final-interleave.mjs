// Deterministic worker-side scheduling only; SDK, receipt, journal and lock are real.
// Loaded only for the private fixture worker, never for the child SDK process.
import fs from "node:fs";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
const root = process.env.STEER_ENDED_ROOT;
if (root && process.argv.includes("--steer-file")) {
  const open = fs.openSync.bind(fs), write = fs.writeFileSync.bind(fs), stat = fs.fstatSync.bind(fs);
  const descriptors = new Map();
  const physical = file => fs.realpathSync(String(file));
  const armed = path.join(root, "interleave.json");
  let fired = false;
  const pauseForReceipt = directory => {
    write(path.join(root, "final"), "");
    const deadline = Date.now() + 10000;
    while (!fs.existsSync(path.join(directory, "final-answer.json"))) {
      if (Date.now() >= deadline) throw new Error("Interleaving receipt deadline exceeded");
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
    }
    fs.appendFileSync(path.join(root, "interleave-log.jsonl"), JSON.stringify({ event: "receipt-before-injection", pid: process.pid }) + "\n");
  };
  fs.openSync = (file, ...args) => {
    const fd = open(file, ...args);
    descriptors.delete(fd);
    const actual = physical(process.platform === "linux" ? `/proc/self/fd/${fd}` : file);
    if (path.basename(actual) === "steer.jsonl") descriptors.set(fd, actual);
    return fd;
  };
  // Hook the held FILE's size snapshot, not a directory fd pathname which is
  // already closed when the bounded read starts.
  fs.fstatSync = (fd, ...args) => {
    const before = stat(fd, ...args);
    const file = descriptors.get(fd);
    if (!fired && file && fs.existsSync(armed)) {
      const setting = JSON.parse(fs.readFileSync(armed, "utf8"));
      if (setting.mode === "read") {
        fired = true;
        fs.appendFileSync(file, JSON.stringify(setting.entry) + "\n");
        fs.appendFileSync(path.join(root, "interleave-log.jsonl"), JSON.stringify({ event: "stat-before-append", bytes: Number(before.size), pid: process.pid }) + "\n");
        pauseForReceipt(path.dirname(file));
      }
    }
    return before;
  };
  fs.writeFileSync = (file, ...args) => {
    const result = write(file, ...args);
    const actual = typeof file === "number" ? descriptors.get(file) : physical(file);
    if (!fired && actual && path.basename(path.dirname(actual)) === "deliveries" && fs.existsSync(armed)) {
      const setting = JSON.parse(fs.readFileSync(armed, "utf8"));
      if (setting.mode === "inject") {
        fired = true;
        pauseForReceipt(path.dirname(path.dirname(actual)) + ".ctl");
      }
    }
    return result;
  };
  syncBuiltinESMExports();
}
