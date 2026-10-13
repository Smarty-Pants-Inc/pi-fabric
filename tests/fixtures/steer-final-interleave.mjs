// Deterministic worker-side scheduling only; SDK, receipt, journal and lock are real.
// Loaded only for the private fixture worker, never for the child SDK process.
import fs from "node:fs";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
const root = process.env.STEER_ENDED_ROOT;
if (root && path.basename(process.argv[1] ?? "") === "worker.js") {
  const open = fs.openSync.bind(fs), write = fs.writeFileSync.bind(fs);
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
    if (!fired && path.basename(String(file)) === "steer.jsonl" && (args[0] === "r" || (typeof args[0] === "number" && (args[0] & (fs.constants.O_WRONLY | fs.constants.O_RDWR)) === 0)) && fs.existsSync(armed)) {
      const setting = JSON.parse(fs.readFileSync(armed, "utf8"));
      if (setting.mode === "read") {
        fired = true;
        const before = fs.statSync(file).size;
        fs.appendFileSync(file, JSON.stringify(setting.entry) + "\n");
        fs.appendFileSync(path.join(root, "interleave-log.jsonl"), JSON.stringify({ event: "stat-before-append", bytes: before, pid: process.pid }) + "\n");
        pauseForReceipt(path.dirname(String(file)));
      }
    }
    return open(file, ...args);
  };
  fs.writeFileSync = (file, ...args) => {
    const result = write(file, ...args);
    if (!fired && path.basename(path.dirname(String(file))) === "deliveries" && fs.existsSync(armed)) {
      const setting = JSON.parse(fs.readFileSync(armed, "utf8"));
      if (setting.mode === "inject") {
        fired = true;
        pauseForReceipt(path.dirname(path.dirname(String(file))) + ".ctl");
      }
    }
    return result;
  };
  syncBuiltinESMExports();
}
