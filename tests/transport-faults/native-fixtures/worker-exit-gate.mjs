import fs from "node:fs";
import path from "node:path";

// Fault injection only: pause the REAL compiled worker immediately after its
// first receipt-bound terminal status rename, before native/tree close. Unlike
// beforeExit, this boundary also works with a live IPC handle/native Pi child.
// Every inherited NODE_OPTIONS preload (including deletion guard) stays intact.
const gate = process.env.FABRIC_NATIVE_WORKER_EXIT_GATE;
if (gate && path.basename(process.argv[1] ?? "") === "worker.js") {
  const rename = fs.renameSync;
  fs.renameSync = (source, target) => {
    rename(source, target);
    if (path.basename(String(target)) !== "status.json" || fs.existsSync(gate)) return;
    const record = JSON.parse(fs.readFileSync(target, "utf8"));
    if (record.status !== "completed" || !record.finalAnswerReceipt?.id) return;
    fs.writeFileSync(gate, JSON.stringify({ pid: process.pid, runId: record.id,
      statusFile: String(target), receiptId: record.finalAnswerReceipt.id }), { flag: "wx", mode: 0o600 });
    // One finite fault hold, not a poll or a cleanup mechanism. Tests inject a
    // birth-checked SIGKILL; the production transport owns the remaining tree.
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 90_000);
  };
}
