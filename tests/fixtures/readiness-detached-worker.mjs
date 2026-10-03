// Counterfactual S4 activation: inference is fake, but the escaped native helper is real.
import fs from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
const file = fileURLToPath(import.meta.url);
if (process.argv[2] === "--helper") {
  const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
  fs.writeFileSync(process.env.PI_FABRIC_TEST_READINESS_HELPER, JSON.stringify({
    pid: process.pid, started: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19],
  }));
  const timer = setInterval(() => {
    if (fs.existsSync(`${process.env.PI_FABRIC_TEST_READINESS_HELPER}.release`)) {
      clearInterval(timer);
      process.exit(0);
    }
  }, 20);
} else if (process.argv[2] === "--intermediate") {
  const helper = spawn(process.execPath, [file, "--helper"], { detached: true, stdio: "ignore" });
  helper.unref();
  // Exit before a supervisor's shutdown ancestry sample; the helper is reparented.
} else {
  const intermediate = spawn(process.execPath, [file, "--intermediate"], { detached: true, stdio: "ignore" });
  intermediate.unref();
  await import("./fake-worker.mjs");
}
