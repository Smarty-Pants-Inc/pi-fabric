// Hold the REAL worker's native close after it publishes its terminal result.
// A file gate, rather than a delay, lets the test inspect scratch deterministically.
import fs from "node:fs";
import path from "node:path";

const statusFile = process.argv[process.argv.indexOf("--status-file") + 1];
const releaseFile = path.join(path.dirname(path.dirname(statusFile)), "release-native-close");
const hold = setInterval(() => {
  if (fs.existsSync(releaseFile)) clearInterval(hold);
}, 10);
await import("../../dist/worker.js");
