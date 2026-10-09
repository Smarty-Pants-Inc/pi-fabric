import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { setTimeout as pause } from "node:timers/promises";
import { createJiti } from "jiti";

const [root, id, mode, contender = "0"] = process.argv.slice(2);
const jiti = createJiti(pathToFileURL(path.join(process.cwd(), "index.js")).href);
const { acquireHostLeasePublishLock, hostLeasePublishLockPath, HostLeasePublishLockBusyError } =
  await jiti.import("./src/topology/host-leases.ts");
const lock = hostLeasePublishLockPath(root, id);
if (mode.startsWith("crash-")) {
  const link = fs.linkSync.bind(fs);
  fs.linkSync = (from, to) => {
    if (String(to) === lock) {
      if (mode === "crash-before-link") process.exit(42);
      link(from, to);
      process.exit(43);
    }
    return link(from, to);
  };
  acquireHostLeasePublishLock(root, id);
  throw new Error("Crash fence not reached");
}

const waitFor = async file => {
  const deadline = Date.now() + 15_000;
  while (!fs.existsSync(file)) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${file}`);
    await pause(5);
  }
};
fs.writeFileSync(path.join(root, `ready-${contender}`), String(process.pid));
await waitFor(path.join(root, "go"));
let release;
try { release = acquireHostLeasePublishLock(root, id); }
catch (error) {
  if (!(error instanceof HostLeasePublishLockBusyError)) throw error;
  fs.writeFileSync(path.join(root, `result-${contender}`), "held");
  process.exit(0);
}
try {
  fs.writeFileSync(path.join(root, `result-${contender}`), "won");
  await waitFor(path.join(root, "release"));
} finally { release(); }
