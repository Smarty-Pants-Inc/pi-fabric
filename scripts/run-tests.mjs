// Ordinary test entrypoint. Keep this process outside Vitest's process group so a
// forwarded signal cannot terminate the owner before all writers have stopped.
import { spawn, spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { constants } from "node:os";
import { dirname, join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { cleanupIsolatedTestTemp, launcherTestTemp } from "./test-temp.ts";

const windows = process.platform === "win32";
let child;
let receivedSignal;
let escalation;
function signalGroup(signal) {
  if (!child?.pid) return;
  if (windows) {
    // Windows has no POSIX process groups; taskkill's tree semantics are the
    // existing runner equivalent. Job temp itself remains runner-owned.
    spawnSync("taskkill", ["/PID", String(child.pid), "/T", "/F"], { stdio: "ignore", timeout: 10_000 });
    return;
  }
  try { process.kill(-child.pid, signal); }
  catch { /* Keep waiting if signaling is refused; never release a live group's root. */ }
}
function forward(signal) {
  receivedSignal ??= signal;
  signalGroup(signal);
  escalation ??= setTimeout(() => signalGroup("SIGKILL"), 3_000).unref();
}
const onTerm = () => forward("SIGTERM");
const onInt = () => forward("SIGINT");
process.on("SIGTERM", onTerm);
process.on("SIGINT", onInt);

function groupAlive() {
  if (windows || !child?.pid) return false;
  try { process.kill(-child.pid, 0); }
  catch (error) { return error.code !== "ESRCH"; }
  if (process.platform !== "linux") return true;
  // Orphaned zombies can outlive us under a container's non-reaping PID 1.
  // They cannot write to the root; do not wait forever for their group to vanish.
  try {
    for (const pid of readdirSync("/proc")) {
      if (!/^\d+$/.test(pid)) continue;
      try {
        const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
        const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
        if (Number(fields[2]) === child.pid && !["Z", "X", "x"].includes(fields[0])) return true;
      } catch (error) {
        if (error.code !== "ENOENT" && error.code !== "ESRCH") return true;
      }
    }
    return false;
  } catch { return true; }
}

let exitCode = 1;
try {
  Object.assign(process.env, launcherTestTemp());
  const require = createRequire(import.meta.url);
  const cli = join(dirname(require.resolve("vitest/package.json")), "vitest.mjs");
  const args = process.argv.slice(2);
  if (args[0] === "--") args.shift();
  child = spawn(process.execPath, [cli, "run", ...args], {
    stdio: "inherit", env: process.env, detached: !windows,
  });
  const result = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal }));
    if (receivedSignal) signalGroup(receivedSignal);
  });
  const signal = receivedSignal ?? result.signal;
  exitCode = signal ? 128 + (constants.signals[signal] ?? 1) : (result.code ?? 1);
} catch (error) {
  console.error(error);
} finally {
  // This barrier also covers spawn errors: if a pid/group exists, drain it before
  // cleanup even when no normal child-exit result was delivered.
  if (groupAlive()) {
    signalGroup(receivedSignal ?? "SIGTERM");
    const deadline = Date.now() + 3_000;
    while (groupAlive() && Date.now() < deadline) await sleep(50);
    if (groupAlive()) signalGroup("SIGKILL");
    while (groupAlive()) await sleep(50);
  }
  clearTimeout(escalation);
  if (receivedSignal) exitCode = 128 + (constants.signals[receivedSignal] ?? 1);
  cleanupIsolatedTestTemp();
  process.off("SIGTERM", onTerm);
  process.off("SIGINT", onInt);
}
process.exitCode = exitCode;
