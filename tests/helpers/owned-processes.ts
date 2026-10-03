import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

/**
 * Test process ownership (pi-fabric#146, #160 security review S4). A fixture owns a process only
 * if that process recorded itself, at its own launch, in the fixture's launch log: a preload
 * (NODE_OPTIONS --import) that every node process the fixture starts inherits appends its pid and
 * start time before any other code runs. So a record never comes from current parentage, argv or
 * a record a pid may already have outlived. Before each signal the pid must still have that start.
 */
export type Owned = { pid: number; started: string; at: number; argv: string[] };

// Linux: starttime (field 22 of /proc/<pid>/stat, clock ticks since boot), read after the last ")"
// because comm may hold spaces and parentheses. Elsewhere ps lstart (one-second resolution).
// The preload below repeats this logic in plain JavaScript; keep the two the same.
export const startTime = (pid: number): string => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] ?? "";
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" && fs.existsSync("/proc/self/stat")) return "";
    try { return execFileSync("ps", ["-o", "lstart=", "-p", String(pid)], { encoding: "utf8" }).trim(); } catch { return ""; }
  }
};

const PRELOAD = `import fs from "node:fs";
import { execFileSync } from "node:child_process";
const log = process.env.PI_FABRIC_TEST_LAUNCH_LOG;
if (log) {
  let started = "";
  try {
    const stat = fs.readFileSync("/proc/" + process.pid + "/stat", "utf8");
    started = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] ?? "";
  } catch {
    try { started = execFileSync("ps", ["-o", "lstart=", "-p", String(process.pid)], { encoding: "utf8" }).trim(); } catch {}
  }
  if (started) {
    // F4 runs the scoped worker through a fixed --eval gate. NODE_OPTIONS
    // preloads execute after shell attachment but before that gate rewrites
    // process.argv. Decode only its JSON literal assignment for classification;
    // process ownership still comes solely from this native PID/birth receipt.
    let argv = process.argv.slice(1);
    const evalIndex = process.execArgv.indexOf("--eval");
    const source = evalIndex < 0 ? "" : process.execArgv[evalIndex + 1] ?? "";
    const assignment = source.match(/process\\.argv = \\[process\\.execPath, (.+), \\.\\.\\.(\\[[^\\n]*\\])\\];/);
    if (assignment) { try { const worker = JSON.parse(assignment[1]), args = JSON.parse(assignment[2]); if (typeof worker === "string" && Array.isArray(args) && args.every(value => typeof value === "string")) argv = [worker, ...args]; } catch {} }
    try { fs.appendFileSync(log, JSON.stringify({ pid: process.pid, started, at: Date.now(), argv, nativeArgv: process.argv.slice(1) }) + "\\n"); } catch {}
  }
}
`;

let ticksPerSecond: number | undefined;
// Wall-clock ms of a start time; Linux ticks are anchored on /proc/uptime (the same boot clock).
export const startedAtMs = (started: string): number => {
  if (!/^\d+$/.test(started)) return Date.parse(started);
  ticksPerSecond ??= Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }).trim()) || 100;
  const uptime = Number(fs.readFileSync("/proc/uptime", "utf8").split(" ")[0]);
  return Date.now() - uptime * 1000 + (Number(started) * 1000) / ticksPerSecond;
};
// Start-time clock skew allowed: tick rounding on Linux, ps lstart's one second elsewhere.
const SLACK_MS = 1_000;

export const same = (owned: Pick<Owned, "pid" | "started">): boolean => startTime(owned.pid) === owned.started;

export type LaunchLog = {
  /** Environment that makes a node child (and its node descendants) record itself at launch. */
  env: Record<string, string>;
  file: string;
  /** Records whose start lies in the fixture's window: after it was created, not after the record. */
  owned(): Owned[];
};

export const launchLog = (root: string, notBefore = Date.now()): LaunchLog => {
  const file = path.join(root, "launches.jsonl");
  const preload = path.join(root, "launch-preload.mjs");
  fs.writeFileSync(preload, PRELOAD);
  const options = [process.env.NODE_OPTIONS, `--import=${pathToFileURL(preload).href}`].filter(Boolean).join(" ");
  return {
    env: { NODE_OPTIONS: options, PI_FABRIC_TEST_LAUNCH_LOG: file },
    file,
    owned: () => {
      let text = "";
      try { text = fs.readFileSync(file, "utf8"); } catch { return []; }
      return text.split("\n").flatMap((line) => {
        let value: Partial<Owned>;
        try { value = JSON.parse(line) as Partial<Owned>; } catch { return []; }
        const { pid, started, at } = value;
        if (!Number.isSafeInteger(pid) || pid! <= 1 || pid === process.pid) return [];
        if (typeof started !== "string" || !started || typeof at !== "number") return [];
        // A process starts before it records itself, and after the fixture that launched it.
        const startMs = startedAtMs(started);
        if (!(startMs >= notBefore - SLACK_MS && startMs <= at + SLACK_MS)) return [];
        return [{ pid: pid!, started, at, argv: Array.isArray(value.argv) ? value.argv.map(String) : [] }];
      });
    },
  };
};

// Revalidated before every signal: false once the pid no longer names the recorded process.
const signalOwned = (owned: Owned, signal: NodeJS.Signals): boolean => {
  if (!same(owned)) return false;
  try { process.kill(owned.pid, signal); return true; } catch { return false; }
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * SIGTERM, then SIGKILL after termMs, then wait at most killMs more. Resolves the pid if it still
 * names the recorded process then (a zombie or an uninterruptible sleep), else undefined.
 */
export const stopOwned = async (owned: Owned, termMs = 20_000, killMs = 5_000): Promise<number | undefined> => {
  if (!signalOwned(owned, "SIGTERM")) return undefined;
  const killAt = Date.now() + termMs;
  while (same(owned) && Date.now() < killAt) await sleep(50);
  if (!signalOwned(owned, "SIGKILL")) return undefined;
  const giveUpAt = Date.now() + killMs;
  while (same(owned) && Date.now() < giveUpAt) await sleep(50);
  return same(owned) ? owned.pid : undefined;
};

/** Stops every owned process; throws naming any that outlived the bounded wait. */
export const stopAllOwned = async (owned: Owned[], termMs?: number, killMs?: number): Promise<void> => {
  const unique = [...new Map(owned.map((entry) => [`${entry.pid}:${entry.started}`, entry])).values()];
  const remaining = (await Promise.all(unique.map((entry) => stopOwned(entry, termMs, killMs))))
    .filter((pid): pid is number => pid !== undefined);
  if (remaining.length > 0) throw new Error(`Owned fixture processes did not exit: ${remaining.join(", ")}`);
};
