// fabric-mesh-lock-stats: fleet view of a mesh root's lock (smarty-dev#6477 L8). Read-only; it
// sums every process's <mesh>/lock-stats/<host>-<pid>.json, see docs/mesh-lock-stats.md.
import path from "node:path";
import { LOCK_STATS_RETAIN_MINUTES, readLockStats, summarizeLockStats, type LockStatsSummary } from "./mesh/commit-stats.js";
import { resolveMeshRoot } from "./participants-cli.js";

const USAGE = `Usage: fabric-mesh-lock-stats [--mesh DIR] [--minutes N] [--top K] [--json]
                              [--max-busy PCT] [--max-timeouts N]

Prints the mesh lock's busy %, timeouts, top caller classes by hold time and top pids over
the last N complete minutes (default 10, at most ${LOCK_STATS_RETAIN_MINUTES}), summed over every process on the root.
--max-busy and --max-timeouts exit 3 when the window exceeds them (a stage gate).`;

interface Options { mesh?: string; minutes: number; top: number; json: boolean; maxBusy?: number; maxTimeouts?: number }

const parseArgs = (argv: string[]): Options | "help" => {
  const options: Options = { minutes: 10, top: 5, json: false };
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index]!;
    if (flag === "--help" || flag === "-h") return "help";
    if (flag === "--json") { options.json = true; continue; }
    const value = argv[++index];
    if (value === undefined || value.startsWith("--")) throw new Error(`Missing value for ${flag}\n${USAGE}`);
    if (flag === "--mesh") { options.mesh = value; continue; }
    const number = Number(value);
    if (!Number.isFinite(number) || number < 0) throw new Error(`Bad ${flag} ${value}\n${USAGE}`);
    if (flag === "--minutes") options.minutes = Math.max(1, Math.min(LOCK_STATS_RETAIN_MINUTES, Math.floor(number)));
    else if (flag === "--top") options.top = Math.max(1, Math.floor(number));
    else if (flag === "--max-busy") options.maxBusy = number;
    else if (flag === "--max-timeouts") options.maxTimeouts = Math.floor(number);
    else throw new Error(`Bad argument: ${flag}\n${USAGE}`);
  }
  return options;
};

const ms = (value: number): string => value === Number.POSITIVE_INFINITY ? ">10s"
  : value >= 1_000 ? `${(value / 1_000).toFixed(2)}s` : `${value.toFixed(value >= 100 ? 0 : 1)}ms`;
const pct = (value: number): string => `${value.toFixed(1)}%`;
const clock = (minute: number): string => new Date(minute * 60_000).toISOString().slice(11, 16);
const table = (rows: string[][]): string => {
  const widths = rows[0]!.map((_, column) => Math.max(...rows.map(row => row[column]!.length)));
  return rows.map(row => row.map((cell, column) => column === 0 ? cell.padEnd(widths[column]!) : cell.padStart(widths[column]!))
    .join("  ").trimEnd()).join("\n");
};

const formatLockStats = (summary: LockStatsSummary): string => {
  const lines = [
    `mesh lock ${summary.root}: last ${summary.minutes} min (${clock(summary.fromMinute)}-${clock(summary.toMinute + 1)} UTC), ` +
      `${summary.processes} process${summary.processes === 1 ? "" : "es"}`,
    `busy ${pct(summary.busyPct)} (peak minute ${pct(summary.peakMinuteBusyPct)}), ${summary.n} acquisitions, ` +
      `timeouts ${summary.timeouts}, failed tries ${summary.tries}`,
    `wait mean ${ms(summary.waitMeanMs)}, p99 <= ${ms(summary.waitP99Ms)}, max ${ms(summary.waitMaxMs)}; ` +
      `hold mean ${ms(summary.holdMeanMs)}, p99 <= ${ms(summary.holdP99Ms)}, max ${ms(summary.holdMaxMs)}`,
  ];
  if (!summary.n && !summary.timeouts && !summary.tries) return [...lines, "no lock acquisitions recorded in this window"].join("\n");
  lines.push("", "classes by hold time:", table([
    ["class", "acq", "hold", "share", "busy", "hold mean", "hold p99<=", "hold max", "wait mean", "wait p99<=", "wait max", "timeouts", "tries"],
    ...summary.classes.map(row => [row.lockClass, String(row.n), ms(row.holdMs), pct(row.holdSharePct), pct(row.busyPct),
      ms(row.holdMeanMs), ms(row.holdP99Ms), ms(row.holdMaxMs), ms(row.waitMeanMs), ms(row.waitP99Ms), ms(row.waitMaxMs),
      String(row.timeouts), String(row.tries)]),
  ]), "", "top pids by hold time:", table([
    ["host-pid", "acq", "hold", "busy", "hold mean", "wait mean", "wait max", "timeouts", "tries", "top class"],
    ...summary.pids.map(row => [`${row.host}-${row.pid}`, String(row.n), ms(row.holdMs), pct(row.busyPct), ms(row.holdMeanMs),
      ms(row.waitMeanMs), ms(row.waitMaxMs), String(row.timeouts), String(row.tries), row.topClass ?? "-"]),
  ]));
  return lines.join("\n");
};

export const main = (argv: string[], io: { stdout?: (text: string) => void; now?: number; env?: NodeJS.ProcessEnv; cwd?: string } = {}): number => {
  const write = io.stdout ?? ((text: string) => void process.stdout.write(text));
  const options = parseArgs(argv);
  if (options === "help") { write(`${USAGE}\n`); return 0; }
  const root = path.resolve(options.mesh ?? resolveMeshRoot(io.env, io.cwd));
  const summary = summarizeLockStats(root, readLockStats(root), { minutes: options.minutes, top: options.top, now: io.now });
  write(`${options.json ? JSON.stringify(summary, (_key, value: unknown) =>
    value === Number.POSITIVE_INFINITY ? "Infinity" : value, 2) : formatLockStats(summary)}\n`);
  const over = (options.maxBusy !== undefined && summary.busyPct > options.maxBusy) ||
    (options.maxTimeouts !== undefined && summary.timeouts > options.maxTimeouts);
  return over ? 3 : 0;
};
