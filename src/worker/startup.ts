import fs from "node:fs";
import type { AgentRunRecord } from "../agents/types.js";
import { writeCrashRunRecord } from "./run-record.js";

/** The bootstrap must be executable without any host-provided packages. */
export const assertWorkerRuntime = (versions: { node?: string; bun?: string } = process.versions): void => {
  if (!versions.bun && Number.parseInt(versions.node ?? "0", 10) < 24) {
    throw new Error(`Fabric workers require Node.js 24+; detected ${versions.node ?? "unknown"}. Upgrade Node.js; bundled Pi can select Node.js 24+ or Bun with PI_FABRIC_NODE_BINARY.`);
  }
};

/** Even a failed options/module import must leave an actionable terminal record. */
export const writeWorkerStartupFailure = (argv: readonly string[], error: unknown): void => {
  const args = new Map<string, string>();
  for (let i = 2; i + 1 < argv.length; i += 2) args.set(argv[i]!, argv[i + 1]!);
  const statusFile = args.get("--status-file");
  if (!statusFile) return;
  const now = Date.now();
  let task = "";
  try { task = fs.readFileSync(args.get("--task-file") ?? "", "utf8"); } catch { /* task may itself be missing */ }
  let carried: Record<string, unknown> = {};
  try { carried = JSON.parse(args.get("--carry-over") ?? "{}"); } catch { /* invalid options still get a failure */ }
  const number = (value: unknown): number => typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : 0;
  const usage = carried?.usage as Record<string, unknown> | undefined;
  const record: AgentRunRecord = {
    id: args.get("--id") ?? "unknown",
    name: args.get("--name") ?? "Fabric worker",
    task, status: "failed", runner: args.get("--runner") ?? "pi",
    transport: (args.get("--transport") ?? "process") as AgentRunRecord["transport"],
    cwd: args.get("--cwd") ?? process.cwd(), startedAt: now, updatedAt: now,
    turns: number(carried?.turns), toolCalls: number(carried?.toolCalls), text: "",
    usage: { input: number(usage?.input), output: number(usage?.output), cacheRead: number(usage?.cacheRead), cacheWrite: number(usage?.cacheWrite), cost: number(usage?.cost) },
    stderr: (error instanceof Error ? error.stack ?? error.message : String(error)).slice(0, 20_000),
    ...(args.get("--log-file") ? { logFile: args.get("--log-file")! } : {}),
  };
  writeCrashRunRecord(statusFile, record, error);
};
