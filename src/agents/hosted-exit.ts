import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat } from "../storage/scratch.js";
import type { FabricRunStopReason } from "./runner-registry.js";

export const HOSTED_EXIT_FILE = "hosted-exit.json";
const terminal = new Set(["completed", "failed", "stopped", "timed_out"]);
interface HostedExitRecord {
  id?: string;
  runner?: string;
  transport?: string;
  status?: string;
  startedAt?: number;
  outcome?: string;
}
interface HostedExitReceipt {
  version: 1;
  id: string;
  runner: string;
  runDirectory: string;
  startedAt: number;
  locatorDigest: string;
  confirmedAt: number;
  reason: FabricRunStopReason;
}
const readOwned = (file: string): Record<string, unknown> | undefined => {
  let fd: number | undefined;
  try {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 64 * 1024) return;
    fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    const opened = fs.fstatSync(fd);
    const same = (other: fs.Stats | undefined): boolean => !!other && other.dev === stat.dev && other.ino === stat.ino &&
      other.size === stat.size && other.mtimeMs === stat.mtimeMs && other.ctimeMs === stat.ctimeMs;
    if (!same(opened)) return;
    const buffer = Buffer.allocUnsafe(64 * 1024 + 1);
    const bytes = fs.readSync(fd, buffer, 0, buffer.length, 0);
    if (bytes > 64 * 1024 || !same(ownedStat(file))) return;
    const value: unknown = JSON.parse(buffer.subarray(0, bytes).toString("utf8"));
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
  } catch { return; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
};
const digest = (locator: unknown): string => {
  const serialized = JSON.stringify(locator);
  if (serialized === undefined) throw new Error("Hosted locator is missing");
  return createHash("sha256").update(serialized).digest("hex");
};
const validTime = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0;
const matchesState = (directory: string, receipt: HostedExitReceipt): boolean => {
  const state = readOwned(path.join(directory, "hosted.json"));
  const context = state?.context as Record<string, unknown> | undefined;
  return state?.version === 1 && state.runner === receipt.runner &&
    context?.id === receipt.id && typeof context.runDirectory === "string" &&
    path.resolve(context.runDirectory) === path.resolve(directory) &&
    digest(state.locator) === receipt.locatorDigest;
};

/** Only an explicit adapter stop confirmation grants custody release, never a result or liveness. */
export const writeConfirmedHostedExit = (
  directory: string,
  record: HostedExitRecord,
  locator: unknown,
  reason: FabricRunStopReason,
): void => {
  if (record.transport !== "hosted" || !record.id || !record.runner || !validTime(record.startedAt)) {
    throw new Error("Hosted stop confirmation has no bound run identity");
  }
  const receipt: HostedExitReceipt = {
    version: 1, id: record.id, runner: record.runner, runDirectory: path.resolve(directory),
    startedAt: record.startedAt, locatorDigest: digest(locator), confirmedAt: Date.now(), reason,
  };
  if (!matchesState(directory, receipt)) throw new Error("Hosted stop confirmation has no matching durable locator");
  writeJsonAtomic(path.join(directory, HOSTED_EXIT_FILE), receipt, { durable: true });
};

/** Control custody can end on confirmed release even when the logical outcome stays indeterminate. */
export const confirmedHostedRelease = (directory: string, record: HostedExitRecord): boolean => {
  try {
    if (record.transport !== "hosted" || !terminal.has(record.status ?? "")) return false;
    const receipt = readOwned(path.join(directory, HOSTED_EXIT_FILE)) as unknown as HostedExitReceipt | undefined;
    return !!receipt && receipt.version === 1 && receipt.id === record.id && receipt.runner === record.runner &&
      receipt.runDirectory === path.resolve(directory) && receipt.startedAt === record.startedAt &&
      validTime(receipt.startedAt) && validTime(receipt.confirmedAt) && receipt.confirmedAt >= receipt.startedAt &&
      ["requested", "timeout", "shutdown"].includes(receipt.reason) &&
      typeof receipt.locatorDigest === "string" && /^[a-f0-9]{64}$/.test(receipt.locatorDigest) && matchesState(directory, receipt);
  } catch { return false; }
};

/** Collection still refuses an indeterminate outcome; release is not a repaired result. */
export const confirmedHostedExit = (directory: string, record: HostedExitRecord): boolean =>
  record.outcome === undefined && confirmedHostedRelease(directory, record);
