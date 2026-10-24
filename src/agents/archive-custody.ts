import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain, writeJsonAtomic } from "../core/atomic-write.js";
import type { AgentRunResult, AgentRunStopCaller, AgentRunStopIntent } from "./types.js";
import type { CompletionRecipient } from "./completion-journal.js";

export const ARCHIVE_PENDING_FILE = "archive-pending.json";
export const ACTOR_RUN_ARCHIVE_PENDING_FILE = "actor-run-archive-pending.json";
export interface PendingRunArchive {
  format: 1;
  kind: "settlement" | "shutdown";
  result: AgentRunResult;
  actorSessionFile?: string;
  notify?: boolean;
  recipient?: CompletionRecipient;
  actorOnly?: boolean;
  routePending?: boolean;
}
type ArchiveCustodyRecord = { format: number; awaitingResult?: boolean; ownerPid?: number; stopIntent?: AgentRunStopIntent; pending?: Partial<Record<PendingRunArchive["kind"], PendingRunArchive>> } & Partial<PendingRunArchive>;
const read = (directory: string): ArchiveCustodyRecord => {
  try { return JSON.parse(fs.readFileSync(path.join(directory, ARCHIVE_PENDING_FILE), "utf8")); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return { format: 1 }; throw error; }
};
/** Persist manager stop intent in the existing durable archive-custody receipt. */
export const persistRunStopIntent = (directory: string, reason: "explicit" | "deadline", recoveryRecord?: Record<string, unknown>, caller?: AgentRunStopCaller): void => {
  const record = read(directory);
  const by = record.stopIntent?.by ?? caller?.by;
  const stopPath = record.stopIntent?.path ?? caller?.path;
  const recovery = recoveryRecord ?? record.stopIntent?.recoveryRecord;
  writeJsonAtomic(path.join(directory, ARCHIVE_PENDING_FILE), { ...record, stopIntent: {
    reason: record.stopIntent?.reason ?? reason,
    requestedAt: record.stopIntent?.requestedAt ?? Date.now(),
    ...(by ? { by } : {}),
    ...(stopPath ? { path: stopPath } : {}),
    ...(recovery ? { recoveryRecord: recovery } : {}),
  } }, { durable: true });
};

export const readRunStopIntent = (directory: string, statusRecord?: Record<string, unknown>): ArchiveCustodyRecord["stopIntent"] => {
  const fromReceipt = read(directory).stopIntent;
  if (fromReceipt) return fromReceipt;
  const record = statusRecord ?? (() => {
    try { return JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")) as Record<string, unknown>; }
    catch { return undefined; }
  })();
  const intent = record?.stopIntent;
  if (!intent || typeof intent !== "object" || Array.isArray(intent)) return undefined;
  const value = intent as Record<string, unknown>;
  if ((value.reason !== "explicit" && value.reason !== "deadline") || typeof value.requestedAt !== "number") return undefined;
  const by = value.by && typeof value.by === "object" && !Array.isArray(value.by)
    ? value.by as Record<string, unknown> : undefined;
  const caller = by && typeof by.id === "string" && by.id.length > 0 &&
    (by.kind === "main" || by.kind === "agent" || by.kind === "actor")
    ? { kind: by.kind, id: by.id } as AgentRunStopIntent["by"] : undefined;
  const stopPath = typeof value.path === "string" && ["agents.stop", "actors.stop", "actors.remove", "session-reset", "residency-teardown", "deadline"].includes(value.path)
    ? value.path as NonNullable<AgentRunStopIntent["path"]> : undefined;
  return { reason: value.reason, requestedAt: value.requestedAt,
    ...(caller ? { by: caller } : {}), ...(stopPath ? { path: stopPath } : {}),
    ...(value.recoveryRecord && typeof value.recoveryRecord === "object" && !Array.isArray(value.recoveryRecord)
      ? { recoveryRecord: value.recoveryRecord as Record<string, unknown> } : {}),
  };
};

/** Clear only after a terminal status has been durably published. */
export const clearRunStopIntent = (directory: string): void => {
  const record = read(directory);
  if (!record.stopIntent) return;
  const rest = { ...record };
  delete rest.stopIntent;
  if (rest.pending || rest.result || rest.awaitingResult) writeJsonAtomic(path.join(directory, ARCHIVE_PENDING_FILE), rest, { durable: true });
  else { fs.rmSync(path.join(directory, ARCHIVE_PENDING_FILE), { force: true }); syncDirectoryChain(directory); }
};
export const parsePendingRunArchives = (text: string): PendingRunArchive[] => archives(JSON.parse(text));
export const readPendingRunArchives = (directory: string): PendingRunArchive[] => archives(read(directory));
const archives = (record: ReturnType<typeof read>): PendingRunArchive[] => {
  if (record.format !== 1) throw new Error("Unknown archive custody format");
  return record.pending ? Object.values(record.pending) : record.result ? [record as PendingRunArchive] : [];
};
/** Independent sinks cannot discharge each other's full-source obligations. */
export const stageRunArchive = (directory: string, archive: PendingRunArchive): void => {
  const record = read(directory);
  const pending = Object.fromEntries(readPendingRunArchives(directory).map(value => [value.kind, value]));
  pending[archive.kind] = archive;
  writeJsonAtomic(path.join(directory, ARCHIVE_PENDING_FILE), { format: 1, pending, ...(record.stopIntent ? { stopIntent: record.stopIntent } : {}) }, { durable: true });
};
/** Only a successful exact-result archive discharges that sink's source obligation. */
export const commitRunArchive = (directory: string, kind: PendingRunArchive["kind"] = "settlement"): void => {
  const pending = Object.fromEntries(readPendingRunArchives(directory).filter(value => value.kind !== kind).map(value => [value.kind, value]));
  const stopIntent = read(directory).stopIntent;
  if (Object.keys(pending).length || stopIntent) writeJsonAtomic(path.join(directory, ARCHIVE_PENDING_FILE), { format: 1, ...(Object.keys(pending).length ? { pending } : {}), ...(stopIntent ? { stopIntent } : {}) }, { durable: true });
  else { fs.rmSync(path.join(directory, ARCHIVE_PENDING_FILE), { force: true }); syncDirectoryChain(directory); }
};
