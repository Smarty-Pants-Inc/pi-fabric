import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain, writeJsonAtomic } from "../core/atomic-write.js";
import type { AgentRunResult } from "./types.js";
import type { CompletionRecipient } from "./completion-journal.js";

export const ARCHIVE_PENDING_FILE = "archive-pending.json";
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
const read = (directory: string): { format: number; pending?: Partial<Record<PendingRunArchive["kind"], PendingRunArchive>> } & Partial<PendingRunArchive> => {
  try { return JSON.parse(fs.readFileSync(path.join(directory, ARCHIVE_PENDING_FILE), "utf8")); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return { format: 1 }; throw error; }
};
export const parsePendingRunArchives = (text: string): PendingRunArchive[] => archives(JSON.parse(text));
export const readPendingRunArchives = (directory: string): PendingRunArchive[] => archives(read(directory));
const archives = (record: ReturnType<typeof read>): PendingRunArchive[] => {
  if (record.format !== 1) throw new Error("Unknown archive custody format");
  return record.pending ? Object.values(record.pending) : record.result ? [record as PendingRunArchive] : [];
};
/** Independent sinks cannot discharge each other's full-source obligations. */
export const stageRunArchive = (directory: string, archive: PendingRunArchive): void => {
  const pending = Object.fromEntries(readPendingRunArchives(directory).map(value => [value.kind, value]));
  pending[archive.kind] = archive;
  writeJsonAtomic(path.join(directory, ARCHIVE_PENDING_FILE), { format: 1, pending }, { durable: true });
};
/** Only a successful exact-result archive discharges that sink's source obligation. */
export const commitRunArchive = (directory: string, kind: PendingRunArchive["kind"] = "settlement"): void => {
  const pending = Object.fromEntries(readPendingRunArchives(directory).filter(value => value.kind !== kind).map(value => [value.kind, value]));
  if (Object.keys(pending).length) writeJsonAtomic(path.join(directory, ARCHIVE_PENDING_FILE), { format: 1, pending }, { durable: true });
  else { fs.rmSync(path.join(directory, ARCHIVE_PENDING_FILE), { force: true }); syncDirectoryChain(directory); }
};
