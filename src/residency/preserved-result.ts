import fs from "node:fs";
import path from "node:path";
import type { AgentRunRecord } from "../agents/types.js";
import { ownedStat } from "../storage/scratch.js";
import { RESIDENT_HOST_FORMAT, residentResultPath, type ResidentAgentMetadata } from "./protocol.js";

/** Shared saved-result proof for run-file and acknowledged-exchange retention.
 * This is not worker exit evidence: callers must independently veto live or
 * uncertain ownership before using it to release storage capacity. */
export const hasPreservedResidentResult = (runsRoot: string, id: string): boolean => {
  const residencyRoot = path.dirname(runsRoot);
  const metadataPath = path.join(residencyRoot, "agents", `${id}.json`);
  // Only proven absence permits ordinary actor/untracked collection. Unreadable or unsafe
  // metadata may still describe a public task, so uncertainty keeps its run directory.
  try { fs.lstatSync(metadataPath); }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT"; }
  if (!ownedStat(path.dirname(metadataPath))?.isDirectory() ||
      !ownedStat(path.join(residencyRoot, "results"))?.isDirectory()) return false;
  const readOwnedJson = <T>(file: string): T | undefined => {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 1024 * 1024) return undefined;
    try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; } catch { return undefined; }
  };
  const time = (value: unknown): value is number =>
    typeof value === "number" && Number.isFinite(value) && value >= 0;
  const metadata = readOwnedJson<ResidentAgentMetadata>(metadataPath);
  if (metadata?.format !== RESIDENT_HOST_FORMAT || metadata.id !== id ||
      typeof metadata.rootId !== "string" || metadata.handle?.id !== id ||
      metadata.handle.residency !== "durable" || metadata.handle.actorId !== undefined ||
      typeof metadata.handle.name !== "string" || typeof metadata.handle.cwd !== "string" ||
      !["pi", "claude", "veda"].includes(metadata.handle.runner) ||
      !["process", "tmux", "screen", "localterm", "herdr"].includes(metadata.handle.transport) ||
      !["queued", "running", "completed", "failed", "stopped", "timed_out"].includes(metadata.handle.status) ||
      !time(metadata.createdAt) || !time(metadata.updatedAt) ||
      typeof metadata.runDirectory !== "string" ||
      path.resolve(metadata.runDirectory) !== path.resolve(runsRoot, id)) return false;
  const saved = readOwnedJson<AgentRunRecord>(residentResultPath(residencyRoot, id));
  // Validate the terminal record, not just a matching id/status stub: deleting the run must
  // leave a usable result (including its text) for client status/wait across restarts.
  return !!saved && saved.id === id && saved.actorId === undefined &&
    saved.runner === metadata.handle.runner && saved.transport === metadata.handle.transport && saved.cwd === metadata.handle.cwd &&
    ["completed", "failed", "stopped", "timed_out"].includes(saved.status) &&
    typeof saved.name === "string" && typeof saved.task === "string" &&
    typeof saved.cwd === "string" && typeof saved.text === "string" &&
    ["pi", "claude", "veda"].includes(saved.runner) &&
    ["process", "tmux", "screen", "localterm", "herdr"].includes(saved.transport) &&
    time(saved.startedAt) && time(saved.updatedAt) &&
    (saved.finishedAt === undefined || time(saved.finishedAt)) &&
    time(saved.turns) && time(saved.toolCalls) &&
    (saved.error === undefined || typeof saved.error === "string") &&
    !!saved.usage && [saved.usage.input, saved.usage.output, saved.usage.cacheRead,
      saved.usage.cacheWrite, saved.usage.cost].every(time);
};
