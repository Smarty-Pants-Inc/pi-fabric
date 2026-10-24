import { diagnostics } from "../core/diagnostics.js";
import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain } from "../core/atomic-write.js";
import { detachedPreview } from "../core/preview.js";
import type { FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { MeshStore } from "../mesh/store.js";
import { pruneActorRunArchives, pruneActorSessionBackups } from "../storage/retention.js";
import { openPrivateDir, writeArchiveFile, type PrivateDirectory } from "../storage/private-dir.js";
import type { FabricActorMessage } from "./types.js";

export class ActorRunArchiveRefusedError extends Error {
  readonly code = "FABRIC_ACTOR_RUN_ARCHIVE_REFUSED";
  constructor(readonly runId: string, cause: unknown) {
    super(`Actor run ${runId} archive refused: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
    this.name = "ActorRunArchiveRefusedError";
  }
}

export type ActorRunArchiveResult =
  | { archived: true; receipt: { runId: string; directory: string } }
  | { archived: false; reason: "no-source" };

/** Every file owes a durable write; the reopenable name must still identify the held directory. */
const confirmArchiveDirectory = (directory: string, held: PrivateDirectory): void => {
  const confirm = (): void => {
    const named = fs.lstatSync(directory), opened = held.fd === undefined ? fs.statSync(held.path) : fs.fstatSync(held.fd);
    if (!named.isDirectory() || named.dev !== opened.dev || named.ino !== opened.ino) {
      throw new Error(`Archive directory ${directory} changed while it was held`);
    }
  };
  confirm();
  syncDirectoryChain(directory);
  confirm();
};

const copyArchiveFile = (held: PrivateDirectory, file: string, source: string): void => {
  if (!writeArchiveFile(held, file, { source }, true)) {
    throw new Error(`Archive entry ${file} exists and is not our regular file`);
  }
};

/** Idempotent retries of partial nested copies use the same no-follow, durable entry protocol as the root. */
const copyArchiveTree = (source: string, directory: string, anchor: string, depth = 0): void => {
  if (depth > 64) throw new Error(`Archive tree ${source} exceeds maximum depth`);
  if (!fs.lstatSync(source).isDirectory()) throw new Error(`Archive source ${source} is not a directory`);
  const held = openPrivateDir(directory, anchor);
  try {
    if (held.fd !== undefined) fs.fchmodSync(held.fd, 0o700);
    for (const file of fs.readdirSync(source)) {
      const src = path.join(source, file);
      if (fs.lstatSync(src).isDirectory()) copyArchiveTree(src, path.join(directory, file), anchor, depth + 1);
      else copyArchiveFile(held, file, src);
    }
    confirmArchiveDirectory(directory, held);
  } finally { held.close(); }
};

export const ACTOR_MESSAGE_HISTORY_LIMIT = 100;

export const ACTOR_MESSAGE_ENVELOPE_BYTES = 4_096;
const ACTOR_TRUNCATION_SUFFIX = "\n[actor message truncated]";

const serializedBytes = (value: unknown): number =>
  Buffer.byteLength(JSON.stringify(value), "utf8");

const truncateUtf8 = (value: string, maxBytes: number, suffix = ""): string => {
  if (Buffer.byteLength(value, "utf8") <= maxBytes) return detachedPreview(value, value.length);
  const boundedSuffix = Buffer.byteLength(suffix, "utf8") <= maxBytes
    ? suffix
    : truncateUtf8(suffix, maxBytes);
  const available = Math.max(0, maxBytes - Buffer.byteLength(boundedSuffix, "utf8"));
  let low = 0;
  let high = value.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (Buffer.byteLength(value.slice(0, middle), "utf8") <= available) low = middle;
    else high = middle - 1;
  }
  return `${detachedPreview(value, low)}${boundedSuffix}`;
};

const boundedActorText = (value: string, maxBytes: number): string => {
  if (serializedBytes({ text: value }) <= maxBytes) return detachedPreview(value, value.length);
  const suffix = serializedBytes({ text: ACTOR_TRUNCATION_SUFFIX }) <= maxBytes
    ? ACTOR_TRUNCATION_SUFFIX
    : "";
  let low = 0;
  let high = value.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (serializedBytes({ text: `${value.slice(0, middle)}${suffix}` }) <= maxBytes) {
      low = middle;
    } else {
      high = middle - 1;
    }
  }
  return `${detachedPreview(value, low)}${suffix}`;
};

const boundedActorData = (data: unknown, maxBytes: number): unknown => {
  let serialized: string;
  try {
    const encoded = JSON.stringify(data);
    serialized = typeof encoded === "string" ? encoded : String(data);
    if (serializedBytes({ data }) <= maxBytes) return data;
  } catch {
    serialized = String(data);
  }
  const originalBytes = Buffer.byteLength(serialized, "utf8");
  let preview = truncateUtf8(serialized, Math.max(0, maxBytes - 256));
  let bounded = { fabricTruncated: true, originalBytes, preview };
  while (preview && serializedBytes({ data: bounded }) > maxBytes) {
    preview = truncateUtf8(preview, Math.floor(Buffer.byteLength(preview, "utf8") / 2));
    bounded = { fabricTruncated: true, originalBytes, preview };
  }
  return serializedBytes({ data: bounded }) <= maxBytes
    ? bounded
    : { fabricTruncated: true, originalBytes };
};

interface ActorLogTarget {
  sessionFile: string;
  lastRunId?: string;
}

/** Archive I/O and message bounds only; ownership and sweep eligibility stay with the manager. */
export class ActorLogStore {
  constructor(
    readonly mesh: Pick<MeshStore, "maxEventBytes">,
    readonly config: Pick<FabricMeshConfig, "eventContextChars">,
    readonly retention: Pick<FabricRetentionConfig, "actorRunArchiveMs"> &
      Partial<Pick<FabricRetentionConfig, "terminalRunEventsAgeMs" | "terminalRunEventsMaxBytes">>,
  ) {}

  async retainRun(actor: ActorLogTarget, runId: string, runDirectory: string | undefined): Promise<ActorRunArchiveResult> {
    try {
      if (!runDirectory) return { archived: false, reason: "no-source" };
      const sourceDirectory = fs.lstatSync(runDirectory, { throwIfNoEntry: false });
      if (!sourceDirectory) return { archived: false, reason: "no-source" };
      if (!sourceDirectory.isDirectory()) throw new Error(`Archive source ${runDirectory} is not a directory`);
      const dest = path.join(path.dirname(actor.sessionFile), "runs", runId);
      // The trusted descriptor walk refuses unsafe ancestors before copying. Only Fabric-owned directories at or
      // below the anchor may self-heal; a refusal must leave source custody intact, never resolve as success.
      const sessionDirectory = path.dirname(path.resolve(actor.sessionFile));
      const actorsAt = sessionDirectory.lastIndexOf(`${path.sep}actors${path.sep}`);
      const anchor = actorsAt > 0 ? sessionDirectory.slice(0, actorsAt) : sessionDirectory;
      const held = openPrivateDir(dest, anchor);
      try {
        const receipt = path.join(runDirectory, "route-dispatch-receipt.json");
        if (fs.lstatSync(receipt, { throwIfNoEntry: false })) {
          const { readRouteDispatchReceipt } = await import("../agents/model-route.js");
          const source = readRouteDispatchReceipt(receipt);
          if (!source || source.runId !== runId) throw new Error("Mismatched route dispatch receipt");
          if (!writeArchiveFile(held, "route-dispatch-receipt.json", { bytes: Buffer.from(JSON.stringify(source)) }, true)) {
            throw new Error("Archive entry route-dispatch-receipt.json exists and is not our regular file");
          }
        }
        // Every present entry must succeed. Missing optional files are not refusals; unreadable files, symlinks,
        // unsafe destination entries and I/O failures are. Never discharge custody after a best-effort copy.
        for (const file of ["events.jsonl", "status.json", "reply.json", "task.txt", "relaunches.jsonl"]) {
          const src = path.join(runDirectory, file);
          if (fs.lstatSync(src, { throwIfNoEntry: false })) copyArchiveFile(held, file, src);
        }
        const nested = path.join(runDirectory, "nested");
        if (fs.lstatSync(nested, { throwIfNoEntry: false })) copyArchiveTree(nested, path.join(dest, "nested"), anchor);
        // Apply the durable confirmation even to runs without a routing receipt, AFTER all copies have succeeded.
        confirmArchiveDirectory(dest, held);
      } finally { held.close(); }
      // Retention sweeps run separately: pruning here could delete a just-copied prior run before cleanup commits.
      return { archived: true, receipt: { runId, directory: dest } };
    } catch (cause) {
      const error = new ActorRunArchiveRefusedError(runId, cause);
      diagnostics.error("src/actors/log-store.ts", `[pi-fabric] ${error.message}; source retained`);
      throw error;
    }
  }

  pruneRuns(actor: ActorLogTarget, now = Date.now()): void {
    // smarty-dev#16444: archives, never deletes: older session backups move into <actor dir>/session-archive/.
    pruneActorSessionBackups(actor.sessionFile);
    pruneActorRunArchives({
      runsDirectory: path.join(path.dirname(actor.sessionFile), "runs"),
      ...(actor.lastRunId ? { latestRunId: actor.lastRunId } : {}),
      retentionMs: this.retention.actorRunArchiveMs,
      ...(this.retention.terminalRunEventsAgeMs !== undefined
        ? { terminalRunEventsAgeMs: this.retention.terminalRunEventsAgeMs } : {}),
      ...(this.retention.terminalRunEventsMaxBytes !== undefined
        ? { terminalRunEventsMaxBytes: this.retention.terminalRunEventsMaxBytes } : {}),
      now,
    });
  }

  retainedRunIds(actor: ActorLogTarget): string[] {
    const runsDir = path.join(path.dirname(actor.sessionFile), "runs");
    try {
      return fs.readdirSync(runsDir).sort();
    } catch {
      return [];
    }
  }

  recordMessage(messages: FabricActorMessage[], message: FabricActorMessage): void {
    let bounded = structuredClone(message);
    const maxPayloadBytes = Math.max(1, this.mesh.maxEventBytes - ACTOR_MESSAGE_ENVELOPE_BYTES);
    const fixed = structuredClone(bounded);
    delete fixed.text;
    delete fixed.data;
    const contentBytes = Math.max(1, maxPayloadBytes - serializedBytes(fixed) - 128);
    const hasText = Boolean(bounded.text);
    const hasData = bounded.data !== undefined;
    const textBytes = hasText && hasData ? Math.floor(contentBytes / 2) : contentBytes;
    const dataBytes = hasText && hasData ? contentBytes - textBytes : contentBytes;
    if (bounded.text) {
      const contextBounded = bounded.text.length > this.config.eventContextChars
        ? `${detachedPreview(bounded.text, this.config.eventContextChars)}${ACTOR_TRUNCATION_SUFFIX}`
        : bounded.text;
      bounded.text = boundedActorText(contextBounded, textBytes);
    }
    if (bounded.data !== undefined) {
      bounded.data = boundedActorData(bounded.data, dataBytes);
    }
    if (serializedBytes(bounded) > maxPayloadBytes) {
      delete bounded.data;
      if (bounded.text) {
        bounded.text = boundedActorText(bounded.text, contentBytes);
      }
    }
    if (serializedBytes(bounded) > maxPayloadBytes) {
      bounded = {
        id: bounded.id,
        actorId: bounded.actorId,
        actorName: bounded.actorName,
        direction: bounded.direction,
        source: boundedActorText(bounded.source, 1_024),
        createdAt: bounded.createdAt,
        ...(bounded.action ? { action: bounded.action } : {}),
        ...(bounded.runId ? { runId: bounded.runId } : {}),
        error: "Actor message content exceeded the mesh event limit",
      };
    }
    for (const key of Object.keys(message)) {
      delete (message as unknown as Record<string, unknown>)[key];
    }
    Object.assign(message, bounded);
    messages.push(bounded);
    if (messages.length > ACTOR_MESSAGE_HISTORY_LIMIT) {
      messages.splice(0, messages.length - ACTOR_MESSAGE_HISTORY_LIMIT);
    }
  }
}
