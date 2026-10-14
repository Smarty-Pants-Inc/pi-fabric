import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain } from "../core/atomic-write.js";
import type { FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { MeshStore } from "../mesh/store.js";
import { pruneActorRunArchives, pruneActorSessionBackups } from "../storage/retention.js";
import { closeDirectory, openPrivateDir, writeArchiveFile, type PrivateDirectory } from "../storage/private-dir.js";
import { MAX_STEER_BYTES, MAX_STEER_ENTRY_BYTES, terminalAdmissionFromEntry, terminalControlMatchesAdmission, terminalSenderMatches, type TerminalAdmission } from "../agents/terminal-notices.js";
import { MAX_FINAL_ANSWER_BYTES, MAX_TASK_CONTROL_BYTES, MAX_TASK_CONTROL_ROWS, createArchiveTaskControlDirectory, isTaskControlDirectoryEntry, legacyTaskControlSibling, openTaskControlFile, readFinalAnswerReceipt, readTaskRunStatus, taskControlDirectory, withLegacyTaskControlDirectory, withTaskControlDirectory, withTaskControlReadDirectory, withTaskControlSubdirectory } from "../worker/terminal-answer.js";
import type { FabricActorMessage } from "./types.js";

const controlFiles = new Set(["steer.jsonl", "final-answer.json"]);
/** Read-only sibling-v1 location; new controls live in the central namespace. */
const legacyControlSibling = legacyTaskControlSibling;
const controlDirectories = new Set(["terminal-controls", "terminal-notices"]);
/** Preserve bounded original bytes, never copy the inode-bound legacy grant.
 * Eligible legacy evidence is promoted into the protected archive namespace.
 * Journals stay legacy-first; only matching delivered overlays replace old debt.
 * Duplicate final answers always refuse, even when their bytes are identical. */
const archiveControls = (runDirectory: string): Map<string, Buffer> | undefined => {
  // Protected controls are central (central-v1) or an old sibling-v1 directory.
  const sibling = !!fs.lstatSync(taskControlDirectory(runDirectory), { throwIfNoEntry: false }) ||
    !!fs.lstatSync(legacyControlSibling(runDirectory), { throwIfNoEntry: false });
  const local = [...controlFiles, ...controlDirectories].some(name => fs.lstatSync(path.join(runDirectory, name), { throwIfNoEntry: false }));
  // The shared status reader also proves the source root owned and unaliased.
  // Keep the older receipt-only / opaque-status archive behavior when there are
  // no controls, but never excuse an unsafe root or a marked missing sibling.
  try {
    const record = readTaskRunStatus<Record<string, unknown>>(runDirectory);
    if (!sibling && !local && record && !("taskControlLayout" in record)) return;
  } catch (error) {
    if (!sibling && !local && (error instanceof SyntaxError || (error as NodeJS.ErrnoException).code === "ENOENT")) return;
    throw error;
  }
  const result = new Map<string, Buffer>(), overlays = new Map<string, [Buffer, Buffer]>();
  let entries = 0, bytes = 0, artifactBytes = 0;
  const read = (directory: string, name: string, relative = name): void => {
    if (++entries > MAX_TASK_CONTROL_ROWS * 3 + 2) throw new Error("Archive control entry bound exceeded");
    const max = relative === "final-answer.json" ? MAX_FINAL_ANSWER_BYTES : relative === "steer.jsonl" ? MAX_STEER_BYTES : MAX_TASK_CONTROL_BYTES;
    const fd = openTaskControlFile(path.join(directory, name));
    try {
      if (fs.fstatSync(fd).size > max) throw new Error("Archive control byte bound exceeded");
      const buffer = Buffer.alloc(max + 1);
      let length = 0;
      while (length < buffer.length) {
        const count = fs.readSync(fd, buffer, length, buffer.length - length, length);
        if (!count) break;
        length += count;
      }
      if (length > max || (bytes += length) > MAX_FINAL_ANSWER_BYTES + MAX_STEER_BYTES * 5) throw new Error("Archive control byte bound exceeded");
      // Count both copies BEFORE normalization: collapsing whitespace or old
      // debt must not turn retention's oversized source into new authority.
      if (path.dirname(relative) !== "." && (artifactBytes += length) > MAX_STEER_BYTES * 4) throw new Error("Archive terminal artifact byte bound exceeded");
      const content = Buffer.from(buffer.subarray(0, length)), existing = result.get(relative);
      if (existing) {
        if (relative === "steer.jsonl") {
          if (existing.length && existing.at(-1) !== 10 || content.length && content.at(-1) !== 10 || existing.length + content.length > MAX_STEER_BYTES) {
            throw new Error("Invalid combined archive steering journal");
          }
          result.set(relative, Buffer.concat([existing, content]));
        } else if (relative === "final-answer.json") throw new Error(`Conflicting legacy archive control ${relative}`);
        else overlays.set(relative, [existing, content]);
      } else result.set(relative, content);
    } finally { fs.closeSync(fd); }
  };
  const scan = (directory: string, legacy: boolean, markerAllowed: boolean): void => {
    const cursor = fs.opendirSync(directory, { bufferSize: 32 });
    try {
      let seen = 0;
      for (;;) {
        const entry = cursor.readSync();
        if (!entry) break;
        if (++seen > MAX_TASK_CONTROL_ROWS * 3 + 32) throw new Error("Archive control namespace bound exceeded");
        if (entry.name === ".legacy-read.json" && !legacy && markerAllowed) continue;
        if (controlFiles.has(entry.name)) { read(directory, entry.name); continue; }
        if (controlDirectories.has(entry.name)) {
          withTaskControlSubdirectory(directory, entry.name, child => {
            const children = fs.opendirSync(child, { bufferSize: 32 });
            try {
              for (;;) {
                const item = children.readSync();
                if (!item) break;
                const pattern = entry.name === "terminal-controls" ? /^[a-zA-Z0-9_-]{1,200}\.json$/ : /^[a-zA-Z0-9_-]{1,200}\.json(?:\.delivered)?$/;
                if (!pattern.test(item.name)) throw new Error("Unknown archive control artifact");
                read(child, item.name, path.join(entry.name, item.name));
              }
            } finally { children.closeSync(); }
          });
          continue;
        }
        if (!legacy) throw new Error(`Unknown archive control artifact ${entry.name}`);
      }
    } finally { cursor.closeSync(); }
  };
  withTaskControlReadDirectory(runDirectory, selected => {
    if (!sibling) { scan(selected, true, false); return; }
    const legacy = withLegacyTaskControlDirectory(runDirectory, directory => { scan(directory, true, false); return true; }) === true;
    if (!legacy && local) throw new Error("Unproven legacy archive controls");
    scan(selected, false, legacy);
  });
  if (overlays.size || result.has("steer.jsonl")) {
    const admissions = new Map<string, TerminalAdmission>(), journal = result.get("steer.jsonl")?.toString("utf8") ?? "";
    if (journal && !journal.endsWith("\n")) throw new Error("Incomplete archive steering journal");
    let rows = 0;
    for (const line of journal.split("\n")) {
      if (!line.trim()) continue;
      if (++rows > MAX_TASK_CONTROL_ROWS || Buffer.byteLength(line) > MAX_STEER_ENTRY_BYTES) throw new Error("Archive steering row bound exceeded");
      const entry = JSON.parse(line);
      if (entry?.type !== "steer" && entry?.type !== "follow_up") continue;
      const admission = terminalAdmissionFromEntry(entry);
      if (!admission || admissions.has(admission.id)) throw new Error("Invalid or duplicate archive admission");
      admissions.set(admission.id, admission);
    }
    // Mirror retention's consumed legacy no-answer overlay, not a receipt claim:
    // only old queued debt plus an independently matching delivered sibling.
    const record = overlays.size ? readTaskRunStatus<{ id?: string; status?: string; finalAnswerReceipt?: { id: string; recordedAt: number } }>(runDirectory) : undefined;
    const answer = typeof record?.id === "string" ? readFinalAnswerReceipt(runDirectory, record.id) : undefined;
    const noAnswer = !!record && typeof record.id === "string" && !('finalAnswerReceipt' in record) &&
      !result.has("final-answer.json") && ["stopped", "failed"].includes(record.status ?? "");
    if (noAnswer && [...result.keys()].some(relative => path.dirname(relative) === "terminal-notices")) {
      throw new Error("Archive no-answer overlay has terminal notices");
    }
    for (const [relative, [oldBytes, newBytes]] of overlays) {
      if (!noAnswer && (!answer || record?.finalAnswerReceipt?.id !== answer.id || record.finalAnswerReceipt.recordedAt !== answer.recordedAt)) {
        throw new Error("Archive overlay lacks its original final receipt");
      }
      const old = JSON.parse(oldBytes.toString("utf8")), newer = JSON.parse(newBytes.toString("utf8"));
      const name = path.basename(relative), id = name.replace(/\.json(?:\.delivered)?$/, ""), admission = admissions.get(id);
      if (path.dirname(relative) === "terminal-controls") {
        const keys = ["id", "state", "delivery", "provenance", ...(noAnswer ? [] : ["reason"])];
        if (!old || !newer || !(noAnswer ? ["queued"] : ["queued", "refused"]).includes(old.state) || newer.state !== "delivered" ||
            Object.keys(old).some(key => !keys.includes(key)) || Object.keys(newer).some(key => !keys.includes(key)) ||
            old.reason !== undefined && (old.state !== "refused" || old.reason !== "run-ended") || newer.reason !== undefined ||
            !terminalControlMatchesAdmission(old, admission) || !terminalControlMatchesAdmission(newer, admission)) {
          throw new Error(`Conflicting legacy archive control ${relative}`);
        }
      } else {
        if (!answer || !admission || JSON.stringify(old) !== JSON.stringify(newer) || newer?.code !== "FABRIC_TARGET_TERMINAL" || newer.targetId !== answer.runId ||
            newer.finalAnswerReceiptId !== answer.id || newer.messageId !== id || newer.delivery !== admission.delivery ||
            !terminalSenderMatches(newer.sender, admission.sender)) throw new Error(`Conflicting legacy archive notice ${relative}`);
      }
      result.set(relative, newBytes);
    }
  }
  return result;
};

/** Validate every nested pair before publication. The copy excludes ALL source
 * control namespaces; each validated run receives a fresh protected sibling. */
const archiveNestedControls = (runDirectory: string): Map<string, Map<string, Buffer> | undefined> => {
  const result = new Map<string, Map<string, Buffer> | undefined>();
  let entries = 0;
  const visit = (run: string, depth: number): void => {
    if (depth > 32) throw new Error("Archive nested depth bound exceeded");
    const nested = path.join(run, "nested");
    if (!fs.lstatSync(nested, { throwIfNoEntry: false })) return;
    withTaskControlSubdirectory(run, "nested", directory => {
      const cursor = fs.opendirSync(directory, { bufferSize: 32 });
      try {
        for (;;) {
          const entry = cursor.readSync();
          if (!entry) break;
          if (++entries > MAX_TASK_CONTROL_ROWS * 3 + 32) throw new Error("Archive nested entry bound exceeded");
          // Use the canonical path only after the held, no-follow parent check;
          // the shared reader rejects aliases through any source symlink.
          if (isTaskControlDirectoryEntry(nested, entry.name)) continue;
          if (entry.isFile() && entry.name !== ".legacy-read.json") {
            // Legacy nested files are opaque bytes, not run/control evidence.
            // Prove owned, single-link and no-follow through the held parent.
            const fd = openTaskControlFile(path.join(directory, entry.name));
            fs.closeSync(fd);
            continue;
          }
          if (!entry.isDirectory()) throw new Error("Unsafe nested archive run");
          const child = path.join(nested, entry.name);
          readTaskRunStatus(child);
          result.set(child, archiveControls(child));
          visit(child, depth + 1);
        }
      } finally { cursor.closeSync(); }
    });
  };
  visit(path.resolve(runDirectory), 0);
  return result;
};

const assertArchiveRunIdentity = (dest: string, held: PrivateDirectory): void => {
  const named = fs.lstatSync(dest), opened = held.fd === undefined ? fs.statSync(held.path) : fs.fstatSync(held.fd);
  if (!named.isDirectory() || named.isSymbolicLink() || named.dev !== opened.dev || named.ino !== opened.ino || fs.realpathSync(dest) !== path.resolve(dest)) {
    throw new Error(`Archive directory ${dest} changed while it was held`);
  }
};

const writeArchivedControls = (dest: string, anchor: string, controls: Map<string, Buffer>, run: PrivateDirectory): void => {
  assertArchiveRunIdentity(dest, run);
  if (fs.lstatSync(taskControlDirectory(dest), { throwIfNoEntry: false })) withTaskControlDirectory(dest, () => undefined);
  // Publish into the CENTRAL namespace keyed by the archive path, never a raw
  // `<dest>.ctl` sibling inside the (task-visible) archive tree.
  if (fs.lstatSync(legacyControlSibling(dest), { throwIfNoEntry: false })) throw new Error(`Archive destination ${dest} has a stray control sibling`);
  void anchor;
  assertArchiveRunIdentity(dest, run);
  createArchiveTaskControlDirectory(dest);
  assertArchiveRunIdentity(dest, run);
  withTaskControlDirectory(dest, (directory, fd) => {
      assertArchiveRunIdentity(dest, run);
      for (const [relative, bytes] of controls) {
        const parent = path.dirname(relative);
        const write = (at: string, parentFd: number): void => {
          if (!writeArchiveFile({ path: at, fd: parentFd, close() {} }, path.basename(relative), { bytes }, true)) throw new Error(`Unsafe archive control ${relative}`);
        };
        if (parent === ".") write(directory, fd);
        else {
          try { fs.mkdirSync(path.join(directory, parent), { mode: 0o700 }); fs.chmodSync(path.join(directory, parent), 0o700); }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
          withTaskControlSubdirectory(directory, parent, write);
        }
      }
      if (process.platform !== "win32") fs.fsyncSync(fd);
      const named = fs.lstatSync(taskControlDirectory(dest)), opened = fs.fstatSync(fd);
      if (named.dev !== opened.dev || named.ino !== opened.ino) throw new Error("Archive controls changed while held");
      assertArchiveRunIdentity(dest, run);
      syncDirectoryChain(taskControlDirectory(dest));
      assertArchiveRunIdentity(dest, run);
  });
};

/** 0700 every directory of a copied tree, each through its own O_NOFOLLOW fd after fstat proves it ours; a link, a
 * file or a foreign entry is skipped, never followed (pi-fabric#875 round 1 P1-2). */
const closeDirectories = (directory: string, depth = 0): void => {
  if (depth > 64 || !closeDirectory(directory)) return;
  for (const name of fs.readdirSync(directory)) closeDirectories(path.join(directory, name), depth + 1);
};

export const ACTOR_MESSAGE_HISTORY_LIMIT = 100;

export const ACTOR_MESSAGE_ENVELOPE_BYTES = 4_096;
const ACTOR_TRUNCATION_SUFFIX = "\n[actor message truncated]";

const serializedBytes = (value: unknown): number =>
  Buffer.byteLength(JSON.stringify(value), "utf8");

const truncateUtf8 = (value: string, maxBytes: number, suffix = ""): string => {
  if (Buffer.byteLength(value, "utf8") <= maxBytes) return value;
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
  return `${value.slice(0, low)}${boundedSuffix}`;
};

const boundedActorText = (value: string, maxBytes: number): string => {
  if (serializedBytes({ text: value }) <= maxBytes) return value;
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
  return `${value.slice(0, low)}${suffix}`;
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

  async retainRun(actor: ActorLogTarget, runId: string, runDirectory: string | undefined): Promise<void> {
    if (!runDirectory || !fs.existsSync(runDirectory)) return;
    const dest = path.join(path.dirname(actor.sessionFile), "runs", runId);
    // pi-fabric#875 round 2 P1-b: the archive destination is created or validated BEFORE anything is copied, by the
    // trusted descriptor walk (private-dir.ts), and every copy goes through the held descriptor's path. A symlinked,
    // foreign or group/other-writable step anywhere in the chain refuses the archive: the run stays unarchived (its
    // source is untouched) and one line says why.
    let held: PrivateDirectory;
    // The anchor is the mesh root (the parent of the `actors` directory the session file lies in), else the actor's own
    // directory: our own group-writable containers below it are tightened on first use (pi-fabric#875 round 3).
    const sessionDirectory = path.dirname(path.resolve(actor.sessionFile));
    const actorsAt = sessionDirectory.lastIndexOf(`${path.sep}actors${path.sep}`);
    const anchor = actorsAt > 0 ? sessionDirectory.slice(0, actorsAt) : sessionDirectory;
    try { held = openPrivateDir(dest, anchor); }
    catch (error) {
      console.warn(`[pi-fabric] actor run ${runId} not archived: ${(error as Error).message}`);
      return;
    }
    try {
      runDirectory = path.resolve(runDirectory);
      const controls = archiveControls(runDirectory), nestedControls = archiveNestedControls(runDirectory);
      const receipt = path.join(runDirectory, "route-dispatch-receipt.json");
      if (fs.existsSync(receipt)) {
        const { readRouteDispatchReceipt } = await import("../agents/model-route.js");
        const source = readRouteDispatchReceipt(receipt);
        if (!source || source.runId !== runId) throw new Error("Mismatched route dispatch receipt");
        // Durable archive confirmation precedes source cleanup (F2 custody): the file and the held directory are
        // fsynced through their fds, then the chain above by path, which must still name the held directory.
        if (!writeArchiveFile(held, "route-dispatch-receipt.json", { bytes: Buffer.from(JSON.stringify(source)) }, true)) {
          throw new Error(`Archive entry ${path.join(dest, "route-dispatch-receipt.json")} exists and is not our regular file`);
        }
        assertArchiveRunIdentity(dest, held);
        syncDirectoryChain(dest);
      }
      // The source was validated before publication. Never transport
      // .legacy-read.json: its source inode is not the archive's.
      assertArchiveRunIdentity(dest, held);
      if (controls) writeArchivedControls(dest, anchor, controls, held);
      assertArchiveRunIdentity(dest, held);
      // pi-fabric#875 round 4 SEC P1: no path write into the archive. Each entry is created O_EXCL|O_NOFOLLOW through
      // the held fd, or replaced by rename only when it is our own regular file; anything else (a symlink planted while
      // a healed directory was writable) refuses that entry, with one line, and the source is untouched.
      for (const file of ["events.jsonl", "status.json", "reply.json", "task.txt", "relaunches.jsonl"]) {
        const src = path.join(runDirectory, file);
        if (!fs.existsSync(src)) continue;
        try {
          if (!writeArchiveFile(held, file, { source: src })) console.warn(`[pi-fabric] actor run ${runId}: archive entry ${file} exists and is not our regular file; not written`);
        } catch (error) { console.warn(`[pi-fabric] actor run ${runId}: archive entry ${file} not written: ${(error as Error).message}`); }
      }
      const nested = path.join(runDirectory, "nested");
      if (fs.existsSync(nested)) {
        try {
          // Copied only into a nested/ directory this archive creates now (fresh, 0700): never into existing entries.
          fs.mkdirSync(path.join(held.path, "nested"), { mode: 0o700 });
          fs.chmodSync(path.join(held.path, "nested"), 0o700); // our fresh directory; a umask 0400 left 0300 (unopenable)
          if (!closeDirectory(path.join(held.path, "nested"))) throw new Error("the new nested directory is not ours");
          const excluded = new Set<string>();
          for (const source of nestedControls.keys()) {
            // Source trees only contain OLD sibling-v1 controls; central ones are outside the tree.
            excluded.add(legacyControlSibling(source));
            for (const name of [...controlFiles, ...controlDirectories]) excluded.add(path.join(source, name));
          }
          let unsafeArtifact = false;
          try {
            fs.cpSync(nested, path.join(held.path, "nested"), { recursive: true, filter: source => {
              if (excluded.has(path.resolve(source))) return false;
              try {
                // Never transport an unproven inode-bound host grant.
                if (path.basename(source) === ".legacy-read.json") throw new Error("Unproven nested legacy grant");
                const stat = fs.lstatSync(source);
                if (stat.isSymbolicLink() || process.getuid && stat.uid !== process.getuid() ||
                    !stat.isDirectory() && (!stat.isFile() || stat.nlink !== 1)) throw new Error("Unsafe nested archive artifact");
                return true;
              } catch (error) { unsafeArtifact = true; throw error; }
            } });
          } catch (error) {
            // Preserve best-effort opaque legacy copies ONLY: validation and
            // any root/nested control transfer must still fail closed.
            if (unsafeArtifact || controls !== undefined || [...nestedControls.values()].some(value => value !== undefined)) throw error;
            console.warn(`[pi-fabric] actor run ${runId}: legacy nested/ not copied: ${(error as Error).message}`);
          }
          // Copied directories may have permissive modes; make fresh archives
          // private before publishing protected siblings through held descriptors.
          if (process.platform !== "win32") closeDirectories(path.join(held.path, "nested"));
          assertArchiveRunIdentity(dest, held);
          for (const [source, childControls] of nestedControls) {
            if (!childControls) continue;
            const child = path.join(dest, path.relative(runDirectory, source)), childHeld = openPrivateDir(child, anchor);
            try { writeArchivedControls(child, anchor, childControls, childHeld); }
            finally { childHeld.close(); }
          }
          assertArchiveRunIdentity(dest, held);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "EEXIST") console.warn(`[pi-fabric] actor run ${runId}: archive nested/ exists; not copied again`);
          else throw error; // incomplete control transfer never confirms archival
        }
      }
    } finally { held.close(); }
    this.pruneRuns(actor);
  }

  pruneRuns(actor: ActorLogTarget, now = Date.now()): void {
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
      // Skip ONLY validated sibling-v1 control directories of a surviving run; an
      // unknown/unvalidated `.ctl` entry stays visible rather than hidden.
      return fs.readdirSync(runsDir).filter(name => {
        try { return !isTaskControlDirectoryEntry(runsDir, name); } catch { return true; }
      }).sort();
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
        ? `${bounded.text.slice(0, this.config.eventContextChars)}${ACTOR_TRUNCATION_SUFFIX}`
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
