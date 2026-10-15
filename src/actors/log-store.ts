import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain } from "../core/atomic-write.js";
import { detachedPreview } from "../core/preview.js";
import type { FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { MeshStore } from "../mesh/store.js";
import { readRunLogPage } from "../log-tail.js";
import { pruneActorRunArchives, pruneActorSessionBackups } from "../storage/retention.js";
import { closeDirectory, openPrivateDir, writeArchiveFile, type PrivateDirectory } from "../storage/private-dir.js";
import type { FabricActorMessage } from "./types.js";

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
  id?: string;
  sessionFile: string;
  lastRunId?: string;
}

const archiveReadFlags = fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0);
const sameFile = (a: fs.BigIntStats, b: fs.BigIntStats): boolean =>
  a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;
const ownedFile = (stat: fs.BigIntStats): boolean => stat.isFile() && stat.nlink === 1n &&
  (!process.getuid || stat.uid === BigInt(process.getuid()));
const hashDescriptor = (fd: number): string => {
  const hash = createHash("sha256"), chunk = Buffer.allocUnsafe(64 * 1024);
  let position = 0, read: number;
  while ((read = fs.readSync(fd, chunk, 0, chunk.length, position)) > 0) {
    hash.update(chunk.subarray(0, read));
    position += read;
  }
  return hash.digest("hex");
};

/** Mandatory archive custody, not a new copy protocol: reuse writeArchiveFile's private/durable write,
 * then verify its bytes against a held, unchanged source. Keep both descriptors until the whole archive
 * is durable, so a later status/nested copy cannot silently replace an already verified event entry. */
const holdArchiveFile = (file: string, durable = false) => {
  // Windows requires a writable descriptor for fsync; only a source-less archive rejoin syncs this fd.
  const flags = durable && process.platform === "win32" ? archiveReadFlags | fs.constants.O_RDWR : archiveReadFlags;
  const fd = fs.openSync(file, flags);
  try {
    const stat = fs.fstatSync(fd, { bigint: true });
    if (!ownedFile(stat)) throw new Error(`Unsafe actor archive file ${file}`);
    const verify = (): void => {
      const opened = fs.fstatSync(fd, { bigint: true }), named = fs.lstatSync(file, { bigint: true });
      if (!ownedFile(opened) || !ownedFile(named) || !sameFile(stat, opened) || !sameFile(stat, named)) {
        throw new Error(`Actor archive file changed: ${file}`);
      }
    };
    verify();
    return { fd, verify, close: () => fs.closeSync(fd) };
  } catch (error) { fs.closeSync(fd); throw error; }
};

/** Archive I/O and message bounds only; ownership and sweep eligibility stay with the manager. */
export class ActorLogStore {
  constructor(
    readonly mesh: Pick<MeshStore, "maxEventBytes">,
    readonly config: Pick<FabricMeshConfig, "eventContextChars">,
    readonly retention: Pick<FabricRetentionConfig, "actorRunArchiveMs"> &
      Partial<Pick<FabricRetentionConfig, "terminalRunEventsAgeMs" | "terminalRunEventsMaxBytes">>,
  ) {}

  async retainRun(actor: ActorLogTarget, runId: string, runDirectory: string | undefined): Promise<void> {
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
    // Previously cleaned runs still appear as the actor's previousRunId. Absence alone is never
    // success: securely verify the complete existing archive, or leave the owner's join pending.
    if (!runDirectory) return this.#verifyArchivedRun(actor, runId, dest, anchor);
    let sourceStat: fs.Stats;
    try { sourceStat = fs.lstatSync(runDirectory); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      return this.#verifyArchivedRun(actor, runId, dest, anchor);
    }
    if (!sourceStat.isDirectory()) throw new Error(`Unsafe actor run archive source ${runDirectory}`);
    try { held = openPrivateDir(dest, anchor); }
    catch (error) {
      console.warn(`[pi-fabric] actor run ${runId} not archived: ${(error as Error).message}`);
      throw error;
    }
    let source: PrivateDirectory | undefined;
    const files: ReturnType<typeof holdArchiveFile>[] = [];
    try {
      source = openPrivateDir(runDirectory, runDirectory);
      const sourceOpened = fs.statSync(source.path);
      if (sourceOpened.dev !== sourceStat.dev || sourceOpened.ino !== sourceStat.ino) throw new Error(`Archive source ${runDirectory} changed`);
      const plain = fs.lstatSync(path.join(source.path, "events.jsonl"), { throwIfNoEntry: false });
      const compressed = fs.lstatSync(path.join(source.path, "events.jsonl.zst"), { throwIfNoEntry: false });
      if (!plain && !compressed) throw new Error(`Actor run ${runId} event artifact is missing`);
      // Plain + zst is an uncommitted finalizer attempt. Preserve BOTH exact artifacts; readers still
      // prefer plain and the owning run's codec diagnostic remains available. Neither is tail-reduced.
      const mandatory = [...(plain ? ["events.jsonl"] : []), ...(compressed ? ["events.jsonl.zst"] : []), "status.json"];
      const sources = mandatory.map(name => {
        const file = holdArchiveFile(path.join(source!.path, name));
        files.push(file);
        return { name, file, hash: hashDescriptor(file.fd) };
      });
      for (const { name, file, hash } of sources) {
        file.verify();
        if (!writeArchiveFile(held, name, { source: path.join(source.path, name) }, true)) {
          throw new Error(`Unsafe actor run ${runId} archive entry ${name}`);
        }
        const copied = holdArchiveFile(path.join(held.path, name));
        files.push(copied);
        if (hashDescriptor(copied.fd) !== hash) throw new Error(`Actor run ${runId} archive hash mismatch: ${name}`);
        file.verify();
        copied.verify();
      }
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
        const named = fs.statSync(dest), opened = fs.statSync(held.path);
        if (named.dev !== opened.dev || named.ino !== opened.ino) throw new Error(`Archive directory ${dest} changed while it was held`);
        syncDirectoryChain(dest);
      }
      // pi-fabric#875 round 4 SEC P1: no path write into the archive. Each entry is created O_EXCL|O_NOFOLLOW through
      // the held fd, or replaced by rename only when it is our own regular file; anything else (a symlink planted while
      // a healed directory was writable) refuses that entry, with one line, and the source is untouched.
      for (const file of ["reply.json", "task.txt", "relaunches.jsonl"]) {
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
          fs.cpSync(nested, path.join(held.path, "nested"), { recursive: true });
          // cpSync keeps the source's modes; a run made under umask 002 is 0775. Archived directories are 0700, like
          // the archive itself (smarty-dev#9016), each through its own O_NOFOLLOW fd. Files keep their modes.
          if (process.platform !== "win32") closeDirectories(path.join(held.path, "nested"));
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "EEXIST") console.warn(`[pi-fabric] actor run ${runId}: archive nested/ exists; not copied again`);
          /* otherwise best-effort recursive run retention */
        }
      }
      for (const file of files) file.verify();
      const verifyDirectories = (): void => {
        const namedSource = fs.lstatSync(runDirectory), namedDest = fs.lstatSync(dest), openedDest = fs.statSync(held.path);
        if (!namedSource.isDirectory() || namedSource.dev !== sourceStat.dev || namedSource.ino !== sourceStat.ino ||
            !namedDest.isDirectory() || namedDest.dev !== openedDest.dev || namedDest.ino !== openedDest.ino) {
          throw new Error(`Actor run ${runId} archive directory changed`);
        }
      };
      verifyDirectories();
      // writeArchiveFile(true) synced each mandatory file and the held directory; sync the namespace
      // chain too before a resolved retainRun may authorize commitActorArchive/source cleanup.
      syncDirectoryChain(dest);
      verifyDirectories();
      for (const file of files) file.verify();
    } finally {
      for (const file of files) file.close();
      source?.close();
      held.close();
    }
    this.pruneRuns(actor, Date.now(), runId);
  }

  /** A source-less prior join needs complete, secure reader proof, not mere artifact existence.
   * The common reader validates compressed frames/checksums/caps; its physical generation must
   * identify our held event file. Until that reader is integrated, compressed-only rejoins fail closed. */
  #verifyArchivedRun(actor: ActorLogTarget, runId: string, dest: string, anchor: string): void {
    if (!fs.lstatSync(dest, { throwIfNoEntry: false })?.isDirectory()) throw new Error(`Actor run ${runId} archive source is missing and no verified archive exists`);
    const held = openPrivateDir(dest, anchor), files: ReturnType<typeof holdArchiveFile>[] = [];
    try {
      const plain = fs.lstatSync(path.join(held.path, "events.jsonl"), { throwIfNoEntry: false });
      const compressed = fs.lstatSync(path.join(held.path, "events.jsonl.zst"), { throwIfNoEntry: false });
      const event = holdArchiveFile(path.join(held.path, plain ? "events.jsonl" : "events.jsonl.zst"), true);
      files.push(event);
      if (plain && compressed) files.push(holdArchiveFile(path.join(held.path, "events.jsonl.zst"), true));
      const status = holdArchiveFile(path.join(held.path, "status.json"), true);
      files.push(status);
      if (fs.fstatSync(status.fd).size > 1024 * 1024) throw new Error(`Unsafe actor run ${runId} archived status`);
      const record = JSON.parse(fs.readFileSync(status.fd, "utf8"));
      if (!record || record.id !== runId || !["completed", "failed", "stopped"].includes(record.status) ||
          (record.actorId !== undefined && record.actorId !== actor.id)) {
        throw new Error(`Actor run ${runId} archived status does not prove ownership/terminal state`);
      }
      const hashes = files.map(file => hashDescriptor(file.fd));
      const page = readRunLogPage(path.join(held.path, "events.jsonl"), 1);
      const stat = fs.fstatSync(event.fd, { bigint: true });
      if (page.generation !== `${stat.dev}:${stat.ino}`) throw new Error(`Actor run ${runId} archive event artifact is not securely readable`);
      for (const [index, file] of files.entries()) {
        file.verify();
        if (hashDescriptor(file.fd) !== hashes[index]) throw new Error(`Actor run ${runId} archive changed during verification`);
        fs.fsyncSync(file.fd);
      }
      const verifyDirectory = (): void => {
        const named = fs.lstatSync(dest), opened = fs.statSync(held.path);
        if (!named.isDirectory() || named.dev !== opened.dev || named.ino !== opened.ino) throw new Error(`Actor run ${runId} archive directory changed`);
      };
      verifyDirectory();
      if (held.fd !== undefined) fs.fsyncSync(held.fd);
      syncDirectoryChain(dest);
      verifyDirectory();
      for (const file of files) file.verify();
    } finally { for (const file of files) file.close(); held.close(); }
  }

  pruneRuns(actor: ActorLogTarget, now = Date.now(), archivingRunId?: string): void {
    pruneActorSessionBackups(actor.sessionFile);
    pruneActorRunArchives({
      runsDirectory: path.join(path.dirname(actor.sessionFile), "runs"),
      ...(actor.lastRunId ? { latestRunId: actor.lastRunId } : {}),
      // A prior pending run can already be past TTL. Do not prune the very archive whose verified
      // success is about to discharge source custody; keep the actual latest-run fence as well.
      ...(archivingRunId ? { isRetained: (id: string) => id === archivingRunId } : {}),
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
