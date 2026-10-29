import fs from "node:fs";
import path from "node:path";
import { ACTOR_RUN_ARCHIVE_PENDING_FILE } from "./archive-custody.js";

/** Same ownership rules as ownedStat, without filesystem work on the admission event loop. */
const owned = (stat: fs.Stats): boolean => !stat.isSymbolicLink() && (!stat.isFile() || stat.nlink === 1) &&
  (!process.getuid || stat.uid === process.getuid());
const directoryOwned = async (directory: string): Promise<boolean> => {
  try { const stat = await fs.promises.lstat(directory); return owned(stat) && stat.isDirectory(); }
  catch { return false; }
};

/** A fixed-size read, even if a custody marker grows after lstat. Never follows a marker symlink. */
async function matches(directory: string, runId: string, actorId: string, sessionFile: string): Promise<boolean> {
  let handle: fs.promises.FileHandle | undefined;
  try {
    if (!await directoryOwned(directory)) return false;
    const file = path.join(directory, ACTOR_RUN_ARCHIVE_PENDING_FILE);
    const before = await fs.promises.lstat(file);
    if (!owned(before) || !before.isFile() || before.size > 4096) return false;
    handle = await fs.promises.open(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    const opened = await handle.stat();
    if (!owned(opened) || !opened.isFile() || opened.size > 4096 || before.dev !== opened.dev || before.ino !== opened.ino) return false;
    const buffer = Buffer.alloc(4097);
    let bytes = 0;
    while (bytes < buffer.length) {
      const read = await handle.read(buffer, bytes, buffer.length - bytes, bytes);
      if (!read.bytesRead) break;
      bytes += read.bytesRead;
    }
    if (bytes > 4096) return false;
    const marker = JSON.parse(buffer.toString("utf8", 0, bytes));
    return marker?.format === 1 && marker.runId === runId && marker.actorId === actorId && marker.sessionFile === sessionFile;
  } catch { return false; } // Unreadable custody stays a collection veto, never authority.
  finally { await handle?.close(); }
}

/** Stream the run root via native asynchronous I/O; at most 32 marker FDs plus one directory handle.
 * Fixed batches preserve enumeration order regardless of completion order, without a full listing or unbounded fan-out.
 * Known run IDs (archive commit/restarted-owner retry) inspect only their exact fenced source.
 * There is deliberately no cache: a failed sink or a later publication must remain discoverable.
 */
export async function discoverActorArchiveSources(runRoot: string, actorId: string, sessionFile: string, runId?: string): Promise<Map<string, string>> {
  const sources = new Map<string, string>();
  if (runId !== undefined && !/^[a-f0-9]{32}$/.test(runId)) return sources;
  if (!await directoryOwned(runRoot)) return sources;
  if (runId !== undefined) {
    const directory = path.join(runRoot, runId);
    if (await matches(directory, runId, actorId, sessionFile)) sources.set(runId, directory);
    return sources;
  }
  const directory = await fs.promises.opendir(runRoot, { bufferSize: 32 });
  const pending: { runId: string; source: string; result: Promise<PromiseSettledResult<boolean>> }[] = [];
  const flush = async () => {
    const batch = pending.splice(0);
    const results = await Promise.all(batch.map(candidate => candidate.result));
    for (let i = 0; i < batch.length; i++) {
      const result = results[i]!;
      if (result.status === "rejected") throw result.reason;
      if (result.value) sources.set(batch[i]!.runId, batch[i]!.source);
    }
  };
  try {
    let entry: fs.Dirent | null;
    while ((entry = await directory.read()) !== null) {
      if (!entry.isDirectory() || !/^[a-f0-9]{32}$/.test(entry.name)) continue;
      const source = path.join(runRoot, entry.name);
      // Handle rejection immediately: even a close failure must not become unhandled while reading the directory.
      const result = matches(source, entry.name, actorId, sessionFile).then(
        value => ({ status: "fulfilled" as const, value }), reason => ({ status: "rejected" as const, reason }));
      pending.push({ runId: entry.name, source, result });
      if (pending.length === 32) await flush();
    }
    await flush();
    return sources;
  } finally {
    // Iterator/read failure can occur with a partial batch. Drain every marker before returning/throwing and closing the root.
    await Promise.all(pending.map(candidate => candidate.result));
    await directory.close();
  }
}
