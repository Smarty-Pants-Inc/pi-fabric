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

/** Stream the run root via native asynchronous I/O; one marker/handle at a time, no full listing or queued fan-out.
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
  for await (const entry of directory) {
    if (!entry.isDirectory() || !/^[a-f0-9]{32}$/.test(entry.name)) continue;
    const source = path.join(runRoot, entry.name);
    if (await matches(source, entry.name, actorId, sessionFile)) sources.set(entry.name, source);
  }
  return sources;
}
