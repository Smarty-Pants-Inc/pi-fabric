import fs from "node:fs";
import path from "node:path";

const stamp = (file: string): string => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent";
    throw error;
  }
};

/** Stamp of one directory beside the mesh (`participants`, `host-leases`): it changes when a file
 * in it is created, replaced by rename or removed. Windows directory timestamps are not replacement
 * receipts, so there the leaf stamps are included. Validation only, never authority: a caller that
 * read the directory before the state transaction (smarty-dev#6477 R11) compares this stamp inside
 * the transaction and reads again only when it moved. */
export const meshDirectoryStamp = (meshRoot: string, name: string): string => {
  const directory = path.join(meshRoot, name);
  const files = [directory];
  if (process.platform === "win32") {
    try { files.push(...fs.readdirSync(directory).filter(file => file.endsWith(".json")).sort().map(file => path.join(directory, file))); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  }
  return files.map(stamp).join("|");
};

/** A mesh whose state backend can name its committed revision (smarty-dev#6477 R7). The `file`
 * backend has no such counter, so the stat of `state.json` stands in; a backend that commits
 * elsewhere (SQLite) supplies `stateRevision` from its `meta` counter, comparable across time
 * and connections. */
export interface PublicationGenerationSource {
  readonly root: string;
  stateRevision?(): string | undefined;
}

/** Cheap invalidation of a prepared ownership observation. Atomic replacements
 * change the file inode / parent change-time, even within one wall-clock tick.
 * This is validation, never authority; callers still prepare a fresh directory read. */
export const publicationGeneration = (mesh: string | PublicationGenerationSource): string => {
  const meshRoot = typeof mesh === "string" ? mesh : mesh.root;
  const revision = typeof mesh === "string" || typeof mesh.stateRevision !== "function" ? undefined : mesh.stateRevision();
  const state = revision === undefined ? stamp(path.join(meshRoot, "state.json")) : `revision:${revision}`;
  return [state, meshDirectoryStamp(meshRoot, "participants"), meshDirectoryStamp(meshRoot, "host-leases")].join("|");
};
