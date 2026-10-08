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

/** A mesh whose ACTIVE state backend names its committed revision (smarty-dev#6477 R7, MeshStore).
 * `stateRevision()` is undefined when state.json is the authority (file, shadow): its stat stands
 * in. A backend that commits elsewhere (SQLite) returns its commit stamp, comparable across time
 * and connections. Required, not optional: a source without it would silently stamp a state.json
 * that SQLite commits never update (pi-fabric#640 review round 1, P1). */
export interface PublicationGenerationSource {
  readonly root: string;
  stateRevision(): string | undefined;
}

/** Cheap invalidation of a prepared ownership observation. Atomic replacements
 * change the file inode / parent change-time, even within one wall-clock tick.
 * This is validation, never authority; callers still prepare a fresh directory read. */
export const publicationGeneration = (mesh: string | PublicationGenerationSource): string => {
  const meshRoot = typeof mesh === "string" ? mesh : mesh.root;
  // A bare root names a file-backed mesh (tests, tools without a store); a store names its backend.
  const revision = typeof mesh === "string" ? undefined : mesh.stateRevision();
  const state = revision === undefined ? stamp(path.join(meshRoot, "state.json")) : `revision:${revision}`;
  return [state, meshDirectoryStamp(meshRoot, "participants"), meshDirectoryStamp(meshRoot, "host-leases")].join("|");
};
