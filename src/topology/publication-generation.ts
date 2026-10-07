import fs from "node:fs";
import path from "node:path";

/** Cheap invalidation of a prepared ownership observation. Atomic replacements
 * change the file inode / parent change-time, even within one wall-clock tick.
 * Windows directory timestamps are not replacement receipts: include leaf stamps.
 * This is validation, never authority; callers still prepare a fresh directory read. */
export const publicationGeneration = (meshRoot: string): string => {
  const stamp = (file: string): string => {
    try {
      const stat = fs.statSync(file, { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent";
      throw error;
    }
  };
  const files = [path.join(meshRoot, "state.json")];
  for (const name of ["participants", "host-leases"]) {
    const directory = path.join(meshRoot, name);
    files.push(directory);
    if (process.platform === "win32") {
      try { files.push(...fs.readdirSync(directory).filter(file => file.endsWith(".json")).sort().map(file => path.join(directory, file))); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    }
  }
  return files.map(stamp).join("|");
};
