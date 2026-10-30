import fs from "node:fs";
import path from "node:path";

/** Resolve a source run either in place or through its durable whole-root custody receipt. */
export const retainedRunDirectory = (directory: string): string => {
  if (fs.existsSync(path.join(directory, "status.json"))) return directory;
  let source = directory;
  while (path.dirname(source) !== source) {
    const parent = path.dirname(source), archive = `${parent}.trace-archive`;
    try {
      for (const name of fs.readdirSync(archive)) {
        const receipt = JSON.parse(fs.readFileSync(path.join(archive, name, "custody.json"), "utf8")) as {
          state?: string; source?: string; destination?: string;
        };
        if (receipt.state === "moved" && receipt.source === source && typeof receipt.destination === "string") {
          return path.join(receipt.destination, path.relative(source, directory));
        }
      }
    } catch { /* This ancestor is not a matching whole-root archive. */ }
    source = parent;
  }
  return directory;
};
