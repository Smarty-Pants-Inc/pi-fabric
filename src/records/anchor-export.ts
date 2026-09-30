import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { parseAnchors, type RecordsAnchor } from "./chain.js";

/** Each file fits the existing verify-chain anchor limit, with room to spare. */
export const ANCHORS_PER_SEGMENT = 9_999;
const SEGMENT = /^anchors-([0-9]{12})\.jsonl$/;
const owned = (stat: fs.Stats): boolean => process.getuid === undefined || stat.uid === process.getuid();

/**
 * Service-owned, append-only JSONL. A flock serializes publishers, including across restarts.
 * Only the newest, non-full segment is appendable. A torn final line seals that segment: its
 * newline-terminated prefix is valid, and no recovery ever truncates or rewrites its bytes.
 */
export class AnchorExport {
  constructor(readonly directory: string, readonly takeLock: (file: string) => Promise<number>) {}

  async publish(anchor: RecordsAnchor): Promise<RecordsAnchor> {
    await fs.promises.mkdir(this.directory, { recursive: true, mode: 0o755 });
    const directory = await fs.promises.open(this.directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
    let lock: number | undefined;
    try {
      const stat = await directory.stat();
      if (!stat.isDirectory() || !owned(stat)) throw new Error("anchor export directory must be owned by the records user");
      await directory.chmod(0o755); // UMask=0007 must not keep the factory from traversing it.
      // A fresh recursive mkdir also needs its parent entries durable. Do this on retries too,
      // which may encounter directories left behind by an earlier failed publication.
      for (let dir = path.dirname(this.directory); ; dir = path.dirname(dir)) {
        const parent = await fs.promises.open(dir, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
        let stop: boolean;
        try { stop = !owned(await parent.stat()) || dir === path.dirname(dir); await parent.sync(); } finally { await parent.close(); }
        if (stop) break;
      }
      lock = await this.takeLock(path.join(this.directory, ".publish.lock"));
      const names = (await fs.promises.readdir(this.directory)).filter((name) => SEGMENT.test(name)).sort();
      const name = names.at(-1);
      let number = name ? Number(SEGMENT.exec(name)![1]) : 0;
      let lines: string[] = [];
      let torn = false;
      if (name) {
        const current = await fs.promises.open(path.join(this.directory, name), fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
        try {
          const stat = await current.stat();
          if (!stat.isFile() || !owned(stat) || (stat.mode & 0o777) !== 0o644) throw new Error("anchor segment must be a records-owned regular file with mode 0644");
          // A segment is bounded; reject unexpected growth instead of reading an unbounded file.
          if (stat.size > 16 * 1024 * 1024) throw new Error("anchor segment is too large");
          const text = await current.readFile("utf8");
          torn = !text.endsWith("\n");
          lines = text.slice(0, text.lastIndexOf("\n") + 1).split("\n").slice(0, -1);
        } finally { await current.close(); }
      }
      if (lines.length > ANCHORS_PER_SEGMENT) throw new Error("anchor segment has too many anchors");
      const anchors = lines.map((line) => JSON.parse(line) as RecordsAnchor);
      parseAnchors(anchors);
      if (anchors.some((item) => item.org !== anchor.org || typeof item.at !== "string")) throw new Error("anchor segment belongs to another org or has an invalid timestamp");
      const last = anchors.at(-1);
      if (last?.seq === anchor.seq && last.hash === anchor.hash) {
        // Also retry durability after an earlier fsync failure, before reporting success.
        const current = await fs.promises.open(path.join(this.directory, name!), fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
        try { await current.sync(); } finally { await current.close(); }
        await directory.sync();
        return last;
      }
      const line = Buffer.from(`${JSON.stringify(anchor)}\n`, "utf8");
      if (name && !torn && lines.length < ANCHORS_PER_SEGMENT) {
        const current = await fs.promises.open(path.join(this.directory, name), fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_NOFOLLOW);
        try {
          const { bytesWritten } = await current.write(line);
          await current.sync();
          if (bytesWritten !== line.length) throw new Error("short anchor append; incomplete tail retained");
        } finally { await current.close(); }
      } else {
        if (++number > 999_999_999_999) throw new Error("anchor segment numbers exhausted");
        const file = path.join(this.directory, `anchors-${String(number).padStart(12, "0")}.jsonl`);
        const temp = path.join(this.directory, `.anchor-${process.pid}-${randomBytes(8).toString("hex")}.tmp`);
        try {
          const pending = await fs.promises.open(temp, "wx", 0o600);
          try {
            await pending.writeFile(line);
            await pending.chmod(0o644);
            await pending.sync();
          } finally { await pending.close(); }
          // Under the flock, no publisher can race this rename or replace an existing segment.
          try {
            await fs.promises.lstat(file);
            throw new Error("refusing to replace an existing anchor segment");
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
          }
          await fs.promises.rename(temp, file);
        } finally { await fs.promises.rm(temp, { force: true }); }
      }
      await directory.sync();
      return anchor;
    } finally {
      if (lock !== undefined) fs.closeSync(lock);
      await directory.close();
    }
  }
}
