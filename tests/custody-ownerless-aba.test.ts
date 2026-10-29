import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshCustodyUnrecoverableError, acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("independent ownerless-grace review variant", () => {
  it("fails closed when a path disappears and is recreated with a recycled (dev, ino)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-review-variant-")); roots.push(root);
    const lock = path.join(root, "custody.lock"); fs.mkdirSync(lock);
    await expect(acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
      const original = fs.lstatSync(lock);
      fs.rmdirSync(lock); fs.mkdirSync(lock);
      const replacement = fs.lstatSync(lock);
      // Model a legal inode-number reuse (observed on this filesystem in a prior run). The lock code
      // receives the same dev/ino pair even though the path was absent and recreated between reads.
      const realLstat = fs.lstatSync.bind(fs);
      const recycled = vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
        const current = realLstat(target as fs.PathLike, options as never);
        if (String(target) !== lock || !current.isDirectory()) return current;
        return new Proxy(current, { get(stats, property) {
          if (property === "dev") return original.dev;
          if (property === "ino") return original.ino;
          return Reflect.get(stats, property, stats);
        } });
      }) as typeof fs.lstatSync);
      try {
        await expect(acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
        expect(fs.existsSync(lock)).toBe(true);
      } finally { recycled.mockRestore(); }
    expect(replacement.isDirectory()).toBe(true);
  });
});
