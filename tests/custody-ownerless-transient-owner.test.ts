import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshCustodyUnrecoverableError, acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("transient owner receipt custody review", () => {
  it("does not transfer stale ownerless grace across an unobserved owner create/unlink", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-transient-owner-")); roots.push(root);
    const lock = path.join(root, "custody.lock"); fs.mkdirSync(lock);
    const clock = vi.spyOn(performance, "now").mockReturnValueOnce(0).mockReturnValue(30_001);
    const realStat = fs.statSync.bind(fs);
    const coarseMetadata = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
      const stat = realStat(target, options as never);
      if (String(target) !== lock || typeof options !== "object" || options === null || !("bigint" in options) || !options.bigint) return stat;
      // Model a filesystem whose directory ctime/birthtime observations alias within one timestamp granule.
      return new Proxy(stat, { get(value, property) {
        if (property === "ctimeNs" || property === "birthtimeNs") return 1_000_000_000n;
        return Reflect.get(value, property, value);
      } });
    }) as typeof fs.statSync);
    try {
      let first: unknown;
      try { await acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined }); }
      catch (error) { first = error; }
      expect(first).toBeDefined();

      // A different owner generation appears and vanishes entirely between this contender's reads.
      fs.writeFileSync(path.join(lock, "owner"), `transient\n${process.pid}\n${Date.now()}\n`);
      fs.unlinkSync(path.join(lock, "owner"));
      let release: (() => void) | undefined;
      let second: unknown;
      try { release = await acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined }); }
      catch (error) { second = error; }
      finally { release?.(); }
      expect(second).toBeInstanceOf(MeshCustodyUnrecoverableError);
      expect(fs.existsSync(lock)).toBe(true);
      expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
    } finally { coarseMetadata.mockRestore(); clock.mockRestore(); }
  });
});
