import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const { loads, archiveLoads } = vi.hoisted(() => ({ loads: vi.fn(), archiveLoads: vi.fn() }));
vi.mock("../src/storage/trace-archive.js", async original => {
  archiveLoads();
  return original<typeof import("../src/storage/trace-archive.js")>();
});
vi.mock("../src/storage/verified-remove.js", async original => {
  loads();
  return original<typeof import("../src/storage/verified-remove.js")>();
});
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  // Restore first-use seams after a test deliberately mutates during an import/copy.
  vi.doMock("../src/storage/verified-remove.js", async () => {
    loads();
    return vi.importActual<typeof import("../src/storage/verified-remove.js")>("../src/storage/verified-remove.js");
  });
  vi.doMock("../src/storage/trace-archive.js", async () => {
    archiveLoads();
    return vi.importActual<typeof import("../src/storage/trace-archive.js")>("../src/storage/trace-archive.js");
  });
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Startup/first-use proof for #7916: no product delete runs. Only fixture cleanup is real; every helper
// unlink/rmdir is recorded, so the production first-use path is exercised without executing its deletions.
describe.skipIf(process.platform !== "linux")("scratch verified remover loads at first removal, not startup", () => {
  it("stays unloaded through cold import, idle, empty sweeps and preview; loads once for eligible removals", async () => {
    vi.resetModules(); loads.mockClear(); archiveLoads.mockClear();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scratch-lazy-"));
    roots.push(root);
    const scratch = await import("../src/storage/scratch.js");
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(loads).not.toHaveBeenCalled();
    expect(archiveLoads).not.toHaveBeenCalled();
    expect((await scratch.sweepScratch({ tempRoot: root })).removed).toEqual([]);
    expect(loads).not.toHaveBeenCalled();
    expect(archiveLoads).not.toHaveBeenCalled();

    const active = fs.mkdtempSync(path.join(root, "pi-fabric-output-"));
    fs.writeFileSync(path.join(active, scratch.SCRATCH_OWNER_FILE), JSON.stringify({
      app: "pi-fabric-scratch", version: 1, kind: "output", pid: process.pid, createdAt: 1,
    }));
    fs.writeFileSync(path.join(active, "output.txt"), "live output");
    expect((await scratch.sweepScratch({ tempRoot: root, now: 100 * 3_600_000, maxItems: 0 })).eligible).toEqual([]);
    expect(loads).not.toHaveBeenCalled();
    expect(archiveLoads).not.toHaveBeenCalled();

    const fixtures = Array.from({ length: 2 }, () => {
      const directory = fs.mkdtempSync(path.join(root, "pi-fabric-output-"));
      fs.writeFileSync(path.join(directory, scratch.SCRATCH_OWNER_FILE), JSON.stringify({
        app: "pi-fabric-scratch", version: 1, kind: "output", pid: process.pid, createdAt: 1, closedAt: 1,
      }));
      fs.writeFileSync(path.join(directory, "output.txt"), "keep bytes");
      return directory;
    });
    const now = 100 * 3_600_000;
    expect((await scratch.sweepScratch({ tempRoot: root, now, dryRun: true })).eligible.sort()).toEqual([...fixtures].sort());
    expect(loads).not.toHaveBeenCalled();
    expect(archiveLoads).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, ".fabric-scratch-archive"))).toBe(false);
    const unlinks: string[] = [], rmdirs: string[] = [];
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { unlinks.push(String(file)); });
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => { rmdirs.push(String(file)); });
    const result = await scratch.sweepScratch({ tempRoot: root, now });
    expect(result.removed.sort()).toEqual([...fixtures].sort());
    expect(loads).toHaveBeenCalledTimes(1);
    expect(archiveLoads).toHaveBeenCalledTimes(1);
    expect(unlinks.map(file => path.basename(file)).sort()).toEqual([
      scratch.SCRATCH_OWNER_FILE, scratch.SCRATCH_OWNER_FILE, "output.txt", "output.txt",
    ].sort());
    expect(rmdirs).toHaveLength(4); // each target plus an attempted (recorded) empty-container cleanup
  });

  it("loads the remover before the final checks: output appended during the first-use import keeps the root", async () => {
    vi.resetModules(); loads.mockClear();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scratch-lazy-"));
    roots.push(root);
    const scratch = await import("../src/storage/scratch.js");
    const directory = fs.mkdtempSync(path.join(root, "pi-fabric-output-"));
    fs.writeFileSync(path.join(directory, scratch.SCRATCH_OWNER_FILE), JSON.stringify({
      app: "pi-fabric-scratch", version: 1, kind: "output", pid: process.pid, createdAt: 1, closedAt: 1,
    }));
    fs.writeFileSync(path.join(directory, "output.txt"), "keep bytes");
    const rootMtime = fs.lstatSync(directory).mtimeMs;
    // A writer appends while the module loads (an async window); the root's own mtime does not change. The hoisted
    // factory's result survives resetModules, so the same loader seam is re-registered for this first use.
    vi.doMock("../src/storage/verified-remove.js", async () => {
      loads();
      fs.appendFileSync(path.join(directory, "output.txt"), " and new output");
      return vi.importActual<typeof import("../src/storage/verified-remove.js")>("../src/storage/verified-remove.js");
    });
    const ops: string[] = [];
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { ops.push(`unlink ${String(file)}`); });
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => { ops.push(`rmdir ${String(file)}`); });
    vi.spyOn(fs, "rmSync").mockImplementation(file => { ops.push(`rm ${String(file)}`); });
    vi.spyOn(fs, "renameSync").mockImplementation(file => { ops.push(`rename ${String(file)}`); });
    const result = await scratch.sweepScratch({ tempRoot: root, now: 100 * 3_600_000 });
    expect(loads).toHaveBeenCalledTimes(1);
    expect(fs.lstatSync(directory).mtimeMs).toBe(rootMtime);
    expect(result).toMatchObject({ eligible: [directory], removed: [] });
    expect(ops).toEqual([]);
    expect(fs.readFileSync(path.join(directory, "output.txt"), "utf8")).toBe("keep bytes and new output");
  });

  it("rechecks after the archive's first-use import, before copying or removing", async () => {
    vi.resetModules(); loads.mockClear(); archiveLoads.mockClear();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scratch-lazy-"));
    roots.push(root);
    const scratch = await import("../src/storage/scratch.js");
    const directory = fs.mkdtempSync(path.join(root, "pi-fabric-output-"));
    fs.writeFileSync(path.join(directory, scratch.SCRATCH_OWNER_FILE), JSON.stringify({
      app: "pi-fabric-scratch", version: 1, kind: "output", pid: process.pid, createdAt: 1, closedAt: 1,
    }));
    fs.writeFileSync(path.join(directory, "output.txt"), "original full output");
    vi.doMock("../src/storage/trace-archive.js", async () => {
      archiveLoads();
      fs.appendFileSync(path.join(directory, "output.txt"), " plus late bytes");
      return vi.importActual<typeof import("../src/storage/trace-archive.js")>("../src/storage/trace-archive.js");
    });
    const deletes: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation(file => { deletes.push(String(file)); });
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { deletes.push(String(file)); });
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => { deletes.push(String(file)); });
    expect(await scratch.sweepScratch({ tempRoot: root, now: 100 * 3_600_000 })).toMatchObject({ eligible: [directory], removed: [] });
    expect(archiveLoads).toHaveBeenCalledTimes(1);
    expect(deletes).toEqual([]);
    expect(fs.existsSync(path.join(root, ".fabric-scratch-archive"))).toBe(false);
    expect(fs.readFileSync(path.join(directory, "output.txt"), "utf8")).toBe("original full output plus late bytes");
  });

  it.each(["late entry", "refreshed", "reactivated", "replaced"] as const)("a %s after verified archival still vetoes deletion", async change => {
    vi.resetModules(); loads.mockClear(); archiveLoads.mockClear();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scratch-lazy-"));
    roots.push(root);
    const scratch = await import("../src/storage/scratch.js");
    const directory = fs.mkdtempSync(path.join(root, "pi-fabric-output-"));
    const marker = path.join(directory, scratch.SCRATCH_OWNER_FILE);
    fs.writeFileSync(marker, JSON.stringify({
      app: "pi-fabric-scratch", version: 1, kind: "output", pid: process.pid, createdAt: 1, closedAt: 1,
    }));
    fs.writeFileSync(path.join(directory, "output.txt"), "original full output");
    vi.doMock("../src/storage/trace-archive.js", async () => {
      archiveLoads();
      const original = await vi.importActual<typeof import("../src/storage/trace-archive.js")>("../src/storage/trace-archive.js");
      return { ...original, archiveTraceTreeCopy: (...args: Parameters<typeof original.archiveTraceTreeCopy>) => {
        const copy = original.archiveTraceTreeCopy(...args);
        if (change === "late entry") fs.writeFileSync(path.join(directory, "unknown.jsonl"), "late trace\n");
        else if (change === "refreshed") fs.appendFileSync(path.join(directory, "output.txt"), " plus late bytes");
        else if (change === "reactivated") {
          const owner = JSON.parse(fs.readFileSync(marker, "utf8"));
          delete owner.closedAt;
          fs.writeFileSync(marker, JSON.stringify(owner));
        } else {
          fs.renameSync(directory, `${directory}.saved`);
          fs.mkdirSync(directory);
          fs.writeFileSync(path.join(directory, "mine.txt"), "new directory, not ours to remove");
        }
        return copy;
      } };
    });
    const deletes: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation(file => { deletes.push(String(file)); });
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { deletes.push(String(file)); });
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => { deletes.push(String(file)); });
    expect(await scratch.sweepScratch({ tempRoot: root, now: 100 * 3_600_000 })).toMatchObject({ eligible: [directory], removed: [] });
    expect(archiveLoads).toHaveBeenCalledTimes(1);
    expect(deletes).toEqual([]);
    expect(fs.existsSync(directory)).toBe(true);
    const receipts = fs.readFileSync(path.join(root, ".fabric-scratch-archive", "MANIFEST.jsonl"), "utf8")
      .trim().split("\n").map(line => JSON.parse(line));
    const receipt = receipts.find(row => row.kind === "tree" && row.from === directory);
    expect(receipt).toBeDefined();
    expect(fs.readFileSync(path.join(receipt.to, "output.txt"), "utf8")).toBe("original full output");
    if (change === "late entry") expect(fs.readFileSync(path.join(directory, "unknown.jsonl"), "utf8")).toBe("late trace\n");
    else if (change === "refreshed") expect(fs.readFileSync(path.join(directory, "output.txt"), "utf8")).toBe("original full output plus late bytes");
    else if (change === "reactivated") expect(JSON.parse(fs.readFileSync(marker, "utf8")).closedAt).toBeUndefined();
    else expect(fs.readFileSync(path.join(directory, "mine.txt"), "utf8")).toBe("new directory, not ours to remove");
  });
});
