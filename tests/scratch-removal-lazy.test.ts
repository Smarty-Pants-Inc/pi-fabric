import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
vi.mock("../src/storage/verified-remove.js", async original => {
  loads();
  return original<typeof import("../src/storage/verified-remove.js")>();
});
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Startup/first-use proof for #7916: no product delete runs. Only fixture cleanup is real; every helper
// unlink/rmdir is recorded, so the production first-use path is exercised without executing its deletions.
describe.skipIf(process.platform !== "linux")("scratch verified remover loads at first removal, not startup", () => {
  it("stays unloaded through cold import, idle, empty sweeps and preview; loads once for eligible removals", async () => {
    vi.resetModules(); loads.mockClear();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scratch-lazy-"));
    roots.push(root);
    const scratch = await import("../src/storage/scratch.js");
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(loads).not.toHaveBeenCalled();
    expect((await scratch.sweepScratch({ tempRoot: root })).removed).toEqual([]);
    expect(loads).not.toHaveBeenCalled();

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
    const unlinks: string[] = [], rmdirs: string[] = [];
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { unlinks.push(String(file)); });
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => { rmdirs.push(String(file)); });
    const result = await scratch.sweepScratch({ tempRoot: root, now });
    expect(result.removed.sort()).toEqual([...fixtures].sort());
    expect(loads).toHaveBeenCalledTimes(1);
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
});
