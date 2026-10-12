import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// pi-fabric#913 round 4 (Astra P1): the Windows helper gets each entry's judged last-write time (a FILETIME) and size,
// so it can keep a file refreshed after the scan with the same id. This runs off Windows too: the platform is forced
// to win32, the cross-built helper is found by its digest, and only the spawn is recorded (nothing runs).
const spawnSync = vi.hoisted(() => vi.fn());
vi.mock("node:child_process", async (original) => ({ ...await original<typeof import("node:child_process")>(), spawnSync }));
const { setWindowsVerifiedRemoval, verifiedRemove, windowsRemoveHelper } = await import("../src/storage/verified-remove.js");

const dirs: string[] = [];
afterEach(() => {
  setWindowsVerifiedRemoval(false);
  vi.restoreAllMocks();
  spawnSync.mockReset();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe.runIf(process.platform === "linux")("Windows helper arguments (pi-fabric#913 round 4)", () => {
  const forceWindows = () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.spyOn(os, "type").mockReturnValue("Windows_NT");
    setWindowsVerifiedRemoval(true);
  };
  const judgedFile = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "win-remove-args-")); dirs.push(dir);
    const file = path.join(dir, "old.bak");
    fs.writeFileSync(file, "x".repeat(10));
    return file;
  };
  const expectedEntry = (file: string) => {
    const stat = fs.lstatSync(file, { bigint: true });
    return ["f", stat.dev.toString(16), stat.ino.toString(16), (stat.mtimeNs / 100n + 116_444_736_000_000_000n).toString(16), "a", file];
  };

  it("passes each entry's judged last-write time and size to the helper", () => {
    const file = judgedFile();
    forceWindows();
    const helper = windowsRemoveHelper();
    expect(helper).toMatch(/fabric-win-remove\.exe$/);
    spawnSync.mockReturnValue({ status: 0, stdout: "ok 1 removed\n" });
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test" })).toEqual({ removed: true, bytes: 10 });
    expect(spawnSync).toHaveBeenCalledTimes(1);
    expect(spawnSync.mock.calls[0]![0]).toBe(helper);
    expect(spawnSync.mock.calls[0]![1]).toEqual(expectedEntry(file));
  });

  it("reports the helper's keep of a refreshed entry as kept by the final check", () => {
    const file = judgedFile();
    forceWindows();
    spawnSync.mockReturnValue({ status: 4, stdout: "kept 0 modified since judgment\n" });
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test" }))
      .toEqual({ removed: false, reason: `kept by the final check (${file} modified since judgment)` });
  });
});
