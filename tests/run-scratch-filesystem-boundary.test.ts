import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as scopes from "../src/storage/process-scratch-scope.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, hasNeverStartedReceipt, safeRunTmpTree, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// No mount privileges required: only st_dev changes. Real directories, files,
// custody receipts and deletion calls exercise the production cleanup paths.
describe.skipIf(process.platform === "win32")("D17 scratch filesystem boundaries", () => {
  it.each(["unscoped", "scoped", "never-started"] as const)("refuses a nested mount before traversal or deletion (%s)", mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "scratch-device-")); roots.push(root);
    const scope: scopes.ProcessScratchScope = { directory: "/sys/fs/cgroup/pi-fabric-scratch-00000000-0000-0000-0000-000000000001", dev: 1, ino: 2, bootId: "test-boot" };
    vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(mode === "scoped" ? scope : undefined);
    vi.spyOn(scopes, "checkedProcessScratchScope").mockImplementation(value => value ? scope : undefined);
    const removeScope = vi.spyOn(scopes, "removeEmptyProcessScratchScope").mockReturnValue(true);
    const allocation = allocateRunTmpDirectory(root);
    if (mode !== "never-started") {
      allocation.workerClosed(2147483647);
      fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647" }));
    }
    const mounted = path.join(allocation.directory, "mounted"), data = path.join(mounted, "data");
    fs.mkdirSync(mounted); fs.writeFileSync(data, "foreign filesystem: preserve");
    const fence = fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8");
    const lstat = fs.lstatSync;
    const device = vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      const stat = lstat(file);
      if (String(file) === mounted) stat.dev += 1;
      return stat;
    }) as typeof fs.lstatSync);
    const readdir = vi.spyOn(fs, "readdirSync"), removal = vi.spyOn(fs, "rmSync"), warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    if (mode === "never-started") allocation.neverStarted();
    else expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(removal).not.toHaveBeenCalled();
    expect(readdir.mock.calls.some(([file]) => String(file) === mounted)).toBe(false);
    expect(removeScope).not.toHaveBeenCalled();
    expect(fs.readFileSync(data, "utf8")).toBe("foreign filesystem: preserve");
    expect(fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8")).toBe(fence);
    expect(hasNeverStartedReceipt(root)).toBe(false);
    expect(warning).toHaveBeenCalledWith(expect.stringContaining("filesystem boundary"));
    expect(warning).toHaveBeenCalledWith(expect.stringContaining(mounted));
    device.mockRestore();
    // A refusal does not corrupt custody: normal same-device cleanup can retry.
    if (mode === "never-started") { allocation.neverStarted(); expect(hasNeverStartedReceipt(root)).toBe(true); }
    else expect(disposeRunTmpDirectory(root)).toBe(true);
    expect(fs.existsSync(allocation.directory)).toBe(false);
  });

  it.each(["root", "file"] as const)("rejects a foreign-device scratch %s with a recorded reason", boundary => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "scratch-device-")); roots.push(root);
    const tmp = path.join(root, "tmp"), file = path.join(tmp, "file");
    fs.mkdirSync(tmp); fs.writeFileSync(file, "keep");
    const foreign = boundary === "root" ? tmp : file, lstat = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike) => {
      const stat = lstat(target);
      if (String(target) === foreign) stat.dev += 1;
      return stat;
    }) as typeof fs.lstatSync);
    const readdir = vi.spyOn(fs, "readdirSync"), warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(safeRunTmpTree(tmp, () => false)).toBe(false);
    if (boundary === "root") expect(readdir).not.toHaveBeenCalled();
    expect(warning).toHaveBeenCalledWith(expect.stringContaining("filesystem boundary"));
    expect(fs.readFileSync(file, "utf8")).toBe("keep");
  });
});
