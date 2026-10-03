import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as scopes from "../src/storage/process-scratch-scope.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, JOINED_SCRATCH_FILE, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "scratch-close-custody-"));
  roots.push(root);
  const scope: scopes.ProcessScratchScope = {
    directory: "/sys/fs/cgroup/pi-fabric-scratch-00000000-0000-0000-0000-000000000001",
    dev: 1, ino: 2, bootId: "test-boot",
  };
  vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(scope);
  vi.spyOn(scopes, "checkedProcessScratchScope").mockImplementation(value =>
    value && JSON.stringify(value) === JSON.stringify(scope) ? scope : undefined);
  const remove = vi.spyOn(scopes, "removeEmptyProcessScratchScope").mockReturnValue(false);
  return { root, scope, remove };
};

describe.skipIf(process.platform === "win32")("captured scoped native-close custody", () => {
  it("joins a pre-receipt close but cannot collect a populated scope", () => {
    const { root, scope, remove } = setup();
    const allocation = allocateRunTmpDirectory(root);
    fs.writeFileSync(path.join(allocation.directory, "writer"), "retained");
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(remove).not.toHaveBeenCalled();
    allocation.workerClosed(123);
    expect(JSON.parse(fs.readFileSync(path.join(root, JOINED_SCRATCH_FILE), "utf8")))
      .toMatchObject({ ...scope, launchNonce: allocation.scope!.launchNonce });
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(fs.readFileSync(path.join(allocation.directory, "writer"), "utf8")).toBe("retained");
    expect(fs.existsSync(path.join(root, UNRESOLVED_SCRATCH_FILE))).toBe(true);
    remove.mockReturnValue(true);
    expect(disposeRunTmpDirectory(root)).toBe(true);
    expect(remove).toHaveBeenLastCalledWith(scope);
    expect(fs.existsSync(allocation.directory)).toBe(false);
  });

  it("cannot let an old captured close arm a pending retry generation", () => {
    const { root, remove } = setup();
    const first = allocateRunTmpDirectory(root);
    const second = allocateRunTmpDirectory(root);
    first.workerClosed(123);
    expect(fs.existsSync(path.join(root, JOINED_SCRATCH_FILE))).toBe(false);
    remove.mockReturnValue(true);
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(remove).not.toHaveBeenCalled();
    second.workerClosed(456);
    const joined = fs.readFileSync(path.join(root, JOINED_SCRATCH_FILE), "utf8");
    first.workerClosed(123);
    expect(fs.readFileSync(path.join(root, JOINED_SCRATCH_FILE), "utf8")).toBe(joined);
    expect(JSON.parse(joined).launchNonce).toBe(second.scope!.launchNonce);
    expect(disposeRunTmpDirectory(root)).toBe(true);
  });
});
