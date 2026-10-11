import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { verifiedRemove, verifiedRemoveTestHooks, windowsPrivate, windowsRemoveHelper } from "../src/storage/verified-remove.js";

// smarty-dev#7858: Windows verified removal through native/fabric-win-remove.c (built and digest-pinned by the build;
// the Windows CI job builds it with the pinned Windows zig). Owner SID, volume serial and file index are checked on
// the HANDLE, reparse points are opened as themselves, and the delete is by handle.
const dirs: string[] = [];
afterEach(() => {
  delete verifiedRemoveTestHooks.beforeHelper;
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const tempDir = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-win-remove-")); dirs.push(dir); return dir; };
const ids = (file: string) => { const stat = fs.lstatSync(file, { bigint: true }); return [stat.dev.toString(16), stat.ino.toString(16)]; };

describe.skipIf(process.platform !== "win32")("Windows verified removal (smarty-dev#7858)", () => {
  it("has the digest-verified helper in this build", () => {
    expect(windowsRemoveHelper()).toMatch(/fabric-win-remove\.exe$/);
  });

  it("removes our own file and a directory tree by handle", () => {
    const dir = tempDir();
    const file = path.join(dir, "old.bak");
    fs.writeFileSync(file, "x".repeat(10));
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test" })).toEqual({ removed: true, bytes: 10 });
    expect(fs.existsSync(file)).toBe(false);
    const tree = path.join(dir, "run");
    fs.mkdirSync(path.join(tree, "nested"), { recursive: true });
    fs.writeFileSync(path.join(tree, "nested", "events.jsonl"), "{}\n");
    expect(verifiedRemove(tree, fs.lstatSync(tree), { deleter: "test" })).toMatchObject({ removed: true });
    expect(fs.existsSync(tree)).toBe(false);
  });

  it("keeps a file whose id changed after it was judged", () => {
    const dir = tempDir();
    const file = path.join(dir, "old.bak");
    fs.writeFileSync(file, "judged");
    const judged = fs.lstatSync(file);
    const [volume, index] = ids(file);
    fs.unlinkSync(file);
    fs.writeFileSync(file, "replacement");
    expect(verifiedRemove(file, judged, { deleter: "test" })).toMatchObject({ removed: false, reason: expect.stringContaining("replaced since judgment") });
    // The helper itself, given the judged id for the replaced name, refuses on the handle.
    const helper = spawnSync(windowsRemoveHelper()!, ["f", volume!, index!, path.toNamespacedPath(file)], { encoding: "utf8", windowsHide: true });
    expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 file id changed since judgment" });
    expect(fs.readFileSync(file, "utf8")).toBe("replacement");
  });

  // pi-fabric#913 (Astra P1): an ancestor in the judged tree swapped for a junction AFTER the walk and before the
  // helper opens the entries: the helper opens the junction itself and refuses; nothing is deleted anywhere.
  it("refuses an ancestor swapped for a junction after the walk, and deletes nothing", () => {
    const dir = tempDir();
    const tree = path.join(dir, "run");
    const nested = path.join(tree, "nested");
    fs.mkdirSync(nested, { recursive: true });
    fs.writeFileSync(path.join(nested, "events.jsonl"), "{}\n");
    verifiedRemoveTestHooks.beforeHelper = () => {
      fs.renameSync(nested, `${nested}.real`);
      fs.symlinkSync(`${nested}.real`, nested, "junction");
    };
    expect(verifiedRemove(tree, fs.lstatSync(tree), { deleter: "test" })).toMatchObject({ removed: false, reason: expect.stringContaining("reparse point") });
    expect(fs.readFileSync(path.join(`${nested}.real`, "events.jsonl"), "utf8")).toBe("{}\n");
    expect(fs.existsSync(tree)).toBe(true);
  });

  // pi-fabric#913 (Astra P1): a record we own whose DACL lets another SID write it is not private.
  it("accepts our private record and refuses one that another SID may write", () => {
    const file = path.join(tempDir(), "approval.json");
    fs.writeFileSync(file, "{}");
    expect(windowsPrivate(file, fs.lstatSync(file, { bigint: true }))).toBe(true);
    const granted = spawnSync("icacls", [file, "/grant", "*S-1-1-0:(W)"], { encoding: "utf8", windowsHide: true });
    expect(granted.status, granted.stdout + granted.stderr).toBe(0);
    expect(windowsPrivate(file, fs.lstatSync(file, { bigint: true }))).toBe(false);
    const [volume, index] = ids(file);
    const helper = spawnSync(windowsRemoveHelper()!, ["--private", volume!, index!, path.toNamespacedPath(file)], { encoding: "utf8", windowsHide: true });
    expect(helper.stdout.trim()).toBe("refused 0 writable by another SID");
  });

  it("never follows a reparse point: a junction in the tree refuses the removal, and its target is untouched", () => {
    const dir = tempDir();
    const outside = path.join(tempDir(), "keep.txt");
    fs.writeFileSync(outside, "keep");
    const tree = path.join(dir, "run");
    fs.mkdirSync(tree);
    const junction = path.join(tree, "link");
    fs.symlinkSync(path.dirname(outside), junction, "junction");
    expect(verifiedRemove(tree, fs.lstatSync(tree), { deleter: "test" })).toMatchObject({ removed: false, reason: expect.stringContaining("reparse point") });
    // The helper opens the junction itself (FILE_FLAG_OPEN_REPARSE_POINT) and refuses it.
    const [volume, index] = ids(junction);
    const helper = spawnSync(windowsRemoveHelper()!, ["d", volume!, index!, path.toNamespacedPath(junction)], { encoding: "utf8", windowsHide: true });
    expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 a reparse point" });
    expect(fs.readFileSync(outside, "utf8")).toBe("keep");
    expect(fs.existsSync(junction)).toBe(true);
  });
});
