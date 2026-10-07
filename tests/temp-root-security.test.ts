import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fabricDataRoot } from "../src/storage/temp-root.js";

const roots: string[] = [];
const sandbox = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "data-root-security-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("review F2: file-data namespace safety", () => {
  it("fails closed when native Windows ACL inspection fails, preserving the unset OS-temp fallback", () => {
    const directory = path.join(sandbox(), "windows-data");
    vi.spyOn(childProcess, "execFileSync").mockImplementation(() => { throw new Error("ACL inspection unavailable"); });
    vi.stubEnv("SystemRoot", "C:\\Windows");
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      Object.defineProperty(process, "platform", { value: "win32" });
      vi.stubEnv("PI_FABRIC_TMPDIR", "C:\\private\\windows-data");
      expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*could not prove native Windows ACL/);
      expect(fs.existsSync(directory)).toBe(false);
      vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
      expect(fabricDataRoot()).toBe(os.tmpdir());
    } finally { Object.defineProperty(process, "platform", platform); }
  });
  it.skipIf(process.platform === "win32").each([0o702, 0o720, 0o1777])("rejects an existing other-writable root (%s), without chmod", mode => {
    const directory = path.join(sandbox(), "unsafe");
    fs.mkdirSync(directory);
    fs.chmodSync(directory, mode);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*writable/);
    expect(fs.statSync(directory).mode & 0o7777).toBe(mode);
  });

  it.skipIf(process.platform === "win32")("rejects a foreign-owned root without chmod", () => {
    const directory = path.join(sandbox(), "foreign");
    fs.mkdirSync(directory, { mode: 0o700 });
    const lstat = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      const stat = lstat(file);
      if (String(file) === directory) Object.defineProperty(stat, "uid", { value: process.getuid!() + 1 });
      return stat;
    }) as typeof fs.lstatSync);
    const chmod = vi.spyOn(fs, "chmodSync");
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*owned/);
    expect(chmod).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32").each([false, true])("rejects a replaceable ancestor before creating data (existing root=%s)", existing => {
    const ancestor = path.join(sandbox(), "shared");
    fs.mkdirSync(ancestor);
    fs.chmodSync(ancestor, 0o777);
    const parent = path.join(ancestor, "private");
    fs.mkdirSync(parent, { mode: 0o700 });
    const directory = path.join(parent, "data");
    if (existing) fs.mkdirSync(directory, { mode: 0o700 });
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*writable/);
    expect(fs.existsSync(directory)).toBe(existing);
  });

  it.skipIf(process.platform === "win32").each([false, true])("rejects symlink redirection, including trailing separators (ancestor=%s)", ancestor => {
    const root = sandbox();
    const target = path.join(root, "target");
    fs.mkdirSync(target);
    const link = path.join(root, "link");
    fs.symlinkSync(target, link, process.platform === "win32" ? "junction" : "dir");
    vi.stubEnv("PI_FABRIC_TMPDIR", (ancestor ? path.join(link, "new-data") : link) + path.sep);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*real directory/);
    expect(fs.readdirSync(target)).toEqual([]);
  });

  it.skipIf(process.platform === "win32")("rejects foreign-owned ancestors even when sticky", () => {
    const ancestor = path.join(sandbox(), "foreign-ancestor");
    fs.mkdirSync(ancestor);
    fs.chmodSync(ancestor, 0o1777);
    const directory = path.join(ancestor, "data");
    const lstat = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      const stat = lstat(file);
      if (String(file) === ancestor) Object.defineProperty(stat, "uid", { value: process.getuid!() + 1 });
      return stat;
    }) as typeof fs.lstatSync);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*owned/);
    expect(fs.existsSync(directory)).toBe(false);
  });

  // #369 r9: umask-002 hosts create 0775 user-owned ancestors under a private home.
  // Make every real user-owned ancestor above `top` group-traversable, so only
  // the chain built by the test can provide (or withhold) a private boundary.
  const withoutOuterBoundary = (top: string, overrides: Record<string, { uid?: number; gid?: number }> = {}) => {
    const lstat = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      const stat = lstat(file);
      const name = path.resolve(String(file));
      if (name.length < top.length && top.startsWith(name === path.sep ? name : name + path.sep) && stat.uid === process.getuid!()) {
        Object.defineProperty(stat, "mode", { value: stat.mode | 0o055 });
      }
      if (overrides[name]?.uid !== undefined) Object.defineProperty(stat, "uid", { value: overrides[name]!.uid });
      if (overrides[name]?.gid !== undefined) Object.defineProperty(stat, "gid", { value: overrides[name]!.gid });
      return stat;
    }) as typeof fs.lstatSync);
  };
  const groupChain = (base: string): { shared: string; directory: string } => {
    const shared = path.join(base, "local");
    const share = path.join(shared, "share");
    fs.mkdirSync(share, { recursive: true });
    fs.chmodSync(shared, 0o775);
    fs.chmodSync(share, 0o775);
    return { shared, directory: path.join(share, "residency", "runs") };
  };

  it.skipIf(process.platform === "win32")("accepts 0775 user-owned ancestors below a 0700 user-owned boundary (umask 002)", () => {
    const home = sandbox();
    fs.chmodSync(home, 0o700);
    const { shared, directory } = groupChain(home);
    withoutOuterBoundary(home);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(fabricDataRoot()).toBe(directory);
    expect(fs.statSync(directory).mode & 0o777).toBe(0o700);
    expect(fs.statSync(shared).mode & 0o777).toBe(0o775);
  });

  it.skipIf(process.platform === "win32").each([0o755, 0o750, 0o775])("refuses the same 0775 chain without a private boundary (top %s)", mode => {
    const home = sandbox();
    fs.chmodSync(home, mode);
    const { directory } = groupChain(home);
    withoutOuterBoundary(home);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*(local|home|data-root).* is writable by other users/);
    expect(fs.existsSync(path.dirname(directory))).toBe(false);
  });

  // smarty-dev#4010 N1: 0701/0705 let any user traverse to the group bit below.
  it.skipIf(process.platform === "win32").each([{ mode: 0o701, label: "0701" }, { mode: 0o705, label: "0705" }, { mode: 0o711, label: "0711" }, { mode: 0o741, label: "0741" }])("refuses a 0775 chain below a traversable $label home (smarty-dev#4010 N1)", ({ mode }) => {
    const home = sandbox();
    fs.chmodSync(home, mode);
    const { directory } = groupChain(home);
    withoutOuterBoundary(home);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*local is writable by other users/);
    expect(fs.existsSync(path.dirname(directory))).toBe(false);
  });

  it.skipIf(process.platform === "win32").each([{ mode: 0o740, label: "0740" }, { mode: 0o744, label: "0744" }])("seals a same-group 0775 chain below a no-traverse $label home, refusing another group (smarty-dev#4010 N1)", ({ mode }) => {
    const home = sandbox();
    fs.chmodSync(home, mode);
    const { shared, directory } = groupChain(home);
    withoutOuterBoundary(home);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(fabricDataRoot()).toBe(directory);
    fs.rmSync(path.dirname(directory), { recursive: true });
    vi.restoreAllMocks();
    withoutOuterBoundary(home, { [shared]: { gid: fs.statSync(home).gid + 1 } });
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*local is writable by other users/);
    expect(fs.existsSync(path.dirname(directory))).toBe(false);
  });

  it.skipIf(process.platform === "win32")("an owner-only 0700 home seals a 0775 chain of any group (smarty-dev#4010 N1)", () => {
    const home = sandbox();
    fs.chmodSync(home, 0o700);
    const { shared, directory } = groupChain(home);
    withoutOuterBoundary(home, { [shared]: { gid: fs.statSync(home).gid + 1 } });
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(fabricDataRoot()).toBe(directory);
  });

  it.skipIf(process.platform === "win32").each([{ uid: "other" }, { uid: "root" }])("refuses a writable $uid-owned directory in the chain even below a private boundary", ({ uid }) => {
    const home = sandbox();
    fs.chmodSync(home, 0o700);
    const { shared, directory } = groupChain(home);
    withoutOuterBoundary(home, { [shared]: { uid: uid === "root" ? 0 : process.getuid!() + 1 } });
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(uid === "root" ? /PI_FABRIC_TMPDIR.*local is writable by other users/ : /PI_FABRIC_TMPDIR.*local is owned by another user/);
    expect(fs.existsSync(path.dirname(directory))).toBe(false);
  });

  it.skipIf(process.platform === "win32")("keeps refusing other-writable and final group-writable roots below a private boundary", () => {
    const home = sandbox();
    fs.chmodSync(home, 0o700);
    const { shared, directory } = groupChain(home);
    withoutOuterBoundary(home);
    fs.chmodSync(shared, 0o777);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*local is writable by other users/);
    fs.chmodSync(shared, 0o775);
    fs.mkdirSync(directory, { recursive: true });
    fs.chmodSync(directory, 0o770);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR.*runs is writable by other users/);
    expect(fs.statSync(directory).mode & 0o777).toBe(0o770);
  });

  it.skipIf(process.platform === "win32")("accepts private recursive roots below an owned sticky ancestor", () => {
    const ancestor = path.join(sandbox(), "sticky");
    fs.mkdirSync(ancestor);
    fs.chmodSync(ancestor, 0o1777);
    const directory = path.join(ancestor, "private", "data");
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(fabricDataRoot()).toBe(directory);
    expect(fs.statSync(path.dirname(directory)).mode & 0o777).toBe(0o700);
    expect(fs.statSync(directory).mode & 0o777).toBe(0o700);
  });
});
