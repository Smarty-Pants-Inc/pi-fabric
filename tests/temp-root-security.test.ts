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
