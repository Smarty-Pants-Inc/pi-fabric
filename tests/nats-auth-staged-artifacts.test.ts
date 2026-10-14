import * as constants from "node:constants";
import { spawnSync } from "node:child_process";
import os from "node:os";
import { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const harness = vi.hoisted(() => ({
  fs: undefined as any,
  mint: undefined as any,
  failure: "" as string,
  failRenameAt: 0,
  replaceAfterFirstRename: false,
  renameCount: 0,
  writes: [] as Array<{ file: string; flags: number; mode: number }>,
  events: [] as string[],
  entries: new Map<string, { kind: "dir" | "file"; dev: number; ino: number; mode: number; uid: number; bytes?: number }>(),
  descriptors: new Map<number, string>(),
  nextFd: 10,
  nextIno: 100,
  currentUid: 1234,
}));

vi.mock("node:fs", () => ({
  default: new Proxy({}, { get: (_target, key) => harness.fs?.[key as any] }),
}));
vi.mock("../src/mesh/nats-auth/mint.js", () => ({
  NatsMintError: class extends Error {},
  createNatsOperator: (...args: any[]) => harness.mint.createNatsOperator(...args),
  createNatsAccount: (...args: any[]) => harness.mint.createNatsAccount(...args),
  mintNatsUser: (...args: any[]) => harness.mint.mintNatsUser(...args),
  mintNatsBreakGlassUser: (...args: any[]) => harness.mint.mintNatsBreakGlassUser(...args),
  mintNatsSystemUser: (...args: any[]) => harness.mint.mintNatsSystemUser(...args),
}));
vi.mock("../src/mesh/nats-auth/server-config.js", () => ({
  NatsServerConfigError: class extends Error { field = "config"; },
  renderNatsServerConfig: () => "inert config",
  validateNatsServerConfigInput: (value: unknown) => value,
}));

import { runNatsCli } from "../src/mesh/nats-auth/cli.js";

const TARGET = "/virtual/output";
const FILES = ["one.jwt", "two.key"];
const DATA = [new Uint8Array([11, 22]), new Uint8Array([33, 44])];
const OWN_UID = process.getuid?.() ?? 1234;

function stat(entry: { kind: "dir" | "file"; dev: number; ino: number; mode: number; uid: number; bytes?: number }) {
  return {
    ...entry,
    size: entry.bytes ?? 0,
    isDirectory: () => entry.kind === "dir",
    isFile: () => entry.kind === "file",
    isSymbolicLink: () => false,
  };
}

function resetFs() {
  const entries = harness.entries;
  entries.clear();
  entries.set("/", { kind: "dir", dev: 1, ino: 1, mode: 0o755, uid: OWN_UID });
  entries.set("/virtual", { kind: "dir", dev: 1, ino: 2, mode: 0o755, uid: OWN_UID });
  harness.failure = "";
  harness.failRenameAt = 0;
  harness.replaceAfterFirstRename = false;
  harness.renameCount = 0;
  harness.writes = [];
  harness.events = [];
  harness.descriptors.clear();
  harness.nextFd = 10;
  harness.nextIno = 100;

  const ensure = (file: string, kind: "dir" | "file", mode: number) => {
    if (entries.has(file)) throw Object.assign(new Error("exists"), { code: "EEXIST" });
    const entry = { kind, dev: 1, ino: harness.nextIno++, mode, uid: OWN_UID, ...(kind === "file" ? { bytes: 0 } : {}) };
    entries.set(file, entry);
    return entry;
  };

  harness.fs = {
    constants: { O_WRONLY: constants.O_WRONLY, O_CREAT: constants.O_CREAT, O_EXCL: constants.O_EXCL, O_NOFOLLOW: constants.O_NOFOLLOW },
    existsSync: (file: string) => entries.has(file),
    lstatSync: (file: string) => {
      const entry = entries.get(file);
      if (!entry) throw Object.assign(new Error("missing"), { code: "ENOENT" });
      return stat(entry);
    },
    mkdirSync: (file: string, options?: { mode?: number }) => {
      harness.events.push(`mkdir:${file}`);
      ensure(file, "dir", options?.mode ?? 0o777);
    },
    mkdtempSync: (prefix: string) => {
      const file = `${prefix}virtual`;
      harness.events.push(`mkdtemp:${file}`);
      ensure(file, "dir", 0o700);
      return file;
    },
    chmodSync: (file: string, mode: number) => { const entry = entries.get(file); if (!entry) throw new Error("missing"); entry.mode = mode; },
    openSync: (file: string, flags: number, mode?: number) => {
      harness.events.push(`open:${file}`);
      if (harness.failure === "open-second" && harness.writes.length === 1) throw new Error("injected open failure");
      const entry = entries.get(file);
      if (!entry && (flags & constants.O_CREAT) === 0) throw Object.assign(new Error("missing"), { code: "ENOENT" });
      if (entry?.kind === "dir" || (flags & constants.O_CREAT) === 0) {
        const fd = harness.nextFd++;
        harness.descriptors.set(fd, file);
        return fd;
      }
      ensure(file, "file", mode ?? 0o600);
      const fd = harness.nextFd++;
      harness.descriptors.set(fd, file);
      harness.writes.push({ file, flags, mode: mode ?? 0 });
      return fd;
    },
    fchmodSync: (fd: number, mode: number) => { const entry = entries.get(harness.descriptors.get(fd)!); if (!entry) throw new Error("bad fd"); entry.mode = mode; },
    fstatSync: (fd: number) => {
      const entry = entries.get(harness.descriptors.get(fd)!);
      if (!entry) throw new Error("bad fd");
      return stat(entry);
    },
    writeSync: (fd: number, bytes: Uint8Array, offset: number) => {
      const file = harness.descriptors.get(fd)!;
      if (harness.failure === "write-second" && harness.writes.findIndex((write) => write.file === file) === 1) throw new Error("injected write failure");
      const entry = entries.get(file)!;
      const remaining = bytes.length - offset;
      entry.bytes = (entry.bytes ?? 0) + remaining;
      return remaining;
    },
    fsyncSync: (fd: number) => {
      const file = harness.descriptors.get(fd);
      harness.events.push(`fsync:${file ?? `fd-${fd}`}`);
      if (file && harness.failure === "fsync-second" && harness.writes.findIndex((write) => write.file === file) === 1) throw new Error("injected fsync failure");
      if (file === TARGET && harness.failure === "target-dir-fsync") throw new Error("injected directory fsync failure");
    },
    closeSync: (fd: number) => { harness.descriptors.delete(fd); },
    renameSync: (from: string, to: string) => {
      harness.renameCount++;
      harness.events.push(`rename:${from}->${to}`);
      if (harness.failRenameAt === harness.renameCount) throw new Error("injected rename failure");
      const entry = entries.get(from);
      if (!entry) throw new Error("missing rename source");
      entries.delete(from);
      entries.set(to, entry);
      if (harness.replaceAfterFirstRename && harness.renameCount === 1) {
        entries.set(to, { kind: "file", dev: 9, ino: 900, mode: 0o600, uid: OWN_UID, bytes: 1 });
      }
    },
    unlinkSync: (file: string) => { harness.events.push(`unlink:${file}`); if (!entries.delete(file)) throw Object.assign(new Error("missing"), { code: "ENOENT" }); },
    rmSync: (file: string) => {
      harness.events.push(`rm:${file}`);
      for (const name of [...entries.keys()]) if (name === file || name.startsWith(`${file}/`)) entries.delete(name);
    },
  };
}

harness.mint = {
  NatsMintError: class extends Error {},
  createNatsOperator: () => ({ metadata: { kind: "operator" }, artifacts: FILES.map((name, i) => ({ name, bytes: DATA[i]!.slice() })) }),
  createNatsAccount: () => ({ metadata: { kind: "account" }, artifacts: FILES.map((name, i) => ({ name, bytes: DATA[i]!.slice() })) }),
  mintNatsUser: () => ({ metadata: { kind: "user" }, artifacts: FILES.map((name, i) => ({ name, bytes: DATA[i]!.slice() })) }),
  mintNatsBreakGlassUser: () => ({ metadata: { kind: "user" }, artifacts: FILES.map((name, i) => ({ name, bytes: DATA[i]!.slice() })) }),
  mintNatsSystemUser: () => ({ metadata: { kind: "user" }, artifacts: FILES.map((name, i) => ({ name, bytes: DATA[i]!.slice() })) }),
};

let stdout: string[];
let stderr: string[];
let originalPlatform: PropertyDescriptor | undefined;

beforeEach(() => {
  resetFs();
  stdout = [];
  stderr = [];
  vi.spyOn(process.stdout, "write").mockImplementation(((chunk: string | Uint8Array) => { stdout.push(String(chunk)); return true; }) as any);
  vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array) => { stderr.push(String(chunk)); return true; }) as any);
});

afterEach(() => {
  vi.restoreAllMocks();
  if (originalPlatform) Object.defineProperty(process, "platform", originalPlatform);
  originalPlatform = undefined;
});

async function invoke(args = ["mint", "operator", "--out-dir", TARGET]) {
  vi.spyOn(process, "stdin", "get").mockReturnValue(Readable.from([Buffer.from("{}")]) as any);
  return runNatsCli(args);
}

const pathsAt = (root: string) => [...harness.entries.keys()].filter((name) => name === root || name.startsWith(`${root}/`)).sort();
const stagingPaths = () => [...harness.entries.keys()].filter((name) => name.startsWith("/virtual/") && name !== TARGET && !name.startsWith(`${TARGET}/`));
const targetFiles = () => pathsAt(TARGET).filter((name) => name !== TARGET);

function hasFlags(write: { flags: number; mode: number }) {
  const required = constants.O_EXCL | (constants.O_NOFOLLOW ?? 0);
  return (write.flags & required) === required && write.mode === 0o600;
}

describe.skipIf(process.platform === "win32")("NATS staged artifact publication", () => {
  it.each(["write-second", "fsync-second", "open-second"] as const)("cleans target and staging entries after second artifact %s failure", async (failure) => {
    harness.failure = failure;
    expect(await invoke()).toBe(1);
    expect(pathsAt(TARGET)).toEqual([]);
    expect(stagingPaths()).toEqual([]);
    expect(stderr.join("").startsWith("pi-fabric nats: refused:")).toBe(true);
    expect(stdout).toEqual([]);
  });

  it("publishes every artifact only after all staged-file fsyncs; uses private exclusive no-follow files owned by this UID", async () => {
    expect(await invoke()).toBe(0);
    expect(targetFiles()).toEqual(FILES.map((name) => path.join(TARGET, name)).sort());
    expect(stagingPaths()).toEqual([]);
    const target = harness.entries.get(TARGET)!;
    expect([target.kind, target.mode, target.uid]).toEqual(["dir", 0o700, OWN_UID]);
    for (const file of targetFiles()) {
      const entry = harness.entries.get(file)!;
      expect([entry.kind, entry.mode, entry.uid]).toEqual(["file", 0o600, OWN_UID]);
    }
    expect(harness.writes).toHaveLength(FILES.length);
    expect(harness.writes.every(hasFlags)).toBe(true);
    const fileSyncs = harness.events.map((event, index) => event.startsWith("fsync:") && harness.writes.some((write) => event === `fsync:${write.file}`) ? index : -1).filter((index) => index >= 0);
    const renames = harness.events.map((event, index) => event.startsWith("rename:") ? index : -1).filter((index) => index >= 0);
    expect(fileSyncs.length).toBe(FILES.length);
    expect(renames.length).toBe(FILES.length);
    expect(Math.max(...fileSyncs)).toBeLessThan(Math.min(...renames));
    expect(stdout.join("").includes("one.jwt")).toBe(true);
  });

  it("rolls back only successfully renamed artifact paths when a later rename fails", async () => {
    harness.failRenameAt = 2;
    expect(await invoke()).toBe(1);
    expect(targetFiles()).toEqual([]);
    expect([[], [TARGET]]).toContainEqual(pathsAt(TARGET));
    expect(stagingPaths()).toEqual([]);
  });

  it("does not unlink a target path whose inode changed after its staged inode was renamed", async () => {
    harness.failRenameAt = 2;
    harness.replaceAfterFirstRename = true;
    expect(await invoke()).toBe(1);
    expect(harness.entries.get(path.join(TARGET, FILES[0]))).toMatchObject({ dev: 9, ino: 900 });
    expect(harness.entries.has(path.join(TARGET, FILES[1]))).toBe(false);
    expect(stagingPaths()).toEqual([]);
  });

  it("retains a pre-existing target without touching it", async () => {
    harness.entries.set(TARGET, { kind: "dir", dev: 7, ino: 77, mode: 0o755, uid: OWN_UID });
    expect(await invoke()).toBe(1);
    expect(pathsAt(TARGET)).toEqual([TARGET]);
    expect(harness.writes).toEqual([]);
  });

  it("rolls back published targets if syncing the final target directory fails", async () => {
    harness.failure = "target-dir-fsync";
    expect(await invoke()).toBe(1);
    expect(targetFiles()).toEqual([]);
    expect([[], [TARGET]]).toContainEqual(pathsAt(TARGET));
    expect(stagingPaths()).toEqual([]);
  });
});

it("refuses Windows before any writes", async () => {
  originalPlatform = Object.getOwnPropertyDescriptor(process, "platform");
  Object.defineProperty(process, "platform", { configurable: true, value: "win32" });
  expect(await invoke()).toBe(1);
  expect(harness.writes).toEqual([]);
  expect(pathsAt(TARGET)).toEqual([]);
  expect(stderr.join("").includes("smarty-dev#11482")).toBe(true);
});

describe.skipIf(process.env.CI !== "true" || process.platform === "win32")("NATS hosted actual-CLI cleanup (E2E; owner fabric-store)", () => {
  const roots: string[] = [];
  const bun = process.versions.bun ? process.execPath : "bun";

  afterEach(async () => {
    const realFs = await vi.importActual<typeof import("node:fs")>("node:fs");
    for (const root of roots.splice(0)) realFs.rmSync(root, { recursive: true, force: true });
  });

  async function runFault(fault: "write-second" | "fsync-second" | "rename-second") {
    const realFs = await vi.importActual<typeof import("node:fs")>("node:fs");
    const root = realFs.mkdtempSync(path.join(os.tmpdir(), "nats-staged-cli-"));
    roots.push(root);
    const target = path.join(root, "output");
    const script = `
import fs from "node:fs";
const fault = ${JSON.stringify(fault)};
let writes = 0;
let fileSyncs = 0;
let renames = 0;
if (fault === "write-second") {
  const write = fs.writeSync.bind(fs);
  fs.writeSync = (fd, bytes, ...rest) => {
    if (++writes === 2) throw Object.assign(new Error("injected"), { code: "EIO" });
    return write(fd, bytes, ...rest);
  };
}
if (fault === "fsync-second") {
  const sync = fs.fsyncSync.bind(fs);
  fs.fsyncSync = (fd) => {
    if (fs.fstatSync(fd).isFile() && ++fileSyncs === 2) throw Object.assign(new Error("injected"), { code: "EIO" });
    return sync(fd);
  };
}
if (fault === "rename-second") {
  const rename = fs.renameSync.bind(fs);
  fs.renameSync = (from, to) => {
    if (++renames === 2) throw Object.assign(new Error("injected"), { code: "EIO" });
    return rename(from, to);
  };
}
const { runNatsCli } = await import(${JSON.stringify(fileURLToPath(new URL("../src/mesh/nats-auth/cli.ts", import.meta.url)))});
process.exitCode = await runNatsCli(["mint", "operator", "--out-dir", ${JSON.stringify(target)}]);
`;
    const result = spawnSync(bun, ["-e", script], { encoding: "utf8", input: JSON.stringify({ operatorName: "fixture-staged" }), timeout: 30_000 });
    return { root, target, status: result.status, stdoutNonempty: Boolean(result.stdout), stderrNonempty: Boolean(result.stderr) };
  }

  it.each(["write-second", "fsync-second"] as const)("actual CLI leaves no target or sibling staging entries after %s fault", async (fault) => {
    const result = await runFault(fault);
    expect(result.status).toBe(1);
    const realFs = await vi.importActual<typeof import("node:fs")>("node:fs");
    expect(realFs.readdirSync(result.root)).toEqual([]);
    expect(() => realFs.lstatSync(result.target)).toThrow();
    expect(result.stdoutNonempty).toBe(false);
    expect(result.stderrNonempty).toBe(true);
  });

  it("actual CLI removes only its partial publication after a later artifact rename fails", async () => {
    const result = await runFault("rename-second");
    expect(result.status).toBe(1);
    const realFs = await vi.importActual<typeof import("node:fs")>("node:fs");
    const remaining = realFs.readdirSync(result.root);
    expect(remaining.every((name) => name === "output")).toBe(true);
    if (remaining.includes("output")) expect(realFs.readdirSync(result.target)).toEqual([]);
    expect(result.stdoutNonempty).toBe(false);
    expect(result.stderrNonempty).toBe(true);
  });
});
