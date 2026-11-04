import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshLock } from "../src/mesh/mesh-lock.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const root = (): string => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-torn-owner-"));
  roots.push(value);
  return value;
};
const age = (file: string, ms: number): void => {
  const old = new Date(Date.now() - ms);
  fs.utimesSync(file, old, old);
};
const acquire = <T = string>(directory: string, options: { lockTimeoutMs?: number; staleLockMs?: number } = {},
  body: () => T = (() => "entered" as T)) =>
  new MeshLock(directory, { lockProtocol: 1, lockTimeoutMs: 250, staleLockMs: 100, ...options }, () => undefined)
    .withLock(body);

it.each(["", "partial\n123"])("recovers a stale torn owner (%j) through contention", async bytes => {
  const directory = root(), lock = path.join(directory, ".lock"), owner = path.join(lock, "owner");
  fs.mkdirSync(lock);
  fs.writeFileSync(owner, bytes);
  age(owner, 500);
  const record = await acquire(directory, {}, () => fs.readFileSync(owner, "utf8"));
  expect(record).toMatch(/^.+\n\d+\n\d+\n$/);
  expect(fs.readdirSync(directory).some(name => name.startsWith(".lock.dead."))).toBe(true);
});

it("recovers a stale directory containing only a crashed publisher's private owner receipt", async () => {
  const directory = root(), lock = path.join(directory, ".lock");
  const temporary = path.join(lock, ".owner.crashed-writer.tmp");
  fs.mkdirSync(lock);
  fs.writeFileSync(temporary, `crashed-token\n2147483647\n${Date.now()}\n`);
  age(temporary, 500);
  age(lock, 500);

  await expect(acquire(directory, {}, () => fs.readFileSync(path.join(lock, "owner"), "utf8")))
    .resolves.toMatch(/^.+\n\d+\n\d+\n$/);
  expect(fs.existsSync(temporary)).toBe(false);
  expect(fs.readdirSync(directory).some(name => name.startsWith(".lock.dead."))).toBe(true);
});

it("a publisher entering at the stale-receipt removal fence is never detached", async () => {
  const directory = root(), lock = path.join(directory, ".lock"), owner = path.join(lock, "owner");
  const stale = path.join(lock, ".owner.crashed-writer.tmp");
  fs.mkdirSync(lock);
  fs.writeFileSync(stale, "crashed-token\n2147483647\n1\n");
  age(stale, 500);
  age(lock, 500);

  const realRmdir = fs.rmdirSync;
  let injected = false, entered = false;
  const rmdir = vi.spyOn(fs, "rmdirSync").mockImplementation((target: fs.PathLike) => {
    if (!injected && String(target) === lock) {
      injected = true;
      const temporary = path.join(lock, ".owner.live-writer.tmp");
      const record = `live-token\n${process.pid}\n${Date.now()}\n`;
      fs.writeFileSync(temporary, record, { flag: "wx" });
      fs.linkSync(temporary, owner);
      fs.unlinkSync(temporary);
    }
    return realRmdir.call(fs, target);
  });
  try {
    let failure: unknown;
    try { await acquire(directory, { lockTimeoutMs: 250 }, () => { entered = true; return "entered"; }); }
    catch (error) { failure = error; }
    expect(injected).toBe(true);
    expect(failure).toBeDefined();
    expect(entered).toBe(false);
    expect(fs.readFileSync(owner, "utf8")).toMatch(/^live-token\n\d+\n\d+\n$/);
    expect(fs.lstatSync(lock).isDirectory()).toBe(true);
    expect(fs.readdirSync(directory).filter(name => name.startsWith(".lock.dead."))).toHaveLength(0);
  } finally { rmdir.mockRestore(); }
});

it.each([false, true])("does not recover a private owner receipt from a live writer (aged=%s)", async aged => {
  const directory = root(), lock = path.join(directory, ".lock");
  const temporary = path.join(lock, ".owner.live-writer.tmp");
  const bytes = `live-token\n${process.pid}\n${Date.now()}\n`;
  fs.mkdirSync(lock);
  fs.writeFileSync(temporary, bytes);
  if (aged) { age(temporary, 500); age(lock, 500); }
  await expect(acquire(directory, { lockTimeoutMs: 180 })).rejects.toThrow();
  expect(fs.readFileSync(temporary, "utf8")).toBe(bytes);
  expect(fs.existsSync(lock)).toBe(true);
});

it("leaves a slow writer's young empty owner untouched", async () => {
  const directory = root(), lock = path.join(directory, ".lock"), owner = path.join(lock, "owner");
  fs.mkdirSync(lock);
  fs.writeFileSync(owner, "");
  await expect(acquire(directory, { lockTimeoutMs: 180, staleLockMs: 5_000 })).rejects.toThrow();
  expect(fs.existsSync(lock)).toBe(true);
  expect(fs.readFileSync(owner, "utf8")).toBe("");
  expect(fs.readdirSync(directory).filter(name => name.startsWith(".lock.dead."))).toHaveLength(0);
});

it("a resumed protocol-1 initializer fenced while its owner write is torn cannot enter", async () => {
  const directory = root(), lock = path.join(directory, ".lock"), owner = path.join(lock, "owner");
  fs.mkdirSync(lock);
  const oldWriterRecord = `old-token\n${process.pid}\n${Date.now()}\n`;
  const oldWriter = fs.openSync(owner, "wx"); // old process has created the canonical 0-byte owner and is paused.
  const oldDirectory = fs.lstatSync(lock);
  age(owner, 500);
  const successorRecord = await acquire(directory, {}, () => {
    const record = fs.readFileSync(owner, "utf8");
    const fence = path.join(directory, fs.readdirSync(directory).find(name => name.startsWith(".lock.dead."))!);
    fs.writeSync(oldWriter, oldWriterRecord); // resumed descriptor writes only into the fenced old inode.
    fs.closeSync(oldWriter);
    // The old initializer's mandatory canonical identity/readback rejects entry and cannot alter successor bytes.
    expect(fs.lstatSync(lock).ino).not.toBe(oldDirectory.ino);
    expect(fs.lstatSync(fence).ino).toBe(oldDirectory.ino);
    expect(fs.readFileSync(path.join(fence, "owner"), "utf8")).toBe(oldWriterRecord);
    expect(fs.readFileSync(owner, "utf8")).toBe(record);
    expect(record).not.toBe(oldWriterRecord);
    return record;
  });
  expect(successorRecord).not.toBe(oldWriterRecord);
});

it("releases a published lock when temporary-owner cleanup fails", async () => {
  const directory = root(), lock = path.join(directory, ".lock");
  const realUnlink = fs.unlinkSync;
  let injected = false;
  const unlink = vi.spyOn(fs, "unlinkSync").mockImplementation((file: fs.PathLike) => {
    if (!injected && String(file).startsWith(`${lock}${path.sep}.owner.`)) {
      injected = true;
      throw Object.assign(new Error("injected cleanup failure"), { code: "EIO" });
    }
    return realUnlink.call(fs, file);
  });
  await expect(acquire(directory)).rejects.toThrow("injected cleanup failure");
  expect(injected).toBe(true);
  expect(fs.existsSync(lock)).toBe(false);
});

it("legacy protocol-1 and modern protocol-2 writers exchange complete compatible records", async () => {
  const directory = root();
  const legacy = new MeshLock(directory, { lockProtocol: 1, lockTimeoutMs: 500 }, () => undefined);
  await expect(legacy.withLock(() => {
    const record = fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8");
    expect(record.split("\n")).toHaveLength(4);
    expect(record.endsWith("\n")).toBe(true);
    return record;
  })).resolves.toMatch(/^.+\n\d+\n\d+\n$/);
  const modern = new MeshLock(directory, { lockProtocol: 2, lockTimeoutMs: 500 }, () => undefined);
  await expect(modern.withLock(() => {
    const record = fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8");
    expect(record.split("\n")).toHaveLength(5);
    expect(record.endsWith("\n")).toBe(true);
    return record;
  })).resolves.toMatch(/^.+\n\d+\n\d+\n.+\n$/);
  // The legacy version can acquire again after modern release; neither version changes the other's wire format.
  await expect(acquire(directory)).resolves.toBe("entered");
});
