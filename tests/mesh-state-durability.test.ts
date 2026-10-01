import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DurableDirectory, writeFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-durable-")); roots.push(directory); return directory; };
const identity = { id: "session:durability", name: "main", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });

const observe = (directory: string) => {
  const events: Array<{ kind: string; file: string; locked: boolean }> = [];
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
  const record = (kind: string, file: string) => events.push({ kind, file, locked: fs.existsSync(path.join(directory, ".lock", "owner")) });
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { record("sync", descriptors.get(fd)!); sync(fd); });
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { record("rename", String(to)); rename(from, to); });
  return events;
};
const expectCommit = (events: ReturnType<typeof observe>, directory: string) => {
  const syncs = events.filter(event => event.kind === "sync");
  expect(syncs.length).toBeLessThanOrEqual(2);
  expect(syncs.length).toBe(process.platform === "win32" ? 1 : 2);
  expect(syncs[0]!.file).toMatch(/state\.json\..*\.tmp$/);
  expect(syncs.every(event => event.locked)).toBe(true);
  const stateRename = events.findIndex(event => event.kind === "rename" && event.file === path.join(directory, "state.json"));
  expect(stateRename).toBeGreaterThan(events.indexOf(syncs[0]!));
  const signalRename = events.findIndex(event => event.kind === "rename" && event.file === path.join(directory, "state.read-signal.json"));
  if (process.platform !== "win32") {
    expect(syncs[1]!.file).toBe(directory);
    expect(events.indexOf(syncs[1]!)).toBeGreaterThan(stateRename);
    expect(signalRename).toBeGreaterThan(events.indexOf(syncs[1]!));
  } else expect(signalRename).toBeGreaterThan(stateRename);
};

describe("#2479 two-barrier mesh state commits", () => {
  it.each(["put", "delete", "batch"])("uses at most two fsyncs per steady-state %s commit", async (kind) => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    await mesh.put({ key: "test/key", value: 1, identity });
    const events = observe(directory);
    if (kind === "put") await mesh.put({ key: "test/key", value: 2, identity });
    else if (kind === "delete") await mesh.delete({ key: "test/key" });
    else await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "test/key", value: 2 }, { kind: "put", key: "test/other", value: 3 }] });
    expectCommit(events, directory);
    events.length = 0;
    await mesh.publish({ topic: "test", from: identity, text: "volatile hint" });
    expect(events.filter(event => event.kind === "sync")).toEqual([]);
  });

  it("does not prepare durability receipts on construction, reads, or volatile publication", async () => {
    const directory = root(), events = observe(directory), mesh = new MeshStore(directory, 65536, 100);
    mesh.listAll("", { fresh: true });
    mesh.get("test/absent", { fresh: true });
    await mesh.publish({ topic: "test", from: identity, text: "volatile" });
    expect(events.filter(event => event.kind === "sync")).toEqual([]);
  });

  it("establishes the full namespace before the first lock, not in the commit", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100), events = observe(directory);
    await mesh.put({ key: "test/key", value: 1, identity });
    const setup = events.filter(event => event.kind === "sync" && !event.locked);
    if (process.platform !== "win32") {
      expect(setup[0]!.file).toBe(directory);
      expect(setup.at(-1)!.file).toBe(path.parse(directory).root);
      expect(events.indexOf(setup.at(-1)!)).toBeLessThan(events.findIndex(event => event.locked));
    }
    expectCommit(events.filter(event => event.locked), directory);
  });

  it.skipIf(process.platform === "win32")("retries failed ancestor setup without entering the lock or acknowledging a token", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const failing = path.dirname(directory), descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    let fail = true, failed = 0;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      if (fail && descriptors.get(fd) === failing) { failed++; expect(fs.existsSync(path.join(directory, ".lock"))).toBe(false); throw new Error("ancestor barrier unavailable"); }
      sync(fd);
    });
    for (let n = 0; n < 2; n++) await expect(mesh.put({ key: "test/key", value: n, identity })).rejects.toThrow("ancestor barrier unavailable");
    const fresh = new MeshStore(directory, 65536, 100);
    await expect(fresh.put({ key: "test/key", value: 2, identity })).rejects.toThrow("ancestor barrier unavailable");
    expect(failed).toBe(3);
    expect(fs.existsSync(path.join(directory, "state.json"))).toBe(false);
    fail = false;
    expect((await mesh.put({ key: "test/key", value: 3, identity })).version).toBeGreaterThan(0);
  });

  it.each(process.platform === "win32" ? [1] : [1, 2])("does not acknowledge or publish a signal when commit barrier %i fails", async (barrier) => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const old = await mesh.put({ key: "test/key", value: 1, identity });
    const signal = fs.readFileSync(path.join(directory, "state.read-signal.json"), "utf8");
    const sync = fs.fsyncSync.bind(fs);
    let calls = 0;
    const spy = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { if (++calls === barrier) throw new Error("commit barrier unavailable"); sync(fd); });
    await expect(mesh.put({ key: "test/key", value: 2, identity })).rejects.toThrow("commit barrier unavailable");
    expect(fs.readFileSync(path.join(directory, "state.read-signal.json"), "utf8")).toBe(signal);
    expect(fs.existsSync(path.join(directory, ".lock"))).toBe(false);
    expect(fs.readdirSync(directory).filter(file => file.endsWith(".tmp"))).toEqual([]);
    expect(mesh.get("test/key", { fresh: true })!.version).toBe(barrier === 1 ? old.version : old.version + 1);
    spy.mockRestore();
    expect((await mesh.put({ key: "test/key", value: 3, identity })).version).toBeGreaterThan(old.version);
  });

  it.skipIf(process.platform === "win32")("re-establishes replaced root ancestry outside the lock", async () => {
    const parent = root(), directory = path.join(parent, "mesh"), mesh = new MeshStore(directory, 65536, 100);
    await mesh.put({ key: "test/key", value: 1, identity });
    fs.renameSync(directory, path.join(parent, "old"));
    const events = observe(directory);
    await mesh.put({ key: "test/key", value: 2, identity });
    expect(events.some(event => event.kind === "sync" && event.file === parent && !event.locked)).toBe(true);
    expectCommit(events.filter(event => event.locked), directory);
    events.length = 0;
    await mesh.put({ key: "test/key", value: 3, identity });
    expectCommit(events, directory);
  });

  it.skipIf(process.platform === "win32")("re-establishes a same-inode root detach/reattach rather than treating it as unchanged", async () => {
    const parent = root(), directory = path.join(parent, "mesh"), mesh = new MeshStore(directory, 65536, 100);
    await mesh.put({ key: "test/key", value: 1, identity });
    fs.renameSync(directory, path.join(parent, "away"));
    fs.renameSync(path.join(parent, "away"), directory);
    const events = observe(directory);
    await mesh.put({ key: "test/key", value: 2, identity });
    expect(events.some(event => event.kind === "sync" && event.file === parent && !event.locked)).toBe(true);
    expectCommit(events.filter(event => event.locked), directory);
  });

  it.skipIf(process.platform === "win32")("re-establishes a replaced symlink even when it targets the same directory inode", async () => {
    const parent = root(), directory = path.join(parent, "physical"), alias = path.join(parent, "alias");
    fs.mkdirSync(directory); fs.symlinkSync("physical", alias, "dir");
    const mesh = new MeshStore(alias, 65536, 100);
    await mesh.put({ key: "test/key", value: 1, identity });
    const events = observe(alias);
    await mesh.put({ key: "test/key", value: 2, identity });
    expect(events.filter(event => event.kind === "sync").map(event => event.file)).toEqual([expect.stringMatching(/state\.json\..*\.tmp$/), directory]);
    fs.unlinkSync(alias); fs.symlinkSync("./physical", alias, "dir");
    events.length = 0;
    await mesh.put({ key: "test/key", value: 3, identity });
    expect(events.some(event => event.kind === "sync" && event.file === parent && !event.locked)).toBe(true);
    expect(events.filter(event => event.kind === "sync" && event.locked)).toHaveLength(2);
  });

  it.skipIf(process.platform === "win32")("fails closed if the namespace changes after preparation and before publication", () => {
    const parent = root(), directory = path.join(parent, "mesh"); fs.mkdirSync(directory);
    const receipt = new DurableDirectory(directory); receipt.prepare();
    fs.renameSync(directory, path.join(parent, "old")); fs.mkdirSync(directory);
    const target = path.join(directory, "state.json");
    expect(() => writeFileAtomic(target, "unacknowledged", { durable: true, directoryReceipt: receipt })).toThrow("Durability receipt namespace changed");
    receipt.prepare();
    expect(() => writeFileAtomic(target, "acknowledged", { durable: true, directoryReceipt: receipt })).not.toThrow();
  });
});
