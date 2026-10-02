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
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 5));
const until = async (condition: () => boolean) => { for (let n = 0; !condition(); n++) { if (n > 1000) throw new Error("probe timed out"); await tick(); } };

const observe = (directory: string) => {
  const events: Array<{ kind: string; file: string; locked: boolean; at: number }> = [];
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), asyncSync = fs.fsync.bind(fs), rename = fs.renameSync.bind(fs);
  const record = (kind: string, file: string) => events.push({ kind, file, locked: fs.existsSync(path.join(directory, ".lock", "owner")), at: Date.now() });
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { record("sync", descriptors.get(fd)!); sync(fd); });
  vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => { record("barrier-file-sync", descriptors.get(fd)!); asyncSync(fd, callback); });
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { record("rename", String(to)); rename(from, to); });
  return events;
};
const expectCommit = (events: ReturnType<typeof observe>, directory: string) => {
  const barrierStart = events.findIndex(event => event.kind === "barrier-file-sync");
  const syncs = events.slice(barrierStart).filter(event => event.kind === "sync" || event.kind === "barrier-file-sync");
  expect(events.filter(event => event.kind === "sync" && event.locked)).toEqual([]);
  expect(syncs).toHaveLength(process.platform === "win32" ? 1 : 2);
  expect(syncs[0]!.file).toMatch(/state\.durable\.json\..*\.tmp$/);
  expect(syncs.every(event => !event.locked)).toBe(true);
  const stateRename = events.findIndex(event => event.kind === "rename" && event.file === path.join(directory, "state.json"));
  expect(events.indexOf(syncs[0]!)).toBeGreaterThan(stateRename);
  const checkpointRename = events.findIndex(event => event.kind === "rename" && event.file === path.join(directory, "state.durable.json"));
  expect(checkpointRename).toBeGreaterThan(events.indexOf(syncs[0]!));
  if (process.platform !== "win32") {
    expect(syncs[1]!.file).toBe(directory);
    expect(events.indexOf(syncs[1]!)).toBeGreaterThan(checkpointRename);
  }
};

const pauseBarriers = () => {
  const original = fs.fsync.bind(fs);
  const pending: Array<(error?: Error) => void> = [];
  vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => { pending.push((error) => error ? callback(error) : original(fd, callback)); });
  return pending;
};

describe("#2479 mesh group durability barrier", () => {
  it.skipIf(process.platform === "win32").each([false, true])("#2479 R3 F3 reconfirms ancestor changes during the leaf directory barrier (reconfirmation fails: %s)", (fail) => {
    const parent = root(), directory = path.join(parent, "mesh"); fs.mkdirSync(directory);
    const receipt = new DurableDirectory(directory); receipt.prepare();
    const descriptors = new Map<number, string>(), barriers: string[] = [];
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    let changed = false;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = descriptors.get(fd)!; barriers.push(file);
      if (changed && fail && file === parent) throw new Error("ABA reconfirmation unavailable");
      sync(fd);
      if (!changed && file === directory) {
        changed = true;
        fs.renameSync(directory, path.join(parent, "away")); fs.renameSync(path.join(parent, "away"), directory);
      }
    });
    if (fail) expect(() => receipt.sync(directory)).toThrow("ABA reconfirmation unavailable");
    else expect(() => receipt.sync(directory)).not.toThrow();
    expect(barriers).toContain(parent);
  });
  it.skipIf(process.platform === "win32")("#2479 R3 F3 reconfirms same-inode namespace ABA during the asynchronous file barrier before ack", async () => {
    const parent = root(), directory = path.join(parent, "mesh");
    const mesh = new MeshStore(directory, 65536, 100);
    await mesh.put({ key: "test/key", value: 0, identity });
    const events: Array<{ kind: string; file: string; locked: boolean }> = [], descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { events.push({ kind: "sync", file: descriptors.get(fd)!, locked: fs.existsSync(path.join(directory, ".lock", "owner")) }); sync(fd); });
    const pending = pauseBarriers();
    const receipt = mesh.put({ key: "test/key", value: 1, identity });
    // Attach a rejection handler immediately: a failed reconfirmation must fail closed.
    const settled = receipt.then(value => ({ value }), error => ({ error }));
    await until(() => pending.length === 1);
    const before = fs.statSync(directory), ancestor = fs.statSync(parent);
    await tick(); // make the ctime evidence unambiguous even on low-resolution filesystems
    const away = path.join(parent, "away");
    fs.renameSync(directory, away); fs.renameSync(away, directory);
    expect([fs.statSync(directory).dev, fs.statSync(directory).ino]).toEqual([before.dev, before.ino]);
    expect([fs.statSync(parent).dev, fs.statSync(parent).ino]).toEqual([ancestor.dev, ancestor.ino]);
    expect(fs.statSync(parent).ctimeMs).not.toBe(ancestor.ctimeMs);
    const changedAt = events.length;
    pending.shift()!();
    const result = await settled;
    expect(result).not.toHaveProperty("error");
    expect(events.slice(changedAt).some(event => event.kind === "sync" && event.file === parent && !event.locked)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8"))).not.toHaveProperty("error");
  });
  it.each(["put", "delete", "batch"])("uses <=2 fsyncs outside the lock per steady-state %s barrier", async (kind) => {
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

  it("re-resolves a state inode lost to concurrent replacement while pinning", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const link = fs.linkSync.bind(fs); let races = 2;
    vi.spyOn(fs, "linkSync").mockImplementation((source, target) => {
      if (races-- > 0) throw Object.assign(new Error("replaced source inode"), { code: "ENOENT" });
      link(source, target);
    });
    const result = await mesh.put({ key: "test/key", value: 1, identity });
    expect(result.version).toBe(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8")).generation).toBe(1);
  });

  it("uses a writable noncreating/nontruncating file-barrier handle on Windows", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const opened = vi.spyOn(fs, "openSync");
    try {
      Object.defineProperty(process, "platform", { value: "win32" });
      await mesh.put({ key: "test/key", value: 1, identity });
      const barriers = opened.mock.calls.filter(([file, flags]) => String(file).includes("state.durable.json.") && flags === "r+");
      expect(barriers).toHaveLength(1);
      expect(mesh.get("test/key", { fresh: true })!.value).toBe(1);
    } finally { Object.defineProperty(process, "platform", platform); }
  });

  it("does not prepare receipts on construction, reads, or volatile event publication", async () => {
    const directory = root(), events = observe(directory), mesh = new MeshStore(directory, 65536, 100);
    mesh.listAll("", { fresh: true }); mesh.get("test/absent", { fresh: true });
    await mesh.publish({ topic: "test", from: identity, text: "volatile" });
    expect(events.filter(event => event.kind === "sync")).toEqual([]);
  });

  it("establishes full ancestry before the first mutation lock", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100), events = observe(directory);
    await mesh.put({ key: "test/key", value: 1, identity });
    expect(events.filter(event => event.kind === "sync" && event.locked)).toEqual([]);
    if (process.platform !== "win32") {
      const setup = events.filter(event => event.kind === "sync" && !event.file.includes("state.durable") && event.file !== directory);
      expect(setup.at(-1)!.file).toBe(path.parse(directory).root);
      expect(events.indexOf(setup.at(-1)!)).toBeLessThan(events.findIndex(event => event.kind === "rename"));
    }
  });

  it.skipIf(process.platform === "win32")("retries failed ancestor setup before mutation or ack", async () => {
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
    await expect(new MeshStore(directory, 65536, 100).put({ key: "test/key", value: 2, identity })).rejects.toThrow("ancestor barrier unavailable");
    expect(failed).toBe(3); expect(fs.existsSync(path.join(directory, "state.json"))).toBe(false);
    fail = false; await mesh.put({ key: "test/key", value: 3, identity });
  });

  it("coalesces a burst across stores, waits for the covering generation, and throttles to 250ms", async () => {
    const directory = root(), first = new MeshStore(directory, 65536, 100), second = new MeshStore(directory, 65536, 100);
    await first.put({ key: "test/key", value: 0, identity });
    const events = observe(directory);
    const receipts = await Promise.all(Array.from({ length: 12 }, (_, n) => (n % 2 ? first : second).put({ key: `test/k${n}`, value: n, identity })));
    const syncs = events.filter(event => event.kind === "barrier-file-sync");
    expect(syncs).toHaveLength(1);
    const completion = JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8"));
    expect(completion.generation).toBe(Math.max(...receipts.map(receipt => receipt.version)));
    expect(syncs.every(event => !event.locked)).toBe(true);
    const at = syncs[0]!.at;
    await Promise.all([1, 2].map(value => first.put({ key: "test/key", value, identity })));
    const fileSyncs = events.filter(event => event.kind === "barrier-file-sync");
    expect(fileSyncs[1]!.at - at).toBeGreaterThanOrEqual(245);
  });

  it("throttles a peer queued during the preceding barrier even when its local queue has one caller", async () => {
    const directory = root(), owner = new MeshStore(directory, 65536, 100), peer = new MeshStore(directory, 65536, 100);
    const pending = pauseBarriers();
    const first = owner.put({ key: "test/key", value: 1, identity });
    await until(() => pending.length === 1);
    const second = peer.put({ key: "test/key", value: 2, identity });
    await until(() => peer.get("test/key", { fresh: true })?.value === 2);
    await tick(); await tick(); pending.shift()!(); await first;
    const completedAt = JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8")).at;
    await until(() => pending.length === 1);
    expect(Date.now() - completedAt).toBeGreaterThanOrEqual(245);
    pending.shift()!(); await second;
  });

  it("never acks before its barrier and never acks a successor on a predecessor's barrier", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const pending = pauseBarriers(); let ack1 = false, ack2 = false;
    const first = mesh.put({ key: "test/key", value: 1, identity }).then(entry => { ack1 = true; return entry; });
    await until(() => pending.length === 1);
    expect(fs.existsSync(path.join(directory, ".lock"))).toBe(false); expect(ack1).toBe(false);
    const second = mesh.put({ key: "test/key", value: 2, identity }).then(entry => { ack2 = true; return entry; });
    await until(() => mesh.get("test/key", { fresh: true })?.value === 2);
    expect(ack2).toBe(false); pending.shift()!();
    const one = await first;
    expect(ack1).toBe(true); expect(ack2).toBe(false);
    await until(() => pending.length === 1); pending.shift()!();
    const two = await second; expect(two.version).toBeGreaterThan(one.version);
  });

  it.each(process.platform === "win32" ? ["file"] : ["file", "directory"])("propagates a failed %s barrier to every waiter and retries", async (phase) => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const old = await mesh.put({ key: "test/key", value: 0, identity });
    const asyncSync = fs.fsync.bind(fs), sync = fs.fsyncSync.bind(fs);
    const asyncSpy = vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => phase === "file" ? callback(new Error("barrier unavailable")) : asyncSync(fd, callback));
    const syncSpy = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { if (phase === "directory" && fs.fstatSync(fd).isDirectory()) throw new Error("barrier unavailable"); sync(fd); });
    const results = await Promise.allSettled([1, 2, 3].map(value => mesh.put({ key: "test/key", value, identity })));
    expect(results.every(result => result.status === "rejected" && String(result.reason).includes("barrier unavailable"))).toBe(true);
    expect(fs.existsSync(path.join(directory, ".lock"))).toBe(false);
    expect(fs.readdirSync(directory).filter(file => file.endsWith(".tmp"))).toEqual([]);
    asyncSpy.mockRestore(); syncSpy.mockRestore();
    const next = await mesh.put({ key: "test/key", value: 4, identity }); expect(next.version).toBeGreaterThan(old.version + 3);
  });

  it("propagates a failed elected barrier to a peer's no-op receipt, then retries", async () => {
    const directory = root(), owner = new MeshStore(directory, 65536, 100), peer = new MeshStore(directory, 65536, 100);
    const pending = pauseBarriers();
    const mutation = owner.put({ key: "resource/grant", value: 1, identity });
    const firstResult = Promise.allSettled([mutation]);
    await until(() => pending.length === 1);
    const unchanged = peer.delete({ key: "test/absent" });
    const peerResult = Promise.allSettled([unchanged]);
    await tick(); await tick();
    pending.shift()!(new Error("elected barrier unavailable"));
    expect((await firstResult)[0]!.status).toBe("rejected");
    expect((await peerResult)[0]!.status).toBe("rejected");
    vi.restoreAllMocks();
    expect(await peer.delete({ key: "test/absent" })).toEqual({ deleted: false });
    const completion = JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8"));
    expect(completion.error).toBeUndefined(); expect(completion.generation).toBe(1);
  });

  it("waits for a no-op batch's observed CAS generation before returning its skip result", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100), pending = pauseBarriers();
    const mutation = mesh.put({ key: "test/key", value: 1, identity });
    await until(() => pending.length === 1);
    let acked = false;
    const batch = mesh.writeBatch({ identity, ops: [{ kind: "put", key: "test/key", value: 2, ifVersion: 999, onConflict: "skip" }] }).then(result => { acked = true; return result; });
    await tick(); expect(acked).toBe(false);
    pending.shift()!(); const entry = await mutation;
    expect(await batch).toEqual([{ key: "test/key", applied: false, version: entry.version }]);
  });

  it("keeps non-durable writers fast while a file barrier is pending", async () => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100), pending = pauseBarriers();
    const durable = mesh.put({ key: "delivery/receipts/r", value: 1, identity });
    await until(() => pending.length === 1);
    const volatile = await mesh.put({ key: "topology/participants/p", value: 1, identity, durable: false });
    expect(volatile.value).toBe(1); expect(pending).toHaveLength(1); pending.shift()!(); await durable;
    await until(() => pending.length === 1); pending.shift()!();
    // Join the pending generation before cleanup, without issuing another mutation.
    await until(() => JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8")).generation >= volatile.version);
    await until(() => !fs.existsSync(path.join(directory, ".state-durability-lock")));
  });

  it.each(["missing", "torn", "older"])("recovers acked state after a %s unsynced successor, without reissuing its clock", async (loss) => {
    const directory = root(), mesh = new MeshStore(directory, 65536, 100);
    const acked = await mesh.put({ key: "test/key", value: "acked", identity });
    const pending = pauseBarriers(); let acknowledged = false;
    const speculative = mesh.put({ key: "test/key", value: "unacked", identity }).then(entry => { acknowledged = true; return entry; });
    await until(() => pending.length === 1); expect(acknowledged).toBe(false);
    // Deterministic power-loss model: retain synced checkpoint, discard/torn/newer namespace.
    const file = path.join(directory, "state.json");
    if (loss === "missing") fs.unlinkSync(file);
    else {
      const replacement = path.join(directory, "crash-replacement");
      fs.writeFileSync(replacement, loss === "torn" ? "{\"highWater\":" : JSON.stringify({ format: 1, entries: {}, highWater: 0 }));
      fs.renameSync(replacement, file);
    }
    const rebooted = new MeshStore(directory, 65536, 100);
    expect(rebooted.get("test/key", { fresh: true })!.value).toBe("acked");
    expect(rebooted.get("test/key", { fresh: true })!.version).toBe(acked.version);
    // Finish the simulated in-flight syscall before ending the test. It pinned the
    // speculative inode, so completing it may legitimately recover that unacked write.
    pending.shift()!(); await speculative;
    vi.restoreAllMocks();
    const recovered = await rebooted.put({ key: "test/new", value: 1, identity });
    expect(recovered.version).toBeGreaterThan(acked.version);
  });

  it.skipIf(process.platform === "win32")("re-establishes replaced ancestry, detach/reattach and same-target symlink receipts outside the lock", async () => {
    const parent = root(), directory = path.join(parent, "physical"), alias = path.join(parent, "alias");
    fs.mkdirSync(directory); fs.symlinkSync("physical", alias, "dir");
    const mesh = new MeshStore(alias, 65536, 100); await mesh.put({ key: "test/key", value: 1, identity });
    fs.renameSync(directory, path.join(parent, "away")); fs.renameSync(path.join(parent, "away"), directory);
    fs.unlinkSync(alias); fs.symlinkSync("./physical", alias, "dir");
    const events = observe(alias); await mesh.put({ key: "test/key", value: 2, identity });
    expect(events.some(event => event.kind === "sync" && event.file === parent && !event.locked)).toBe(true);
    expect(events.filter(event => event.kind === "sync" && event.locked)).toEqual([]);
    events.length = 0; await mesh.put({ key: "test/key", value: 3, identity });
    expect(events.filter(event => event.kind === "barrier-file-sync")).toHaveLength(1);
    expect(events.filter(event => event.kind === "sync")).toHaveLength(1);
  });

  it.skipIf(process.platform === "win32")("retains ordinary atomic-write receipt failure semantics", () => {
    const parent = root(), directory = path.join(parent, "mesh"); fs.mkdirSync(directory);
    const receipt = new DurableDirectory(directory); receipt.prepare();
    fs.renameSync(directory, path.join(parent, "old")); fs.mkdirSync(directory);
    const target = path.join(directory, "state.json");
    expect(() => writeFileAtomic(target, "unacknowledged", { durable: true, directoryReceipt: receipt })).toThrow("Durability receipt namespace changed");
    receipt.prepare(); expect(() => writeFileAtomic(target, "acknowledged", { durable: true, directoryReceipt: receipt })).not.toThrow();
  });
});
