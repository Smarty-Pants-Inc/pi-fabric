import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryCheckpointBarrierError, ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorRegistryPayloads } from "../src/actors/registry-payloads.js";

// smarty-dev#6477 L7: payload fsyncs happen before registry custody; under the
// lock only the registry rename (one directory barrier) and checkpoint renames.

// Windows has no directory fsync: syncDirectoryChain skips every directory barrier on
// win32 (src/core/atomic-write.ts). Each test runs on the host platform; POSIX hosts also
// run it on a simulated win32 (process.platform is read at call time), so the Windows
// path is covered on every CI host, and Windows CI runs it natively.
const hostPlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
const PLATFORMS: NodeJS.Platform[] = process.platform === "win32" ? ["win32"] : [process.platform, "win32"];
const usePlatform = (platform: NodeJS.Platform) => {
  if (platform !== hostPlatform.value) Object.defineProperty(process, "platform", { ...hostPlatform, value: platform });
};

const roots: string[] = [];
afterEach(() => {
  Object.defineProperty(process, "platform", hostPlatform);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const ids = ["a", "b", "c"].map(letter => letter.repeat(32));
const m0 = { id: "m0", direction: "in", text: "before" };
const m1 = { id: "m1", direction: "in", text: "after" };
const withoutRefs = ({ messageHistory: _history, messages: _messages, ...row }: Record<string, unknown>) => row;

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "registry-crash-points-")); roots.push(root);
  const actorRoot = path.join(root, "actors");
  const store = new ActorRegistryStore(actorRoot);
  await store.update(() => ({ actors: ids.map(id => ({ id, name: id, rootId: "session:crash", residency: "session",
    instructions: "persona", messages: [m0], status: "idle" })), durable: true, value: true }));
  return { actorRoot, store };
};
// Exactly what the manager saves: no selecting reference, one pending append per actor.
const appendAll = (store: ActorRegistryStore) => store.update(current => ({
  actors: current.map(row => ({ ...withoutRefs(row), registryMessageAppend: [m1] })), value: true }));

describe.each(PLATFORMS.map(platform => ({ platform, windows: platform === "win32" })))("actor registry payload staging (smarty-dev#6477 L7) on $platform", ({ platform, windows }) => {
  it("fsyncs appends and checkpoint temps before custody; under the lock only renames and one registry barrier", async () => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const events: string[] = [];
    const paths = new Map<number, string>();
    const original = { open: fs.openSync, fsync: fs.fsyncSync, rename: fs.renameSync, mkdir: fs.mkdirSync, rm: fs.rmSync };
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: never[]) => {
      const fd = (original.open as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      const file = paths.get(fd) ?? "?";
      events.push(`fsync ${fs.statSync(file).isDirectory() ? "dir" : "file"} ${path.relative(actorRoot, file)}`);
      original.fsync(fd);
    });
    vi.spyOn(fs, "renameSync").mockImplementation((from: fs.PathLike, to: fs.PathLike) => {
      events.push(`rename ${path.relative(actorRoot, String(to))}`);
      original.rename(from, to);
    });
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      const made = original.mkdir(file, options as fs.MakeDirectoryOptions & { recursive: true });
      if (String(file).endsWith("actors.json.lock")) events.push("lock");
      return made;
    }) as typeof fs.mkdirSync);
    vi.spyOn(fs, "rmSync").mockImplementation((file: fs.PathLike, options?: fs.RmOptions) => {
      if (String(file).endsWith("actors.json.lock")) events.push("unlock");
      original.rm(file, options);
    });
    await appendAll(store);
    vi.restoreAllMocks();
    const lock = events.indexOf("lock"), unlock = events.indexOf("unlock");
    expect(lock).toBeGreaterThan(0);
    expect(unlock).toBeGreaterThan(lock);
    const before = events.slice(0, lock), under = events.slice(lock + 1, unlock), after = events.slice(unlock + 1);
    // No file data barrier at all under custody; only the registry rename, its
    // directory-chain barrier and the three checkpoint renames.
    expect(under.filter(event => event.startsWith("fsync file"))).toEqual([]);
    expect(under.filter(event => event.startsWith("rename"))).toEqual(["rename actors.json",
      ...ids.map(id => `rename ${path.join(id, "registry", "messages-head.json")}`)]);
    for (const id of ids) {
      expect(before).toContain(`fsync file ${path.join(id, "registry", "messages.jsonl")}`);
      expect(before.some(event => event.startsWith(`fsync file ${path.join(id, "registry", "messages-head.json.")}`) &&
        event.endsWith(".prepared"))).toBe(true);
      // POSIX: the checkpoint directory barrier is deferred past the lock, before acknowledgment.
      if (!windows) expect(after).toContain(`fsync dir ${path.join(id, "registry")}`);
    }
    // Windows: no directory barrier is taken anywhere (none exists); file barriers stay.
    if (windows) expect(events.filter(event => event.startsWith("fsync dir"))).toEqual([]);
    expect(before.some(event => /^fsync file actors\.json\..*\.prepared\..*\.tmp$/.test(event))).toBe(true);
    for (const id of ids) {
      const row = store.records().find(record => record.id === id)!;
      expect(store.messages(row)).toEqual([m0, m1]);
      expect(new ActorRegistryPayloads(actorRoot).savedHead(id)).toEqual(row.messageHistory);
      expect(fs.readdirSync(path.join(actorRoot, id, "registry")).filter(file => file.endsWith(".prepared"))).toEqual([]);
    }
  });

  it("a crash at every file-system step leaves the registry selecting only complete payloads", async () => {
    const MUTATORS = ["openSync", "writeSync", "writeFileSync", "fsyncSync", "renameSync", "rmSync", "mkdirSync"] as const;
    const real = Object.fromEntries(MUTATORS.map(name => [name, (fs[name] as (...args: unknown[]) => unknown).bind(fs)]));
    // Barrier ORDER is proven by the test above; this one checks the visible state at
    // every crash point, so data barriers are counted as steps but not paid for.
    real.fsyncSync = () => undefined;
    usePlatform(platform);
    const quiet = () => vi.spyOn(fs, "fsyncSync").mockImplementation(() => undefined);
    // Arm a simulated crash at the n-th call; the process is "dead" afterwards, so no
    // rollback or cleanup runs either (that is what distinguishes a crash from an error).
    const arm = (crashAt: number) => {
      let calls = 0, dead = false;
      for (const name of MUTATORS) {
        vi.spyOn(fs, name).mockImplementation(((...args: unknown[]) => {
          if (dead || calls++ === crashAt) { dead = true; throw Object.assign(new Error("simulated crash"), { code: "ECRASH" }); }
          return real[name]!(...args);
        }) as never);
      }
      return { calls: () => calls, crashed: () => dead };
    };
    quiet();
    const dry = await fixture();
    const probe = arm(Number.POSITIVE_INFINITY);
    await appendAll(dry.store);
    vi.restoreAllMocks();
    const steps = probe.calls();
    expect(steps).toBeGreaterThan(20);
    const outcomes = new Set<string>();
    for (let crashAt = 0; crashAt <= steps; crashAt++) {
      quiet();
      const { actorRoot, store } = await fixture();
      const crash = arm(crashAt);
      const result = await appendAll(store).then(() => "committed", () => "crashed");
      vi.restoreAllMocks();
      quiet();
      expect(result).toBe(crash.crashed() ? "crashed" : "committed");
      // Recover in a "new process": every referenced range is complete, all actors agree
      // (one atomic registry), and a checkpoint never leads the registry.
      const fresh = new ActorRegistryStore(actorRoot);
      const histories = fresh.records().map(row => JSON.stringify(fresh.messages(row)));
      expect(fresh.records().map(row => row.id).sort()).toEqual(ids);
      expect(new Set(histories).size).toBe(1);
      const state = histories[0]!;
      expect([JSON.stringify([m0]), JSON.stringify([m0, m1])]).toContain(state);
      outcomes.add(state === JSON.stringify([m0]) ? "old" : "new");
      const payloads = new ActorRegistryPayloads(actorRoot);
      for (const id of ids) {
        const saved = payloads.savedHead(id);
        const checkpoint = JSON.stringify(payloads.messages({ id, messageHistory: saved }));
        if (state === JSON.stringify([m0])) expect(checkpoint).toBe(state);
        else expect([JSON.stringify([m0]), state]).toContain(checkpoint);
      }
      // A dead holder's lock is reaped by the next process; abandoned appends never
      // become a predecessor of the next commit.
      fs.rmSync(path.join(actorRoot, "actors.json.lock"), { recursive: true, force: true });
      await fresh.update(current => ({ actors: current.map(row => ({ ...withoutRefs(row),
        registryMessageAppend: [{ id: "m2", direction: "in", text: "next" }] })), value: true }));
      const next = new ActorRegistryStore(actorRoot);
      for (const row of next.records()) {
        expect(next.messages(row)).toEqual([...JSON.parse(state), { id: "m2", direction: "in", text: "next" }]);
      }
    }
    expect([...outcomes].sort()).toEqual(["new", "old"]);
  }, 120_000);

  it("fails closed when every checkpoint barrier attempt fails: the commit is rolled back under the lock, update() rejects and readers see a consistent registry (pi-fabric#590)", async () => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const registries = new Set(ids.map(id => path.join(actorRoot, id, "registry")));
    const paths = new Map<number, string>();
    const original = { open: fs.openSync, fsync: fs.fsyncSync, rm: fs.rmSync };
    let unlocked = false, failing = true, directorySyncs = 0;
    const failures = new Map<string, number>();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: never[]) => {
      const fd = (original.open as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "rmSync").mockImplementation((file: fs.PathLike, options?: fs.RmOptions) => {
      original.rm(file, options);
      if (String(file).endsWith("actors.json.lock")) unlocked = true;
    });
    // Every checkpoint directory barrier fails AFTER the commit's lock is released: both
    // deferred settle() attempts and both retries under the reacquired lock.
    // Pre-custody barriers of the same directory succeed.
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      const file = paths.get(fd);
      if (fs.fstatSync(fd).isDirectory()) directorySyncs++;
      if (failing && unlocked && file && registries.has(file)) {
        failures.set(file, (failures.get(file) ?? 0) + 1);
        throw Object.assign(new Error("EIO: checkpoint barrier"), { code: "EIO" });
      }
      original.fsync(fd);
    });
    // The next reader sees one consistent state: every actor selects the expected history
    // and its checkpoint equals the registry head (never leads, here does not lag either).
    const consistent = (expected: unknown[]) => {
      const fresh = new ActorRegistryStore(actorRoot);
      const payloads = new ActorRegistryPayloads(actorRoot);
      expect(fresh.records().map(row => row.id).sort()).toEqual(ids);
      for (const row of fresh.records()) {
        expect(fresh.messages(row)).toEqual(expected);
        expect(payloads.savedHead(row.id)).toEqual(row.messageHistory);
        expect(fs.readdirSync(path.join(actorRoot, row.id, "registry")).filter(file => file.endsWith(".prepared"))).toEqual([]);
      }
      // The store's own snapshot (invalidated after a failed barrier) matches the disk.
      expect(store.snapshot().bytes).toBe(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8"));
    };
    const m2 = { id: "m2", direction: "in", text: "next" };
    const error = await appendAll(store).then(() => undefined, (reason: unknown) => reason);
    if (windows) {
      // Windows has no directory fsync, so there is no checkpoint barrier to fail: the
      // save is acknowledged, nothing is owed, and the next save is not blocked.
      expect(error).toBeUndefined();
      expect(directorySyncs).toBe(0);
      expect(failures.size).toBe(0);
      consistent([m0, m1]);
      await expect(store.update(current => ({ actors: current.map(row => ({ ...withoutRefs(row),
        registryMessageAppend: [m2] })), value: "ok" }))).resolves.toBe("ok");
      vi.restoreAllMocks();
      consistent([m0, m1, m2]);
      return;
    }
    expect(directorySyncs).toBeGreaterThan(0);
    // No success: a clear, retryable error. Nobody committed on top, so the commit was
    // rolled back under the lock: nothing is acknowledged and the caller re-appends.
    expect(error).toBeInstanceOf(ActorRegistryCheckpointBarrierError);
    const barrier = error as ActorRegistryCheckpointBarrierError<boolean>;
    expect(barrier.message).toMatch(/checkpoint barrier failed under the registry lock; the commit was rolled back/);
    expect(barrier.retryable).toBe(true);
    expect(barrier.rolledBack).toBe(true);
    expect(barrier.committed).toBeUndefined();
    expect([...barrier.directories].sort()).toEqual([...registries].sort());
    // Two deferred attempts, two retries under the reacquired lock, then the rollback's
    // own (best-effort) barrier: the directory stays owed.
    for (const directory of registries) expect(failures.get(directory)).toBe(5);
    consistent([m0]);

    // While the owed barrier still fails, the next update fails closed BEFORE it
    // writes anything: no registry rename, no append, no success.
    const registryBefore = fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8");
    const logsBefore = ids.map(id => fs.statSync(path.join(actorRoot, id, "registry", "messages.jsonl")).size);
    let selected = false;
    const blocked = await store.update(current => { selected = true; return { actors: current.map(row => ({ ...withoutRefs(row),
      registryMessageAppend: [{ id: "m2", direction: "in", text: "next" }] })), value: true }; }).then(() => undefined, (reason: unknown) => reason);
    expect(blocked).toBeInstanceOf(ActorRegistryCheckpointBarrierError);
    expect((blocked as ActorRegistryCheckpointBarrierError).committed).toBeUndefined();
    expect((blocked as Error).message).toMatch(/still fails; nothing was committed/);
    expect(selected).toBe(false);
    expect(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8")).toBe(registryBefore);
    expect(ids.map(id => fs.statSync(path.join(actorRoot, id, "registry", "messages.jsonl")).size)).toEqual(logsBefore);
    consistent([m0]);

    // Once the barrier works again, the owed barrier settles first and the save succeeds.
    failing = false;
    await expect(store.update(current => ({ actors: current.map(row => ({ ...withoutRefs(row),
      registryMessageAppend: [{ id: "m2", direction: "in", text: "next" }] })), value: "ok" }))).resolves.toBe("ok");
    vi.restoreAllMocks();
    consistent([m0, { id: "m2", direction: "in", text: "next" }]);
  });

  // pi-fabric#590 security round: an OLDER-release writer knows nothing about owed
  // barriers; only the registry lock stops it. It saves its owned rows without the
  // selecting `messageHistory` reference, so after an OS crash the checkpoint is the
  // only selector. Model the crash precisely: a checkpoint rename whose directory
  // barrier never succeeded is lost, i.e. each checkpoint reverts to its content at
  // the last successful barrier of its directory.
  const olderWriterScenario = async (options: { recoverUnderLock: boolean }) => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const lockPath = path.join(actorRoot, "actors.json.lock");
    const registries = new Set(ids.map(id => path.join(actorRoot, id, "registry")));
    const headFile = (directory: string) => path.join(directory, "messages-head.json");
    const durableHead = new Map([...registries].map(directory => [directory, fs.readFileSync(headFile(directory), "utf8")]));
    const paths = new Map<number, string>();
    const original = { open: fs.openSync, fsync: fs.fsyncSync, rm: fs.rmSync, exists: fs.existsSync };
    let unlocked = false;
    const failedBarriers: Array<{ directory: string; lockHeld: boolean }> = [];
    const olderCommits: Array<{ unsynced: string[] }> = [];
    let olderWriter: Promise<void> | undefined;
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: never[]) => {
      const fd = (original.open as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "rmSync").mockImplementation((file: fs.PathLike, options?: fs.RmOptions) => {
      original.rm(file, options);
      if (String(file).endsWith("actors.json.lock")) unlocked = true;
    });
    // An older release's owned save: under the registry lock, rows without the selecting
    // reference and with an empty inline ring, written with its own durable atomic write.
    const startOlderWriter = () => new ActorRegistryStore(actorRoot).withLock(() => {
      olderCommits.push({ unsynced: [...registries].filter(directory =>
        fs.readFileSync(headFile(directory), "utf8") !== durableHead.get(directory)) });
      const raw = JSON.parse(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8")) as { actors: Record<string, unknown>[] };
      const tmp = path.join(actorRoot, "actors.json.older.tmp");
      fs.writeFileSync(tmp, JSON.stringify({ format: 1, actors: raw.actors.map(row => ({ ...withoutRefs(row), messages: [] })) }));
      fs.renameSync(tmp, path.join(actorRoot, "actors.json"));
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      const file = paths.get(fd);
      if (unlocked && file && registries.has(file)) {
        const lockHeld = original.exists(lockPath);
        if (!(options.recoverUnderLock && lockHeld)) {
          failedBarriers.push({ directory: file, lockHeld });
          // After both deferred attempts failed, an older writer tries to commit.
          if (lockHeld && !olderWriter) olderWriter = startOlderWriter();
          throw Object.assign(new Error("EIO: checkpoint barrier"), { code: "EIO" });
        }
      }
      original.fsync(fd);
      if (file && registries.has(file)) durableHead.set(file, fs.readFileSync(headFile(file), "utf8"));
    });
    const outcome = await appendAll(store).then(() => ({ ok: true as const }), (error: unknown) => ({ ok: false as const, error }));
    if (!olderWriter) olderWriter = startOlderWriter();
    await olderWriter;
    vi.restoreAllMocks();
    // Windows has no directory fsync (renames are not modelled as losable here).
    if (windows) return { outcome, failedBarriers };
    // What the manager treats as appended: a success, or an error carrying `committed`.
    const acknowledged = outcome.ok || (outcome.error instanceof ActorRegistryCheckpointBarrierError && outcome.error.committed)
      ? [m0, m1] : [m0];
    // The older writer never committed over a checkpoint whose barrier had not succeeded.
    expect(olderCommits).toEqual([{ unsynced: [] }]);
    // OS crash now: unsynced checkpoint renames are lost.
    for (const directory of registries) fs.writeFileSync(headFile(directory), durableHead.get(directory)!);
    const fresh = new ActorRegistryStore(actorRoot);
    expect(fresh.records().map(row => row.id).sort()).toEqual(ids);
    for (const row of fresh.records()) {
      expect(row.messageHistory).toBeUndefined();
      // Recovery selects the checkpoint and keeps every acknowledged message, nothing more.
      expect(fresh.messages(row)).toEqual(acknowledged);
    }
    return { outcome, failedBarriers };
  };

  it("an older-release writer cannot commit over an unsynced checkpoint after both deferred barriers fail; a crash keeps every acknowledged message (pi-fabric#590 security round)", async () => {
    const { outcome, failedBarriers } = await olderWriterScenario({ recoverUnderLock: false });
    if (windows) { expect(outcome.ok).toBe(true); expect(failedBarriers).toEqual([]); return; }
    expect(outcome.ok).toBe(false);
    const error = (outcome as { error: unknown }).error as ActorRegistryCheckpointBarrierError;
    expect(error).toBeInstanceOf(ActorRegistryCheckpointBarrierError);
    expect(error.rolledBack).toBe(true);
    expect(error.committed).toBeUndefined();
    // Per directory: two deferred failures outside the lock, then two retries and the
    // rollback's barrier under it.
    for (const directory of new Set(failedBarriers.map(entry => entry.directory))) {
      expect(failedBarriers.filter(entry => entry.directory === directory).map(entry => entry.lockHeld)).toEqual([false, false, true, true, true]);
    }
  });

  it("a deferred barrier that fails twice succeeds under the reacquired lock: the save is acknowledged and survives a crash (pi-fabric#590 security round)", async () => {
    const { outcome, failedBarriers } = await olderWriterScenario({ recoverUnderLock: true });
    expect(outcome.ok).toBe(true);
    if (windows) { expect(failedBarriers).toEqual([]); return; }
    expect(failedBarriers.length).toBe(ids.length * 2);
    expect(failedBarriers.every(entry => !entry.lockHeld)).toBe(true);
  });
});
