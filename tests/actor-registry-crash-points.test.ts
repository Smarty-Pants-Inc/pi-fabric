import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryCheckpointBarrierError, ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorRegistryPayloads } from "../src/actors/registry-payloads.js";

// smarty-dev#6477 L7: payload and temp-file fsyncs happen before registry custody;
// under the lock only the registry and checkpoint renames and their directory
// barriers (pi-fabric#590 scope cut: the checkpoint barrier never leaves the lock).

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
  it("fsyncs appends and checkpoint temps before custody; under the lock only renames and their directory barriers", async () => {
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
    // directory-chain barrier, the three checkpoint renames and their directory barriers.
    expect(under.filter(event => event.startsWith("fsync file"))).toEqual([]);
    expect(under.filter(event => event.startsWith("rename"))).toEqual(["rename actors.json",
      ...ids.map(id => `rename ${path.join(id, "registry", "messages-head.json")}`)]);
    for (const id of ids) {
      expect(before).toContain(`fsync file ${path.join(id, "registry", "messages.jsonl")}`);
      expect(before.some(event => event.startsWith(`fsync file ${path.join(id, "registry", "messages-head.json.")}`) &&
        event.endsWith(".prepared"))).toBe(true);
      // POSIX: the checkpoint directory barrier runs under the lock, after its rename.
      if (!windows) {
        expect(under).toContain(`fsync dir ${path.join(id, "registry")}`);
        expect(under.indexOf(`fsync dir ${path.join(id, "registry")}`)).toBeGreaterThan(under.indexOf(`rename ${path.join(id, "registry", "messages-head.json")}`));
      }
    }
    // Nothing is left for after the lock: no barrier runs once custody is released.
    expect(after.filter(event => event.startsWith("fsync"))).toEqual([]);
    if (!windows) expect(under.indexOf("fsync dir ")).toBeGreaterThan(under.indexOf("rename actors.json"));
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

  it.each([{ failBarriers: false }, { failBarriers: true }])("a crash at every file-system step leaves the registry selecting only complete payloads (checkpoint barriers fail under the lock: $failBarriers)", async ({ failBarriers }) => {
    // Windows has no directory barrier to fail; the failing mode equals the plain one there.
    if (failBarriers && windows) return;
    const MUTATORS = ["openSync", "writeSync", "writeFileSync", "fsyncSync", "renameSync", "rmSync", "mkdirSync"] as const;
    const real = Object.fromEntries(MUTATORS.map(name => [name, (fs[name] as (...args: unknown[]) => unknown).bind(fs)]));
    // Barrier ORDER is proven by the test above; this one checks the visible state at
    // every crash point, so data barriers are counted as steps but not paid for.
    real.fsyncSync = () => undefined;
    usePlatform(platform);
    const quiet = () => vi.spyOn(fs, "fsyncSync").mockImplementation(() => undefined);
    // Arm a simulated crash at the n-th call; the process is "dead" afterwards, so no
    // rollback or cleanup runs either (that is what distinguishes a crash from an error).
    // With failBarriers, every checkpoint directory barrier taken under the registry lock
    // fails (EIO): the commit's barrier, its retry and the rollback's own barriers, so
    // the crash points cover the whole rollback under the lock (pi-fabric#590).
    const arm = (crashAt: number, actorRoot: string) => {
      let calls = 0, dead = false, locked = false;
      const registries = new Set(ids.map(id => path.join(actorRoot, id, "registry")));
      const lockPath = path.join(actorRoot, "actors.json.lock");
      const paths = new Map<number, string>();
      for (const name of MUTATORS) {
        vi.spyOn(fs, name).mockImplementation(((...args: unknown[]) => {
          if (dead || calls++ === crashAt) { dead = true; throw Object.assign(new Error("simulated crash"), { code: "ECRASH" }); }
          if (failBarriers && locked && name === "fsyncSync" && registries.has(paths.get(args[0] as number) ?? "")) {
            throw Object.assign(new Error("EIO: checkpoint barrier"), { code: "EIO" });
          }
          const result = real[name]!(...args);
          if (name === "openSync") paths.set(result as number, String(args[0]));
          if (name === "mkdirSync" && String(args[0]) === lockPath) locked = true;
          if (name === "rmSync" && String(args[0]) === lockPath) locked = false;
          return result;
        }) as never);
      }
      return { calls: () => calls, crashed: () => dead };
    };
    const expected = failBarriers ? "failed" : "committed";
    const run = (store: ActorRegistryStore, crashed: () => boolean) => appendAll(store).then(() => "committed", (error: unknown) =>
      crashed() ? "crashed" : error instanceof ActorRegistryCheckpointBarrierError && error.rolledBack ? "failed" : `error ${String(error)}`);
    quiet();
    const dry = await fixture();
    const probe = arm(Number.POSITIVE_INFINITY, dry.actorRoot);
    expect(await run(dry.store, probe.crashed)).toBe(expected);
    vi.restoreAllMocks();
    const steps = probe.calls();
    expect(steps).toBeGreaterThan(20);
    const outcomes = new Set<string>();
    for (let crashAt = 0; crashAt <= steps; crashAt++) {
      quiet();
      const { actorRoot, store } = await fixture();
      const crash = arm(crashAt, actorRoot);
      const result = await run(store, crash.crashed);
      vi.restoreAllMocks();
      quiet();
      expect(result).toBe(crash.crashed() ? "crashed" : expected);
      // Recover in a "new process": every referenced range is complete, all actors agree
      // (one atomic registry), and a checkpoint never leads the registry.
      const fresh = new ActorRegistryStore(actorRoot);
      const histories = fresh.records().map(row => JSON.stringify(fresh.messages(row)));
      expect(fresh.records().map(row => row.id).sort()).toEqual(ids);
      expect(new Set(histories).size).toBe(1);
      const state = histories[0]!;
      expect([JSON.stringify([m0]), JSON.stringify([m0, m1])]).toContain(state);
      // A save that failed (rolled back) and was not interrupted leaves the old state.
      if (result === "failed") expect(state).toBe(JSON.stringify([m0]));
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
    // Crashing after the durable registry rename but before the rollback finished
    // leaves the (unacknowledged) new state; both outcomes occur in both modes.
    expect([...outcomes].sort()).toEqual(["new", "old"]);
  }, 240_000);

  // pi-fabric#590 (scope cut at round 5): the checkpoint directory barrier runs UNDER the
  // registry lock again. An OLDER-release writer knows nothing about barriers; only the
  // registry lock stops it. It saves its owned rows without the selecting
  // `messageHistory` reference, so after an OS crash the checkpoint is the only
  // selector. Model the crash precisely: a checkpoint rename whose directory barrier
  // never succeeded is lost, i.e. each checkpoint reverts to its content at the last
  // successful barrier of its directory.
  const olderWriterScenario = async (failures: number) => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const registryPath = path.join(actorRoot, "actors.json");
    const lockPath = path.join(actorRoot, "actors.json.lock");
    const registries = new Set(ids.map(id => path.join(actorRoot, id, "registry")));
    const headFile = (directory: string) => path.join(directory, "messages-head.json");
    const durableHead = new Map([...registries].map(directory => [directory, fs.readFileSync(headFile(directory), "utf8")]));
    const registryBefore = fs.readFileSync(registryPath, "utf8");
    const paths = new Map<number, string>();
    const original = { open: fs.openSync, fsync: fs.fsyncSync, exists: fs.existsSync };
    const failedBarriers: Array<{ directory: string; lockHeld: boolean }> = [];
    const olderCommits: Array<{ unsynced: string[]; registry: string }> = [];
    let olderWriter: Promise<void> | undefined;
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: never[]) => {
      const fd = (original.open as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as typeof fs.openSync);
    // An older release's owned save: under the registry lock, rows without the selecting
    // reference and with an empty inline ring, written with its own atomic write.
    const startOlderWriter = () => new ActorRegistryStore(actorRoot).withLock(() => {
      olderCommits.push({ registry: fs.readFileSync(registryPath, "utf8"), unsynced: [...registries].filter(directory =>
        fs.readFileSync(headFile(directory), "utf8") !== durableHead.get(directory)) });
      const raw = JSON.parse(fs.readFileSync(registryPath, "utf8")) as { actors: Record<string, unknown>[] };
      const tmp = path.join(actorRoot, "actors.json.older.tmp");
      fs.writeFileSync(tmp, JSON.stringify({ format: 1, actors: raw.actors.map(row => ({ ...withoutRefs(row), messages: [] })) }));
      fs.renameSync(tmp, registryPath);
    });
    // The first `failures` checkpoint directory barriers under the lock fail (EIO). The
    // pre-custody append barriers (no lock held) succeed.
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      const file = paths.get(fd);
      const lockHeld = original.exists(lockPath);
      if (file && registries.has(file) && lockHeld && failedBarriers.length < failures) {
        failedBarriers.push({ directory: file, lockHeld });
        // The older writer tries to commit while the barrier is failing.
        olderWriter ??= startOlderWriter();
        throw Object.assign(new Error("EIO: checkpoint barrier"), { code: "EIO" });
      }
      original.fsync(fd);
      if (file && registries.has(file)) durableHead.set(file, fs.readFileSync(headFile(file), "utf8"));
    });
    const outcome = await appendAll(store).then(() => ({ ok: true as const }), (error: unknown) => ({ ok: false as const, error }));
    olderWriter ??= startOlderWriter();
    await olderWriter;
    vi.restoreAllMocks();
    // Windows has no directory fsync (renames are not modelled as losable here).
    if (windows) return { outcome, failedBarriers, olderCommits, registryBefore };
    const acknowledged = outcome.ok ? [m0, m1] : [m0];
    // The older writer committed exactly once, never over a checkpoint whose barrier had
    // not succeeded.
    expect(olderCommits).toHaveLength(1);
    expect(olderCommits[0]!.unsynced).toEqual([]);
    // OS crash now: unsynced checkpoint renames are lost.
    for (const directory of registries) fs.writeFileSync(headFile(directory), durableHead.get(directory)!);
    const fresh = new ActorRegistryStore(actorRoot);
    expect(fresh.records().map(row => row.id).sort()).toEqual(ids);
    for (const row of fresh.records()) {
      expect(row.messageHistory).toBeUndefined();
      // Recovery selects the checkpoint and keeps every acknowledged message, nothing more.
      expect(fresh.messages(row)).toEqual(acknowledged);
    }
    return { outcome, failedBarriers, olderCommits, registryBefore };
  };

  it("a checkpoint barrier that fails twice under the lock rolls the commit back under that same lock; an older-release writer cannot interleave and a crash keeps every acknowledged message (pi-fabric#590)", async () => {
    const { outcome, failedBarriers, olderCommits, registryBefore } = await olderWriterScenario(2);
    if (windows) { expect(outcome.ok).toBe(true); expect(failedBarriers).toEqual([]); return; }
    expect(outcome.ok).toBe(false);
    const error = (outcome as { error: unknown }).error as ActorRegistryCheckpointBarrierError;
    expect(error).toBeInstanceOf(ActorRegistryCheckpointBarrierError);
    expect(error.message).toMatch(/barrier failed under the registry lock; the commit was rolled back and nothing was committed: EIO/);
    expect(error.rolledBack).toBe(true);
    expect(error.retryable).toBe(true);
    expect((error.cause as NodeJS.ErrnoException).code).toBe("EIO");
    // The first try and its one retry, both for the same directory and both under the lock.
    expect(failedBarriers).toEqual([{ directory: error.directory, lockHeld: true }, { directory: error.directory, lockHeld: true }]);
    // The older writer, started while the barrier failed, ran only after the rollback had
    // restored the previous registry: it never saw the rolled-back commit.
    expect(olderCommits[0]!.registry).toBe(registryBefore);
  });

  it("a checkpoint barrier that fails once succeeds on its retry under the lock: the save is acknowledged and survives a crash (pi-fabric#590)", async () => {
    const { outcome, failedBarriers, olderCommits, registryBefore } = await olderWriterScenario(1);
    expect(outcome.ok).toBe(true);
    if (windows) { expect(failedBarriers).toEqual([]); return; }
    expect(failedBarriers).toHaveLength(1);
    expect(failedBarriers[0]!.lockHeld).toBe(true);
    expect(olderCommits[0]!.registry).not.toBe(registryBefore);
  });

  it("a persistently failing checkpoint barrier also fails the rollback's own checkpoint barriers: the registry is still restored, the save fails and a crash keeps the old state (pi-fabric#590)", async () => {
    const { outcome, failedBarriers } = await olderWriterScenario(Number.POSITIVE_INFINITY);
    if (windows) { expect(outcome.ok).toBe(true); expect(failedBarriers).toEqual([]); return; }
    expect(outcome.ok).toBe(false);
    const error = (outcome as { error: unknown }).error as ActorRegistryCheckpointBarrierError;
    expect(error.rolledBack).toBe(true);
    // Two failed tries for the first directory, then one failed rollback barrier per
    // restored checkpoint, all under the lock.
    expect(failedBarriers.length).toBe(2 + ids.length);
    expect(failedBarriers.every(entry => entry.lockHeld)).toBe(true);
  });

  // Errors are classified by code: only ENOENT for a removed checkpoint directory is
  // ignored (review round 5: existsSync hid EACCES/ENOTDIR/stat errors).
  it.each(["EIO", "EACCES", "ENOTDIR", "EPERM"])("a %s checkpoint barrier error fails the save and rolls it back under the lock", async (code) => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const registries = new Set(ids.map(id => path.join(actorRoot, id, "registry")));
    const lockPath = path.join(actorRoot, "actors.json.lock");
    const paths = new Map<number, string>();
    const original = { open: fs.openSync, fsync: fs.fsyncSync, exists: fs.existsSync };
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: never[]) => {
      const fd = (original.open as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as typeof fs.openSync);
    let failed = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      if (registries.has(paths.get(fd) ?? "") && original.exists(lockPath)) { failed++; throw Object.assign(new Error(`${code}: checkpoint barrier`), { code }); }
      original.fsync(fd);
    });
    const error = await appendAll(store).then(() => undefined, (reason: unknown) => reason);
    vi.restoreAllMocks();
    const fresh = new ActorRegistryStore(actorRoot);
    if (windows) {
      expect(error).toBeUndefined();
      expect(failed).toBe(0);
      for (const row of fresh.records()) expect(fresh.messages(row)).toEqual([m0, m1]);
      return;
    }
    expect(error).toBeInstanceOf(ActorRegistryCheckpointBarrierError);
    expect(((error as Error).cause as NodeJS.ErrnoException).code).toBe(code);
    expect((error as ActorRegistryCheckpointBarrierError).rolledBack).toBe(true);
    for (const row of fresh.records()) {
      expect(fresh.messages(row)).toEqual([m0]);
      expect(new ActorRegistryPayloads(actorRoot).savedHead(row.id)).toEqual(row.messageHistory);
    }
    // The store's cached snapshot was invalidated: it matches the disk, and the next save
    // (barriers healthy again) commits on top of the restored registry.
    expect(store.snapshot().bytes).toBe(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8"));
    const m2 = { id: "m2", direction: "in", text: "next" };
    await store.update(current => ({ actors: current.map(row => ({ ...withoutRefs(row), registryMessageAppend: [m2] })), value: true }));
    const next = new ActorRegistryStore(actorRoot);
    for (const row of next.records()) expect(next.messages(row)).toEqual([m0, m2]);
  });

  it("ENOENT for the registry directory barrier is NOT ignored: the save fails and rolls back", async () => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const lockPath = path.join(actorRoot, "actors.json.lock");
    const paths = new Map<number, string>();
    const original = { open: fs.openSync, fsync: fs.fsyncSync, exists: fs.existsSync };
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: never[]) => {
      const fd = (original.open as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as typeof fs.openSync);
    let failed = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      // The commit's registry barrier and its retry; the rollback's restore succeeds.
      if (paths.get(fd) === actorRoot && original.exists(lockPath) && failed < 2) { failed++; throw Object.assign(new Error("ENOENT: registry barrier"), { code: "ENOENT" }); }
      original.fsync(fd);
    });
    const error = await appendAll(store).then(() => undefined, (reason: unknown) => reason);
    vi.restoreAllMocks();
    const fresh = new ActorRegistryStore(actorRoot);
    if (windows) { expect(error).toBeUndefined(); return; }
    expect(error).toBeInstanceOf(ActorRegistryCheckpointBarrierError);
    expect((error as ActorRegistryCheckpointBarrierError).directory).toBe(actorRoot);
    expect((error as ActorRegistryCheckpointBarrierError).rolledBack).toBe(true);
    expect(failed).toBe(2);
    for (const row of fresh.records()) expect(fresh.messages(row)).toEqual([m0]);
  });

  it("a checkpoint directory removed under the lock (ENOENT: the actor was deleted) owes no barrier; the save commits", async () => {
    usePlatform(platform);
    const { actorRoot, store } = await fixture();
    const removed = ids[1]!;
    const original = { rename: fs.renameSync, rm: fs.rmSync };
    vi.spyOn(fs, "renameSync").mockImplementation((from: fs.PathLike, to: fs.PathLike) => {
      original.rename(from, to);
      // Concurrent actor removal right after the checkpoint rename, before its barrier.
      if (String(to) === path.join(actorRoot, removed, "registry", "messages-head.json")) {
        original.rm(path.join(actorRoot, removed), { recursive: true, force: true });
      }
    });
    await expect(appendAll(store)).resolves.toBe(true);
    vi.restoreAllMocks();
    expect(fs.existsSync(path.join(actorRoot, removed))).toBe(false);
    const fresh = new ActorRegistryStore(actorRoot);
    for (const row of fresh.records().filter(record => record.id !== removed)) {
      expect(fresh.messages(row)).toEqual([m0, m1]);
      expect(new ActorRegistryPayloads(actorRoot).savedHead(row.id)).toEqual(row.messageHistory);
    }
  });
});
