import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorRegistryPayloads } from "../src/actors/registry-payloads.js";

// smarty-dev#6477 L7: payload and temp-file fsyncs happen before registry custody;
// under the lock only the registry and checkpoint renames and their directory
// barriers remain, with main's failure handling unchanged (pi-fabric#590 round 6
// scope cut; tests/actor-registry-main-equivalence.test.ts compares it with main).

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
    // fails (EIO), including the restore's own barriers, so the crash points also cover
    // main's restore path under the lock (pi-fabric#590).
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
      crashed() ? "crashed" : (error as NodeJS.ErrnoException).code === "EIO" ? "failed" : `error ${String(error)}`);
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
      // A save that failed (restored, as on main) and was not interrupted leaves the old state.
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
    // Crashing after the durable registry rename but before the restore finished
    // leaves the (unacknowledged) new state; both outcomes occur in both modes.
    expect([...outcomes].sort()).toEqual(["new", "old"]);
  }, 240_000);
});
