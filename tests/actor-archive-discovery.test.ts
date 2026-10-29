import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { discoverActorArchiveSources } from "../src/agents/actor-archive-discovery.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { MeshStore } from "../src/mesh/store.js";
import type { AgentRunResult } from "../src/agents/types.js";

// Keep: recorded 36,427-entry incident + raw-trace/archive custody data safety.
// Fixtures and raw sentinels are deliberately retained. Commit's metadata removal is recorded, never performed.
afterEach(() => vi.restoreAllMocks());
const actorId = "archive-owner";
const id = (value: number) => value.toString(16).padStart(32, "0");
// Not a production PI_FABRIC_* selector: fleet isolation deliberately scrubs those before suite imports.
const retainedRoot = process.env.TEST_ACTOR_ARCHIVE_RETAINED_ROOT;
const roots: string[] = [], sentinels = new Map<string, string>();
const within = (root: string, target: string) => target === root || target.startsWith(`${root}${path.sep}`);
afterAll(() => {
  if (!retainedRoot) return; // Ordinary CI may use disposable fixtures; retained local runs must prove retention.
  const base = fs.realpathSync(retainedRoot), disposable = fs.realpathSync(os.tmpdir());
  expect(within(disposable, base)).toBe(false);
  for (const root of roots) {
    expect(within(base, fs.realpathSync(root))).toBe(true);
    expect(within(disposable, fs.realpathSync(root))).toBe(false);
  }
  for (const [file, bytes] of sentinels) expect(fs.readFileSync(file, "utf8")).toBe(bytes);
  fs.writeFileSync(path.join(base, "fixture-retention.json"), JSON.stringify({ base, disposable, roots,
    sentinels: [...sentinels].map(([file, bytes]) => ({ file, bytes })) }), { flag: "wx", mode: 0o600 });
});
function fixture() {
  if (retainedRoot) expect(within(fs.realpathSync(os.tmpdir()), fs.realpathSync(retainedRoot))).toBe(false);
  const root = fs.mkdtempSync(path.join(retainedRoot ?? os.tmpdir(), "actor-archive-discovery-test-"));
  roots.push(root);
  const runRoot = path.join(root, "runs"), sessionFile = path.join(root, "session.jsonl");
  fs.mkdirSync(runRoot, { mode: 0o700 });
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, { runRoot });
  const source = (runId: string, overrides: Record<string, unknown> = {}) => {
    const directory = path.join(runRoot, runId);
    fs.mkdirSync(directory, { mode: 0o700 });
    fs.writeFileSync(path.join(directory, "actor-run-archive-pending.json"), JSON.stringify({ format: 1, runId, actorId, sessionFile, ...overrides }), { mode: 0o600 });
    const sentinel = path.join(directory, "events.jsonl"), bytes = `raw trace ${runId}\n`;
    fs.writeFileSync(sentinel, bytes, { mode: 0o600 });
    sentinels.set(sentinel, bytes);
    return directory;
  };
  return { root, runRoot, sessionFile, agents, source };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
type MarkerRead = (handle: fs.promises.FileHandle, file: string, buffer: Buffer, offset: number,
  length: number, position: number) => Promise<{ bytesRead: number; buffer: Buffer }>;
/** Real descriptors, with only read scheduling/short-read/error injection. Counts exclude the directory handle. */
function trackMarkers(read?: MarkerRead, onClose?: (file: string) => void,
  onOpen?: (handle: fs.promises.FileHandle, file: string) => void, beforeOpen?: (file: string) => void) {
  const open = fs.promises.open.bind(fs.promises), active = new Set<fs.promises.FileHandle>();
  const handles: fs.promises.FileHandle[] = [], opened: string[] = [];
  let peak = 0;
  vi.spyOn(fs.promises, "open").mockImplementation(async (file, flags, mode) => {
    const name = String(file); beforeOpen?.(name);
    const handle = await open(file, flags, mode);
    handles.push(handle); opened.push(path.basename(path.dirname(name))); active.add(handle);
    peak = Math.max(peak, active.size); onOpen?.(handle, name);
    return new Proxy(handle, { get(target, key) {
      if (key === "read" && read) return (buffer: Buffer, offset: number, length: number, position: number) =>
        read(target, name, buffer, offset, length, position);
      if (key === "close") return async () => {
        try { await target.close(); } finally { active.delete(target); }
        onClose?.(name);
      };
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    } });
  });
  return { active, handles, opened, peak: () => peak };
}

describe("actor archive source discovery", () => {
  it("uses the real manager entry point without synchronous filesystem discovery", async () => {
    const f = fixture(), directory = f.source(id(1));
    for (const method of ["readdirSync", "opendirSync", "lstatSync", "statSync", "readFileSync"] as const) {
      vi.spyOn(fs, method).mockImplementation(() => { throw new Error(`synchronous discovery: ${method}`); });
    }
    const pending = f.agents.actorArchiveSources(actorId, f.sessionFile);
    expect(pending).toBeInstanceOf(Promise);
    expect(await pending).toEqual(new Map([[id(1), directory]]));
  });

  it("performs no synchronous filesystem discovery in the asynchronous helper", async () => {
    const f = fixture(), directory = f.source(id(1));
    for (const method of ["readdirSync", "opendirSync", "lstatSync", "statSync", "readFileSync"] as const) {
      vi.spyOn(fs, method).mockImplementation(() => { throw new Error(`synchronous discovery: ${method}`); });
    }
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
  });

  it("validates exact fenced sources through the asynchronous helper without altering raw traces", async () => {
    const f = fixture(), directory = f.source(id(1));
    const trace = fs.readFileSync(path.join(directory, "events.jsonl"), "utf8");
    f.source(id(2), { actorId: "other" }); f.source(id(3), { sessionFile: `${f.sessionFile}.other` });
    f.source(id(4), { runId: id(5) }); f.source(id(5), { format: 2 });
    const malformed = f.source(id(6)), large = f.source(id(7));
    fs.writeFileSync(path.join(malformed, "actor-run-archive-pending.json"), "not JSON");
    fs.writeFileSync(path.join(large, "actor-run-archive-pending.json"), " ".repeat(4097));
    f.source("not-a-run-id");
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
    expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe(trace);
    expect(fs.existsSync(path.join(directory, "actor-run-archive-pending.json"))).toBe(true);
    expect(fs.existsSync(path.join(malformed, "actor-run-archive-pending.json"))).toBe(true);
  });

  it.skipIf(process.platform === "win32")("rejects symlink roots, run directories, marker symlinks and hard-linked markers", async () => {
    const f = fixture(), directory = f.source(id(1));
    // Only hostile aliases live in disposable TMPDIR: the deletion guard must not be bypassed to link into retained roots.
    const aliases = fs.mkdtempSync(path.join(os.tmpdir(), "archive-aliases-"));
    const linkRoot = path.join(aliases, "linked-root"); fs.symlinkSync(f.runRoot, linkRoot, "dir");
    expect(await discoverActorArchiveSources(linkRoot, actorId, f.sessionFile)).toEqual(new Map());
    const aliasRuns = path.join(aliases, "runs"); fs.mkdirSync(aliasRuns);
    fs.symlinkSync(directory, path.join(aliasRuns, id(2)), "dir");
    const linked = path.join(aliasRuns, id(3)); fs.mkdirSync(linked);
    fs.symlinkSync(path.join(directory, "actor-run-archive-pending.json"), path.join(linked, "actor-run-archive-pending.json"));
    const hard = path.join(aliasRuns, id(4)); fs.mkdirSync(hard);
    fs.writeFileSync(path.join(hard, "actor-run-archive-pending.json"), JSON.stringify({ format: 1, runId: id(4), actorId, sessionFile: f.sessionFile }));
    fs.linkSync(path.join(hard, "actor-run-archive-pending.json"), path.join(aliases, "extra-marker-link"));
    expect(await discoverActorArchiveSources(aliasRuns, actorId, f.sessionFile)).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
  });

  it("uses exact helper lookup without root enumeration and observes later markers without a cache", async () => {
    const f = fixture(), directory = f.source(id(1));
    const enumeration = vi.spyOn(fs.promises, "opendir").mockRejectedValue(new Error("known runs must not enumerate"));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(1))).toEqual(new Map([[id(1), directory]]));
    expect(await discoverActorArchiveSources(f.runRoot, "other", f.sessionFile, id(1))).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, `${f.sessionFile}.other`, id(1))).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, "../escape")).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(2))).toEqual(new Map());
    const later = f.source(id(2));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(2))).toEqual(new Map([[id(2), later]]));
    expect(enumeration).not.toHaveBeenCalled();
  });

  it("inspects only the exact run for retries/commit and preserves collection custody on mismatch", async () => {
    const f = fixture(), directory = f.source(id(1));
    const marker = path.join(directory, "actor-run-archive-pending.json");
    const trace = fs.readFileSync(path.join(directory, "events.jsonl"), "utf8");
    const enumeration = vi.spyOn(fs.promises, "opendir").mockRejectedValue(new Error("must not enumerate for a known run"));
    const syncEnumeration = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("must not enumerate for commit"); });
    const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => {});
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(1))).toEqual(new Map([[id(1), directory]]));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, "../escape")).toEqual(new Map());
    await f.agents.commitActorArchive(id(1), "other", f.sessionFile);
    await f.agents.commitActorArchive(id(1), actorId, `${f.sessionFile}.other`);
    await f.agents.commitActorArchive("../escape", actorId, f.sessionFile);
    expect(remove).not.toHaveBeenCalled();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const discover = f.agents.actorArchiveSources.bind(f.agents);
    const lookup = vi.spyOn(f.agents, "actorArchiveSources").mockImplementationOnce(async (owner, session, runId) => {
      await gate;
      return discover(owner, session, runId);
    });
    const committing = f.agents.commitActorArchive(id(1), actorId, f.sessionFile);
    try {
      expect(lookup).toHaveBeenCalledWith(actorId, f.sessionFile, id(1));
      await Promise.resolve();
      expect(remove).not.toHaveBeenCalled(); // Exact lookup must settle before discharging custody.
    } finally { release(); }
    await committing;
    expect(remove.mock.calls).toEqual([[marker]]);
    expect(enumeration).not.toHaveBeenCalled(); expect(syncEnumeration).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe(trace);
    expect(fs.existsSync(marker)).toBe(true); // Recorded-call probe, never a real custody discharge/deletion.
    expect(f.agents.retentionCustodyVeto(id(1))).toBe(true);
  });

  it("awaits completion/close discovery and archives restarted-owner sources before recorded cleanup", async () => {
    const f = fixture();
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
    // Mutable mesh/lock metadata and archive-copy destinations stay inside the guard; raw run sources stay retained.
    const mesh = new MeshStore(fs.mkdtempSync(path.join(os.tmpdir(), "archive-metadata-")), 64 * 1024, 100);
    const identity = { id: "session:archive-discovery", name: "Main", kind: "main" as const, sessionId: "archive-discovery" };
    const actors = new ActorManager(identity.sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, f.agents, () => {},
      { actorRoot: path.join(mesh.root, "actors"), persistent: true });
    let logDir: string | undefined;
    const cleanup = vi.spyOn(f.agents, "cleanup").mockImplementation(async runId => {
      // Record cleanup only after the real archive has the exact raw source bytes. Never execute cleanup.
      const source = path.join(f.runRoot, runId, "events.jsonl");
      expect(logDir).toBeDefined();
      expect(fs.readFileSync(path.join(logDir!, runId, "events.jsonl"), "utf8")).toBe(fs.readFileSync(source, "utf8"));
      return { cleaned: false };
    });
    const rm = fs.rmSync.bind(fs), commits: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (path.basename(String(file)) === "actor-run-archive-pending.json") { commits.push(String(file)); return; }
      const target = path.resolve(String(file));
      if (target === f.runRoot || target.startsWith(`${f.runRoot}${path.sep}`)) throw new Error("raw source deletion is forbidden");
      rm(file, options); // Metadata lock release only; raw trees are never removed.
    });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let scanning = false;
    const discover = f.agents.actorArchiveSources.bind(f.agents);
    const calls: (string | undefined)[] = [];
    vi.spyOn(f.agents, "actorArchiveSources").mockImplementation(async (owner, session, runId?: string) => {
      calls.push(runId);
      if (runId === undefined) { scanning = true; await gate; }
      return discover(owner, session, runId);
    });
    try {
      const actor = await actors.create({ name: "discovery", instructions: "Answer briefly.", protected: false });
      logDir = actor.logDir;
      const current = f.source(id(1), { actorId: actor.id, sessionFile: actor.sessionFile });
      const prior = f.source(id(2), { actorId: actor.id, sessionFile: actor.sessionFile });
      vi.spyOn(f.agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
        onSpawned?.({ id: id(1) } as Parameters<NonNullable<typeof onSpawned>>[0]);
        return { id: id(1), status: "completed", text: "answer", usage: {}, inferenceStarted: true } as AgentRunResult;
      });
      await actors.ask(actor.id, "Complete one run.");
      await vi.waitFor(() => expect(scanning).toBe(true));
      expect(fs.existsSync(path.join(current, "events.jsonl"))).toBe(true);
      expect(cleanup).not.toHaveBeenCalled(); // The full completion discovery is still pending.
      release();
      await vi.waitFor(() => expect(actors.inFlightCount()).toBe(0));
      for (const [runId, source] of [[id(1), current], [id(2), prior]]) {
        expect(fs.readFileSync(path.join(actor.logDir!, runId!, "events.jsonl"), "utf8")).toBe(fs.readFileSync(path.join(source!, "events.jsonl"), "utf8"));
        expect(commits).toContain(path.join(source!, "actor-run-archive-pending.json"));
      }
      expect(cleanup.mock.calls.map(([runId]) => runId).sort()).toEqual([id(1), id(2)]);
      expect(calls).toContain(id(1)); expect(calls).toContain(id(2)); expect(calls).toContain(undefined);
      const atClose = f.source(id(3), { actorId: actor.id, sessionFile: actor.sessionFile });
      await actors.close();
      expect(fs.readFileSync(path.join(actor.logDir!, id(3), "events.jsonl"), "utf8")).toBe(fs.readFileSync(path.join(atClose, "events.jsonl"), "utf8"));
      expect(fs.existsSync(path.join(atClose, "actor-run-archive-pending.json"))).toBe(true);
      expect(commits).toContain(path.join(atClose, "actor-run-archive-pending.json"));
      expect(cleanup.mock.calls.map(([runId]) => runId).sort()).toEqual([id(1), id(2)]); // Close archives, never collects these raw sources.
    } finally { release(); await actors.close(); }
  });

  it("bounds marker FDs at 32 plus one directory handle and preserves enumeration order despite reverse completion", async () => {
    const f = fixture();
    for (let n = 1; n <= 70; n++) f.source(id(n));
    const gates = new Map<string, ReturnType<typeof deferred>>(), closed = new Map<string, ReturnType<typeof deferred>>();
    const firstWave = deferred(), completed: string[] = [], enumeration: string[] = [];
    for (let n = 1; n <= 70; n++) { gates.set(id(n), deferred()); closed.set(id(n), deferred()); }
    const io = trackMarkers(async (handle, file, buffer, offset, length, position) => {
      if (io.opened.length === 32) firstWave.resolve();
      if (position === 0 && io.opened.indexOf(path.basename(path.dirname(file))) < 32) {
        await gates.get(path.basename(path.dirname(file)))!.promise;
      }
      return handle.read(buffer, offset, length, position);
    }, file => {
      const runId = path.basename(path.dirname(file)); completed.push(runId); closed.get(runId)!.resolve();
    });
    const opendir = fs.promises.opendir.bind(fs.promises);
    let held: fs.Dir | undefined, directories = 0, directoryPeak = 0;
    vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args) => {
      held = await opendir(...args); directories++; directoryPeak = Math.max(directoryPeak, directories);
      const read = held.read.bind(held), close = held.close.bind(held);
      vi.spyOn(held, "read").mockImplementation(async () => {
        const entry = await read(); if (entry) enumeration.push(entry.name); return entry;
      });
      vi.spyOn(held, "close").mockImplementation(async () => { await close(); directories--; });
      return held;
    });
    const pending = discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile);
    // Attach a rejection handler immediately, including while the test is controlling the first wave.
    const outcome = pending.then(value => ({ value }), error => ({ error }));
    try {
      await firstWave.promise;
      expect(io.active.size).toBe(32); expect(directories).toBe(1);
      const reverse = io.opened.slice(0, 32).reverse();
      for (const runId of reverse) { gates.get(runId)!.resolve(); await closed.get(runId)!.promise; }
      expect(completed.slice(0, 32)).toEqual(reverse);
    } finally { for (const gate of gates.values()) gate.resolve(); }
    const result = await outcome;
    expect("error" in result ? result.error : undefined).toBeUndefined();
    expect("value" in result ? [...result.value] : []).toEqual(enumeration.map(runId => [runId, path.join(f.runRoot, runId)]));
    expect(enumeration).toHaveLength(70); expect(io.peak()).toBe(32); expect(io.active.size).toBe(0);
    expect(directoryPeak).toBe(1); expect(directories).toBe(0);
    expect(io.handles.every(handle => handle.fd === -1)).toBe(true);
    await expect(held!.read()).rejects.toMatchObject({ code: "ERR_DIR_CLOSED" });
  });

  it("completes partial reads and skips malformed, unreadable and identity-mismatched markers without leaking FDs", async () => {
    const f = fixture();
    for (let n = 1; n <= 40; n++) f.source(id(n));
    fs.writeFileSync(path.join(f.runRoot, id(2), "actor-run-archive-pending.json"), "not JSON");
    const io = trackMarkers(async (handle, file, buffer, offset, length, position) => {
      const runId = path.basename(path.dirname(file));
      if (runId === id(3)) throw new Error("marker read failed");
      if (runId === id(6)) { // Marker grew after lstat/opened stat: the fixed 4,097-byte read must still veto it.
        buffer.fill(32, offset, offset + length); return { bytesRead: length, buffer };
      }
      return handle.read(buffer, offset, Math.min(7, length), position);
    }, undefined, (handle, file) => {
      if (path.basename(path.dirname(file)) === id(5)) {
        const stat = handle.stat.bind(handle);
        vi.spyOn(handle, "stat").mockImplementation((async () => {
          const value = await stat(); value.ino++; return value;
        }) as typeof handle.stat);
      }
    }, file => { if (path.basename(path.dirname(file)) === id(4)) throw new Error("marker open failed"); });
    const lstat = fs.promises.lstat.bind(fs.promises);
    vi.spyOn(fs.promises, "lstat").mockImplementation((...args) => {
      if (String(args[0]) === path.join(f.runRoot, id(7), "actor-run-archive-pending.json")) return Promise.reject(new Error("marker lstat failed"));
      return lstat(...args);
    });
    const expected = Array.from({ length: 40 }, (_, n) => id(n + 1)).filter(runId => ![2, 3, 4, 5, 6, 7].map(id).includes(runId));
    expect([...(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).keys()].sort()).toEqual(expected);
    expect(io.active.size).toBe(0); expect(io.peak()).toBeLessThanOrEqual(32);
    expect(io.handles.every(handle => handle.fd === -1)).toBe(true);
  });

  it.each(["directory read", "marker close"] as const)("drains in-flight descriptors before rejecting a %s failure and permits retry", async failureKind => {
    const f = fixture();
    for (let n = 1; n <= 9; n++) f.source(id(n));
    const gate = deferred(), siblingReading = deferred(), failureObserved = deferred(), failure = new Error(`${failureKind} failed`);
    const io = trackMarkers(async (handle, file, buffer, offset, length, position) => {
      if (failureKind === "marker close" && path.basename(path.dirname(file)) === id(1)) {
        await siblingReading.promise;
        return handle.read(buffer, offset, length, position);
      }
      siblingReading.resolve();
      await gate.promise;
      return handle.read(buffer, offset, length, position);
    }, file => {
      if (failureKind === "marker close" && path.basename(path.dirname(file)) === id(1)) {
        failureObserved.resolve(); throw failure; // Actual close completed first.
      }
    });
    const opendir = fs.promises.opendir.bind(fs.promises), held = await opendir(f.runRoot);
    let reads = 0;
    if (failureKind === "directory read") {
      const read = held.read.bind(held);
      vi.spyOn(held, "read").mockImplementation(async () => {
        if (++reads === 10) { failureObserved.resolve(); throw failure; }
        return read();
      });
    }
    vi.spyOn(fs.promises, "opendir").mockResolvedValueOnce(held).mockImplementation(opendir);
    let settled = false;
    const outcome = discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile).then(
      value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
    try {
      await failureObserved.promise;
      await siblingReading.promise;
      await Promise.resolve();
      expect(settled).toBe(false); // Failure cannot hand back with still-open sibling markers.
      expect(io.active.size).toBeGreaterThan(0);
    } finally { gate.resolve(); }
    expect(await outcome).toEqual({ error: failure });
    expect(io.active.size).toBe(0); expect(io.peak()).toBeLessThanOrEqual(32);
    expect(io.handles.every(handle => handle.fd === -1)).toBe(true);
    await expect(held.read()).rejects.toMatchObject({ code: "ERR_DIR_CLOSED" });
    vi.restoreAllMocks();
    expect((await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).size).toBe(9);
  });

  it("closes the real directory on I/O failure and retries rather than caching an incomplete view", async () => {
    const f = fixture(), directory = f.source(id(1));
    const opendir = fs.promises.opendir.bind(fs.promises), open = fs.promises.open.bind(fs.promises);
    const held = await opendir(f.runRoot);
    vi.spyOn(fs.promises, "opendir").mockResolvedValueOnce(held).mockImplementation(opendir);
    vi.spyOn(fs.promises, "open").mockImplementationOnce(async (file, flags, mode) => {
      const handle = await open(file, flags, mode), close = handle.close.bind(handle);
      vi.spyOn(handle, "close").mockImplementationOnce(async () => { await close(); throw new Error("marker close failed"); });
      return handle;
    }).mockImplementation(open);
    await expect(discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).rejects.toThrow("marker close failed");
    await expect(held.read()).rejects.toMatchObject({ code: "ERR_DIR_CLOSED" });
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
  });
});
