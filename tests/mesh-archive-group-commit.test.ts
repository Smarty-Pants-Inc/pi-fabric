import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive, archiveFileName } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#8305, option D (scope cut). The file name is historical: the off-lock two-phase
// group commit moved to a follow-up issue, and NOTHING here asserts an off-lock or shared sync.
// The strict v1 protocol stays under `.lock`: an archived publish's line is durable before its
// live append, and the publish resolves only after both. The cut: a warmed ordinary archived
// publish makes exactly ONE data barrier, the segment fdatasync (the base made 28). Unkeyed
// index, PENDING, HEAD and digest are plain advisory writes; keyed index fences stay durable.
// Only a new segment name or directory gets
// a namespace barrier. These tests pin observable file state through a durable-image model and
// sync spies, and SIGKILL a real child at each boundary a removed metadata sync used to guard.

const roots: string[] = [];
const children: ChildProcess[] = [];
const from: MeshIdentity = { id: "session:single-sync", name: "single-sync", kind: "main", sessionId: "single-sync" };
const linuxOnly = process.platform !== "linux"; // fd classification reads /proc/self/fd

const setup = (options: { maxEventLogBytes?: number; retainedEventLogBytes?: number } = {}) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-archive-single-sync-"));
  roots.push(base);
  const root = path.join(base, "mesh");
  const dir = path.join(base, "archive");
  fs.mkdirSync(root, { recursive: true });
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  const open = () => new MeshStore(root, 4_096, 500, { lockTimeoutMs: 15_000, staleLockMs: 100, ...options });
  const store = open();
  const lines = (target: string) => fs.existsSync(target) ? fs.readFileSync(target, "utf8").split("\n").filter(Boolean) : [];
  const live = (): MeshEvent[] => lines(path.join(root, "events.jsonl")).map(line => JSON.parse(line));
  const segment = (topic: string, day = today()) => path.join(dir, day, archiveFileName(topic));
  return { base, root, dir, store, open, lines, live, segment };
};
type Setup = ReturnType<typeof setup>;
const today = () => new Date().toISOString().slice(0, 10).replaceAll("-", "/");
const fdPath = (fd: number): string => { try { return fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return ""; } };
const idsOf = (text: string): string[] => text.split("\n").filter(Boolean).flatMap(line => {
  try { return [String(JSON.parse(line).id)]; } catch { return []; }
});
const segmentFiles = (dir: string): string[] => {
  const found: string[] = [];
  const walk = (directory: string) => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const target = path.join(directory, entry.name);
      if (entry.isDirectory() && /^\d+$/.test(entry.name)) walk(target);
      else if (entry.isFile() && entry.name.endsWith(".jsonl")) found.push(target);
    }
  };
  walk(dir);
  return found;
};

type SyncKind = "segment" | "namespace" | "archive-file" | "live" | "other";
interface Sync { kind: SyncKind; call: string; target: string; held: boolean }
/**
 * Every fsync/fdatasync (sync and callback forms) and every live-log write in this process, with
 * a power-cut model. A segment's durable image is its bytes as of its last completed data sync;
 * its NAME is durable once its directory was synced after it existed. Segments that exist when
 * the watcher starts count as fully durable (their publishes resolved). A live write may only
 * carry events whose line is in a durable image under a durable name (`violations`).
 */
const watchBarriers = (mesh: Setup, fail?: (sync: { kind: SyncKind; target: string }) => boolean) => {
  const archive = fs.realpathSync(mesh.dir);
  const liveFile = path.join(fs.realpathSync(mesh.root), "events.jsonl");
  const syncs: Sync[] = [];
  const liveWrites: Array<{ sequences: number[]; held: boolean; syncsBefore: number }> = [];
  const violations: string[] = [];
  const durable = new Map<string, string>();
  const named = new Set<string>();
  for (const file of segmentFiles(mesh.dir)) {
    const real = fs.realpathSync(file);
    durable.set(real, fs.readFileSync(file, "utf8"));
    named.add(real);
  }
  const durableIds = () => new Set([...durable].filter(([file]) => named.has(file)).flatMap(([, text]) => idsOf(text)));
  const held = () => fs.existsSync(path.join(mesh.root, ".lock"));
  const liveIds = () => idsOf(fs.existsSync(liveFile) ? fs.readFileSync(liveFile, "utf8") : "");
  const kindOf = (target: string): SyncKind => {
    if (target === liveFile) return "live";
    if (target !== archive && !target.startsWith(`${archive}/`)) return "other";
    if (target.endsWith(".jsonl")) return "segment";
    return fs.statSync(target, { throwIfNoEntry: false })?.isDirectory() ? "namespace" : "archive-file";
  };
  const onSync = (fd: number, call: string) => {
    const target = fdPath(fd);
    const kind = kindOf(target);
    syncs.push({ kind, call, target, held: held() });
    if (fail?.({ kind, target })) throw Object.assign(new Error("archive barrier EIO"), { code: "EIO" });
    const snapshot = kind === "segment" ? fs.readFileSync(target, "utf8") : undefined;
    const children = kind === "namespace" ? fs.readdirSync(target).filter(name => name.endsWith(".jsonl")).map(name => path.join(target, name)) : [];
    return () => {
      if (snapshot !== undefined) durable.set(target, snapshot);
      for (const child of children) named.add(child);
    };
  };
  const onLiveWrite = (chunk: unknown) => {
    const text = Buffer.isBuffer(chunk) ? chunk.toString("utf8") : typeof chunk === "string" ? chunk
      : ArrayBuffer.isView(chunk) ? Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength).toString("utf8") : "";
    const events = text.split("\n").filter(Boolean).flatMap(line => { try { return [JSON.parse(line) as MeshEvent]; } catch { return []; } });
    if (!events.length) return;
    const isHeld = held();
    liveWrites.push({ sequences: events.map(event => event.sequence), held: isHeld, syncsBefore: syncs.length });
    if (!isHeld) violations.push(`live write of ${events.map(event => event.sequence)} without .lock`);
    const known = durableIds();
    for (const event of events) if (!known.has(event.id)) violations.push(`live write of ${event.sequence} before its archive line and name were durable`);
  };
  const append = fs.appendFileSync.bind(fs);
  const write = fs.writeSync.bind(fs) as (...args: unknown[]) => number;
  const originals = {
    fsyncSync: fs.fsyncSync.bind(fs), fdatasyncSync: fs.fdatasyncSync.bind(fs),
    fsync: fs.fsync.bind(fs) as (fd: number, callback: (error: Error | null) => void) => void,
    fdatasync: fs.fdatasync.bind(fs) as (fd: number, callback: (error: Error | null) => void) => void,
  };
  const spies = [
    vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: unknown, ...rest: unknown[]) => {
      const target = typeof file === "number" ? fdPath(file) : typeof file === "string" ? path.resolve(file) : "";
      if (target === liveFile || target === path.resolve(mesh.root, "events.jsonl")) onLiveWrite(data);
      (append as (...args: unknown[]) => void)(file, data, ...rest);
    }) as typeof fs.appendFileSync),
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, data: unknown, ...rest: unknown[]) => {
      if (fd > 2 && fdPath(fd) === liveFile) onLiveWrite(data);
      return write(fd, data, ...rest);
    }) as typeof fs.writeSync),
    ...(["fsyncSync", "fdatasyncSync"] as const).map(name => vi.spyOn(fs, name).mockImplementation(fd => {
      const done = onSync(fd, name);
      originals[name](fd);
      done();
    })),
    ...(["fsync", "fdatasync"] as const).map(name => vi.spyOn(fs, name).mockImplementation(((fd: number, callback: (error: Error | null) => void) => {
      let done: () => void;
      try { done = onSync(fd, name); } catch (error) { process.nextTick(callback, error as Error); return; }
      originals[name](fd, error => { if (!error) done(); callback(error); });
    }) as never)),
  ];
  return {
    syncs, liveWrites, violations,
    /** The resolved publish's contract: its archive line is durable and it is in the live log. */
    expectResolved: (event: MeshEvent, label = "") => {
      expect(durableIds().has(event.id), `${label} resolved before its archive line was durable`).toBe(true);
      expect(liveIds(), `${label} resolved before its live append`).toContain(event.id);
    },
    reset: () => { syncs.length = 0; liveWrites.length = 0; },
    restore: () => spies.forEach(spy => spy.mockRestore()),
  };
};
const summary = (syncs: Sync[]) => syncs.map(sync => `${sync.kind}:${sync.call}:${sync.held ? "held" : "released"}`);

/** Every archived line across all days. */
const archivedEvents = (dir: string): MeshEvent[] => segmentFiles(dir)
  .flatMap(target => fs.readFileSync(target, "utf8").split("\n").slice(0, -1).filter(Boolean).map(line => JSON.parse(line) as MeshEvent))
  .sort((left, right) => left.sequence - right.sequence);
/** No live event without its archive line, no ID twice anywhere, all in sequence order. */
const expectConsistent = (mesh: Setup, store: MeshStore = mesh.store) => {
  const live = mesh.live();
  expect(new Set(live.map(event => event.id)).size).toBe(live.length);
  expect(live.map(event => event.sequence)).toEqual([...live.map(event => event.sequence)].sort((a, b) => a - b));
  const all = store.read({ after: 0, limit: 500 });
  expect(new Set(all.map(event => event.id)).size).toBe(all.length);
  expect(all.map(event => event.sequence)).toEqual([...all.map(event => event.sequence)].sort((a, b) => a - b));
  const archived = archivedEvents(mesh.dir);
  expect(new Set(archived.map(event => event.id)).size, "an archived ID appears twice").toBe(archived.length);
  // Each committed event is archived exactly once, and the archive holds nothing unpublished.
  expect(archived.map(event => event.id)).toEqual(all.map(event => event.id));
  for (const event of live) expect(archived.map(other => other.id), `live ${event.sequence} has no archive line`).toContain(event.id);
  return { live, all, archived };
};

type Metadata = "HEAD" | "PENDING" | "index" | "digest";
/** Removes the plain advisory metadata a power loss (or a lost page cache) may drop. */
const dropMetadata = (dir: string, which: Metadata[]) => {
  if (which.includes("HEAD")) fs.rmSync(path.join(dir, "HEAD.json"), { force: true });
  if (which.includes("PENDING")) fs.rmSync(path.join(dir, "PENDING.json"), { force: true });
  if (which.includes("index")) fs.rmSync(path.join(dir, "sequence-index"), { recursive: true, force: true });
  if (which.includes("digest")) for (const file of segmentFiles(dir)) {
    for (const name of fs.readdirSync(path.dirname(file))) if (name.startsWith(".digest-")) fs.rmSync(path.join(path.dirname(file), name), { force: true });
  }
};
/** Power loss: the unsynced live tail is gone (keep its first `keep` lines) and BOOT names another boot. */
const powerLoss = (mesh: Setup, keep: number) => {
  const live = mesh.lines(path.join(mesh.root, "events.jsonl")).slice(0, keep);
  fs.writeFileSync(path.join(mesh.root, "events.jsonl"), live.map(line => `${line}\n`).join(""));
  fs.writeFileSync(path.join(mesh.root, "sequence"), String(live.length ? JSON.parse(live.at(-1)!).sequence : 0));
  fs.writeFileSync(path.join(mesh.dir, "BOOT"), "an earlier boot");
};

const fixture = path.resolve("tests/fixtures/mesh-archive-group-crash.mjs");
type Phase = "after-pending" | "after-datasync" | "after-live";
interface Paused { pid: number; phase: Phase; call: string; target: string; lockHeldBySelf: boolean; pending: string | null; head: string | null; segmentLines: string[] }
/** A child publisher stopped at one explicit boundary; one stdout IPC receipt, blocked on stdin. */
const pausedPublisher = async (mesh: Setup, packet: Record<string, unknown> | Array<Record<string, unknown>>, phase: Phase, preload?: string) => {
  const child = spawn(process.execPath, [...(preload ? ["--import", preload] : []), fixture, mesh.root, mesh.dir, JSON.stringify(packet), phase], {
    cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"],
  });
  children.push(child);
  let stderr = "", buffered = "";
  const records: Array<Record<string, unknown>> = [];
  let notify: (() => void) | undefined;
  child.stderr!.on("data", chunk => { stderr += chunk; });
  child.stdout!.on("data", chunk => {
    buffered += chunk;
    for (let end = buffered.indexOf("\n"); end >= 0; end = buffered.indexOf("\n")) {
      records.push(JSON.parse(buffered.slice(0, end)));
      buffered = buffered.slice(end + 1);
      notify?.();
    }
  });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve =>
    child.once("close", (code, signal) => { notify?.(); resolve({ code, signal }); }));
  const record = (key: string) => records.find(item => key in item)?.[key];
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`publisher never reached ${phase}: ${stderr}`)), 20_000);
    notify = () => {
      if (record("paused")) { clearTimeout(timer); resolve(); }
      else if (child.exitCode !== null || child.signalCode !== null || record("done")) {
        clearTimeout(timer);
        reject(new Error(`publisher finished without pausing at ${phase}: ${JSON.stringify(record("done"))} ${stderr}`));
      }
    };
  });
  return {
    paused: record("paused") as Paused,
    closeInput: () => child.stdin!.end(),
    exitWithin: async (timeoutMs: number) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          closed,
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error(`paused publisher did not exit within ${timeoutMs}ms: ${stderr}`)), timeoutMs);
          }),
        ]);
      } finally { clearTimeout(timer); }
    },
    kill: async () => { child.kill("SIGKILL"); const result = await closed; expect(result.signal).toBe("SIGKILL"); },
    release: async () => {
      child.stdin!.write("r");
      const result = await closed;
      expect(result.code, stderr).toBe(0);
      return (record("done") as { result: MeshEvent | MeshEvent[] }).result;
    },
  };
};

afterEach(async () => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  await Promise.all(children.splice(0).map(child => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    return new Promise<void>(resolve => { child.once("close", () => resolve()); child.kill("SIGKILL"); });
  }));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("archived publish: one segment data barrier, strict v1 visibility (smarty-dev#8305 option D, #754)", () => {
  it.skipIf(linuxOnly)("warmed ordinary publish: exactly one segment fdatasync under .lock before the live append, and no other sync", async () => {
    const mesh = setup();
    // Warm: the segment, its day directories and the first index bucket exist.
    await mesh.store.publish({ topic: "ops.single", from, text: "warm 1" });
    await mesh.store.publish({ topic: "ops.single", from, text: "warm 2" });
    const watcher = watchBarriers(mesh);
    for (let index = 0; index < 5; index++) {
      watcher.reset();
      const event = await mesh.store.publish({ topic: "ops.single", from, text: `warm ordinary ${index}` });
      watcher.expectResolved(event, event.text);
      expect(summary(watcher.syncs), event.text).toEqual(["segment:fdatasyncSync:held"]);
      expect(watcher.syncs[0]!.target).toBe(fs.realpathSync(mesh.segment("ops.single")));
      // The data barrier precedes the live append, inside the same hold.
      expect(watcher.liveWrites).toEqual([{ sequences: [event.sequence], held: true, syncsBefore: 1 }]);
      // The plain metadata still carries its v1 meaning in this boot.
      expect(JSON.parse(fs.readFileSync(path.join(mesh.dir, "HEAD.json"), "utf8"))).toMatchObject({ sequence: event.sequence, id: event.id });
      expect(fs.existsSync(path.join(mesh.dir, "PENDING.json"))).toBe(false);
      expect(new MeshArchive(mesh.dir, mesh.root).lookup(event.sequence)).toEqual(event);
    }
    expect(watcher.violations).toEqual([]);
    watcher.restore();
    expectConsistent(mesh);
  });

  it.skipIf(linuxOnly)("a new segment name gets its namespace barriers before the live append; a new day syncs its directory chain", async () => {
    const mesh = setup();
    await mesh.store.publish({ topic: "ops.warm", from, text: "warm" });
    await mesh.store.publish({ topic: "ops.warm", from, text: "warm 2" });
    const watcher = watchBarriers(mesh);
    const event = await mesh.store.publish({ topic: "ops.fresh-topic", from, text: "new segment on a warm day" });
    watcher.expectResolved(event);
    const segmentSyncs = watcher.syncs.filter(sync => sync.kind === "segment");
    expect(segmentSyncs.map(sync => sync.target)).toEqual([fs.realpathSync(mesh.segment("ops.fresh-topic"))]);
    expect(segmentSyncs[0]!.call).toBe("fdatasyncSync");
    const day = fs.realpathSync(path.dirname(mesh.segment("ops.fresh-topic")));
    // Beyond the data barrier only directory (namespace) barriers, including the new name's day.
    expect(watcher.syncs.filter(sync => sync.kind !== "segment").every(sync => sync.kind === "namespace"), JSON.stringify(watcher.syncs)).toBe(true);
    expect(watcher.syncs.filter(sync => sync.kind === "namespace").map(sync => sync.target)).toContain(day);
    expect(watcher.syncs.every(sync => sync.held)).toBe(true);
    expect(watcher.liveWrites[0]!.syncsBefore).toBe(watcher.syncs.length);
    expect(watcher.violations).toEqual([]);
    watcher.restore();

    vi.useFakeTimers({ now: Date.parse("2026-09-27T12:00:00.000Z"), toFake: ["Date"] });
    const fresh = setup();
    const freshWatcher = watchBarriers(fresh);
    const first = await fresh.store.publish({ topic: "ops.day", from, text: "first of a new archive day" });
    freshWatcher.expectResolved(first);
    const chain = ["2026/09/27", "2026/09", "2026", ""].map(part => fs.realpathSync(path.join(fresh.dir, part)));
    expect(freshWatcher.syncs.filter(sync => sync.kind === "namespace").map(sync => sync.target)).toEqual(expect.arrayContaining(chain));
    // Only the once-per-archive MESH.json and once-per-boot BOOT are durable files; never the
    // per-publish advisory index, digest, HEAD or PENDING.
    expect(freshWatcher.syncs.filter(sync => sync.kind === "archive-file").map(sync => path.basename(sync.target).split(".")[0]).sort()).toEqual(["BOOT", "MESH"]);
    expect(freshWatcher.violations).toEqual([]);
    freshWatcher.restore();
    expectConsistent(fresh);
  });

  it.skipIf(linuxOnly)("concurrent stores: each publish its own single data barrier, sequences ordered, no ID twice", async () => {
    const mesh = setup();
    const stores = [mesh.store, mesh.open(), mesh.open()];
    await mesh.store.publish({ topic: "ops.a", from, text: "warm a" });
    await mesh.store.publish({ topic: "ops.b", from, text: "warm b" });
    const watcher = watchBarriers(mesh);
    const published = await Promise.all(Array.from({ length: 12 }, (_, index) =>
      stores[index % 3]!.publish({ topic: index % 2 ? "ops.a" : "ops.b", from, text: `c${index}` })
        .then(event => { watcher.expectResolved(event, event.text); return event; })));
    // No group sync: twelve archived publishes, twelve segment data barriers, nothing else.
    expect(summary(watcher.syncs)).toEqual(Array(12).fill("segment:fdatasyncSync:held"));
    expect(watcher.liveWrites.map(write => write.sequences.length)).toEqual(Array(12).fill(1));
    expect(published.map(event => event.sequence).sort((a, b) => a - b)).toEqual(Array.from({ length: 12 }, (_, index) => index + 3));
    expect(watcher.violations).toEqual([]);
    watcher.restore();
    const { archived } = expectConsistent(mesh);
    for (const topic of ["ops.a", "ops.b"]) {
      const sequences = mesh.lines(mesh.segment(topic)).map(line => JSON.parse(line).sequence as number);
      expect(sequences).toEqual([...sequences].sort((a, b) => a - b));
    }
    expect(archived).toHaveLength(14);
  });

  it.skipIf(linuxOnly)("keyed and batch publishes keep strict visibility; keyed fences are extra and reported separately", async () => {
    const mesh = setup();
    await mesh.store.publish({ topic: "ops.k", from, text: "warm" });
    await mesh.store.publish({ topic: "ops.k", from, text: "warm 2" });
    const watcher = watchBarriers(mesh);
    const keyed = await mesh.store.publish({ topic: "ops.k", from, text: "keyed", dedupeKey: "single-sync-keyed" });
    watcher.expectResolved(keyed, "keyed");
    const keyedSyncs = summary(watcher.syncs);
    // The ordinary target (one) does not apply: the intent/receipt fences stay durable.
    expect(keyedSyncs.filter(sync => sync.startsWith("segment:"))).toEqual(["segment:fdatasyncSync:held"]);
    expect(watcher.liveWrites).toHaveLength(1);
    console.info(`[8305] keyed warmed publish syncs (${keyedSyncs.length}): ${keyedSyncs.join(", ")}`);
    watcher.reset();
    const batch = await mesh.store.publishBatch([0, 1, 2].map(index => ({ topic: "ops.k", from, text: `batch ${index}` })));
    for (const event of batch) watcher.expectResolved(event, event.text);
    const batchSyncs = summary(watcher.syncs);
    // The 50 ms batch budget may return a shorter prefix on a busy host. Every returned event
    // still has one data barrier under the hold; the live barrier is after release.
    expect(batch.length).toBeGreaterThan(0);
    expect(batch.map(event => event.text)).toEqual([0, 1, 2].slice(0, batch.length).map(index => `batch ${index}`));
    expect(batchSyncs.filter(sync => sync.startsWith("segment:"))).toEqual(Array(batch.length).fill("segment:fdatasyncSync:held"));
    expect(watcher.syncs.filter(sync => sync.kind === "archive-file" || sync.kind === "namespace")).toEqual([]);
    console.info(`[8305] warmed batch returned ${batch.length}/3 events, syncs (${batchSyncs.length}): ${batchSyncs.join(", ")}`);
    expect(watcher.violations).toEqual([]);
    watcher.restore();
    expect(await mesh.open().publish({ topic: "ops.k", from, text: "keyed", dedupeKey: "single-sync-keyed" })).toEqual(keyed);
    expectConsistent(mesh);
  });

  it.skipIf(linuxOnly)("a failed data barrier rejects, cuts the line, never goes live; the next sequence leaves a gap", async () => {
    const mesh = setup();
    await mesh.store.publish({ topic: "ops.fail", from, text: "before" });
    let failures = 1;
    const watcher = watchBarriers(mesh, sync => sync.kind === "segment" && failures-- > 0);
    await expect(mesh.store.publish({ topic: "ops.fail", from, text: "unsynced" })).rejects.toThrow("EIO");
    expect(watcher.liveWrites).toEqual([]);
    expect(mesh.lines(mesh.segment("ops.fail")).map(line => JSON.parse(line).text)).toEqual(["before"]);
    expect(fs.existsSync(path.join(mesh.dir, "PENDING.json"))).toBe(false);
    // The burned reservation is positively absent, not merely unknown.
    expect(new MeshArchive(mesh.dir, mesh.root).lookup(2)).toBeUndefined();
    const next = await mesh.open().publish({ topic: "ops.fail", from, text: "after" });
    watcher.expectResolved(next);
    expect(next.sequence).toBe(3);
    expect(watcher.violations).toEqual([]);
    watcher.restore();
    expect(expectConsistent(mesh).all.map(event => [event.sequence, event.text])).toEqual([[1, "before"], [3, "after"]]);
  });

  it.skipIf(linuxOnly)("a failed namespace barrier for a new segment also rejects with nothing live", async () => {
    const mesh = setup();
    await mesh.store.publish({ topic: "ops.warm", from, text: "warm" });
    let failures = 1;
    const watcher = watchBarriers(mesh, sync => sync.kind === "namespace" && failures-- > 0);
    await expect(mesh.store.publish({ topic: "ops.new-name", from, text: "name never durable" })).rejects.toThrow("EIO");
    expect(watcher.liveWrites).toEqual([]);
    expect(mesh.lines(mesh.segment("ops.new-name"))).toEqual([]);
    const next = await mesh.store.publish({ topic: "ops.new-name", from, text: "retried" });
    expect(next.sequence).toBe(3);
    expect(watcher.violations).toEqual([]);
    watcher.restore();
    expect(expectConsistent(mesh).all.map(event => event.text)).toEqual(["warm", "retried"]);
  });

  it("mixed writers: the begin/commit API and the publish path leave the same files with the same meanings", async () => {
    const mesh = setup();
    const archive = new MeshArchive(mesh.dir, mesh.root);
    const first = await mesh.store.publish({ topic: "ops.mixed", from, text: "new path" });
    // An old v1 writer: begin (archive), live append, commit, under the same lock discipline.
    const old: MeshEvent = { ...first, id: "00000000-0000-4000-8000-000000000802", sequence: 2, text: "old api", createdAt: Date.now() };
    const line = JSON.stringify(old);
    fs.writeFileSync(path.join(mesh.root, "sequence"), "2");
    const pending = archive.begin({ event: old, line });
    // v1 stores POSIX relative addresses on every OS; native joins are only for disk access.
    const relative = path.posix.join(today(), archiveFileName("ops.mixed"));
    expect(pending).toMatchObject({ sequence: 2, id: old.id, file: relative, indexed: true });
    expect(archive.pending()).toMatchObject({ sequence: 2, id: old.id });
    fs.appendFileSync(path.join(mesh.root, "events.jsonl"), `${line}\n`);
    archive.commit(pending);
    const third = await mesh.open().publish({ topic: "ops.mixed", from, text: "new path again" });
    expect(third.sequence).toBe(3);
    const index = (sequence: number) => JSON.parse(fs.readFileSync(path.join(mesh.dir, "sequence-index", "0", `${sequence}.json`), "utf8"));
    const shape = (entry: Record<string, unknown>) => Object.keys(entry).sort();
    expect(shape(index(1))).toEqual(shape(index(2)));
    expect(shape(index(3))).toEqual(shape(index(2)));
    for (const event of [first, old, third]) {
      expect(index(event.sequence).file).toBe(relative);
      expect(archive.lookupEntry(event.sequence)).toMatchObject({ event, committed: true });
    }
    expect(archive.head()).toEqual({ sequence: 3, id: third.id, file: pending.file });
    expect(archive.pending()).toBeUndefined();
    expect(archive.readAfter(0, 3, () => true, 10).map(event => event.id)).toEqual([first.id, old.id, third.id]);
    expectConsistent(mesh);
  });

  it("dropped digests are rebuilt: a closed day still seals the exact bytes", async () => {
    const mesh = setup();
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:58.000Z"), toFake: ["Date"] });
    await Promise.all([0, 1, 2, 3].map(index =>
      (index % 2 ? mesh.store : mesh.open()).publish({ topic: index % 2 ? "ops.a" : "ops.b", from, text: `day A ${index}` })));
    dropMetadata(mesh.dir, ["digest", "HEAD"]);
    vi.setSystemTime(Date.parse("2026-09-28T00:00:01.000Z"));
    await mesh.store.publish({ topic: "ops.a", from, text: "day B" });
    const seal = JSON.parse(fs.readFileSync(path.join(mesh.dir, "2026/09/27/SEAL.json"), "utf8"));
    for (const topic of ["ops.a", "ops.b"]) {
      const bytes = fs.readFileSync(mesh.segment(topic, "2026/09/27"));
      expect(seal.files[archiveFileName(topic)]).toMatchObject({ lines: 2, sha256: createHash("sha256").update(bytes).digest("hex") });
    }
    expect(expectConsistent(mesh).all).toHaveLength(5);
  });

  describe("plain metadata lost after acknowledged publishes", () => {
    const acknowledged = async () => {
      const mesh = setup();
      const events = [];
      for (const [index, topic] of ["ops.a", "ops.b", "ops.a", "ops.b"].entries()) {
        events.push(await mesh.store.publish({ topic, from, text: `acked ${index}` }));
      }
      return { mesh, events };
    };
    const all: Metadata[] = ["HEAD", "PENDING", "index", "digest"];

    it.each([[["HEAD"]], [["index"]], [["digest"]], [["HEAD", "index", "digest"]]] as Array<[Metadata[]]>)("same boot, dropped %j: the next publishes add nothing twice", async which => {
      const { mesh, events } = await acknowledged();
      dropMetadata(mesh.dir, which);
      const restarted = mesh.open();
      await restarted.publish({ topic: "ops.unrelated", from, text: "unrelated" });
      await restarted.publish({ topic: "ops.a", from, text: "same segment" });
      const { all: published } = expectConsistent(mesh, restarted);
      expect(published.map(event => event.text)).toEqual([...events.map(event => event.text), "unrelated", "same segment"]);
      if (which.includes("index")) {
        // A lost advisory index is UNKNOWN for that sequence, never evidence of absence.
        expect(() => new MeshArchive(mesh.dir, mesh.root).lookup(events[0]!.sequence)).toThrow("unavailable");
      }
    });

    it.each([[["HEAD"]], [["PENDING"]], [["index"]], [["digest"]], [all]] as Array<[Metadata[]]>)("BOOT mismatch, live tail lost, dropped %j: acknowledged events return once", async which => {
      const { mesh, events } = await acknowledged();
      powerLoss(mesh, 1);
      dropMetadata(mesh.dir, which);
      const rebooted = mesh.open();
      await rebooted.publish({ topic: "ops.unrelated", from, text: "after reboot" });
      const { all: published, live } = expectConsistent(mesh, rebooted);
      expect(published.map(event => event.id)).toEqual([...events.map(event => event.id), published.at(-1)!.id]);
      expect(live.map(event => event.sequence)).toEqual([1, 2, 3, 4, 5]);
    });
  });

  describe.skipIf(linuxOnly)("paused publisher lifetime", () => {
    it("open but silent stdin exits within the 30-second bound and the abandoned lock is reclaimable", async () => {
      const mesh = setup();
      await mesh.store.publish({ topic: "ops.crash", from, text: "before" });
      const child = await pausedPublisher(mesh, { topic: "ops.crash", from, text: "abandoned" }, "after-datasync");
      expect(child.paused.lockHeldBySelf).toBe(true);
      const started = performance.now();
      // Keep the pipe open, but never write the release byte: exercise real EAGAIN reads.
      expect(await child.exitWithin(32_000)).toEqual({ code: 4, signal: null });
      expect(performance.now() - started).toBeGreaterThanOrEqual(29_000);
      const restarted = mesh.open();
      await restarted.publish({ topic: "ops.crash", from, text: "after timeout" });
      expect(expectConsistent(mesh, restarted).all.map(event => event.text)).toEqual(["before", "after timeout"]);
    }, 55_000);

    it("stdin EOF exits immediately instead of retaining the publish lock", async () => {
      const mesh = setup();
      const child = await pausedPublisher(mesh, { topic: "ops.crash", from, text: "abandoned" }, "after-datasync");
      expect(child.paused.lockHeldBySelf).toBe(true);
      child.closeInput();
      expect(await child.exitWithin(2_000)).toEqual({ code: 3, signal: null });
    });

    it.each(["reparented", "changed parent", "missing parent"] as const)("%s exits immediately even when stdin keeps returning EAGAIN", async loss => {
      const mesh = setup();
      const preload = path.join(mesh.base, "parent-loss.mjs");
      // Simulate parent loss only after the first stdin read so the real child reaches its
      // locked pause first. No production seam: these probes patch only this child's APIs.
      fs.writeFileSync(preload, `
        import fs from "node:fs";
        const parentPid = process.ppid;
        let lost = false;
        const read = fs.readSync.bind(fs);
        fs.readSync = (fd, ...args) => {
          if (fd !== 0) return read(fd, ...args);
          lost = true;
          throw Object.assign(new Error("silent stdin"), { code: "EAGAIN" });
        };
        ${loss === "missing parent" ? `
          const probe = process.kill.bind(process);
          process.kill = (pid, signal) => {
            if (lost && pid === parentPid && signal === 0) throw Object.assign(new Error("parent gone"), { code: "ESRCH" });
            return probe(pid, signal);
          };
        ` : `
          Object.defineProperty(process, "ppid", { get: () => lost ? ${loss === "reparented" ? "1" : "parentPid + 1"} : parentPid });
        `}
      `);
      const child = await pausedPublisher(mesh, { topic: "ops.crash", from, text: "abandoned" }, "after-datasync", preload);
      expect(child.paused.lockHeldBySelf).toBe(true);
      expect(await child.exitWithin(2_000)).toEqual({ code: 5, signal: null });
    });
  });

  describe.skipIf(linuxOnly)("SIGKILL at a removed metadata-sync boundary", () => {
    const expectedAfterCrash = (phase: Phase) => phase === "after-live" ? ["before", "killed", "unrelated"] : ["before", "unrelated"];

    it.each(["after-pending", "after-datasync", "after-live"] as const)("%s: nothing visible early; same-boot restart publishes once", async phase => {
      const mesh = setup();
      await mesh.store.publish({ topic: "ops.crash", from, text: "before" });
      const child = await pausedPublisher(mesh, { topic: "ops.crash", from, text: "killed" }, phase);
      expect(child.paused.lockHeldBySelf, "strict v1: the boundary is inside the .lock hold").toBe(true);
      expect(child.paused.pending, "PENDING precedes the archive line").not.toBeNull();
      if (phase === "after-datasync") expect(child.paused.segmentLines.map(line => JSON.parse(line).text)).toEqual(["before", "killed"]);
      // Lock-free readers: nothing of the killed publish before its live append.
      if (phase !== "after-live") {
        expect(mesh.live().map(event => event.text)).toEqual(["before"]);
        expect(mesh.open().read({ after: 0, limit: 10 }).map(event => event.text)).toEqual(["before"]);
      }
      await child.kill();
      const restarted = mesh.open();
      await restarted.publish({ topic: "ops.unrelated", from, text: "unrelated" });
      const { all } = expectConsistent(mesh, restarted);
      expect(all.map(event => event.text)).toEqual(expectedAfterCrash(phase));
      await restarted.publish({ topic: "ops.crash", from, text: "after" });
      expect(expectConsistent(mesh, restarted).all.map(event => event.text)).toEqual([...expectedAfterCrash(phase), "after"]);
    });

    it.each(["after-pending", "after-datasync", "after-live"] as const)("%s keyed: an unrelated publish, then same-key retries publish the key once", async phase => {
      const mesh = setup();
      await mesh.store.publish({ topic: "ops.crash", from, text: "before" });
      const packet = { topic: "ops.crash", from, text: "keyed", dedupeKey: `crash-${phase}` };
      const child = await pausedPublisher(mesh, packet, phase);
      const killedLine = phase === "after-pending" ? undefined : child.paused.segmentLines.at(-1) ?? mesh.lines(mesh.segment("ops.crash")).at(-1);
      await child.kill();
      const restarted = mesh.open();
      await restarted.publish({ topic: "ops.unrelated", from, text: "unrelated" });
      const retried = await restarted.publish(packet);
      if (phase === "after-live") expect(retried.id).toBe(JSON.parse(killedLine!).id);
      expect(await mesh.open().publish(packet)).toEqual(retried);
      const { all } = expectConsistent(mesh, restarted);
      expect(all.filter(event => event.dedupeKey === packet.dedupeKey).map(event => event.id)).toEqual([retried.id]);
    });

    it.each([
      ["after-pending", ["PENDING"]], ["after-pending", ["HEAD", "index", "digest"]],
      ["after-datasync", ["PENDING"]], ["after-datasync", ["HEAD", "PENDING", "index", "digest"]],
      ["after-live", ["HEAD"]], ["after-live", ["HEAD", "PENDING", "index", "digest"]],
    ] as Array<[Phase, Metadata[]]>)("%s then power loss, dropped %j: no live event without its archive line, no ID twice", async (phase, which) => {
      const mesh = setup();
      await mesh.store.publish({ topic: "ops.crash", from, text: "before" });
      const child = await pausedPublisher(mesh, { topic: "ops.crash", from, text: "killed" }, phase);
      await child.kill();
      // The never-synced archive bytes are gone; a synced line survives the power loss.
      if (phase === "after-pending") {
        const [first] = mesh.lines(mesh.segment("ops.crash"));
        fs.truncateSync(mesh.segment("ops.crash"), Buffer.byteLength(`${first}\n`));
      }
      powerLoss(mesh, 1);
      dropMetadata(mesh.dir, which);
      const rebooted = mesh.open();
      await rebooted.publish({ topic: "ops.unrelated", from, text: "after reboot" });
      const { all } = expectConsistent(mesh, rebooted);
      // A synced, never-acknowledged line may come back (at least once across a power loss), exactly once.
      expect(all.map(event => event.text)).toEqual(phase === "after-pending" ? ["before", "after reboot"] : ["before", "killed", "after reboot"]);
    });

    it("after-live keyed, then power loss with index and HEAD dropped: the same-key retry returns the synced event once", async () => {
      const mesh = setup();
      await mesh.store.publish({ topic: "ops.crash", from, text: "before" });
      const packet = { topic: "ops.crash", from, text: "keyed", dedupeKey: "crash-power-keyed" };
      const child = await pausedPublisher(mesh, packet, "after-live");
      const killed = archivedEvents(mesh.dir).find(event => event.dedupeKey === packet.dedupeKey)!;
      await child.kill();
      powerLoss(mesh, 1);
      dropMetadata(mesh.dir, ["HEAD", "index"]);
      const rebooted = mesh.open();
      const retried = await rebooted.publish(packet);
      expect(retried.id).toBe(killed.id);
      expect(await mesh.open().publish(packet)).toEqual(retried);
      const { all } = expectConsistent(mesh, rebooted);
      expect(all.map(event => event.text)).toEqual(["before", "keyed"]);
    });

    it("a paused holder blocks only behind .lock: its release publishes, then a waiting publish follows in order", async () => {
      const mesh = setup();
      await mesh.store.publish({ topic: "ops.crash", from, text: "before" });
      const child = await pausedPublisher(mesh, { topic: "ops.crash", from, text: "held" }, "after-datasync");
      const waiting = mesh.open().publish({ topic: "ops.crash", from, text: "waiting" });
      const finished = await child.release() as MeshEvent;
      const event = await waiting;
      expect(event.sequence).toBe(finished.sequence + 1);
      expect(expectConsistent(mesh).all.map(item => item.text)).toEqual(["before", "held", "waiting"]);
    });
  });

  it("compaction: concurrent publishes past the live log's limit stay archived exactly once, in order", async () => {
    const mesh = setup({ maxEventLogBytes: 6_000, retainedEventLogBytes: 1_500 });
    for (let round = 0; round < 3; round++) {
      await Promise.all(Array.from({ length: 10 }, (_, index) =>
        (index % 2 ? mesh.store : mesh.open()).publish({ topic: index % 3 ? "team.a" : "team.b", from, text: `r${round} e${index} ${"p".repeat(200)}` })));
    }
    expect(mesh.store.oldestSequence()).toBeGreaterThan(1);
    expect(expectConsistent(mesh).all.map(event => event.sequence)).toEqual(Array.from({ length: 30 }, (_, index) => index + 1));
  });
});
