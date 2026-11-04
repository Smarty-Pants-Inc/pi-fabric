import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { performance } from "node:perf_hooks";
import { afterEach, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide, type BridgedIds } from "../src/mesh/bridge.js";
import { MESH_ARCHIVE_CONFIG, archiveFileName } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

// Data safety: finite on-disk histories exercise the public recovery boundary,
// including races that are impractical to reach in the large startup flow.
const roots: string[] = [];
const from = { id: "fixture-writer", name: "writer", kind: "main" as const };
const day = new Date().toISOString().slice(0, 10).replaceAll("-", "/");
const nextDay = new Date(Date.now() + 86_400_000).toISOString().slice(0, 10).replaceAll("-", "/");
const line = (event: MeshEvent) => JSON.stringify(event) + "\n";
const event = (sequence: number, id = `original-${sequence}`, topic = "fixture.history"): MeshEvent => ({
  id: `event-${sequence}`, sequence, topic, kind: "message", from, createdAt: 1,
  data: { bridge: { from: "spoke", id } },
});
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-recovery-reader-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  syncBuiltinESMExports();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (archived: MeshEvent[], live: MeshEvent[], archiveEnabled = true, maxEventBytes = 64 * 1024) => {
  const root = scratch(), archive = path.join(root, "history");
  const appendArchive = (rows: MeshEvent[], date = day) => {
    const directory = path.join(archive, date);
    fs.mkdirSync(directory, { recursive: true });
    const topics = new Map<string, string[]>();
    for (const row of rows) {
      const lines = topics.get(row.topic) ?? [];
      lines.push(line(row));
      topics.set(row.topic, lines);
    }
    for (const [topic, lines] of topics) fs.appendFileSync(path.join(directory, archiveFileName(topic)), lines.join(""));
  };
  const appendLive = (rows: MeshEvent[]) => {
    fs.appendFileSync(path.join(root, "events.jsonl"), rows.map(line).join(""));
    if (rows.length) fs.writeFileSync(path.join(root, "sequence"), String(rows.at(-1)!.sequence));
  };
  if (archiveEnabled) {
    appendArchive(archived);
    // Deliberately legacy: no MESH.json, seals, or per-sequence indexes.
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  }
  appendLive(live);
  const store = new MeshStore(root, maxEventBytes, 500);
  return { root, archive, store, side: new StoreBridgeSide(store, "spoke"), appendArchive, appendLive,
    file: (topic: string) => path.join(archive, day, archiveFileName(topic)) };
};

const heartbeatDuring = async <T>(work: () => Promise<T>) => {
  let finished = false, turns = 0, previous = performance.now(), maxGapMs = 0;
  // Recovery progress is driven by reads; this self-rescheduling check-phase
  // observer is only an occupancy measurement, not a polling safety net.
  const heartbeat = () => {
    const now = performance.now();
    maxGapMs = Math.max(maxGapMs, now - previous);
    previous = now;
    turns++;
    if (!finished) setImmediate(heartbeat);
  };
  setImmediate(heartbeat);
  try {
    const value = await work();
    return { value, turns, get maxGapMs() { return maxGapMs; } };
  } finally {
    maxGapMs = Math.max(maxGapMs, performance.now() - previous);
    finished = true;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
};

it("fills an exhausted archive from live without resurrecting an exact-ID aborted tail", async () => {
  const mismatchedAbort = event(20), abortedTail = event(30);
  const native: MeshEvent = { ...event(40), data: undefined };
  const f = fixture([event(10), mismatchedAbort, abortedTail, event(1_000, "archive-only", "fixture.future")],
    [abortedTail, native, event(60), event(100)]);
  fs.writeFileSync(path.join(f.archive, day, "ABORTED.json"), JSON.stringify({
    [mismatchedAbort.sequence]: "a-different-event-id", [abortedTail.sequence]: abortedTail.id,
  }));

  expect(await f.side.bridgedIds(0)).toEqual({
    ids: ["original-10", "original-20", "original-60", "original-100"], through: 100, done: true,
  });
});

it("fills 10k visible positions across an older horizon after sparse history grows", async () => {
  const initial = Array.from({ length: 30_000 }, (_, index) => {
    const row = event((index + 1) * 2 + (index >= 14_000 ? 17 : 0), `initial-${index}`, `fixture.topic.${index % 3}`);
    return index % 7 === 0 ? { ...row, data: undefined } : row;
  });
  const oldHead = initial.at(-1)!;
  const f = fixture(initial, [oldHead]);
  // The old stage's final position is hidden, so its third page needs one
  // position from the new stage; sequence-number holes are not page entries.
  fs.writeFileSync(path.join(f.archive, day, "ABORTED.json"), JSON.stringify({ [oldHead.sequence]: oldHead.id }));
  const first = await f.side.bridgedIds(0);
  expect(first).toEqual({ ids: Array.from({ length: 10_000 }, (_, index) => index)
    .filter(index => index % 7 !== 0).map(index => `initial-${index}`),
    through: initial[9_999]!.sequence, done: false });

  const later = Array.from({ length: 10_007 }, (_, index) =>
    event(oldHead.sequence + 3 * (index + 1), `later-${index}`, index % 2 ? "fixture.new-topic" : "fixture.topic.0"));
  f.appendArchive(later.slice(0, 5_000));
  f.appendArchive(later.slice(5_000), nextDay);
  f.appendLive(later);
  const visible = [...initial.slice(0, -1), ...later];
  const expectedByPosition = [...Array.from({ length: 29_999 }, (_, index) => index % 7 === 0 ? undefined : `initial-${index}`),
    ...Array.from({ length: later.length }, (_, index) => `later-${index}`)];
  const matching = (ids: Array<string | undefined>) => ids.filter((id): id is string => id !== undefined);
  const expected = matching(expectedByPosition);
  const ids = [...first.ids];
  let cursor = first.through;
  // A finite bound also catches a nonadvancing or prematurely completed page.
  for (let offset = 10_000; offset < visible.length; offset += 10_000) {
    const page = await f.side.bridgedIds(cursor);
    const end = Math.min(offset + 10_000, visible.length);
    expect(page.ids).toEqual(matching(expectedByPosition.slice(offset, end)));
    expect(page.through).toBe(visible[end - 1]!.sequence);
    expect(page.done).toBe(end === visible.length);
    ids.push(...page.ids);
    cursor = page.through;
  }
  expect(ids).toEqual(expected);
  expect(cursor).toBe(later.at(-1)!.sequence);
});

it("restarts mismatched continuations, serializes overlapping calls, and discards a slot above a lower live horizon", async () => {
  const rows = Array.from({ length: 30_000 }, (_, index) => event(index + 1));
  const f = fixture(rows, [rows.at(-1)!]);
  const first = await f.side.bridgedIds(0);
  expect(first).toEqual({ ids: Array.from({ length: 10_000 }, (_, index) => `original-${index + 1}`),
    through: 10_000, done: false });

  // The first call must discard the retained 10k cursor and honor its explicit
  // after. The overlapping call's known after is the first call's exact through.
  const [restarted, serialized] = await Promise.all([
    f.side.bridgedIds(1_000), f.side.bridgedIds(11_000),
  ]);
  expect(restarted).toEqual({ ids: Array.from({ length: 10_000 }, (_, index) => `original-${index + 1_001}`),
    through: 11_000, done: false });
  expect(serialized).toEqual({ ids: Array.from({ length: 10_000 }, (_, index) => `original-${index + 11_001}`),
    through: 21_000, done: false });

  // Replace only our live anchor, leaving the 30k archive and reservation file
  // intact. The retained slot's H=30000 is now stale, but after=21000 is below h.
  const replacement = path.join(f.root, "events.jsonl.lower-head");
  fs.writeFileSync(replacement, line(rows[24_999]!));
  fs.renameSync(replacement, path.join(f.root, "events.jsonl"));
  expect((await f.side.latestCursor()).through).toBe(25_000);
  expect(await f.side.bridgedIds(serialized.through)).toEqual({
    ids: Array.from({ length: 4_000 }, (_, index) => `original-${index + 21_001}`), through: 25_000, done: true,
  });
});

it("re-reads replaced above-horizon bytes and a completed torn UTF-8 tail at the next horizon", async () => {
  const prefix = Array.from({ length: 10_001 }, (_, index) => event(index + 1));
  const anchor = event(20_000);
  const pending = event(30_000, "obsolete-projection", "fixture.pending");
  const completed = { ...event(30_001, "completed-🛰", "fixture.torn"), text: "split UTF-8 🛰" };
  const f = fixture([...prefix, anchor, pending], [anchor]);
  const tornFile = f.file(completed.topic);
  const bytes = Buffer.from(JSON.stringify(completed) + "\r\n");
  const split = bytes.indexOf(Buffer.from("🛰")) + 2;
  expect(split).toBeGreaterThan(2);
  fs.writeFileSync(tornFile, bytes.subarray(0, split));
  const first = await f.side.bridgedIds(0);
  expect(first.through).toBe(10_000);
  expect(first.done).toBe(false);
  expect(first.ids).toEqual(Array.from({ length: 10_000 }, (_, index) => `original-${index + 1}`));

  const replacement = event(30_000, "replacement", pending.topic);
  // Only uncommitted suffix bytes are cut back, on this test's own archive.
  fs.truncateSync(f.file(pending.topic), 0);
  fs.appendFileSync(f.file(pending.topic), line(replacement));
  fs.appendFileSync(tornFile, bytes.subarray(split));
  f.appendLive([replacement, completed]);
  expect(await f.side.bridgedIds(first.through)).toEqual({
    ids: ["original-10001", "original-20000", "replacement", "completed-🛰"], through: 30_001, done: true,
  });
});

// Observe native data access without replacing any product read result. The
// callback gets a real event-loop turn after the first data read, not a timer
// guessed to land somewhere in a page. Both the baseline and async reader use
// real bytes; built-in named imports see the same observer.
const onArchiveRead = (archive: string, callback: () => void) => {
  const isData = (file: unknown) => typeof file === "string" &&
    file.startsWith(archive + path.sep) && file.endsWith(".jsonl");
  let queued = false;
  const observed = () => {
    if (queued) return;
    queued = true;
    setImmediate(() => setImmediate(callback));
  };
  const nativeRead = fs.readFileSync, nativeOpen = fs.promises.open;
  fs.readFileSync = ((...args: Parameters<typeof fs.readFileSync>) => {
    const result = nativeRead(...args);
    if (isData(args[0])) observed();
    return result;
  }) as typeof fs.readFileSync;
  fs.promises.open = async (...args) => {
    const handle = await nativeOpen(...args);
    if (isData(args[0])) {
      const read = handle.read;
      handle.read = (async (...readArgs: Parameters<typeof read>) => {
        const result = await Reflect.apply(read, handle, readArgs) as Awaited<ReturnType<typeof read>>;
        if (result.bytesRead > 0) observed();
        return result;
      }) as typeof handle.read;
    }
    return handle;
  };
  syncBuiltinESMExports();
  return () => {
    fs.readFileSync = nativeRead;
    fs.promises.open = nativeOpen;
    syncBuiltinESMExports();
  };
};

it("rejects a page fenced during real archive work without readiness or saved mark movement", async () => {
  const rows = Array.from({ length: 25_000 }, (_, index) => event(index + 1));
  const f = fixture(rows, [rows.at(-1)!]);
  const remote = new StoreBridgeSide(new MeshStore(scratch(), 64 * 1024, 500), "hub");
  const cursorPath = path.join(f.root, "cursor.json");
  const saved = JSON.stringify({ format: 1, local: "hub", remote: "spoke",
    toLocal: { after: 0, mark: 0 }, toRemote: { after: 25_000, mark: 0 } });
  fs.writeFileSync(cursorPath, saved);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "spoke", local: f.side, remote, cursorPath });
  let page: Promise<BridgedIds> | undefined;
  const read = f.side.bridgedIds.bind(f.side);
  vi.spyOn(f.side, "bridgedIds").mockImplementation(after => {
    const result = read(after);
    page ??= result;
    void result.catch(() => undefined);
    return result;
  });
  let fence: Promise<void> | undefined;
  const restore = onArchiveRead(f.archive, () => { fence = f.side.fence(); });
  let startError: unknown;
  try {
    await bridge.start().catch(error => { startError = error; });
    // Drain the event-source callback even when the synchronous baseline has
    // already (incorrectly) completed its whole page before it can run.
    await new Promise<void>(resolve => setImmediate(() => setImmediate(resolve)));
    await fence;
    expect(fence).toBeDefined();
    expect.soft(startError).toBeInstanceOf(Error);
    expect(page).toBeDefined();
    await expect.soft(page!).rejects.toThrow(/fenced|cancelled/i);
    expect.soft(fs.readFileSync(cursorPath, "utf8")).toBe(saved);
    await expect.soft(bridge.deliverRelay("another-spoke", {
      topic: "fabric.control.command", kind: "command", from, to: "session:recipient",
      data: { bridge: { from: "another-spoke", id: "not-ready" } },
    })).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
  } finally {
    restore();
    await bridge.stop();
  }
});

it("closes an archive handle acquired during fencing and permits fresh-side recovery", async () => {
  const f = fixture([event(1), event(2)], [event(3)]);
  const side = new StoreBridgeSide(f.store, "spoke");
  const nativeOpen = fs.promises.open;
  let archiveOpens = 0, closeCalls = 0;
  let acquired: Awaited<ReturnType<typeof nativeOpen>> | undefined;
  let closeAcquired: (() => Promise<void>) | undefined;
  let fence: Promise<void> | undefined;
  fs.promises.open = async (...args) => {
    const isArchiveData = typeof args[0] === "string" &&
      args[0].startsWith(f.archive + path.sep) && args[0].endsWith(".jsonl");
    if (isArchiveData) archiveOpens++;
    const handle = await nativeOpen(...args);
    if (isArchiveData && !acquired) {
      acquired = handle;
      closeAcquired = handle.close.bind(handle);
      const close = closeAcquired;
      handle.close = () => {
        closeCalls++;
        return close();
      };
      // Native open has resolved, but its caller has not received the handle.
      // fence aborts synchronously, before Work.io's post-await signal check.
      fence = side.fence();
    }
    return handle;
  };
  syncBuiltinESMExports();
  try {
    await f.side.fence();
    await expect(f.side.bridgedIds(0)).rejects.toThrow(/fenced|cancelled/i);
    expect(archiveOpens).toBe(0);

    await expect.soft(side.bridgedIds(0)).rejects.toThrow(/fenced|cancelled/i);
    await fence;
    expect.soft(fence).toBeDefined();
    expect.soft(archiveOpens).toBe(1);
    expect(acquired).toBeDefined();
    expect.soft(closeCalls).toBe(1);
    expect(acquired!.fd).toBe(-1);
  } finally {
    fs.promises.open = nativeOpen;
    syncBuiltinESMExports();
    // Assert custody above before cleaning up a regression's leaked native FD.
    // Bypass the observer so cleanup cannot satisfy the close-count assertion.
    if (acquired && acquired.fd !== -1) await closeAcquired!();
  }

  // A cancelled side must not poison the same store's archive/live handover.
  const restarted = new StoreBridgeSide(f.store, "spoke");
  expect(await restarted.bridgedIds(0)).toEqual({
    ids: ["original-1", "original-2", "original-3"], through: 3, done: true,
  });
});

it.each(["missing", "null"] as const)(
  "rejects a seal with %s files without exposing a partial page or reaching readiness",
  async fileShape => {
    const earlier = event(1, "earlier-archive-identity", "fixture.earlier");
    const required = event(2, "required-archive-identity", "fixture.required");
    const liveHead = event(3, "live-head-identity", "fixture.live");
    const f = fixture([], [liveHead]);
    const earlierDay = "2000/01/01", malformedDay = "2000/01/02";
    f.appendArchive([earlier], earlierDay);
    f.appendArchive([required], malformedDay);
    const requiredFile = path.join(f.archive, malformedDay, archiveFileName(required.topic));
    expect(fs.readFileSync(requiredFile, "utf8")).toBe(line(required));
    expect(required.data).toMatchObject({ bridge: { from: "spoke", id: "required-archive-identity" } });
    expect(required.sequence).toBeLessThan(liveHead.sequence);
    fs.writeFileSync(path.join(f.archive, malformedDay, "SEAL.json"),
      JSON.stringify(fileShape === "missing" ? {} : { files: null }));

    let page: BridgedIds | undefined, failure: unknown;
    try { page = await f.side.bridgedIds(0); }
    catch (error) { failure = error; }
    expect(failure).toBeInstanceOf(TypeError);
    expect(page).toBeUndefined();
    // A prior archive day was read, but the failed call publishes no partial page.
    expect(f.side.recoveryReadCost?.events).toBe(1);

    const remote = new StoreBridgeSide(new MeshStore(scratch(), 64 * 1024, 500), "hub");
    const cursorPath = path.join(f.root, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, local: "hub", remote: "spoke",
      toLocal: { after: 0, mark: 0 }, toRemote: { after: 0, mark: 0 } }));
    const bridge = new MeshBridge({ localName: "hub", remoteName: "spoke", local: f.side, remote, cursorPath });
    try {
      await expect(bridge.start()).rejects.toBeInstanceOf(TypeError);
      await expect(bridge.deliverRelay("another-spoke", {
        topic: "fabric.control.command", kind: "command", from, to: "session:recipient",
        data: { bridge: { from: "another-spoke", id: "not-ready" } },
      })).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    } finally { await bridge.stop(); }
  },
);

it("preserves all IDs and the page shape with a valid seal on a fresh side", async () => {
  const first = event(1, "archive-identity-one", "fixture.archive.one");
  const second = event(2, "archive-identity-two", "fixture.archive.two");
  const liveHead = event(3, "live-head-identity", "fixture.live");
  const f = fixture([first, second], [liveHead]);
  const files = {
    [path.basename(f.file(first.topic))]: { lastSequence: first.sequence },
    [path.basename(f.file(second.topic))]: { lastSequence: second.sequence },
  };
  fs.writeFileSync(path.join(f.archive, day, "SEAL.json"), JSON.stringify({ files }));
  const freshSide = new StoreBridgeSide(f.store, "spoke");

  expect(await freshSide.bridgedIds(0)).toEqual({
    ids: ["archive-identity-one", "archive-identity-two", "live-head-identity"], through: 3, done: true,
  });
});

// Measure the real public call's metadata, synchronous event probes and all JSON.parse
// attempts. A readFile is one read, not also its internal readSync calls. Async event
// FileHandle.read is observed separately by the race tests below.
const observeBoundaryWork = (
  onMetadataRead?: (file: unknown, bytes: string | Buffer) => void,
  onParse?: (text: string) => void,
) => {
  const cost = { metadataReadCalls: 0, metadataReadBytes: 0, probeReadCalls: 0, probeReadBytes: 0, parseAttempts: 0 };
  const readFileSync = fs.readFileSync, readSync = fs.readSync, readFile = fs.promises.readFile, parse = JSON.parse;
  let nested = 0;
  const syncFiles = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof readFileSync>) => {
    cost.metadataReadCalls++;
    nested++;
    try {
      const result = Reflect.apply(readFileSync, fs, args) as string | Buffer;
      cost.metadataReadBytes += Buffer.byteLength(result);
      return result;
    } finally { nested--; }
  }) as typeof readFileSync);
  const syncReads = vi.spyOn(fs, "readSync").mockImplementation(((...args: Parameters<typeof readSync>) => {
    if (!nested) cost.probeReadCalls++;
    const bytes = Reflect.apply(readSync, fs, args) as number;
    if (!nested) cost.probeReadBytes += bytes;
    return bytes;
  }) as typeof readSync);
  const asyncFiles = vi.spyOn(fs.promises, "readFile").mockImplementation((async (...args: Parameters<typeof readFile>) => {
    cost.metadataReadCalls++;
    const result = await Reflect.apply(readFile, fs.promises, args) as string | Buffer;
    cost.metadataReadBytes += Buffer.byteLength(result);
    onMetadataRead?.(args[0], result);
    return result;
  }) as typeof readFile);
  const parses = vi.spyOn(JSON, "parse").mockImplementation((...args: Parameters<typeof parse>) => {
    cost.parseAttempts++;
    onParse?.(args[0]);
    return parse(...args);
  });
  return () => {
    syncFiles.mockRestore(); syncReads.mockRestore(); asyncFiles.mockRestore(); parses.mockRestore();
    syncBuiltinESMExports();
    return { ...cost };
  };
};

// Data safety: an optional hint's byte ceiling must change only pruning work, never
// archive coverage, sequence ordering, the public page shape, or actual I/O accounting.
it.each(["small", "oversized"] as const)(
  "preserves ordered after>0 recovery with a %s valid seal hint", async size => {
    const covered = event(2, "already-covered", "fixture.archive.covered");
    const first = event(4, "archive-first", "fixture.archive.outer");
    const middle = event(6, "archive-middle", "fixture.archive.inner");
    const last = event(8, "archive-last", first.topic);
    const liveHead = event(10, "live-head", "fixture.live");
    const f = fixture([covered, first, middle, last], [liveHead]);
    const oversized = size === "oversized", ceiling = 1024 * 1024;
    const files = {
      [path.basename(f.file(covered.topic))]: { lastSequence: covered.sequence },
      [path.basename(f.file(first.topic))]: { lastSequence: last.sequence },
      [path.basename(f.file(middle.topic))]: { lastSequence: middle.sequence },
    };
    // Padding changes no topic metadata. Multibyte text also distinguishes the
    // raw-byte ceiling from a decoded-character limit.
    const sealText = JSON.stringify({ files, padding: oversized ? "é".repeat(ceiling / 2) : "" });
    const sealBytes = Buffer.byteLength(sealText);
    expect(sealBytes > ceiling).toBe(oversized);
    expect(sealText.length).toBeLessThan(ceiling);
    const sealFile = path.join(f.archive, day, "SEAL.json");
    fs.writeFileSync(sealFile, sealText);

    const open = vi.spyOn(fs.promises, "open");
    let sealReads = 0, sealReadBytes = 0, sealDecodes = 0, sealParses = 0;
    const stopBoundary = observeBoundaryWork((file, bytes) => {
      if (file !== sealFile) return;
      sealReads++;
      sealReadBytes += Buffer.byteLength(bytes);
      expect(Buffer.isBuffer(bytes)).toBe(true);
      if (Buffer.isBuffer(bytes)) {
        const toString = bytes.toString.bind(bytes);
        vi.spyOn(bytes, "toString").mockImplementation((...args) => {
          sealDecodes++;
          return toString(...args);
        });
      }
    }, text => { if (text === sealText) sealParses++; });
    syncBuiltinESMExports();
    const page = await f.side.bridgedIds(covered.sequence);
    const boundary = stopBoundary();

    expect(page).toEqual({ ids: ["archive-first", "archive-middle", "archive-last", "live-head"], through: 10, done: true });
    const opened = open.mock.calls.map(([file]) => file);
    expect(opened).toContain(f.file(first.topic));
    expect(opened).toContain(f.file(middle.topic));
    // The small seal still prunes a wholly covered topic. An oversized seal is
    // UNSEALED, not an empty complete seal: even that topic is read and filtered.
    expect(opened.includes(f.file(covered.topic))).toBe(oversized);
    expect(sealReads).toBe(1);
    expect(sealReadBytes).toBe(sealBytes);
    expect(sealDecodes).toBe(oversized ? 0 : 1);
    expect(sealParses).toBe(oversized ? 0 : 1);
    const cost = f.side.recoveryReadCost!;
    expect(cost.events).toBe(4);
    expect(cost.metadataReadCalls).toBe(boundary.metadataReadCalls);
    expect(cost.metadataReadBytes).toBe(boundary.metadataReadBytes);
    expect(cost.metadataReadBytes).toBeGreaterThanOrEqual(sealBytes);
    expect(cost.archiveParseAttempts + cost.liveParseAttempts + cost.probeParseAttempts + cost.metadataParseAttempts)
      .toBe(boundary.parseAttempts);
  },
);

it("retains an oversized seal's completed native bytes on cancellation without parsing or publishing a page", async () => {
  const archived = event(1), f = fixture([archived], [event(2)]);
  const sealFile = path.join(f.archive, day, "SEAL.json");
  const sealText = JSON.stringify({ files: {
    [path.basename(f.file(archived.topic))]: { lastSequence: archived.sequence },
  }, padding: "é".repeat(512 * 1024) });
  const sealBytes = Buffer.byteLength(sealText);
  expect(sealBytes).toBeGreaterThan(1024 * 1024);
  fs.writeFileSync(sealFile, sealText);
  const open = vi.spyOn(fs.promises, "open");
  let sealReadBytes = 0, sealReads = 0, sealParses = 0;
  let fence: Promise<void> | undefined;
  const stopBoundary = observeBoundaryWork((file, bytes) => {
    if (file !== sealFile) return;
    sealReads++;
    sealReadBytes += Buffer.byteLength(bytes);
    // The native read returned real bytes; its caller has not resumed yet.
    fence = f.side.fence();
  }, text => { if (text === sealText) sealParses++; });
  syncBuiltinESMExports();
  let page: BridgedIds | undefined, failure: unknown;
  try { page = await f.side.bridgedIds(0); }
  catch (error) { failure = error; }
  await fence;
  const boundary = stopBoundary();

  expect(fence).toBeDefined();
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).message).toMatch(/fenced|cancelled/i);
  expect(page).toBeUndefined();
  expect(sealReads).toBe(1);
  expect(sealReadBytes).toBe(sealBytes);
  expect(sealParses).toBe(0);
  expect(open.mock.calls.some(([file]) => typeof file === "string" && file.startsWith(f.archive + path.sep))).toBe(false);
  const cost = f.side.recoveryReadCost!;
  expect(cost).toMatchObject({ events: 0, eventBytes: 0, archiveReadCalls: 0, archiveParseAttempts: 0,
    metadataReadCalls: boundary.metadataReadCalls, metadataReadBytes: boundary.metadataReadBytes });
  expect(cost.metadataReadBytes).toBeGreaterThanOrEqual(sealBytes);
  expect(cost.archiveParseAttempts + cost.liveParseAttempts + cost.probeParseAttempts + cost.metadataParseAttempts)
    .toBe(boundary.parseAttempts);
});

// Data safety / native-I/O race: fence after the real read completes but before Work.io
// resumes. Frontier initialization can already have parsed a head without popping any position.
it.each(["archive", "frontier", "live", "probe", "hint"] as const)(
  "retains completed %s read accounting on cancellation without publishing a partial page", async kind => {
    const liveOnly = kind === "live" || kind === "hint";
    const rows = [event(1, "first", "fixture.first"), event(2, "second", "fixture.second"), event(3)];
    const f = liveOnly ? fixture([], rows, false) : fixture(rows.slice(0, 2), rows.slice(2));
    const after = kind === "hint" ? 2 : 0;
    if (kind === "hint") expect(f.store.read({ after: 0, limit: 2 }).map(row => row.sequence)).toEqual([1, 2]);
    const nativeOpen = fs.promises.open;
    const handles: Array<Awaited<ReturnType<typeof nativeOpen>>> = [];
    let nativeBytes = 0, nativeCalls = 0, closeCalls = 0, otherProbeCalls = 0, otherProbeBytes = 0;
    let fence: Promise<void> | undefined;
    const cancelAt = kind === "frontier" ? 2 : 1;
    const archiveRead = kind === "archive" || kind === "frontier";
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await nativeOpen(...args);
      handles.push(handle);
      const close = handle.close.bind(handle);
      handle.close = () => { closeCalls++; return close(); };
      const target = typeof args[0] === "string" && (archiveRead
        ? args[0].startsWith(f.archive + path.sep) && args[0].endsWith(".jsonl")
        : args[0] === path.join(f.root, "events.jsonl"));
      const read = handle.read;
      handle.read = (async (...readArgs: Parameters<typeof read>) => {
        const result = await Reflect.apply(read, handle, readArgs) as Awaited<ReturnType<typeof read>>;
        if (target) {
          nativeCalls++;
          nativeBytes += result.bytesRead;
          if (nativeCalls === cancelAt) fence = f.side.fence();
        } else {
          otherProbeCalls++;
          otherProbeBytes += result.bytesRead;
        }
        return result;
      }) as typeof handle.read;
      return handle;
    });
    const stopBoundary = observeBoundaryWork();
    syncBuiltinESMExports();
    let page: BridgedIds | undefined, failure: unknown;
    try { page = await f.side.bridgedIds(after); }
    catch (error) { failure = error; }
    await fence;
    const boundary = stopBoundary();
    expect(fence).toBeDefined();
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toMatch(/fenced|cancelled/i);
    expect(page).toBeUndefined();
    const cost = f.side.recoveryReadCost;
    expect(cost).toBeDefined();
    const category = archiveRead ? "archive" : kind === "live" ? "live" : "probe";
    expect(cost![`${category}ReadCalls`]).toBe(nativeCalls + (category === "probe" ? boundary.probeReadCalls : 0));
    expect(cost![`${category}ReadBytes`]).toBe(nativeBytes + (category === "probe" ? boundary.probeReadBytes : 0));
    expect(cost!.probeReadCalls).toBe(boundary.probeReadCalls + otherProbeCalls + (category === "probe" ? nativeCalls : 0));
    expect(cost!.probeReadBytes).toBe(boundary.probeReadBytes + otherProbeBytes + (category === "probe" ? nativeBytes : 0));
    expect(cost!.metadataReadCalls).toBe(boundary.metadataReadCalls);
    expect(cost!.metadataReadBytes).toBe(boundary.metadataReadBytes);
    expect(cost!.archiveParseAttempts + cost!.liveParseAttempts + cost!.probeParseAttempts + cost!.metadataParseAttempts)
      .toBe(boundary.parseAttempts);
    expect(nativeCalls).toBe(cancelAt);
    expect(nativeBytes).toBeGreaterThan(0);
    expect(cost!.events).toBe(0);
    expect(cost!.eventBytes).toBe(0);
    expect(cost!.archiveParseAttempts).toBe(kind === "frontier" ? 1 : 0);
    expect(cost!.maxSliceMs).toBeGreaterThan(0);
    if (kind === "hint") expect(nativeBytes).toBe(1);
    expect(closeCalls).toBe(handles.length);
    expect(handles.every(handle => handle.fd === -1)).toBe(true);

    // A subsequent pre-work cancellation must clear stale diagnostics without doing more I/O.
    await expect(f.side.bridgedIds(after)).rejects.toThrow(/fenced|cancelled/i);
    expect(f.side.recoveryReadCost).toBeUndefined();
    expect(nativeCalls).toBe(cancelAt);
  },
);

it("retains metadata bytes completed before cancellation without parsing that file or opening archive data", async () => {
  const f = fixture([event(1)], [event(2)]);
  // Malformed generation metadata is still a real parse attempt; its invalid UTF-8
  // also distinguishes native byte length from the decoded string's byte length.
  fs.writeFileSync(path.join(f.root, "generation"), Buffer.from([0xff]));
  const open = vi.spyOn(fs.promises, "open");
  let nativeBytes = 0;
  let fence: Promise<void> | undefined;
  const stopBoundary = observeBoundaryWork((file, bytes) => {
    if (file === path.join(f.root, MESH_ARCHIVE_CONFIG)) {
      nativeBytes += Buffer.byteLength(bytes);
      fence = f.side.fence();
    }
  });
  syncBuiltinESMExports();
  let failure: unknown;
  try { await f.side.bridgedIds(0); }
  catch (error) { failure = error; }
  await fence;
  const boundary = stopBoundary();
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).message).toMatch(/fenced|cancelled/i);
  expect(fence).toBeDefined();
  expect(nativeBytes).toBeGreaterThan(0);
  expect(f.side.recoveryReadCost).toMatchObject({
    events: 0, eventBytes: 0, metadataReadCalls: boundary.metadataReadCalls, metadataReadBytes: boundary.metadataReadBytes,
    metadataParseAttempts: 1, archiveReadCalls: 0, archiveParseAttempts: 0, liveReadCalls: 0,
    probeReadCalls: boundary.probeReadCalls, probeReadBytes: boundary.probeReadBytes, probeParseAttempts: 1,
  });
  expect(boundary.metadataReadBytes).toBe(nativeBytes + 1);
  expect(open).not.toHaveBeenCalled();
  const cost = f.side.recoveryReadCost!;
  expect(cost.archiveParseAttempts + cost.liveParseAttempts + cost.probeParseAttempts + cost.metadataParseAttempts)
    .toBe(boundary.parseAttempts);
});

it("counts a rejected native archive read attempt with no known bytes and preserves its error", async () => {
  const f = fixture([event(1)], [event(2)]);
  const nativeOpen = fs.promises.open;
  const handles: Array<Awaited<ReturnType<typeof nativeOpen>>> = [];
  let attempts = 0, nativeError: unknown, failure: unknown, page: BridgedIds | undefined;
  vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
    const handle = await nativeOpen(...args);
    handles.push(handle);
    if (typeof args[0] === "string" && args[0].startsWith(f.archive + path.sep) && args[0].endsWith(".jsonl")) {
      const read = handle.read, close = handle.close.bind(handle);
      handle.read = (async (...readArgs: Parameters<typeof read>) => {
        // Invoke the real FileHandle.read on a closed handle, not a fabricated rejection.
        await close();
        attempts++;
        try { return await Reflect.apply(read, handle, readArgs); }
        catch (error) { nativeError = error; throw error; }
      }) as typeof handle.read;
    }
    return handle;
  });
  syncBuiltinESMExports();
  try { page = await f.side.bridgedIds(0); }
  catch (error) { failure = error; }
  expect(attempts).toBe(1);
  expect(nativeError).toBeInstanceOf(Error);
  expect(failure).toBe(nativeError);
  expect(page).toBeUndefined();
  expect(f.side.recoveryReadCost).toMatchObject({
    events: 0, eventBytes: 0, archiveReadCalls: attempts, archiveReadBytes: 0, archiveParseAttempts: 0,
  });
  expect(handles.length).toBeGreaterThan(0);
  expect(handles.every(handle => handle.fd === -1)).toBe(true);
});

it("yields inside a live-only page, not just once before a 25k-row recovery", async () => {
  const rows = Array.from({ length: 25_000 }, (_, index) => event(index + 1));
  const f = fixture([], rows, false);
  const measured = await heartbeatDuring(() => f.side.bridgedIds(0));
  const first = measured.value;
  expect(first).toEqual({ ids: Array.from({ length: 10_000 }, (_, index) => `original-${index + 1}`),
    through: 10_000, done: false });
  console.log(JSON.stringify({ recoveryLivePage: { positions: 10_000, turns: measured.turns, maxGapMs: measured.maxGapMs } }));
  expect.soft(measured.maxGapMs).toBeLessThan(250);
  expect.soft(measured.turns).toBeGreaterThan(1);
  const second = await f.side.bridgedIds(first.through);
  expect(second.through).toBe(20_000);
  expect(second.done).toBe(false);
  const last = await f.side.bridgedIds(second.through);
  expect(last.through).toBe(25_000);
  expect(last.done).toBe(true);
  expect([...first.ids, ...second.ids, ...last.ids]).toEqual(rows.map((_, index) => `original-${index + 1}`));
});

it.each(["replacement", "shrink", "missing"] as const)("rejects %s of an archive file with a retained in-horizon head", async damage => {
  const prefix = Array.from({ length: 10_000 }, (_, index) => event(index + 1, `prefix-${index}`, "fixture.prefix"));
  const buffered = [event(10_001, "buffered", "fixture.lookahead"), event(12_000, "later", "fixture.lookahead")];
  const f = fixture([...prefix, ...buffered], [event(20_000)]);
  const first = await f.side.bridgedIds(0);
  expect(first).toEqual({ ids: Array.from({ length: 10_000 }, (_, index) => `prefix-${index}`),
    through: 10_000, done: false });
  const target = f.file("fixture.lookahead");
  if (damage === "replacement") {
    const replacement = target + ".replacement";
    fs.writeFileSync(replacement, line(event(10_001, "replacement-must-not-be-a-successful-continuation", "fixture.lookahead")));
    fs.renameSync(replacement, target);
  } else if (damage === "shrink") fs.truncateSync(target, 0);
  else fs.renameSync(target, target + ".held");
  const nativeStat = fs.promises.stat;
  let statAttempts = 0;
  vi.spyOn(fs.promises, "stat").mockImplementation(((...args: Parameters<typeof nativeStat>) => {
    statAttempts++;
    return Reflect.apply(nativeStat, fs.promises, args);
  }) as typeof nativeStat);
  await expect(f.side.bridgedIds(first.through)).rejects.toThrow();
  expect(statAttempts).toBeGreaterThan(0);
  expect(f.side.recoveryReadCost?.sourceValidationCalls).toBe(statAttempts);
});

// Fabric's global writer maximum is 4 MiB. Gate both record shapes here;
// larger historical records retain compatibility, not a 250 ms occupancy SLA.
it("recovers a near-4 MiB supported text-heavy archive row with a heartbeat below 250 ms", async () => {
  const maxEventBytes = 4 * 1024 * 1024;
  const historical = { ...event(1, "supported-text-heavy"), text: "" };
  historical.text = "x".repeat(maxEventBytes - Buffer.byteLength(line(historical)) - 1_024);
  const rawBytes = Buffer.byteLength(line(historical));
  expect(rawBytes).toBeGreaterThan(3.8 * 1024 * 1024);
  expect(rawBytes).toBeLessThanOrEqual(maxEventBytes);
  const head = event(2, "small-live-head");
  const f = fixture([historical, head], [head], true, maxEventBytes);
  expect(f.store.maxEventBytes).toBe(maxEventBytes);

  // Payload construction, serialization and disk setup are outside the timer;
  // only real public recovery is measured, with a small synchronous live head.
  const measured = await heartbeatDuring(() => f.side.bridgedIds(0));
  console.log(JSON.stringify({ recoverySupportedText: { rawBytes, turns: measured.turns, maxGapMs: measured.maxGapMs } }));
  expect(measured.value).toEqual({ ids: ["supported-text-heavy", "small-live-head"], through: 2, done: true });
  expect(measured.maxGapMs).toBeLessThan(250);
});

it("recovers a near-4 MiB supported object-heavy archive row with a heartbeat below 250 ms", async () => {
  const maxEventBytes = 4 * 1024 * 1024;
  const member = { label: "part", text: "tiny text" };
  const historical = { ...event(1, "supported-object-heavy"), data: {
    bridge: { from: "spoke", id: "supported-object-heavy" }, members: [] as Array<typeof member>,
  } };
  // Each member is a distinct small object with two strings, not one giant
  // string disguised as an object payload. Include each array comma in sizing.
  const memberBytes = Buffer.byteLength(JSON.stringify(member)) + 1;
  const emptyBytes = Buffer.byteLength(line(historical));
  historical.data.members = Array.from({ length: Math.floor((maxEventBytes - emptyBytes - 1_024) / memberBytes) },
    () => ({ ...member }));
  const rawBytes = Buffer.byteLength(line(historical));
  expect(rawBytes).toBeGreaterThan(3.8 * 1024 * 1024);
  expect(rawBytes).toBeLessThanOrEqual(maxEventBytes);
  const head = event(2, "small-live-head");
  const f = fixture([historical, head], [head], true, maxEventBytes);
  expect(f.store.maxEventBytes).toBe(maxEventBytes);

  const measured = await heartbeatDuring(() => f.side.bridgedIds(0));
  console.log(JSON.stringify({ recoverySupportedObjects: {
    rawBytes, members: historical.data.members.length, turns: measured.turns, maxGapMs: measured.maxGapMs,
  } }));
  expect(measured.value).toEqual({ ids: ["supported-object-heavy", "small-live-head"], through: 2, done: true });
  expect(measured.maxGapMs).toBeLessThan(250);
});

it("visits a complete 6 MiB historical row above today's configured store maximum", async () => {
  const historical = { ...event(1, "large-historical"), text: "x".repeat(6 * 1024 * 1024) };
  const rawBytes = Buffer.byteLength(line(historical));
  const head = event(2, "small-live-head");
  const f = fixture([historical, head], [head]);
  expect(rawBytes).toBeGreaterThan(f.store.maxEventBytes);
  const measured = await heartbeatDuring(() => f.side.bridgedIds(0));
  // Deliberately no timing assertion above the supported writer envelope.
  console.log(JSON.stringify({ recoveryHistoricalCompatibility: { rawBytes, turns: measured.turns, maxGapMs: measured.maxGapMs } }));
  expect(measured.value).toEqual({ ids: ["large-historical", "small-live-head"], through: 2, done: true });
});
