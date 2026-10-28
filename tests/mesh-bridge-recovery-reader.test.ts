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
    let nativeBytes = 0, nativeCalls = 0, closeCalls = 0;
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
      if (target) {
        const read = handle.read;
        handle.read = (async (...readArgs: Parameters<typeof read>) => {
          const result = await Reflect.apply(read, handle, readArgs) as Awaited<ReturnType<typeof read>>;
          nativeCalls++;
          nativeBytes += result.bytesRead;
          if (nativeCalls === cancelAt) fence = f.side.fence();
          return result;
        }) as typeof handle.read;
      }
      return handle;
    });
    syncBuiltinESMExports();
    let page: BridgedIds | undefined, failure: unknown;
    try { page = await f.side.bridgedIds(after); }
    catch (error) { failure = error; }
    await fence;
    expect(fence).toBeDefined();
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toMatch(/fenced|cancelled/i);
    expect(page).toBeUndefined();
    const cost = f.side.recoveryReadCost;
    expect(cost).toBeDefined();
    const category = archiveRead ? "archive" : kind === "live" ? "live" : "probe";
    expect(cost![`${category}ReadCalls`]).toBe(nativeCalls);
    expect(cost![`${category}ReadBytes`]).toBe(nativeBytes);
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

it("retains metadata bytes completed before cancellation without parsing or opening data", async () => {
  const f = fixture([event(1)], [event(2)]);
  const nativeRead = fs.promises.readFile;
  let nativeBytes = 0;
  let fence: Promise<void> | undefined;
  vi.spyOn(fs.promises, "readFile").mockImplementation((async (...args: Parameters<typeof nativeRead>) => {
    const result = await nativeRead(...args);
    if (args[0] === path.join(f.root, MESH_ARCHIVE_CONFIG)) {
      nativeBytes += Buffer.byteLength(result);
      fence = f.side.fence();
    }
    return result;
  }) as typeof nativeRead);
  syncBuiltinESMExports();
  await expect(f.side.bridgedIds(0)).rejects.toThrow(/fenced|cancelled/i);
  await fence;
  expect(fence).toBeDefined();
  expect(nativeBytes).toBeGreaterThan(0);
  expect(f.side.recoveryReadCost).toMatchObject({
    events: 0, eventBytes: 0, metadataReadCalls: 1, metadataReadBytes: nativeBytes,
    archiveReadCalls: 0, archiveParseAttempts: 0, liveReadCalls: 0, probeReadCalls: 0,
  });
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

it.each(["replacement", "shrink"] as const)("rejects %s of an archive file with a retained in-horizon head", async damage => {
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
  } else fs.truncateSync(target, 0);
  await expect(f.side.bridgedIds(first.through)).rejects.toThrow();
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
