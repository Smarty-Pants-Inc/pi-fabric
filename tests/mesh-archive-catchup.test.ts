import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { archiveFileName, MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshSnapshotRetriesExhaustedError } from "../src/core/atomic-write.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

// Keep: data safety and the real bridge archived-history startup stall. Expectations come
// from the pre-change reader below, not the new range index, stream parser, or merge logic.
const from: MeshIdentity = { id: "session:archive-catchup", name: "archive-catchup", kind: "main" };
const all = () => true;
const MAX = Number.MAX_SAFE_INTEGER;
const missing = (error: unknown) => (error as NodeJS.ErrnoException).code === "ENOENT";
const json = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch (error) { if (missing(error)) return undefined; throw error; }
};
const numeric = (directory: string): string[] => {
  try { return fs.readdirSync(directory).filter(name => /^\d+$/.test(name)).sort(); }
  catch (error) { if (missing(error)) return []; throw error; }
};
const days = (dir: string) => numeric(dir).flatMap(year => numeric(path.join(dir, year))
  .flatMap(month => numeric(path.join(dir, year, month)).map(day => `${year}/${month}/${day}`)));

/** Literal old readAfter algorithm from 98e903559b7a6e048393a7d94f464b3f34f5705b.
 * Only private helpers are inlined; do not optimize this acceptance oracle. In particular,
 * it parses every complete eligible line and sorts an entire day before taking a page.
 */
const legacyReadAfter = (dir: string, after: number, through: number,
  matches: (event: MeshEvent) => boolean, limit: number, topic?: string): MeshEvent[] => {
  const found: MeshEvent[] = [];
  for (const day of days(dir)) {
    const aborted = json<Record<string, string>>(path.join(dir, day, "ABORTED.json"));
    const directory = path.join(dir, day);
    const seal = json<{ files: Record<string, { lastSequence: number }> }>(path.join(directory, "SEAL.json"));
    if (seal && Math.max(0, ...Object.values(seal.files).map(file => file.lastSequence)) <= after) continue;
    const names = topic !== undefined ? [archiveFileName(topic)] : fs.readdirSync(directory).filter(name => name.endsWith(".jsonl"));
    const dayEvents: MeshEvent[] = [];
    for (const name of names) {
      if (seal?.files[name] && seal.files[name].lastSequence <= after) continue;
      let text: string;
      try { text = fs.readFileSync(path.join(directory, name), "utf8"); }
      catch (error) { if (missing(error)) continue; throw error; }
      const lines = text.split("\n");
      lines.pop();
      for (const line of lines.filter(Boolean)) {
        let event: MeshEvent | undefined;
        try {
          const parsed = JSON.parse(line) as MeshEvent;
          event = typeof parsed.sequence === "number" && typeof parsed.id === "string" ? parsed : undefined;
        } catch { event = undefined; }
        if (event && event.sequence > after && event.sequence <= through && aborted?.[event.sequence] !== event.id && matches(event)) {
          dayEvents.push(event);
        }
      }
    }
    found.push(...dayEvents.sort((left, right) => left.sequence - right.sequence));
    if (found.length >= limit) break;
  }
  return found.slice(0, limit);
};

const event = (sequence: number, topic = "fleet.work.catchup", text = "payload"): MeshEvent => ({
  id: `catchup-${String(sequence).padStart(6, "0")}`, sequence, topic, kind: "message", from,
  to: sequence % 3 === 0 ? "session:other" : "session:receiver", createdAt: Date.UTC(2026, 0, 1), text,
  data: { ordinal: sequence, nested: [null, false, "é / 中文 / 🛰️", { escaped: "\n\t\"\\" }] },
});
const setup = () => {
  // The test runner owns final temp teardown; retain the fixture on an interrupted run.
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "archive-catchup-"));
  const root = path.join(base, "mesh"), dir = path.join(base, "archive");
  fs.mkdirSync(root); fs.mkdirSync(dir);
  return { base, root, dir, archive: new MeshArchive(dir, root) };
};
const segment = (dir: string, day: string, topic: string, events: MeshEvent[]) => {
  const file = path.join(dir, day, archiveFileName(topic));
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, events.map(value => JSON.stringify(value) + "\n").join(""));
  return file;
};
const makeFixture = (bridgeStamps = false) => {
  const f = setup();
  const events: MeshEvent[] = [];
  const files: string[] = [];
  for (let day = 0; day < 16; day++) {
    const grouped = new Map<string, MeshEvent[]>();
    for (let index = 0; index < 256; index++) {
      const sequence = day * 256 + index + 1;
      const topic = day === 0 ? "fleet.work.large" : `fleet.work.topic${index % 16}`;
      const value = event(sequence, topic, day === 0 ? "large archived work result / é 🛰️ ".repeat(400) : `result ${sequence}`);
      value.createdAt += day * 86_400_000;
      if (bridgeStamps) value.data = { bridge: { from: sequence % 4 === 0 ? "hub" : "other",
        ...(sequence % 5 === 0 ? { via: "hub" } : {}), id: `original-${sequence}` } };
      events.push(value);
      const group = grouped.get(topic) ?? [];
      group.push(value); grouped.set(topic, group);
    }
    for (const [topic, group] of grouped) files.push(segment(f.dir, `2026/01/${String(day + 1).padStart(2, "0")}`, topic, group));
  }
  return { ...f, events, files, through: events.at(-1)!.sequence };
};
const digest = (events: MeshEvent[]) => createHash("sha256").update(events.map(value => JSON.stringify(value) + "\n").join("")).digest("hex");
const exact = (actual: MeshEvent[], expected: MeshEvent[]) => {
  expect(actual.map(value => value.sequence)).toEqual(expected.map(value => value.sequence));
  // Hash the full serialized output, not a text prefix, id set, or projected payload.
  expect(digest(actual)).toBe(digest(expected));
};
const parsedEvents = () => {
  let count = 0;
  const parse = JSON.parse;
  const spy = vi.spyOn(JSON, "parse").mockImplementation((...args: Parameters<typeof JSON.parse>) => {
    const value: unknown = parse(...args);
    if (value !== null && typeof value === "object" && "sequence" in value && "id" in value && "topic" in value) count++;
    return value;
  });
  return { get count() { return count; }, restore: () => spy.mockRestore() };
};
const stores: MeshStore[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const store of stores.splice(0)) store.closeState(); });

let fixture: ReturnType<typeof makeFixture>;
beforeAll(() => { fixture = makeFixture(); });

describe("lossless archived-history catch-up", () => {
  it("has thousands of events in many topic/day files, including a real multi-MB topic file", () => {
    expect(fixture.events).toHaveLength(4096);
    expect(fixture.files.length).toBeGreaterThan(200);
    expect(fs.statSync(fixture.files[0]!).size).toBeGreaterThan(3 * 1024 * 1024);
  });

  it.each(["sync", "async"] as const)("matches legacy byte/sequence output for middle and older-than-everything cursors (%s)", async mode => {
    const read = mode === "sync" ? fixture.archive.readAfter.bind(fixture.archive) : fixture.archive.readAfterAsync.bind(fixture.archive);
    for (const after of [-1, 0, 2037, fixture.through, fixture.through + 20]) {
      exact(await read(after, fixture.through, all, 5000), legacyReadAfter(fixture.dir, after, fixture.through, all, 5000));
    }
  });

  it.each(["sync", "async"] as const)("repeated small pages neither drop nor duplicate archived events (%s)", async mode => {
    const archive = new MeshArchive(fixture.dir, fixture.root);
    const read = mode === "sync" ? archive.readAfter.bind(archive) : archive.readAfterAsync.bind(archive);
    const found: MeshEvent[] = [];
    let after = 0;
    for (let page = 0; page < 150; page++) {
      const expected = legacyReadAfter(fixture.dir, after, fixture.through, all, 37);
      const actual = await read(after, fixture.through, all, 37);
      exact(actual, expected);
      if (!actual.length) break;
      expect(actual[0]!.sequence).toBeGreaterThan(after);
      found.push(...actual); after = actual.at(-1)!.sequence;
    }
    exact(found, fixture.events);
    expect(new Set(found.map(value => value.id)).size).toBe(found.length);
  });

  it.each(["sync", "async"] as const)("preserves through, predicate, exact topic and page limit semantics (%s)", async mode => {
    const read = mode === "sync" ? fixture.archive.readAfter.bind(fixture.archive) : fixture.archive.readAfterAsync.bind(fixture.archive);
    for (const [after, through, limit, topic] of [
      [259, 2999, 19, undefined], [0, 2277, 5000, "fleet.work.topic7"],
      [2800, 2815, 4, "fleet.work.topic3"], [0, MAX, 3, "fleet.work.absent"],
      [3000, 2999, 5, undefined], [255, 256, 1, "fleet.work.large"],
    ] as const) {
      const matches = (value: MeshEvent) => value.to === "session:receiver" && value.sequence % 5 !== 0;
      exact(await read(after, through, matches, limit, topic), legacyReadAfter(fixture.dir, after, through, matches, limit, topic));
    }
  });

  it.each(["sync", "async"] as const)("uses actual range extrema, not the last line, when old input is out of order (%s)", async mode => {
    const f = setup();
    segment(f.dir, "2026/01/01", "fleet.work.a", [event(10, "fleet.work.a"), event(90, "fleet.work.a"), event(20, "fleet.work.a")]);
    segment(f.dir, "2026/01/01", "fleet.work.b", [event(80, "fleet.work.b"), event(30, "fleet.work.b"), event(70, "fleet.work.b")]);
    const read = mode === "sync" ? f.archive.readAfter.bind(f.archive) : f.archive.readAfterAsync.bind(f.archive);
    for (let pass = 0; pass < 2; pass++) {
      for (const [after, through, limit] of [[0, MAX, 2], [50, MAX, 10], [50, 85, 2], [75, MAX, 1]]) {
        exact(await read(after!, through!, all, limit!), legacyReadAfter(f.dir, after!, through!, all, limit!));
      }
    }
  });

  it.each(["sync", "async"] as const)("preserves legacy stable equal-sequence ties when a limit cuts across topic files (%s)", async mode => {
    const f = setup();
    segment(f.dir, "2026/01/01", "fleet.work.a", [event(50, "fleet.work.a"),
      { ...event(100, "fleet.work.a"), id: "tie-a-first" }, { ...event(100, "fleet.work.a"), id: "tie-a-second" }]);
    segment(f.dir, "2026/01/01", "fleet.work.b", [event(1, "fleet.work.b"),
      { ...event(100, "fleet.work.b"), id: "tie-b" }]);
    const read = mode === "sync" ? f.archive.readAfter.bind(f.archive) : f.archive.readAfterAsync.bind(f.archive);
    for (let pass = 0; pass < 2; pass++) {
      for (const limit of [1, 2, 3, 10]) exact(await read(75, MAX, all, limit), legacyReadAfter(f.dir, 75, MAX, all, limit));
    }
  });

  it.each(["sync", "async"] as const)("honors exact positive aborts but never hides pending or committed:false events inside through (%s)", async mode => {
    const f = setup();
    const values = Array.from({ length: 7 }, (_, index) => event(index + 1));
    const file = segment(f.dir, "2026/01/01", values[0]!.topic, values);
    fs.writeFileSync(path.join(path.dirname(file), "ABORTED.json"), JSON.stringify({ 2: values[1]!.id, 3: "different-id" }));
    fs.writeFileSync(path.join(f.dir, "PENDING.json"), JSON.stringify({ sequence: 4, id: values[3]!.id, file: path.relative(f.dir, file), size: 0 }));
    fs.mkdirSync(path.join(f.dir, "sequence-index", "0"), { recursive: true });
    fs.writeFileSync(path.join(f.dir, "sequence-index", "0", "5.json"), JSON.stringify({ sequence: 5, id: values[4]!.id, file: path.relative(f.dir, file), committed: false }));
    const read = mode === "sync" ? f.archive.readAfter.bind(f.archive) : f.archive.readAfterAsync.bind(f.archive);
    exact(await read(0, 6, all, 100), [values[0]!, ...values.slice(2, 6)]);
    exact(await read(0, 6, all, 100), legacyReadAfter(f.dir, 0, 6, all, 100));
    // Abort metadata can change independently of otherwise unchanged segment/range metadata.
    fs.writeFileSync(path.join(path.dirname(file), "ABORTED.json"), JSON.stringify({ 4: values[3]!.id }));
    exact(await read(0, 7, all, 100), legacyReadAfter(f.dir, 0, 7, all, 100));
  });

  it.each(["sync", "async"] as const)("ignores malformed complete lines and valid JSON without its final newline, then sees completion (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(1), event(5)]);
    fs.appendFileSync(file, '\nnot-json\nnull\n{"sequence":"9","id":"bad"}\n');
    fs.appendFileSync(file, JSON.stringify(event(777, "fleet.work.catchup", "é🛰️".repeat(20_000))));
    const read = mode === "sync" ? f.archive.readAfter.bind(f.archive) : f.archive.readAfterAsync.bind(f.archive);
    exact(await read(0, MAX, all, 1000), [event(1), event(5)]);
    exact(await read(500, MAX, all, 1000), []);
    fs.appendFileSync(file, "\n");
    exact(await read(500, MAX, all, 1000), legacyReadAfter(f.dir, 500, MAX, all, 1000));
    expect((await read(500, MAX, all, 1000))[0]!.sequence).toBe(777);
  });

  it.each(["sync", "async"] as const)("invalidates warmed and reopened ranges after append, truncate and same-size replacement (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(20)]);
    const prefixBytes = fs.statSync(file).size;
    const fixedTime = 1_700_000_000;
    fs.utimesSync(file, fixedTime, fixedTime);
    const read = async (after: number) => {
      const retained = mode === "sync" ? f.archive.readAfter(after, MAX, all, 100) : await f.archive.readAfterAsync(after, MAX, all, 100);
      const archive = new MeshArchive(f.dir, f.root);
      const reopened = mode === "sync" ? archive.readAfter(after, MAX, all, 100) : await archive.readAfterAsync(after, MAX, all, 100);
      exact(retained, reopened);
      return retained;
    };
    exact(await read(0), legacyReadAfter(f.dir, 0, MAX, all, 100));
    fs.appendFileSync(file, JSON.stringify(event(900)) + "\n");
    exact(await read(100), [event(900)]);
    fs.truncateSync(file, prefixBytes);
    exact(await read(0), [event(10), event(20)]);
    const previous = fs.statSync(file);
    const replacement = `${file}.replacement`;
    fs.writeFileSync(replacement, [event(70), event(80)].map(value => JSON.stringify(value) + "\n").join(""));
    expect(fs.statSync(replacement).size).toBe(previous.size);
    fs.utimesSync(replacement, previous.atime, previous.mtime);
    fs.renameSync(replacement, file);
    exact(await read(50), [event(70), event(80)]);
    exact(await read(0), legacyReadAfter(f.dir, 0, MAX, all, 100));
    const replaced = fs.statSync(file);
    fs.writeFileSync(file, [event(40), event(50)].map(value => JSON.stringify(value) + "\n").join(""));
    fs.utimesSync(file, replaced.atime, replaced.mtime);
    const rewritten = fs.statSync(file);
    expect(rewritten.ino).toBe(replaced.ino);
    expect(rewritten.size).toBe(replaced.size);
    expect(rewritten.mtimeMs).toBe(replaced.mtimeMs);
    exact(await read(30), [event(40), event(50)]);
    exact(await read(60), []);
  });

  it.each(["sync", "async"] as const)("does not trust an identity-free stale SEAL after a legacy append or added topic (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.a", [event(10, "fleet.work.a")]);
    fs.writeFileSync(path.join(path.dirname(file), "SEAL.json"), JSON.stringify({ files: {
      [path.basename(file)]: { lines: 1, firstSequence: 10, lastSequence: 10, sha256: "0".repeat(64) },
    } }));
    const read = async (after: number, topic?: string) => mode === "sync"
      ? f.archive.readAfter(after, MAX, all, 100, topic) : f.archive.readAfterAsync(after, MAX, all, 100, topic);
    exact(await read(0), [event(10, "fleet.work.a")]);
    fs.appendFileSync(file, JSON.stringify(event(90, "fleet.work.a")) + "\n");
    segment(f.dir, "2026/01/01", "fleet.work.b", [event(100, "fleet.work.b")]);
    // Intentional safety correction to the old oracle: its stale day seal drops both events.
    exact(await read(50), [event(90, "fleet.work.a"), event(100, "fleet.work.b")]);
    exact(await read(50, "fleet.work.a"), [event(90, "fleet.work.a")]);
    exact(await read(50, "fleet.work.b"), [event(100, "fleet.work.b")]);
  });

  it.each([
    ["warm", "sync"], ["reopened", "sync"], ["warm", "async"], ["reopened", "async"],
  ] as const)("invalidates cached day names on real add, replace and rename with restored directory mtime (%s %s)", async (state, mode) => {
    const f = setup(), day = "2026/01/01";
    const a = segment(f.dir, day, "fleet.work.a", [event(10, "fleet.work.a")]);
    const directory = path.dirname(a);
    const read = async (after = 0) => {
      const archive = state === "reopened" ? new MeshArchive(f.dir, f.root) : f.archive;
      return mode === "sync" ? archive.readAfter(after, 100, all, 100) : archive.readAfterAsync(after, 100, all, 100);
    };
    exact(await read(), [event(10, "fleet.work.a")]); // Warm the process-local cache.
    exact(await read(), [event(10, "fleet.work.a")]); // Reuse only the exact directory generation.
    fs.utimesSync(directory, 1_700_000_000, 1_700_000_000);
    exact(await read(), [event(10, "fleet.work.a")]);
    const readdir = fs.readdirSync;
    let dayReads = 0;
    vi.spyOn(fs, "readdirSync").mockImplementation(((...args: Parameters<typeof fs.readdirSync>) => {
      if (String(args[0]) === directory) dayReads++;
      return readdir(...args);
    }) as typeof fs.readdirSync);
    exact(await read(), [event(10, "fleet.work.a")]);
    expect(dayReads).toBe(0); // The real reader, not a helper-only cache assertion.
    const beforeAdd = fs.statSync(directory);
    const b = segment(f.dir, day, "fleet.work.b", [event(50, "fleet.work.b"), event(200, "fleet.work.b")]);
    fs.utimesSync(directory, beforeAdd.atime, beforeAdd.mtime);
    expect(fs.statSync(directory).mtimeMs).toBe(beforeAdd.mtimeMs);
    exact(await read(40), [event(50, "fleet.work.b")]); // Never omit an added name inside through.
    exact(await read(40), [event(50, "fleet.work.b")]);
    fs.utimesSync(directory, 1_700_000_000, 1_700_000_000);
    exact(await read(40), [event(50, "fleet.work.b")]);
    const beforeReplace = fs.statSync(directory), oldFile = fs.statSync(a);
    const replacement = `${a}.replacement`;
    fs.writeFileSync(replacement, JSON.stringify(event(70, "fleet.work.a")) + "\n");
    fs.utimesSync(replacement, oldFile.atime, oldFile.mtime);
    fs.renameSync(replacement, a);
    fs.utimesSync(directory, beforeReplace.atime, beforeReplace.mtime);
    expect(fs.statSync(directory).mtimeMs).toBe(beforeReplace.mtimeMs);
    exact(await read(40), [event(50, "fleet.work.b"), event(70, "fleet.work.a")]);
    fs.utimesSync(directory, 1_700_000_000, 1_700_000_000);
    exact(await read(40), [event(50, "fleet.work.b"), event(70, "fleet.work.a")]);
    const beforeRename = fs.statSync(directory);
    fs.renameSync(b, `${b}.retained`); // Remove its archive name without deleting fixture bytes.
    fs.utimesSync(directory, beforeRename.atime, beforeRename.mtime);
    expect(fs.statSync(directory).mtimeMs).toBe(beforeRename.mtimeMs);
    exact(await read(40), [event(70, "fleet.work.a")]);
    expect(dayReads).toBeGreaterThanOrEqual(3);
  });

  it.each([
    ["enumeration", "sync"], ["cached", "sync"], ["enumeration", "async"], ["cached", "async"],
  ] as const)("retries a real added name during directory %s validation (%s)", async (state, mode) => {
    const f = setup(), day = "2026/01/01";
    const a = segment(f.dir, day, "fleet.work.a", [event(90, "fleet.work.a")]);
    const directory = path.dirname(a), stat = fs.statSync, readdir = fs.readdirSync;
    if (state === "cached") {
      exact(f.archive.readAfter(0, 100, all, 10), [event(90, "fleet.work.a")]);
      exact(f.archive.readAfter(0, 100, all, 10), [event(90, "fleet.work.a")]);
    }
    fs.utimesSync(directory, 1_700_000_000, 1_700_000_000);
    if (state === "cached") exact(f.archive.readAfter(0, 100, all, 10), [event(90, "fleet.work.a")]);
    let mutations = 0, observations = 0;
    const add = () => {
      if (mutations++) return;
      const before = stat(directory);
      segment(f.dir, day, "fleet.work.b", [event(50, "fleet.work.b"), event(200, "fleet.work.b")]);
      fs.utimesSync(directory, before.atime, before.mtime);
    };
    if (state === "enumeration") {
      vi.spyOn(fs, "readdirSync").mockImplementation(((...args: Parameters<typeof fs.readdirSync>) => {
        const names = readdir(...args);
        if (String(args[0]) === directory && ++observations === 1) add();
        return names; // A real but now obsolete enumeration must never be cached as current.
      }) as typeof fs.readdirSync);
    } else {
      vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
        if (String(args[0]) === directory && ++observations === 2) add();
        return stat(...args); // After initial cache lookup and the final segment sweep.
      }) as typeof fs.statSync);
    }
    const actual = mode === "sync" ? f.archive.readAfter(40, 100, all, 2) : await f.archive.readAfterAsync(40, 100, all, 2);
    expect(mutations).toBe(1);
    expect(observations).toBeGreaterThanOrEqual(state === "cached" ? 4 : 2);
    exact(actual, [event(50, "fleet.work.b"), event(90, "fleet.work.a")]);
    exact(actual, legacyReadAfter(f.dir, 40, 100, all, 2));
  });

  it.each(["sync", "async"] as const)("never installs deferred range files during catch-up (%s)", async mode => {
    const f = setup();
    const values = [event(10, "fleet.work.a"), event(20, "fleet.work.b")];
    const file = segment(f.dir, "2026/01/01", values[0]!.topic, [values[0]!]);
    segment(f.dir, "2026/01/01", values[1]!.topic, [values[1]!]);
    const names = fs.readdirSync(path.dirname(file));
    const rename = vi.spyOn(fs, "renameSync");
    const write = vi.spyOn(fs, "writeFileSync");
    const open = vi.spyOn(fs, "openSync");
    const actual = mode === "sync" ? f.archive.readAfter(0, MAX, all, 100) : await f.archive.readAfterAsync(0, MAX, all, 100);
    exact(actual, values);
    expect(rename).not.toHaveBeenCalled();
    expect(write).not.toHaveBeenCalled();
    expect(open.mock.calls.every(call => call[1] === "r")).toBe(true);
    expect(fs.readdirSync(path.dirname(file))).toEqual(names);
  });

  it.each(["sync", "async"] as const)("reopens dense small-block ranges from memory without reparsing the full segment (%s)", async mode => {
    const f = setup();
    const values = Array.from({ length: 3072 }, (_, index) => event(index + 1, "fleet.work.dense", "x".repeat(64)));
    const file = segment(f.dir, "2026/01/01", "fleet.work.dense", values);
    const names = fs.readdirSync(path.dirname(file));
    exact(await f.archive.readAfterAsync(0, 3072, all, 4000), values);
    const reopened = new MeshArchive(f.dir, f.root), parsed = parsedEvents();
    const actual = mode === "sync" ? reopened.readAfter(3040, 3072, all, 32) : await reopened.readAfterAsync(3040, 3072, all, 32);
    const count = parsed.count; parsed.restore();
    exact(actual, values.slice(-32));
    expect(count).toBeLessThan(100);
    expect(fs.readdirSync(path.dirname(file))).toEqual(names);
  });

  it.each(["sync", "async"] as const)("keeps small-block ordering, ties and unknown ranges across the full published horizon (%s)", async mode => {
    const f = setup();
    const values = Array.from({ length: 200 }, (_, index) => event((index * 73) % 200 + 1, "fleet.work.mixed", "x".repeat(80)));
    values[0] = { ...event(190, "fleet.work.mixed"), id: "early-tie" };
    values[199] = { ...event(190, "fleet.work.mixed"), id: "late-tie" };
    const file = segment(f.dir, "2026/01/01", "fleet.work.mixed", values);
    fs.appendFileSync(file, '{"sequence":1e309,"id":"unknown"}\n' + JSON.stringify(event(250, "fleet.work.mixed")) + "\n");
    for (let pass = 0; pass < 2; pass++) {
      const archive = new MeshArchive(f.dir, f.root);
      for (const [after, through, limit] of [[0, 300, 300], [80, 130, 17], [189, 190, 2], [240, 260, 10]]) {
        const actual = mode === "sync" ? archive.readAfter(after!, through!, all, limit!) : await archive.readAfterAsync(after!, through!, all, limit!);
        exact(actual, legacyReadAfter(f.dir, after!, through!, all, limit!));
      }
    }
  });

  it.each(["sync", "async"] as const)("ignores even checksummed identity-matching legacy range sidecars (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90), event(20)]);
    const directory = path.dirname(file), stat = fs.statSync(file);
    const identity = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    const blocks = [[0, stat.size, 0, 1]];
    const index = { version: 1, identity, min: 0, max: 1, blocks,
      checksum: createHash("sha256").update(JSON.stringify([identity, 0, 1, blocks])).digest("hex") };
    const files = [[path.basename(file), identity, 0, 1, blocks]];
    const catalog = { version: 1, files, checksum: createHash("sha256").update(JSON.stringify(files)).digest("hex") };
    const sibling = path.join(directory, `.range-${createHash("sha256").update(path.basename(file)).digest("hex")}.json`);
    fs.writeFileSync(sibling, JSON.stringify(index));
    fs.writeFileSync(path.join(directory, ".ranges.json"), JSON.stringify(catalog));
    const before = fs.readdirSync(directory).map(name => [name, fs.readFileSync(path.join(directory, name)), fs.statSync(path.join(directory, name)).mtimeMs]);
    const open = fs.openSync;
    const noSidecars = vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      expect(path.basename(String(args[0]))).not.toMatch(/^\.range/);
      return open(...args);
    }) as typeof fs.openSync);
    for (let pass = 0; pass < 8; pass++) {
      const archive = new MeshArchive(f.dir, f.root);
      const actual = mode === "sync" ? archive.readAfter(50, MAX, all, 100) : await archive.readAfterAsync(50, MAX, all, 100);
      exact(actual, legacyReadAfter(f.dir, 50, MAX, all, 100));
    }
    noSidecars.mockRestore();
    expect(fs.readdirSync(directory).map(name => [name, fs.readFileSync(path.join(directory, name)), fs.statSync(path.join(directory, name)).mtimeMs])).toEqual(before);
  });

  it.each(["sync", "async"] as const)("leaves torn and corrupt old sidecars untouched rather than repairing them (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90), event(20)]);
    const directory = path.dirname(file), catalogFile = path.join(directory, ".ranges.json");
    const sibling = path.join(directory, `.range-${createHash("sha256").update(path.basename(file)).digest("hex")}.json`);
    for (const damage of ["{torn", "null", "[]", '{"version":999}', '{"files":[["../unsafe.jsonl","stale",0,1]]}']) {
      fs.writeFileSync(catalogFile, damage);
      fs.writeFileSync(sibling, damage);
      const archive = new MeshArchive(f.dir, f.root);
      const actual = mode === "sync" ? archive.readAfter(50, 100, all, 100) : await archive.readAfterAsync(50, 100, all, 100);
      exact(actual, legacyReadAfter(f.dir, 50, 100, all, 100));
      expect(fs.readFileSync(catalogFile, "utf8")).toBe(damage);
      expect(fs.readFileSync(sibling, "utf8")).toBe(damage);
    }
  });

  it("reuses process-local ranges across fromRoot/new objects, checking real segments every read", () => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90)]);
    fs.writeFileSync(path.join(f.root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: f.dir }));
    const read = () => MeshArchive.fromRoot(f.root)!.readAfter(50, 100, all, 10);
    exact(read(), [event(90)]); // Builds only process-local hints.
    const open = fs.openSync, stat = fs.statSync;
    let catalogOpens = 0, segmentStats = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      if (String(args[0]) === path.join(path.dirname(file), ".ranges.json")) catalogOpens++;
      return open(...args);
    }) as typeof fs.openSync);
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
      if (String(args[0]) === file) segmentStats++;
      return stat(...args);
    }) as typeof fs.statSync);
    exact(read(), [event(90)]); // Fresh objects share memory, never a disk catalog.
    expect(catalogOpens).toBe(0);
    exact(read(), [event(90)]);
    exact(new MeshArchive(f.dir, f.root).readAfter(50, 100, all, 10), [event(90)]);
    expect(catalogOpens).toBe(0);
    expect(segmentStats).toBeGreaterThanOrEqual(6);
    fs.appendFileSync(file, JSON.stringify(event(95)) + "\n");
    exact(read(), [event(90), event(95)]); // A catalog hit never vouches for the current segment.
  });

  it.each([
    ["cold", "sync"], ["warm", "sync"], ["reopened", "sync"],
    ["cold", "async"], ["warm", "async"], ["reopened", "async"],
  ] as const)("retries a real same-inode truncate/rewrite/regrow after initial stat (%s %s)", async (state, mode) => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.a", [event(10, "fleet.work.a")]);
    segment(f.dir, "2026/01/01", "fleet.work.b", [event(90, "fleet.work.b")]);
    if (state !== "cold") exact(f.archive.readAfter(0, 100, all, 100), legacyReadAfter(f.dir, 0, 100, all, 100));
    const archive = state === "reopened" ? new MeshArchive(f.dir, f.root) : f.archive;
    const before = fs.statSync(file), stat = fs.statSync;
    let observations = 0, rewrites = 0;
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
      if (String(args[0]) === file && ++observations === 2) {
        // The first stat was real and already consumed. This is the cold post-scan or
        // warm post-prune validation, not a forged stat result or mocked event stream.
        fs.truncateSync(file, 0);
        fs.appendFileSync(file, [event(10, "fleet.work.a"), event(50, "fleet.work.a"), event(200, "fleet.work.a")]
          .map(value => JSON.stringify(value) + "\n").join(""));
        rewrites++;
      }
      return stat(...args);
    }) as typeof fs.statSync);
    const actual = mode === "sync" ? archive.readAfter(40, 100, all, 2) : await archive.readAfterAsync(40, 100, all, 2);
    expect(rewrites).toBe(1);
    const after = stat(file);
    expect(after.ino).toBe(before.ino);
    expect(after.size).toBeGreaterThan(before.size);
    // 50 is within the captured published horizon. 200 must not leak through; returning
    // only 90 would advance a bridge cursor past the omitted event on the next page.
    exact(actual, [event(50, "fleet.work.a"), event(90, "fleet.work.b")]);
    exact(actual, legacyReadAfter(f.dir, 40, 100, all, 2));
  });

  it.each(["sync", "async"] as const)("surfaces bounded archive read races as retryable snapshot exhaustion, never a partial page (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90)]);
    exact(f.archive.readAfter(0, 100, all, 10), [event(10), event(90)]);
    const stat = fs.statSync;
    let changes = 0;
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
      const captured = stat(...args);
      if (String(args[0]) === file) fs.appendFileSync(file, JSON.stringify(event(200 + ++changes)) + "\n");
      return captured;
    }) as typeof fs.statSync);
    const read = async () => mode === "sync" ? f.archive.readAfter(40, 100, all, 1) : f.archive.readAfterAsync(40, 100, all, 1);
    const error: unknown = await read().then(() => undefined, failure => failure);
    // The busy file must not cause an unbounded loop, and the error contract records the
    // fixed retry budget consumed by this read (recovery preparation retains its own error).
    expect(error).toBeInstanceOf(MeshSnapshotRetriesExhaustedError);
    expect((error as MeshSnapshotRetriesExhaustedError).attempts).toBe(8);
    expect(changes).toBe(8);
  });

  it.each([
    ["absent", "sync"], ["damaged", "sync"], ["absent", "async"], ["damaged", "async"],
  ] as const)("does not prune by corrupted full-digest counters with intact identity/hash (%s hints, %s)", async (hints, mode) => {
    const f = setup();
    const values = [event(1), event(100)];
    f.archive.catchUp(values.map(value => ({ event: value, line: JSON.stringify(value) })));
    const directory = path.join(f.dir, "2026/01/01"), name = archiveFileName(values[0]!.topic);
    const suffix = createHash("sha256").update(name).digest("hex");
    const checkpointFile = path.join(directory, `.digest-${suffix}.json`);
    const checkpoint = JSON.parse(fs.readFileSync(checkpointFile, "utf8"));
    expect(checkpoint).toMatchObject({ firstSequence: 1, lastSequence: 100,
      hash: { bytes: fs.statSync(path.join(directory, name)).size } });
    if (hints === "damaged") {
      exact(f.archive.readAfter(50, 100, all, 10), [values[1]!]);
      fs.writeFileSync(path.join(directory, `.range-${suffix}.json`), "{torn");
      fs.writeFileSync(path.join(directory, ".ranges.json"), "{torn");
    }
    // Preserve the exact segment identity, SHA-256 state, byte count and valid shape.
    // Only the otherwise-unprotected maximum changes, making a false skip plausible.
    fs.writeFileSync(checkpointFile, JSON.stringify({ ...checkpoint, lastSequence: 1 }));
    const archive = new MeshArchive(f.dir, f.root);
    const actual = mode === "sync" ? archive.readAfter(50, 100, all, 10) : await archive.readAfterAsync(50, 100, all, 10);
    exact(actual, [values[1]!]);
    exact(actual, legacyReadAfter(f.dir, 50, 100, all, 10));
  });

  it.each(["sync", "async"] as const)("ignores legacy summary-only catalog extrema derived from unchecked counters (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(1), event(100)]);
    const stat = fs.statSync(file);
    const files = [[path.basename(file), `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`, 1, 1]];
    fs.writeFileSync(path.join(path.dirname(file), ".ranges.json"), JSON.stringify({ version: 1, files,
      checksum: createHash("sha256").update(JSON.stringify(files)).digest("hex") }));
    const actual = mode === "sync" ? f.archive.readAfter(50, 100, all, 10) : await f.archive.readAfterAsync(50, 100, all, 10);
    exact(actual, [event(100)]);
  });

  it.each(["sync", "async"] as const)("retries when a positive abort appears after the initial missing-marker stat (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90)]);
    const marker = path.join(path.dirname(file), "ABORTED.json"), stat = fs.statSync;
    let observations = 0;
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
      if (String(args[0]) === marker && ++observations === 2) fs.writeFileSync(marker, JSON.stringify({ 90: event(90).id }));
      return stat(...args);
    }) as typeof fs.statSync);
    const actual = mode === "sync" ? f.archive.readAfter(0, 100, all, 10) : await f.archive.readAfterAsync(0, 100, all, 10);
    exact(actual, [event(10)]);
    expect(observations).toBeGreaterThanOrEqual(4);
  });

  it.each(["sync", "async"] as const)("reduces real event parsing by >10x after index warmup and object re-open (%s)", async mode => {
    const f = makeFixture();
    await f.archive.readAfterAsync(0, f.through, all, 5000);
    const after = f.through - 32;
    const baseline = parsedEvents();
    const expected = legacyReadAfter(f.dir, after, f.through, all, 32);
    const oldCount = baseline.count; baseline.restore();
    const measured = parsedEvents();
    const reopened = new MeshArchive(f.dir, f.root);
    const actual = mode === "sync" ? reopened.readAfter(after, f.through, all, 32) : await reopened.readAfterAsync(after, f.through, all, 32);
    const newCount = measured.count; measured.restore();
    exact(actual, expected);
    expect(oldCount).toBe(4096);
    expect(newCount).toBeLessThan(oldCount / 10);
  });

  it("async catch-up tolerates a bounded burst of appends beyond its captured through horizon", async () => {
    const f = makeFixture();
    let completed = false;
    let appends = 0;
    const pending = f.archive.readAfterAsync(0, f.through, all, 5000).finally(() => { completed = true; });
    const growing = async () => {
      // Real I/O completion and a bounded setImmediate probe drive this, not timer polling.
      // No concurrent append is inside the published sequence horizon of this read.
      for (let turn = 0; turn < 200 && !completed; turn++) {
        await new Promise<void>(resolve => setImmediate(resolve));
        if (completed) break;
        fs.appendFileSync(f.files[0]!, JSON.stringify(event(f.through + ++appends, "fleet.work.large")) + "\n");
      }
    };
    const growth = growing();
    try { exact(await pending, f.events); }
    finally { await growth; }
    expect(appends).toBeGreaterThan(0);
  });

  it("cold async archive scanning yields to a real event-loop turn before completion", async () => {
    const f = makeFixture();
    let yielded = false;
    const heartbeat = new Promise<void>(resolve => setImmediate(() => { yielded = true; resolve(); }));
    const pending = f.archive.readAfterAsync(0, f.through, all, 5000);
    const actual = await pending;
    expect(yielded).toBe(true); // A synchronous scan wrapped in Promise.resolve fails this.
    await heartbeat;
    exact(actual, f.events);
  });
});

const archivedStore = (bridgeStamps = false) => {
  const f = makeFixture(bridgeStamps);
  fs.writeFileSync(path.join(f.root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: f.dir }));
  fs.writeFileSync(path.join(f.dir, "MESH.json"), JSON.stringify({ version: 1, firstSequence: 1, meshRoot: f.root }));
  // A compacted real live log: the old cursor must route through archived history.
  fs.writeFileSync(path.join(f.root, "events.jsonl"), f.events.slice(-8).map(value => JSON.stringify(value) + "\n").join(""));
  fs.writeFileSync(path.join(f.root, "sequence"), String(f.through));
  const store = new MeshStore(f.root, 64 * 1024, 47, { stateBackend: "file" });
  stores.push(store);
  return { ...f, store, side: new StoreBridgeSide(store, "hub") };
};

describe("real MeshStore → StoreBridgeSide archived catch-up", () => {
  it("readAsync matches synchronous store filters and returns detached event data", async () => {
    const f = archivedStore();
    const input = { after: 2050, topic: "fleet.work.topic7", to: "session:receiver", limit: 23 };
    const expected = f.store.read(input);
    const actual = await f.store.readAsync(input);
    exact(actual, expected);
    actual[0]!.text = "caller mutation";
    exact(await f.store.readAsync(input), expected);
    exact(await f.store.readAsync({ after: 0, limit: 10_000 }), f.events.slice(0, f.store.maxReadEvents));
  });

  it.each(["read", "tail"] as const)("%s uses store.readAsync and catches up every archive page, not just the live suffix", async method => {
    const f = archivedStore();
    const asyncReads = vi.spyOn(f.store, "readAsync");
    vi.spyOn(f.store, "read").mockImplementation(() => { throw new Error("bridge catch-up regressed to synchronous store.read"); });
    vi.spyOn(MeshArchive.prototype, "readAfter").mockImplementation(() => { throw new Error("async bridge regressed to synchronous archive scan"); });
    const found: MeshEvent[] = [];
    let after = 0;
    let offset: number | undefined;
    for (let pageNo = 0; pageNo < 120; pageNo++) {
      const page = method === "read" ? await f.side.read(after) : await f.side.tail(after, offset);
      expect(page.skipped ?? []).toEqual([]);
      if (page.through === after) break;
      expect(page.through).toBeGreaterThan(after);
      found.push(...page.events); after = page.through; offset = page.offset;
    }
    expect(asyncReads).toHaveBeenCalled();
    exact(found, f.events);
    expect(after).toBe(f.through);
    if (method === "tail") expect(offset).toBe(f.store.latestOffset());
  });

  it.each(["stale", "same"] as const)("cold saved-offset tail yields and preserves every archived event across a %s generation rename", async generation => {
    const f = archivedStore();
    const live = path.join(f.root, "events.jsonl");
    // Persist a real byte boundary after 2037 in the pre-compaction live log.
    fs.writeFileSync(live, f.events.slice(2036).map(value => JSON.stringify(value) + "\n").join(""));
    const saved = { after: 2037, offset: f.store.tail(0, 1).nextOffset };
    const cursor = path.join(f.root, "saved-tail.json");
    fs.writeFileSync(cursor, JSON.stringify(saved));
    exact(await f.store.readAsync({ after: saved.after, limit: 1 }), [f.events[2037]!]);
    // Actual compaction rename; the same-generation case pauses before the gen write.
    const replacement = `${live}.compacted`;
    fs.writeFileSync(replacement, f.events.slice(-8).map(value => JSON.stringify(value) + "\n").join(""));
    fs.renameSync(replacement, live);
    if (generation === "stale") fs.writeFileSync(path.join(f.root, "generation"), "1");
    expect(saved.offset).toBeLessThan(fs.statSync(live).size); // no truncation-only escape
    vi.spyOn(MeshArchive.prototype, "readAfter").mockImplementation(() => { throw new Error("saved-offset tail cold-scanned synchronously"); });
    vi.spyOn(f.store, "nextEventAfter").mockImplementation(() => { throw new Error("tail used sync lookup"); });
    const lookup = vi.spyOn(f.store, "nextEventAfterAsync");
    // Independent inbound progress can be at head: startup does not warm this archive.
    expect(await f.side.bridgedIds(f.through)).toEqual({ ids: [], through: f.through, done: true });
    let { after, offset } = JSON.parse(fs.readFileSync(cursor, "utf8")) as typeof saved;
    let yielded = false;
    const heartbeat = new Promise<void>(resolve => setImmediate(() => { yielded = true; resolve(); }));
    const first = await f.side.tail(after, offset);
    expect(yielded).toBe(true);
    await heartbeat;
    expect(lookup).toHaveBeenCalledWith(saved.after);
    expect(first.offset).toBeUndefined(); // never adopt the live suffix's byte cursor across the gap
    const found: MeshEvent[] = [];
    let page = first;
    for (let pageNo = 0; pageNo < 60; pageNo++) {
      const expected = f.events.slice(after, after + f.store.maxReadEvents);
      exact(page.events, expected);
      expect(page.skipped ?? []).toEqual([]);
      expect(page.through).toBe(expected.at(-1)?.sequence ?? after);
      found.push(...page.events);
      after = page.through;
      if (after === f.through) break;
      page = await f.side.tail(after, page.offset);
    }
    exact(found, f.events.slice(saved.after));
    expect(after).toBe(f.through);
    expect(page.offset).toBe(f.store.latestOffset());
  });

  it.each(["absent", "empty", "starts-above-live"] as const)("async next-event lookup preserves sync authority and live fallback (%s archive)", async mode => {
    const f = setup();
    const live = [event(1), event(5), event(7)];
    fs.writeFileSync(path.join(f.root, "events.jsonl"), live.map(value => JSON.stringify(value) + "\n").join(""));
    fs.writeFileSync(path.join(f.root, "sequence"), "7");
    if (mode !== "absent") fs.writeFileSync(path.join(f.root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: f.dir }));
    if (mode === "starts-above-live") {
      fs.writeFileSync(path.join(f.dir, "MESH.json"), JSON.stringify({ version: 1, firstSequence: 3, meshRoot: f.root }));
      segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(3), event(7)]);
    }
    const store = new MeshStore(f.root, 64 * 1024, 47, { stateBackend: "file" });
    stores.push(store);
    const cursors = [0, 1, 3, 5, 7];
    const expected = cursors.map(after => store.nextEventAfter(after));
    if (mode === "starts-above-live") expect(expected.map(value => value?.sequence)).toEqual([1, 3, 7, 7, undefined]);
    vi.spyOn(MeshArchive.prototype, "readAfter").mockImplementation(() => { throw new Error("async lookup used sync archive"); });
    for (const [index, after] of cursors.entries()) expect(await store.nextEventAfterAsync(after)).toEqual(expected[index]);
    const detached = await store.nextEventAfterAsync(0);
    detached!.text = "caller mutation";
    expect(await store.nextEventAfterAsync(0)).toEqual(expected[0]);
  });

  it("startup bridgedIds scans all archived pages asynchronously and returns exact origin/via ids", async () => {
    const f = archivedStore(true);
    const expected = legacyReadAfter(f.dir, 0, f.through, all, 5000).flatMap(value => {
      const stamp = (value.data as { bridge: { from: string; via?: string; id: string } }).bridge;
      return (stamp.via ?? stamp.from) === "hub" ? [stamp.id] : [];
    });
    const asyncReads = vi.spyOn(f.store, "readAsync");
    vi.spyOn(f.store, "read").mockImplementation(() => { throw new Error("bridgedIds used synchronous store.read"); });
    vi.spyOn(MeshArchive.prototype, "readAfter").mockImplementation(() => { throw new Error("bridgedIds used synchronous archive scan"); });
    let completed = false;
    const pending = f.side.bridgedIds(0).then(page => { completed = true; return page; });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(completed).toBe(false);
    const actual = await pending;
    expect(actual).toEqual({ ids: expected, through: f.through, done: true });
    expect(asyncReads.mock.calls.length).toBeGreaterThan(1);
    expect(await f.side.bridgedIds(f.through)).toEqual({ ids: [], through: f.through, done: true });
  });

  it("the cold real bridge read stays pending while a heartbeat can run", async () => {
    const f = archivedStore();
    let completed = false;
    const pending = f.side.read(0).then(page => { completed = true; return page; });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(completed).toBe(false);
    exact((await pending).events, f.events.slice(0, f.store.maxReadEvents));
  });
});
