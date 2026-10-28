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
    exact(await read(), [event(10, "fleet.work.a")]); // Sidecar/catalog installs change the directory.
    exact(await read(), [event(10, "fleet.work.a")]); // Re-enumerate their actual new generation.
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

  it.each([
    ["sibling", "sync"], ["catalog", "sync"], ["sibling", "async"], ["catalog", "async"],
  ] as const)("does not rebind old names after a new JSONL arrives during deferred %s installation (%s)", async (target, mode) => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.a", [event(90, "fleet.work.a")]);
    const directory = path.dirname(file), rename = fs.renameSync;
    let additions = 0;
    vi.spyOn(fs, "renameSync").mockImplementation(((...args: Parameters<typeof fs.renameSync>) => {
      rename(...args);
      const name = path.basename(String(args[1]));
      if (!additions && path.dirname(String(args[1])) === directory &&
          (target === "catalog" ? name === ".ranges.json" : name.startsWith(".range-"))) {
        additions++;
        segment(f.dir, "2026/01/01", "fleet.work.b", [event(50, "fleet.work.b"), event(200, "fleet.work.b")]);
      }
    }) as typeof fs.renameSync);
    const read = async () => {
      const archive = new MeshArchive(f.dir, f.root);
      return mode === "sync" ? archive.readAfter(40, 100, all, 2) : archive.readAfterAsync(40, 100, all, 2);
    };
    exact(await read(), [event(90, "fleet.work.a")]); // The validated pre-install snapshot.
    expect(additions).toBe(1);
    exact(await read(), [event(50, "fleet.work.b"), event(90, "fleet.work.a")]);
    exact(await read(), legacyReadAfter(f.dir, 40, 100, all, 2));
  });

  it("yields between deferred range installations during a real async catch-up", async () => {
    const f = setup();
    const values = [event(10, "fleet.work.a"), event(20, "fleet.work.b")];
    const file = segment(f.dir, "2026/01/01", values[0]!.topic, [values[0]!]);
    segment(f.dir, "2026/01/01", values[1]!.topic, [values[1]!]);
    const directory = path.dirname(file), rename = fs.renameSync;
    const heartbeatAtInstall: boolean[] = [];
    let heartbeatRan = false;
    let heartbeat: Promise<void> | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((...args: Parameters<typeof fs.renameSync>) => {
      rename(...args);
      const target = String(args[1]);
      if (path.dirname(target) !== directory || !path.basename(target).startsWith(".range-")) return;
      heartbeatAtInstall.push(heartbeatRan);
      if (heartbeatAtInstall.length === 1) {
        heartbeat = new Promise<void>(resolve => setImmediate(() => { heartbeatRan = true; resolve(); }));
      }
    });
    try { exact(await f.archive.readAfterAsync(0, MAX, all, 100), values); }
    finally { await heartbeat; }
    // The old tight install loop records [false, false], even though data scanning yields.
    expect(heartbeatAtInstall).toEqual([false, true]);
  });

  it.each(["sync", "async"] as const)("reopens dense small-block siblings without self-rejecting their metadata size (%s)", async mode => {
    const f = setup();
    const values = Array.from({ length: 3072 }, (_, index) => event(index + 1, "fleet.work.dense", "x".repeat(64)));
    const file = segment(f.dir, "2026/01/01", "fleet.work.dense", values);
    exact(await f.archive.readAfterAsync(0, 3072, all, 4000), values);
    const sibling = path.join(path.dirname(file), `.range-${createHash("sha256").update(path.basename(file)).digest("hex")}.json`);
    const index = JSON.parse(fs.readFileSync(sibling, "utf8"));
    expect(index.blocks.length).toBeGreaterThan(100);
    expect(index.blocks.length).toBeLessThanOrEqual(1024);
    expect(fs.statSync(sibling).size).toBeGreaterThan(4096 + Math.ceil(fs.statSync(file).size / (64 * 1024)) * 256);
    fs.writeFileSync(path.join(path.dirname(file), ".ranges.json"), "{torn");
    const reopened = new MeshArchive(f.dir, f.root), parsed = parsedEvents();
    const actual = mode === "sync" ? reopened.readAfter(3040, 3072, all, 32) : await reopened.readAfterAsync(3040, 3072, all, 32);
    const count = parsed.count; parsed.restore();
    exact(actual, values.slice(-32));
    expect(count).toBeLessThan(100); // The valid sibling was used, not a full-file rebuild.
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

  it.each(["sync", "async"] as const)("persists sibling ranges and safely rebuilds corrupt metadata on re-open (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90), event(20)]);
    const cache = path.join(path.dirname(file), `.range-${createHash("sha256").update(path.basename(file)).digest("hex")}.json`);
    const read = async () => {
      const reopened = new MeshArchive(f.dir, f.root);
      return mode === "sync" ? reopened.readAfter(50, MAX, all, 100) : reopened.readAfterAsync(50, MAX, all, 100);
    };
    exact(await read(), [event(90)]);
    expect(fs.existsSync(cache)).toBe(true);
    const saved = fs.readFileSync(cache, "utf8");
    expect(JSON.parse(saved)).toMatchObject({ version: 1, min: 10, max: 90, blocks: expect.any(Array), checksum: expect.any(String) });
    for (const damage of [
      "{torn", "null", "[]", '{"version":999}',
      JSON.stringify({ ...JSON.parse(saved), checksum: "0".repeat(64) }),
      JSON.stringify({ ...JSON.parse(saved), min: 0, max: 1 }),
      JSON.stringify({ ...JSON.parse(saved), identity: "stale-identity" }),
      JSON.stringify({ ...JSON.parse(saved), blocks: [[0, 1, 0, 1]] }),
    ]) {
      fs.writeFileSync(cache, damage);
      // Damage both independent sources: a valid catalog is allowed to bypass its sibling.
      fs.writeFileSync(path.join(path.dirname(file), ".ranges.json"), "{torn");
      exact(await read(), legacyReadAfter(f.dir, 50, MAX, all, 100));
    }
  });

  it.each(["sync", "async"] as const)("validates catalogs independently, including combined catalog/sibling corruption (%s)", async mode => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90), event(20)]);
    const directory = path.dirname(file), catalogFile = path.join(directory, ".ranges.json");
    const sibling = path.join(directory, `.range-${createHash("sha256").update(path.basename(file)).digest("hex")}.json`);
    const read = async () => {
      const reopened = new MeshArchive(f.dir, f.root);
      return mode === "sync" ? reopened.readAfter(50, 100, all, 100) : reopened.readAfterAsync(50, 100, all, 100);
    };
    exact(await read(), [event(90)]);
    const saved = JSON.parse(fs.readFileSync(catalogFile, "utf8"));
    const withFiles = (files: unknown[]) => JSON.stringify({ ...saved, files,
      checksum: createHash("sha256").update(JSON.stringify(files)).digest("hex") });
    fs.writeFileSync(sibling, "{torn");
    // Both a disk-validated catalog and the subsequently cached catalog are independent
    // of damaged siblings, rather than accidentally relying on the sibling's parse result.
    exact(await read(), [event(90)]);
    exact(await read(), [event(90)]);
    for (const damage of [
      "{torn", "null", "[]", '{"version":999}',
      JSON.stringify({ ...saved, checksum: "0".repeat(64) }),
      withFiles([[saved.files[0][0], saved.files[0][1], 0, 1, saved.files[0][4]]]),
      withFiles([[saved.files[0][0], saved.files[0][1], 10, 90, [[0, 1, 10, 90]]]]),
      withFiles([[saved.files[0][0], "stale-identity", 10, 90, saved.files[0][4]]]),
      withFiles([saved.files[0], saved.files[0]]),
      withFiles([["../unsafe.jsonl", ...saved.files[0].slice(1)]]),
    ]) {
      fs.writeFileSync(catalogFile, damage);
      fs.writeFileSync(sibling, "{torn");
      exact(await read(), legacyReadAfter(f.dir, 50, 100, all, 100));
    }
    // A cached catalog's same-inode, same-size corruption with restored mtime must be
    // rejected by ctime, even though its old validated rows could still prune this read.
    exact(await read(), [event(90)]);
    const before = fs.statSync(catalogFile), text = fs.readFileSync(catalogFile, "utf8");
    const changed = text.replace(/"checksum":"[a-f0-9]/, '"checksum":"!');
    expect(changed.length).toBe(text.length);
    fs.writeFileSync(catalogFile, changed);
    fs.utimesSync(catalogFile, before.atime, before.mtime);
    fs.writeFileSync(sibling, "{torn");
    exact(await read(), [event(90)]);
    expect(JSON.parse(fs.readFileSync(catalogFile, "utf8")).checksum).not.toContain("!");
  });

  it("reuses only exact-current validated catalogs across fromRoot/new objects, checking real segments every read", () => {
    const f = setup();
    const file = segment(f.dir, "2026/01/01", "fleet.work.catchup", [event(10), event(90)]);
    fs.writeFileSync(path.join(f.root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: f.dir }));
    const read = () => MeshArchive.fromRoot(f.root)!.readAfter(50, 100, all, 10);
    exact(read(), [event(90)]); // Builds persisted metadata, not a populated module cache.
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
    exact(read(), [event(90)]); // Honest first reopened read loads and validates the disk catalog.
    expect(catalogOpens).toBe(1);
    exact(read(), [event(90)]);
    exact(new MeshArchive(f.dir, f.root).readAfter(50, 100, all, 10), [event(90)]);
    expect(catalogOpens).toBe(1);
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
