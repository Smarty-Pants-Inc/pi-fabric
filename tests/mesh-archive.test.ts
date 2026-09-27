import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive, archiveFileName } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#754 (Paul's decision 2): every mesh event goes into plain append-only files, one per
// topic and UTC day. An event is archived durably before it goes live, and the live append
// commits it; a publish that fails or stops before that leaves nothing a reader can see.
const roots: string[] = [];
const from: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };

const setup = (options: { archive?: boolean; maxEventLogBytes?: number; retainedEventLogBytes?: number } = {}) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-archive-"));
  roots.push(base);
  const root = path.join(base, "mesh");
  const dir = path.join(base, "org", "mesh", "dev1", "fleet");
  fs.mkdirSync(root, { recursive: true });
  const enable = () => fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  if (options.archive !== false) enable();
  const store = new MeshStore(root, 4_096, 500, {
    ...(options.maxEventLogBytes ? { maxEventLogBytes: options.maxEventLogBytes } : {}),
    ...(options.retainedEventLogBytes ? { retainedEventLogBytes: options.retainedEventLogBytes } : {}),
  });
  const file = (day: string, topic: string) => path.join(dir, day, archiveFileName(topic));
  const lines = (target: string) => fs.existsSync(target) ? fs.readFileSync(target, "utf8").split("\n").filter(Boolean) : [];
  const live = () => lines(path.join(root, "events.jsonl"));
  const sequences = (target: string) => lines(target).map((line) => JSON.parse(line).sequence);
  return { root, dir, store, enable, file, lines, live, sequences };
};
const today = () => new Date().toISOString().slice(0, 10).replaceAll("-", "/");
const seal = (dir: string, day: string) => JSON.parse(fs.readFileSync(path.join(dir, day, "SEAL.json"), "utf8"));

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mesh event archive", () => {
  it("writes each event to its topic's file for its UTC day, with the live log's bytes", async () => {
    const { store, file, lines, live, dir } = setup();
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:59.500Z"), toFake: ["Date"] });
    await store.publish({ topic: "github.pi-fabric.pulls", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    vi.setSystemTime(Date.parse("2026-09-28T00:00:00.100Z"));
    await store.publish({ topic: "github.pi-fabric.pulls", from, text: "three" });

    expect(lines(file("2026/09/27", "github.pi-fabric.pulls"))).toEqual([live()[0]]);
    expect(lines(file("2026/09/27", "ops.owner"))).toEqual([live()[1]]);
    expect(lines(file("2026/09/28", "github.pi-fabric.pulls"))).toEqual([live()[2]]);
    // The first event of a new day seals the closed one.
    expect(seal(dir, "2026/09/27").files["ops.owner.jsonl"]).toMatchObject({ lines: 1, firstSequence: 2, lastSequence: 2 });
    expect(fs.existsSync(path.join(dir, "2026/09/28/SEAL.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(dir, "HEAD.json"), "utf8"))).toMatchObject({ sequence: 3 });
    expect(fs.existsSync(path.join(dir, "PENDING.json"))).toBe(false);
  });

  it("names topic files reversibly for the two topic characters a file name cannot hold", () => {
    expect(archiveFileName("team/auth:x.y")).toBe("team%2Fauth%3Ax.y.jsonl");
  });

  it("finishes a short write before the event counts as archived (review F1)", async () => {
    const { store, file, lines, live } = setup();
    const write = fs.writeSync;
    let shortened = false;
    const passThrough = write as (...args: unknown[]) => number;
    vi.spyOn(fs, "writeSync").mockImplementation(((...args: unknown[]) => {
      const [descriptor, buffer, offset, length] = args as [number, unknown, number | undefined, number | undefined];
      if (shortened || !Buffer.isBuffer(buffer)) return passThrough(...args);
      shortened = true;
      const start = offset ?? 0;
      return passThrough(descriptor, buffer, start, Math.floor((length ?? buffer.length - start) / 2));
    }) as typeof fs.writeSync);
    await store.publish({ topic: "ops.owner", from, text: "x".repeat(300) });
    expect(shortened).toBe(true);
    expect(lines(file(today(), "ops.owner"))).toEqual(live());
  });

  it("fails the publish, leaves no archive record, and resumes, when the archive cannot take it", async () => {
    const { store, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "archived" });
    // The append itself fails: a directory stands where the topic's file goes.
    fs.mkdirSync(file(today(), "ops.blocked"), { recursive: true });
    await expect(store.publish({ topic: "ops.blocked", from, text: "refused" })).rejects.toThrow();
    expect(live().map((line) => JSON.parse(line).text)).toEqual(["archived"]);
    fs.rmSync(file(today(), "ops.blocked"), { recursive: true });
    await store.publish({ topic: "ops.blocked", from, text: "accepted" });
    expect(live().map((line) => [JSON.parse(line).sequence, JSON.parse(line).text])).toEqual([[1, "archived"], [3, "accepted"]]);
    expect(sequences(file(today(), "ops.blocked"))).toEqual([3]);
  });

  it("cuts a written but unsynced event back out when its sync fails (review F2)", async () => {
    const { store, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    vi.spyOn(fs, "fdatasyncSync").mockImplementationOnce(() => { throw Object.assign(new Error("EIO"), { code: "EIO" }); });
    await expect(store.publish({ topic: "ops.owner", from, text: "unsynced" })).rejects.toThrow("EIO");
    expect(sequences(file(today(), "ops.owner"))).toEqual([1]);
    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 3]);
    expect(sequences(file(today(), "ops.owner"))).toEqual([1, 3]);
  });

  it("cuts the event back out of the archive when its live append fails", async () => {
    const { store, root, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    const append = fs.appendFileSync;
    vi.spyOn(fs, "appendFileSync").mockImplementationOnce(((target: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (target === path.join(root, "events.jsonl")) throw Object.assign(new Error("ENOSPC"), { code: "ENOSPC" });
      return (append as (...args: unknown[]) => void)(target, ...rest);
    }) as typeof fs.appendFileSync);
    await expect(store.publish({ topic: "ops.owner", from, text: "not live" })).rejects.toThrow("ENOSPC");
    expect(sequences(file(today(), "ops.owner"))).toEqual([1]);
    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 3]);
  });

  it.each([
    ["a complete line", "\n"],
    ["a line without its newline", ""],
  ])("cuts back %s that a crash left before the live append (review F2)", async (_label, ending) => {
    const { store, dir, root, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    // The crash: the event was reserved and written to the archive, but never went live.
    const target = file(today(), "ops.owner");
    const orphan: MeshEvent = { id: "11111111-1111-4111-8111-111111111111", sequence: 2, topic: "ops.owner", kind: "message", from, text: "two", createdAt: Date.now() };
    const size = fs.statSync(target).size;
    fs.appendFileSync(target, `${JSON.stringify(orphan)}${ending}`);
    fs.writeFileSync(path.join(root, "sequence"), "2");
    // Before recovery, no reader sees it: it is past the newest live sequence, and it is pending.
    const archive = new MeshArchive(dir, root);
    expect(archive.readAfter(0, 1, () => true, 10).map((event) => event.sequence)).toEqual([1]);
    fs.writeFileSync(path.join(dir, "PENDING.json"), JSON.stringify({ sequence: 2, id: orphan.id, file: path.relative(dir, target), size }));
    expect(archive.readAfter(0, 2, () => true, 10).map((event) => event.sequence)).toEqual([1]);

    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 3]);
    expect(sequences(target)).toEqual([1, 3]);
    expect(fs.existsSync(path.join(dir, "PENDING.json"))).toBe(false);
  });

  it("cuts a torn line that no pending record names before appending", async () => {
    const { store, file, lines, live } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    fs.appendFileSync(file(today(), "ops.owner"), '{"id":"torn');
    await store.publish({ topic: "ops.owner", from, text: "two" });
    expect(lines(file(today(), "ops.owner"))).toEqual(live());
  });

  it.each([
    ["before the head moved", 1],
    ["after the head moved, before PENDING was removed (review F5)", 2],
  ])("keeps an event that went live when the commit stopped %s", async (_label, headSequence) => {
    const { store, dir, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    const target = file(today(), "ops.owner");
    const [first, second] = live().map((line) => JSON.parse(line));
    const relative = path.relative(dir, target).split(path.sep).join("/");
    const head = headSequence === 1 ? first : second;
    // The stop: event 2 is archived and live, but its commit did not finish.
    fs.writeFileSync(path.join(dir, "HEAD.json"), JSON.stringify({ sequence: head.sequence, id: head.id, file: relative }));
    const sizeBefore = fs.readFileSync(target, "utf8").indexOf('\n') + 1;
    fs.writeFileSync(path.join(dir, "PENDING.json"), JSON.stringify({ sequence: 2, id: second.id, file: relative, size: sizeBefore }));
    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(sequences(target)).toEqual([1, 2, 3]);
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 2, 3]);
  });

  it("places a retried multi-day catch-up where the interrupted one did (review F6)", async () => {
    const { store, enable, dir, root } = setup({ archive: false, maxEventLogBytes: 5_000, retainedEventLogBytes: 600 });
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:58.000Z"), toFake: ["Date"] });
    await store.publish({ topic: "topic.x", from, text: "1" });
    await store.publish({ topic: "topic.x", from, text: "2" });
    vi.setSystemTime(Date.parse("2026-09-28T00:00:01.000Z"));
    await store.publish({ topic: "topic.y", from, text: "3" });
    enable();
    // The interrupted catch-up: its files are written and synced, but it stopped before the head.
    const entries = fs.readFileSync(path.join(root, "events.jsonl"), "utf8").split("\n").filter(Boolean)
      .map((line) => ({ event: JSON.parse(line), line }));
    new MeshArchive(dir, root).catchUp(entries);
    for (const name of ["HEAD.json", "2026/09/27/SEAL.json"]) fs.rmSync(path.join(dir, name), { force: true });

    for (let index = 4; index <= 8; index++) await store.publish({ topic: "topic.y", from, text: "x".repeat(900) });
    const ids = ["2026/09/27", "2026/09/28"].flatMap((day) => fs.readdirSync(path.join(dir, day))
      .filter((name) => name.endsWith(".jsonl"))
      .flatMap((name) => fs.readFileSync(path.join(dir, day, name), "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line).id)));
    expect(ids).toHaveLength(8);
    expect(new Set(ids).size).toBe(8);
    expect(store.oldestSequence()).toBeGreaterThan(1);
    expect(store.read({ after: 0, limit: 10 }).map((event) => event.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it("archives events that a store without the archive appended, without duplicates", async () => {
    const { store, enable, file, lines, live } = setup({ archive: false });
    await store.publish({ topic: "ops.owner", from, text: "before one" });
    await store.publish({ topic: "fabric.actor.output", from, text: "before two" });
    enable();
    await store.publish({ topic: "ops.owner", from, text: "after" });
    await store.publish({ topic: "ops.owner", from, text: "again" });
    expect(lines(file(today(), "ops.owner"))).toEqual([live()[0], live()[2], live()[3]]);
    expect(lines(file(today(), "fabric.actor.output"))).toEqual([live()[1]]);
  });

  it("adds nothing when a catch-up repeats events it already archived", async () => {
    const { store, dir, root, file, lines, live } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    new MeshArchive(dir, root).catchUp(live().map((line) => ({ event: JSON.parse(line), line })));
    expect(lines(file(today(), "ops.owner"))).toEqual(live());
  });

  it("seals a closed day even when the first publish of the next day failed or stopped (review F3)", async () => {
    const { store, dir, file } = setup();
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:59.000Z"), toFake: ["Date"] });
    await store.publish({ topic: "ops.owner", from, text: "day A" });
    vi.setSystemTime(Date.parse("2026-09-28T00:00:01.000Z"));
    fs.mkdirSync(file("2026/09/28", "ops.owner"), { recursive: true });
    await expect(store.publish({ topic: "ops.owner", from, text: "day B refused" })).rejects.toThrow();
    fs.rmSync(file("2026/09/28", "ops.owner"), { recursive: true });
    await store.publish({ topic: "ops.owner", from, text: "day B" });
    expect(seal(dir, "2026/09/27").files["ops.owner.jsonl"]).toMatchObject({ lines: 1, lastSequence: 1 });
    // A stop between the commit and the seal: the next publish seals the day.
    fs.rmSync(path.join(dir, "2026/09/27/SEAL.json"));
    await store.publish({ topic: "ops.owner", from, text: "day B again" });
    expect(seal(dir, "2026/09/27").files["ops.owner.jsonl"]).toMatchObject({ lines: 1, lastSequence: 1 });
  });

  it("never writes into a sealed day when the clock goes back (review F4)", async () => {
    const { store, dir, file, sequences } = setup({ maxEventLogBytes: 5_000, retainedEventLogBytes: 600 });
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:59.000Z"), toFake: ["Date"] });
    await store.publish({ topic: "ops.owner", from, text: "A".repeat(900) });
    vi.setSystemTime(Date.parse("2026-09-28T00:00:01.000Z"));
    await store.publish({ topic: "ops.owner", from, text: "B".repeat(900) });
    const sealed = fs.readFileSync(path.join(dir, "2026/09/27/SEAL.json"), "utf8");
    vi.setSystemTime(Date.parse("2026-09-27T23:59:58.000Z"));
    for (let index = 0; index < 5; index++) await store.publish({ topic: "ops.owner", from, text: "C".repeat(900) });
    expect(sequences(file("2026/09/27", "ops.owner"))).toEqual([1]);
    expect(sequences(file("2026/09/28", "ops.owner"))).toEqual([2, 3, 4, 5, 6, 7]);
    expect(fs.readFileSync(path.join(dir, "2026/09/27/SEAL.json"), "utf8")).toBe(sealed);
    expect(store.oldestSequence()).toBeGreaterThan(2);
    expect(store.read({ after: 1, limit: 10 }).map((event) => event.sequence)).toEqual([2, 3, 4, 5, 6, 7]);
    expect(store.read({ after: 0, limit: 10 }).map((event) => event.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("serves a read older than the live log from the archive, in order and filtered", async () => {
    const { store } = setup({ maxEventLogBytes: 6_000, retainedEventLogBytes: 1_500 });
    for (let index = 1; index <= 60; index++) {
      await store.publish({ topic: index % 2 ? "team.odd" : "team.even", from, text: `event ${index}`, ...(index % 3 ? {} : { to: "reviewer" }) });
    }
    expect(store.oldestSequence()).toBeGreaterThan(10);
    expect(store.read({ after: 0, limit: 5 }).map((event) => event.sequence)).toEqual([1, 2, 3, 4, 5]);
    expect(store.read({ after: 2, topic: "team.even", limit: 3 }).map((event) => event.sequence)).toEqual([4, 6, 8]);
    expect(store.read({ after: 0, to: "reviewer", limit: 3 }).map((event) => event.sequence)).toEqual([3, 6, 9]);
    const all = store.read({ after: 0, limit: 500 }).map((event) => event.sequence);
    expect(all).toEqual(Array.from({ length: 60 }, (_, index) => index + 1));
  });
});
