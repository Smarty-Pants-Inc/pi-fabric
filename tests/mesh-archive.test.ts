import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive, archiveFileName } from "../src/mesh/archive.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#754 (Paul's decision 2): every mesh event goes into plain append-only files, one per
// topic and UTC day, and a publish fails closed when the archive cannot take it.
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
  const day = (at: string, topic: string) => path.join(dir, at, archiveFileName(topic));
  const lines = (file: string) => fs.readFileSync(file, "utf8").split("\n").filter(Boolean);
  const live = () => lines(path.join(root, "events.jsonl"));
  return { root, dir, store, enable, day, lines, live };
};

afterEach(() => {
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mesh event archive", () => {
  it("writes each event to its topic's file for its UTC day, with the live log's bytes", async () => {
    const { store, day, lines, live, dir } = setup();
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:59.500Z"), toFake: ["Date"] });
    await store.publish({ topic: "github.pi-fabric.pulls", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    vi.setSystemTime(Date.parse("2026-09-28T00:00:00.100Z"));
    await store.publish({ topic: "github.pi-fabric.pulls", from, text: "three" });

    expect(lines(day("2026/09/27", "github.pi-fabric.pulls"))).toEqual([live()[0]]);
    expect(lines(day("2026/09/27", "ops.owner"))).toEqual([live()[1]]);
    expect(lines(day("2026/09/28", "github.pi-fabric.pulls"))).toEqual([live()[2]]);
    // The first event of a new day seals the closed one.
    const seal = JSON.parse(fs.readFileSync(path.join(dir, "2026/09/27/SEAL.json"), "utf8"));
    expect(seal.files["ops.owner.jsonl"]).toMatchObject({ lines: 1, firstSequence: 2, lastSequence: 2 });
    expect(fs.existsSync(path.join(dir, "2026/09/28/SEAL.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(dir, "HEAD.json"), "utf8"))).toMatchObject({ sequence: 3 });
  });

  it("names topic files reversibly for the two topic characters a file name cannot hold", () => {
    expect(archiveFileName("team/auth:x.y")).toBe("team%2Fauth%3Ax.y.jsonl");
  });

  it("fails the publish, and shows no reader the event, when the archive cannot take it", async () => {
    const { store, day, live } = setup();
    await store.publish({ topic: "ops.owner", from, text: "archived" });
    // The append itself fails: a directory stands where the topic's file goes.
    const today = new Date().toISOString().slice(0, 10).replaceAll("-", "/");
    fs.mkdirSync(day(today, "ops.blocked"), { recursive: true });
    await expect(store.publish({ topic: "ops.blocked", from, text: "refused" })).rejects.toThrow();
    expect(live().map((line) => JSON.parse(line).text)).toEqual(["archived"]);
    expect(store.read({ after: 0 }).map((event) => event.text)).toEqual(["archived"]);
    // Once the archive takes events again, publishing resumes; the refused one left a gap.
    fs.rmSync(day(today, "ops.blocked"), { recursive: true });
    await store.publish({ topic: "ops.blocked", from, text: "accepted" });
    expect(live().map((line) => [JSON.parse(line).sequence, JSON.parse(line).text])).toEqual([[1, "archived"], [3, "accepted"]]);
  });

  it("puts a publish that crashed after its archive append into the live log, once", async () => {
    const { store, dir, root, live } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    // Simulate the crash: the event is archived and reserved, but never reached the live log.
    const orphan = { id: "11111111-1111-4111-8111-111111111111", sequence: 2, topic: "ops.owner", kind: "message", from, text: "two", createdAt: Date.now() };
    const line = JSON.stringify(orphan);
    new MeshArchive(dir, root).append([{ event: orphan, line }], { intent: true });
    fs.writeFileSync(path.join(root, "sequence"), "2");

    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(live().map((entry) => JSON.parse(entry).sequence)).toEqual([1, 2, 3]);
    expect(live()[1]).toBe(line);
  });

  it("archives events that a store without the archive appended, without duplicates", async () => {
    const { store, enable, day, lines, live } = setup({ archive: false });
    await store.publish({ topic: "ops.owner", from, text: "before one" });
    await store.publish({ topic: "fabric.actor.output", from, text: "before two" });
    enable();
    await store.publish({ topic: "ops.owner", from, text: "after" });
    await store.publish({ topic: "ops.owner", from, text: "again" });
    const today = new Date().toISOString().slice(0, 10).replaceAll("-", "/");
    expect(lines(day(today, "ops.owner"))).toEqual([live()[0], live()[2], live()[3]]);
    expect(lines(day(today, "fabric.actor.output"))).toEqual([live()[1]]);
  });

  it("adds nothing when a catch-up repeats events it already archived", async () => {
    const { store, dir, root, day, lines, live } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    const entries = live().map((line) => ({ event: JSON.parse(line), line }));
    new MeshArchive(dir, root).append(entries);
    const today = new Date().toISOString().slice(0, 10).replaceAll("-", "/");
    expect(lines(day(today, "ops.owner"))).toEqual(live());
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
