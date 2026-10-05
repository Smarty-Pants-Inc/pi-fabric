import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive, MeshArchiveRecoveryChanged, archiveFileName, currentBoot } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#754 (Paul's decision 2): every mesh event goes into plain append-only files, one per
// topic and UTC day. An event is archived durably before it goes live, and the live append
// commits it; positive archived bytes survive ambiguous mixed-version recovery without replay.
const roots: string[] = [];
const from: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };

const setup = (options: { archive?: boolean; maxEventLogBytes?: number; retainedEventLogBytes?: number } = {}) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-archive-"));
  roots.push(base);
  const root = path.join(base, "mesh");
  const dir = path.join(base, "org", "mesh", "dev1", "fleet");
  fs.mkdirSync(root, { recursive: true });
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 }); // whoever enables the archive makes its root
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
  it("installs a durable direct sequence address before a live append and returns only its exact line", async () => {
    const { store, root, dir } = setup();
    const append = fs.appendFileSync.bind(fs);
    const spy = vi.spyOn(fs, "appendFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (target === path.join(root, "events.jsonl")) {
        const event = JSON.parse(String(args[0]));
        expect(new MeshArchive(dir, root).lookup(event.sequence)).toEqual(event);
      }
      return (append as (...args: unknown[]) => void)(target, ...args);
    }) as typeof fs.appendFileSync);
    const first = await store.publish({ topic: "indexed.first", from, text: "one", dedupeKey: "indexed" });
    spy.mockRestore();
    await store.publish({ topic: "indexed.second", from, text: "two" });
    const directories = vi.spyOn(fs, "readdirSync");
    const reads = vi.spyOn(fs, "readSync");
    expect(new MeshArchive(dir, root).lookup(first.sequence)).toEqual(first);
    expect(directories).not.toHaveBeenCalled();
    expect(reads).toHaveBeenCalledTimes(1);
    expect(reads.mock.results[0]!.value).toBe(Buffer.byteLength(JSON.stringify(first) + "\n"));
  });

  it("keeps an explicit negative index after rollback and treats a missing index as unavailable", async () => {
    const { root, dir } = setup();
    const archive = new MeshArchive(dir, root);
    archive.reserveLookup(1);
    expect(archive.lookup(1)).toBeUndefined();
    const event: MeshEvent = { id: "indexed-pending", sequence: 1, topic: "indexed.pending", kind: "message", from, createdAt: Date.now() };
    const pending = archive.begin({ event, line: JSON.stringify(event) });
    expect(archive.lookup(1)).toEqual(event);
    archive.rollback(pending);
    expect(archive.lookup(1)).toBeUndefined();
    expect(() => archive.lookup(2)).toThrow("sequence index is unavailable");
  });

  it("hides an aborted reservation even if death interrupts its negative sidecar, including reboot promotion", async () => {
    const { root, dir, store } = setup();
    const seed = await store.publish({ topic: "indexed.abort", from, text: "seed" });
    const archive = new MeshArchive(dir, root);
    const event = { ...seed, id: "archive-only", sequence: 2, dedupeKey: "abort-after-overtake", text: "orphan" };
    archive.begin({ event, line: JSON.stringify(event) });
    expect(archive.lookupEntry(2)?.committed).toBe(false);
    const later = { ...seed, id: "later-live", sequence: 3 };
    const pending = archive.begin({ event: later, line: JSON.stringify(later) });
    fs.appendFileSync(path.join(root, "events.jsonl"), JSON.stringify(later) + "\n");
    archive.commit(pending); // Overwrite/remove PENDING, as a mixed-version writer can do.
    expect(archive.readAfter(1, 3, () => true, 100)).toEqual([event, later]);
    const negative = vi.spyOn(archive, "reserveLookup").mockImplementationOnce(() => { throw new Error("death before negative index"); });
    expect(() => archive.abort({ event, line: JSON.stringify(event) })).toThrow("death before negative index");
    negative.mockRestore();
    const index = path.join(dir, "sequence-index", "0", "2.json");
    expect(JSON.parse(fs.readFileSync(index, "utf8"))).toMatchObject({ id: event.id, committed: false });
    expect(archive.lookup(2)).toBeUndefined(); // The durable abort marker takes precedence over the positive address.
    expect(archive.readAfter(1, 3, () => true, 100)).toEqual([later]);
    fs.writeFileSync(path.join(dir, "BOOT"), "earlier-boot");
    const recovery = archive.recover(1, archive.prepareRecovery(1));
    expect(recovery.rebooted).toBe(true);
    expect(recovery.promote.map(entry => entry.event)).toEqual([later]);
    archive.recovered(recovery.promote.at(-1), recovery.promote);
    expect(archive.readAfter(1, 3, () => true, 100)).toEqual([later]);
  });

  it("rejects an abort-marker change after off-lock reboot preflight", async () => {
    const { root, dir, store } = setup();
    const seed = await store.publish({ topic: "indexed.abort-race", from, text: "seed" });
    const archive = new MeshArchive(dir, root);
    const event = { ...seed, id: "abort-after-preflight", sequence: 2 };
    const pending = archive.begin({ event, line: JSON.stringify(event) });
    archive.commit(pending);
    const markers = path.join(dir, path.dirname(pending.file), "ABORTED.json");
    fs.writeFileSync(markers, "{}");
    fs.writeFileSync(path.join(dir, "BOOT"), "earlier-boot");
    const plan = archive.prepareRecovery(1);
    expect(plan?.promote.map(entry => entry.event.id)).toEqual([event.id]);
    // In-place mutation does not change the parent directory identity. The
    // off-lock plan must also fence the existing positive abort-marker file.
    fs.writeFileSync(markers, JSON.stringify({ [event.sequence]: event.id }));
    expect(() => archive.recover(1, plan)).toThrow(MeshArchiveRecoveryChanged);
    expect(archive.prepareRecovery(1)?.promote).toEqual([]);
  });

  it.each(["at-offset", "before-offset", "shorter-replacement", "longer-replacement", "torn", "corrupt", "wrong-length"])("directly distinguishes old cutback/replacement from unavailable bytes (%s)", async damage => {
    const { root, dir, store } = setup();
    const seed = await store.publish({ topic: "indexed.cutback", from, text: "seed" });
    const archive = new MeshArchive(dir, root);
    const event = { ...seed, id: "cut-back-reservation", sequence: 2, dedupeKey: "cutback", text: "x".repeat(100) };
    const pending = archive.begin({ event, line: JSON.stringify(event) });
    const target = path.join(dir, pending.file);
    const index = path.join(dir, "sequence-index", "0", "2.json");
    const indexed = JSON.parse(fs.readFileSync(index, "utf8"));
    if (damage === "at-offset" || damage === "before-offset") {
      fs.truncateSync(target, damage === "at-offset" ? pending.size : pending.size - 1);
    } else if (damage.endsWith("replacement")) {
      fs.truncateSync(target, pending.size);
      const later = { ...seed, id: "old-writer-later", sequence: 3, text: damage === "longer-replacement" ? "x".repeat(5000) : "short" };
      fs.appendFileSync(target, JSON.stringify(later) + "\n");
    } else if (damage === "torn") {
      fs.truncateSync(target, pending.size + 10);
    } else if (damage === "corrupt") {
      fs.truncateSync(target, pending.size);
      fs.appendFileSync(target, "not-json\n");
    } else {
      fs.writeFileSync(index, JSON.stringify({ ...indexed, length: indexed.length + 1 }));
    }
    const directories = vi.spyOn(fs, "readdirSync");
    const history = vi.spyOn(fs, "readFileSync");
    if (["torn", "corrupt", "wrong-length"].includes(damage)) expect(() => archive.lookup(2)).toThrow("is unavailable");
    else expect(archive.lookup(2)).toBeUndefined();
    expect(directories).not.toHaveBeenCalled();
    expect(history.mock.calls.some(([file]) => String(file).endsWith(".jsonl"))).toBe(false);
    // Direct absence does not rewrite old metadata or manufacture an abort.
    expect(JSON.parse(fs.readFileSync(index, "utf8"))).toEqual(damage === "wrong-length" ? { ...indexed, length: indexed.length + 1 } : indexed);
  });

  it("marks a keyed reboot promotion live before archive cursor readers can deliver it", async () => {
    const { root, dir, store } = setup();
    const seed = await store.publish({ topic: "indexed.reboot", from, text: "seed" });
    const archive = new MeshArchive(dir, root);
    const event = { ...seed, id: "synced-keyed", sequence: 2, dedupeKey: "reboot-promotion", text: "synced" };
    archive.begin({ event, line: JSON.stringify(event) });
    fs.writeFileSync(path.join(root, "sequence"), "2");
    fs.writeFileSync(path.join(dir, "BOOT"), "earlier-boot");
    await store.publish({ topic: seed.topic, from, text: "after reboot" });
    expect(archive.lookupEntry(2)).toMatchObject({ event, committed: true });
    expect(store.read({ after: 1 }).filter(item => item.dedupeKey === event.dedupeKey)).toEqual([event]);
    expect(store.nextEventAfter(1)).toEqual(event);
  });

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

  // review F7: a valid 128-character topic can encode to more than a file name may hold.
  const longTopic = (end: string) => `${"a/".repeat(63)}${end}`;

  it("bounds a long topic's file name and keeps distinct topics apart", () => {
    const first = archiveFileName(longTopic("bc"));
    const second = archiveFileName(longTopic("bd"));
    expect(longTopic("bc")).toHaveLength(128);
    expect(Buffer.byteLength(first)).toBeLessThanOrEqual(255);
    expect(first).toContain("~");
    expect(second).not.toBe(first);
    expect(archiveFileName("a".repeat(128))).toBe(`${"a".repeat(128)}.jsonl`);
  });

  it("publishes to a long topic and reads it back from the archive", async () => {
    const { store, file, lines, live } = setup({ maxEventLogBytes: 5_000, retainedEventLogBytes: 600 });
    await store.publish({ topic: longTopic("bc"), from, text: "long" });
    for (let index = 0; index < 6; index++) await store.publish({ topic: "ops.owner", from, text: "x".repeat(900) });
    expect(lines(file(today(), longTopic("bc")))).toHaveLength(1);
    expect(store.oldestSequence()).toBeGreaterThan(1);
    expect(store.read({ after: 0, topic: longTopic("bc") }).map((event) => event.text)).toEqual(["long"]);
    expect(live().length).toBeGreaterThan(0);
  });

  it("enables the archive over a live log that already holds a long topic's event", async () => {
    const { store, enable, file, lines } = setup({ archive: false, maxEventLogBytes: 5_000, retainedEventLogBytes: 600 });
    await store.publish({ topic: longTopic("bc"), from, text: "long" });
    enable();
    for (let index = 0; index < 6; index++) await store.publish({ topic: "ops.owner", from, text: "x".repeat(900) });
    expect(lines(file(today(), longTopic("bc")))).toHaveLength(1);
    expect(lines(file(today(), "ops.owner"))).toHaveLength(6);
    expect(store.read({ after: 0, topic: longTopic("bc") }).map((event) => event.text)).toEqual(["long"]);
    expect(store.read({ after: 0, topic: "ops.owner" }).map((event) => event.sequence)).toEqual([2, 3, 4, 5, 6, 7]);
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
    // Before recovery, no reader sees it: it is past the actual newest live sequence.
    // PENDING alone cannot hide an event inside that horizon (old recovery may leave it stale).
    const archive = new MeshArchive(dir, root);
    expect(archive.readAfter(0, 1, () => true, 10).map((event) => event.sequence)).toEqual([1]);
    fs.writeFileSync(path.join(dir, "PENDING.json"), JSON.stringify({ sequence: 2, id: orphan.id, file: path.relative(dir, target), size }));
    expect(archive.readAfter(0, 1, () => true, 10).map((event) => event.sequence)).toEqual([1]);
    expect(store.nextEventAfter(1)).toBeUndefined();

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

  // review F8: after a power loss only synced data is sure. The archive line of an acknowledged
  // publish was synced; its live append, HEAD and PENDING's removal may all be gone.
  it("puts back an acknowledged event whose live append a power loss took (review F8)", async () => {
    const { store, dir, root, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    const target = file(today(), "ops.owner");
    const [first, second] = live();
    const relative = path.relative(dir, target).split(path.sep).join("/");
    fs.writeFileSync(path.join(dir, "PENDING.json"), JSON.stringify({ sequence: 2, id: JSON.parse(second!).id, file: relative, size: Buffer.byteLength(`${first}\n`) }));
    fs.writeFileSync(path.join(dir, "HEAD.json"), JSON.stringify({ sequence: 1, id: JSON.parse(first!).id, file: relative }));
    fs.writeFileSync(path.join(dir, "BOOT"), "an earlier boot");
    fs.writeFileSync(path.join(root, "events.jsonl"), `${first}\n`);
    fs.writeFileSync(path.join(root, "sequence"), "1");

    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 2, 3]);
    expect(live()[1]).toBe(second);
    expect(sequences(target)).toEqual([1, 2, 3]);
    expect(fs.readFileSync(path.join(dir, "BOOT"), "utf8")).toBe(currentBoot());
  });

  it.each(["retry-first", "unrelated-first"] as const)("promotes the full lost live suffix before keyed intent settlement after reboot (%s)", async ordering => {
    const { root, dir, store, live } = setup();
    const seed = await store.publish({ topic: "ops.owner", from, text: "seed" });
    expect(seed.sequence).toBe(1);
    const livePath = path.join(root, "events.jsonl");
    const seedSize = fs.statSync(livePath).size;
    const recipient: MeshIdentity = { id: "session:recipient", name: "recipient", kind: "main" };
    const inbox = (mesh: MeshStore) => new RootInbox(mesh, recipient, () => [recipient.id], { steerGraceMs: 0, pageSize: 1 });
    // Persist the real recipient cursor before its ordinary, acknowledged work arrives.
    await store.put({ key: inbox(store).key, value: { after: 1 }, identity: recipient });
    const ordinary = await store.publish({ topic: "fleet.work.reboot", from, to: recipient.id, text: "acknowledged ordinary work" });
    expect(ordinary.sequence).toBe(2);
    const packet = { topic: ordinary.topic, from, to: recipient.id, text: "keyed work", dedupeKey: "reboot-key-3" };
    const crash = vi.spyOn(MeshArchive.prototype, "commit").mockImplementationOnce(() => { throw new Error("death before live confirmation/receipt"); });
    await expect(store.publish(packet)).rejects.toThrow("death before live confirmation/receipt");
    crash.mockRestore();
    const archive = new MeshArchive(dir, root);
    const keyed = archive.lookupEntry(3)!;
    expect(keyed).toMatchObject({ committed: false, event: { sequence: 3, dedupeKey: packet.dedupeKey } });
    const base = path.join(root, "event-receipts", createHash("sha256").update(packet.dedupeKey).digest("hex"));
    expect(fs.existsSync(base + ".pending.json")).toBe(true);
    expect(fs.existsSync(base + ".json")).toBe(false);
    expect(archive.pending()?.id).toBe(keyed.event.id);
    expect(archive.readAfter(0, 3, () => true, 100)).toEqual([seed, ordinary, keyed.event]);
    // A reboot loses unsynced live appends but keeps both synced archive lines and the intent.
    fs.truncateSync(livePath, seedSize);
    fs.writeFileSync(path.join(dir, "BOOT"), "previous-boot");
    expect(live().map(line => JSON.parse(line).sequence)).toEqual([1]);
    const restarted = new MeshStore(root, store.maxEventBytes, store.maxReadEvents);
    let unrelated: MeshEvent | undefined;
    if (ordering === "unrelated-first") unrelated = await restarted.publish({ topic: "ops.owner", from, text: "unrelated" });
    // In retry-first, publish(key 3) is the first operation on this store after restart.
    expect(await restarted.publish(packet)).toEqual(keyed.event);
    const expected = [ordinary, keyed.event, ...(unrelated ? [unrelated] : [])];
    expect(restarted.read({ after: 1 })).toEqual(expected);
    expect(live().map(line => JSON.parse(line).sequence)).toEqual([1, ...expected.map(event => event.sequence)]);
    expect(fs.readFileSync(path.join(dir, "BOOT"), "utf8")).toBe(currentBoot());
    const beforeRetry = fs.readFileSync(livePath, "utf8");
    expect(await restarted.publish(packet)).toEqual(keyed.event);
    expect(fs.readFileSync(livePath, "utf8")).toBe(beforeRetry);
    expect(fs.existsSync(base + ".pending.json")).toBe(false);
    expect(archive.readAfter(1, restarted.latestSequence(), () => true, 100).filter(event => event.dedupeKey === packet.dedupeKey)).toEqual([keyed.event]);
    // Exercise RootInbox.#scan's actual paged sequence cursor, not an archive-only reader.
    const recipientInbox = inbox(restarted);
    const batch = await recipientInbox.next(rootInboxSession([]));
    expect(batch.events).toEqual([ordinary, keyed.event]);
    expect(batch.through).toBe(unrelated?.sequence ?? 3);
    const delivered = rootInboxSession([{ type: "custom_message", ...rootInboxMessage(batch.events) }]);
    expect((await recipientInbox.next(delivered)).events).toEqual([]);
    expect(restarted.get(recipientInbox.key)?.value).toMatchObject({ after: batch.through });
    expect((await inbox(new MeshStore(root, store.maxEventBytes, store.maxReadEvents)).next(delivered)).events).toEqual([]);
  });

  it("cuts a torn pending line after a reboot, and keeps its sequence unused", async () => {
    const { store, dir, root, file, live, sequences } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    const target = file(today(), "ops.owner");
    const [first] = live();
    const relative = path.relative(dir, target).split(path.sep).join("/");
    fs.appendFileSync(target, '{"id":"22222222-2222-4222-8222-222222222222","sequence":2,"top');
    fs.writeFileSync(path.join(dir, "PENDING.json"), JSON.stringify({ sequence: 2, id: "22222222-2222-4222-8222-222222222222", file: relative, size: Buffer.byteLength(`${first}\n`) }));
    fs.writeFileSync(path.join(dir, "HEAD.json"), JSON.stringify({ sequence: 1, id: JSON.parse(first!).id, file: relative }));
    fs.writeFileSync(path.join(dir, "BOOT"), "an earlier boot");
    fs.writeFileSync(path.join(root, "sequence"), "2");
    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 3]);
    expect(sequences(target)).toEqual([1, 3]);
  });

  it("syncs the archive line before the event goes live", async () => {
    const { store, root } = setup();
    const sync = vi.spyOn(fs, "fdatasyncSync");
    const append = vi.spyOn(fs, "appendFileSync");
    await store.publish({ topic: "ops.owner", from, text: "one" });
    const liveAppend = append.mock.calls.findIndex(([target]) => target === path.join(root, "events.jsonl"));
    expect(liveAppend).toBeGreaterThanOrEqual(0);
    expect(sync.mock.invocationCallOrder[0]).toBeLessThan(append.mock.invocationCallOrder[liveAppend]!);
  });

  const syncedDirectories = () => {
    const opened = vi.spyOn(fs, "openSync");
    return () => opened.mock.calls
      .filter(([target, flags]) => flags === "r" && fs.statSync(String(target), { throwIfNoEntry: false })?.isDirectory())
      .map(([target]) => String(target));
  };

  it.skipIf(process.platform === "win32")("syncs a new file's day, month, year and root directories, and only then (review F8)", async () => {
    const { store, dir } = setup();
    const synced = syncedDirectories();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    const [year, month, day] = today().split("/");
    const chain = [path.join(dir, year!, month!, day!), path.join(dir, year!, month!), path.join(dir, year!), dir];
    expect(new Set(synced())).toEqual(new Set(chain));
    vi.restoreAllMocks();
    const later = syncedDirectories();
    await store.publish({ topic: "ops.owner", from, text: "two" });
    expect(later()).toEqual([]);
  });

  it.skipIf(process.platform === "win32")("finishes the directory syncs that an interrupted first attempt left (review F8)", async () => {
    const { store, dir, file } = setup();
    await store.publish({ topic: "ops.owner", from, text: "one" });
    // The earlier attempt made the file and stopped before its first line and its syncs.
    fs.writeFileSync(file(today(), "ops.new"), "");
    const synced = syncedDirectories();
    await store.publish({ topic: "ops.new", from, text: "two" });
    const [year, month, day] = today().split("/");
    expect(new Set(synced())).toEqual(new Set([path.join(dir, year!, month!, day!), path.join(dir, year!, month!), path.join(dir, year!), dir]));
  });

  it.skipIf(process.platform === "win32")("finishes a stopped catch-up's directory syncs before any publish succeeds (review F8)", async () => {
    const { store, enable, dir, file, lines } = setup({ archive: false });
    await store.publish({ topic: "ops.owner", from, text: "one" });
    await store.publish({ topic: "ops.owner", from, text: "two" });
    enable();
    const [year, month, day] = today().split("/");
    const dayDirectory = path.join(dir, year!, month!, day!);
    // The catch-up writes and syncs its lines, then its first directory sync fails.
    const open = fs.openSync;
    const failing = vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === dayDirectory && rest[0] === "r") {
        failing.mockRestore();
        throw Object.assign(new Error("EIO"), { code: "EIO" });
      }
      return (open as (...args: unknown[]) => number)(target, ...rest);
    }) as typeof fs.openSync);
    await expect(store.publish({ topic: "ops.owner", from, text: "three" })).rejects.toThrow("EIO");
    expect(lines(file(today(), "ops.owner"))).toHaveLength(2);
    // The retry finds the lines already there, and still syncs the day's directories first.
    const synced = syncedDirectories();
    await store.publish({ topic: "ops.owner", from, text: "three" });
    expect(synced()).toEqual(expect.arrayContaining([dayDirectory, path.join(dir, year!, month!), path.join(dir, year!), dir]));
    expect(lines(file(today(), "ops.owner"))).toHaveLength(3);
  });

  it("fails every publish when the archive root is missing, instead of starting a new archive", async () => {
    const { store, dir, live } = setup();
    fs.rmSync(dir, { recursive: true, force: true });
    await expect(store.publish({ topic: "ops.owner", from, text: "nowhere" })).rejects.toThrow("archive directory is missing");
    expect(live()).toEqual([]);
    expect(fs.existsSync(dir)).toBe(false);
  });

  it("puts back acknowledged events of earlier days that a power loss took (review F9)", async () => {
    const { store, dir, root, file, live } = setup({ maxEventLogBytes: 5_000, retainedEventLogBytes: 600 });
    vi.useFakeTimers({ now: Date.parse("2026-09-27T23:59:58.000Z"), toFake: ["Date"] });
    await store.publish({ topic: "ops.owner", from, text: "1" });
    await store.publish({ topic: "ops.owner", from, text: "2" });
    vi.setSystemTime(Date.parse("2026-09-28T00:00:01.000Z"));
    await store.publish({ topic: "ops.owner", from, text: "3" });
    const [first] = live();
    // After the power loss: HEAD (day B, event 3) survived, the live log kept only event 1.
    fs.writeFileSync(path.join(root, "events.jsonl"), `${first}\n`);
    fs.writeFileSync(path.join(root, "sequence"), "1");
    fs.writeFileSync(path.join(dir, "BOOT"), "an earlier boot");
    await store.publish({ topic: "ops.owner", from, text: "4" });
    expect(live().map((line) => JSON.parse(line).sequence)).toEqual([1, 2, 3, 4]);
    expect(store.read({ after: 1, limit: 10 }).map((event) => event.sequence)).toEqual([2, 3, 4]);
    expect([...fs.readFileSync(file("2026/09/27", "ops.owner"), "utf8").matchAll(/"sequence":(\d+)/g)].map((match) => Number(match[1]))).toEqual([1, 2]);
  });

  it.each([["after", "\n"], ["before", ""]])(
    "rolls back a first publish that crashed %s its archive sync, in the same boot (review F2)",
    async (_label, ending) => {
      const { store, dir, root, file, live, sequences } = setup();
      // The first publish of a fresh archive: BOOT is recorded, the line is written, then a
      // process crash (no reboot) before the live append and before any HEAD.
      fs.writeFileSync(path.join(dir, "BOOT"), currentBoot());
      const target = file(today(), "ops.owner");
      fs.mkdirSync(path.dirname(target), { recursive: true });
      const orphan: MeshEvent = { id: "33333333-3333-4333-8333-333333333333", sequence: 1, topic: "ops.owner", kind: "message", from, text: "never live", createdAt: Date.now() };
      fs.writeFileSync(target, `${JSON.stringify(orphan)}${ending}`);
      fs.writeFileSync(path.join(dir, "PENDING.json"), JSON.stringify({ sequence: 1, id: orphan.id, file: path.relative(dir, target).split(path.sep).join("/"), size: 0 }));
      fs.writeFileSync(path.join(root, "sequence"), "1");
      await store.publish({ topic: "ops.owner", from, text: "two" });
      expect(live().map((line) => JSON.parse(line).sequence)).toEqual([2]);
      expect(sequences(target)).toEqual([2]);
    },
  );

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
