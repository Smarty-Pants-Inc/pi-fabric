import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive, archiveFileName, currentBoot } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

// smarty-dev#4383 round 6: the live fleet mesh is archive-configured. Archive publish and
// publishBatch must hold the mesh lock with ZERO fsyncs; every barrier runs after release.
const bases: string[] = [];
const from = { id: "session:archive-fsync", name: "archive-fsync", kind: "main" as const };
const setup = (archive = true) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-archive-fsync-"));
  bases.push(base);
  const root = path.join(base, "mesh");
  const dir = path.join(base, "archive");
  fs.mkdirSync(root, { recursive: true });
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (archive) fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  return { root, dir, store: new MeshStore(root, 64 * 1024, 500) };
};
const today = () => new Date().toISOString().slice(0, 10).replaceAll("-", "/");
const lines = (file: string) => fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean) : [];
const liveLines = (root: string) => lines(path.join(root, "events.jsonl"));
const archived = (dir: string, topic: string) => lines(path.join(dir, today(), archiveFileName(topic)));
const unsynced = (dir: string) => fs.existsSync(path.join(dir, "unsynced")) ? fs.readdirSync(path.join(dir, "unsynced")) : [];
const intent = (root: string, key: string, suffix = ".pending.json") =>
  path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const watch = (root: string) => {
  const held: boolean[] = [];
  const originals = [fs.fsyncSync.bind(fs), fs.fdatasyncSync.bind(fs)];
  ((["fsyncSync", "fdatasyncSync"] as const)).forEach((name, index) => vi.spyOn(fs, name).mockImplementation(fd => {
    held.push(fs.existsSync(path.join(root, ".lock")));
    originals[index]!(fd);
  }));
  return held;
};
const crash = async (root: string, packet: Record<string, unknown>, fence: string) => {
  const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-archive-before-live-crash.mjs"), root, JSON.stringify(packet)], {
    env: { ...process.env, [fence]: "1" }, stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.on("data", bytes => { stderr += bytes; });
  child.stdout.resume();
  const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  expect(stderr).toBe("");
  expect(result.code).not.toBe(0);
  if (process.platform !== "win32") expect(result.signal).toBe("SIGKILL");
};
afterEach(() => { vi.restoreAllMocks(); for (const base of bases.splice(0)) fs.rmSync(base, { recursive: true, force: true }); });

describe("archive-configured publication holds the lock with zero fsyncs (smarty-dev#4383)", () => {
  it("publish (keyed and unkeyed) and publishBatch run every archive and live barrier after release", async () => {
    const { root, dir, store } = setup();
    const other = new MeshStore(root, 64 * 1024, 500);
    // The first publish of a boot records BOOT durably (reboot recovery, once per boot), as
    // the live fleet archive already has: measure steady state.
    const events: MeshEvent[] = [await store.publish({ topic: "mesh.archive", from, text: "boot" })];
    const held = watch(root);
    events.push(await store.publish({ topic: "mesh.archive", from, text: "unkeyed", durable: true }));
    events.push(await store.publish({ topic: "mesh.archive", from, text: "keyed", dedupeKey: "k1" }));
    events.push(...await Promise.all([store, other, store, other].map((mesh, index) =>
      mesh.publish({ topic: "mesh.archive", from, text: "concurrent", ...(index % 2 ? { dedupeKey: `c${index}` } : {}) }))));
    events.push(...await store.publishBatch(Array.from({ length: 5 }, (_, index) => ({ topic: "mesh.archive", from, text: `batch ${index}` }))));
    // A keyed head commits alone through the keyed protocol; the caller continues the suffix.
    const mixed = [{ topic: "mesh.archive", from, text: "batch keyed", dedupeKey: "bk" }, { topic: "mesh.archive", from, text: "after" }];
    for (let index = 0; index < mixed.length;) { const done = await store.publishBatch(mixed.slice(index)); events.push(...done); index += done.length; }
    vi.restoreAllMocks();
    expect(held.length).toBeGreaterThan(20);
    expect(held.filter(Boolean)).toEqual([]);
    const live = liveLines(root);
    expect(live.map(line => (JSON.parse(line) as MeshEvent).id).sort()).toEqual(events.map(event => event.id).sort());
    expect(archived(dir, "mesh.archive")).toEqual(live);
    expect(MeshArchive.fromRoot(root)!.head()?.sequence).toBe(events.reduce((max, event) => Math.max(max, event.sequence), 0));
    expect(MeshArchive.fromRoot(root)!.pending()).toBeUndefined();
    expect(unsynced(dir)).toEqual([]);
    expect(MeshArchive.fromRoot(root)!.lookupEntry(events[1]!.sequence)).toMatchObject({ committed: true, event: events[1] });
    expect(fs.readdirSync(path.join(root, "event-receipts")).filter(name => !/^[a-f0-9]{64}\.json$/.test(name))).toEqual([]);
  });

  it("publishBatch without an archive (keyed and unkeyed) also holds no fsync", async () => {
    const { root, store } = setup(false);
    const held = watch(root);
    const inputs = [{ topic: "mesh.plain", from, text: "a" }, { topic: "mesh.plain", from, text: "b" }, { topic: "mesh.plain", from, text: "k", dedupeKey: "pk" }];
    let committed = 0;
    while (committed < inputs.length) committed += (await store.publishBatch(inputs.slice(committed))).length;
    vi.restoreAllMocks();
    expect(held.length).toBeGreaterThan(0);
    expect(held.filter(Boolean)).toEqual([]);
    expect(liveLines(root)).toHaveLength(3);
  });
});

describe("SIGKILL at each deferred-archive crash point (smarty-dev#4383)", () => {
  it.each([
    ["archive staged (marker synced, nothing written)", "PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_STAGE"],
    ["archive renamed (line, index, PENDING written; not live)", "PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_BEGIN"],
    ["live appended (before the archive commit)", "PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_COMMIT"],
    ["live appended and committed", "PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND"],
    ["barrier pending (released, nothing synced yet)", "PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_BARRIER"],
  ])("keyed: %s -> exactly one event, archive equals live", async (_label, fence) => {
    const { root, dir, store } = setup();
    const seed = await store.publish({ topic: "mesh.crash", from, text: "seed" });
    const packet = { topic: "mesh.crash", from, dedupeKey: "crash-key", text: "once" };
    await crash(root, packet, fence);
    const restarted = new MeshStore(root, 64 * 1024, 500);
    const event = await restarted.publish(packet);
    expect(await restarted.publish(packet)).toEqual(event);
    await restarted.publish({ topic: "mesh.crash", from, text: "after" });
    const live = liveLines(root).map(line => JSON.parse(line) as MeshEvent);
    expect(live.filter(entry => entry.dedupeKey === packet.dedupeKey)).toEqual([event]);
    expect(live[0]).toEqual(seed);
    expect(archived(dir, "mesh.crash")).toEqual(liveLines(root));
    expect(MeshArchive.fromRoot(root)!.pending()).toBeUndefined();
    expect(fs.existsSync(intent(root, packet.dedupeKey))).toBe(false);
    // An archive record that reached its live line is the event (promoted); one that never did is discarded.
    if (fence !== "PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_STAGE") expect(event.sequence).toBe(2);
  }, 30_000);

  it.each([
    ["PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_STAGE", 0],
    ["PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_BEGIN", 0],
    ["PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_COMMIT", 1],
    ["PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_BARRIER", 1],
  ] as const)("unkeyed: %s -> the archive-only line is discarded, a live one kept, exactly as before", async (fence, kept) => {
    const { root, dir, store } = setup();
    await store.publish({ topic: "mesh.crash", from, text: "seed" });
    await crash(root, { topic: "mesh.crash", from, text: "crashed" }, fence);
    await new MeshStore(root, 64 * 1024, 500).publish({ topic: "mesh.crash", from, text: "after" });
    const live = liveLines(root).map(line => JSON.parse(line) as MeshEvent);
    expect(live.filter(entry => entry.text === "crashed")).toHaveLength(kept);
    expect(archived(dir, "mesh.crash")).toEqual(liveLines(root));
    expect(MeshArchive.fromRoot(root)!.pending()).toBeUndefined();
  }, 30_000);

  it("reboot recovery restores a live event whose unsynced archive line a power loss took", async () => {
    const { root, dir, store } = setup();
    const seed = await store.publish({ topic: "mesh.reboot", from, text: "seed" });
    const lost = await store.publish({ topic: "mesh.reboot", from, text: "lost from the archive" });
    const archive = MeshArchive.fromRoot(root)!;
    // The power loss: the live line survived (written back), the archive line and its
    // index did not, but the head did and the publisher's synced marker did.
    const marker = archive.stageUnsynced(seed.sequence);
    fs.writeFileSync(marker, JSON.stringify({ since: seed.sequence, pid: 1, boot: "an earlier boot", at: Date.now() }));
    const file = path.join(dir, today(), archiveFileName("mesh.reboot"));
    fs.writeFileSync(file, `${JSON.stringify(seed)}\n`);
    fs.rmSync(path.join(dir, "sequence-index", "0", `${lost.sequence}.json`));
    fs.writeFileSync(path.join(dir, "BOOT"), "an earlier boot");
    const next = await new MeshStore(root, 64 * 1024, 500).publish({ topic: "mesh.reboot", from, text: "next" });
    expect(lines(file).map(line => (JSON.parse(line) as MeshEvent).sequence)).toEqual([seed.sequence, lost.sequence, next.sequence]);
    expect(lines(file)).toEqual(liveLines(root));
    expect(unsynced(dir)).toEqual([]);
    expect(fs.readFileSync(path.join(dir, "BOOT"), "utf8")).toBe(currentBoot());
  });
});

describe("round-6 P3s (smarty-dev#4383)", () => {
  it("crash fences are inert in a process that never armed the test-only symbol", async () => {
    const { root, store } = setup();
    vi.stubEnv("PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND", "1");
    vi.stubEnv("PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_BEGIN", "1");
    try {
      await store.publish({ topic: "mesh.p3", from, text: "survives", dedupeKey: "survives" });
      await store.publish({ topic: "mesh.p3", from, text: "survives too" });
    } finally { vi.unstubAllEnvs(); }
    expect(liveLines(root)).toHaveLength(2);
  });

  it("preserves a same-PID intent owned by another isolate (unknown store token) instead of deleting it", async () => {
    const { root, store } = setup(false);
    const mesh = new MeshStore(root, 64 * 1024, 500, { lockTimeoutMs: 200 });
    const key = "other-isolate";
    const text = JSON.stringify({ version: 2, dedupeKey: key, eventId: randomUUID(), payloadHash: "0".repeat(64),
      owner: { pid: process.pid, token: "attempt", at: Date.now(), store: `${randomUUID()}.${randomUUID()}` } });
    fs.mkdirSync(path.join(root, "event-receipts"), { recursive: true });
    fs.writeFileSync(intent(root, key), text);
    await expect(mesh.publish({ topic: "mesh.p3", from, text: "waits", dedupeKey: key })).rejects.toThrow();
    expect(fs.readFileSync(intent(root, key), "utf8")).toBe(text);
    void store;
  });

  it("recovery sweeps dead attempts' .prepared and .retained stages", async () => {
    const { root, store } = setup(false);
    const dead = spawnSync(process.execPath, ["-e", ""]).pid!;
    const key = "abandoned";
    fs.mkdirSync(path.join(root, "event-receipts"), { recursive: true });
    fs.writeFileSync(intent(root, key), JSON.stringify({ version: 2, dedupeKey: key, eventId: randomUUID(), payloadHash: "0".repeat(64),
      owner: { pid: dead, token: "attempt", at: Date.now() } }));
    const stages = [`${intent(root, key)}.${dead}.${randomUUID()}.prepared`, `${intent(root, key, ".json")}.${dead}.${randomUUID()}.prepared`,
      path.join(root, `events.jsonl.${dead}.${randomUUID()}.retained`)];
    for (const stage of stages) fs.writeFileSync(stage, "x");
    const live = `${intent(root, "live")}.${process.pid}.${randomUUID()}.prepared`;
    fs.writeFileSync(live, "x");
    await store.publish({ topic: "mesh.p3", from, text: "once", dedupeKey: key });
    for (const stage of stages) expect(fs.existsSync(stage)).toBe(false);
    expect(fs.existsSync(live)).toBe(true); // A fresh stage of this live process is kept.
  });
});
