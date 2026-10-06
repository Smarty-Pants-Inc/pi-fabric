import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

const roots: string[] = [];
const from = { id: "session:fsync", name: "fsync", kind: "main" as const };
const store = (compact = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fsync-"));
  roots.push(root);
  return new MeshStore(root, 1024, 100, compact ? { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 } : {});
};
const receipt = (root: string, key: string, suffix = ".json") =>
  path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const watchBarriers = (root: string, onSync?: (fd: number) => void) => {
  const held: boolean[] = [];
  const originals = [fs.fsyncSync.bind(fs), fs.fdatasyncSync.bind(fs)];
  const spies = (["fsyncSync", "fdatasyncSync"] as const).map((name, index) =>
    vi.spyOn(fs, name).mockImplementation(fd => {
      held.push(fs.existsSync(path.join(root, ".lock")));
      onSync?.(fd);
      originals[index]!(fd);
    }));
  return { held, restore: () => spies.forEach(spy => spy.mockRestore()) };
};
const killAtFence = async (root: string, key: string, fence: string) => {
  const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-archive-before-live-crash.mjs"), root,
    JSON.stringify({ topic: "mesh.fsync", from, dedupeKey: key, text: "once" })], {
    env: { ...process.env, [fence]: "1" }, stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "", stdout = "", timedOut = false;
  child.stderr.on("data", bytes => { stderr += bytes; });
  child.stdout.on("data", bytes => { stdout += bytes; });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  const timer = setTimeout(() => { timedOut = true; child.kill("SIGKILL"); }, 15_000);
  try {
    const result = await closed;
    expect(timedOut, stderr).toBe(false);
    expect(stderr).toBe("");
    expect(stdout).toBe(""); // No durable receipt was emitted by the child.
    expect(result.code).not.toBe(0);
    if (process.platform !== "win32") expect(result.signal).toBe("SIGKILL");
  } finally {
    clearTimeout(timer);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await closed.catch(() => undefined);
  }
};
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("mesh publication barriers outside custody", () => {
  it("stages fresh intents/receipts and confirms repeated receipts without any held fsync", async () => {
    const mesh = store();
    const watcher = watchBarriers(mesh.root);
    const packet = { topic: "mesh.fsync", from, dedupeKey: "fresh", text: "once" };
    const event = await mesh.publish(packet);
    expect(await mesh.publish(packet)).toEqual(event);
    expect(watcher.held.length).toBeGreaterThan(0);
    expect(watcher.held).not.toContain(true);
    expect(mesh.read()).toEqual([event]);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
    expect(fs.readdirSync(path.dirname(receipt(mesh.root, packet.dedupeKey))).filter(name => name.endsWith(".prepared"))).toEqual([]);
  });

  it("retains validation and cancellation on the off-lock existing-receipt fast path", async () => {
    const mesh = store();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "fast-path-guards", text: "once" };
    const event = await mesh.publish(packet);
    const watcher = watchBarriers(mesh.root);
    await expect(mesh.publish({ ...packet, topic: "" })).rejects.toThrow();
    await expect(mesh.publish({ ...packet, to: " " })).rejects.toThrow("Mesh recipient is empty");
    const controller = new AbortController();
    controller.abort();
    await expect(mesh.publish({ ...packet, signal: controller.signal })).rejects.toMatchObject({ name: "AbortError" });
    const stopped = new MeshStore(mesh.root, 1024, 100, { writeSignal: controller.signal });
    await expect(stopped.publish(packet)).rejects.toMatchObject({ name: "AbortError" });
    expect(watcher.held).toEqual([]); // Reject before confirming any existing receipt.
    expect(mesh.read()).toEqual([event]);
  });

  it("groups all already queued live confirmations, including across store instances", async () => {
    const mesh = store(), other = new MeshStore(mesh.root, 1024, 100);
    let liveSyncs = 0;
    const watcher = watchBarriers(mesh.root, fd => {
      const opened = fs.fstatSync(fd);
      const live = fs.statSync(path.join(mesh.root, "events.jsonl"));
      if (opened.isFile() && opened.dev === live.dev && opened.ino === live.ino) liveSyncs++;
    });
    const events = await Promise.all(Array.from({ length: 6 }, (_, index) => (index % 2 ? mesh : other)
      .publish({ topic: "mesh.fsync", from, text: String(index), durable: true })));
    expect(liveSyncs).toBe(1);
    expect(watcher.held).not.toContain(true);
    expect(mesh.read()).toEqual(events);
    await mesh.publish({ topic: "mesh.fsync", from, durable: true, text: "later" });
    expect(liveSyncs).toBe(2); // A later append never borrows an already completed barrier.
  });

  it("fails the whole confirmation group, issues no durable receipt, and permits a fresh barrier", async () => {
    const mesh = store();
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementationOnce(() => { throw new Error("group barrier unavailable"); });
    const outcomes = await Promise.allSettled([0, 1, 2].map(index => mesh.publish({ topic: "mesh.fsync", from, text: String(index), durable: true })));
    expect(outcomes.every(result => result.status === "rejected")).toBe(true);
    vi.mocked(fs.fsyncSync).mockImplementation(sync);
    const event = await mesh.publish({ topic: "mesh.fsync", from, durable: true, text: "next" });
    expect(event.sequence).toBe(4);
    expect(mesh.read().map(event => event.sequence)).toEqual([1, 2, 3, 4]);
  });

  it("never issues a receipt when the post-append barrier fails; direct retry syncs off-lock", async () => {
    const mesh = store();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "failed-live", text: "once" };
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const opened = fs.fstatSync(fd);
      let live: fs.Stats | undefined;
      try { live = fs.statSync(path.join(mesh.root, "events.jsonl")); } catch { /* preparation */ }
      if (live && opened.isFile() && opened.dev === live.dev && opened.ino === live.ino) throw new Error("live barrier failed");
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("live barrier failed");
    fail.mockRestore();
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey))).toBe(false);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(true);
    const committed = mesh.read()[0]!;
    const watcher = watchBarriers(mesh.root);
    expect(await mesh.publish(packet)).toEqual(committed);
    expect(watcher.held).not.toContain(true);
    expect(mesh.read()).toEqual([committed]);
  });

  it("deduplicates concurrent writers of the same key", async () => {
    const mesh = store(), other = new MeshStore(mesh.root, 1024, 100);
    const watcher = watchBarriers(mesh.root);
    const packet = { topic: "mesh.fsync", from, dedupeKey: "same-key", text: "once" };
    const events = await Promise.all([mesh.publish(packet), other.publish(packet), mesh.publish(packet)]);
    expect(new Set(events.map(event => event.id)).size).toBe(1);
    expect(mesh.read()).toEqual([events[0]]);
    expect(watcher.held).not.toContain(true);
  });

  it.each(["PI_FABRIC_TEST_CRASH_AFTER_INTENT_PREPARE", "PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND"])(
    "recovers an actual child death at %s without an issued receipt", async fence => {
      const mesh = store(), key = "killed-" + fence;
      await killAtFence(mesh.root, key, fence);
      expect(fs.existsSync(receipt(mesh.root, key))).toBe(false);
      const before = mesh.read();
      expect(before).toHaveLength(fence.endsWith("LIVE_APPEND") ? 1 : 0);
      const recovered = await new MeshStore(mesh.root, 1024, 100).publish({ topic: "mesh.fsync", from, dedupeKey: key, text: "once" });
      if (before.length) expect(recovered).toEqual(before[0]);
      expect(mesh.read()).toEqual([recovered]);
      expect(await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: key })).toEqual(recovered);
    }, 20_000);

  it("settles an unconfirmed keyed anchor off-lock before compaction drops its offset", async () => {
    const mesh = store(true);
    for (let index = 0; index < 3; index++) await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    const packet = { topic: "mesh.fsync", from, dedupeKey: "compact-pending", text: "x".repeat(500) };
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const opened = fs.fstatSync(fd), live = fs.statSync(path.join(mesh.root, "events.jsonl"));
      if (opened.isFile() && opened.dev === live.dev && opened.ino === live.ino) throw new Error("pending barrier failed");
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("pending barrier failed");
    fail.mockRestore();
    const committed = mesh.read().at(-1)!;
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(true);
    const watcher = watchBarriers(mesh.root);
    const latest = await mesh.publish({ topic: "mesh.fsync", from, text: "after pending" });
    expect(await mesh.publish(packet)).toEqual(committed);
    expect(mesh.read().at(-1)).toEqual(latest);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
    expect(fs.statSync(path.join(mesh.root, "events.jsonl")).size).toBeLessThan(2800);
    expect(watcher.held).not.toContain(true);
  });

  it("retries a prepared compaction after a racing append, with every file barrier off-lock", async () => {
    const mesh = store(true), other = new MeshStore(mesh.root, 1024, 100, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 });
    for (let index = 0; index < 3; index++) await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    const open = fs.openSync.bind(fs);
    const stages = new Set<number>();
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = open(...args);
      if (String(args[0]).endsWith(".retained")) stages.add(fd);
      return fd;
    }) as typeof fs.openSync);
    let raced: Promise<MeshEvent> | undefined;
    const watcher = watchBarriers(mesh.root, fd => {
      if (stages.delete(fd) && !raced) raced = other.publish({ topic: "mesh.fsync", from, text: "raced" });
    });
    const trigger = await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    expect(raced).toBeDefined();
    const event = await raced!;
    expect(event.sequence).toBe(trigger.sequence + 1);
    expect(mesh.read().at(-1)).toEqual(event);
    expect(new MeshStore(mesh.root, 1024, 100).read().at(-1)).toEqual(event);
    expect(watcher.held).not.toContain(true);
    const lines = fs.readFileSync(path.join(mesh.root, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line) as MeshEvent);
    expect(lines.every((event, index) => index === 0 || event.sequence > lines[index - 1]!.sequence)).toBe(true);
    expect(fs.readdirSync(mesh.root).filter(name => name.endsWith(".retained"))).toEqual([]);
  });
});
