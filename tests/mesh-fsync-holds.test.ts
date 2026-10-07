import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, meshPublishCounters, type MeshEvent } from "../src/mesh/store.js";

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
    await mesh.settleCompaction();
    expect(mesh.read().at(-1)).toEqual(latest);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
    expect(fs.statSync(path.join(mesh.root, "events.jsonl")).size).toBeLessThan(2800);
    expect(watcher.held).not.toContain(true);
  });

  it("folds a racing append into a prepared compaction, with every file barrier off-lock", async () => {
    const mesh = store(true), other = new MeshStore(mesh.root, 1024, 100, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 });
    for (let index = 0; index < 3; index++) await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    const open = fs.openSync.bind(fs);
    const stages = new Set<number>();
    let prepared = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = open(...args);
      if (String(args[0]).endsWith(".retained") && args[1] === "wx") { prepared++; stages.add(fd); }
      return fd;
    }) as typeof fs.openSync);
    let raced: Promise<MeshEvent> | undefined;
    const watcher = watchBarriers(mesh.root, fd => {
      if (stages.delete(fd) && !raced) raced = other.publish({ topic: "mesh.fsync", from, text: "raced" });
    });
    const trigger = await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    await mesh.settleCompaction();
    expect(raced).toBeDefined();
    expect(prepared).toBe(1); // One stage: overtaken only by appends, it is folded, never re-prepared.
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

/** A child's stderr minus the one tolerated line: on win32 a rename over a log that
 * another process holds open is refused (EPERM), and compaction is best effort. */
const unexpectedStderr = (stderr: string) => process.platform !== "win32" ? stderr : stderr.split(/\r?\n/)
  .filter(line => !/\[PI_FABRIC_MESH_COMPACTION\] Warning: .*compaction deferred/.test(line) && !/--trace-warnings/.test(line))
  .join("\n").trim();
// A second process acts while this one is frozen at a chosen point of its own publish.
const peer = (root: string, mode: "publish" | "compact", arg: unknown = {}, options: object = {}) => {
  const result = spawnSync(process.execPath, [path.resolve("tests/fixtures/mesh-intent-race.mjs"), root, mode, JSON.stringify(arg)], {
    env: { ...process.env, MESH_RACE_OPTIONS: JSON.stringify(options) }, encoding: "utf8", timeout: 30_000,
  });
  expect(unexpectedStderr(result.stderr)).toBe("");
  return JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { ok: boolean; event?: MeshEvent; message?: string };
};
const compactOptions = { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 };
const live = (root: string) => path.join(root, "events.jsonl");
const generation = (root: string) => { try { return fs.readFileSync(path.join(root, "generation"), "utf8"); } catch { return "0"; } };
/** Freeze the publisher in its off-lock intent barrier (after install, before append).
 * Hook the barrier's namespace check of the intent itself (lstat), not an fsync: win32
 * has no directory fsync, so its first unlocked fsync after install follows the append. */
const betweenInstallAndAppend = (root: string, key: string, act: () => void) => {
  const intent = receipt(root, key, ".pending.json");
  let installs = 0, acted = false, kept: boolean | undefined;
  const rename = fs.renameSync.bind(fs), lstat = fs.lstatSync.bind(fs);
  const appended = () => { try { return fs.readFileSync(live(root), "utf8").includes(`"dedupeKey":${JSON.stringify(key)}`); } catch { return false; } };
  vi.spyOn(fs, "renameSync").mockImplementation((source, target) => { rename(source, target); if (target === intent) installs++; });
  vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
    if (!acted && installs === 1 && path.basename(String(target)) === path.basename(intent) && fs.existsSync(intent) &&
      !fs.existsSync(path.join(root, ".lock")) && !appended()) {
      acted = true;
      const before = fs.readFileSync(intent, "utf8");
      act();
      kept = fs.existsSync(intent) && fs.readFileSync(intent, "utf8") === before;
    }
    return lstat(target, options);
  }) as typeof fs.lstatSync);
  return { installs: () => installs, kept: () => kept };
};

describe("owner-checked intent recovery (P2-2)", () => {
  it("keeps a live intent when a same-key peer process recovers, and publishes once", async () => {
    const mesh = store();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "same-key-live", text: "once" };
    let raced: ReturnType<typeof peer> | undefined;
    const probe = betweenInstallAndAppend(mesh.root, packet.dedupeKey, () => { raced = peer(mesh.root, "publish", packet, { lockTimeoutMs: 100 }); });
    const event = await mesh.publish(packet);
    vi.restoreAllMocks();
    expect(probe.kept()).toBe(true); // The frozen owner's reservation was never deleted.
    expect(raced).toMatchObject({ ok: false, message: "Mesh archive changed during off-lock recovery" });
    expect(probe.installs()).toBe(1); // The owner never had to retry.
    expect(mesh.read()).toEqual([event]);
    expect(peer(mesh.root, "publish", packet).event).toEqual(event);
    expect(mesh.read()).toEqual([event]);
  }, 60_000);

  it("keeps a live intent while an in-process same-key peer waits for its receipt", async () => {
    const mesh = store(), other = new MeshStore(mesh.root, 1024, 100);
    const packet = { topic: "mesh.fsync", from, dedupeKey: "same-key-waiter", text: "once" };
    let waiter: Promise<MeshEvent> | undefined;
    const probe = betweenInstallAndAppend(mesh.root, packet.dedupeKey, () => { waiter = other.publish(packet); });
    const event = await mesh.publish(packet);
    expect(await waiter!).toEqual(event);
    expect(probe.installs()).toBe(1);
    expect(mesh.read()).toEqual([event]);
  });

  it("keeps a live intent when a peer process compacts between install and append, and publishes once", async () => {
    const mesh = store();
    for (let index = 0; index < 6; index++) await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    const before = fs.statSync(live(mesh.root));
    expect(before.size).toBeGreaterThan(compactOptions.maxEventLogBytes);
    const packet = { topic: "mesh.fsync", from, dedupeKey: "compact-live", text: "once" };
    let compacted: ReturnType<typeof peer> | undefined;
    const probe = betweenInstallAndAppend(mesh.root, packet.dedupeKey, () => { compacted = peer(mesh.root, "compact", {}, compactOptions); });
    const event = await mesh.publish(packet);
    vi.restoreAllMocks();
    expect(compacted).toEqual({ ok: true });
    expect(probe.kept()).toBe(true);
    expect(probe.installs()).toBe(1);
    expect(mesh.read({ after: 0 }).filter(entry => entry.dedupeKey === packet.dedupeKey)).toEqual([event]);
    // Pending -> refused, then compacts on the next trigger.
    const compactor = new MeshStore(mesh.root, 1024, 100, compactOptions);
    await compactor.settleCompaction();
    expect(fs.statSync(live(mesh.root)).size).toBeLessThan(compactOptions.maxEventLogBytes);
    expect(await compactor.publish(packet)).toEqual(event);
    expect(compactor.read().filter(entry => entry.dedupeKey === packet.dedupeKey)).toEqual([event]);
  }, 60_000);

  it("recovers only provably abandoned foreign owners: exited pid or over-age", async () => {
    const exited = spawnSync(process.execPath, ["-e", ""]).pid!;
    const owners = [
      { pid: exited, at: Date.now(), abandoned: true },
      { pid: process.ppid, at: Date.now() - 60 * 60_000, abandoned: true },
      { pid: process.ppid, at: Date.now(), abandoned: false },
    ];
    for (const [index, owner] of owners.entries()) {
      const mesh = new MeshStore(store().root, 1024, 100, { lockTimeoutMs: 100 });
      const packet = { topic: "mesh.fsync", from, dedupeKey: `foreign-${index}`, text: "once" };
      const intent = receipt(mesh.root, packet.dedupeKey, ".pending.json");
      fs.mkdirSync(path.dirname(intent), { recursive: true });
      fs.writeFileSync(intent, JSON.stringify({ dedupeKey: packet.dedupeKey, reservedSequence: 1, eventId: "never-appended", liveOffset: 0,
        owner: { pid: owner.pid, token: "foreign", at: owner.at } }));
      if (owner.abandoned) {
        const event = await mesh.publish(packet);
        expect(mesh.read()).toEqual([event]);
        expect(fs.existsSync(intent)).toBe(false);
      } else {
        await expect(mesh.publish(packet)).rejects.toThrow("Mesh archive changed during off-lock recovery");
        expect(fs.existsSync(intent)).toBe(true);
        expect(mesh.read()).toEqual([]);
      }
    }
  });
});

describe("best-effort compaction (P2-1)", () => {
  it("never rejects or delays a committed publish or batch when compaction fails", async () => {
    const mesh = store(true);
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(source).endsWith(".retained")) throw new Error("compaction unavailable");
      rename(source, target);
    });
    const events: MeshEvent[] = [];
    for (let index = 0; index < 8; index++) {
      events.push(await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(400), ...(index % 2 ? { dedupeKey: `failing-${index}` } : {}) }));
    }
    events.push(...await mesh.publishBatch([0, 1, 2].map(index => ({ topic: "mesh.fsync", from, text: `batch ${index}` }))));
    await mesh.settleCompaction();
    expect(warn.mock.calls.some(([message]) => String(message).includes("compaction unavailable"))).toBe(true);
    expect(fs.statSync(live(mesh.root)).size).toBeGreaterThan(2800);
    expect(mesh.read({ after: 0, limit: 100 })).toEqual(events); // Each committed exactly once.
    vi.mocked(fs.renameSync).mockRestore();
    await mesh.settleCompaction();
    expect(fs.statSync(live(mesh.root)).size).toBeLessThanOrEqual(2800);
    expect(mesh.read().at(-1)).toEqual(events.at(-1));
  });

  it("compacts a log under sustained appends from another process in ONE prepared stage", async () => {
    const filler = store();
    for (let index = 0; index < 6; index++) await filler.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    const compactor = new MeshStore(filler.root, 1024, 100, compactOptions);
    const generationBefore = generation(filler.root);
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    const stages = new Set<number>();
    let prepared = 0, appended = 0, heldStageSyncs = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = open(...args);
      if (String(args[0]).endsWith(".retained")) { stages.add(fd); if (args[1] === "wx") prepared++; }
      return fd;
    }) as typeof fs.openSync);
    const raced: MeshEvent[] = [];
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (stages.delete(fd)) {
        if (fs.existsSync(path.join(filler.root, ".lock"))) heldStageSyncs++;
        // Every off-lock stage barrier is overtaken by another process's append.
        else if (appended < 5) { appended++; raced.push(peer(filler.root, "publish", { topic: "mesh.fsync", from, text: `raced ${appended}` }).event!); }
      }
      sync(fd);
    });
    await compactor.settleCompaction();
    vi.restoreAllMocks();
    expect(appended).toBe(5);
    expect(prepared).toBe(1); // Folded, never discarded and re-staged.
    expect(heldStageSyncs).toBeLessThanOrEqual(1); // Only the final remainder after every catch-up round.
    expect(Number(generation(filler.root))).toBe(Number(generationBefore) + 1);
    expect(fs.statSync(live(filler.root)).size).toBeLessThanOrEqual(compactOptions.maxEventLogBytes);
    const lines = fs.readFileSync(live(filler.root), "utf8").trim().split("\n").map(line => JSON.parse(line) as MeshEvent);
    expect(lines.slice(-5)).toEqual(raced);
    expect(lines.every((event, index) => index === 0 || event.sequence === lines[index - 1]!.sequence + 1)).toBe(true);
  }, 90_000);

  it("takes one locked attempt when another process rewrote the log under the snapshot", async () => {
    const filler = store();
    for (let index = 0; index < 6; index++) await filler.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    const compactor = new MeshStore(filler.root, 1024, 100, compactOptions);
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    let rewritten: ReturnType<typeof peer> | undefined;
    const stages = new Set<number>();
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = open(...args);
      if (String(args[0]).endsWith(".retained")) stages.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (stages.delete(fd) && !rewritten) rewritten = peer(filler.root, "compact", {}, compactOptions);
      sync(fd);
    });
    await compactor.settleCompaction();
    vi.restoreAllMocks();
    expect(rewritten).toEqual({ ok: true });
    expect(warn).not.toHaveBeenCalled();
    expect(generation(filler.root)).toBe("1"); // The peer's rewrite stands; ours did not discard it.
    expect(fs.statSync(live(filler.root)).size).toBeLessThanOrEqual(compactOptions.maxEventLogBytes);
    expect(fs.readdirSync(filler.root).filter(name => name.endsWith(".retained"))).toEqual([]);
  }, 60_000);
});

// A real second process appending as fast as it can, until stopped.
const publishLoop = async (root: string) => {
  const stop = path.join(root, "..", path.basename(root) + ".stop");
  const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-publish-loop.mjs"), root, stop], { stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = "";
  child.stdout.on("data", bytes => { stdout += bytes; });
  child.stderr.on("data", bytes => { stderr += bytes; });
  const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
  const lines = () => { try { return fs.readFileSync(live(root), "utf8").split("\n").length - 1; } catch { return 0; } };
  const ready = Date.now();
  while (lines() < 50) {
    if (Date.now() - ready > 20_000 || child.exitCode !== null) throw new Error(`publish loop did not start: ${stderr}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  return {
    lines,
    async stop() {
      fs.writeFileSync(stop, "");
      await closed;
      fs.rmSync(stop, { force: true });
      expect(unexpectedStderr(stderr)).toBe("");
      const result = JSON.parse(stdout.trim().split("\n").at(-1)!) as { ok: boolean; count: number };
      expect(result.ok).toBe(true);
      return result.count;
    },
    kill() { if (child.exitCode === null) child.kill("SIGKILL"); fs.rmSync(stop, { force: true }); },
  };
};
const settled = () => new Promise(resolve => setTimeout(resolve, 50));
/** Slow every barrier of THIS process and record whether THIS process held the mesh lock
 * at each one (the owner record's pid line), so a peer's hold is never counted. */
const slowOwnBarriers = (root: string, ms: number) => {
  const wait = new Int32Array(new SharedArrayBuffer(4));
  const held: boolean[] = [];
  const ownsLock = () => {
    try { return fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1] === String(process.pid); }
    catch { return false; }
  };
  const originals = [fs.fsyncSync.bind(fs), fs.fdatasyncSync.bind(fs)];
  ((["fsyncSync", "fdatasyncSync"] as const)).forEach((name, index) => vi.spyOn(fs, name).mockImplementation(fd => {
    held.push(ownsLock());
    Atomics.wait(wait, 0, 0, ms);
    originals[index]!(fd);
  }));
  return { held };
};

describe("position-independent keyed intents under constant peer appends (round 4)", () => {
  it("runs ZERO fsyncs under the lock for keyed publishes while another process runs a tight publish loop", async () => {
    const mesh = new MeshStore(store().root, 1024, 100, { lockTimeoutMs: 5_000 });
    const other = new MeshStore(mesh.root, 1024, 100, { lockTimeoutMs: 5_000 });
    const loop = await publishLoop(mesh.root);
    try {
      const barriers = slowOwnBarriers(mesh.root, 20);
      const retries = meshPublishCounters.keyedRetries;
      const serial: MeshEvent[] = [];
      for (let index = 0; index < 3; index++) serial.push(await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: `serial-${index}`, text: "once" }));
      const concurrent = await Promise.all([0, 1, 2, 3].map(index => (index % 2 ? other : mesh)
        .publish({ topic: "mesh.fsync", from, dedupeKey: `concurrent-${index}`, text: "once" })));
      const same = await Promise.all([mesh, other].map(store => store.publish({ topic: "mesh.fsync", from, dedupeKey: "same", text: "once" })));
      vi.restoreAllMocks();
      // Every barrier of this process (intent, log, receipt, namespaces) ran outside its holds.
      expect(barriers.held.length).toBeGreaterThan(20);
      expect(barriers.held.filter(Boolean)).toEqual([]);
      // Only the same-key pair may wait on each other; peer appends voided nothing.
      expect(meshPublishCounters.keyedRetries - retries).toBeLessThanOrEqual(50);
      expect(same[0]).toEqual(same[1]);
      await loop.stop();
      const all = fs.readFileSync(live(mesh.root), "utf8").trim().split("\n").map(line => JSON.parse(line) as MeshEvent);
      for (const event of [...serial, ...concurrent, same[0]!]) expect(all.filter(entry => entry.dedupeKey === event.dedupeKey)).toEqual([event]);
      expect(new Set(all.map(entry => entry.id)).size).toBe(all.length);
      expect(all.every((entry, index) => index === 0 || entry.sequence === all[index - 1]!.sequence + 1)).toBe(true);
      expect(fs.readdirSync(path.join(mesh.root, "event-receipts")).filter(name => !/^[a-f0-9]{64}\.json$/.test(name))).toEqual([]);
    } finally { loop.kill(); }
  }, 90_000);

  it("stages a position-independent intent: no sequence, offset, size or mtime", async () => {
    const mesh = store();
    const rename = fs.renameSync.bind(fs);
    let intent: Record<string, unknown> | undefined;
    const target = receipt(mesh.root, "shape", ".pending.json");
    vi.spyOn(fs, "renameSync").mockImplementation((source, to) => {
      if (to === target) intent = JSON.parse(fs.readFileSync(source, "utf8")) as Record<string, unknown>;
      rename(source, to);
    });
    const event = await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "shape", text: "once" });
    expect(Object.keys(intent!).sort()).toEqual(["dedupeKey", "eventId", "owner", "payloadHash", "version"]);
    expect(intent).toMatchObject({ version: 2, dedupeKey: "shape", eventId: event.id, owner: { pid: process.pid } });
  });
});

describe("bounded keyed publish and recovery under constant peer appends (round 3)", () => {
  it("completes a keyed publish within a bounded time while another process runs a tight publish loop", async () => {
    const mesh = new MeshStore(store().root, 1024, 100, { lockTimeoutMs: 3_000 });
    const loop = await publishLoop(mesh.root);
    try {
      const barriers = slowOwnBarriers(mesh.root, 20);
      const retries = meshPublishCounters.keyedRetries;
      const started = Date.now();
      const event = await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "under-load", text: "once" });
      const elapsed = Date.now() - started;
      vi.restoreAllMocks();
      expect(elapsed).toBeLessThan(3_000);
      expect(meshPublishCounters.keyedRetries).toBe(retries); // Peer appends never void the intent.
      expect(barriers.held.length).toBeGreaterThan(0);
      expect(barriers.held).not.toContain(true);
      const before = loop.lines();
      while (loop.lines() < before + 20) await new Promise(resolve => setTimeout(resolve, 5)); // Still appending.
      expect(await loop.stop()).toBeGreaterThan(50);
      const all = fs.readFileSync(live(mesh.root), "utf8").trim().split("\n").map(line => JSON.parse(line) as MeshEvent);
      expect(all.filter(entry => entry.dedupeKey === "under-load")).toEqual([event]);
      expect(all.every((entry, index) => index === 0 || entry.sequence === all[index - 1]!.sequence + 1)).toBe(true);
      expect(fs.existsSync(receipt(mesh.root, "under-load", ".pending.json"))).toBe(false);
      expect(await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "under-load" })).toEqual(event);
    } finally { loop.kill(); }
  }, 60_000);

  it("recovers a crash-left intent (publish and compaction settle) under the same loop", async () => {
    const root = store().root;
    await killAtFence(root, "crashed-publish", "PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND");
    await killAtFence(root, "crashed-compact", "PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND");
    const crashed = new MeshStore(root, 1024, 100).read();
    expect(crashed.map(event => event.dedupeKey)).toEqual(["crashed-publish", "crashed-compact"]);
    const loop = await publishLoop(root);
    try {
      const barriers = slowOwnBarriers(root, 20);
      const mesh = new MeshStore(root, 1024, 100, { lockTimeoutMs: 3_000 });
      let started = Date.now();
      expect(await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "crashed-publish", text: "once" })).toEqual(crashed[0]);
      expect(Date.now() - started).toBeLessThan(3_000);
      expect(fs.existsSync(receipt(root, "crashed-publish", ".pending.json"))).toBe(false);
      const recovery = barriers.held.length;
      expect(recovery).toBeGreaterThan(0);
      expect(barriers.held).not.toContain(true); // Keyed recovery: reads and renames only under the lock.
      // Compaction's off-lock settle matches by key and event id: peer appends cannot void it.
      while (fs.statSync(live(root)).size <= compactOptions.maxEventLogBytes) await new Promise(resolve => setTimeout(resolve, 5));
      started = Date.now();
      await new MeshStore(root, 1024, 100, { ...compactOptions, lockTimeoutMs: 3_000 }).settleCompaction();
      expect(Date.now() - started).toBeLessThan(6_000);
      vi.restoreAllMocks();
      // Only compaction's own final remainder fold under custody (P2-1) may sync while held.
      expect(barriers.held.slice(recovery).filter(Boolean).length).toBeLessThanOrEqual(1);
      expect(fs.existsSync(receipt(root, "crashed-compact", ".pending.json"))).toBe(false);
      expect(JSON.parse(fs.readFileSync(receipt(root, "crashed-compact"), "utf8"))).toEqual(crashed[1]);
      await loop.stop();
      expect(await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "crashed-compact" })).toEqual(crashed[1]);
    } finally { loop.kill(); }
  }, 90_000);
});

describe("gated compaction staging (round 3)", () => {
  const plantLiveIntent = (root: string, key: string) => {
    const intent = receipt(root, key, ".pending.json");
    fs.mkdirSync(path.dirname(intent), { recursive: true });
    fs.writeFileSync(intent, JSON.stringify({ dedupeKey: key, reservedSequence: 1_000_000, eventId: "stalled", liveOffset: 1_000_000,
      owner: { pid: process.ppid, token: "stalled-peer", at: Date.now() } }));
    return intent;
  };

  it("never stages while an intent stays pending, and backs off its settle attempts", async () => {
    const mesh = store(true);
    const intent = plantLiveIntent(mesh.root, "stalled");
    const stages = meshPublishCounters.compactionStages;
    for (let index = 0; index < 10; index++) {
      await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
      await settled();
    }
    await mesh.settleCompaction(); // Explicit: ignores the backoff, still never stages.
    expect(fs.statSync(live(mesh.root)).size).toBeGreaterThan(2800);
    expect(meshPublishCounters.compactionStages - stages).toBe(0);
    expect(fs.existsSync(intent)).toBe(true);
    fs.rmSync(intent);
    await mesh.settleCompaction();
    expect(meshPublishCounters.compactionStages - stages).toBe(1);
    expect(fs.statSync(live(mesh.root)).size).toBeLessThanOrEqual(2800);
  });

  it("backs off after a refused or failed rename instead of re-staging on every publish", async () => {
    const mesh = store(true);
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(source).endsWith(".retained")) throw new Error("compaction unavailable");
      rename(source, target);
    });
    const stages = meshPublishCounters.compactionStages;
    for (let index = 0; index < 10; index++) {
      await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
      await settled();
    }
    expect(meshPublishCounters.compactionStages - stages).toBe(1);
    expect(warn).toHaveBeenCalledTimes(1);
    vi.mocked(fs.renameSync).mockRestore();
    await mesh.settleCompaction();
    expect(fs.statSync(live(mesh.root)).size).toBeLessThanOrEqual(2800);
  });

  it("stages once per mesh root: another process's live claim skips publish-triggered compaction", async () => {
    const mesh = store(true);
    const claim = live(mesh.root) + ".compacting";
    fs.writeFileSync(claim, JSON.stringify({ pid: process.ppid, token: "peer", at: Date.now() }));
    const stages = meshPublishCounters.compactionStages;
    for (let index = 0; index < 6; index++) {
      await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
      await settled();
    }
    expect(meshPublishCounters.compactionStages - stages).toBe(0);
    expect(fs.readFileSync(claim, "utf8")).toContain("peer");
    // A claim whose process has exited is abandoned and replaced.
    const exited = spawnSync(process.execPath, ["-e", ""]).pid!;
    fs.writeFileSync(claim, JSON.stringify({ pid: exited, token: "dead", at: Date.now() }));
    await mesh.publish({ topic: "mesh.fsync", from, text: "x".repeat(500) });
    await settled();
    expect(meshPublishCounters.compactionStages - stages).toBe(1);
    expect(fs.statSync(live(mesh.root)).size).toBeLessThanOrEqual(2800);
    expect(fs.existsSync(claim)).toBe(false);
  });
});
