import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshStore, type MeshPublishInput } from "../src/mesh/store.js";
// @ts-expect-error -- plain .mjs fixture, shared with the golden generator run against main.
import { normalizeLine, publishGoldenEvents } from "./fixtures/mesh-event-golden.mjs";

// smarty-dev#6729 (step 1b of smarty-dev#6477): publishBatch's live-log barrier and its
// no-archive keyed receipts run after \`.lock\` is released, and fixed-data envelopes are encoded
// before it. A batch still resolves only once its events and receipts are durable.

const roots: string[] = [];
const from = { id: "session:batch-hold", name: "batch", kind: "main" as const };
const scratch = (prefix = "mesh-batch-hold-") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  return root;
};
const store = () => new MeshStore(scratch(), 64 * 1024, 100);
const archived = () => {
  const root = scratch();
  const dir = path.join(root, "event-archive");
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  return new MeshStore(root, 64 * 1024, 100);
};
const events = (root: string) => path.join(root, "events.jsonl");
const receipt = (root: string, key: string, suffix = ".json") =>
  path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const held = (root: string) => fs.existsSync(path.join(root, ".lock"));
const sameFile = (fd: number, file: string): boolean => {
  try {
    const opened = fs.fstatSync(fd), named = fs.statSync(file);
    return opened.isFile() && opened.dev === named.dev && opened.ino === named.ino;
  } catch { return false; }
};
const fdPath = (fd: number): string => { try { return fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return ""; } };
const mixed = (prefix: string): MeshPublishInput[] => [
  { topic: "mesh.batch", from, text: prefix + "-a", data: { n: 1 } },
  { topic: "mesh.batch", from, text: prefix + "-b", dedupeKey: prefix + "-k1" },
  { topic: "mesh.batch", from, text: prefix + "-c" },
  { topic: "mesh.batch", from, text: prefix + "-d", dedupeKey: prefix + "-k2", data: { n: 2 } },
  { topic: "mesh.batch", from, text: prefix + "-e", dedupeKey: prefix + "-k3" },
];

// Deterministic batch extent: on a slow disk the 50 ms work bound would split a batch.
const unbounded = () => vi.spyOn(performance, "now").mockReturnValue(0);

afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("publishBatch hold (smarty-dev#6729)", () => {
  it("writes byte-identical event lines to main's encoder (golden)", async () => {
    const golden = JSON.parse(fs.readFileSync(path.resolve("tests/fixtures/mesh-event-golden.json"), "utf8")) as string[];
    const lines = await publishGoldenEvents({ MeshStore, StoreBridgeSide }, scratch()) as string[];
    expect(lines.map(normalizeLine)).toEqual(golden);
    // The stitched envelope is exactly JSON.stringify of the event, field order included.
    for (const line of lines) expect(JSON.stringify(JSON.parse(line))).toBe(line);
  });

  it("resolves only after its after-release barrier; nothing on the live log or a receipt is synced under the lock", async () => {
    const mesh = store();
    unbounded();
    let resolved = false;
    const syncs: Array<{ held: boolean; live: boolean; resolved: boolean; file: string }> = [];
    let image = "";
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const live = sameFile(fd, events(mesh.root));
      syncs.push({ held: held(mesh.root), live, resolved, file: fdPath(fd) });
      sync(fd);
      if (live) image = fs.readFileSync(events(mesh.root), "utf8");
    });
    const published = await mesh.publishBatch(mixed("barrier")).then(result => { resolved = true; return result; });
    expect(published).toHaveLength(5);
    const live = syncs.filter(entry => entry.live);
    expect(live.length).toBeGreaterThan(0);
    expect(live.every(entry => !entry.held && !entry.resolved)).toBe(true);
    // Every event is in the image the barrier made durable before the batch resolved.
    for (const event of published) expect(image).toContain(`"id":"${event.id}"`);
    if (process.platform === "linux") {
      // Under the lock only each keyed event's intent fence (its file and namespace chain).
      expect(syncs.filter(entry => entry.held && /[a-f0-9]{64}\.json\.\d+\..+\.tmp$/.test(entry.file))).toEqual([]);
      expect(syncs.filter(entry => entry.held && entry.file.endsWith("/events.jsonl"))).toEqual([]);
      expect(syncs.some(entry => entry.held && /\.pending\.json\.\d+\..+\.tmp$/.test(entry.file))).toBe(true);
    }
  });

  it("writes keyed receipts after release, in commit order, and dedupes a key repeated in one batch", async () => {
    const mesh = store();
    unbounded();
    const order: string[] = [];
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      const name = path.basename(String(target));
      if (String(target).includes("event-receipts")) order.push(`${held(mesh.root) ? "held" : "released"}:${name}`);
      return rename(source, target);
    });
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) order.push(held(mesh.root) ? "held:barrier" : "released:barrier");
      sync(fd);
    });
    const inputs = [...mixed("order"), { topic: "mesh.batch", from, text: "order-repeat", dedupeKey: "order-k1" }];
    const published = await mesh.publishBatch(inputs);
    const hash = (key: string, suffix: string) => createHash("sha256").update(key).digest("hex") + suffix;
    // Intents are the crash fence under the lock; the barrier, then the receipts, follow release.
    expect(order).toEqual([
      `held:${hash("order-k1", ".pending.json")}`, `held:${hash("order-k2", ".pending.json")}`, `held:${hash("order-k3", ".pending.json")}`,
      "released:barrier",
      `released:${hash("order-k1", ".json")}`, `released:${hash("order-k2", ".json")}`, `released:${hash("order-k3", ".json")}`, `released:${hash("order-k1", ".json")}`,
    ]);
    // The repeated key returns the first event and appends nothing.
    expect(published).toHaveLength(6);
    expect(published[5]).toEqual(published[1]);
    expect(mesh.read({ limit: 100 })).toEqual(published.slice(0, 5));
    for (const [index, key] of [[1, "order-k1"], [3, "order-k2"], [4, "order-k3"]] as const) {
      expect(JSON.parse(fs.readFileSync(receipt(mesh.root, key), "utf8"))).toEqual(published[index]);
      expect(fs.existsSync(receipt(mesh.root, key, ".pending.json"))).toBe(false);
    }
    expect(await mesh.publishBatch([inputs[1]!])).toEqual([published[1]]);
  });

  it("keeps the archive-coupled receipt protocol under the lock (smarty-dev#6000)", async () => {
    const mesh = archived();
    unbounded();
    const receipts: boolean[] = [];
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (/event-receipts\/[a-f0-9]{64}\.json$/.test(String(target))) receipts.push(held(mesh.root));
      return rename(source, target);
    });
    const published = await mesh.publishBatch(mixed("archived"));
    expect(receipts).toEqual([true, true, true]);
    expect(mesh.read({ limit: 100 })).toEqual(published);
  });

  it("a failed barrier rejects, never re-appends, and leaves keyed intents that recover exactly once", async () => {
    const mesh = store();
    unbounded();
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
      sync(fd);
    });
    const inputs = mixed("failed");
    await expect(mesh.publishBatch(inputs)).rejects.toThrow("live barrier failed");
    fail.mockRestore();
    const committed = mesh.read({ limit: 100 });
    expect(committed.map(event => event.text)).toEqual(inputs.map(input => input.text));
    expect(fs.existsSync(receipt(mesh.root, "failed-k1"))).toBe(false);
    expect(fs.existsSync(receipt(mesh.root, "failed-k1", ".pending.json"))).toBe(true);
    // A same-key retry settles the stranded intents to the committed events.
    const retried = await mesh.publishBatch(inputs.filter(input => input.dedupeKey));
    expect(retried).toEqual(committed.filter(event => event.dedupeKey));
    expect(mesh.read({ limit: 100 })).toEqual(committed);
    expect(fs.readdirSync(path.join(mesh.root, "event-receipts")).filter(name => name.includes(".pending."))).toEqual([]);
  });

  describe.skipIf(process.platform === "win32")("process death after release, before the barrier", () => {
    const crashBatch = async (root: string, inputs: MeshPublishInput[]) => {
      const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-batch-crash.mjs"), root, JSON.stringify(inputs)], {
        env: { ...process.env, PI_FABRIC_TEST_CRASH_BEFORE_BATCH_BARRIER: "1" }, stdio: ["ignore", "ignore", "pipe"],
      });
      let stderr = "";
      child.stderr.on("data", bytes => { stderr += bytes; });
      const signal = await new Promise<NodeJS.Signals | null>(resolve => child.once("close", (_code, signal) => resolve(signal)));
      expect(signal, stderr).toBe("SIGKILL");
    };

    it("no archive: the lock is released, the intents stand, and same-key retries recover each event exactly once", async () => {
      const mesh = store();
      unbounded();
      const inputs = mixed("crash");
      await crashBatch(mesh.root, inputs);
      expect(held(mesh.root)).toBe(false);
      const committed = mesh.read({ limit: 100 });
      expect(committed.map(event => event.text)).toEqual(inputs.map(input => input.text));
      // The state a death after a keyed live append leaves (as PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND).
      for (const key of ["crash-k1", "crash-k2", "crash-k3"]) {
        expect(fs.existsSync(receipt(mesh.root, key))).toBe(false);
        expect(fs.existsSync(receipt(mesh.root, key, ".pending.json"))).toBe(true);
      }
      const keyed = inputs.filter(input => input.dedupeKey);
      const recovered = await mesh.publishBatch(keyed);
      expect(recovered).toEqual(committed.filter(event => event.dedupeKey));
      expect(await mesh.publish(keyed[1]!)).toEqual(recovered[1]);
      expect(mesh.read({ limit: 100 })).toEqual(committed);
      for (const [index, key] of ["crash-k1", "crash-k2", "crash-k3"].entries()) {
        expect(JSON.parse(fs.readFileSync(receipt(mesh.root, key), "utf8"))).toEqual(recovered[index]);
        expect(fs.existsSync(receipt(mesh.root, key, ".pending.json"))).toBe(false);
      }
    }, 30_000);

    it("archive-coupled: receipts were installed under the lock, so a retry returns them unchanged", async () => {
      const mesh = archived();
      unbounded();
      const inputs = mixed("crash-archive");
      await crashBatch(mesh.root, inputs);
      expect(held(mesh.root)).toBe(false);
      const committed = mesh.read({ limit: 100 });
      expect(committed.map(event => event.text)).toEqual(inputs.map(input => input.text));
      const keyed = committed.filter(event => event.dedupeKey);
      for (const event of keyed) {
        expect(JSON.parse(fs.readFileSync(receipt(mesh.root, event.dedupeKey!), "utf8"))).toEqual(event);
        expect(fs.existsSync(receipt(mesh.root, event.dedupeKey!, ".pending.json"))).toBe(false);
      }
      expect(await mesh.publishBatch(inputs.filter(input => input.dedupeKey))).toEqual(keyed);
      expect(mesh.read({ limit: 100 })).toEqual(committed);
    }, 30_000);
  });
});
