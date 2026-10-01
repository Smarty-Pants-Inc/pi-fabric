import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";

// Structural test seam while the separately owned public method is landing.
// No prototype/private access: an absent method is modeled on this instance only.
type WatchStore = MeshStore & { namespaceWatchHint?: (namespace: string) => string | undefined };
const owned: Array<{ root: string; client: ResidencyClient }> = [];
const emptyDigest = createHash("sha256").digest("base64");

async function harness() {
  fs.mkdirSync(path.resolve(".local"), { recursive: true });
  const root = fs.mkdtempSync(path.resolve(".local/residency-namespace-watch-"));
  const rootId = `session:${path.basename(root)}`;
  const writer = new MeshStore(path.join(root, "mesh"), 1_048_576, 1_000);
  await writer.put({ key: "unrelated/seed/value", value: 0, identity: { id: rootId, name: "main", kind: "main" } });
  const reader: WatchStore = new MeshStore(writer.root, 1_048_576, 1_000, { readCacheMs: 60_000 });
  const prefix = residentDeliveryPrefix(rootId);
  const config = {
    format: RESIDENT_HOST_FORMAT, rootId, cwd: root, meshRoot: writer.root,
    residencyRoot: path.join(root, "resident"), actorRoot: path.join(root, "actors"), sessionId: "test",
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 250 },
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete: true },
  } as ResidentHostConfig;
  let invalid = false;
  const productionHint = reader.namespaceWatchHint?.bind(reader);
  const hint = vi.fn((namespace: string): string | undefined => {
    if (invalid) return undefined;
    if (productionHint) return productionHint(namespace);
    // Bounded model of CURRENT publication, never a cached-reader digest.
    const file = path.join(writer.root, "state.read-signal.json");
    if (!fs.existsSync(file) || fs.statSync(file).size > 256_000) return undefined;
    try {
      const signal = JSON.parse(fs.readFileSync(file, "utf8")) as { namespaces: Record<string, string> };
      return signal.namespaces[namespace] ?? emptyDigest;
    } catch { return undefined; }
  });
  Object.defineProperty(reader, "namespaceWatchHint", { value: hint, configurable: true });
  const delivered: string[] = [];
  const receipts = new Set<string>();
  const deliverAgent = vi.fn((request: { message: string; deliveryId?: string }) => {
    const id = request.deliveryId ?? request.message;
    if (!receipts.has(id)) { receipts.add(id); delivered.push(request.message); }
    return { queued: true, messageId: id, routed: "main" };
  });
  const main = { id: rootId, local: true, deliverAgent } as unknown as FabricMainAgentTarget;
  const callbacks: Array<(event: string, name: string | null) => void> = [];
  const watches: Array<EventEmitter & { close: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> }> = [];
  vi.spyOn(fs, "watch").mockImplementation(((_root: string, callback: (event: string, name: string | null) => void) => {
    callbacks.push(callback);
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() });
    watcher.close.mockImplementation(() => watcher.emit("close"));
    watches.push(watcher);
    return watcher;
  }) as unknown as typeof fs.watch);
  const scans = vi.spyOn(reader, "listAll");
  const client = new ResidencyClient({ config, mesh: reader, mainAgent: main, participants: {} as never });
  owned.push({ root, client });
  const put = (id: string, message = id, ifVersion = 0, authenticated = true) => writer.put({
    key: `${prefix}${id}`, ifVersion,
    identity: { id: authenticated ? residentHostId(rootId) : "intruder", name: "resident", kind: "main" },
    value: { format: RESIDENT_HOST_FORMAT, rootId, id, message,
      from: { id: "actor", name: "actor", kind: "actor" }, delivery: "followUp", triggerTurn: true, createdAt: Date.now() },
  });
  const notify = (name: string | null = "state.json") => callbacks.at(-1)!("change", name);
  const freshCount = () => scans.mock.calls.filter(([p, options]) => p === prefix && options?.fresh === true).length;
  const start = async () => { vi.useFakeTimers(); client.start(); await vi.advanceTimersByTimeAsync(0); };
  return { root, writer, reader, prefix, client, hint, scans, delivered, deliverAgent, watches, put, notify, freshCount, start,
    invalidate: () => { invalid = true; } };
}

afterEach(async () => {
  // close can await an in-flight delete using a real bounded timer.
  vi.useRealTimers();
  for (const { root, client } of owned.splice(0)) {
    await client.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
  vi.restoreAllMocks();
});

describe("resident namespace watch hints", () => {
  it("always takes a fresh startup snapshot, then skips unchanged valid bursts even with a warm cache", async () => {
    const h = await harness();
    h.reader.listAll(h.prefix); // deliberately warm the reader before start
    await h.start();
    expect(h.freshCount()).toBe(1);
    const before = h.freshCount();
    for (let burst = 0; burst < 4; burst++) {
      for (let i = 0; i < 50; i++) h.notify(i % 2 ? "state.json" : "events.jsonl");
      await vi.advanceTimersByTimeAsync(10);
    }
    expect(h.freshCount()).toBe(before);
    expect(h.hint).toHaveBeenCalledWith("residency/deliveries/");
    expect(h.hint.mock.calls.every(([namespace]) => namespace === "residency/deliveries/")).toBe(true);
  });

  it("a changed namespace hint promptly freshly reads a state-only publication", async () => {
    const h = await harness();
    await h.start();
    const before = h.freshCount();
    await h.put("new");
    expect(fs.existsSync(path.join(h.writer.root, "events.jsonl"))).toBe(false);
    h.notify();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.freshCount()).toBeGreaterThan(before);
    expect(h.delivered).toEqual(["new"]);
    expect(h.deliverAgent).toHaveBeenCalledWith(expect.objectContaining({ verification: "mesh", deliveryId: expect.stringContaining(":new") }));
  });

  it.each(["missing", "malformed", "stale"])("%s signal after an identical marker forces a fresh fallback", async (kind) => {
    const h = await harness();
    await h.start();
    const signal = path.join(h.writer.root, "state.read-signal.json");
    const old = fs.readFileSync(signal, "utf8");
    h.notify();
    await vi.advanceTimersByTimeAsync(0);
    const before = h.freshCount();
    if (kind === "missing") fs.unlinkSync(signal);
    if (kind === "malformed") fs.writeFileSync(signal, "{broken");
    if (kind === "stale") {
      await h.writer.put({ key: "unrelated/seed/value", value: 1, identity: { id: "main", name: "main", kind: "main" } });
      fs.writeFileSync(signal, old);
      // Model binding failure only until the production API is available.
      if (!(MeshStore.prototype as WatchStore).namespaceWatchHint) h.invalidate();
    }
    h.notify();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.freshCount()).toBeGreaterThan(before);
  });

  it("unknown hint is never an equality-based skip", async () => {
    const h = await harness();
    h.invalidate();
    await h.start();
    const before = h.freshCount();
    for (let i = 0; i < 3; i++) { h.notify(null); await vi.advanceTimersByTimeAsync(1); }
    expect(h.freshCount()).toBeGreaterThan(before);
  });

  it("a throwing public hint observer falls back freshly without losing delivery", async () => {
    const h = await harness();
    await h.start();
    h.hint.mockImplementation(() => { throw new Error("signal unavailable"); });
    await h.put("observer-error");
    h.notify();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.delivered).toEqual(["observer-error"]);
  });

  it("unrelated watch storms cannot move the absolute 30s reconciliation boundary", async () => {
    const h = await harness();
    await h.start();
    const initial = h.freshCount();
    // No notification for this new delivery: force the timer to find it.
    for (let i = 0; i < 29; i++) { h.notify(); await vi.advanceTimersByTimeAsync(1_000); }
    expect(h.freshCount()).toBe(initial);
    await h.put("reconcile");
    await vi.advanceTimersByTimeAsync(999);
    expect(h.delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["reconcile"]);
  });

  it("receiver errors keep a 1s floor despite unchanged hints and burst notifications, and retry freshly", async () => {
    const h = await harness();
    await h.put("retry");
    h.deliverAgent.mockImplementationOnce(() => { throw new Error("receiver full"); });
    await h.start();
    const before = h.freshCount();
    for (let i = 0; i < 9; i++) { h.notify(); await vi.advanceTimersByTimeAsync(100); }
    expect(h.deliverAgent).toHaveBeenCalledTimes(1);
    expect(h.freshCount()).toBe(before);
    await vi.advanceTimersByTimeAsync(100);
    expect(h.freshCount()).toBeGreaterThan(before);
    expect(h.delivered).toEqual(["retry"]);
  });

  it("retained unauthorized envelopes retry freshly without shortening the same 1s floor", async () => {
    const h = await harness();
    // A validly shaped but unauthenticated record must remain, never reach Main.
    await h.put("unauthorized", "unauthorized", 0, false);
    await h.start();
    const before = h.freshCount();
    for (let i = 0; i < 9; i++) { h.notify(); await vi.advanceTimersByTimeAsync(100); }
    expect(h.freshCount()).toBe(before);
    expect(h.deliverAgent).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(100);
    expect(h.freshCount()).toBeGreaterThan(before);
    expect(h.deliverAgent).not.toHaveBeenCalled();
  });

  it("does not capture a newer post-delete hint as the baseline and lose new same-key work", async () => {
    const h = await harness();
    await h.put("same", "first");
    const originalDelete = h.reader.delete.bind(h.reader);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    let deletedVersion: number | undefined;
    const deletion = vi.spyOn(h.reader, "delete").mockImplementationOnce(async (input) => {
      const result = await originalDelete(input);
      deletedVersion = result.version;
      // Pause acknowledgement after the conditional commit: recreating this
      // key is legitimate, not an ifVersion conflict / receiver retry.
      await blocked;
      return result;
    });
    await h.start();
    expect(h.delivered).toEqual(["first"]);
    const firstVersion = deletion.mock.calls[0]![0].ifVersion!;
    try {
      expect(h.writer.listAll(h.prefix, { fresh: true })).toEqual([]);
      expect(deletedVersion).toBeTypeOf("number");
      await h.put("same", "replacement", deletedVersion);
      await h.put("while-busy");
      h.notify();
    } finally { release(); }
    // The awaited deletion's continuation installs a zero-delay follow-up;
    // advancing one tick lets that newly installed timer run as well.
    await vi.advanceTimersByTimeAsync(1);
    // Receipt replay of same id is not a second admission, but must still be
    // offered to the receiver and conditionally deleted at its newer version.
    expect(h.deliverAgent.mock.calls.map(([request]) => request.message)).toContain("replacement");
    expect(h.delivered).toEqual(["first", "while-busy"]);
    expect(deletion.mock.calls[0]![0]).toEqual({ key: `${h.prefix}same`, ifVersion: firstVersion });
    expect(deletion.mock.calls.some(([input]) => input.key === `${h.prefix}same` && input.ifVersion !== firstVersion)).toBe(true);
  });

  it.each(["installation", "runtime"])("%s watch failure retains finite fresh reconciliation", async (failure) => {
    const h = await harness();
    if (failure === "installation") vi.mocked(fs.watch).mockImplementationOnce(() => { throw new Error("EMFILE"); });
    await h.start();
    if (failure === "runtime") h.watches[0]!.emit("error", new Error("watch failed"));
    await h.put("fallback");
    await vi.advanceTimersByTimeAsync(29_999);
    expect(h.delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["fallback"]);
    expect(h.watches.length).toBeGreaterThan(0);
  });

  it("close removes every timer and watcher and fences future notifications/start", async () => {
    const h = await harness();
    await h.start();
    await h.client.close();
    expect(vi.getTimerCount()).toBe(0);
    expect(h.watches.every((watcher) => watcher.close.mock.calls.length === 1)).toBe(true);
    const before = h.freshCount();
    h.notify();
    h.client.start();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(h.freshCount()).toBe(before);
    expect(vi.getTimerCount()).toBe(0);
  });
});
