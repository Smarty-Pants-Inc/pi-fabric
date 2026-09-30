import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentDeliveryRequest, FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";

const owned: Array<{ root: string; clients: ResidencyClient[] }> = [];
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const until = async (predicate: () => boolean) => {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("delivery watcher deadline exceeded");
    await delay(10);
  }
};

function harness() {
  fs.mkdirSync(path.resolve(".local"), { recursive: true });
  const root = fs.mkdtempSync(path.resolve(".local/residency-delivery-watch-"));
  const rootId = `session:${path.basename(root)}`;
  const mesh = new MeshStore(path.join(root, "mesh"), 1_048_576, 1_000);
  const config = {
    format: RESIDENT_HOST_FORMAT, rootId, cwd: root, meshRoot: mesh.root,
    residencyRoot: path.join(root, "resident"),
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 250 },
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete: true },
  } as ResidentHostConfig;
  const delivered: string[] = [];
  const admitted = new Set<string>();
  // The real Main uses its journal to admit a stable delivery id once. Keep
  // that receiver contract here, without starting a Pi or a resident host.
  const main = {
    id: rootId, local: true,
    deliverAgent: vi.fn((request: FabricMainAgentDeliveryRequest) => {
      const id = request.deliveryId ?? request.message;
      if (!admitted.has(id)) {
        admitted.add(id);
        delivered.push(request.message);
      }
      return { queued: true, messageId: id, routed: "main" };
    }),
  } as unknown as FabricMainAgentTarget;
  const clients: ResidencyClient[] = [];
  owned.push({ root, clients });
  const client = (store = mesh, target = main) => {
    const instance = new ResidencyClient({ config, mesh: store, mainAgent: target, participants: {} as never });
    clients.push(instance);
    return instance;
  };
  const put = async (id: string) => {
    await mesh.put({
      key: `${residentDeliveryPrefix(rootId)}${id}`, ifVersion: 0,
      identity: { id: residentHostId(rootId), name: "resident", kind: "main" },
      value: { format: RESIDENT_HOST_FORMAT, rootId, id, message: id,
        from: { id: "actor", name: "actor", kind: "actor" },
        delivery: "followUp", triggerTurn: true, createdAt: Date.now() },
    });
  };
  const pending = () => mesh.listAll(residentDeliveryPrefix(rootId), { fresh: true }).length;
  return { root, rootId, mesh, config, main, delivered, client, put, pending };
}

function fakeWatch() {
  const watches: Array<EventEmitter & { close: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> }> = [];
  const callbacks: Array<(event: string, name: string | null) => void> = [];
  vi.spyOn(fs, "watch").mockImplementation(((_root: string, callback: (event: string, name: string | null) => void) => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() });
    watcher.close.mockImplementation(() => watcher.emit("close"));
    watches.push(watcher);
    callbacks.push(callback);
    return watcher;
  }) as unknown as typeof fs.watch);
  return { watches, notify: (index = 0, name: string | null = "events.jsonl") => callbacks[index]!("change", name) };
}

afterEach(async () => {
  // Restore real timers before close's bounded in-flight wait.
  vi.useRealTimers();
  for (const item of owned.splice(0)) {
    await Promise.all(item.clients.map((client) => client.close()));
    fs.rmSync(item.root, { recursive: true, force: true });
  }
  vi.restoreAllMocks();
});

describe("resident delivery wake scheduling", () => {
  it("drains startup work immediately and does not poll an idle mesh every 250ms", async () => {
    const h = harness();
    await h.put("startup");
    const scans = vi.spyOn(h.mesh, "listAll");
    const client = h.client();
    client.start();
    client.start();
    await until(() => h.delivered.length === 1);
    await delay(80); // absorb the drain's own state replacement notification
    const idleScans = scans.mock.calls.length;
    await delay(800);
    expect(scans.mock.calls.length).toBe(idleScans);
    expect(h.delivered).toEqual(["startup"]);
  });

  it("wakes on a real append, then create/rename/replacement of events.jsonl", async () => {
    const h = harness();
    h.client().start();
    await delay(30);
    const events = path.join(h.mesh.root, "events.jsonl");
    const scans = vi.spyOn(h.mesh, "listAll");
    for (const change of [
      () => fs.appendFileSync(events, "{}\n"),
      () => { fs.unlinkSync(events); fs.writeFileSync(events, "{}\n"); },
      () => { const replacement = `${events}.replacement`; fs.writeFileSync(replacement, "{}\n"); fs.renameSync(replacement, events); },
      () => fs.appendFileSync(events, "{}\n"),
    ]) {
      const before = scans.mock.calls.length;
      change();
      await until(() => scans.mock.calls.length > before);
    }
  });

  it("a real event append wakes an already pending envelope promptly", async () => {
    const h = harness();
    await h.put("append-pending");
    const list = h.mesh.listAll.bind(h.mesh);
    let visible = false;
    vi.spyOn(h.mesh, "listAll").mockImplementation((prefix, options) => visible ? list(prefix, options) : []);
    h.client().start();
    await delay(30);
    expect(h.delivered).toEqual([]);
    visible = true;
    const started = Date.now();
    fs.appendFileSync(path.join(h.mesh.root, "events.jsonl"), "{}\n");
    await until(() => h.delivered.length === 1);
    expect(Date.now() - started).toBeLessThan(200);
    expect(h.delivered).toEqual(["append-pending"]);
  });

  it("delivers promptly on actual host-style state publication without an event append", async () => {
    const h = harness();
    // A separate writer exercises the reader cache, not just its own cached put.
    const reader = new MeshStore(h.mesh.root, 1_048_576, 1_000, { readCacheMs: 10_000 });
    h.client(reader).start();
    await delay(30);
    const started = Date.now();
    await h.put("host-put");
    await until(() => h.delivered.length === 1);
    expect(Date.now() - started).toBeLessThan(200);
    expect(fs.existsSync(path.join(h.mesh.root, "events.jsonl"))).toBe(false);
    expect(h.pending()).toBe(0);
  });

  it("does not lose a notification during an awaited delete", async () => {
    const h = harness();
    const watch = fakeWatch();
    await h.put("first");
    const original = h.mesh.delete.bind(h.mesh);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    vi.spyOn(h.mesh, "delete").mockImplementationOnce(async (input) => { await blocked; return original(input); });
    h.client().start();
    await until(() => h.delivered.length === 1);
    await h.put("while-busy");
    watch.notify();
    release();
    await until(() => h.delivered.length === 2);
    expect(h.delivered).toEqual(["first", "while-busy"]);
  });

  it("retains throwing receiver work, bounds burst retries, then recovers", async () => {
    const h = harness();
    const watch = fakeWatch();
    await h.put("retry");
    let full = true;
    const normal = vi.mocked(h.main.deliverAgent).getMockImplementation()!;
    vi.mocked(h.main.deliverAgent).mockImplementation((input) => {
      if (full) throw new Error("receiver full");
      return normal(input);
    });
    vi.useFakeTimers();
    h.client().start();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.pending()).toBe(1);
    for (let i = 0; i < 100; i++) watch.notify();
    await vi.advanceTimersByTimeAsync(900);
    expect(h.main.deliverAgent).toHaveBeenCalledTimes(1);
    full = false;
    await vi.advanceTimersByTimeAsync(100);
    expect(h.pending()).toBe(0);
    expect(h.delivered).toEqual(["retry"]);
  });

  it("reconciles and reinstalls a watcher after installation and runtime failures", async () => {
    const h = harness();
    const watch = fakeWatch();
    vi.mocked(fs.watch).mockImplementationOnce(() => { throw new Error("EMFILE"); });
    vi.useFakeTimers();
    h.client().start();
    await vi.advanceTimersByTimeAsync(0);
    await h.put("unwatched");
    await vi.advanceTimersByTimeAsync(29_999);
    expect(h.delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["unwatched"]);
    watch.watches[0]!.emit("error", new Error("watch failed"));
    expect(watch.watches[0]!.close).toHaveBeenCalledTimes(1);
    await h.put("after-error");
    await vi.advanceTimersByTimeAsync(30_000);
    expect(h.delivered).toEqual(["unwatched", "after-error"]);
    expect(watch.watches).toHaveLength(2);
  });

  it("duplicate watches and a stale second client do not double-admit a delivery", async () => {
    const h = harness();
    const watch = fakeWatch();
    await h.put("once");
    const stale = h.mesh.listAll(residentDeliveryPrefix(h.rootId));
    const secondStore = new MeshStore(h.mesh.root, 1_048_576, 1_000);
    vi.spyOn(secondStore, "listAll").mockReturnValue(stale);
    h.client().start();
    await until(() => h.pending() === 0);
    h.client(secondStore).start();
    for (let i = 0; i < 20; i++) { watch.notify(0); watch.notify(1); }
    await delay(50);
    expect(h.delivered).toEqual(["once"]);
  });

  it("close fences an in-flight snapshot and leaves no watcher or timer", async () => {
    const h = harness();
    const watch = fakeWatch();
    await h.put("a");
    await h.put("b");
    const original = h.mesh.delete.bind(h.mesh);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    vi.spyOn(h.mesh, "delete").mockImplementationOnce(async (input) => { await blocked; return original(input); });
    const client = h.client();
    client.start();
    await until(() => h.delivered.length === 1);
    const closing = client.close();
    watch.notify();
    release();
    await closing;
    client.start();
    watch.notify();
    await delay(40);
    expect(h.delivered).toEqual(["a"]);
    expect(watch.watches[0]!.close).toHaveBeenCalledTimes(1);
    const scans = vi.spyOn(h.mesh, "listAll");
    vi.useFakeTimers();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(scans).not.toHaveBeenCalled();
  });

  it("preserves processwide root+id admission across separate receivers after a failed delete", async () => {
    const h = harness();
    fakeWatch();
    await h.put("processwide-once");
    vi.spyOn(h.mesh, "delete").mockImplementationOnce(async () => { throw new Error("delete failed"); });
    const first = h.client();
    first.start();
    await until(() => h.delivered.length === 1);
    await first.close();
    const secondDelivery = vi.fn(() => ({ queued: true, messageId: "second", routed: "main" as const }));
    h.client(h.mesh, { ...h.main, deliverAgent: secondDelivery }).start();
    await until(() => h.pending() === 0);
    expect(secondDelivery).not.toHaveBeenCalled();
  });

});
