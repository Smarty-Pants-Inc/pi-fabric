import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
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

  it("preserves Main's durable delivery receipt across new controllers after a failed delete", async () => {
    const h = harness();
    fakeWatch();
    await h.put("processwide-once");
    const key = `${residentDeliveryPrefix(h.rootId)}processwide-once`;
    const version = h.mesh.listAll(residentDeliveryPrefix(h.rootId), { fresh: true })[0]!.version;
    const deliveryId = `resident:${h.rootId}:processwide-once`;
    const journal = path.join(h.root, "main-followups", "root.json");
    const sessionFile = path.join(h.root, "session.jsonl");
    const entries: unknown[] = [];
    const context = {
      isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false },
      sessionManager: { getEntries: () => entries, getSessionFile: () => sessionFile },
    } as unknown as ExtensionContext;
    // Same fake ExtensionAPI/boundary seam as main-agent-delivery-id.test.ts;
    // controllers, journal writes and session receipt barriers are production.
    const fakePi = () => {
      type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
      const handlers = new Map<string, Handler[]>();
      const sendMessage = vi.fn();
      const pi = {
        on: (name: string, handler: Handler) => {
          handlers.set(name, [...(handlers.get(name) ?? []), handler]);
          return () => handlers.set(name, (handlers.get(name) ?? []).filter((item) => item !== handler));
        },
        sendMessage, sendUserMessage: vi.fn(), getThinkingLevel: () => "off",
      } as unknown as ExtensionAPI;
      return { pi, sendMessage, emit: (name: string, event: unknown) => {
        for (const handler of handlers.get(name) ?? []) handler(event, context);
      } };
    };
    const firstPi = fakePi();
    const firstMain = new MainAgentController(firstPi.pi, h.rootId, true, h.root, "root");
    const secondPi = fakePi();
    const secondMain = new MainAgentController(secondPi.pi, h.rootId, true, h.root, "root");
    const first = h.client(h.mesh, firstMain);
    const second = h.client(h.mesh, secondMain);
    let journaledBeforeDelete = false;
    const deletion = vi.spyOn(h.mesh, "delete").mockImplementationOnce(async () => {
      // Observe inside deletion, assert outside: the client's retry isolation
      // intentionally catches errors here, including any thrown assertions.
      journaledBeforeDelete = fs.readFileSync(journal, "utf8").includes(deliveryId);
      throw new Error("delete failed");
    });
    try {
      firstMain.attachFollowUpDrain(context, 60_000, journal);
      first.start();
      await until(() => deletion.mock.calls.length === 1);
      expect(journaledBeforeDelete).toBe(true);
      expect(deletion).toHaveBeenNthCalledWith(1, { key, ifVersion: version });
      expect(firstPi.sendMessage).toHaveBeenCalledTimes(1);
      expect(h.pending()).toBe(1);
      await first.close();
      const message = firstPi.sendMessage.mock.calls[0]![0] as { details: { deliveryId: string } };
      expect(message.details.deliveryId).toBe(deliveryId);
      expect(message).toMatchObject({ content: expect.stringContaining("processwide-once") });
      const persisted = { type: "custom_message", customType: "pi-fabric-agent-message", details: message.details };
      entries.push(persisted);
      fs.writeFileSync(sessionFile, `${JSON.stringify(persisted)}\n`);
      firstPi.emit("agent_settled", { outcome: "completed" });
      expect(fs.existsSync(journal)).toBe(false);
      expect(fs.readFileSync(`${journal}.delivered`, "utf8")).toContain(deliveryId);
      firstMain.closeFollowUpDrain();
      // The new controller cannot rely on the old controller's in-memory
      // entries: its shared journal/receipt files remain authoritative.
      entries.length = 0;

      secondMain.attachFollowUpDrain(context, 60_000, journal);
      const secondDelivery = vi.spyOn(secondMain, "deliverAgent");
      second.start();
      await until(() => h.pending() === 0);
      // The controller MUST receive the replay and consult its durable receipt;
      // obsolete client-global suppression would fail this assertion.
      expect(secondDelivery).toHaveBeenCalledTimes(1);
      expect(secondDelivery).toHaveBeenCalledWith(expect.objectContaining({ deliveryId }));
      expect(secondDelivery.mock.results[0]!.value).toMatchObject({ queued: true, duplicate: true });
      expect(deletion).toHaveBeenNthCalledWith(2, { key, ifVersion: version });
      secondPi.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
      expect(secondPi.sendMessage).not.toHaveBeenCalled();
      expect(firstPi.sendMessage.mock.calls.length + secondPi.sendMessage.mock.calls.length).toBe(1);
      expect(fs.readFileSync(sessionFile, "utf8").trim().split("\n")).toHaveLength(1);
    } finally {
      await Promise.all([first.close(), second.close()]);
      firstMain.closeFollowUpDrain();
      secondMain.closeFollowUpDrain();
    }
  });

});
