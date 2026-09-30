import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { FOLLOW_UP_LIMITS, MainAgentController, type FabricMainAgentDeliveryRequest, type FabricMainAgentTarget } from "../src/main-agent.js";
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

  it("retries failed deletion through Main's durable journal across a controller restart", async () => {
    const h = harness();
    fakeWatch();
    const journal = path.join(h.root, "main-followups", "root.json");
    // Each controller owns fresh fake Pi ports and session entries. Only the journal survives.
    const receiver = () => {
      type Handler = (event: unknown, context: ExtensionContext) => unknown;
      const handlers = new Map<string, Handler[]>();
      const entries: unknown[] = [];
      const context = {
        isIdle: () => false, hasPendingMessages: () => false,
        signal: { aborted: false }, sessionManager: { getEntries: () => entries },
      } as unknown as ExtensionContext;
      const sendMessage = vi.fn();
      const pi = {
        on: (name: string, handler: Handler) => {
          handlers.set(name, [...(handlers.get(name) ?? []), handler]);
          return () => handlers.set(name, (handlers.get(name) ?? []).filter((fn) => fn !== handler));
        },
        sendMessage,
      } as unknown as ExtensionAPI;
      const main = new MainAgentController(pi, h.rootId, true, h.root, path.basename(h.root));
      main.attachFollowUpDrain(context, 60_000, journal);
      const emit = (name: string, event: unknown) => {
        for (const handler of handlers.get(name) ?? []) handler(event, context);
      };
      return { main, context, entries, sendMessage, emit };
    };
    const firstReceiver = receiver();
    let secondReceiver: ReturnType<typeof receiver> | undefined;
    const clients: ResidencyClient[] = [];
    const start = (main: MainAgentController) => {
      const client = h.client(h.mesh, main);
      clients.push(client);
      client.start();
      return client;
    };
    const items = () => (JSON.parse(fs.readFileSync(journal, "utf8")) as {
      items: Array<{ id: string; deliveryId: string; message: string }>;
    }).items;
    try {
      await h.put("journal-once");
      const deliveryId = `resident:${h.rootId}:journal-once`;
      const firstAdmission = vi.spyOn(firstReceiver.main, "deliverAgent");
      const deletion = vi.spyOn(h.mesh, "delete").mockImplementationOnce(async () => {
        // The deletion fails only AFTER a successful, durable Main admission.
        expect(firstAdmission.mock.results[0]!.type).toBe("return");
        expect(items()).toHaveLength(1);
        expect(items()[0]!.deliveryId).toBe(deliveryId);
        throw new Error("delete failed");
      });
      const firstClient = start(firstReceiver.main);
      await until(() => deletion.mock.calls.length === 1);
      await firstClient.close();
      const admitted = items()[0]!;
      expect(h.pending()).toBe(1);
      expect(firstReceiver.main.queueDepth().pendingFollowUps).toBe(1);
      expect(firstReceiver.sendMessage).not.toHaveBeenCalled();
      firstReceiver.main.closeFollowUpDrain();

      secondReceiver = receiver(); // same root/journal, no shared receiver state
      const secondAdmission = vi.spyOn(secondReceiver.main, "deliverAgent");
      expect(secondReceiver.main.queueDepth().pendingFollowUps).toBe(1);
      const resumed = start(secondReceiver.main);
      await until(() => h.pending() === 0);
      await resumed.close();
      expect(secondAdmission).toHaveBeenCalledTimes(1); // retry reaches the REAL receiver
      expect(secondAdmission.mock.calls[0]![0].deliveryId).toBe(deliveryId);
      expect(secondAdmission.mock.results[0]!.value).toMatchObject({ duplicate: true, messageId: admitted.id });
      expect(secondReceiver.main.queueDepth().pendingFollowUps).toBe(1); // no second admission
      expect(items()).toEqual([admitted]); // message identity stable; no duplicate journal item
      expect(secondReceiver.sendMessage).not.toHaveBeenCalled();
      const boundary = { outcome: "completed", context: { pendingMessages: [] } };
      secondReceiver.emit("agent_before_settle", boundary); // next safe boundary releases it
      expect(secondReceiver.sendMessage).toHaveBeenCalledTimes(1);
      const [message, options] = secondReceiver.sendMessage.mock.calls[0]!;
      expect(message.details).toMatchObject({ id: admitted.id, deliveryId });
      expect(message.details.items).toBeUndefined();
      expect(options).toEqual({ deliverAs: "followUp", triggerTurn: true });
      secondReceiver.entries.push({ type: "custom_message", customType: message.customType, details: message.details });
      secondReceiver.emit("agent_settled", { outcome: "completed" });
      secondReceiver.emit("agent_before_settle", boundary);
      expect(secondReceiver.sendMessage).toHaveBeenCalledTimes(1);
      expect(fs.existsSync(journal)).toBe(false);
      expect(JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")).ids).toContain(deliveryId);

      // A closed journal and a REAL quota refusal must keep future work for a later drain.
      secondReceiver.main.closeFollowUpDrain();
      await h.put("future");
      const closedClient = start(secondReceiver.main);
      await until(() => secondAdmission.mock.calls.length === 2);
      await closedClient.close();
      expect(secondAdmission.mock.results[1]!.type).toBe("throw");
      expect(secondAdmission.mock.results[1]!.value.message).toMatch(/journal/);
      expect(h.pending()).toBe(1);
      const replaceFuture = async (message: string) => {
        const record = h.mesh.listAll(residentDeliveryPrefix(h.rootId), { fresh: true })[0]!;
        await h.mesh.put({ key: record.key, ifVersion: record.version, identity: record.updatedBy,
          value: { ...(record.value as Record<string, unknown>), message } });
      };
      await replaceFuture("x".repeat(FOLLOW_UP_LIMITS.senderBytes + 1));
      secondReceiver.main.attachFollowUpDrain(secondReceiver.context, 60_000, journal);
      const fullClient = start(secondReceiver.main);
      await until(() => secondAdmission.mock.calls.length === 3);
      await fullClient.close();
      expect(secondAdmission.mock.results[2]!.type).toBe("throw");
      expect(secondAdmission.mock.results[2]!.value.message).toMatch(/followUp queue is full/);
      expect(h.pending()).toBe(1);
      expect(secondReceiver.main.queueDepth().pendingFollowUps).toBe(0);
      expect(secondReceiver.sendMessage).toHaveBeenCalledTimes(1);
      await replaceFuture("future");
      start(secondReceiver.main);
      await until(() => h.pending() === 0);
      expect(secondAdmission.mock.results[3]!.type).toBe("return");
      expect(items()).toHaveLength(1);
      expect(items()[0]!.deliveryId).toBe(`resident:${h.rootId}:future`);
    } finally {
      await Promise.all(clients.map((client) => client.close()));
      firstReceiver.main.closeFollowUpDrain();
      secondReceiver?.main.closeFollowUpDrain();
    }
  });

});
