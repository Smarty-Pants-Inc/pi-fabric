import fs from "node:fs";
import { EventEmitter } from "node:events";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { MeshLockTimeoutError } from "../src/mesh.js";
import { CompletionJournal, saveCompletion } from "../src/agents/completion-journal.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentHostConfig } from "../src/residency/protocol.js";
import { closeNatsStateBackend, NatsStateBackend, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const realSetTimeout = globalThis.setTimeout; // captured before any fake timers
const realSleep = (ms: number) => new Promise<void>((resolve) => realSetTimeout(resolve, ms));
const until = async (check: () => boolean, ms = 3_000): Promise<void> => {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() > end) throw new Error("condition not reached"); await realSleep(10); }
};

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-client-events-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:events", sessionId: "events", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(path.join(config.meshRoot, "agent-completions"), { recursive: true });
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
    mainAgent: { local: true } as FabricMainAgentTarget });
  const read = vi.spyOn(mesh, "listAll");
  const drains = () => read.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/").length;
  // A journal envelope lands by atomic rename (one notification), as a resident completion does.
  const landCompletion = (name: string) => {
    const temporary = path.join(root, `${name}.tmp`);
    fs.writeFileSync(temporary, "{}");
    fs.renameSync(temporary, path.join(config.meshRoot, "agent-completions", `${name}.json`));
  };
  return { root, config, mesh, client, read, drains, landCompletion,
    cleanup: () => fs.rmSync(root, { recursive: true, force: true }) };
};

describe("resident client event-driven delivery", () => {
  it("idle: one startup catch-up drain, then no timers and no recurring drains", async () => {
    const { client, drains, cleanup } = fixture();
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(drains()).toBe(1);
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(drains()).toBe(1);
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("a new journal completion (external resident writer) drains once", async () => {
    const { client, drains, landCompletion, cleanup } = fixture();
    try {
      client.start();
      await until(() => drains() === 1);
      await realSleep(50);
      landCompletion("external");
      await until(() => drains() === 2);
      await realSleep(200);
      expect(drains()).toBe(2);
    } finally { await client.close(); cleanup(); }
  });

  it("Windows rename/change notification burst drains once and leaves no timer", async () => {
    const { client, config, drains, cleanup } = fixture();
    const journal = vi.spyOn(CompletionJournal.prototype, "drain");
    let notify!: (event: string, name: string) => void;
    const watch = vi.spyOn(fs, "watch").mockImplementation(((directory: string, callback: typeof notify) => {
      if (directory === path.join(config.meshRoot, "agent-completions")) notify = callback;
      return Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher;
    }) as unknown as typeof fs.watch);
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      // Join startup's real filesystem IO before advancing a fake burst clock.
      await journal.mock.results[0]!.value;
      await vi.advanceTimersByTimeAsync(0);
      expect(drains()).toBe(1);
      notify("rename", "external.json");
      await vi.advanceTimersByTimeAsync(10);
      notify("change", "external.json");
      await vi.advanceTimersByTimeAsync(15);
      expect(drains()).toBe(2);
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(drains()).toBe(2);
      notify("rename", "late.json");
      expect(vi.getTimerCount()).toBe(1);
      await client.close();
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(drains()).toBe(2);
    } finally { await client.close(); vi.useRealTimers(); journal.mockRestore(); watch.mockRestore(); cleanup(); }
  });

  it.each([true, false])("delayed NATS projector wakes authenticated delivery only with the hook (%s), and close unsubscribes", async (withHook) => {
    const { config, cleanup } = fixture();
    // No directory notification can carry this wake: the receiver sees only projection callbacks.
    const watch = vi.spyOn(fs, "watch").mockImplementation(() =>
      Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher);
    const server = fakeJetStream();
    const receiverPort = server.port();
    await openNatsStateBackend({ root: config.meshRoot, port: receiverPort, lockTimeoutMs: 2_000 });
    const mesh = new MeshStore(config.meshRoot, 64 * 1024, 100, { stateBackend: "nats" });
    const remote = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot",
      maxEventBytes: 1 << 20, lockTimeoutMs: 2_000 });
    const deliverAgent = vi.fn();
    const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
      mainAgent: { local: true, deliverAgent } as unknown as FabricMainAgentTarget });
    const stop = vi.fn();
    const subscribe = mesh.subscribeStateChanges.bind(mesh);
    const subscription = vi.spyOn(mesh, "subscribeStateChanges").mockImplementation((listener) => {
      const unsubscribe = withHook ? subscribe(listener) : () => {};
      return () => { stop(); unsubscribe(); };
    });
    // Record deletion intent only: this local probe must not run source-retirement deletion.
    const deletion = vi.spyOn(mesh, "delete").mockResolvedValue({ deleted: true });
    const reads = vi.spyOn(mesh, "listAll");
    const key = residentDeliveryPrefix(config.rootId) + "remote";
    const envelope = { format: RESIDENT_HOST_FORMAT, id: "remote", rootId: config.rootId,
      from: { id: "actor:remote", name: "remote", kind: "actor" }, source: "actor-output",
      message: "remote followUp", delivery: "followUp", triggerTurn: true };
    try {
      client.start();
      client.start();
      expect(subscription).toHaveBeenCalledOnce();
      await until(() => reads.mock.calls.length > 0);
      await realSleep(50);
      // A remote commit is a wake hint, not recipient/producer authority.
      await remote.put({ key: key + "-foreign", value: { ...envelope, id: "foreign" },
        identity: { id: "foreign-resident", name: "foreign", kind: "actor" } });
      await until(() => mesh.get(key + "-foreign") !== undefined);
      await realSleep(50);
      expect(deliverAgent).not.toHaveBeenCalled();
      expect(deletion).not.toHaveBeenCalled();
      receiverPort.pause();
      await remote.put({ key, value: envelope, identity: { id: client.hostId, name: "resident", kind: "actor" } });
      await realSleep(50);
      expect(mesh.get(key)).toBeUndefined();
      expect(deliverAgent).not.toHaveBeenCalled();
      receiverPort.resume();
      await until(() => mesh.get(key) !== undefined);
      if (withHook) await until(() => deliverAgent.mock.calls.length === 1);
      await realSleep(100);
      expect(deliverAgent).toHaveBeenCalledTimes(withHook ? 1 : 0);
      if (withHook) {
        expect(deliverAgent).toHaveBeenCalledWith(expect.objectContaining({
          message: "remote followUp", delivery: "followUp", triggerTurn: true,
          deliveryId: `resident:${config.rootId}:remote`, verification: "mesh",
        }));
        expect(deletion).toHaveBeenCalledExactlyOnceWith({ key, ifVersion: mesh.get(key)!.version });
      } else expect(deletion).not.toHaveBeenCalled();
      await client.close();
      expect(stop).toHaveBeenCalledOnce();
      const closedReads = reads.mock.calls.length;
      await remote.put({ key: "unrelated/after-close", value: 1, identity: { id: "peer", name: "peer", kind: "agent" } });
      await until(() => mesh.get("unrelated/after-close") !== undefined);
      await realSleep(50);
      expect(reads).toHaveBeenCalledTimes(closedReads);
    } finally { await client.close(); remote.close(); closeNatsStateBackend(config.meshRoot); watch.mockRestore(); cleanup(); }
  });

  it("a peer mesh write wakes delivery and the wake converges", async () => {
    const { client, mesh, drains, cleanup } = fixture();
    try {
      client.start();
      await until(() => drains() === 1);
      await realSleep(50);
      await mesh.put({ key: "peer/unrelated", value: { at: 1 }, identity: { id: "peer", name: "peer", kind: "agent" } });
      await until(() => drains() >= 2);
      await realSleep(300);
      const settled = drains();
      expect(settled).toBeLessThanOrEqual(4);
      await realSleep(300);
      expect(drains()).toBe(settled);
    } finally { await client.close(); cleanup(); }
  });

  it("failed delivery retries once on its own backoff without another event", async () => {
    const { client, read, drains, cleanup } = fixture();
    read.mockImplementationOnce(() => { throw new MeshLockTimeoutError("fixture outage", 1, 100); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(drains()).toBe(1);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(drains()).toBe(2);
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(drains()).toBe(2);
    } finally { await client.close(); vi.useRealTimers(); warn.mockRestore(); cleanup(); }
  });

  it("an event during a slow in-flight drain is not lost", async () => {
    const { client, drains, landCompletion, cleanup } = fixture();
    let release!: () => void;
    const original = CompletionJournal.prototype.drain;
    const drain = vi.spyOn(CompletionJournal.prototype, "drain").mockImplementationOnce(function (this: CompletionJournal) {
      return new Promise<void>((resolve) => { release = resolve; });
    }).mockImplementation(function (this: CompletionJournal, deliver?: boolean) { return original.call(this, deliver); });
    try {
      client.start();
      await until(() => drain.mock.calls.length === 1);
      landCompletion("during-drain");
      await realSleep(200);
      expect(drains()).toBe(1); // coalesced behind the in-flight pass
      release();
      await until(() => drains() === 2);
      await realSleep(200);
      expect(drains()).toBe(2);
    } finally { await client.close(); drain.mockRestore(); cleanup(); }
  });

  it("exhausted resident watches still see external deliveries and completions on one minute tick, then return to events", async () => {
    const { config, mesh, cleanup, landCompletion } = fixture();
    const delivered = vi.fn();
    const completion = vi.fn(); // no ACK: do not execute journal/source retirement locally
    const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
      mainAgent: { local: true, deliverAgent: delivered } as unknown as FabricMainAgentTarget,
      onBackgroundComplete: completion });
    const callbacks: Array<{ directory: string; notify: (event: string, name: string) => void }> = [];
    let exhausted = true;
    const watch = vi.spyOn(fs, "watch").mockImplementation(((directory: string, notify: (event: string, name: string) => void) => {
      if (exhausted) throw Object.assign(new Error("ENOSPC: watcher limit"), { code: "ENOSPC" });
      callbacks.push({ directory, notify });
      return Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher;
    }) as unknown as typeof fs.watch);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    // Record intended source retirement and hide only its state-view result.
    const retired = new Set<string>();
    const list = MeshStore.prototype.listAll.bind(mesh);
    const reads = vi.spyOn(mesh, "listAll").mockImplementation((...args) => list(...args).filter((entry) => !retired.has(entry.key)));
    const deletion = vi.spyOn(mesh, "delete").mockImplementation(async ({ key }) => { retired.add(key); return { deleted: true }; });
    const journal = vi.spyOn(CompletionJournal.prototype, "drain");
    const drains = () => reads.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/").length;
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(63_000);
      await realSleep(50); // let the bounded re-arm rescans' real IO settle
      await vi.advanceTimersByTimeAsync(0);
      expect(client.observationHealth()).toEqual(["observation-degraded:residency"]);
      const key = residentDeliveryPrefix(config.rootId) + "degraded";
      await mesh.put({ key, identity: { id: client.hostId, name: "resident", kind: "actor" }, value: {
        format: RESIDENT_HOST_FORMAT, id: "degraded", rootId: config.rootId,
        from: { id: "actor:producer", name: "producer", kind: "actor" }, source: "actor-output",
        message: "external delivery", delivery: "followUp", triggerTurn: true,
      } });
      saveCompletion(config.meshRoot, { rootId: config.rootId, sessionId: config.sessionId,
        cwd: config.cwd, projectRoot: config.projectRoot, name: "main", startedAt: 0 }, {
        id: "d".repeat(32), name: "external completion", task: "task", status: "completed",
        runner: "pi", transport: "process", cwd: config.cwd, text: "completed externally",
        startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      });
      const before = drains();
      const journalBefore = journal.mock.calls.length;
      await vi.advanceTimersByTimeAsync(59_999);
      expect(drains()).toBe(before);
      expect(delivered).not.toHaveBeenCalled();
      expect(completion).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await realSleep(100); // the minute tick's coalesced pass includes real journal IO
      await Promise.all(journal.mock.results.map((result) => result.value));
      await vi.advanceTimersByTimeAsync(0);
      expect(drains()).toBeGreaterThan(before);
      expect(delivered).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ message: "external delivery", delivery: "followUp" }));
      expect(deletion).toHaveBeenCalledOnce();
      expect(completion).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ id: "d".repeat(32), text: "completed externally" }), expect.any(Function));
      expect(journal.mock.calls.length).toBeGreaterThan(journalBefore);
      expect(warn).toHaveBeenCalledTimes(2);
      exhausted = false;
      await vi.advanceTimersByTimeAsync(60_000);
      await Promise.all(journal.mock.results.map((result) => result.value));
      await vi.advanceTimersByTimeAsync(0);
      expect(client.observationHealth()).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      expect(warn).toHaveBeenCalledTimes(3);
      const recovered = drains();
      await vi.advanceTimersByTimeAsync(300_000);
      expect(drains()).toBe(recovered);
      landCompletion("event-completion");
      callbacks.find((entry) => entry.directory === path.join(config.meshRoot, "agent-completions"))!.notify("rename", "event-completion.json");
      await vi.advanceTimersByTimeAsync(25);
      expect(drains()).toBe(recovered + 1);
      await client.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); journal.mockRestore(); watch.mockRestore(); warn.mockRestore(); cleanup(); }
  });

  it("watcher error rescans, warns once, and re-arms on a bounded one-shot", async () => {
    const { client, drains, cleanup } = fixture();
    const watchers: fs.FSWatcher[] = [];
    const realWatch = fs.watch;
    const watch = vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      const watcher = realWatch(...args);
      watchers.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      await realSleep(100); // let the startup pass's real file I/O settle
      expect(drains()).toBe(1);
      const armed = watch.mock.calls.length;
      expect(armed).toBeGreaterThan(0);
      watchers[0]!.emit("error", new Error("fixture watcher failure"));
      await vi.advanceTimersByTimeAsync(0);
      await realSleep(100);
      expect(drains()).toBe(2); // rescan
      expect(warn).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(1); // one re-arm, not a poll
      await vi.advanceTimersByTimeAsync(1_000);
      await realSleep(100);
      expect(watch.mock.calls.length).toBe(armed * 2);
      expect(drains()).toBe(3); // post-rearm rescan
      expect(vi.getTimerCount()).toBe(0);
      watchers.at(-1)!.emit("error", new Error("fixture watcher failure"));
      await vi.advanceTimersByTimeAsync(0);
      expect(warn).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(1);
      await client.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); watch.mockRestore(); warn.mockRestore(); cleanup(); }
  });
});
