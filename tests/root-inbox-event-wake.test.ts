import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, type AgentSession } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ROOT_INBOX_PREFIX, RootInbox, RootInboxEventWake, rootInboxSession, type RootInboxKnownWake } from "../src/topology/root-inbox.js";

const roots: string[] = [];
const observers: RootInboxEventWake[] = [];
const boxes: RootInbox[] = [];
const sessions: AgentSession[] = [];
const envKeys = ["PI_CODING_AGENT_DIR", "PI_FABRIC_MESH_ROOT", "PI_FABRIC_INBOX_WAKE_MS", "PI_FABRIC_INBOX_WAKE_COOLDOWN_MS"] as const;
const savedEnv = Object.fromEntries(envKeys.map(key => [key, process.env[key]]));
const capabilities = Symbol.for("pi-fabric.test.hostCapabilities");
const me: MeshIdentity = { id: "session:me", name: "Main", kind: "main", sessionId: "me" };
const peer: MeshIdentity = { id: "session:peer", name: "Peer", kind: "main", sessionId: "peer" };
const held = { holdsBatch: () => true, holdsSteer: () => false };
const root = () => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "root-event-wake-"));
  roots.push(value);
  return value;
};
const settleMicrotasks = async () => { for (let n = 0; n < 12; n++) await Promise.resolve(); };
afterEach(async () => {
  for (const session of sessions.splice(0)) {
    await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    session.dispose();
  }
  for (const [key, value] of Object.entries(savedEnv)) {
    if (value === undefined) delete process.env[key]; else process.env[key] = value;
  }
  delete (globalThis as Record<symbol, unknown>)[capabilities];
  for (const observer of observers.splice(0)) observer.close();
  for (const box of boxes.splice(0)) await box.close();
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});

const watched = (wake: () => Promise<void>, safetyMs?: number) => {
  const observer = new RootInboxEventWake(root(), wake, safetyMs);
  observers.push(observer);
  observer.start();
  return observer;
};
const mockWatch = () => {
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), ref: vi.fn(), unref: vi.fn() });
  let notify: (event: string, filename: string | null) => void = () => {};
  const spy = vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
    notify = args[2] as typeof notify;
    return watcher;
  }) as typeof fs.watch);
  return { watcher, spy, notify: (filename: string | null) => notify("change", filename) };
};

describe("root mesh event wake observation", () => {
  it("owns no idle timer and takes no idle wake for five minutes, even with missed work and a configured fast safety interval", async () => {
    vi.useFakeTimers();
    const mock = mockWatch(), interval = vi.spyOn(globalThis, "setInterval");
    const wake = vi.fn().mockResolvedValue(undefined);
    const observer = watched(wake, 1);
    await observer.request();
    expect(interval).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    fs.writeFileSync(path.join(observer.root, "events.jsonl"), "suppressed notification\n");
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(wake).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    mock.notify("events.jsonl"); await observer.request();
    expect(wake).toHaveBeenCalledTimes(2);
    observer.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("watches only events, coalesces bursts, and retains a notification during an awaited drain", async () => {
    vi.useFakeTimers();
    const mock = mockWatch();
    let release!: () => void;
    const wake = vi.fn().mockImplementationOnce(() => new Promise<void>(resolve => { release = resolve; }))
      .mockResolvedValue(undefined);
    const observer = watched(wake);
    await settleMicrotasks();
    expect(wake).toHaveBeenCalledTimes(1);
    mock.notify("state.json");
    await settleMicrotasks();
    mock.notify("events.jsonl"); mock.notify(null); mock.notify("events.jsonl");
    release();
    await observer.request();
    expect(wake).toHaveBeenCalledTimes(2);
    observer.close();
    mock.notify("events.jsonl");
    await vi.advanceTimersByTimeAsync(120_000);
    expect(wake).toHaveBeenCalledTimes(2);
    expect(mock.watcher.close).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["error", "unavailable"])("has no fast fallback after watch %s and stops on replacement", async (mode) => {
    vi.useFakeTimers();
    const mock = mockWatch();
    if (mode === "unavailable") mock.spy.mockImplementation(() => { throw new Error("watch unavailable"); });
    const wake = vi.fn().mockResolvedValue(undefined);
    const old = watched(wake);
    await settleMicrotasks();
    if (mode === "error") mock.watcher.emit("error", new Error("watch lost"));
    await vi.advanceTimersByTimeAsync(59_999);
    expect(wake).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(wake).toHaveBeenCalledTimes(1); // Attachment repair never requests work.
    old.close();
    const successorWake = vi.fn().mockResolvedValue(undefined);
    const successor = watched(successorWake);
    await settleMicrotasks();
    expect(successorWake).toHaveBeenCalledTimes(1);
    await old.request();
    expect(successorWake).toHaveBeenCalledTimes(1);
    successor.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reattaches a silently replaced physical root only at a trusted request, then wakes on new appends without a tick", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    const watch = vi.spyOn(fs, "watch"); // Native watchers, including the silently stranded old inode.
    const wake = vi.fn().mockResolvedValue(undefined);
    const observer = watched(wake);
    await observer.request();
    const retired = path.join(root(), "retired-root");
    fs.renameSync(observer.root, retired);
    fs.mkdirSync(observer.root);
    expect(watch).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(watch).toHaveBeenCalledTimes(1); // No idle reattachment timer.
    await observer.request(); // Event/settle/explicit request repairs attachment.
    expect(watch).toHaveBeenCalledTimes(2);
    const before = wake.mock.calls.length;
    fs.appendFileSync(path.join(observer.root, "events.jsonl"), "new root event\n");
    await vi.waitFor(() => expect(wake.mock.calls.length).toBeGreaterThan(before));
    expect(watch).toHaveBeenCalledTimes(2);
  });

  it("retires a missing physical root and reattaches only at a trusted request", async () => {
    vi.useFakeTimers();
    const mock = mockWatch();
    const observer = watched(vi.fn().mockResolvedValue(undefined));
    await observer.request();
    fs.rmSync(observer.root, { recursive: true });
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(mock.watcher.close).not.toHaveBeenCalled(); // No idle tick observes the loss.
    await observer.request();
    expect(mock.watcher.close).toHaveBeenCalledTimes(1);
    expect(mock.spy).toHaveBeenCalledTimes(1);
    fs.mkdirSync(observer.root);
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(mock.spy).toHaveBeenCalledTimes(1);
    await observer.request();
    expect(mock.spy).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
    mock.notify(null);
    await observer.request();
  });

  it("does not churn watchers or reread an unchanged root's log/state/directory while idle", async () => {
    vi.useFakeTimers();
    const mock = mockWatch();
    const { mesh, box } = fixture(0);
    const observer = new RootInboxEventWake(mesh.root, async () => { await box.wake(held, () => true); });
    observers.push(observer);
    observer.start();
    await observer.request();
    const read = vi.spyOn(mesh, "read");
    const readFile = vi.spyOn(fs, "readFileSync");
    const readdir = vi.spyOn(fs, "readdirSync");
    await vi.advanceTimersByTimeAsync(6 * 60_000);
    expect(vi.getTimerCount()).toBe(0);
    expect(mock.spy).toHaveBeenCalledTimes(1);
    expect(mock.watcher.close).not.toHaveBeenCalled();
    expect(read).not.toHaveBeenCalled();
    expect(readFile).not.toHaveBeenCalled();
    expect(readdir).not.toHaveBeenCalled();
  });

  it("uses a real directory watch for a replaced event log without a timer tick", async () => {
    const wake = vi.fn().mockResolvedValue(undefined);
    const observer = watched(wake);
    await observer.request();
    const before = wake.mock.calls.length;
    const temp = path.join(observer.root, "new-log");
    fs.writeFileSync(temp, "new bytes\n");
    fs.renameSync(temp, path.join(observer.root, "events.jsonl"));
    await vi.waitFor(() => expect(wake.mock.calls.length).toBeGreaterThan(before));
  });
});

// This uses src/index.ts, not the parent's baseline dist: it exercises the actual changed
// activation wiring and native gate while leaving build ownership with the integrator.
const startSession = async (capable = true, tokensPerSecond = 1_000, safetyMs = 100) => {
  const cwd = root(), agentDir = path.join(cwd, "agent"), meshRoot = path.join(cwd, "mesh");
  fs.mkdirSync(agentDir);
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.env.PI_FABRIC_MESH_ROOT = meshRoot;
  process.env.PI_FABRIC_INBOX_WAKE_MS = String(safetyMs);
  process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS = "0";
  if (capable) (globalThis as Record<symbol, unknown>)[capabilities] = {
    triggeredMessageQueuesBehindPreflight: true, promptPendingVisible: true,
  };
  const faux = fauxProvider({ tokensPerSecond });
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(cwd, "auth.json") });
  modelRuntime.registerNativeProvider(faux.provider);
  const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, noSkills: true, noPromptTemplates: true,
    noThemes: true, noContextFiles: true, additionalExtensionPaths: [path.resolve("src/index.ts")] });
  await resourceLoader.reload();
  const { session } = await createAgentSession({ cwd, agentDir, modelRuntime, model: faux.getModel(), resourceLoader,
    sessionManager: SessionManager.inMemory(cwd) });
  sessions.push(session);
  await session.bindExtensions({});
  faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("ready")]);
  await session.prompt("activate");
  const inbox = () => session.messages.filter(message => message.role === "custom" &&
    (message as { customType?: string }).customType === "pi-fabric-inbox");
  const publish = (text: string, ageMs = 120_000) => {
    const log = path.join(meshRoot, "events.jsonl");
    const lines = fs.existsSync(log) ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean) : [];
    const sequence = (lines.length ? JSON.parse(lines.at(-1)!).sequence : 0) + 1;
    fs.appendFileSync(log, JSON.stringify({ id: randomUUID(), sequence, topic: "fleet.work.test.1", kind: "ack", from: peer,
      to: `session:${session.sessionManager.getSessionId()}`, text, createdAt: Date.now() - ageMs }) + "\n");
    fs.writeFileSync(path.join(meshRoot, "sequence"), String(sequence));
  };
  return { session, faux, inbox, publish, meshRoot };
};

describe("changed source activation and Main gate", () => {
  it("uses the current runtime's actual grace hint before any 60-second safety tick", async () => {
    const h = await startSession(true, 1_000, 60_000);
    h.faux.setResponses([fauxAssistantMessage("deadline received")]);
    h.publish("near grace maturity", 59_500);
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(h.inbox()).toHaveLength(0);
    await vi.waitFor(() => { expect(h.inbox()).toHaveLength(1); expect(h.session.isStreaming).toBe(false); }, { timeout: 3_000 });
  }, 60_000);

  it("wakes after external publication, then stops the active watcher on shutdown", async () => {
    const h = await startSession();
    h.faux.setResponses([fauxAssistantMessage("woken")]);
    h.publish("external event");
    await vi.waitFor(() => { expect(h.inbox()).toHaveLength(1); expect(h.session.isStreaming).toBe(false); }, { timeout: 10_000 });
    await h.session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    h.publish("after shutdown");
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(h.inbox()).toHaveLength(1);
  }, 60_000);

  it("keeps capability gating off for idle work, but the next native turn drains it", async () => {
    const h = await startSession(false);
    h.publish("without capability");
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(h.inbox()).toHaveLength(0);
    h.faux.setResponses([fauxAssistantMessage("next turn")]);
    await h.session.prompt("next");
    expect(h.inbox()).toHaveLength(1);
  }, 60_000);

  it("leaves a busy Main's event to its turn-end reconciliation without a duplicate settle delivery", async () => {
    const h = await startSession(true, 80);
    const order: string[] = [];
    h.session.subscribe(event => {
      if (event.type === "agent_settled") order.push("settled");
      if (event.type === "message_end" && (event.message as { customType?: string }).customType === "pi-fabric-inbox") order.push("inbox");
    });
    h.faux.setResponses([() => { h.publish("during busy completion"); return fauxAssistantMessage("working ".repeat(20)); },
      fauxAssistantMessage("settle received")]);
    await h.session.prompt("work");
    await vi.waitFor(() => { expect(h.inbox()).toHaveLength(1); expect(h.session.isStreaming).toBe(false); }, { timeout: 10_000 });
    // Main reconciles before settle at turn_end (#4313); its queue lease keeps the
    // following settle from duplicating the unreceived followUp. The watch never interrupts work.
    expect(order.slice(0, 2)).toEqual(["inbox", "settled"]);
  }, 60_000);

  it("rearms a young event arriving while Main is busy for its exact due time after settlement", async () => {
    const h = await startSession(true, 80, 60_000);
    let dueAt = 0;
    h.faux.setResponses([() => {
      dueAt = Date.now() + 2_000;
      h.publish("young while busy", 58_000);
      return fauxAssistantMessage("working ".repeat(20));
    }, fauxAssistantMessage("young deadline received")]);
    await h.session.prompt("work");
    expect(Date.now()).toBeLessThan(dueAt); expect(h.inbox()).toHaveLength(0);
    await vi.waitFor(() => { expect(h.inbox()).toHaveLength(1); expect(h.session.isStreaming).toBe(false); }, { timeout: 4_000 });
    expect(Date.now()).toBeGreaterThanOrEqual(dueAt);
    expect(JSON.stringify(h.inbox()[0])).toContain("young while busy");
    await new Promise(resolve => setTimeout(resolve, 100)); expect(h.inbox()).toHaveLength(1);
  }, 60_000);

  it("disarms owner-aborted work and rearms on the next start", async () => {
    const h = await startSession(true, 40);
    h.faux.setResponses([() => { h.publish("busy and aborted"); return fauxAssistantMessage("long answer ".repeat(300)); }]);
    const run = h.session.prompt("work");
    await vi.waitFor(() => expect(h.session.isStreaming).toBe(true));
    await new Promise(resolve => setTimeout(resolve, 200));
    await h.session.abort(); await run.catch(() => undefined);
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(h.inbox()).toHaveLength(0);
    h.faux.setResponses([fauxAssistantMessage("resumed")]);
    await h.session.prompt("resume");
    expect(h.inbox()).toHaveLength(1);
    h.faux.setResponses([fauxAssistantMessage("rearmed")]);
    h.publish("after rearm");
    await vi.waitFor(() => { expect(h.inbox()).toHaveLength(2); expect(h.session.isStreaming).toBe(false); }, { timeout: 10_000 });
  }, 60_000);

  it("queues one followUp across an idle delivery and the following turn_end/settle, then the receipt retires it", async () => {
    const h = await startSession();
    h.faux.setResponses([fauxAssistantMessage("woken")]);
    h.publish("idle then turn");
    await vi.waitFor(() => { expect(h.inbox()).toHaveLength(1); expect(h.session.isStreaming).toBe(false); }, { timeout: 10_000 });
    // The next native turn reconciles at turn_end and settle; the idle lease must not requeue it.
    h.faux.setResponses([fauxAssistantMessage("next")]);
    await h.session.prompt("next");
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(h.inbox()).toHaveLength(1);
    const cursor = new MeshStore(h.meshRoot, 64 * 1024, 500).listAll(ROOT_INBOX_PREFIX, { fresh: true });
    expect(cursor).toHaveLength(1);
    expect(cursor[0]!.value).not.toHaveProperty("pending"); // The session receipt retired the batch.
  }, 60_000);
});

const fixture = (grace = 60_000, cooldown = 300_000) => {
  const mesh = new MeshStore(path.join(root(), "mesh"), 64 * 1024, 500);
  let offset = 0;
  const now = () => Date.now() + offset;
  const box = new RootInbox(mesh, me, () => [me.id], { now, steerGraceMs: grace, wakeCooldownMs: cooldown });
  boxes.push(box);
  box.start();
  const work = (text: string, kind = "ack") => mesh.publish({ topic: "fleet.work.test.1", to: me.id, from: peer, text, kind });
  return { mesh, box, work, advance: (ms: number) => { offset += ms; } };
};

describe("root inbox known-work one-shot deadlines", () => {
  const observe = (mesh: MeshStore, box: RootInbox, allowed = () => true) => {
    const delivered: string[] = [];
    const observed: (readonly string[])[] = [];
    let observer!: RootInboxEventWake;
    const wake = vi.fn(async (hint?: RootInboxKnownWake) => {
      observer.cancelKnownDeadline();
      if (!allowed()) return;
      const batch = await box.wake(held, allowed, hint);
      if (!allowed()) return;
      delivered.push(...(batch?.events.map(event => event.text ?? "") ?? []));
      observer.armKnownDeadline(box.knownWake);
    });
    observer = new RootInboxEventWake(mesh.root, wake, 60_000, () => { observed.push(box.observe(held)); });
    observers.push(observer); observer.start();
    return { observer, wake, delivered, observed };
  };

  it("matures actual event grace between safety ticks without another append or safety tick", async () => {
    vi.useFakeTimers(); mockWatch();
    const { mesh, box, work } = fixture();
    const h = observe(mesh, box);
    await h.observer.request();
    await vi.advanceTimersByTimeAsync(15_000);
    await work("young"); await h.observer.request();
    expect(box.knownWakeDueAt).toBe(Date.now() + 60_000);
    const read = vi.spyOn(mesh, "read");
    await vi.advanceTimersByTimeAsync(45_000); // Safety tick sees the same young event.
    expect(read).not.toHaveBeenCalled();
    expect(h.delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(14_999);
    expect(h.delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["young"]);
    expect(vi.getTimerCount()).toBe(0); // The known item's deadline was cleared; no idle timer.
    const calls = h.wake.mock.calls.length;
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(h.wake).toHaveBeenCalledTimes(calls);
    expect(h.delivered).toEqual(["young"]);
  });

  it("does not admit a suppressed unknown arrival at a known item's deadline", async () => {
    vi.useFakeTimers(); mockWatch();
    const { mesh, box, work } = fixture(0, 75_000);
    const h = observe(mesh, box); await h.observer.request();
    await work("first"); await h.observer.request(); await box.close();
    await vi.advanceTimersByTimeAsync(1_000);
    const known = await work("known"); await h.observer.request();
    expect(box.knownWake?.ids).toEqual([known.id]);
    await vi.advanceTimersByTimeAsync(1_000);
    const unknown = await work("lost notification", "p0"); // No watcher callback/request.
    const read = vi.spyOn(mesh, "read");
    await vi.advanceTimersByTimeAsync(73_000);
    expect(read).toHaveBeenCalled(); // Fresh ordered read, not a retained payload.
    expect(h.delivered).toEqual(["first", "known"]);
    expect(mesh.get(box.key)?.value).toMatchObject({ pending: { ids: [known.id], through: known.sequence } });
    expect(box.knownWake).toBeUndefined();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(h.delivered).toEqual(["first", "known"]); // Safety cannot consume/wake.
    expect(h.observed).toEqual([]); // No idle observation tick; only a trusted request reads.
    expect(mesh.get(box.key)?.value).toMatchObject({ pending: { ids: [known.id] } });
    await h.observer.request(); // The normal event path still sees the unknown urgent item.
    expect(h.delivered).toEqual(["first", "known", "lost notification"]);
    expect(mesh.get(box.key)?.value).toMatchObject({ pending: { ids: [unknown.id] } });
  });

  it("matures actual cooldown before the next safety tick without further publication", async () => {
    vi.useFakeTimers(); mockWatch();
    const { mesh, box, work } = fixture(0, 75_000);
    const h = observe(mesh, box);
    await h.observer.request();
    await work("first"); await h.observer.request(); await box.close();
    await vi.advanceTimersByTimeAsync(1_000);
    await work("cooling"); await h.observer.request();
    const dueAt = box.knownWakeDueAt;
    expect(dueAt).toBe(Date.now() + 74_000);
    await vi.advanceTimersByTimeAsync(73_999);
    expect(h.delivered).toEqual(["first"]);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["first", "cooling"]);
  });

  it("arms no timer for unchanged empty and foreign work", async () => {
    vi.useFakeTimers(); mockWatch();
    const { mesh, box } = fixture();
    const h = observe(mesh, box); await h.observer.request();
    expect(box.knownWakeDueAt).toBeUndefined(); expect(vi.getTimerCount()).toBe(0);
    await mesh.publish({ topic: "fleet.work.test.1", to: peer.id, from: peer, text: "foreign" });
    await h.observer.request();
    expect(box.knownWakeDueAt).toBeUndefined(); expect(vi.getTimerCount()).toBe(0);
    const read = vi.spyOn(mesh, "read");
    await vi.advanceTimersByTimeAsync(59_999);
    expect(read).not.toHaveBeenCalled(); expect(h.delivered).toEqual([]);
  });

  it.each(["busy", "preflight", "closed"])("does not rearm or spin an expired hint while %s", async (gate) => {
    vi.useFakeTimers(); mockWatch();
    const { mesh, box, work } = fixture(10_000);
    let idle = true;
    const h = observe(mesh, box, () => idle); await h.observer.request();
    await work("waiting"); await h.observer.request();
    expect(vi.getTimerCount()).toBe(1); // The known deadline only.
    idle = false;
    if (gate === "closed") h.observer.close();
    await vi.advanceTimersByTimeAsync(10_000);
    const calls = h.wake.mock.calls.length;
    h.observer.armKnownDeadline(box.knownWake); // Expired hints never rearm.
    await vi.advanceTimersByTimeAsync(40_000);
    expect(h.wake).toHaveBeenCalledTimes(calls); expect(h.delivered).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
    expect((await box.next(held)).events.map(event => event.text)).toEqual(["waiting"]);
  });

  it("rechecks the gate after await before delivery or arming", async () => {
    vi.useFakeTimers(); mockWatch();
    const { mesh, box, work } = fixture(10_000);
    let idle = true;
    const h = observe(mesh, box, () => idle); await h.observer.request();
    await work("waiting");
    const original = box.wake.bind(box);
    vi.spyOn(box, "wake").mockImplementation(async (...args) => {
      const batch = await original(...args); idle = false; return batch;
    });
    await h.observer.request();
    expect(box.knownWakeDueAt).toBeDefined(); expect(vi.getTimerCount()).toBe(0);
    expect(h.delivered).toEqual([]);
  });
});

describe("root inbox foreign-head grace deadlines", () => {
  const setup = async (backlog: "own" | "foreign" | "stale" | "receipted" | "reserved-name" | "nonwork", pageSize = 500, kind = "ack") => {
    vi.useFakeTimers(); mockWatch();
    const { mesh } = fixture();
    const session = { holdsBatch: () => true, holdsSteer: (_from: string, key: string) => key === "received" };
    const box = new RootInbox(mesh, me, () => [me.id, peer.id], { pageSize });
    boxes.push(box); box.start();
    const delivered: string[] = [];
    let observer!: RootInboxEventWake;
    const wake = vi.fn(async (hint?: RootInboxKnownWake) => {
      const batch = await box.wake(session, () => true, hint);
      delivered.push(...(batch?.events.map(event => event.text ?? "") ?? []));
      observer.armKnownDeadline(box.knownWake);
    });
    observer = new RootInboxEventWake(mesh.root, wake);
    observers.push(observer); observer.start(); await observer.request();
    await vi.advanceTimersByTimeAsync(15_000);
    const headAt = Date.now();
    // One validated fetched page, sequence order deliberately unlike timestamp order.
    const rows = [
      { id: randomUUID(), sequence: 1, topic: "fleet.work.test.1", kind: "ack", from: peer,
        to: "session:other", text: "foreign head", createdAt: headAt },
      { id: randomUUID(), sequence: 2, topic: backlog === "nonwork" ? "fabric.other" : "fleet.work.test.1",
        kind, from: peer, to: backlog === "foreign" ? "session:other" : backlog === "reserved-name" ? peer.id : me.id,
        text: "older backlog", createdAt: headAt - (backlog === "stale" ? 3 * 60 * 60_000 : 120_000),
        ...(backlog === "receipted" ? { data: { key: "received" } } : {}) },
    ];
    fs.appendFileSync(path.join(mesh.root, "events.jsonl"), rows.map(row => JSON.stringify(row)).join("\n") + "\n");
    fs.writeFileSync(path.join(mesh.root, "sequence"), "2");
    const read = vi.spyOn(mesh, "read");
    await observer.request();
    return { mesh, box, observer, wake, delivered, read, dueAt: headAt + 60_000 };
  };

  it.each(["ack", "p0"])("wakes at exact foreign head maturity for older own %s backlog without admitting or skipping the head early", async (kind) => {
    const h = await setup("own", 500, kind);
    expect(h.box.knownWakeDueAt).toBe(h.dueAt);
    expect(h.delivered).toEqual([]);
    expect(h.mesh.get(h.box.key)?.value).toMatchObject({ after: 0 });
    expect(h.mesh.get(h.box.key)?.value).not.toHaveProperty("pending");
    expect(h.read).toHaveBeenCalledTimes(2); // Peek and trusted empty drain, no extra page.
    const readdir = vi.spyOn(fs, "readdirSync");
    const readFile = vi.spyOn(fs, "readFileSync");
    await vi.advanceTimersByTimeAsync(59_999); // No idle tick; only the head deadline is armed.
    expect(h.delivered).toEqual([]);
    expect(h.read).toHaveBeenCalledTimes(2);
    expect(readdir).not.toHaveBeenCalled(); expect(readFile).not.toHaveBeenCalled();
    expect(h.wake).toHaveBeenCalledTimes(2); // Start and notification only.
    await vi.advanceTimersByTimeAsync(1); // t=75s: the exact head deadline.
    expect(h.wake).toHaveBeenCalledTimes(3);
    expect(h.delivered).toEqual(["older backlog"]);
    expect(h.mesh.get(h.box.key)?.value).toMatchObject({ after: 0, pending: { through: 2 } });
  });

  it.each(["foreign", "stale", "receipted", "reserved-name", "nonwork"] as const)("has no short timer for %s backlog behind a foreign head", async (backlog) => {
    const h = await setup(backlog);
    expect(h.box.knownWakeDueAt).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(h.delivered).toEqual([]);
    expect(h.wake).toHaveBeenCalledTimes(2); // Neither an idle tick nor the foreign head wakes.
  });

  it("does not fetch another page to discover own backlog behind a foreign head", async () => {
    const h = await setup("own", 1);
    expect(h.box.knownWakeDueAt).toBeUndefined();
    expect(h.read).toHaveBeenCalledTimes(2);
    expect(h.read.mock.calls.every(([input]) => input?.after === 0 && input.limit === 1)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("root inbox unchanged-log deadline reconciliation", () => {
  it("does not rescan a young shadow on unchanged ticks, but matures it with no new append", async () => {
    const { mesh, box, work, advance } = fixture();
    await work("young");
    const read = vi.spyOn(mesh, "read");
    expect(await box.wake(held, () => true)).toBeUndefined();
    const scanned = read.mock.calls.length;
    for (let n = 0; n < 8; n++) { advance(1_000); expect(await box.wake(held, () => true)).toBeUndefined(); }
    expect(read).toHaveBeenCalledTimes(scanned);
    advance(60_000);
    expect((await box.wake(held, () => true))?.events.map(event => event.text)).toEqual(["young"]);
  });

  it("does not rescan an empty unchanged log on safety ticks", async () => {
    const { mesh, box, advance } = fixture(0);
    await box.wake(held, () => true);
    const read = vi.spyOn(mesh, "read");
    for (let n = 0; n < 6; n++) { advance(60_000); await box.wake(held, () => true); }
    expect(read).not.toHaveBeenCalled();
  });

  it("matures ordinary cooldown with no append and detects urgent work behind a full cooling backlog", async () => {
    const { mesh, box, work, advance } = fixture(0);
    await work("first"); advance(1);
    const first = await box.wake(held, () => true);
    expect(first?.events).toHaveLength(1);
    await box.close(); // Join the wake publication before measuring the unchanged log.
    await work("cooling"); advance(1);
    expect(await box.wake(held, () => true)).toBeUndefined();
    const read = vi.spyOn(mesh, "read");
    for (let n = 0; n < 3; n++) { advance(60_000); expect(await box.wake(held, () => true)).toBeUndefined(); }
    expect(read).not.toHaveBeenCalled();
    advance(120_000);
    expect((await box.wake(held, () => true))?.events.map(event => event.text)).toEqual(["cooling"]);
    for (let n = 0; n < 21; n++) await work(`backlog ${n}`);
    await work("urgent", "p0"); advance(1);
    expect((await box.wake(held, () => true))?.events).toHaveLength(20);
    expect((await box.next(held)).events.map(event => event.text)).toEqual(["backlog 20", "urgent"]);
  });

  it("keeps the pending receipt when idle changes after await, and excludes inherited activation events", async () => {
    const mesh = new MeshStore(path.join(root(), "mesh"), 64 * 1024, 500);
    await mesh.publish({ topic: "fleet.work.test.1", to: me.id, from: peer, text: "inherited" });
    const box = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 }); boxes.push(box); box.start();
    await mesh.publish({ topic: "fleet.work.test.1", to: me.id, from: peer, text: "new" });
    expect(await box.wake(rootInboxSession([]), () => false)).toBeUndefined();
    expect((await box.next(rootInboxSession([]))).events.map(event => event.text)).toEqual(["new"]);
  });
});
