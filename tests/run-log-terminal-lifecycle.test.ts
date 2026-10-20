import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { registerAgentRunner, type FabricHostedReporter, type FabricHostedRunner } from "../src/agents/runner-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const codec = vi.hoisted(() => ({ compress: vi.fn() }));
vi.mock("../src/storage/run-log-compression.js", () => ({ compressTerminalRunLog: codec.compress }));

const roots: string[] = [];
const managers: AgentManager[] = [];
beforeEach(() => { codec.compress.mockReset(); });
const unregister: Array<() => void> = [];
const deferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

afterEach(async () => {
  await Promise.allSettled(managers.splice(0).map(manager => manager.close()));
  for (const dispose of unregister.splice(0)) dispose();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
});

const managerFor = (
  root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-run-log-life-")),
  options: ConstructorParameters<typeof AgentManager>[2] = {},
  config: Partial<ConstructorParameters<typeof AgentManager>[1]> = {},
) => {
  roots.push(root);
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0, maxConcurrent: 4, sessionExport: false,
    childQuestions: "route", ...config,
  }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false, ...options });
  managers.push(manager);
  return manager;
};

const processWorker = () => {
  let alive = true;
  let launch!: AgentTransportLaunch;
  const closed = deferred();
  const transport: AgentTransportHandle = {
    kind: "process", sessionId: String(process.pid + 100000), isAlive: async () => alive,
    stop: vi.fn(async () => { alive = false; closed.resolve(); }), closed: closed.promise,
  };
  const publish = (patch: Partial<AgentRunRecord> = {}) => {
    const now = Date.now();
    const statusFile = launch.workerArguments[launch.workerArguments.indexOf("--status-file") + 1]!;
    fs.writeFileSync(statusFile, JSON.stringify({
      id: launch.id, name: launch.name, task: "terminal lifecycle", runner: "pi", transport: "process",
      sessionId: transport.sessionId, cwd: launch.cwd, status: "running", startedAt: now, updatedAt: now,
      turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, ...patch,
    }));
  };
  return {
    transport, publish,
    launch: async (request: AgentTransportLaunch) => { launch = request; publish(); return transport; },
    exit: () => { alive = false; closed.resolve(); },
  };
};

const hostedRunner = (options: { questions?: boolean; steer?: FabricHostedRunner["steer"]; stopConfirmed?: boolean } = {}) => {
  let reporter!: FabricHostedReporter;
  const adapter: FabricHostedRunner = {
    kind: "hosted", id: "run-log-lifecycle-hosted", label: "run-log hosted",
    capabilities: { recursiveFabric: false, steer: !!options.steer, followUp: false, persistentSessions: false, kernels: false,
      handoff: false, modelDiscovery: false, imageInput: false, compaction: false, questions: options.questions ?? false, sleep: false, writePolicy: false },
    ...(options.steer ? { steer: options.steer } : {}),
    prepare: () => ({ owned: true }),
    start: async (_locator, _context, target) => { reporter = target; },
    attach: async () => {}, liveness: () => "running", stop: () => ({ confirmed: options.stopConfirmed ?? true }),
  };
  unregister.push(registerAgentRunner(adapter));
  return { adapter, reporter: () => reporter };
};

describe("owner-terminal run-log finalization", () => {
  it("waits for native exit and late append, then finalizes exactly once", async () => {
    codec.compress.mockResolvedValue({ kind: "compressed", originalBytes: 40, compressedBytes: 20 });
    const manager = managerFor();
    const fake = processWorker();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(fake.launch);
    const handle = await manager.spawn({ task: "lifecycle", transport: "process", extensions: false });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fs.writeFileSync(logFile, "first\n");
    fake.publish({ status: "completed", text: "answer", finishedAt: Date.now() });
    await expect(manager.finalizeRunLog(handle.id)).rejects.toThrow(/unsettled|writer|quiescen/i);
    fs.appendFileSync(logFile, "late\n");
    fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "answer" });
    await Promise.all([manager.finalizeRunLog(handle.id), manager.finalizeRunLog(handle.id)]);
    expect(codec.compress).toHaveBeenCalledOnce();
    expect(codec.compress).toHaveBeenCalledWith(logFile);
  });

  it("closes a hosted reporter before compression and fences late reports", async () => {
    codec.compress.mockResolvedValue({ kind: "compressed", originalBytes: 40, compressedBytes: 20 });
    const manager = managerFor();
    const fake = hostedRunner();
    const handle = await manager.spawn({ task: "hosted lifecycle", runner: fake.adapter.id });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "before_finish" } });
    fake.reporter().finish({ status: "completed", output: "hosted answer" });
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "before_owner_close" } });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "hosted answer" });
    await manager.finalizeRunLog(handle.id);
    const frozen = fs.readFileSync(logFile, "utf8");
    expect(frozen).toContain("before_finish");
    expect(frozen).toContain("before_owner_close");
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "late" } });
    fake.reporter().progress({ text: "late" });
    expect(frozen).not.toContain('"content":"late"');
    expect(fs.readFileSync(logFile, "utf8")).toBe(frozen);
    expect(codec.compress).toHaveBeenCalledOnce();
  });

  it("automatically finalizes after an aborted hosted question finally drains", async () => {
    const compressed = deferred();
    codec.compress.mockImplementation(async () => {
      compressed.resolve();
      return { kind: "compressed", originalBytes: 40, compressedBytes: 20 };
    });
    const asked = deferred();
    const aborted = deferred();
    const answer = deferred<{ cancelled: true }>();
    const manager = managerFor(undefined, {
      onChildQuestion: request => {
        request.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
        asked.resolve();
        return answer.promise;
      },
    });
    const fake = hostedRunner({ questions: true });
    const handle = await manager.spawn({ task: "held hosted question", runner: fake.adapter.id });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fake.reporter().transcript({ type: "extension_error", error: "before finish" });
    const question = fake.reporter().question({ method: "confirm", title: "Continue?" });
    await asked.promise;
    fake.reporter().finish({ status: "completed", output: "done with question pending" });
    try {
      await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed" });
      await aborted.promise;
      expect(codec.compress).not.toHaveBeenCalled();
      const frozen = fs.readFileSync(logFile, "utf8");
      answer.resolve({ cancelled: true });
      await expect(question).resolves.toEqual({ cancelled: true });
      // The reporter drain itself must wake the owner: no close or manual finalization.
      await compressed.promise;
      expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
      fake.reporter().transcript({ type: "extension_error", error: "late transcript" });
      fake.reporter().progress({ text: "late progress" });
      fake.reporter().finish({ status: "completed", output: "late finish" });
      await expect(fake.reporter().question({ method: "confirm", title: "Late?" }))
        .resolves.toEqual({ cancelled: true });
      expect(fs.readFileSync(logFile, "utf8")).toBe(frozen);
    } finally {
      answer.resolve({ cancelled: true });
      await question;
    }
  });

  it("joins the hosted question drain before immediate finalize/archive publication", async () => {
    const codecEntered = deferred();
    const codecDone = deferred();
    codec.compress.mockImplementation(async () => {
      codecEntered.resolve();
      await codecDone.promise;
      return { kind: "compressed", originalBytes: 40, compressedBytes: 20 };
    });
    const asked = deferred();
    const answer = deferred<{ cancelled: true }>();
    const manager = managerFor(undefined, {
      onChildQuestion: () => { asked.resolve(); return answer.promise; },
    });
    const fake = hostedRunner({ questions: true });
    const handle = await manager.spawn({ task: "immediate hosted archive", runner: fake.adapter.id });
    const directory = manager.runDirectory(handle.id)!;
    const logFile = path.join(directory, "events.jsonl");
    const archiveFile = path.join(directory, "archived-events.jsonl");
    fake.reporter().transcript({ type: "extension_error", error: "archive input" });
    const question = fake.reporter().question({ method: "confirm", title: "Continue?" });
    await asked.promise;
    fake.reporter().finish({ status: "completed", output: "archive me" });
    await manager.wait(handle.id);
    let closeComplete = false;
    const close = manager.close().then(() => { closeComplete = true; });
    let publication = "pending";
    const archive = Promise.all([manager.finalizeRunLog(handle.id), manager.finalizeRunLog(handle.id)])
      .then(() => {
        fs.copyFileSync(logFile, archiveFile);
        publication = "published";
        return undefined;
      }, error => { publication = "failed"; return error; });
    try {
      // One event-loop boundary runs already queued continuations; this is not polling.
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(publication).toBe("pending");
      expect(closeComplete).toBe(false);
      expect(fs.existsSync(archiveFile)).toBe(false);
      expect(codec.compress).not.toHaveBeenCalled();
      answer.resolve({ cancelled: true });
      await expect(question).resolves.toEqual({ cancelled: true });
      await codecEntered.promise;
      expect(publication).toBe("pending");
      expect(closeComplete).toBe(false);
      expect(fs.existsSync(archiveFile)).toBe(false);
      codecDone.resolve();
      await expect(archive).resolves.toBeUndefined();
      await close;
      expect(closeComplete).toBe(true);
      expect(publication).toBe("published");
      expect(fs.readFileSync(archiveFile, "utf8")).toBe(fs.readFileSync(logFile, "utf8"));
      expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    } finally {
      answer.resolve({ cancelled: true });
      codecDone.resolve();
      await question;
      await archive;
      await close;
    }
  });

  it("ignores an in-flight hosted control failure after reporter closure", async () => {
    codec.compress.mockImplementation(async (logFile: string) => {
      // Model codec publication/removal without invoking the independently owned codec.
      fs.renameSync(logFile, `${logFile}.zst`);
      return { kind: "compressed", originalBytes: 40, compressedBytes: 20 };
    });
    const deliveryStarted = deferred();
    const delivery = deferred();
    const fake = hostedRunner({
      steer: async () => {
        deliveryStarted.resolve();
        await delivery.promise;
        throw new Error("late control failure");
      },
    });
    const manager = managerFor();
    const handle = await manager.spawn({ task: "late hosted control", runner: fake.adapter.id });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fake.reporter().transcript({ type: "extension_error", error: "before control" });
    manager.steer(handle.id, "held control");
    await deliveryStarted.promise;
    fake.reporter().finish({ status: "completed", output: "done" });
    try {
      await manager.wait(handle.id);
      await manager.finalizeRunLog(handle.id);
      const frozen = fs.readFileSync(`${logFile}.zst`, "utf8");
      delivery.resolve();
      fake.reporter().transcript({ type: "extension_error", error: "late transcript" });
      // The held adapter rejection and its logging continuation are microtasks.
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(fs.existsSync(logFile)).toBe(false);
      expect(fs.readFileSync(`${logFile}.zst`, "utf8")).toBe(frozen);
      expect(codec.compress).toHaveBeenCalledOnce();
    } finally {
      delivery.resolve();
    }
  });

  it.each([false, true])("joins an owned codec on close before collection (codec fails: %s)", async fails => {
    const codecEntered = deferred();
    const codecDone = deferred();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    codec.compress.mockImplementation(async () => {
      codecEntered.resolve();
      await codecDone.promise;
      if (fails) throw new Error("held codec failure");
      return { kind: "compressed", originalBytes: 40, compressedBytes: 20 };
    });
    const manager = managerFor(undefined, {}, { retainRuns: false });
    const fake = processWorker();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(fake.launch);
    const handle = await manager.spawn({ task: "codec close custody", transport: "process", extensions: false });
    const directory = manager.runDirectory(handle.id)!;
    const logFile = path.join(directory, "events.jsonl");
    fs.writeFileSync(logFile, "retained source\n");
    fake.publish({ status: "completed", text: "preserved worker result", finishedAt: Date.now() });
    fake.exit();
    await manager.wait(handle.id);
    await codecEntered.promise;
    let closeComplete = false;
    const close = manager.close().then(() => { closeComplete = true; });
    try {
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(closeComplete).toBe(false);
      expect(fs.readFileSync(logFile, "utf8")).toBe("retained source\n");
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
        .toMatchObject({ status: "completed", text: "preserved worker result" });
      codecDone.resolve();
      await close;
      expect(closeComplete).toBe(true);
      expect(codec.compress).toHaveBeenCalledOnce();
      if (fails) {
        expect(fs.readFileSync(logFile, "utf8")).toBe("retained source\n");
        await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "preserved worker result" });
        await expect(manager.finalizeRunLog(handle.id)).rejects.toMatchObject({ code: "run-log-codec" });
        expect(() => manager.readLog(handle.id)).toThrow("held codec failure");
      } else {
        expect(fs.existsSync(directory)).toBe(false);
      }
    } finally {
      codecDone.resolve();
      await close;
    }
  });

  it("keeps an indeterminate hosted stop uncompressed at close", async () => {
    const manager = managerFor(undefined, {}, { retainRuns: false });
    const fake = hostedRunner({ stopConfirmed: false });
    const handle = await manager.spawn({ task: "unconfirmed hosted stop", runner: fake.adapter.id });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fake.reporter().transcript({ type: "extension_error", error: "preserve unconfirmed source" });
    await expect(manager.stop(handle.id)).resolves.toMatchObject({ status: "stopped", outcome: "indeterminate" });
    await manager.close();
    expect(codec.compress).not.toHaveBeenCalled();
    expect(fs.readFileSync(logFile, "utf8")).toContain("preserve unconfirmed source");
    await expect(manager.finalizeRunLog(handle.id)).rejects.toThrow(/writer exit is unconfirmed/);
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "stopped", outcome: "indeterminate" });
  });

  it("retains lost-contact native source and custody on close without starting a codec", async () => {
    const manager = managerFor(undefined, {}, { retainRuns: false });
    const fake = processWorker();
    Object.assign(fake.transport, { lostContact: () => "native exit identity unconfirmed" });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(fake.launch);
    const handle = await manager.spawn({ task: "lost native custody", transport: "process", extensions: false });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fs.writeFileSync(logFile, "preserve lost source\n");
    fake.publish({ status: "completed", text: "cannot transfer custody", finishedAt: Date.now() });
    fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
    await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
    expect(codec.compress).not.toHaveBeenCalled();
    expect(fs.readFileSync(logFile, "utf8")).toBe("preserve lost source\n");
    await expect(manager.finalizeRunLog(handle.id)).rejects.toThrow(/writer exit is unconfirmed/);
  });

  it("counts each oversized skip once and distinguishes other skips", async () => {
    const warns = vi.spyOn(console, "warn").mockImplementation(() => {});
    codec.compress
      .mockResolvedValueOnce({ kind: "skipped", reason: "plain-too-large", originalBytes: 134217729 })
      .mockResolvedValueOnce({ kind: "skipped", reason: "plain-too-large", originalBytes: 134217730 })
      .mockResolvedValueOnce({ kind: "skipped", reason: "codec-unavailable", originalBytes: 12 });
    const manager = managerFor();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const fake = processWorker();
      const transport = await fake.launch(request);
      queueMicrotask(() => { fake.publish({ status: "completed", text: "done", finishedAt: Date.now() }); fake.exit(); });
      return transport;
    });
    for (let index = 0; index < 3; index++) {
      const handle = await manager.spawn({ task: `skip-${index}`, transport: "process", extensions: false });
      await manager.wait(handle.id);
      await manager.finalizeRunLog(handle.id);
      await manager.finalizeRunLog(handle.id);
    }
    const messages = warns.mock.calls.map(([message]) => String(message));
    expect(messages.filter(message => message.includes("plain-too-large"))).toHaveLength(2);
    expect(messages[0]).toContain("oversizedSkips=1");
    expect(messages[1]).toContain("oversizedSkips=2");
    expect(messages.some(message => message.includes("codec-unavailable") && message.includes("oversizedSkips"))).toBe(false);
  });
});
