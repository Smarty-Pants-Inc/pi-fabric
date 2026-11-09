// Keep: observation/cancellation trust boundary and unread-result data safety.
// Exercise the public native/durable provider and hosted dispatch with fake time,
// no worker process, no native deletion, and no access to the user's alpha flag.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { AgentService } from "../src/agents/service.js";
import { createAgentsProvider, createAgentServiceClient, createAgentServiceHandler } from "../src/agents/service-provider.js";
import type { AgentExecutionRequest, AgentExecutionResponse } from "../src/agents/service-types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { AgentWaitBoundError } from "../src/agents/wait-bound.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { ResidencyClient } from "../src/residency/client.js";

const fixture = vi.hoisted(() => ({ home: "/fixture-wait-home" }));
vi.mock("node:os", async original => ({ ...await original<typeof import("node:os")>(), homedir: () => fixture.home }));
vi.mock("node:fs", async original => {
  const real = await original<typeof import("node:fs")>();
  return { ...real, existsSync: (file: fs.PathLike) => String(file) === path.join(fixture.home, ".config/smarty/bash-guard.disabled") ? false : real.existsSync(file) };
});

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
};
const outcome = <T>(promise: Promise<T>) => {
  let settled = false;
  const result = promise.then(value => { settled = true; return { value, error: undefined }; }, error => {
    settled = true; return { value: undefined, error };
  });
  return { result, settled: () => settled };
};
const managers: AgentManager[] = [];
const services: AgentService[] = [];
const releases: Array<() => void> = [];
const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 };
type ProviderArgs = ConstructorParameters<typeof AgentsProvider>;
const context = (mode = "rpc", signal?: AbortSignal, mainDeadlineAt?: number): FabricInvocationContext => ({
  cwd: process.cwd(), signal, mainDeadlineAt, update() {}, activity() {},
  extensionContext: { mode, sessionManager: { getSessionId: () => "wait-session" } },
}) as unknown as FabricInvocationContext;
const providerFor = (manager: AgentManager, residency?: ResidencyClient) => {
  const main = { id: "main", local: true, flushHeldAtNextBoundary: vi.fn() };
  const provider = new AgentsProvider(manager, { identity: { id: "task", kind: "agent" } } as ProviderArgs[1],
    {} as ProviderArgs[2], main as unknown as ProviderArgs[3],
    { scheduleRefresh() {} } as unknown as ProviderArgs[4], undefined, {} as ProviderArgs[6], () => false, residency, false);
  return { provider, main };
};
const native = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wait-alpha-"));
  const closed = deferred<void>();
  let alive = true;
  let finish!: () => void;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    finish = () => {
      if (!alive) return;
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record: AgentRunResult = { id: request.id, name: request.name, task: "wait fixture", cwd: request.cwd,
        runner: "pi", transport: "process", status: "completed", startedAt: Date.now(), updatedAt: Date.now(),
        finishedAt: Date.now(), turns: 1, toolCalls: 0, text: "done", usage };
      writeJsonAtomic(file, record);
      alive = false;
      closed.resolve();
    };
    return { kind: "process", sessionId: "2147483647", closed: closed.promise, isAlive: async () => alive,
      waitForClose: () => closed.promise, stop: async () => { finish(); } };
  });
  const consumed = vi.fn();
  const completed = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0,
    maxConcurrent: 1, retainRuns: true, sessionExport: false }, {
    runRoot: root, fullCodeMode: false, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    onResultConsumed: consumed, onBackgroundComplete: completed,
  });
  managers.push(manager);
  const handle = await manager.spawn({ task: "held wait fixture", transport: "process", extensions: false });
  releases.push(() => finish());
  return { ...providerFor(manager), manager, handle, finish, consumed, completed };
};
const durable = () => {
  const done = deferred<AgentRunResult>();
  const status = { id: "durable", name: "held durable", status: "running" };
  const acknowledgeCompletion = vi.fn();
  const waitAgent = vi.fn((_id: string, signal?: AbortSignal, consume?: (ack: () => void) => void) => new Promise((resolve, reject) => {
    const abort = () => reject(signal?.reason ?? new Error("aborted"));
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    done.promise.then(record => {
      signal?.removeEventListener("abort", abort);
      if (signal?.aborted) return;
      if (consume) consume(acknowledgeCompletion); else acknowledgeCompletion();
      resolve(record);
    });
  }));
  const residency = { hasAgent: () => true, statusAgent: () => status, waitAgent, acknowledgeCompletion } as unknown as ResidencyClient;
  // Only constructor metadata is needed; no native execution is launched.
  const manager = { cwd: process.cwd() } as AgentManager;
  return { ...providerFor(manager, residency), waitAgent, acknowledgeCompletion, done, status };
};
const hosted = async (options: Partial<ConstructorParameters<typeof AgentService>[0]> = {}, executionTimeoutMs?: number) => {
  const done = deferred<AgentExecutionResponse>();
  const entered = deferred<AgentExecutionRequest>();
  const service = new AgentService({ rootId: "root", port: { execute: async request => {
    entered.resolve(request);
    request.signal.addEventListener("abort", () => done.resolve({ status: "stopped" }), { once: true });
    return done.promise;
  } }, ...options });
  services.push(service);
  const handle = await service.spawn("root", { task: "held hosted", ...(executionTimeoutMs ? { timeoutMs: executionTimeoutMs } : {}) });
  const execution = await entered.promise;
  const dispatch = createAgentServiceHandler(service, "root");
  const client = createAgentServiceClient(dispatch);
  return { service, handle, execution, done, client, provider: createAgentsProvider(client), dispatch };
};
const clock = () => vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
beforeEach(() => {
  vi.stubEnv("SMARTY_ALPHA", "1");
  vi.stubEnv("PI_FABRIC_PARENT_RUN", "parent");
  vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET", "PI_FABRIC_WRITE_POLICY"])
    vi.stubEnv(key, undefined);
  // AbortSignal.timeout uses Node's internal timers, so explicitly bridge it to
  // the fake clock rather than spending five real minutes in a regression test.
  vi.spyOn(AbortSignal, "timeout").mockImplementation(ms => {
    const controller = new AbortController();
    setTimeout(() => controller.abort(new DOMException("Wait bound", "TimeoutError")), ms).unref?.();
    return controller.signal;
  });
  // Record cleanup only. No fixture or production deletion runs in this suite.
  vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  vi.spyOn(fs, "unlinkSync").mockImplementation(() => {});
  vi.spyOn(fs, "rmdirSync").mockImplementation(() => {});
  vi.spyOn(fs.promises, "rm").mockImplementation(async () => {});
});
afterEach(async () => {
  vi.useRealTimers();
  for (const release of releases.splice(0)) release();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(services.splice(0).map(service => service.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("alpha native observation", () => {
  it.each(["wait", "join"])("honors an explicit non-Main %s beyond five minutes", async action => {
    const state = await native();
    clock();
    const waiting = outcome(state.provider.invoke(action, { id: state.handle.id, timeoutMs: 420_000 }, context()));
    await vi.advanceTimersByTimeAsync(300_001);
    expect(waiting.settled()).toBe(false);
    expect(state.consumed).not.toHaveBeenCalled();
    expect(state.main.flushHeldAtNextBoundary).not.toHaveBeenCalled();
    state.finish();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await waiting.result).toMatchObject({ value: { status: "completed", text: "done" } });
  });

  it.each([undefined, 1_000, 420_000])("returns an early live handle at bound %s without metrics failure, consumption or Main flushing", async timeoutMs => {
    const state = await native();
    expect(state.manager.status(state.handle.id)).not.toHaveProperty("usage");
    const deferResultConsumption = vi.fn();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs }, { ...context(), deferResultConsumption }));
    await vi.advanceTimersByTimeAsync(timeoutMs ?? 300_000);
    expect(await waiting.result).toMatchObject({ value: { id: state.handle.id, status: "running", waitTimedOut: true } });
    expect(state.consumed).not.toHaveBeenCalled();
    expect(deferResultConsumption).not.toHaveBeenCalled();
    expect(state.main.flushHeldAtNextBoundary).not.toHaveBeenCalled();
    state.finish();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(state.completed).toHaveBeenCalledOnce();
    expect(await state.manager.wait(state.handle.id)).toMatchObject({ status: "completed" });
    expect(state.consumed).toHaveBeenCalledOnce();
  });

  it.each(["1", "0"])("forwards non-Main caller/execution cancellation with alpha=%s", async alpha => {
    vi.stubEnv("SMARTY_ALPHA", alpha);
    const state = await native();
    const caller = new AbortController();
    const reason = new Error("execution deadline reached");
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs: 420_000 }, context("rpc", caller.signal)));
    await vi.advanceTimersByTimeAsync(1);
    caller.abort(reason);
    await vi.advanceTimersByTimeAsync(1);
    expect(waiting.settled()).toBe(true);
    expect(await waiting.result).toMatchObject({ error: reason });
    expect(state.manager.isSettled(state.handle.id)).toBe(false);
    expect(state.consumed).not.toHaveBeenCalled();
  });

  it.each(["1", "0"])("keeps interactive Main's exact 60 s pending bound with alpha=%s", async alpha => {
    vi.stubEnv("SMARTY_ALPHA", alpha);
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    const state = await native();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs: 420_000 }, context()));
    await vi.advanceTimersByTimeAsync(59_999);
    expect(waiting.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(await waiting.result).toMatchObject({ value: { status: "running", waitTimedOut: true, note: expect.stringContaining("Main waits are capped at 60 s") } });
    expect(state.main.flushHeldAtNextBoundary).toHaveBeenCalledOnce();
    expect(state.consumed).not.toHaveBeenCalled();
  });

  it.each(["print", "json"])("does not mistake a root %s script for interactive Main", async mode => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    const state = await native();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs: 420_000 }, context(mode)));
    await vi.advanceTimersByTimeAsync(300_001);
    expect(waiting.settled()).toBe(false);
    state.finish();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await waiting.result).toMatchObject({ value: { status: "completed" } });
    expect(state.main.flushHeldAtNextBoundary).not.toHaveBeenCalled();
  });

  it("preserves the absolute Main budget reserve rather than granting a fresh 60 s", async () => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    const state = await native();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs: 420_000 }, context("rpc", undefined, Date.now() + 10_000)));
    await vi.advanceTimersByTimeAsync(7_999);
    expect(waiting.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(await waiting.result).toMatchObject({ value: { waitTimedOut: true } });
    expect(state.consumed).not.toHaveBeenCalled();
  });

  it("does not turn a caller's bound-shaped cancellation reason into pending", async () => {
    const state = await native();
    const caller = new AbortController();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id }, context("rpc", caller.signal)));
    await vi.advanceTimersByTimeAsync(1);
    const reason = new AgentWaitBoundError("caller deadline, not observation bound");
    caller.abort(reason);
    expect((await waiting.result).error).toBe(reason);
    expect(state.consumed).not.toHaveBeenCalled();
  });

  it("retains the five-minute cap and error when alpha is OFF", async () => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    const state = await native();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs: 420_000 }, context()));
    await vi.advanceTimersByTimeAsync(300_000);
    expect((await waiting.result).error).toBeInstanceOf(AgentWaitBoundError);
    expect(state.consumed).not.toHaveBeenCalled();
  });

  it("keeps the direct manager wait uncapped and its bounded-result sentinel unconsumed", async () => {
    const state = await native();
    clock();
    const waiting = outcome(state.manager.wait(state.handle.id, { timeoutMs: 420_000 }));
    await vi.advanceTimersByTimeAsync(300_001);
    expect(waiting.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(119_999);
    expect((await waiting.result).error).toBeInstanceOf(AgentWaitBoundError);
    expect(state.consumed).not.toHaveBeenCalled();
  });
});

describe("alpha durable observation", () => {
  it.each(["wait", "join"])("honors explicit non-Main %s beyond five minutes, then returns pending at its requested bound", async action => {
    const state = durable();
    const deferResultConsumption = vi.fn();
    clock();
    const waiting = outcome(state.provider.invoke(action, { id: "durable", timeoutMs: 420_000 }, { ...context(), deferResultConsumption }));
    await vi.advanceTimersByTimeAsync(300_001);
    expect(waiting.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(119_999);
    expect(await waiting.result).toMatchObject({ value: { ...state.status, waitTimedOut: true } });
    expect(state.acknowledgeCompletion).not.toHaveBeenCalled();
    expect(deferResultConsumption).not.toHaveBeenCalled();
    expect(state.main.flushHeldAtNextBoundary).not.toHaveBeenCalled();
  });

  it.each(["1", "0"])("preserves caller cancellation instead of pending with alpha=%s", async alpha => {
    vi.stubEnv("SMARTY_ALPHA", alpha);
    const state = durable();
    const caller = new AbortController();
    const reason = new Error("execution expired");
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: "durable", timeoutMs: 420_000 }, context("rpc", caller.signal)));
    await vi.advanceTimersByTimeAsync(1);
    caller.abort(reason);
    expect((await waiting.result).error).toBe(reason);
    expect(state.acknowledgeCompletion).not.toHaveBeenCalled();
  });

  it.each(["1", "0"])("keeps durable interactive Main at 60 s with alpha=%s", async alpha => {
    vi.stubEnv("SMARTY_ALPHA", alpha);
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    const state = durable();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: "durable", timeoutMs: 420_000 }, context()));
    await vi.advanceTimersByTimeAsync(59_999);
    expect(waiting.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(await waiting.result).toMatchObject({ value: { waitTimedOut: true } });
    expect(state.main.flushHeldAtNextBoundary).toHaveBeenCalledOnce();
    expect(state.acknowledgeCompletion).not.toHaveBeenCalled();
  });

  it.each([undefined, 420_000])("keeps OFF default/cap errors for request %s", async timeoutMs => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    const state = durable();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: "durable", timeoutMs }, context()));
    await vi.advanceTimersByTimeAsync(300_000);
    expect((await waiting.result).error).toBeInstanceOf(AgentWaitBoundError);
    expect(state.acknowledgeCompletion).not.toHaveBeenCalled();
  });

  it("returns pending at the alpha default without acknowledging a terminal notification", async () => {
    const state = durable();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: "durable" }, context("print")));
    await vi.advanceTimersByTimeAsync(300_000);
    expect(await waiting.result).toMatchObject({ value: { ...state.status, waitTimedOut: true } });
    expect(state.acknowledgeCompletion).not.toHaveBeenCalled();
  });
});

describe("alpha hosted receiver observation", () => {
  it.each(["wait", "join"] as const)("honors an explicit %s beyond five minutes through the bound caller/client", async action => {
    const state = await hosted();
    clock();
    const waiting = outcome(state.client[action](state.handle.id, undefined, 420_000));
    await vi.advanceTimersByTimeAsync(300_001);
    expect(waiting.settled()).toBe(false);
    state.done.resolve({ status: "completed", text: "done", checkpoint: { private: true } });
    await vi.advanceTimersByTimeAsync(1);
    const result = await waiting.result;
    expect(result).toMatchObject({ value: { status: "completed", text: "done" } });
    expect(result.value).not.toHaveProperty("checkpoint");
  });

  it.each([undefined, 1_000, 420_000])("returns only authorized public live status at bound %s, then preserves the eventual result", async timeoutMs => {
    const state = await hosted();
    clock();
    const waiting = outcome(state.provider.invoke("wait", { id: state.handle.id, timeoutMs }, context()));
    await vi.advanceTimersByTimeAsync(timeoutMs ?? 300_000);
    expect(await waiting.result).toMatchObject({ value: { id: state.handle.id, status: "running" } });
    expect(state.execution.signal.aborted).toBe(false);
    state.done.resolve({ status: "completed", text: "later", checkpoint: { private: true } });
    expect(await state.client.wait(state.handle.id)).toMatchObject({ status: "completed", text: "later" });
  });

  it.each(["1", "0"])("retains hosted caller cancellation with alpha=%s", async alpha => {
    vi.stubEnv("SMARTY_ALPHA", alpha);
    const state = await hosted();
    const caller = new AbortController();
    clock();
    const waiting = outcome(state.provider.invoke("join", { id: state.handle.id, timeoutMs: 420_000 }, context("rpc", caller.signal)));
    await vi.advanceTimersByTimeAsync(1);
    caller.abort();
    expect((await waiting.result).error).toBeInstanceOf(Error);
    expect(state.execution.signal.aborted).toBe(false);
  });

  it.each([undefined, 420_000])("keeps hosted OFF default/cap errors for request %s", async timeoutMs => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    const state = await hosted();
    clock();
    const waiting = outcome(state.client.wait(state.handle.id, undefined, timeoutMs));
    await vi.advanceTimersByTimeAsync(300_000);
    expect((await waiting.result).error).toMatchObject({ message: expect.stringContaining("still running after 5 min") });
    expect(state.execution.signal.aborted).toBe(false);
  });

  it("does not extend the child's execution deadline when a longer observation was requested", async () => {
    // Capture the execution timer installed by spawn as well as the observation timer.
    clock();
    const timed = await hosted({}, 300_000);
    const waiting = outcome(timed.client.wait(timed.handle.id, undefined, 420_000));
    await vi.advanceTimersByTimeAsync(300_001);
    expect(await waiting.result).toMatchObject({ value: { status: "timed_out" } });
    expect(timed.execution.signal.aborted).toBe(true);
  });

  it("rechecks authority before exposing pending status, and rejects forged receiver callers", async () => {
    let allowed = true;
    const state = await hosted({ assertAuthority() { if (!allowed) throw new Error("lease revoked"); } });
    clock();
    const waiting = outcome(state.client.wait(state.handle.id));
    await vi.advanceTimersByTimeAsync(1);
    allowed = false;
    await vi.advanceTimersByTimeAsync(299_999);
    expect((await waiting.result).error).toMatchObject({ message: "lease revoked" });
    allowed = true;
    await expect(createAgentServiceHandler(state.service, "forged")("wait", { id: state.handle.id })).rejects.toThrow("Unknown agent caller");
    await expect(state.dispatch("wait", { id: state.handle.id, timeoutMs: "not-a-timeout" })).rejects.toThrow("Invalid hosted");
  });
});
