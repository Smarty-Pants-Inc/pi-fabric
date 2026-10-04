import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { ProgramStore } from "../src/programs/store.js";
import { ProgramsProvider } from "../src/providers/programs-provider.js";

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-saved-budget-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const registry = new ActionRegistry(); cleanups.push(() => registry.close());
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.executor.timeoutMs = 250; config.agents.timeoutMs = 250;
  config.executor.humanWaitRefs = ["demo.ask"];
  config.approvals.read = "allow"; config.approvals.agent = "allow";
  const descriptor = (name: string) => ({ name, description: name, risk: "read" as const, inputSchema: { type: "object", additionalProperties: true } });
  const ask = vi.fn(async (_args: Record<string, unknown>, signal: AbortSignal) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => resolve("answer"), 500);
    const abort = () => { clearTimeout(timer); reject(signal.reason); };
    if (signal.aborted) abort(); else signal.addEventListener("abort", abort, { once: true });
  }));
  registry.register({ name: "demo", description: "test", async list() { return [descriptor("ask"), descriptor("delay")]; }, async describe(name) { return descriptor(name); },
    async invoke(name, args, context) { return name === "ask" ? ask(args, context.signal!) : new Promise(resolve => setTimeout(() => resolve("delayed"), Number(args.ms))); } });
  const service = new FabricExecutionService(registry, config);
  const store = new ProgramStore(path.join(root, "programs"));
  registry.register(new ProgramsProvider(store, () => "typescript", id => service.nestedProgramRunner(id)));
  const context = { cwd: root, mode: "print", hasUI: false, sessionManager: { getSessionId: () => "saved-budget" } } as unknown as ExtensionContext;
  const run = (code: string, signal?: AbortSignal, tokenBudget?: number) => service.execute({ code, context, signal, ...(tokenBudget !== undefined ? { tokenBudget } : {}), parentToolCallId: "saved-budget", onPartial() {} });
  return { config, service, store, registry, context, run, ask };
};

describe("saved program enclosing budgets", () => {
  it.each(["quickjs", "node-process"] as const)("A11 shares sequential saved-program spending and observations in %s", async runtime => {
    const f = fixture(); f.config.executor.runtime = runtime; f.config.executor.timeoutMs = 3000; f.config.agents.timeoutMs = 3000;
    const run = vi.fn(async () => ({ status: "completed", text: "ok", usage: { input: 3, output: 2 } }));
    const descriptor = { name: "run", description: "fake agent", risk: "agent" as const, inputSchema: { type: "object", additionalProperties: true } };
    f.registry.register({ name: "agents", description: "agents", async list() { return [descriptor]; }, async describe() { return descriptor; }, invoke: run });
    await f.store.save({ name: "agent", code: 'await workflow.agent("nested"); return workflow.budget.spent();' }, "typescript");
    const result = await f.run('await workflow.agent("outer"); const nested = await programs.run({ ref: "agent" }); const spent = workflow.budget.spent(); const remaining = workflow.budget.remaining(); let denied = false; try { await programs.run({ ref: "agent" }); } catch { denied = true; } return { nested, spent, remaining, denied };', undefined, 10);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({ nested: 10, spent: 10, remaining: 0, denied: true });
    expect(run).toHaveBeenCalledTimes(2);
  });
  it("A11 accounts spending from a nested program even when that program fails", async () => {
    const f = fixture();
    const descriptor = { name: "run", description: "fake agent", risk: "agent" as const, inputSchema: { type: "object", additionalProperties: true } };
    f.registry.register({ name: "agents", description: "agents", async list() { return [descriptor]; }, async describe() { return descriptor; }, async invoke() { return { status: "completed", text: "ok", usage: { input: 3, output: 2 } }; } });
    await f.store.save({ name: "failed", code: 'await workflow.agent("nested"); throw new Error("after spending");' }, "typescript");
    const result = await f.run('try { await programs.run({ ref: "failed" }); } catch {} return workflow.budget.spent();', undefined, 5);
    expect(result.success, result.error).toBe(true); expect(result.value).toBe(5);
  });
  it("A11 refuses a first nested workflow agent after the caller exhausts its token budget", async () => {
    const f = fixture();
    const run = vi.fn(async () => ({ status: "completed", text: "ok", usage: { input: 4, output: 1 } }));
    const descriptor = { name: "run", description: "fake agent", risk: "agent" as const, inputSchema: { type: "object", additionalProperties: true } };
    f.registry.register({ name: "agents", description: "agents", async list() { return [descriptor]; }, async describe() { return descriptor; }, invoke: run });
    await f.store.save({ name: "agent", code: 'return await workflow.agent("nested");' }, "typescript");
    const result = await f.run('await workflow.agent("outer"); return await programs.run({ ref: "agent" });', undefined, 5);
    expect(result.success).toBe(false); expect(result.error).toMatch(/token budget exhausted/);
    expect(run).toHaveBeenCalledOnce();
  });
  it("A10 pauses every enclosing deadline during nested questions", async () => {
    const f = fixture();
    await f.store.save({ name: "question", code: 'return await tools.call({ ref: "demo.ask", args: {} });' }, "typescript");
    await f.store.save({ name: "wrapper", code: 'return await programs.run({ ref: "question" });' }, "typescript");
    const result = await f.run('return await programs.run({ ref: "wrapper" });');
    expect(result.success, result.error).toBe(true); expect(result.value).toBe("answer");
  });
  it("A10 resumes remaining executable budget after the nested answer", async () => {
    const f = fixture();
    await f.store.save({ name: "question", code: 'return await tools.call({ ref: "demo.ask", args: {} });' }, "typescript");
    const result = await f.run('await programs.run({ ref: "question" }); return await tools.call({ ref: "demo.delay", args: { ms: 400 } });');
    expect(f.ask).toHaveBeenCalledOnce(); expect(result.success).toBe(false); expect(result.error).toMatch(/timed out/);
  });
  it("A10 preserves explicit cancellation while nested human waits are paused", async () => {
    const f = fixture(); const abort = new AbortController();
    f.ask.mockImplementation(async (_args, signal) => { abort.abort(new Error("operator stop")); signal.throwIfAborted(); });
    await f.store.save({ name: "question", code: 'return await tools.call({ ref: "demo.ask", args: {} });' }, "typescript");
    const result = await f.run('return await programs.run({ ref: "question" });', abort.signal);
    expect(f.ask).toHaveBeenCalledOnce(); expect(result.success).toBe(false);
  });
  it("A10 does not extend interactive Main's fixed ceiling for nested human waits", async () => {
    const f = fixture(); f.context.mode = "rpc"; f.config.executor.mainMaxTimeoutMs = 200;
    await f.store.save({ name: "question", code: 'return await tools.call({ ref: "demo.ask", args: {} });' }, "typescript");
    const result = await f.run('return await programs.run({ ref: "question" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/Main ceiling hit/);
  });
});
