import { afterEach, describe, expect, it, vi } from "vitest";
import { setTimeout as delay } from "node:timers/promises";
import { setupJev, launch, callProgram, jevContext } from "./jev-test-helpers.js";
import type { JevProvider } from "../src/providers/jev-provider.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";
import { codeUsesOrchestration, isBlockingOrchestrationRef } from "../src/runtime/orchestration.js";
const providers: JevProvider[] = [];
const setup = (config: Record<string, unknown> = {}, fetcher?: typeof fetch) => {
  const s = setupJev(config, fetcher); providers.push(s.provider); return s;
};
afterEach(async () => { await Promise.all(providers.splice(0).map(p => p.close())); });
describe("Jev reactive programs", () => {
  it("exposes all lifecycle actions and inferred Choice/Noul/Score types", async () => {
    const { provider } = setup();
    expect((await provider.list({})).map(d => d.name)).toEqual(["evaluate", "run", "spawn", "status", "wait", "join", "advise", "stop"]);
    const checked = typeCheckFabricCode(`const r = await jev.evaluate({state: "hello", questions: {
      yes: {type:"noul", instructions:"Is this a greeting?"},
      pick: {type:"choice", instructions:"Which?", criteria:{a:null,b:null}},
      rank: {type:"score", instructions:"Degree?", criteria:["low","high"]}
    }}); const n: number = r.answers.yes.noul; const c: "a" | "b" = r.answers.pick.choice;
    const s: number = r.answers.rank.score; const status = await jev.status({id:"x"}); return status.state;`, guestTypeDeclarations(true));
    expect(checked.errors).toEqual([]);
    expect(codeUsesOrchestration("await jev.run({})")).toBe(true);
    expect(isBlockingOrchestrationRef("jev.join")).toBe(true);
  });
  it("runs a stateful loop with input/output schemas and progress", async () => {
    const { provider } = setup();
    const run = await callProgram(provider, "run", launch(`let total = input.start;
      for (let i=0; i<3; i++) { total++; await program.emit({total}); await program.sleep(1); }
      return { total };`, {
      inputSchema: { type: "object", properties: { start: { type: "integer" } }, required: ["start"], additionalProperties: false },
      outputSchema: { type: "object", properties: { total: { type: "integer" } }, required: ["total"], additionalProperties: false },
    }, { start: 2 }));
    expect(run.state).toBe("completed"); expect(run.result).toEqual({ total: 5 });
    expect(run.events).toHaveLength(3);
    expect(provider.manager.status(run.id, 2).events).toHaveLength(1);
  });
  it.each(["wait", "join"])("detaches spawn from caller cancellation; %s cancellation does not stop it", async (method) => {
    const { provider } = setup(); const caller = new AbortController();
    const run = await callProgram(provider, "spawn", launch("await program.sleep(80); return 7;"), jevContext(caller.signal));
    expect(run.state).toBe("running"); caller.abort();
    await expect(provider.invoke(method, {id: run.id}, jevContext(caller.signal))).rejects.toThrow();
    const result = await provider.invoke(method, {id: run.id}, jevContext());
    expect(result).toEqual(await provider.manager.wait(run.id));
    expect(result).toEqual(await provider.manager.join(run.id));
    expect(result).toMatchObject({state: "completed", result: 7});
  });
  it("holds a self-reload for an unread background result, for a bounded time (smarty-dev#2216)", async () => {
    const { provider } = setup(); const manager = provider.manager;
    const run = await callProgram(provider, "spawn", launch("return 7;"));
    while (manager.list()[0]?.state === "running") await delay(5);
    await delay(5); // the run's finally releases its lease and sets endedAt
    // A reload now would clear the run: a later jev.wait says "Unknown or expired Jev run ID".
    expect(manager.runningCount()).toBe(1);
    expect(manager.runningCount(Date.now() + 10 * 60_000)).toBe(0); // an unread result cannot hold it forever
    expect(manager.status(run.id)).toMatchObject({ state: "completed", result: 7 });
    expect(manager.runningCount()).toBe(0);
    const waited = await callProgram(provider, "spawn", launch("return 8;"));
    await manager.wait(waited.id);
    expect(manager.runningCount()).toBe(0);
  });
  it("bounds the unread result hold even while terminal lease cleanup is delayed (smarty-dev#2216)", async () => {
    const { provider, registry } = setup();
    const manager = provider.manager;
    let release!: () => void, reached!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const releasing = new Promise<void>(resolve => { reached = resolve; });
    const acquire = registry.acquireCapabilityView.bind(registry);
    vi.spyOn(registry, "acquireCapabilityView").mockImplementation(async (...args) => {
      const lease = await acquire(...args);
      return { ...lease, release: async () => { reached(); await gate; await lease.release(); } };
    });
    const run = await callProgram(provider, "spawn", launch("return 7;"));
    try {
      await releasing;
      expect(manager.list()[0]?.state).toBe("completed");
      expect(manager.runningCount()).toBe(1);
      expect(manager.runningCount(Date.now() + 10 * 60_000)).toBe(0);
      expect(manager.status(run.id)).toMatchObject({ state: "completed", result: 7 });
      expect(manager.runningCount()).toBe(0);
    } finally { release(); await manager.wait(run.id); }
  });

  it("stops a long-lived loop and is idempotent", async () => {
    const { provider } = setup();
    const run = await callProgram(provider, "spawn", launch("while(true) { await program.sleep(10); }"));
    await delay(30);
    expect((await provider.manager.stop(run.id)).state).toBe("cancelled");
    expect((await provider.manager.stop(run.id)).state).toBe("cancelled");
  });
  it("propagates foreground cancellation and provider close", async () => {
    const { provider } = setup(); const controller = new AbortController();
    const foreground = callProgram(provider, "run", launch("while(true) await program.sleep(10);"), jevContext(controller.signal));
    await delay(30); controller.abort();
    expect((await foreground).state).toBe("cancelled");
    const background = await callProgram(provider, "spawn", launch("while(true) await program.sleep(10);"));
    const joined = provider.manager.join(background.id);
    await provider.close();
    expect((await joined).state).toBe("cancelled");
    await expect(callProgram(provider, "spawn", launch("return 1;"))).rejects.toThrow("closed");
  });
  it("enforces schemas, typechecking, and JSON-only output", async () => {
    const { provider } = setup();
    await expect(callProgram(provider, "run", launch("return input;", { inputSchema: { type: "integer" } }, "wrong"))).rejects.toThrow("input");
    await expect(callProgram(provider, "run", launch("const a: number = 'wrong'; return a;"))).rejects.toThrow("typecheck");
    const run = await callProgram(provider, "run", launch("return 'wrong';", { outputSchema: { type: "integer" } }));
    expect(run.state).toBe("failed"); expect(run.error).toContain("output");
    await expect(callProgram(provider, "run", launch("return null;", { outputSchema: { $ref: "https://invalid" } }))).rejects.toThrow("schema");
  });
  it("blocks undeclared capabilities and never grants host process access", async () => {
    const { provider } = setup();
    const denied = await callProgram(provider, "run", launch("return await tools.call({ref:'pi.bash',args:{command:'echo forbidden'}});"));
    expect(denied.state).toBe("failed"); expect(denied.error).toContain("Capability not granted");
    const isolated = await callProgram(provider, "run", launch("return {process:typeof globalThis['process'],fetch:typeof globalThis['fetch']};"));
    expect(isolated.result).toEqual({ process: "undefined", fetch: "undefined" });
    await expect(callProgram(provider, "run", launch("return 1;", { requires: ["jev.spawn"] }))).rejects.toThrow("recursive");
    const caller = jevContext();
    const lease = await provider.manager.options.registry.acquireCapabilityView(["jev.run"], caller);
    await expect(callProgram(provider, "run", launch("return 1;", { requires: ["jev.evaluate"] }), { ...caller, capabilityView: lease.view! })).rejects.toThrow("widen");
    await lease.release();
  });
  it("runs every declared tool through policy and preserves capability generations", async () => {
    const { provider, registry } = setup({ approvals: { read: "deny" } }); const invoke = vi.fn(async () => 1);
    registry.register({ name: "fixture", description: "fixture", list: async () => [{ name: "read", description: "read", inputSchema: {}, risk: "read" }], describe: async () => ({ name: "read", description: "read", inputSchema: {}, risk: "read" }), invoke });
    const run = await callProgram(provider, "run", launch("return await tools.call({ref:'fixture.read'});", { requires: ["fixture.read"] }));
    expect(run.state).toBe("failed"); expect(run.error).toContain("denied"); expect(invoke).not.toHaveBeenCalled();
  });
  it("counts inference usage and prevents catch-and-retry budget bypass", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({ model:"jev-latest", answers:{yes:{type:"noul",noul:1}},usage:{input_tokens:3,output_tokens:2} }))) as unknown as typeof fetch;
    const { provider } = setup({}, fetcher);
    const request = "{state:'hello',questions:{yes:{type:'noul',instructions:'Is this a greeting?'}}}";
    const run = await callProgram(provider, "run", launch(`await jev.evaluate(${request});
      try { await jev.evaluate(${request}); } catch {} return 1;`, { requires: ["jev.evaluate"], limits: { maxEvaluations: 1 } }));
    expect(run.state).toBe("failed"); expect(run.evaluations).toBe(1);
    expect(run.usage).toEqual({ input_tokens:3,output_tokens:2 }); expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it("enforces reported-token budgets before answers can drive actions", async () => {
    const fetcher = vi.fn(async () => { await delay(20); return new Response(JSON.stringify({ model:"jev-latest", answers:{yes:{type:"noul",noul:1}},usage:{input_tokens:3,output_tokens:2} })); }) as unknown as typeof fetch;
    const { provider } = setup({}, fetcher);
    const run = await callProgram(provider, "run", launch("return await jev.evaluate({state:'x',questions:{yes:{type:'noul',instructions:'Is x present?'}}});", { requires:["jev.evaluate"], limits:{maxTokens:4} }));
    expect(run.state).toBe("failed"); expect(run.error).toContain("token");
  });
  it("enforces wall time, tool-call budget, concurrency, and bounded event retention", async () => {
    const { provider } = setup({ jev: { maxConcurrentRuns: 1 } });
    const a = await callProgram(provider, "spawn", launch("await program.sleep(100); return null;"));
    await expect(callProgram(provider, "spawn", launch("return null;"))).rejects.toThrow("concurrent");
    await provider.manager.stop(a.id);
    const timed = await callProgram(provider, "run", launch("while(true) await program.sleep(10);", { limits:{timeoutMs:40} }));
    expect(timed.state).toBe("timed_out");
    const limited = await callProgram(provider, "run", launch("while(true) await program.emit(1);", { limits:{maxToolCalls:3} }));
    expect(limited.state).toBe("failed"); expect(limited.error).toContain("tool-call");
    const events = await callProgram(provider, "run", launch("for(let i=0;i<80;i++) await program.emit(i); return null;"));
    expect(events.events).toHaveLength(64); expect(events.events[0]?.sequence).toBe(17);
  });
  it("bounds terminal history after concurrent runs finish", async () => {
    const {provider}=setup({jev:{maxRetainedRuns:1,maxConcurrentRuns:3}});
    const waits=[];
    for(let i=0;i<3;i++) {
      const run=await callProgram(provider,"spawn",launch("await program.sleep(80); return input;",{},i));
      waits.push(provider.manager.join(run.id));
    }
    const results=await Promise.all(waits);
    expect(results.map(r=>r.result)).toEqual([0,1,2]);
    expect(provider.manager.list()).toHaveLength(1);
  });
  it("rejects overlapping inference without starting a second request", async () => {
    const fetcher=vi.fn(async()=>{await delay(15);return new Response(JSON.stringify({model:"jev-latest",answers:{yes:{type:"noul",noul:1}},usage:{input_tokens:2,output_tokens:1}}));}) as unknown as typeof fetch;
    const {provider}=setup({},fetcher);
    const request="{state:'x',questions:{yes:{type:'noul',instructions:'Is x present?'}}}";
    const run=await callProgram(provider,"run",launch(`const results=await Promise.allSettled([jev.evaluate(${request}),jev.evaluate(${request})]);return results.map(r=>r.status);`,{requires:["jev.evaluate"]}));
    expect(run.state,run.error).toBe("completed");expect(run.result).toEqual(["fulfilled","rejected"]);expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it("resets the CPU slice before decoding a large delayed host response", async () => {
    const payload={values:Array.from({length:12000},(_,i)=>({id:i,text:"observed"}))};
    const result=await new QuickJsRuntime().execute("return (await tools.call({ref:'fixture.read'})).values.length;",async()=>{await delay(100);return payload;},{timeoutMs:3000,maxCpuSliceMs:50,memoryLimitBytes:32*1024*1024});
    expect(result.terminationReason,result.error).toBe("completed");expect(result.value).toBe(12000);
  });
  it("interrupts CPU-bound guest loops without blocking the host for the run deadline", async () => {
    const result = await new QuickJsRuntime().execute("while(true) {}", async () => null, { timeoutMs:3000, maxCpuSliceMs:30, memoryLimitBytes:16*1024*1024 });
    expect(result.terminationReason).toBe("timed_out"); expect(result.error).toContain("uninterrupted CPU");
  });
});
