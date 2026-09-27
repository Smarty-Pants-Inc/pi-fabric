import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import type { JevLaunch } from "../src/jev/types.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";
import { callProgram, jevContext, launch, setupJev } from "./jev-test-helpers.js";

const fixture = (fetcher: typeof fetch, overrides = {}) => {
  const jev = setupJev({ fullCodeMode: true, ...overrides }, fetcher);
  const jobs = new FabricShellJobStore();
  jev.registry.register(new PiToolsProvider(process.cwd(), undefined, undefined, { shellJobs: jobs, powerShellToolDefinitionFactory: undefined }));
  jev.registry.register(new TasksProvider(jobs));
  return { ...jev, jobs, async close() { await jev.registry.close(); await jobs.close(); } };
};

const shell = `const started = await pi.bash({cmd: "printf 'BUILD: ready\\n'; sleep 0.3; printf 'BUILD: compiler failure\\n'; exit 7", monitor: {delivery: "ui", match: "BUILD:", intervalMs: 1000, timeoutMs: 10000}, settle: true});
const id = (started.details as {taskId?:string} | null)?.taskId;
if (!id) throw new Error("Expected a tracked task");`;
const receipt = `const receipt = await tools.call({ref:"tasks.wait", args:{id, timeoutMs:5000}}) as {timedOut:boolean; task:{status:string;exitCode:number};output:string};
if (receipt.timedOut) throw new Error("Task still running");`;

describe("Jev shell orchestration", () => {
  it("executes the documented shell starter with an offline command substitution", async () => {
    const code = readFileSync("docs/jev.md", "utf8").match(/```ts\n([\s\S]*?)\n```/)?.[1];
    expect(code).toBeTruthy();
    const f = fixture(vi.fn(async () => { throw new Error("Unexpected inference"); }) as typeof fetch);
    try {
      const result = await new QuickJsRuntime().execute(code!, async (ref, args, signal) => {
        expect(ref).toBe("jev.run");
        const request = args as unknown as JevLaunch;
        expect(request.program.requires).toEqual(["pi.bash", "tasks.wait"]);
        expect(request.program.limits?.maxEvaluations).toBe(0);
        request.input = { command: "printf 'verified fixture'; sleep 0.3" };
        return callProgram(f.provider, "run", request, jevContext(signal));
      }, { timeoutMs: 10000, memoryLimitBytes: 64 * 1024 * 1024 });
      expect(result.terminationReason, result.error).toBe("completed");
      expect(result.value).toMatchObject({ state: "completed", result: { timedOut: false, task: { status: "exited", exitCode: 0 }, output: expect.stringContaining("verified fixture") } });
    } finally { await f.close(); }
  });

  it("runs a real process and watches/waits without inference or connector registration", async () => {
    const fetcher = vi.fn(async () => { throw new Error("Unexpected inference"); });
    const f = fixture(fetcher as typeof fetch);
    try {
      const result = await callProgram(f.provider, "run", launch(`${shell}
const batch = await tools.call({ref:"tasks.watch", args:{id,timeoutMs:5000}});
${receipt}
return {batch, receipt};`, { requires: ["pi.bash", "tasks.watch", "tasks.wait"], limits: { timeoutMs: 10000, maxEvaluations: 0 } }));
      expect(result.state, result.error).toBe("completed");
      expect(result.evaluations).toBe(0); expect(fetcher).not.toHaveBeenCalled();
      expect(result.result).toMatchObject({ batch: { reason: "event", lines: ["BUILD: ready", "BUILD: compiler failure"], omitted: 0 }, receipt: { timedOut: false, task: { status: "failed", exitCode: 7 }, output: expect.stringContaining("compiler failure") } });
    } finally { await f.close(); }
  });
  it("makes one explicit typed judgment only after deterministic exit inspection", async () => {
    const fetcher = vi.fn(async (_url: unknown, init: RequestInit | undefined) => {
      const request = JSON.parse(String(init?.body));
      expect(request.state.exitCode).toBe(7);
      expect(request.state.output).toContain("compiler failure");
      return Response.json({ model: "jev-latest", answers: { next: { type: "choice", choice: "review", confidence: 1, probabilities: { review: 1, done: 0 } } }, usage: { input_tokens: 10, output_tokens: 1 } });
    });
    const f = fixture(fetcher as typeof fetch);
    try {
      const result = await callProgram(f.provider, "run", launch(`${shell}\n${receipt}
if (receipt.task.exitCode === 0) return "done";
const judgment = await jev.evaluate({state:{exitCode:receipt.task.exitCode,output:receipt.output}, questions:{next:{type:"choice",instructions:"Does this failed build require human review?",criteria:{review:"Needs review",done:"No remaining work"}}}});
return judgment.answers.next.choice;`, { requires: ["pi.bash", "tasks.wait", "jev.evaluate"], limits: { timeoutMs: 10000, maxEvaluations: 1 } }));
      expect(result.state, result.error).toBe("completed");
      expect(result).toMatchObject({ result: "review", evaluations: 1 });
      expect(fetcher).toHaveBeenCalledTimes(1);
    } finally { await f.close(); }
  });
  it("preserves exact grants, approvals, full-code policy, and zero-evaluation budgets", async () => {
    const fetcher = vi.fn(async () => { throw new Error("Unexpected inference"); });
    const f = fixture(fetcher as typeof fetch);
    try {
      const missing = await callProgram(f.provider, "run", launch('return await pi.bash({cmd:"echo forbidden"});'));
      expect(missing.state).toBe("failed"); expect(f.jobs.list()).toHaveLength(0);
      f.config.approvals.execute = "deny";
      const denied = await callProgram(f.provider, "run", launch('return await pi.bash({cmd:"echo forbidden"});', { requires: ["pi.bash"] }));
      expect(denied.state).toBe("failed"); expect(f.jobs.list()).toHaveLength(0);
      f.config.fullCodeMode = false;
      await expect(callProgram(f.provider, "run", launch("return null;", { requires: ["pi.bash"] }))).rejects.toThrow("Full-code tool access is disabled");
      const zero = await callProgram(f.provider, "run", launch('return await jev.evaluate({state:"test",questions:{q:{type:"noul",instructions:"true?"}}});', { requires: ["jev.evaluate"], limits: { maxEvaluations: 0 } }));
      expect(zero).toMatchObject({ state: "failed", evaluations: 0 }); expect(fetcher).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });
  it("cancelling a Jev wait leaves its session-owned task for explicit stop", async () => {
    const f = fixture(vi.fn() as unknown as typeof fetch);
    const job = f.jobs.begin("bash", "synthetic live task");
    try {
      const run = await callProgram(f.provider, "spawn", launch('return await tools.call({ref:"tasks.wait",args:{id:input,timeoutMs:30000}});', { requires: ["tasks.wait"] }, job.id));
      expect(await f.provider.invoke("stop", { id: run.id }, jevContext())).toMatchObject({ state: "cancelled" });
      expect(job.abort.signal.aborted).toBe(false);
      await new TasksProvider(f.jobs).invoke("stop", { id: job.id }, jevContext());
      expect(job.abort.signal.aborted).toBe(true);
    } finally { await f.close(); }
  });
});
