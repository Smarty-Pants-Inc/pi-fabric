import { readFileSync } from "node:fs";
import { afterEach, describe, expect, it } from "vitest";
import { JevObservationHost } from "../src/jev/observation.js";
import type { JevAdvice, JevLaunch, JevRequest, JevRunInfo } from "../src/jev/types.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { MontyRuntime } from "../src/runtime/monty-runtime.js";
import { jevContext, setupJev } from "./jev-test-helpers.js";

type Kernel = "typescript" | "python";
const montyAvailable = await import("@pydantic/monty/node").then(() => true, () => false);
const strings = {
  goal: "Implement the migration", acceptance: JSON.stringify(["Migration is correct", "Independent review and focused tests pass"]),
  instructions: "Run focused tests and build; never publish without permission.", cadence: "agent_settled", delivery: "steer",
};
async function starter(kernel: Kernel, overrides: Partial<typeof strings> = {}) {
  const markdown = readFileSync(`skillsets/${kernel}/fabric-foreman/SKILL.md`, "utf8");
  const code = markdown.match(kernel === "typescript" ? /```ts\n([\s\S]*?)\n```/ : /```python\n([\s\S]*?)\n```/)?.[1];
  if (!code) throw new Error("Missing Foreman starter");
  let launch: JevLaunch | undefined;
  const runtime = kernel === "typescript" ? new QuickJsRuntime() : new MontyRuntime();
  const result = await runtime.execute(code, async (ref, args) => {
    expect(ref === "fabric.$call" ? args.ref : ref).toBe("jev.spawn");
    launch = (ref === "fabric.$call" ? args.args : args) as unknown as JevLaunch;
    return { id: "foreman-test", state: "running" };
  }, { timeoutMs: 10000, memoryLimitBytes: 64 * 1024 * 1024, strings: { ...strings, ...overrides } });
  expect(result.terminationReason, result.error).toBe("completed");
  expect(result.value).toMatchObject({ id: "foreman-test", state: "running" });
  if (!launch) throw new Error("Starter did not spawn");
  return launch;
}
const dimensions = ["implementation_complete", "tests_sufficient", "requirements_satisfied", "needs_verification", "ready_to_finish", "meaningful_progress", "worker_stuck", "work_off_track", "agents_md_drift", "needs_human"];
const productive = { implementation_complete: 0.8, requirements_satisfied: 0.8, meaningful_progress: 0.9 };
const ready = { ...productive, tests_sufficient: 0.9, ready_to_finish: 0.95 };
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0)) await close(); });
async function fixture(options: { kernel?: Kernel; cadence?: string; delivery?: string; fail?: boolean; hold?: Promise<void> } = {}) {
  const request = await starter(options.kernel ?? "typescript", { cadence: options.cadence ?? "agent_settled", delivery: options.delivery ?? "steer" });
  const delivered: JevAdvice[] = [];
  const requests: JevRequest[] = [];
  let now = Date.now();
  let scores: Record<string, number> = productive;
  const host = new JevObservationHost("jev-test-session", advice => delivered.push(advice), () => now);
  const fetcher = (async (_url, init) => {
    requests.push(JSON.parse(String(init?.body)) as JevRequest);
    if (options.hold) await options.hold;
    if (options.fail) return new Response("private upstream detail", { status: 429 });
    return new Response(JSON.stringify({ model: "jev-latest", usage: { input_tokens: 10, output_tokens: 10 },
      answers: Object.fromEntries(dimensions.map(key => [key, { type: "noul", noul: scores[key] ?? 0.1 }])) }));
  }) as typeof fetch;
  const { provider } = setupJev({ approvals: { agent: "allow", network: "allow", read: "allow", execute: "allow" } }, fetcher, undefined, host);
  cleanup.push(async () => { host.close(); await provider.close(); });
  const run = await provider.invoke("spawn", request as unknown as Record<string, unknown>, jevContext()) as JevRunInfo;
  const status = () => provider.manager.status(run.id);
  const observe = (event: string, payload: unknown = {}) => host.observe(event, payload, { sessionId: "jev-test-session" });
  let turnIndex = 0;
  const turn = (text = "Implementing the migration", toolError = false) => observe("turn_end", {
    turnIndex: ++turnIndex,
    message: { role: "assistant", content: [{ type: "text", text }], stopReason: "stop" },
    toolResults: [{ toolName: "test", isError: toolError, content: [{ type: "text", text: toolError ? "Relevant test failed" : "Focused test passed" }] }],
  });
  const tick = async (nextScores: Record<string, number> = productive) => {
    scores = nextScores;
    const count = status().events.length;
    turn();
    if (options.cadence !== "turn_end") observe("agent_settled");
    await expect.poll(() => status().events.length).toBe(count + 1);
    return status().events.at(-1)!.value as Record<string, any>;
  };
  return { provider, host, request, run, status, observe, turn, tick, delivered, requests, setScores: (next: Record<string, number>) => { scores = next; }, advance: (ms: number) => { now += ms; }, setTime: (value: number) => { now = value; } };
}

it.skipIf(!montyAvailable)("submits identical artifacts from both selected kernels, including record-only mode", async () => {
  for (const delivery of ["steer", "off"]) {
    const ts = await starter("typescript", { delivery });
    expect(await starter("python", { delivery })).toEqual(ts);
    expect(ts.program.requires).toEqual(delivery === "off" ? ["jev.evaluate"] : ["jev.evaluate", "jev.advise"]);
    expect(ts.observe).toMatchObject({ events: ["input", "turn_end", "agent_settled"], include: ["assistantText", "toolResults"] });
    expect(ts.observe?.delivery).toBe(delivery === "off" ? undefined : "steer");
    expect(ts.observe?.triggerTurn).toBe(delivery === "off" ? undefined : true);
  }
});

for (const kernel of ["typescript", "python"] as const) {
  describe.skipIf(kernel === "python" && !montyAvailable)(`${kernel} Foreman through real Jev runtime`, () => {
    it.each(["turn_end", "agent_settled"])("assesses only at %s, with ten typed questions and bounded history", async cadence => {
      const f = await fixture({ kernel, cadence });
      expect(f.run.state).toBe("running");
      expect(f.requests).toEqual([]);
      expect((await f.tick()).action).toBe("CONTINUE");
      const state = f.requests[0]!.state as Record<string, any>;
      expect(Object.keys(f.requests[0]!.questions).sort()).toEqual([...dimensions].sort());
      expect(Object.values(f.requests[0]!.questions).every(question => question.type === "noul")).toBe(true);
      expect(state).toMatchObject({ goal: strings.goal, repositoryInstructions: strings.instructions, boundary: cadence });
      expect(state.turns[0].payload).toMatchObject({ assistantText: "Implementing the migration", toolResults: [{ toolName: "test", isError: false, text: "Focused test passed" }] });
      for (let i = 0; i < 5; i++) await f.tick();
      expect((f.requests.at(-1)!.state as Record<string, any>).turns).toHaveLength(4);
      expect(f.delivered).toEqual([]);
      expect(JSON.stringify(f.status().events)).not.toContain(strings.instructions);
      expect(JSON.stringify(f.status().events)).not.toContain(strings.goal);
      expect((await f.provider.invoke("stop", { id: f.run.id }, jevContext()) as JevRunInfo).state).toBe("cancelled");
      expect(f.host.size).toBe(0);
    });
  });
}

it("collects turns without assessing at settlement cadence, then deduplicates settlement", async () => {
  const f = await fixture();
  f.turn();
  await expect.poll(() => f.status().observation?.consumed).toBe(1);
  expect(f.requests).toEqual([]);
  f.observe("agent_settled");
  await expect.poll(() => f.status().events.length).toBe(1);
  f.observe("agent_settled");
  await expect.poll(() => f.status().observation?.consumed).toBe(3);
  expect(f.requests).toHaveLength(1);
});

it.each([
  { scores: { needs_human: 0.9, ...ready, work_off_track: 0.99 }, action: "ESCALATE", outcome: "escalated" },
  { scores: ready, action: "FINISH_REVIEW", outcome: "finish_review" },
  { scores: { ...productive, needs_verification: 0.9 }, action: "VERIFY" },
  { scores: { implementation_complete: 0.1, requirements_satisfied: 0.1 }, action: "RESUME" },
  ...["agents_md_drift", "work_off_track", "worker_stuck"].map(key => ({ scores: { ...productive, [key]: 0.9 }, action: "STEER" })),
] as Array<{ scores: Record<string, number>; action: string; outcome?: string }>)("applies deterministic $action policy without claiming completion", async ({ scores, action, outcome }) => {
  const f = await fixture();
  expect(await f.tick(scores)).toMatchObject({ action, advice: { delivered: true } });
  expect(f.delivered).toHaveLength(1);
  expect(f.delivered[0]).toMatchObject({ delivery: "steer", triggerTurn: true, runId: f.run.id });
  if (outcome) expect((await f.provider.manager.wait(f.run.id)).result).toEqual({ outcome, assessments: 1 });
  else expect(f.status().state).toBe("running");
});

it("steers once, allows a grace period, then escalates with honest suppressed delivery", async () => {
  const f = await fixture();
  const drift = { ...productive, work_off_track: 0.9 };
  expect((await f.tick(drift)).action).toBe("STEER");
  expect((await f.tick(drift)).action).toBe("GRACE");
  f.advance(30001);
  expect(await f.tick(drift)).toMatchObject({ action: "ESCALATE", advice: { delivered: false, reason: "budget" } });
  expect((await f.provider.manager.wait(f.run.id)).result).toMatchObject({ outcome: "escalated" });
  expect(f.delivered).toHaveLength(1);
});

it("requests verification once without treating its request or suppressed finish review as proof", async () => {
  const f = await fixture();
  const verifying = { ...productive, needs_verification: 0.9 };
  expect((await f.tick(verifying)).action).toBe("VERIFY");
  expect((await f.tick(verifying)).action).toBe("CONTINUE");
  expect(await f.tick(ready)).toMatchObject({ action: "FINISH_REVIEW", advice: { delivered: false, reason: "budget" } });
  expect((await f.provider.manager.wait(f.run.id)).result).toEqual({ outcome: "finish_review", assessments: 3 });
  expect(f.delivered).toHaveLength(1);
});

it("record-only mode has no advice grant or delivery even for a high-risk judgment", async () => {
  const f = await fixture({ delivery: "off" });
  expect(await f.tick({ needs_human: 0.99 })).toMatchObject({ action: "ESCALATE", advice: null, recordOnly: true });
  expect(f.delivered).toEqual([]);
  expect(f.request.program.requires).toEqual(["jev.evaluate"]);
});

it.each(["missing", "truncated", "compacted", "expired"])("does not infer from %s evidence", async mode => {
  const f = await fixture();
  if (mode !== "missing") {
    f.turn(mode === "truncated" ? "x".repeat(10000) : "Work is ready");
    await expect.poll(() => f.status().observation?.consumed).toBe(1);
  }
  if (mode === "compacted") f.observe("session_compact");
  // Exercise the profile's age guard independently of the queue's age guard.
  if (mode === "expired") {
    f.setTime(Date.now() - 31000);
    f.turn();
    await expect.poll(() => f.status().observation?.consumed).toBe(2);
    f.setTime(Date.now());
  }
  f.observe("agent_settled");
  await expect.poll(() => f.status().events.length).toBe(1);
  expect(f.status().events[0]!.value).toMatchObject({ action: "INSUFFICIENT_EVIDENCE", advice: null });
  expect(f.requests).toEqual([]);
  expect(f.delivered).toEqual([]);
});

it("rejects stale advice after Main completes another turn during inference", async () => {
  let release!: () => void;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const f = await fixture({ cadence: "turn_end", hold });
  f.setScores({ ...productive, work_off_track: 0.9 });
  f.turn();
  await expect.poll(() => f.requests.length).toBe(1);
  f.turn("Newer work");
  release();
  const terminal = await f.provider.manager.wait(f.run.id);
  expect(terminal.result).toMatchObject({ outcome: "escalated" });
  expect(terminal.events[0]!.value).toMatchObject({ advice: { delivered: false, reason: "stale" } });
  expect(f.delivered).toEqual([]);
});

it("records inference failure without leaking upstream detail or retrying", async () => {
  const f = await fixture({ fail: true });
  expect((await f.tick()).action).toBe("ASSESSMENT_FAILED");
  const terminal = await f.provider.manager.wait(f.run.id);
  expect(terminal.result).toMatchObject({ outcome: "escalated" });
  expect(f.requests).toHaveLength(1);
  expect(terminal.evaluations).toBe(1);
  expect(terminal.result).toMatchObject({ assessments: 1 });
  expect(JSON.stringify(terminal)).not.toContain("private upstream detail");
});

it("enforces its assessment ceiling without authorizing new work", async () => {
  const f = await fixture({ delivery: "off" });
  for (let i = 0; i < 19; i++) expect((await f.tick()).action).toBe("CONTINUE");
  expect((await f.tick()).action).toBe("LIMIT");
  expect((await f.provider.manager.wait(f.run.id)).result).toEqual({ outcome: "limit", assessments: 20 });
  expect(f.requests).toHaveLength(20);
});

it("retires on new input instead of reusing the goal and instruction snapshot", async () => {
  const f = await fixture();
  f.observe("input", { text: "Unrelated new task", source: "interactive" });
  expect((await f.provider.manager.wait(f.run.id)).result).toEqual({ outcome: "input_changed", assessments: 0 });
  expect(f.requests).toEqual([]);
  expect(f.delivered).toEqual([]);
});

it("shares Main cancellation with normal supervisors and never relaunches itself", async () => {
  const f = await fixture();
  f.host.halt();
  expect((await f.provider.manager.wait(f.run.id)).state).toBe("cancelled");
  expect(f.host.size).toBe(0);
  expect(f.provider.manager.list()).toHaveLength(1);
  expect(f.requests).toEqual([]);
});
