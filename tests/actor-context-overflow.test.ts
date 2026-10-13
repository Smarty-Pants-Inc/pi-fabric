import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager, ACTOR_ACTIVATION_OVERFLOW_KIND, actorEventRef } from "../src/actors/manager.js";
import type { FabricActorDeliveryRequest } from "../src/actors/types.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// smarty-dev#3238: review actors silently dropped events on "Context exceeds window". A real
// ActorManager, MeshStore, AgentManager and worker drive an offline Pi child that refuses a run
// with the worker's window error: a carried session is reset and the event retried once; an
// event that overflows a fresh session gets one explicit overflow notice and the actor stays live.

const roots: string[] = [];
const managers: Array<{ close(): Promise<void> }> = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
// Every outcome arrives as an event; the run timeout is only a hang guard (smarty-dev#883).
const HANG_GUARD_MS = 120_000;
const TEST_GUARD_MS = HANG_GUARD_MS + 30_000;

interface Prompt { history: boolean; overflow: boolean; session?: string; marker: string }

const waitFor = async (predicate: () => boolean, timeoutMs = 60_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

const setup = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-overflow-"));
  roots.push(dir);
  const log = path.join(dir, "prompts.jsonl");
  vi.stubEnv("FAKE_PI_LOG", log);
  const agents = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: HANG_GUARD_MS }, {
    workerPath: path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER ?? "src/worker.ts"),
    piBinary: path.resolve("tests/fixtures/fake-pi-overflow.mjs"), runRoot: path.join(dir, "runs"),
  });
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const alarms: FabricActorDeliveryRequest[] = [];
  const makeActors = () => new ActorManager("overflow-test", { id: "owner", name: "owner", kind: "main", sessionId: "overflow-test" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, (request) => { alarms.push(request); },
    { actorRoot: path.join(dir, "actors"), persistent: true, maxSessionBytes: 0 });
  const actors = makeActors();
  managers.push(actors, agents);
  const prompts = (): Prompt[] => fs.existsSync(log)
    ? fs.readFileSync(log, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as Prompt) : [];
  const overflowEvents = () => mesh.read({ topic: "fabric.actor.output" }).filter((event) => event.kind === ACTOR_ACTIVATION_OVERFLOW_KIND);
  const ownerAlarms = () => mesh.read({ topic: "ops.owner" }).filter((event) => event.kind === "actor.alarm");
  // The fixture's side-effecting tool executions (TOOL_THEN_OVERFLOW_ON_HISTORY), one line each.
  const toolRuns = (): unknown[] => fs.existsSync(`${log}.tools`)
    ? fs.readFileSync(`${log}.tools`, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as unknown) : [];
  // Every worker result the actor manager received, in order.
  const results: AgentRunResult[] = [];
  const run = agents.run.bind(agents);
  const recordResults = () => vi.spyOn(agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
    const result = await run(...args);
    results.push(result);
    return result;
  });
  const create = (name: string, extra: Record<string, unknown> = {}) => actors.create({
    name, instructions: "Review the event.", model: "test/overflow", tools: [], extensions: false,
    transport: "process", residency: "durable", delivery: "mailbox", ...extra,
  });
  return { dir, agents, mesh, actors, alarms, prompts, overflowEvents, ownerAlarms, create, makeActors, toolRuns, results, recordResults };
};

const header = (file: string): string => JSON.parse(fs.readFileSync(file, "utf8").split("\n", 1)[0]!).id as string;
const storedUsage = (dir: string, id: string): unknown => {
  const registry = JSON.parse(fs.readFileSync(path.join(dir, "actors", "actors.json"), "utf8"));
  return registry.actors.find((actor: { id: string }) => actor.id === id)?.lastContextUsage;
};
const backups = (file: string): string[] => fs.readdirSync(path.dirname(file))
  .filter((name) => name.startsWith("session.jsonl.") && name.endsWith(".bak")).sort();
const resets = (actors: ActorManager, id: string) => actors.messages(id, 50)
  .map((message) => (message.data as { sessionReset?: Record<string, unknown> } | undefined)?.sessionReset)
  .filter((reset): reset is Record<string, unknown> => reset !== undefined);

describe("actor context overflow (smarty-dev#3238, offline Pi child)", () => {
  it("10049 resets an over-window durable activation session after real preflight refusal and processes the same item", async () => {
    const s = setup();
    const actor = await s.create("preflight", { inferenceContext: "activation" });
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    const carried = header(actor.sessionFile!);
    fs.appendFileSync(actor.sessionFile!, JSON.stringify({ type: "message", id: "large-history", parentId: null,
      timestamp: new Date().toISOString(), message: { role: "user", content: "x".repeat(272_000 * 8), timestamp: Date.now() } }) + "\n");
    await expect(s.actors.ask(actor.id, "PREFLIGHT_ON_HISTORY", { pr: 10049 })).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(s.prompts()).toEqual([
      { history: false, overflow: false, session: carried, marker: "FIRST_REVIEW" },
      { history: true, overflow: true, session: carried, marker: "PREFLIGHT_ON_HISTORY" },
      { history: false, overflow: false, session: expect.not.stringMatching(carried), marker: "PREFLIGHT_ON_HISTORY" },
    ]);
    // The refused attempt never reached even the simulated provider dispatch.
    expect(fs.readFileSync(path.join(s.dir, "prompts.jsonl.requests"), "utf8")).toBe("request\nrequest\n");
    expect(backups(actor.sessionFile!)).toHaveLength(1);
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: true })]);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(s.overflowEvents()).toEqual([]);
    expect(s.ownerAlarms()).toEqual([]);
    expect(s.alarms).toEqual([]);
  }, TEST_GUARD_MS);

  it.each(["text", "directive"] as const)("10049 blocks and alarms only after a second real preflight overflow (%s)", async responseMode => {
    const s = setup();
    const actor = await s.create("preflight-stubborn", { inferenceContext: "activation", responseMode });
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    const outcome = s.actors.ask(actor.id, "PREFLIGHT_ALWAYS", { pr: 10049 });
    if (responseMode === "text") await expect(outcome).rejects.toThrow(/Fabric activation window failed: Context exceeds window/);
    else await expect(outcome).resolves.toMatchObject({ action: "silent", error: expect.stringContaining("Context exceeds window") });
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([
      { history: false, marker: "FIRST_REVIEW" }, { history: true, marker: "PREFLIGHT_ALWAYS" },
      { history: false, marker: "PREFLIGHT_ALWAYS" },
    ]);
    expect(fs.readFileSync(path.join(s.dir, "prompts.jsonl.requests"), "utf8")).toBe("request\n");
    expect(resets(s.actors, actor.id)).toHaveLength(1);
    expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ code: "context-overflow", class: "hard", count: 1 });
    expect(s.ownerAlarms()).toHaveLength(1);
    expect(s.overflowEvents()).toHaveLength(1);
    expect(s.alarms).toHaveLength(1);
    expect(s.alarms[0]!.message.text).toContain("retried once after resetting its session, but the fresh session still overflowed");
    expect(s.alarms[0]!.message.text).not.toContain("not retried");
  }, TEST_GUARD_MS);

  it("9486 restores durable context usage on host reload and proactively resets the next activation", async () => {
    const s = setup();
    const actor = await s.create("reloaded-usage");
    await s.actors.ask(actor.id, "FILL_CONTEXT");
    await waitFor(() => s.actors.inFlightCount() === 0 && storedUsage(s.dir, actor.id) !== undefined);
    const carried = header(actor.sessionFile!);
    expect(storedUsage(s.dir, actor.id)).toEqual({ tokens: 244_810, contextWindow: 272_000 });
    await s.actors.close();
    const restarted = s.makeActors();
    managers.push(restarted);
    await expect(restarted.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => restarted.inFlightCount() === 0);
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([
      { history: false, marker: "FILL_CONTEXT" }, { history: false, marker: "NEXT_REVIEW" },
    ]);
    expect(header(actor.sessionFile!)).not.toBe(carried);
    expect(resets(restarted, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: false,
      contextUsage: { tokens: 244_810, contextWindow: 272_000 } })]);
    expect(storedUsage(s.dir, actor.id)).toEqual({ tokens: 1010, contextWindow: 272_000 });
    expect(s.ownerAlarms()).toEqual([]);
  }, TEST_GUARD_MS);
  it("resets a carried session and retries the overflowing event once, which then succeeds", async () => {
    const s = setup();
    const actor = await s.create("reviewer");
    await expect(s.actors.ask(actor.id, "FIRST_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    const carried = header(actor.sessionFile!);
    await expect(s.actors.ask(actor.id, "OVERFLOW_ON_HISTORY", { pr: 3238, repository: "Smarty-Pants-Inc/smarty-dev" }))
      .resolves.toMatchObject({ direction: "out", text: "verdict: ok" });
    expect(s.prompts()).toEqual([
      { history: false, overflow: false, session: carried, marker: "FIRST_REVIEW" },
      { history: true, overflow: true, session: carried, marker: "OVERFLOW_ON_HISTORY" },
      { history: false, overflow: false, session: expect.not.stringMatching(carried), marker: "OVERFLOW_ON_HISTORY" },
    ]);
    expect(header(actor.sessionFile!)).not.toBe(carried);
    const [backup] = backups(actor.sessionFile!);
    expect(fs.readFileSync(path.join(path.dirname(actor.sessionFile!), backup!), "utf8")).toContain("FIRST_REVIEW");
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: true })]);
    expect(s.actors.messages(actor.id, 50).find((message) =>
      (message.data as { contextOverflow?: unknown } | undefined)?.contextOverflow)).toMatchObject({
      source: "fabric-host",
      data: { contextOverflow: { retry: true, eventRef: { source: "direct", pr: 3238, repository: "Smarty-Pants-Inc/smarty-dev" } } },
    });
    // A recovered event is not a failure: no overflow notice, no alarm, no block.
    expect(s.overflowEvents()).toEqual([]);
    expect(s.ownerAlarms()).toEqual([]);
    expect(s.alarms).toEqual([]);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
  }, TEST_GUARD_MS);

  it("publishes one overflow notice for an event that overflows a fresh session, stays live and runs the next event", async () => {
    const s = setup();
    const actor = await s.create("security", { topics: ["github.smarty-dev"] });
    const ingress = { id: "ingress", name: "github-ingress", kind: "agent" as const };
    await s.mesh.publish({ topic: "github.smarty-dev", kind: "github.webhook", from: ingress, data: {
      event: "pull_request", repository: "Smarty-Pants-Inc/smarty-dev",
      payload: { action: "synchronize", title: "OVERFLOW_ALWAYS",
        pull_request: { number: 3238, html_url: "https://github.com/Smarty-Pants-Inc/smarty-dev/pull/3238" } },
    } });
    await waitFor(() => s.overflowEvents().length === 1 && s.actors.inFlightCount() === 0);
    const [notice] = s.overflowEvents();
    expect(notice).toMatchObject({
      topic: "fabric.actor.output", kind: "actor.activation.overflow",
      from: { id: actor.id, name: "security", kind: "actor" },
      text: "security: no verdict (context overflow) for PR Smarty-Pants-Inc/smarty-dev#3238.",
      data: {
        actorId: actor.id, actorName: "security", reason: "no verdict (context overflow)",
        error: "Context exceeds window: estimated 272511 input tokens, window 272000",
        eventRef: { source: "mesh:github.smarty-dev", topic: "github.smarty-dev", kind: "github.webhook",
          repository: "Smarty-Pants-Inc/smarty-dev", pr: 3238, ref: "https://github.com/Smarty-Pants-Inc/smarty-dev/pull/3238" },
      },
    });
    // No retry loop: a fresh session that cannot fit the event ran it exactly once.
    expect(s.prompts()).toEqual([{ history: false, overflow: true, session: header(actor.sessionFile!), marker: "OVERFLOW_ALWAYS" }]);
    // The existing owner alarm, and the failure is recorded on the actor, not as a silent drop.
    expect(s.ownerAlarms()).toEqual([expect.objectContaining({ data: expect.objectContaining({ actorId: actor.id, reason: "context_window" }) })]);
    expect(s.alarms).toHaveLength(1);
    const status = s.actors.status(actor.id);
    expect(status.status).not.toBe("stopped");
    expect(status.activationBlocked).toMatchObject({ code: "context-overflow", count: 1 });
    const messages = s.actors.messages(actor.id, 50);
    expect(messages.filter((message) => message.error?.includes("Context exceeds window"))).toHaveLength(1);
    expect(messages.some((message) => message.error?.startsWith("Dropped"))).toBe(false);

    await s.mesh.publish({ topic: "github.smarty-dev", kind: "github.webhook", from: ingress, data: {
      event: "pull_request", repository: "Smarty-Pants-Inc/smarty-dev", payload: { action: "synchronize", title: "NEXT_REVIEW", pull_request: { number: 3239 } },
    } });
    await waitFor(() => s.prompts().length === 2 && s.actors.inFlightCount() === 0 && s.actors.status(actor.id).status === "idle");
    expect(s.prompts()[1]).toMatchObject({ overflow: false, marker: "NEXT_REVIEW" });
    await waitFor(() => s.actors.status(actor.id).activationBlocked === undefined);
    expect(s.actors.messages(actor.id, 50).at(-1)).toMatchObject({ direction: "out", text: "verdict: ok" });
    expect(s.overflowEvents()).toHaveLength(1);
  }, TEST_GUARD_MS);

  it("retries a carried overflow only once: a fresh-session overflow then fails explicitly", async () => {
    const s = setup();
    const actor = await s.create("stubborn");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await expect(s.actors.ask(actor.id, "OVERFLOW_ALWAYS", { pr: 77 })).rejects.toThrow(/Context exceeds window/);
    expect(s.prompts().map(({ history, overflow }) => ({ history, overflow }))).toEqual([
      { history: false, overflow: false }, { history: true, overflow: true }, { history: false, overflow: true },
    ]);
    expect(resets(s.actors, actor.id)).toHaveLength(1);
    expect(s.overflowEvents()).toEqual([expect.objectContaining({ data: expect.objectContaining({ eventRef: expect.objectContaining({ pr: 77 }) }) })]);
    expect(s.ownerAlarms()).toHaveLength(1);
    expect(s.alarms[0]!.message.text).toContain("retried once after resetting its session");
    expect(s.alarms[0]!.message.text).not.toContain("not retried");
    await expect(s.actors.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    expect(s.prompts()).toHaveLength(4);
  }, TEST_GUARD_MS);

  it("resets a carried session at the run boundary when the last run used 80% or more of its window", async () => {
    const s = setup();
    const actor = await s.create("filling");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    const first = header(actor.sessionFile!);
    await s.actors.ask(actor.id, "FILL_CONTEXT");
    expect(header(actor.sessionFile!)).toBe(first); // 1,010 of 272,000 tokens: no reset
    await s.actors.ask(actor.id, "NEXT_REVIEW");
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([
      { history: false, marker: "FIRST_REVIEW" }, { history: true, marker: "FILL_CONTEXT" }, { history: false, marker: "NEXT_REVIEW" },
    ]);
    expect(header(actor.sessionFile!)).not.toBe(first);
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({
      trigger: "context", overflow: false, contextUsage: { tokens: 244_810, contextWindow: 272_000 },
    })]);
    expect(s.overflowEvents()).toEqual([]);
  }, TEST_GUARD_MS);

  // Round 2 (PR #853 review): overflow is a typed worker code, never matched in error text.
  it("does not retry or report overflow for a non-overflow error that quotes the window phrase", async () => {
    const s = setup();
    const actor = await s.create("quoting");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    const carried = header(actor.sessionFile!);
    await expect(s.actors.ask(actor.id, "QUOTED_PHRASE", { pr: 9 })).rejects.toThrow(/the log line says "Context exceeds window/);
    expect(s.prompts().map(({ marker }) => marker)).toEqual(["FIRST_REVIEW", "QUOTED_PHRASE"]);
    expect(header(actor.sessionFile!)).toBe(carried);
    expect(resets(s.actors, actor.id)).toEqual([]);
    expect(s.actors.messages(actor.id, 50).some((message) =>
      (message.data as { contextOverflow?: unknown } | undefined)?.contextOverflow)).toBe(false);
    expect(s.overflowEvents()).toEqual([]);
    expect(s.ownerAlarms()).toEqual([]);
    expect(s.actors.status(actor.id).activationBlocked?.code).not.toBe("context-overflow");
  }, TEST_GUARD_MS);

  it("retries once on a fresh session for a provider overflow typed by Pi's isContextOverflow", async () => {
    const s = setup();
    const actor = await s.create("provider");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    const carried = header(actor.sessionFile!);
    s.recordResults();
    await expect(s.actors.ask(actor.id, "PROVIDER_OVERFLOW_ON_HISTORY")).resolves.toMatchObject({ text: "verdict: ok" });
    // Round 3: the overflowed run executed no tool call, so replaying the event is safe.
    expect(s.results.map(({ status, errorCode, toolCalls }) => ({ status, errorCode, toolCalls }))).toEqual([
      { status: "failed", errorCode: "CONTEXT_OVERFLOW", toolCalls: 0 }, { status: "completed", errorCode: undefined, toolCalls: 0 },
    ]);
    expect(s.prompts()).toEqual([
      { history: false, overflow: false, session: carried, marker: "FIRST_REVIEW" },
      { history: true, overflow: true, session: carried, marker: "PROVIDER_OVERFLOW_ON_HISTORY" },
      { history: false, overflow: false, session: expect.not.stringMatching(carried), marker: "PROVIDER_OVERFLOW_ON_HISTORY" },
    ]);
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: true })]);
    expect(s.overflowEvents()).toEqual([]);
  }, TEST_GUARD_MS);

  // Round 3 (PR #853 review, P1 src/worker.ts): a provider "stop" with usage over the window is a
  // silent overflow (pi-ai isContextOverflow); Pi keeps that answer, so the worker must fail closed.
  it("fails closed on a stop answer whose usage exceeds the window: CONTEXT_OVERFLOW, nothing delivered", async () => {
    const s = setup();
    s.recordResults();
    const actor = await s.create("silent");
    await expect(s.actors.ask(actor.id, "SILENT_OVERFLOW", { pr: 41 }))
      .rejects.toThrow(/Context overflow: the provider ended the turn with stopReason "stop" with 280000 input tokens against a 272000-token window/);
    expect(s.results).toEqual([expect.objectContaining({ status: "failed", errorCode: "CONTEXT_OVERFLOW", toolCalls: 0 })]);
    // A fresh session ran the event exactly once: no retry loop.
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([{ history: false, marker: "SILENT_OVERFLOW" }]);
    // The unchecked answer reached no one: no actor output and no mesh output carries it.
    expect(s.actors.messages(actor.id, 50).some((message) => message.text?.includes("verdict: ok"))).toBe(false);
    expect(s.mesh.read({ topic: "fabric.actor.output" }).some((event) => event.text?.includes("verdict: ok"))).toBe(false);
    expect(s.overflowEvents()).toEqual([expect.objectContaining({ data: expect.objectContaining({
      reason: "no verdict (context overflow)", eventRef: expect.objectContaining({ pr: 41 }) }) })]);
    expect(s.ownerAlarms()).toEqual([expect.objectContaining({ data: expect.objectContaining({ actorId: actor.id, reason: "context_window" }) })]);
    expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ code: "context-overflow", count: 1 });
  }, TEST_GUARD_MS);

  // Round 3 (PR #853 review, P1 src/actors/manager.ts): a retry replays the event. After a tool ran,
  // a replay could repeat its side effects, so the overflow settles like a fresh-session overflow.
  it("does not replay an event whose run executed a tool before it overflowed: no retry, overflow notice and alarm", async () => {
    const s = setup();
    s.recordResults();
    const actor = await s.create("tooling");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    const carried = header(actor.sessionFile!);
    await expect(s.actors.ask(actor.id, "TOOL_THEN_OVERFLOW_ON_HISTORY", { pr: 88 })).rejects.toThrow(/prompt is too long/);
    expect(s.results.at(-1)).toMatchObject({ status: "failed", errorCode: "CONTEXT_OVERFLOW", toolCalls: 1 });
    // One run on the carried session, never a fresh-session replay; the side effect happened once.
    expect(s.prompts()).toEqual([
      { history: false, overflow: false, session: carried, marker: "FIRST_REVIEW" },
      { history: true, overflow: true, session: carried, marker: "TOOL_THEN_OVERFLOW_ON_HISTORY" },
    ]);
    expect(s.toolRuns()).toHaveLength(1);
    expect(resets(s.actors, actor.id)).toEqual([]);
    const overflowMessages = s.actors.messages(actor.id, 50)
      .map((message) => (message.data as { contextOverflow?: Record<string, unknown> } | undefined)?.contextOverflow)
      .filter((value): value is Record<string, unknown> => value !== undefined);
    expect(overflowMessages).toEqual([expect.objectContaining({ retry: false, toolCalls: 1, eventRef: expect.objectContaining({ pr: 88 }) })]);
    // Settled exactly like a fresh-session overflow: the explicit notice, the alarm, a failed activation.
    expect(s.overflowEvents()).toEqual([expect.objectContaining({ data: expect.objectContaining({
      reason: "no verdict (context overflow)", eventRef: expect.objectContaining({ pr: 88 }) }) })]);
    expect(s.ownerAlarms()).toEqual([expect.objectContaining({ data: expect.objectContaining({ actorId: actor.id, reason: "context_window" }) })]);
    expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ code: "context-overflow", count: 1 });
    expect(s.actors.status(actor.id).status).not.toBe("stopped");
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(s.prompts()).toHaveLength(2);
    expect(s.toolRuns()).toHaveLength(1);
  }, TEST_GUARD_MS);

  it("retries on a fresh session after a restart between the retry's queue checkpoint and the archive", async () => {
    const s = setup();
    const actor = await s.create("restarted", { topics: ["github.restart"] });
    const ingress = { id: "ingress", name: "github-ingress", kind: "agent" as const };
    const publish = (title: string) => s.mesh.publish({ topic: "github.restart", kind: "github.webhook", from: ingress,
      data: { event: "pull_request", repository: "o/r", payload: { action: "synchronize", title, pull_request: { number: 5 } } } });
    await publish("FIRST_REVIEW");
    await waitFor(() => s.prompts().length === 1 && s.actors.inFlightCount() === 0 && s.actors.status(actor.id).status === "idle");
    const carried = header(actor.sessionFile!);
    // Stop the drain as the overflowing run returns: the retry is checkpointed, the reset has not run.
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      if (result.errorCode === "CONTEXT_OVERFLOW") s.actors.pauseForRelease();
      return result;
    });
    await publish("OVERFLOW_ON_HISTORY");
    await waitFor(() => s.prompts().length === 2 && s.actors.inFlightCount() === 0 &&
      s.actors.messages(actor.id, 50).some((message) => (message.data as { contextOverflow?: unknown } | undefined)?.contextOverflow));
    expect(header(actor.sessionFile!)).toBe(carried);
    expect(backups(actor.sessionFile!)).toEqual([]);
    await s.actors.close();
    vi.restoreAllMocks();
    // A restarted host has no in-memory reset marker: the persisted retry alone must reset.
    const restarted = s.makeActors();
    managers.push(restarted);
    await waitFor(() => s.prompts().length === 3 && restarted.inFlightCount() === 0);
    expect(s.prompts()[2]).toEqual({ history: false, overflow: false, session: expect.not.stringMatching(carried), marker: "OVERFLOW_ON_HISTORY" });
    expect(header(actor.sessionFile!)).not.toBe(carried);
    expect(resets(restarted, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: true })]);
    expect(s.overflowEvents()).toEqual([]);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(s.prompts()).toHaveLength(3);
  }, TEST_GUARD_MS);

  it("addresses the triggering event by topic, key and PR or issue reference", () => {
    expect(actorEventRef({ id: "q1", source: "mesh:ops.owner", coalesceKey: "pr:12",
      payload: { id: "e1", topic: "ops.owner", kind: "pr.wake", data: { repo: "o/r", pr: 12 } } }))
      .toEqual({ source: "mesh:ops.owner", itemId: "q1", topic: "ops.owner", kind: "pr.wake", eventId: "e1", key: "pr:12", repository: "o/r", pr: 12 });
    expect(actorEventRef({ id: "q2", source: "mesh:github.r", payload: { topic: "github.r", kind: "github.webhook",
      data: { repository: "o/r", payload: { issue: { number: 5, html_url: "https://x/5" } } } } }))
      .toEqual({ source: "mesh:github.r", itemId: "q2", topic: "github.r", kind: "github.webhook", repository: "o/r", issue: 5, ref: "https://x/5" });
    expect(actorEventRef({ id: "q3", source: "direct", payload: { message: "hi" } })).toEqual({ source: "direct", itemId: "q3" });
  });
});
