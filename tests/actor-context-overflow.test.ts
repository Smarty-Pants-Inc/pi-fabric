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

// RECORDED-CALL seam (#4664): real rotations/stores/worker still run, but this suite never
// executes the product backup deleter. vitest records every prune request instead.
vi.mock("../src/storage/retention.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/storage/retention.js")>(),
  pruneActorSessionBackups: vi.fn(),
}));

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

const setup = (options: ConstructorParameters<typeof ActorManager>[6] = {}) => {
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
    { actorRoot: path.join(dir, "actors"), persistent: true, maxSessionBytes: 0, ...options });
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

  it.each(["text", "directive"] as const)("10049 alarms only after a second real preflight overflow and restores routing (%s)", async responseMode => {
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
    await waitFor(() => resets(s.actors, actor.id).length === 2 && s.actors.status(actor.id).activationBlocked === undefined);
    expect(resets(s.actors, actor.id)).toHaveLength(2);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(s.ownerAlarms()).toHaveLength(1);
    expect(s.overflowEvents()).toHaveLength(1);
    expect(s.alarms).toHaveLength(1);
    expect(s.alarms[0]!.message.text).toContain("retried once after resetting its session, but the fresh session still overflowed");
    expect(s.alarms[0]!.message.text).not.toContain("not retried");
  }, TEST_GUARD_MS);

  it("9486 restores 85% durable context usage on host reload and proactively resets the next activation", async () => {
    const s = setup();
    const actor = await s.create("reloaded-usage");
    await s.actors.ask(actor.id, "FILL_CONTEXT_85");
    await waitFor(() => s.actors.inFlightCount() === 0 && storedUsage(s.dir, actor.id) !== undefined);
    const carried = header(actor.sessionFile!);
    const usage = { tokens: 231_210, contextWindow: 272_000, measuredAt: expect.any(Number), sessionId: carried };
    // Written before close() can save anything: a manager restart needs no graceful shutdown save.
    expect(storedUsage(s.dir, actor.id)).toEqual(usage);
    await s.actors.close();
    const restarted = s.makeActors();
    managers.push(restarted);
    await expect(restarted.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => restarted.inFlightCount() === 0);
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([
      { history: false, marker: "FILL_CONTEXT_85" }, { history: false, marker: "NEXT_REVIEW" },
    ]);
    expect(header(actor.sessionFile!)).not.toBe(carried);
    expect(resets(restarted, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: false,
      contextUsage: { tokens: 231_210, contextWindow: 272_000 } })]);
    expect(storedUsage(s.dir, actor.id)).toEqual({ tokens: 1010, contextWindow: 272_000,
      measuredAt: expect.any(Number), sessionId: header(actor.sessionFile!) });
    expect(s.ownerAlarms()).toEqual([]);
  }, TEST_GUARD_MS);

  it.each(["another-session", "legacy-unbound", "below-threshold"])("9486 does not reset for restored usage that is %s", async (mode) => {
    const s = setup();
    const actor = await s.create("stale-usage");
    await s.actors.ask(actor.id, mode === "below-threshold" ? "FIRST_REVIEW" : "FILL_CONTEXT_85");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const carried = header(actor.sessionFile!);
    await s.actors.close();
    const file = path.join(s.dir, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(file, "utf8"));
    const record = registry.actors.find((candidate: { id: string }) => candidate.id === actor.id);
    if (mode === "another-session") record.lastContextUsage.sessionId = "different-native-session";
    else if (mode === "legacy-unbound") delete record.lastContextUsage.sessionId;
    fs.writeFileSync(file, JSON.stringify(registry));
    const restarted = s.makeActors();
    managers.push(restarted);
    await expect(restarted.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => restarted.inFlightCount() === 0);
    expect(header(actor.sessionFile!)).toBe(carried);
    expect(s.prompts().at(-1)).toMatchObject({ history: true, marker: "NEXT_REVIEW" });
    expect(resets(restarted, actor.id)).toEqual([]);
    expect(storedUsage(s.dir, actor.id)).toMatchObject({ tokens: 1010, sessionId: carried });
  }, TEST_GUARD_MS);

  it("9486 keeps unchanged measurements without adding registry writes", async () => {
    const s = setup();
    const actor = await s.create("unchanged-usage");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const usage = storedUsage(s.dir, actor.id);
    const writes = vi.spyOn(fs, "renameSync");
    const registryWrites = () => writes.mock.calls.filter(([, destination]) =>
      destination === path.join(s.dir, "actors", "actors.json")).length;
    await s.actors.ask(actor.id, "NEXT_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(storedUsage(s.dir, actor.id)).toEqual(usage); // measuredAt is stable too
    const withUnchangedUsage = registryWrites();
    expect(withUnchangedUsage).toBeGreaterThan(0); // baseline activation bookkeeping still writes
    writes.mockClear();
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      delete result.contextUsage;
      return result;
    });
    await s.actors.ask(actor.id, "LAST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(registryWrites()).toBe(withUnchangedUsage); // same writes as a turn with no measurement
    expect(storedUsage(s.dir, actor.id)).toEqual(usage); // kept until a new real measurement
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
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: true, retry: true })]);
    expect(s.actors.messages(actor.id, 50).find((message) =>
      (message.data as { sessionReset?: unknown } | undefined)?.sessionReset)?.reason)
      .toBe("session reset (context overflow): retrying the event once on a fresh session");
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
        error: "context-overflow",
        eventRef: { source: "mesh:github.smarty-dev", topic: "github.smarty-dev", kind: "github.webhook",
          repository: "Smarty-Pants-Inc/smarty-dev", pr: 3238, ref: "https://github.com/Smarty-Pants-Inc/smarty-dev/pull/3238" },
      },
    });
    // No retry loop: a fresh session that cannot fit the event ran it exactly once.
    expect(s.prompts()).toEqual([{ history: false, overflow: true, session: expect.any(String), marker: "OVERFLOW_ALWAYS" }]);
    await waitFor(() => s.actors.status(actor.id).activationBlocked === undefined && resets(s.actors, actor.id).length === 1);
    expect(header(actor.sessionFile!)).not.toBe(s.prompts()[0]!.session);
    // The existing owner alarm, and the failure is recorded on the actor, not as a silent drop.
    expect(s.ownerAlarms()).toEqual([expect.objectContaining({ data: expect.objectContaining({ actorId: actor.id, reason: "context_window" }) })]);
    expect(s.alarms).toHaveLength(1);
    const status = s.actors.status(actor.id);
    expect(status.status).not.toBe("stopped");
    expect(status.activationBlocked).toBeUndefined();
    const messages = s.actors.messages(actor.id, 50);
    expect(messages.filter((message) => message.error?.includes("Context exceeds window"))).toHaveLength(1);
    expect(messages.some((message) => message.error?.startsWith("Dropped"))).toBe(false);
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({ overflow: true, retry: false })]);
    expect(messages.find((message) => (message.data as { sessionReset?: unknown } | undefined)?.sessionReset)?.reason)
      .toBe("session reset (context overflow): failed item will not be replayed; next work starts on a fresh session");

    await s.mesh.publish({ topic: "github.smarty-dev", kind: "github.webhook", from: ingress, data: {
      event: "pull_request", repository: "Smarty-Pants-Inc/smarty-dev", payload: { action: "synchronize", title: "NEXT_REVIEW", pull_request: { number: 3239 } },
    } });
    await waitFor(() => s.prompts().length === 2 && s.actors.inFlightCount() === 0 && s.actors.status(actor.id).status === "idle");
    expect(s.prompts()[1]).toMatchObject({ overflow: false, marker: "NEXT_REVIEW" });
    await waitFor(() => s.actors.status(actor.id).activationBlocked === undefined);
    expect(s.actors.messages(actor.id, 50).at(-1)).toMatchObject({ direction: "out", text: "verdict: ok" });
    expect(s.overflowEvents()).toHaveLength(1);
  }, TEST_GUARD_MS);

  // Keep reason: trust boundary, smarty-dev#12512 — real typed overflow diagnostics stay local.
  it("12512 payload allowlist: typed overflow keeps diagnostics out of mesh output and Main", async () => {
    const s = setup();
    const actor = await s.create("private-overflow");
    const posix = "/private/12512-overflow/owner-record.json";
    const windows = String.raw`C:\Users\Synthetic\12512-overflow\owner-record.json`;
    const secret = "synthetic-private-token-12512-overflow";
    const raw = `Context exceeds window: ${posix}; ${windows}; ${secret}`;
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      if (result.status === "failed") {
        // Decorate only an actual worker-typed overflow, never an ordinary quoted error.
        expect(result.errorCode).toBe("CONTEXT_OVERFLOW");
        result.error = raw;
      }
      s.results.push(result);
      return result;
    });
    await expect(s.actors.ask(actor.id, "OVERFLOW_ALWAYS", { pr: 12512, repository: "synthetic/review" }))
      .rejects.toThrow(raw);
    await waitFor(() => s.actors.inFlightCount() === 0 && s.overflowEvents().length === 1 && s.ownerAlarms().length === 1);
    expect(s.results).toEqual([expect.objectContaining({ status: "failed", errorCode: "CONTEXT_OVERFLOW", error: raw })]);
    expect(s.alarms).toHaveLength(1);
    const visible = JSON.stringify({ output: s.overflowEvents(), owner: s.ownerAlarms(), main: s.alarms });
    for (const value of [posix, windows, secret, actor.sessionFile!, actor.logDir!]) {
      expect(visible).not.toContain(JSON.stringify(value).slice(1, -1));
    }
    expect(s.overflowEvents()[0]).toMatchObject({
      topic: "fabric.actor.output", kind: ACTOR_ACTIVATION_OVERFLOW_KIND,
      from: { id: actor.id, kind: "actor" }, text: expect.stringContaining("no verdict (context overflow)"),
      data: { actorId: actor.id, reason: "no verdict (context overflow)", error: "context-overflow",
        runId: s.results[0]!.id, eventRef: { source: "direct", pr: 12512, repository: "synthetic/review" } },
    });
    expect(s.ownerAlarms()[0]).toMatchObject({
      from: { id: "owner" }, to: actor.rootId, data: { actorId: actor.id, reason: "context_window" },
    });
    expect(s.alarms[0]).toMatchObject({
      delivery: "followUp", triggerTurn: true, actor: { id: actor.id }, message: { source: "fabric-host", actorId: actor.id },
    });
    expect(s.alarms[0]!.message.text).toContain("agents.actorStatus");
    expect(s.alarms[0]!.message.text).toContain("agents.log");
    // Other overflow history is outside this named payload projection and remains diagnostic.
    expect(s.actors.messages(actor.id, 50).some(message => message.error === raw)).toBe(true);
    expect(s.prompts()).toHaveLength(1);
    await expect(s.actors.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    expect(s.overflowEvents()).toHaveLength(1);
    expect(s.alarms).toHaveLength(1);
  }, TEST_GUARD_MS);

  it("retries a carried overflow only once: a fresh-session overflow then fails explicitly", async () => {
    const s = setup();
    const actor = await s.create("stubborn");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await expect(s.actors.ask(actor.id, "OVERFLOW_ALWAYS", { pr: 77 })).rejects.toThrow(/Context exceeds window/);
    expect(s.prompts().map(({ history, overflow }) => ({ history, overflow }))).toEqual([
      { history: false, overflow: false }, { history: true, overflow: true }, { history: false, overflow: true },
    ]);
    await waitFor(() => resets(s.actors, actor.id).length === 2 && s.actors.status(actor.id).activationBlocked === undefined);
    expect(resets(s.actors, actor.id)).toHaveLength(2);
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
    await waitFor(() => s.actors.status(actor.id).activationBlocked === undefined && resets(s.actors, actor.id).length === 1);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
  }, TEST_GUARD_MS);

  // Round 3 (PR #853 review, P1 src/actors/manager.ts): a retry replays the event. After a tool ran,
  // a replay could repeat its side effects, so the overflow settles like a fresh-session overflow.
  it("fails a toolful overflowing item with its typed code, never replays it, and runs already queued work fresh", async () => {
    const s = setup();
    const actor = await s.create("tooling");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const carried = header(actor.sessionFile!);
    // Gate only delivery of the real process result, not the worker or its tool execution.
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      s.results.push(result);
      if (result.toolCalls > 0) { entered(); await gate; }
      return result;
    });
    const failed = s.actors.ask(actor.id, "TOOL_THEN_OVERFLOW_ON_HISTORY", { pr: 88 }).catch(error => error);
    await ready;
    const next = s.actors.ask(actor.id, "NEXT_REVIEW");
    release();
    expect(await failed).toMatchObject({ code: "CONTEXT_OVERFLOW", message: expect.stringContaining("prompt is too long") });
    await expect(next).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(s.results).toEqual([
      expect.objectContaining({ status: "failed", errorCode: "CONTEXT_OVERFLOW", toolCalls: 1 }),
      expect.objectContaining({ status: "completed", toolCalls: 0 }),
    ]);
    // One failed run and one tool effect; only its distinct, already queued successor runs fresh.
    expect(s.prompts()).toEqual([
      { history: false, overflow: false, session: carried, marker: "FIRST_REVIEW" },
      { history: true, overflow: true, session: carried, marker: "TOOL_THEN_OVERFLOW_ON_HISTORY" },
      { history: false, overflow: false, session: expect.not.stringMatching(carried), marker: "NEXT_REVIEW" },
    ]);
    expect(s.toolRuns()).toHaveLength(1);
    await waitFor(() => resets(s.actors, actor.id).length === 1 && s.actors.status(actor.id).activationBlocked === undefined);
    expect(resets(s.actors, actor.id)).toEqual([expect.objectContaining({ trigger: "context", overflow: true })]);
    const overflowMessages = s.actors.messages(actor.id, 50)
      .map((message) => (message.data as { contextOverflow?: Record<string, unknown> } | undefined)?.contextOverflow)
      .filter((value): value is Record<string, unknown> => value !== undefined);
    expect(overflowMessages).toEqual([expect.objectContaining({ retry: false, toolCalls: 1, eventRef: expect.objectContaining({ pr: 88 }) })]);
    // Settled exactly like a fresh-session overflow: the explicit notice, the alarm, a failed activation.
    expect(s.overflowEvents()).toEqual([expect.objectContaining({ data: expect.objectContaining({
      reason: "no verdict (context overflow)", eventRef: expect.objectContaining({ pr: 88 }) }) })]);
    expect(s.ownerAlarms()).toEqual([expect.objectContaining({ data: expect.objectContaining({ actorId: actor.id, reason: "context_window" }) })]);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(s.actors.status(actor.id).status).not.toBe("stopped");
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(s.prompts()).toHaveLength(3);
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

  it("re-arms a successor queued while a fresh overflow's final reset publication is still settling", async () => {
    const s = setup();
    const actor = await s.create("reset-rearm");
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const put = s.mesh.put.bind(s.mesh);
    let gated = false;
    vi.spyOn(s.mesh, "put").mockImplementation(async request => {
      if (!gated && request.key === `actors/overflow-test/${actor.id}` && resets(s.actors, actor.id).length === 1 &&
          !(request.value as { activationBlocked?: unknown }).activationBlocked) {
        gated = true; entered(); await gate;
      }
      return put(request);
    });
    const failed = s.actors.ask(actor.id, "OVERFLOW_ALWAYS").catch(error => error);
    await ready;
    // The drain's loop already ended, but its final reset still owns admission.
    const next = s.actors.ask(actor.id, "NEXT_REVIEW");
    release();
    expect(await failed).toMatchObject({ code: "CONTEXT_OVERFLOW" });
    await expect(next).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([
      { history: false, marker: "OVERFLOW_ALWAYS" }, { history: false, marker: "NEXT_REVIEW" },
    ]);
    expect(s.ownerAlarms()).toHaveLength(1);
    expect(s.overflowEvents()).toHaveLength(1);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
  }, TEST_GUARD_MS);

  it("keeps the exact once-only retry ahead of coalesced successors across a host reload", async () => {
    const s = setup();
    const actor = await s.create("retry-coalesce", { topics: ["github.retry"], coalesceKey: "payload.pull_request.number" });
    const ingress = { id: "ingress", name: "ingress", kind: "agent" as const };
    const publish = (title: string) => s.mesh.publish({ topic: "github.retry", from: ingress,
      data: { payload: { title, pull_request: { number: 996 } } } });
    const queue = () => fs.readdirSync(path.dirname(actor.sessionFile!)).filter(name => /^queue-.*\.json$/.test(name))
      .flatMap(name => (JSON.parse(fs.readFileSync(path.join(path.dirname(actor.sessionFile!), name), "utf8")) as
        { items: Array<{ id: string; overflowRetried?: boolean; executionStarted?: boolean; payload: { data: { payload: { title: string } } } }> }).items);
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    // Hold settlement after the retry checkpoint, but keep ingress live: pauseForRelease
    // also pauses mesh ingestion, so it cannot be used until the successors are accepted.
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const put = s.mesh.put.bind(s.mesh);
    let gated = false;
    vi.spyOn(s.mesh, "put").mockImplementation(async request => {
      if (!gated && request.key === `actors/overflow-test/${actor.id}` &&
          queue().some(item => item.overflowRetried && !item.executionStarted)) {
        gated = true; entered(); await gate;
      }
      return put(request);
    });
    try {
      await publish("OVERFLOW_ON_HISTORY");
      await ready;
      const retryId = queue()[0]!.id;
      await publish("NEXT_REVIEW");
      await publish("LAST_REVIEW");
      await waitFor(() => queue().length === 2 && queue()[1]!.payload.data.payload.title === "LAST_REVIEW");
      expect(queue()[0]).toMatchObject({ id: retryId, overflowRetried: true, payload: { data: { payload: { title: "OVERFLOW_ON_HISTORY" } } } });
      s.actors.pauseForRelease();
    } finally { release(); }
    await s.actors.close();
    vi.restoreAllMocks();
    const restarted = s.makeActors();
    managers.push(restarted);
    await waitFor(() => s.prompts().length === 4 && restarted.inFlightCount() === 0);
    expect(s.prompts().map(({ history, marker }) => ({ history, marker }))).toEqual([
      { history: false, marker: "FIRST_REVIEW" }, { history: true, marker: "OVERFLOW_ON_HISTORY" },
      { history: false, marker: "OVERFLOW_ON_HISTORY" }, { history: true, marker: "LAST_REVIEW" },
    ]);
    expect(resets(restarted, actor.id)).toHaveLength(1);
    expect(s.overflowEvents()).toEqual([]);
    expect(s.ownerAlarms()).toEqual([]);
    expect(queue()).toEqual([]);
  }, TEST_GUARD_MS);

  it.each(["idle", "deferred"] as const)("a real process %s hard-block reset clears durable routing and runs queued/next work fresh", async mode => {
    const s = setup();
    const actor = await s.create(`reset-${mode}`);
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const carried = header(actor.sessionFile!);
    const run = s.agents.run.bind(s.agents);
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { entered = resolve; });
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      if (mode === "deferred" && result.status === "failed") { entered(); await gate; }
      return result;
    });
    const failed = s.actors.ask(actor.id, "QUOTED_PHRASE").catch(error => error);
    let next: ReturnType<ActorManager["ask"]>;
    let owner = s.actors;
    if (mode === "deferred") {
      await ready;
      const reset = s.actors.resetSession(actor.id);
      next = s.actors.ask(actor.id, "NEXT_REVIEW");
      release();
      expect(await failed).toBeInstanceOf(Error);
      await expect(reset).resolves.toMatchObject({ id: actor.id });
    } else {
      expect(await failed).toBeInstanceOf(Error);
      await waitFor(() => s.actors.inFlightCount() === 0);
      expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ class: "hard" });
      await expect(s.actors.resetSession(actor.id)).resolves.toMatchObject({ id: actor.id, status: "idle" });
      // Read through a fresh manager BEFORE graceful close can conceal a missing reset commit.
      owner = s.makeActors();
      managers.push(owner);
      expect(owner.status(actor.id).activationBlocked).toBeUndefined();
      await s.actors.close();
      next = owner.ask(actor.id, "NEXT_REVIEW");
    }
    await expect(next).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => owner.inFlightCount() === 0);
    expect(s.prompts().at(-1)).toMatchObject({ history: false, session: expect.not.stringMatching(carried), marker: "NEXT_REVIEW" });
    expect(owner.status(actor.id).activationBlocked).toBeUndefined();
    const registry = JSON.parse(fs.readFileSync(path.join(s.dir, "actors", "actors.json"), "utf8"));
    expect(registry.actors.find((row: { id: string }) => row.id === actor.id)).not.toHaveProperty("activationBlocked");
    await waitFor(() => (s.mesh.get(`actors/overflow-test/${actor.id}`)?.value as { activationBlocked?: unknown } | undefined)?.activationBlocked === undefined);
    expect(resets(owner, actor.id)).toEqual([expect.objectContaining({ trigger: "requested" })]);
    expect(s.overflowEvents()).toEqual([]);
  }, TEST_GUARD_MS);

  // Keep reason: trust boundary/data safety, #14075. A native retry must retain the
  // last GOOD Responses usage anchor and opaque-item coverage after a failed dispatch.
  // No classifier mocks or ActorManager event replay: the same worker/Pi child retries.
  it.each(["absent", "zero", "present"] as const)("14075 preserves covered Responses history through native overload retry (%s usage)", async usage => {
    vi.stubEnv("FAKE_PI_RESPONSES", "1");
    vi.stubEnv("FAKE_PI_OVERLOAD_USAGE", usage);
    const s = setup();
    s.recordResults();
    const actor = await s.create(`overload-${usage}`, { inferenceContext: "activation", model: "cliproxyapi/gpt-6-luna" });
    const outcome = await s.actors.ask(actor.id, "OVERLOAD_NATIVE_RETRY", { pr: 14075 }).catch(error => error);
    await waitFor(() => s.actors.inFlightCount() === 0);
    const trace = fs.readFileSync(path.join(s.dir, "prompts.jsonl.native"), "utf8").split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as Record<string, any>);
    // Retain compact, non-secret RED/GREEN evidence even when the terminal assertion fails.
    console.info("14075 native retry evidence", JSON.stringify({ usage, trace,
      results: s.results.map(({ status, errorCode, error, toolCalls }) => ({ status, errorCode, error, toolCalls })),
      blocked: s.actors.status(actor.id).activationBlocked, resets: resets(s.actors, actor.id),
      overflowNotices: s.overflowEvents().length, ownerAlarms: s.ownerAlarms().length,
      terminalFrames: s.results.flatMap(result => result.logFile && fs.existsSync(result.logFile)
        ? fs.readFileSync(result.logFile, "utf8").split("\n").filter(line => /fabric_activation_window|auto_retry|worker_stderr/.test(line))
          .map(line => line.slice(0, 1500)) : []) }));
    expect(trace[0]).toMatchObject({ type: "tool_turn", stopReason: "toolUse", totalTokens: 201_471, toolResultBytes: 6144 });
    // The oversized encoded payload MUST have been admitted once with covered ciphertext,
    // before the overload. Otherwise this would test only an ordinary genuinely large prompt.
    expect(trace[1]).toEqual({ type: "pre_overload_admitted" });
    expect(trace[2]).toMatchObject({ type: "overload", stopReason: "error",
      errorMessage: "cliproxyapi/gpt-6-luna: Our servers are currently overloaded. Please try again later." });
    if (usage === "absent") expect(trace[2]).not.toHaveProperty("usage");
    else expect(trace[2]!.usage.totalTokens).toBe(usage === "zero" ? 0 : 201_471);
    expect(trace[3]).toMatchObject({ type: "auto_retry_start", attempt: 1, maxAttempts: 3 });
    expect(trace[4]).toMatchObject({ type: "retry_admission", contextWindow: 272_000,
      nativeEstimate: { usageTokens: 201_471 } });
    expect(trace[4]!.nativeEstimate.tokens).toBeLessThan(272_000);
    expect(trace[4]!.rawPayloadEstimate).toBeGreaterThan(272_000);
    // Baseline RED: the bound hook refusal incorrectly replaces the transient overload
    // with CONTEXT_OVERFLOW, despite the earlier successful usage anchor covering input.
    expect(s.results.map(({ status, errorCode, toolCalls }) => ({ status, errorCode, toolCalls }))).toEqual([
      { status: "completed", errorCode: undefined, toolCalls: 1 },
    ]);
    expect(outcome).toMatchObject({ text: "verdict: ok" });
    expect(trace[5]).toEqual({ type: "retry_admitted" });
    expect(s.prompts()).toEqual([{ history: false, overflow: false, session: expect.any(String), marker: "OVERLOAD_NATIVE_RETRY" }]);
    expect(header(actor.sessionFile!)).toBe(s.prompts()[0]!.session);
    expect(backups(actor.sessionFile!)).toEqual([]);
    expect(resets(s.actors, actor.id)).toEqual([]);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(s.overflowEvents()).toEqual([]);
    expect(s.ownerAlarms()).toEqual([]);
    expect(s.alarms).toEqual([]);
    expect(fs.readFileSync(path.join(s.dir, "prompts.jsonl.requests"), "utf8")).toBe("request\nrequest\nrequest\n");
  }, TEST_GUARD_MS);

  it("14075 still types a genuinely oversized actual-hook payload as CONTEXT_OVERFLOW", async () => {
    const s = setup();
    s.recordResults();
    const actor = await s.create("genuine-hook-overflow", { inferenceContext: "activation" });
    await expect(s.actors.ask(actor.id, "PREFLIGHT_ALWAYS")).rejects.toThrow(/Fabric activation window failed: Context exceeds window/);
    await waitFor(() => s.actors.inFlightCount() === 0 && s.overflowEvents().length === 1 && resets(s.actors, actor.id).length === 1);
    expect(s.results).toEqual([expect.objectContaining({ status: "failed", errorCode: "CONTEXT_OVERFLOW", toolCalls: 0 })]);
    expect(s.prompts()).toHaveLength(1);
    expect(s.ownerAlarms()).toHaveLength(1);
    expect(s.alarms).toHaveLength(1);
  }, TEST_GUARD_MS);

  it.each([false, true])("a real stream disconnect stays transient with bounded backoff, even with a legacy false overflow tag (%s)", async malformed => {
    const cooldown = 987_654;
    const s = setup({ transientBlockCooldownMs: cooldown, transientBlockMaxMs: cooldown });
    const actor = await s.create("stream");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const carried = header(actor.sessionFile!);
    const wakes: Array<() => void> = [];
    const realSetTimeout = globalThis.setTimeout;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void, ms?: number, ...args: unknown[]) => {
      if (ms === cooldown) wakes.push(callback);
      return realSetTimeout(callback, ms, ...args);
    }) as typeof setTimeout);
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      if (malformed && result.status === "failed") result.errorCode = "CONTEXT_OVERFLOW";
      s.results.push(result);
      return result;
    });
    await expect(s.actors.ask(actor.id, "STREAM_DISCONNECT_ONCE")).rejects.toThrow(/stream disconnected before completion/);
    await waitFor(() => s.actors.inFlightCount() === 0);
    const blocked = s.actors.status(actor.id).activationBlocked!;
    expect(blocked).toMatchObject({ code: "provider-transient", class: "transient", retryAt: blocked.since + cooldown });
    expect(s.prompts().map(({ marker }) => marker)).toEqual(["FIRST_REVIEW", "STREAM_DISCONNECT_ONCE"]);
    expect(s.results[0]).toMatchObject({ status: "failed", toolCalls: 0 });
    expect(resets(s.actors, actor.id)).toEqual([]);
    expect(s.overflowEvents()).toEqual([]);
    expect(s.ownerAlarms()).toEqual([]);
    expect(wakes).toHaveLength(1);
    const realNow = Date.now.bind(Date);
    const skew = blocked.retryAt! + 1 - realNow();
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    wakes[0]!();
    const presence = () => s.mesh.get(`actors/overflow-test/${actor.id}`)?.value as
      { activationBlocked?: unknown; activationBlockExpired?: { code: string } } | undefined;
    await waitFor(() => presence()?.activationBlockExpired?.code === "provider-transient");
    expect(presence()?.activationBlocked).toBeUndefined();
    await expect(s.actors.ask(actor.id, "STREAM_DISCONNECT_ONCE")).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => s.actors.inFlightCount() === 0);
    expect(s.prompts().map(({ marker }) => marker)).toEqual(["FIRST_REVIEW", "STREAM_DISCONNECT_ONCE", "STREAM_DISCONNECT_ONCE"]);
    expect(header(actor.sessionFile!)).toBe(carried);
    expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(resets(s.actors, actor.id)).toEqual([]);
  }, TEST_GUARD_MS);

  it.each([
    ["STREAM_DISCONNECT_ONCE", "Refused by security policy", "security-policy", 1],
    ["OVERFLOW_ALWAYS", "Refused by security policy", "security-policy", 1],
    ["OVERFLOW_ALWAYS", "Authentication failed", "auth", 1],
    ["OVERFLOW_ALWAYS", "Context exceeds window; Authentication failed", "auth", 1],
    ["OVERFLOW_ALWAYS", "Context exceeds window; Refused by security policy", "security-policy", 1],
    ["OVERFLOW_ALWAYS", "prompt is too long; unsupported model", "invalid-request", 1],
    ["OVERFLOW_ALWAYS", "prompt is too long; Authentication failed", "auth", 1],
    ["STREAM_DISCONNECT_ONCE", "Authentication failed", "incomplete-evidence", 20],
    ["OVERFLOW_ALWAYS", "Authentication failed", "incomplete-evidence", 20],
    ["STREAM_DISCONNECT_ONCE", "benign diagnostic", "provider-transient", 19],
    ["OVERFLOW_ALWAYS", "benign diagnostic", "unknown", 19],
  ] as const)("classifies same-run %s with warning %s as %s at count %s", async (marker, warning, code, count) => {
    const s = setup();
    const actor = await s.create("stream-security");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      if (result.status === "failed") {
        result.errorCode = "CONTEXT_OVERFLOW";
        // The real process result passes through the worker's capped append sequence.
        const benign = Array.from({ length: count }, (_, index) => `benign diagnostic ${index}`);
        result.warnings = count === 1 ? [warning] : count === 20 ? [warning, ...benign].slice(-20) : benign;
        expect(result.warnings).toHaveLength(count);
        if (count === 20) expect(result.warnings).not.toContain("Authentication failed");
      }
      return result;
    });
    await expect(s.actors.ask(actor.id, marker)).rejects.toThrow(marker === "STREAM_DISCONNECT_ONCE"
      ? /stream disconnected before completion/ : /Context exceeds window/);
    await waitFor(() => s.actors.inFlightCount() === 0);
    if (count === 19) {
      if (code === "provider-transient") {
        const blocked = s.actors.status(actor.id).activationBlocked!;
        expect(blocked).toMatchObject({ code, class: "transient" });
        const realNow = Date.now.bind(Date);
        const skew = blocked.retryAt! + 1 - realNow();
        vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
      } else if ((code as string) === "context-overflow") {
        await waitFor(() => s.actors.status(actor.id).activationBlocked === undefined);
        expect(resets(s.actors, actor.id)).toHaveLength(2);
      } else {
        expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ code: "unknown", class: "hard" });
        expect(resets(s.actors, actor.id)).toHaveLength(0);
        await s.actors.resetSession(actor.id);
      }
      await expect(s.actors.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
      await waitFor(() => s.actors.inFlightCount() === 0);
      expect(s.prompts().at(-1)).toMatchObject({ marker: "NEXT_REVIEW" });
      expect(s.actors.status(actor.id).activationBlocked).toBeUndefined();
      return;
    }
    expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ code, class: "hard" });
    expect(s.actors.status(actor.id).activationBlocked).not.toHaveProperty("retryAt");
    expect(s.prompts()).toHaveLength(2);
    expect(resets(s.actors, actor.id)).toEqual([]);
    expect(s.overflowEvents()).toEqual([]);
  }, TEST_GUARD_MS);

  // Provenance contract: overflow-wording stripping must keep the terminal line, the full error and
  // worker warnings apart, so an earlier 401/403 line or a warning keeps its attribution durably.
  it.each([
    ["STREAM_DISCONNECT_ONCE", "error", "Error: 401 Unauthorized", "auth", / in another failure line\)$/],
    ["STREAM_DISCONNECT_ONCE", "error", "Error: 403 Forbidden", "permission", / in another failure line\)$/],
    ["STREAM_DISCONNECT_ONCE", "warning", "Error: 401 Unauthorized", "auth", / in a worker warning\)$/],
    ["OVERFLOW_ALWAYS", "warning", "Error: 403 Forbidden", "permission", / in a worker warning\)$/],
    ["OVERFLOW_ALWAYS", "error", "Error: unclassified activation failure", "unknown", /^fail-closed: no transient cause recognised/],
    ["OVERFLOW_ALWAYS", "error", "Error: 42", "unknown", /^fail-closed: no transient cause recognised/],
    ["OVERFLOW_ALWAYS", "warning", "Warning: unclassified activation warning", "unknown", /^fail-closed: no transient cause recognised/],
  ] as const)("attributes same-run %s %s evidence %s as %s with provenance across reload", async (marker, where, text, code, provenance) => {
    const s = setup();
    const actor = await s.create("provenance");
    await s.actors.ask(actor.id, "FIRST_REVIEW");
    await waitFor(() => s.actors.inFlightCount() === 0);
    const run = s.agents.run.bind(s.agents);
    vi.spyOn(s.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
      const result = await run(...args);
      if (result.status === "failed") {
        result.errorCode = "CONTEXT_OVERFLOW";
        // The terminal line stays the transient/overflow one; the permanent cause is earlier or a warning.
        if (where === "error") result.error = `${text}\n${result.error}`;
        else result.warnings = [text];
      }
      return result;
    });
    await expect(s.actors.ask(actor.id, marker)).rejects.toThrow(marker === "STREAM_DISCONNECT_ONCE"
      ? /stream disconnected before completion/ : /Context exceeds window/);
    await waitFor(() => s.actors.inFlightCount() === 0);
    const blocked = s.actors.status(actor.id).activationBlocked!;
    expect(blocked).toMatchObject({ code, class: "hard" });
    expect(blocked).not.toHaveProperty("retryAt");
    expect(blocked).not.toMatchObject({ overflowEvidence: { complete: true } });
    expect(blocked.classification).toMatch(provenance);
    expect(resets(s.actors, actor.id)).toEqual([]);
    expect(s.overflowEvents()).toEqual([]);
    await s.actors.close();
    const restarted = s.makeActors();
    managers.push(restarted);
    await waitFor(() => restarted.status(actor.id).activationBlocked?.class === "hard");
    // A current unknown block may be re-derived from its terminal overflow wording on load;
    // either representation remains hard and lacks the complete overflow proof needed to reset.
    expect(restarted.status(actor.id).activationBlocked).toMatchObject({ class: "hard" });
    expect(restarted.status(actor.id).activationBlocked).not.toHaveProperty("retryAt");
    expect(resets(restarted, actor.id)).toEqual([]);
    expect(s.prompts()).toHaveLength(2);
  }, TEST_GUARD_MS);

  // Trust-boundary/data-safety check: old blocks omitted warnings. Use the actual process
  // worker's retained status, then model old metadata and missing/corrupt/mismatched evidence.
  it.each((["stream", "overflow"] as const).flatMap(cause => {
    const evidence = ["pure", "nineteen", "saturated", "marked-saturated", "auth", "security", "missing", "unreadable", "mismatched"] as const;
    return [...evidence, ...(cause === "overflow" ? ["unknown", "marked-unknown"] as const : [])].map(evidence => ({ cause, evidence }));
  }))
  ("restores legacy $cause with $evidence complete-run evidence, never replaying the settled item", async ({ cause, evidence }) => {
    const s = setup();
    const actor = await s.create("legacy");
    const marker = cause === "stream" ? "STREAM_DISCONNECT_ONCE" : "OVERFLOW_ALWAYS";
    await expect(s.actors.ask(actor.id, marker)).rejects.toThrow(cause === "stream"
      ? /stream disconnected before completion/ : /Context exceeds window/);
    await waitFor(() => s.actors.inFlightCount() === 0 && (cause === "stream" || s.actors.status(actor.id).activationBlocked === undefined));
    const runId = s.actors.status(actor.id).lastRunId!;
    const statusFile = path.join(path.dirname(actor.sessionFile!), "runs", runId, "status.json");
    await waitFor(() => fs.existsSync(statusFile));
    const status = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    expect(status).toMatchObject({ id: runId, actorId: actor.id, status: "failed" });
    expect(status.error).toMatch(cause === "stream" ? /stream disconnected/ : /Context exceeds window/);
    const carried = header(actor.sessionFile!);
    const resetCount = resets(s.actors, actor.id).length;
    const promptCount = s.prompts().length;
    await s.actors.close();
    // Legacy worker mistagged pure stream failures, but the complete record retained warnings.
    status.errorCode = "CONTEXT_OVERFLOW";
    if (evidence === "nineteen") status.warnings = Array.from({ length: 19 }, (_, index) => `benign diagnostic ${index}`);
    if (evidence === "saturated" || evidence === "marked-saturated") {
      status.warnings = ["Authentication failed", ...Array.from({ length: 20 }, (_, index) => `benign diagnostic ${index}`)].slice(-20);
      expect(status.warnings).toHaveLength(20);
      expect(status.warnings).not.toContain("Authentication failed");
    }
    if (evidence === "auth") status.warnings = ["Authentication failed"];
    if (evidence === "security") status.warnings = ["Refused by security policy"];
    if (evidence === "unknown" || evidence === "marked-unknown") status.warnings = ["Warning: unclassified activation warning"];
    if (evidence === "mismatched") status.id = "not-the-blocked-run";
    if (evidence === "missing") fs.renameSync(statusFile, `${statusFile}.missing`);
    else fs.writeFileSync(statusFile, evidence === "unreadable" ? "{" : JSON.stringify(status));
    const file = path.join(s.dir, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(file, "utf8"));
    const row = registry.actors.find((candidate: { id: string }) => candidate.id === actor.id);
    row.activationBlocked = { code: "context-overflow", class: "hard", reason: status.error.slice(0, 300),
      since: Date.now() - 24 * 60 * 60 * 1000, count: 3,
      classification: "permanent: context window exceeded (typed CONTEXT_OVERFLOW run code)" };
    if (evidence === "marked-saturated" || evidence === "marked-unknown") row.activationBlocked.overflowEvidence = { version: 1, runId, complete: true };
    row.failureStreak = { count: 3, notified: true };
    fs.writeFileSync(file, JSON.stringify(registry));
    let restarted = s.makeActors();
    managers.push(restarted);
    const presence = () => s.mesh.get(`actors/overflow-test/${actor.id}`)?.value as
      { activationBlocked?: { code: string }; activationBlockExpired?: { code: string } } | undefined;
    const recoverable = evidence === "pure" || (evidence === "nineteen" && cause === "stream");
    if (recoverable && cause === "stream") {
      await waitFor(() => presence()?.activationBlockExpired?.code === "provider-transient");
      expect(presence()?.activationBlocked).toBeUndefined();
      expect(header(actor.sessionFile!)).toBe(carried);
      expect(resets(restarted, actor.id)).toHaveLength(resetCount);
    } else if (recoverable) {
      await waitFor(() => resets(restarted, actor.id).length === resetCount + 1 && presence()?.activationBlocked === undefined);
      expect(header(actor.sessionFile!)).not.toBe(carried);
      expect(resets(restarted, actor.id).at(-1)).toMatchObject({ overflow: true, retry: false });
    } else {
      const code = evidence === "auth" ? "auth" : evidence === "security" ? "security-policy"
        : evidence === "unknown" || evidence === "marked-unknown" || (evidence === "nineteen" && cause === "overflow") ? "unknown"
        : evidence === "saturated" || evidence === "marked-saturated" ? "incomplete-evidence" : "context-overflow";
      await waitFor(() => presence()?.activationBlocked?.code === code);
      expect(restarted.status(actor.id).activationBlocked).toMatchObject({ code, class: "hard" });
      expect(restarted.status(actor.id).activationBlocked).not.toHaveProperty("retryAt");
      if (code === "unknown") expect(restarted.status(actor.id).activationBlocked)
        .toMatchObject({ overflowEvidence: { version: 1, complete: false } });
      expect(header(actor.sessionFile!)).toBe(carried);
      expect(resets(restarted, actor.id)).toHaveLength(resetCount);
      if (code === "context-overflow" || code === "incomplete-evidence") {
        expect(restarted.status(actor.id).activationBlocked).toMatchObject({ overflowEvidence: { version: 1, complete: false } });
        // A later readable record must not let a re-save re-derive the incomplete reason.
        await restarted.close();
        status.id = runId;
        status.warnings = []; // Re-save/reload must not upgrade incomplete evidence.
        fs.writeFileSync(statusFile, JSON.stringify(status));
        restarted = s.makeActors();
        managers.push(restarted);
        await waitFor(() => presence()?.activationBlocked?.code === code);
        expect(restarted.status(actor.id).activationBlocked).toMatchObject({ code, class: "hard" });
        if (code === "context-overflow") expect(restarted.status(actor.id).activationBlocked)
          .toMatchObject({ overflowEvidence: { version: 1, complete: false } });
        expect(header(actor.sessionFile!)).toBe(carried);
        expect(resets(restarted, actor.id)).toHaveLength(resetCount);
      }
      await restarted.resetSession(actor.id);
    }
    expect(s.prompts()).toHaveLength(promptCount);
    await expect(restarted.ask(actor.id, "NEXT_REVIEW")).resolves.toMatchObject({ text: "verdict: ok" });
    await waitFor(() => restarted.inFlightCount() === 0);
    expect(s.prompts()).toHaveLength(promptCount + 1);
    expect(restarted.status(actor.id).activationBlocked).toBeUndefined();
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
