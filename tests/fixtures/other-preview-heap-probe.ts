import assert from "node:assert/strict";
import type { ExtensionAPI, ExtensionContext, SessionEntry } from "@earendil-works/pi-coding-agent";
import { AgentCompletionInbox } from "../../src/agents/completion-inbox.js";
import { HostedRun } from "../../src/agents/hosted-run.js";
import type { AgentRunRecord } from "../../src/agents/types.js";
import type { FabricHostedRunner } from "../../src/agents/runner-registry.js";
import { buildThinkingDigest } from "../../src/agents/thinking-transfer.js";
import { snippetTask } from "../../src/agents/fork-seed.js";
import { FabricWorkflowItemTransitions } from "../../src/activity/workflow-items.js";
import { buildActorContext } from "../../src/actors/context.js";
import { ActorLogStore } from "../../src/actors/log-store.js";
import type { FabricActorMessage } from "../../src/actors/types.js";
import { recordsInboxMessage } from "../../src/records/inbox.js";
import type { RecordEnvelope } from "../../src/records/store.js";
import { truncateMiddle } from "../../src/util.js";
import { boundModelOutput } from "../../src/output-budget.js";
import { ShellMonitor } from "../../src/core/shell-monitor.js";
import { montyErrorText } from "../../src/runtime/monty-source.js";
import { CPythonRuntime } from "../../src/runtime/cpython-runtime.js";
import { MontyRuntime } from "../../src/runtime/monty-runtime.js";
import { presentRecall } from "../../src/memory/context.js";
import { processMemoryExpand } from "../../src/memory/expand-service.js";
import { createMemorySourceRegistry } from "../../src/memory/portable.js";
import { MemoryRequestCache } from "../../src/memory/request-cache.js";
import type { MemoryProviderContext } from "../../src/memory/request-context.js";
import type { NormalizedEntry } from "../../src/memory/normalize.js";
import type { SearchResult, SearchSegment } from "../../src/memory/search.js";
import { JevObservationHost } from "../../src/jev/observation.js";

export const MODES = ["control", "workflow", "completion", "completion-pressure", "hosted", "strings", "actors", "memory", "shell", "observation", "cpython", "monty"] as const;
export type ProbeMode = typeof MODES[number];
const INPUT_BYTES = 4 * 1024 * 1024;
const COUNT = 100;
const coverage = { complete: true, indexedSessions: 1, eligibleSessions: 1, staleSessions: 0, incompleteSessions: 0, reasons: [] };

// Each input is a distinct, flat allocation. Repeated ropes or a shared backing
// string make a sliced-string regression look artificially cheap.
function inputAt(index: number): string {
  const prefix = `unique-${index}|`;
  const text = prefix + String.fromCharCode(97 + index % 26).repeat(INPUT_BYTES - prefix.length);
  // JSON serialization gives V8-owned backing storage. Buffer.toString alone
  // can be external storage whose slices V8 copies during collection.
  return JSON.stringify(text).slice(1, -1);
}

export function branch(text: string): SessionEntry[] {
  return [
    { type: "message", id: "user", parentId: null, timestamp: "2024-01-01", message: { role: "user", content: text, timestamp: 1 } },
    { type: "message", id: "assistant", parentId: "user", timestamp: "2024-01-02", message: {
      role: "assistant", provider: "fixture", model: "source", content: [{ type: "thinking", thinking: text }],
    } },
  ] as SessionEntry[];
}

export function inboxHarness(commit = true) {
  const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => unknown>();
  const messages: Array<{ content: string; details: { ids: string[] } }> = [];
  const context = { hasUI: false, isIdle: () => false, hasPendingMessages: () => false } as ExtensionContext;
  const pi = {
    on: (event: string, handler: (event: unknown, ctx: ExtensionContext) => unknown) => { handlers.set(event, handler); return () => handlers.delete(event); },
    sendMessage: (message: { content: string; details: { ids: string[] } }) => messages.push(message),
  } as unknown as ExtensionAPI;
  const claims: string[][] = [];
  const inbox = new AgentCompletionInbox(pi, context, commit ? ids => { claims.push(ids); } : undefined);
  const boundary = () => handlers.get("turn_end")!({ message: { role: "assistant", stopReason: "stop" } }, context);
  return { inbox, messages, claims, boundary };
}

export function hostedRun(id: string) {
  const record: AgentRunRecord = { id, name: id, task: "fixture", status: "running", runner: "fixture", transport: "hosted", cwd: ".",
    startedAt: 1, updatedAt: 1, turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  // Recovery is the public no-I/O construction path. Progress's best-effort
  // status write fails before opening anything at this impossible path.
  return HostedRun.recover({ id: "fixture", label: "fixture", capabilities: {} } as FabricHostedRunner,
    { version: 1, runner: "fixture", locator: {}, context: { id } } as Parameters<typeof HostedRun.recover>[1],
    { statusFile: "\0", lifecycleFile: "\0", logFile: "\0", stateFile: "\0" }, record, { questionTimeoutMs: 1000 });
}

export function recall(text: string, snippetChars = 480) {
  const entry: NormalizedEntry = { sessionFile: "fixture", sessionId: "s", index: 0, entryId: "e", parentId: null,
    type: "message", role: "toolResult", toolName: "fixture", text, timestamp: 1, isError: false, truncated: false };
  const segment: SearchSegment = { sessionId: "s", sessionFile: "fixture", sourceHash: "hash", branches: "active", lineageFingerprint: "lineage",
    sessionMtime: 1, range: "0", entryRange: { first: 0, last: 0 }, entries: [{ entry, matched: true, marker: ">", score: 1 }],
    exactMatches: [], matchedCount: 1, score: 1, tier: "hot" };
  const result: SearchResult = { matchMode: "browse", matchedCount: 1, totalMatches: 1, totalItems: 1, segmentCount: 1,
    segments: [segment], digestHits: [], items: [{ kind: "entry", segment }], queryCoverage: { complete: true, reasons: [] } };
  return presentRecall({ result, queryMode: "literal", coverage, pageSize: 1, snippetChars, requestArgs: {} });
}

export async function expand(text: string, maxChars = 256, textOffset = 0) {
  const sources = createMemorySourceRegistry();
  sources.register({ interfaceVersion: 1, id: "fixture", async listSessions() { return []; }, async loadSession() {
    return { sessionKey: "s", sessionId: "s", revision: "1", records: [
      { type: "session", id: "s", cwd: "/fixture", timestamp: "2024-01-01" },
      { type: "message", id: "e", parentId: null, timestamp: "2024-01-02", message: { role: "user", content: text } },
    ] };
  } });
  const context = { sources, agentDir: "/fixture", cwd: "/fixture", config: {} } as MemoryProviderContext;
  // Only the returned page survives. The intentional lossless pagination
  // snapshot/cache and the adapter input are not held by this heap probe.
  return await processMemoryExpand({ source: "fixture", session: "s", indices: [0], maxChars, textOffset }, context, new MemoryRequestCache()) as {
    entries: Array<{ text: string; textRange: { start: number; end: number; total: number; complete: boolean } }>;
    next: { args: { textOffset: number } } | null;
    error?: unknown;
  };
}

export function actorMessage(index: number, text?: string, data?: unknown): FabricActorMessage {
  return { id: String(index), actorId: "actor", actorName: "fixture", direction: "out", source: "fixture", createdAt: index,
    ...(text !== undefined ? { text } : {}), ...(data !== undefined ? { data } : {}) };
}

export function record(text: string, data: Record<string, unknown>): RecordEnvelope {
  return { id: "r", org: "fixture", origin: "fixture", sequence: 1, ref: "fixture#1", topic: "record.ask", kind: "ask",
    from: "fixture", createdAt: 1, text, data, key: "fixture" };
}

async function collect(): Promise<void> {
  for (let round = 0; round < 5; round++) {
    (globalThis as typeof globalThis & { gc(): void }).gc();
    await new Promise<void>(resolve => setImmediate(resolve));
  }
}

export async function run(mode: ProbeMode): Promise<void> {
  assert(MODES.includes(mode));
  assert.equal(typeof (globalThis as { gc?: unknown }).gc, "function", "run with --expose-gc");
  const retained: unknown[] = [];
  const workflow = new FabricWorkflowItemTransitions("fixture", undefined);
  const completion = inboxHarness();
  const receipts: string[] = [];
  const prepared: string[] = [];
  const actorStore = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 480 }, { actorRunArchiveMs: 1 });
  const history: FabricActorMessage[] = [];
  const observer = new JevObservationHost("s", () => {});
  const subscription = observer.subscribe({ events: ["turn_end"], include: ["assistantText", "toolResults"], maxChars: 512 }, { id: "run", name: "fixture" }, new AbortController());
  const runtime = mode === "monty" ? new MontyRuntime() : new CPythonRuntime();
  const writer = async () => { throw new Error("artifact sink intentionally unavailable"); };
  const processOne = async (text: string, index: number): Promise<void> => {
    if (mode === "control") retained.push(text.slice(0, 480));
    else if (mode === "workflow") workflow.record({ id: text.slice(0, 36), status: "running" }, text);
    else if (mode === "completion" || mode === "completion-pressure") {
      const id = String(index);
      completion.inbox.enqueue({ id, name: text.slice(0, 80), status: "failed", startedAt: 1, text,
        completionDelivery: { status: "undelivered", addressedTo: text.slice(0, 36), redeliveredFrom: text.slice(0, 36) },
        ...(mode === "completion-pressure" ? { partialText: text, error: text, warnings: Array(20).fill(text) as string[] } : {}) },
      () => { receipts.push(id); }, () => { prepared.push(id); });
    } else if (mode === "hosted") {
      const run = hostedRun(String(index));
      run.reporter.progress({ text, currentTool: text });
      retained.push(run.record);
      run.close();
      const failure = hostedRun("failure-" + index);
      failure.reporter.fail({ error: text });
      retained.push(failure.record);
    } else if (mode === "strings") {
      retained.push(truncateMiddle(text, 480), truncateMiddle(text.slice(0, 480), 480));
      retained.push(await boundModelOutput(text, 512, text, writer));
      retained.push(await boundModelOutput(text.slice(0, 480), 512, undefined, writer));
      retained.push(await boundModelOutput(text, 80, text, writer, { text: "Safety-critical receipt: id=fixture", sections: [text] }));
      retained.push(snippetTask(branch(text), 1, "task"));
      retained.push(buildThinkingDigest(branch(text), { source: { provider: "fixture", modelId: "source" }, target: { provider: "other", modelId: "target" } }));
      retained.push(montyErrorText(new Error(text)));
    } else if (mode === "actors") {
      retained.push(buildActorContext([{ role: "user", content: text }, { role: "toolResult", content: text, isError: true }], 2, 2000));
      const message = actorMessage(index, text, { huge: text });
      actorStore.recordMessage(history, message);
      retained.push(message);
      retained.push(recordsInboxMessage([record("", { huge: text })]));
    } else if (mode === "memory") {
      retained.push(recall(text));
      const page = await expand(text);
      assert.equal(page.error, undefined);
      assert.equal(page.entries[0]?.text, text.slice(0, 256));
      assert.equal(page.next?.args.textOffset, 256);
      retained.push(page);
    } else if (mode === "shell") {
      const monitor = new ShellMonitor({ delivery: "ui", timeoutMs: 1000, intervalMs: 1000 }, batch => retained.push(batch));
      monitor.append(Buffer.from(text + "\n", "utf8"));
      monitor.close();
    } else if (mode === "observation") {
      observer.observe("turn_end", { message: { role: "assistant", stopReason: "stop", content: [{ type: "text", text }] }, toolResults: [] }, { sessionId: "s" });
      retained.push(await subscription.next());
    } else {
      const result = await runtime.execute('print(π.body)\nreturn "done"', async () => null,
        { strings: { body: text }, maxLogChars: 480, timeoutMs: 60_000, memoryLimitBytes: 256 * 1024 * 1024 });
      assert.equal(result.terminationReason, "completed", result.error ?? "Python execution did not complete");
      assert.equal(result.value, "done");
      assert.equal(result.logs[0], text.slice(0, 480));
      assert.equal(result.logs.at(-1), "[Pi Fabric log output truncated]");
      retained.push(result);
    }
  };
  // Pay first-use/native loading and compilation before taking the baseline.
  await processOne(inputAt(-1), -1);
  retained.length = 0;
  history.length = 0;
  completion.inbox.acknowledge("-1");
  await collect();
  const before = process.memoryUsage().heapUsed;
  for (let index = 0; index < COUNT; index++) await processOne(inputAt(index), index);
  await collect();
  const retainedBytes = process.memoryUsage().heapUsed - before;
  if (mode === "completion" || mode === "completion-pressure") {
    for (let batch = 0; batch < 4; batch++) completion.boundary();
    assert.equal(completion.messages.flatMap(message => message.details.ids).length, COUNT);
    assert.equal(new Set(receipts).size, COUNT);
    assert.equal(new Set(prepared).size, COUNT);
  }
  const count = retained.length + history.length;
  // Keep public outputs/caches live through the measurement, and prove labels
  // still emit after their huge input parents have become unreachable.
  if (mode === "workflow") workflow.finish(true);
  completion.inbox.close();
  observer.close();
  console.log(JSON.stringify({ mode, inputs: COUNT, inputBytes: INPUT_BYTES, retainedBytes, count }));
}

if (process.argv[1]?.endsWith("other-preview-heap-probe.ts")) {
  if (process.argv[2] === "compatibility") {
    // Standalone, no repository config/setup: the delegated writer is not
    // allowed to run the normal config's exit-time filesystem cleanup.
    const { createVitest } = await import("vitest/node");
    const tests = await createVitest("test", {
      config: false, watch: false, maxWorkers: 1, fileParallelism: false,
      include: ["tests/other-preview-heap.test.ts"],
      testNamePattern: "other preview compatibility and custody counterexamples",
    });
    try { await tests.start(); } finally { await tests.close(); }
  } else await run(process.argv[2] as ProbeMode);
}
