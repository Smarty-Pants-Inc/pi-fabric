import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import {
  registerAgentRunner,
  type FabricHostedReporter,
  type FabricHostedRunner,
  type FabricRunnerCapabilities,
  type FabricTranscriptEvent,
} from "../src/runners.js";
import { TranscriptAccumulator } from "../src/ui/transcript-parser.js";

// B3: a custom adapter must not depend on src/durable.ts doing the projection.
// Keep reason: the reporter -> physical events.jsonl trust/data-safety boundary.
const ENTRY_PATHS = ["submit", "attach"] as const;
type EntryPath = (typeof ENTRY_PATHS)[number];
const FAKE = "FAKE_HOSTED_UNGUARDED_TOOL_PAYLOAD";
const PERCENT_FAKE = [...FAKE].map((char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`).join("");
const NONE: FabricRunnerCapabilities = {
  recursiveFabric: false, steer: false, followUp: false, persistentSessions: false,
  kernels: false, handoff: false, modelDiscovery: false, imageInput: false,
  compaction: false, questions: false, sleep: false, writePolicy: false,
};
const USAGE = { input: 23, output: 11, cacheRead: 5, cacheWrite: 2, cost: 0.125 };
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

/** Real manager admission and recovery, with no worker process or live service. */
async function persistReports(entryPath: EntryPath, report: (reporter: FabricHostedReporter) => void) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-hosted-log-safety-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const calls: string[] = [];
  const emit = (reporter: FabricHostedReporter): void => {
    reporter.progress({ turns: 2, toolCalls: 2, currentTool: "lookup", text: "working" });
    reporter.usage({ input: 10, output: 4, cacheRead: 1, cacheWrite: 0, cost: 0.05 });
    report(reporter);
    reporter.usage(USAGE);
    reporter.finish({ status: "completed", output: "Hosted work complete.", structured: { ok: true, count: 2 } });
  };
  const adapter: FabricHostedRunner = {
    kind: "hosted",
    id: "hosted-log-safety",
    label: "Hosted log safety probe",
    capabilities: NONE,
    prepare: (context) => {
      calls.push("prepare");
      return { job: context.idempotencyKey };
    },
    start: (_locator, _context, reporter) => {
      calls.push("start");
      if (entryPath === "submit") emit(reporter);
    },
    attach: (_locator, _context, reporter) => {
      calls.push("attach");
      emit(reporter);
    },
    liveness: () => "running",
    stop: () => {
      calls.push("stop");
      return { confirmed: true };
    },
  };
  cleanups.push(registerAgentRunner(adapter));
  const makeManager = (): AgentManager => {
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      runRoot: root, fullCodeMode: false,
    });
    cleanups.push(() => manager.close());
    return manager;
  };
  const first = makeManager();
  let manager = first;
  let id: string;
  if (entryPath === "submit") {
    id = (await first.spawn({ task: "Probe hosted transcript persistence", runner: adapter.id })).id;
  } else {
    id = (await first.spawn({
      task: "Probe hosted transcript persistence", runner: adapter.id, residency: "durable",
    })).id;
    // A real close detaches a durable hosted run. Recovery must attach, not resubmit.
    await first.close();
    expect(calls).toEqual(["prepare", "start"]);
    manager = makeManager();
    expect(await manager.recoverHostedRuns()).toEqual([id]);
  }
  const result = await manager.wait(id);
  const runDirectory = manager.runDirectory(id)!;
  const status = JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8"));
  const settled = {
    status: "completed", runner: adapter.id, transport: "hosted",
    text: "Hosted work complete.", value: { ok: true, count: 2 }, turns: 2, toolCalls: 2, usage: USAGE,
  };
  expect(result).toMatchObject(settled);
  expect(status).toMatchObject(settled);
  expect(result.outcome).toBeUndefined();
  expect(status.currentTool).toBeUndefined();
  expect(calls).toEqual(entryPath === "submit" ? ["prepare", "start"] : ["prepare", "start", "attach"]);
  const bytes = fs.readFileSync(path.join(runDirectory, "events.jsonl"), "utf8");
  const events = bytes.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>);
  return { bytes, events };
}

// The published API is typed. This one deliberate cast models a JS/custom
// adapter supplying extra runtime fields or events, not a new extension API.
function sendRuntimeEvents(reporter: FabricHostedReporter, events: readonly unknown[]): void {
  for (const event of events) reporter.transcript(event as FabricTranscriptEvent);
}

function expectNoPayload(bytes: string): void {
  expect(bytes).not.toContain("FAKE");
  expect(bytes).not.toContain(PERCENT_FAKE);
}

describe("hosted reporter transcript log safety (B3)", () => {
  it.each(ENTRY_PATHS)("%s: drops raw tool payloads and opaque envelopes without losing ordinary text or completions", async (entryPath) => {
    const ordinary: FabricTranscriptEvent[] = [
      { type: "message_end", message: { role: "user", content: "Look up the release notes.\nKeep 50% of the examples." } },
      { type: "message_end", message: { role: "assistant", content: "I will check the notes — café." } },
    ];
    const tools = [false, true].flatMap((isError) => {
      const toolCallId = isError ? "native-error" : "native-success";
      return [
        {
          type: "tool_execution_start", toolCallId, toolName: "lookup",
          args: { query: `${FAKE}_ARGS` }, arguments: { query: `${FAKE}_ARGUMENTS` },
          opaque: { history: [FAKE] },
        },
        {
          type: "tool_execution_update", toolCallId, toolName: "lookup",
          partialResult: { content: [{ type: "text", text: `${FAKE}_UPDATE` }], details: { secret: FAKE } },
        },
        {
          type: "tool_execution_end", toolCallId, toolName: "lookup", isError,
          result: {
            content: [{ type: "text", text: `${FAKE}_${isError ? "ERROR" : "SUCCESS"}_TEXT` }],
            details: { secret: `${FAKE}_RESULT_DETAILS` },
            structuredContent: { secret: `${FAKE}_RESULT_STRUCTURED` },
            percent: PERCENT_FAKE,
          },
          text: `${FAKE}_EVENT_TEXT`, details: { secret: `${FAKE}_EVENT_DETAILS` },
          structuredContent: { secret: `${FAKE}_EVENT_STRUCTURED` },
          opaque: { nested: [FAKE, PERCENT_FAKE] },
        },
      ];
    });
    const assistantBlocks = {
      type: "message_end",
      message: {
        role: "assistant",
        content: [
          { type: "text", text: "The ordinary assistant paragraph survives.", details: FAKE },
          { type: "toolCall", id: "embedded-call", name: "lookup", arguments: { secret: FAKE } },
        ],
        history: [{ role: "toolResult", content: FAKE }],
        structuredContent: { secret: FAKE },
      },
      opaque: { secret: FAKE },
    };
    const extensionError = { type: "extension_error", error: "Hosted transport diagnostic.", details: FAKE, opaque: FAKE };
    const events: unknown[] = [
      ...ordinary.map((event) => ({ ...event, history: [FAKE], opaque: { secret: FAKE } })),
      ...tools, assistantBlocks, extensionError,
      { type: "message_update", assistantMessageEvent: { delta: FAKE }, history: [PERCENT_FAKE] },
      { type: "message_end", message: { role: "toolResult", toolCallId: "raw-history", content: FAKE } },
      { type: "turn_end", messages: [{ role: "toolResult", content: FAKE }], details: PERCENT_FAKE },
      { type: "adapter_private_dump", payload: { text: FAKE, structuredContent: { secret: PERCENT_FAKE } } },
    ];
    const original = structuredClone(events);
    const persisted = await persistReports(entryPath, (reporter) => sendRuntimeEvents(reporter, events));
    expect(events).toStrictEqual(original);
    expectNoPayload(persisted.bytes);
    expect(persisted.events).toStrictEqual([
      ...ordinary,
      ...[false, true].flatMap((isError) => {
        const toolCallId = isError ? "native-error" : "native-success";
        return [
          { type: "tool_execution_start", toolCallId, toolName: "lookup" },
          { type: "tool_execution_end", toolCallId, toolName: "lookup", isError },
        ];
      }),
      { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "The ordinary assistant paragraph survives." }] } },
      { type: "extension_error", error: "Hosted transport diagnostic." },
    ]);
    // Read the actual persisted metadata through its consumer, not a mock call.
    const transcript = new TranscriptAccumulator();
    transcript.append(persisted.events);
    expect(transcript.snapshot().entries.filter((entry) => entry.kind === "tool")).toStrictEqual([
      { id: "native-success", kind: "tool", label: "lookup", toolName: "lookup", status: "completed" },
      { id: "native-error", kind: "tool", label: "lookup", toolName: "lookup", status: "failed" },
    ]);
  });

  it.each(ENTRY_PATHS)("%s: preserves finite byte metadata without traversing omitted payloads or invoking input toJSON", async (entryPath) => {
    const payloadReads: string[] = [];
    let serializationCalls = 0;
    const withPayloadGetters = (event: Record<string, unknown>): Record<string, unknown> => {
      for (const key of ["args", "result", "details", "structuredContent", "opaque"]) {
        Object.defineProperty(event, key, {
          enumerable: true,
          configurable: true,
          get: () => {
            payloadReads.push(`${String(event.type)}.${key}`);
            return { text: FAKE, percent: PERCENT_FAKE };
          },
        });
      }
      return event;
    };
    // argsBytes/resultBytes are the existing built-in durable event shape;
    // reproduce it at the shared boundary without importing that adapter.
    const events = [
      withPayloadGetters({ type: "tool_execution_start", toolCallId: "sized", toolName: "lookup", argsBytes: 0 }),
      withPayloadGetters({ type: "tool_execution_end", toolCallId: "sized", toolName: "lookup", isError: false, resultBytes: 123 }),
      {
        type: "tool_execution_end", toolCallId: "serialization-hook", toolName: "lookup", isError: true, resultBytes: 7,
        toJSON: () => {
          serializationCalls++;
          return { type: "adapter_private_dump", payload: FAKE, percent: PERCENT_FAKE };
        },
      },
      withPayloadGetters({ type: "tool_execution_start", toolCallId: "nonfinite", toolName: "lookup", argsBytes: Infinity }),
      withPayloadGetters({ type: "tool_execution_end", toolCallId: "nonfinite", toolName: "lookup", isError: true, resultBytes: NaN }),
    ];
    // Descriptor snapshots do not themselves evaluate the payload getters.
    const originalDescriptors = events.map((event) => Object.getOwnPropertyDescriptors(event));
    const persisted = await persistReports(entryPath, (reporter) => sendRuntimeEvents(reporter, events));
    expect(payloadReads).toStrictEqual([]);
    expect(serializationCalls).toBe(0);
    expect(events.map((event) => Object.getOwnPropertyDescriptors(event))).toStrictEqual(originalDescriptors);
    expectNoPayload(persisted.bytes);
    expect(persisted.events).toStrictEqual([
      { type: "tool_execution_start", toolCallId: "sized", toolName: "lookup", argsBytes: 0 },
      { type: "tool_execution_end", toolCallId: "sized", toolName: "lookup", isError: false, resultBytes: 123 },
      { type: "tool_execution_end", toolCallId: "serialization-hook", toolName: "lookup", isError: true, resultBytes: 7 },
      { type: "tool_execution_start", toolCallId: "nonfinite", toolName: "lookup" },
      { type: "tool_execution_end", toolCallId: "nonfinite", toolName: "lookup", isError: true },
    ]);
  });
});
