import assert from "node:assert/strict";
import fs from "node:fs";
import { setImmediate as nextTurn } from "node:timers/promises";
import type { ExtensionAPI, ExtensionContext, TurnEndEvent } from "@earendil-works/pi-coding-agent";
import type { ResolvedFabricAction } from "../../src/core/action-registry.js";
import { detachedPreview } from "../../src/core/preview.js";
import { FabricAutoApprovalClassifier } from "../../src/core/auto-approval-classifier.js";
import { compactActivationTools } from "../../src/worker/activation-compaction.js";
import { summarizeActorState } from "../../src/worker/context-reseed.js";
import { PiEventProjection } from "../../src/worker/event-projection.js";
import { formatFabricValue } from "../../src/ui/structured.js";
import { NativeConversationReader } from "../../src/ui/conversation-native-reader.js";
import { ProviderParticipantRegistry } from "../../src/topology/provider-participants.js";
import { registerCompactionRecovery } from "../../src/compaction/recovery.js";

// Keep reason: #10720's retained-parent failure cannot be reached cheaply by a
// UI flow. Measure real public projections without flattening their results.
const SIZE = 4 * 1024 * 1024;
const COUNT = 100;
const LIMIT = 5 * 1024 * 1024;
const CONTROL_MIN = 350 * 1024 * 1024;
const gc = globalThis.gc;
assert.equal(typeof gc, "function", "launch this fixture with --expose-gc");
const collect = () => { gc!(); gc!(); gc!(); };
// JSON serialization allocates a flat V8 heap string, unlike decoder strings
// that this engine may externalize. ASCII keeps every parent exactly SIZE UTF-16
// units. The negative control must retain >350 MiB before any fixed claim.
const large = (index: number) => JSON.stringify(Buffer.alloc(SIZE, 97 + index % 26).toString("utf8")).slice(1, -1);
const samples: Array<{ name: string; retainedBytes: number; limitBytes: number; pass: boolean }> = [];
async function sample<T>(name: string, create: (index: number) => T | Promise<T>, check: (held: T) => void, control = false, limitBytes = LIMIT) {
  await nextTurn(); collect();
  const before = process.memoryUsage().heapUsed;
  const held: T[] = [];
  for (let index = 0; index < COUNT; index++) held.push(await create(index));
  await nextTurn(); collect();
  const retainedBytes = process.memoryUsage().heapUsed - before;
  samples.push({ name, retainedBytes, limitBytes, pass: control ? retainedBytes > CONTROL_MIN : retainedBytes < limitBytes });
  // Check content only AFTER collection: string comparison can flatten a
  // ConsString and accidentally conceal precisely the bug being tested.
  // Defer heap assertions until every receipt is emitted, including baseline.
  for (const value of held) check(value);
}

assert.equal(detachedPreview("A\ud83d\ude80\ud800\udc00\udfffZ", 2), "A\ud83d");
assert.equal(detachedPreview("\ud800\udfff", 99), "\ud800\udfff");
assert.equal(detachedPreview("hello", 0), "");
assert.equal(detachedPreview("hello", -1), "");

// Load the optional YAML renderer before taking its retained-heap baseline.
formatFabricValue({ text: "warm\nup" }, "auto", 2048);
await sample("negative-control", index => large(index).slice(0, 2048), value => assert.equal(value.length, 2048), true);
await sample("detached-prefix", index => detachedPreview(large(index), 2048), value => assert.equal(value.length, 2048));
await sample("structured-head-tail", index => formatFabricValue({ content: `start\n${large(index)}\nend` }, "auto", 2048), value => {
  assert.ok(value.text.length <= 2048);
  assert.ok(value.text.includes("start\n")); assert.ok(value.text.endsWith("\nend"));
  assert.ok(value.text.includes("chars omitted"));
});
await sample("actor-state-excerpts", index => summarizeActorState([{ role: "toolResult", content: `start${large(index)}end` }], 512), value => {
  assert.ok(value.startsWith("toolResult: start")); assert.ok(value.endsWith("end"));
  assert.ok(value.includes("[excerpt omitted]"));
});
await sample("activation-tool-excerpt", index => compactActivationTools({
  messageEntryId: "assistant", toolResultEntryIds: [],
  context: { contextEntries: [{ sourceEntry: { type: "message", id: "old-tool" }, messages: [{
    role: "toolResult", toolCallId: "call", toolName: "read", timestamp: 1, isError: false,
    content: [{ type: "text", text: large(index) }],
  }] }] },
} as unknown as TurnEndEvent)!, value => {
  const message = value.messages[0]!;
  assert.equal(message.role, "toolResult");
  if (message.role !== "toolResult" || message.content[0]?.type !== "text") throw new Error("missing tool excerpt");
  assert.equal(message.content[0].text.length, 1024 + "\n[Compacted tool output; full result in audit journal entry old-tool]".length);
  assert.equal(value.entries.length, 2);
});
await sample("projection-held-key", index => {
  const projection = new PiEventProjection();
  projection.write(`{"text":"${large(index)}","content":[{"type":"image","data":    `);
  return projection;
}, value => {
  assert.equal(value.write('"QUJD"}]}\n'), '"elided":true,"bytes":3}]}\n');
  assert.equal(value.end(), "");
});
await sample("native-reader-error", index => {
  const original = fs.openSync;
  const error = new Error(large(index));
  fs.openSync = (() => { throw error; }) as typeof fs.openSync;
  try {
    const reader = new NativeConversationReader();
    reader.read({ id: "heap", status: "running", eventsFile: "unreadable-events" });
    return reader;
  } finally { fs.openSync = original; }
}, value => {
  assert.equal(value.last?.error?.length, 201);
  assert.ok(value.last?.error?.startsWith("unreadable-events: "));
  assert.ok(value.last?.unavailable?.eventsFile);
});
await sample("participant-trimmed-fields", index => {
  const registry = new ProviderParticipantRegistry();
  const handle = registry.view("probe", "invocation").register({ id: "work", label: "Work", stop: async () => ({ confirmed: true }) });
  handle.update({ phase: `${" ".repeat(SIZE)}phase${" ".repeat(SIZE)}` });
  handle.settle({ status: "completed", summary: `${" ".repeat(SIZE)}summary-${index}${" ".repeat(SIZE)}` });
  return registry;
}, value => {
  assert.equal(value.list()[0]?.phase, "phase");
  assert.ok(value.records("root", "host", "owner")[0]?.currentTool?.startsWith("summary-"));
});
await sample("classifier-reason-and-evidence", async index => {
  const payload = large(index);
  const requests: unknown[] = [];
  const context = {
    cwd: "/probe", model: { provider: "probe", id: "model" },
    sessionManager: { getSessionId: () => "probe", getBranch: () => [
      { type: "message", message: { role: "user", content: payload } },
      { type: "message", message: { role: "assistant", content: [{ type: "toolCall", name: "read", arguments: { payload } }] } },
    ] },
    modelRegistry: {
      getApiKeyAndHeaders: async () => ({ ok: true }),
      streamSimple: (_model: unknown, request: unknown) => {
        requests.push(request);
        return { result: async () => ({ stopReason: "toolUse", usage: {}, content: [
          { type: "toolCall", name: "classify_result", arguments: { decision: "allow", reason: payload } },
        ] }) };
      },
    },
  } as unknown as ExtensionContext;
  const result = await new FabricAutoApprovalClassifier().classify({ ref: "pi.read", risk: "read", description: "Read" } as ResolvedFabricAction, { payload }, context);
  return { result, requests };
}, value => {
  assert.equal(value.result.reason.length, 2000);
  const request = value.requests[0] as { messages: Array<{ content: string }> };
  assert.ok(request.messages[0]!.content.includes("USER: "));
  assert.ok(request.messages[0]!.content.length < 50_000);
// This sample intentionally keeps 100 complete classifier requests as well as
// verdicts: their already-bounded prose alone exceeds the single-preview cap.
}, false, 8 * 1024 * 1024);
await sample("compaction-alarm-fields", async index => {
  const handlers = new Map<string, (event: unknown, context: ExtensionContext) => unknown>();
  let data: Record<string, unknown> | undefined;
  const context = { hasUI: false, isIdle: () => false, sessionManager: { getSessionId: () => "probe" },
    getContextUsage: () => undefined, compact: () => assert.fail("Fabric must not retry native compaction") } as unknown as ExtensionContext;
  registerCompactionRecovery({ on: (name: string, fn: (event: unknown, ctx: ExtensionContext) => unknown) => handlers.set(name, fn) } as unknown as ExtensionAPI,
    { enabled: () => true, alarm: async value => { data = value; },
      retryEnded: () => assert.fail("A failure alarm must not signal recovery") });
  await handlers.get("session_compact_failed")!({ reason: "threshold", errorMessage: large(index) }, context);
  await handlers.get("session_shutdown")!({}, context);
  return { data: data!, expected: String.fromCharCode(97 + index % 26) };
}, value => {
  assert.equal((value.data.error as string).length, 4000);
  assert.equal(value.data.error, value.expected.repeat(4000));
  assert.equal(value.data.fabricAttempts, 0);
  assert.equal(value.data.firstError, undefined);
  assert.equal(value.data.attempts, undefined);
});
console.log(JSON.stringify({ size: SIZE, count: COUNT, limit: LIMIT, controlMin: CONTROL_MIN, samples }));
for (const sample of samples) {
  assert.ok(sample.pass, `${sample.name}: retained ${sample.retainedBytes} bytes for bounded output`);
}
