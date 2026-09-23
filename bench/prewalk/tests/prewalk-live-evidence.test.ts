import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it, vi } from "vitest";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { compareSnapshots, sha256, snapshotTree, type SnapshotEntry } from "../lib/prewalk-bench-lib.mjs";
import {
  aggregateUsage,
  analyzeCell,
  arrivalCompactionBounds,
  assertCompleteRecording,
  assistantTimeline,
  attributePhases,
  createCheckLedger,
  extractPrewalkStatus,
  livePrewalkMessages,
  mergePrewalkMessages,
  parseIndentedStatus,
  parseJsonLines,
  parseRequestContract,
  parseTaskCheckSpec,
  persistedPrewalkMessages,
  requestContractEvidenceProblems,
  requestContractPayloadProblems,
  scanRequestMessages,
  taskCheckReceiptProblems,
  telemetryTimeline,
  toolResultUsages,
  usageMatches,
  type AssistantTimelineItem,
  type PerModelUsage,
  type PrewalkMessage,
  type TelemetryTimeline,
} from "../lib/prewalk-live-evidence.mjs";
import recorder from "../prewalk-canary-telemetry.js";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const tempRoots: string[] = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-live-evidence-"));
  tempRoots.push(root);
  return root;
};
afterAll(() => {
  delete process.env.PREWALK_CANARY_TELEMETRY;
  delete process.env.PREWALK_CANARY_REQUEST_CONTRACT;
  for (const root of tempRoots) fs.rmSync(root, { recursive: true, force: true });
});

const usage = (input: number, output: number, total: number, cost: number) => ({
  input,
  output,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: total,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: cost },
});

const assistantEvent = (model: string, at: number, overrides: Record<string, unknown> = {}) => ({
  type: "message_end",
  message: {
    role: "assistant",
    provider: model.split("/")[0],
    model: model.split("/")[1],
    timestamp: at,
    usage: usage(10, 5, 15, 0.5),
    stopReason: "stop",
    ...overrides,
  },
});

describe("live evidence parsing", () => {
  it("rejects malformed, non-object, and empty recordings with the offending line", () => {
    expect(() => parseJsonLines('{"a":1}\n{oops}', "events")).toThrow(/Malformed events JSONL at line 2/);
    expect(() => parseJsonLines("[1,2]", "events")).toThrow(/expected a JSON object/);
    expect(() => parseJsonLines("\n\n", "events")).toThrow(/recording is empty/);
    expect(parseJsonLines('{"a":1}\n\n{"b":2}')).toEqual([{ a: 1 }, { b: 2 }]);
  });
});

describe("recording completeness", () => {
  const completeEvents = (id = "s1") => [
    { type: "session", version: 3, id, timestamp: "2026-09-19T05:00:00.000Z", cwd: "/tmp" },
    assistantEvent("openai-codex/gpt-6-astra", 1000),
    { type: "agent_settled" },
  ];
  const completeTelemetry = (id = "s1") => [
    { type: "session_start", at: 900, sessionId: id },
    { type: "session_shutdown", at: 5000 },
  ];

  it("accepts a complete matched recording", () => {
    expect(() => assertCompleteRecording(completeEvents(), completeTelemetry())).not.toThrow();
  });
  it("rejects a missing session header or agent_settled", () => {
    expect(() => assertCompleteRecording(completeEvents().slice(1), completeTelemetry())).toThrow(
      /session header or agent_settled missing/,
    );
    expect(
      () => assertCompleteRecording(completeEvents().filter((e) => e.type !== "agent_settled"), completeTelemetry()),
    ).toThrow(/session header or agent_settled missing/);
  });
  it("rejects missing telemetry boundaries and mismatched session identity", () => {
    expect(() => assertCompleteRecording(completeEvents(), [completeTelemetry()[0]!])).toThrow(
      /session_start or session_shutdown missing/,
    );
    expect(() => assertCompleteRecording(completeEvents(), completeTelemetry("s2"))).toThrow(
      /Mismatched live\/telemetry session identity/,
    );
  });
  it("rejects turn-limit runs and failed assistant stop reasons", () => {
    expect(() => assertCompleteRecording(completeEvents(), [...completeTelemetry(), { type: "turn_limit", at: 950 }])).toThrow(
      /turn limit/,
    );
    expect(() =>
      assertCompleteRecording(
        [
          completeEvents()[0]!,
          assistantEvent("openai-codex/gpt-6-astra", 1000, { stopReason: "error" }),
          { type: "agent_settled" },
        ],
        completeTelemetry(),
      )).toThrow(/Assistant did not complete: error/);
  });
});

describe("prewalk message projections", () => {
  it("selects live custom prewalk messages and ignores quoted content and foreign types", () => {
    const events = [
      { type: "message_end", message: { role: "user", content: "go", timestamp: 1 } },
      {
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: 'fake: {"customType":"pi-fabric-prewalk-continue"}' }],
          timestamp: 2,
        },
      },
      { type: "message_end", message: { role: "custom", customType: "other-extension", content: "x", display: true, timestamp: 3 } },
      {
        type: "message_end",
        message: {
          role: "custom",
          customType: "pi-fabric-prewalk-armed",
          content: "armed",
          display: false,
          details: { mode: "in-place", model: "zro/glm-5.3" },
          timestamp: 1500,
        },
      },
      {
        type: "message_end",
        message: {
          role: "custom",
          customType: "pi-fabric-prewalk-continue",
          content: "continue",
          display: false,
          details: { continuationId: "c-1", trigger: { ref: "pi.edit", seq: 2 } },
          timestamp: 2500,
        },
      },
    ];
    const messages = livePrewalkMessages(events);
    expect(messages.map((message) => message.customType)).toEqual([
      "pi-fabric-prewalk-armed",
      "pi-fabric-prewalk-continue",
    ]);
    expect(messages[1]).toMatchObject({ source: "live", at: 2500 });
  });

  it("selects persisted custom_message entries, not message-shaped entries, and parses ISO timestamps", () => {
    const entries = [
      {
        type: "message",
        id: "m1",
        parentId: null,
        timestamp: "2026-09-19T05:00:00.000Z",
        message: { role: "custom", customType: "pi-fabric-prewalk-armed", content: "decoy" },
      },
      {
        type: "custom_message",
        id: "a1",
        parentId: "m1",
        timestamp: "2026-09-19T05:00:01.000Z",
        customType: "pi-fabric-prewalk-armed",
        content: "armed",
        display: false,
        details: { mode: "in-place", model: "zro/glm-5.3" },
      },
      {
        type: "custom_message",
        id: "c1",
        parentId: "a1",
        timestamp: "not-a-date",
        customType: "pi-fabric-prewalk-continue",
        content: "continue",
        display: false,
        details: { continuationId: "c-1" },
      },
    ];
    const messages = persistedPrewalkMessages(entries);
    expect(messages.map((message) => message.customType)).toEqual([
      "pi-fabric-prewalk-armed",
      "pi-fabric-prewalk-continue",
    ]);
    expect(messages[0]).toMatchObject({ source: "session", id: "a1" });
    expect(messages[0]?.at).toBe(Date.parse("2026-09-19T05:00:01.000Z"));
    expect(messages[1]?.at).toBeNull();
  });

  it("preserves repeated live event cardinality and pairs persisted entries FIFO without collapsing", () => {
    const armed = (): PrewalkMessage => ({
      source: "live",
      customType: "pi-fabric-prewalk-armed",
      details: { mode: "in-place", model: "zro/glm-5.3" },
      at: 1500,
    });
    const live: PrewalkMessage[] = [
      armed(),
      armed(),
      {
        source: "live",
        customType: "pi-fabric-prewalk-continue",
        details: { continuationId: "c-1", trigger: { ref: "pi.edit", seq: 2 } },
        at: 2500,
      },
    ];
    const persisted: PrewalkMessage[] = [
      {
        source: "session",
        customType: "pi-fabric-prewalk-armed",
        details: { mode: "in-place", model: "zro/glm-5.3" },
        at: 1501,
        id: "p1",
      },
      {
        source: "session",
        customType: "pi-fabric-prewalk-continue",
        details: { continuationId: "c-2" },
        at: 3500,
        id: "p2",
      },
    ];
    const merged = mergePrewalkMessages(live, persisted);
    expect(merged).toHaveLength(4);
    expect(merged[0]?.sources).toEqual(["live", "session"]);
    expect(merged[0]?.sessionEntryId).toBe("p1");
    expect(merged[1]?.sources).toEqual(["live"]);
    expect(merged[2]?.sources).toEqual(["live"]);
    expect(merged[3]?.sources).toEqual(["session"]);
    expect(merged[3]?.sessionEntryId).toBe("p2");
  });

  it("round-trips persisted custom_message entries through the real SessionManager", () => {
    const session = SessionManager.inMemory();
    session.appendMessage({ role: "user", content: "go", timestamp: 1 });
    session.appendCustomMessageEntry("pi-fabric-prewalk-armed", "armed", false, {
      mode: "in-place",
      model: "zro/glm-5.3",
    });
    session.appendCustomMessageEntry("other-extension", "ignored", false);
    session.appendCustomMessageEntry("pi-fabric-prewalk-continue", "continue", false, {
      continuationId: "c-1",
      trigger: { ref: "pi.edit", seq: 2 },
    });
    const persisted = persistedPrewalkMessages(session.getEntries());
    expect(persisted.map((message) => message.customType)).toEqual([
      "pi-fabric-prewalk-armed",
      "pi-fabric-prewalk-continue",
    ]);
    expect(persisted[0]?.at).not.toBeNull();
    expect(persisted[0]?.id).toMatch(/^[0-9a-f]{8}$/);
    const live: PrewalkMessage[] = [
      {
        source: "live",
        customType: "pi-fabric-prewalk-armed",
        details: { mode: "in-place", model: "zro/glm-5.3" },
        at: 1500,
      },
    ];
    const merged = mergePrewalkMessages(live, persisted);
    expect(merged).toHaveLength(2);
    expect(merged[0]?.sources).toEqual(["live", "session"]);
    expect(merged[0]?.sessionEntryId).toBe(persisted[0]?.id);
  });
});

describe("usage aggregation and matching", () => {
  it("aggregates per-model usage and skips failed attempts without usage", () => {
    const timeline: AssistantTimelineItem[] = [
      { at: 1000, model: "openai-codex/gpt-6-astra", usage: usage(100, 10, 110, 0.25), stopReason: "stop" },
      { at: 2000, model: "openai-codex/gpt-6-astra", usage: usage(50, 5, 55, 0.1), stopReason: "stop" },
      { at: 3000, model: "zro/glm-5.3", usage: usage(20, 2, 22, 0.01), stopReason: "stop" },
      { at: 4000, model: "zro/glm-5.3", usage: null, stopReason: "error" },
    ];
    const perModel = aggregateUsage(timeline, [usage(1, 1, 2, 0.001)]);
    expect(perModel["openai-codex/gpt-6-astra"]).toEqual({
      requests: 2,
      input: 150,
      output: 15,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 165,
      recordedCostEstimateUsd: 0.35,
    });
    expect(perModel["zro/glm-5.3"]?.requests).toBe(1);
    expect(perModel["nested-tool-usage"]?.requests).toBe(1);
  });

  it("rejects invalid usage instead of turning missing data into zero cost", () => {
    const bad = (overrides: Record<string, unknown>) => [
      { at: 1, model: "a/b", usage: { ...usage(1, 1, 1, 0), ...overrides }, stopReason: "stop" },
    ];
    expect(() => aggregateUsage(bad({ input: Number.NaN }))).toThrow(/invalid recorded usage/);
    expect(() => aggregateUsage(bad({ totalTokens: undefined }))).toThrow(/invalid recorded usage/);
    expect(() => aggregateUsage(bad({ output: -1 }))).toThrow(/invalid recorded usage/);
    expect(() => aggregateUsage([{ at: 1, model: "a/b", usage: null, stopReason: "stop" }])).toThrow(
      /invalid recorded usage/,
    );
  });

  it("matches recorded usage exactly except cost tolerance and flags every mismatch", () => {
    const perModel = aggregateUsage(
      [{ at: 1, model: "a/b", usage: usage(10, 5, 15, 0.5), stopReason: "stop" }],
      [],
    );
    expect(usageMatches(perModel, perModel).ok).toBe(true);
    const withCostDrift: Record<string, PerModelUsage> = {
      "a/b": { ...perModel["a/b"]!, recordedCostEstimateUsd: 0.5 + 1e-10 },
    };
    expect(usageMatches(perModel, withCostDrift).ok).toBe(true);
    const withTokenDrift: Record<string, PerModelUsage> = {
      "a/b": { ...perModel["a/b"]!, totalTokens: 999 },
    };
    const mismatch = usageMatches(perModel, withTokenDrift);
    expect(mismatch.ok).toBe(false);
    expect(JSON.stringify(mismatch.mismatches)).toContain("totalTokens");
    expect(usageMatches(perModel, { ...perModel, "c/d": perModel["a/b"]! }).ok).toBe(false);
    expect(() => usageMatches(perModel, perModel, Number.NaN)).toThrow(/Invalid usage tolerance/);
  });

  it("collects nested tool-result usage separately from assistant usage", () => {
    const events = [
      assistantEvent("openai-codex/gpt-6-astra", 1000),
      {
        type: "message_end",
        message: { role: "toolResult", toolCallId: "t1", toolName: "fabric_exec", usage: usage(3, 3, 6, 0.02), isError: false },
      },
    ];
    expect(toolResultUsages(events)).toHaveLength(1);
    expect(assistantTimeline(events)).toHaveLength(1);
  });
});

describe("phase attribution", () => {
  const timelineFor = (): AssistantTimelineItem[] => [
    { at: 1000, model: "main/frontier", usage: null, stopReason: "stop" },
    { at: 2000, model: "main/frontier", usage: null, stopReason: "stop" },
    { at: 2600, model: "exec/model", usage: null, stopReason: "stop" },
    { at: 4000, model: "exec/model", usage: null, stopReason: "stop" },
  ];
  const telemetryFor = (): TelemetryTimeline => ({
    modelSelects: [
      { at: 2500, model: "exec/model", previous: "main/frontier" },
      { at: 5000, model: "main/frontier", previous: "exec/model" },
    ],
    compaction: [],
    boundaries: { session_shutdown: 5500 },
  });

  it("attributes pre-handoff, executor, and return spans from one roundtrip", () => {
    const phases = attributePhases({ timeline: timelineFor(), telemetry: telemetryFor() });
    expect(phases).toMatchObject({
      handoffAt: 2500,
      returnAt: 5000,
      assistantSpanMs: 3000,
      preHandoffMainMs: 1500,
      executorIntervalMs: 2500,
      executorAssistantSpanMs: 1400,
      returnMs: 500,
    });
    expect(phases.missing).toEqual([]);
    expect(Object.values(phases.provenance).every((origin) => typeof origin === "string" && origin.length > 0)).toBe(
      true,
    );
  });

  it("keeps null boundaries with reasons when no model change is recorded (Prewalk OFF)", () => {
    const phases = attributePhases({
      timeline: timelineFor().map((item) => ({ ...item, model: "main/frontier" })),
      telemetry: { modelSelects: [], compaction: [], boundaries: {} },
    });
    expect(phases.handoffAt).toBeNull();
    expect(phases.preHandoffMainMs).toBeNull();
    expect(phases.assistantSpanMs).toBe(3000);
    expect(phases.missing).toContain("no model_select recorded (Prewalk OFF or recorder gap)");
  });

  it("rejects anything that is not exactly one Main/executor roundtrip", () => {
    const extra = attributePhases({
      timeline: timelineFor(),
      telemetry: {
        ...telemetryFor(),
        modelSelects: [
          ...telemetryFor().modelSelects,
          { at: 6000, model: "exec/model", previous: "main/frontier" },
        ],
      },
    });
    expect(extra.handoffAt).toBeNull();
    expect(extra.missing).toContain("model selections are not one Main/executor roundtrip");
    const wrongPrevious = attributePhases({
      timeline: timelineFor(),
      telemetry: {
        ...telemetryFor(),
        modelSelects: [{ at: 2500, model: "exec/model", previous: "other/model" }, telemetryFor().modelSelects[1]!],
      },
    });
    expect(wrongPrevious.missing).toContain("model selections are not one Main/executor roundtrip");
    const wrongReturn = attributePhases({
      timeline: timelineFor(),
      telemetry: {
        ...telemetryFor(),
        modelSelects: [telemetryFor().modelSelects[0]!, { at: 5000, model: "third/model", previous: "exec/model" }],
      },
    });
    expect(wrongReturn.missing).toContain("model selections are not one Main/executor roundtrip");
  });

  it("keeps a single handoff unreturned null and unordered timestamps null", () => {
    const unreturned = attributePhases({
      timeline: timelineFor(),
      telemetry: { ...telemetryFor(), modelSelects: [telemetryFor().modelSelects[0]!] },
    });
    expect(unreturned.handoffAt).toBe(2500);
    expect(unreturned.preHandoffMainMs).toBe(1500);
    expect(unreturned.executorIntervalMs).toBeNull();
    expect(unreturned.returnMs).toBeNull();
    expect(unreturned.missing).toContain("return model_select missing");
    const unordered = attributePhases({
      timeline: timelineFor(),
      telemetry: {
        ...telemetryFor(),
        modelSelects: [
          { at: 500, model: "exec/model", previous: "main/frontier" },
          { at: 5000, model: "main/frontier", previous: "exec/model" },
        ],
      },
    });
    expect(unordered.preHandoffMainMs).toBeNull();
    expect(unordered.missing).toContain("first assistant/handoff timestamp missing or unordered");
  });

  it("bounds executor spans to the model interval and compaction duration to arrival pairs", () => {
    const outside = attributePhases({
      timeline: [
        { at: 1000, model: "main/frontier", usage: null, stopReason: "stop" },
        { at: 1500, model: "main/frontier", usage: null, stopReason: "stop" },
        { at: 2000, model: "exec/model", usage: null, stopReason: "stop" },
        { at: 2600, model: "exec/model", usage: null, stopReason: "stop" },
      ],
      telemetry: telemetryFor(),
    });
    expect(outside.executorAssistantSpanMs).toBeNull();
    expect(outside.missing).toContain("fewer than two executor message starts inside model interval");
    const compactionTelemetry: TelemetryTimeline = {
      ...telemetryFor(),
      compaction: [{ type: "compaction_failed", at: 5100, reason: "manual", error: "too small" }],
    };
    const withoutArrival = attributePhases({ timeline: timelineFor(), telemetry: compactionTelemetry });
    expect(withoutArrival.compaction).toMatchObject({ outcome: "compaction_failed", attemptMs: null });
    expect(withoutArrival.missing).toContain("compaction attempt start/end arrival pair missing");
    const withArrival = attributePhases({
      timeline: timelineFor(),
      telemetry: compactionTelemetry,
      compactionArrival: { startAt: 5050, endAt: 5100 },
    });
    expect(withArrival.compaction?.attemptMs).toBe(50);
  });
});

describe("arrival compaction bounds", () => {
  const streamEvents = [{ type: "session" }, { type: "compaction_start" }, { type: "compaction_end" }];
  const arrivalsFor = (ms: number[]) => ms.map((arrivalMs, index) => ({ lineIndex: index, arrivalMs }));

  it("pairs the last attempt on arrival-clock timestamps only", () => {
    expect(arrivalCompactionBounds(streamEvents, arrivalsFor([100, 150, 200]))).toEqual({ startAt: 150, endAt: 200 });
    expect(
      arrivalCompactionBounds(
        [{ type: "compaction_start" }, { type: "compaction_end" }, { type: "compaction_start" }, { type: "compaction_end" }],
        arrivalsFor([100, 150, 200, 250]),
      ),
    ).toEqual({ startAt: 200, endAt: 250 });
    expect(arrivalCompactionBounds([{ type: "session" }], arrivalsFor([100]))).toBeNull();
    expect(arrivalCompactionBounds(streamEvents, null)).toBeNull();
    expect(arrivalCompactionBounds(streamEvents, undefined)).toBeNull();
  });

  it("rejects misaligned, non-monotonic, or structurally invalid arrival logs", () => {
    expect(() => arrivalCompactionBounds(streamEvents, arrivalsFor([100, 150]).slice(0, 1))).toThrow(/line count differs/);
    expect(() =>
      arrivalCompactionBounds(streamEvents, arrivalsFor([100, 150, 200]).map((a, i) => (i === 1 ? { ...a, lineIndex: 5 } : a)))).toThrow(
      /alignment\/clock/,
    );
    expect(() => arrivalCompactionBounds(streamEvents, arrivalsFor([100, 50, 150]))).toThrow(/alignment\/clock/);
    expect(() => arrivalCompactionBounds([{ type: "compaction_end" }], arrivalsFor([100]))).toThrow(/without start/);
    expect(() =>
      arrivalCompactionBounds([{ type: "compaction_start" }, { type: "compaction_start" }], arrivalsFor([100, 150]))).toThrow(
      /Overlapping/,
    );
  });
});

describe("canonical tree snapshots", () => {
  const digest = (character: string) => character.repeat(64);

  it("compares empty snapshots and sorts every changed path independently of insertion order", () => {
    expect(compareSnapshots({}, {})).toEqual({ same: true, changed: [], added: [], removed: [] });
    const before = { zr: digest("1"), ar: digest("2"), zc: digest("3"), ac: digest("4"), kept: digest("5") };
    const after = { kept: { sha256: digest("5") }, ac: digest("6"), zc: digest("7"), za: digest("8"), aa: digest("9") };
    const expected = { same: false, changed: ["ac", "zc"], added: ["aa", "za"], removed: ["ar", "zr"] };
    expect(compareSnapshots(before, after)).toEqual(expected);
    const reverse = (value: object) => Object.fromEntries(Object.entries(value).reverse());
    expect(compareSnapshots(reverse(before), reverse(after))).toEqual(expected);
  });

  it("distinguishes equal links, different links and file/link transitions with identical strings", () => {
    const value = digest("b");
    expect(compareSnapshots({ p: { link: value } }, { p: { link: value } }).same).toBe(true);
    expect(compareSnapshots({ p: { link: "a" } }, { p: { link: "b" } }).changed).toEqual(["p"]);
    const transitions: Array<[string | SnapshotEntry, string | SnapshotEntry]> = [
      [value, { link: value }],
      [{ sha256: value }, { link: value }],
      [{ link: value }, value],
      [{ link: value }, { sha256: value }],
    ];
    for (const [before, after] of transitions) {
      expect(compareSnapshots({ p: before }, { p: after })).toEqual({ same: false, changed: ["p"], added: [], removed: [] });
    }
  });

  it("rejects invalid shapes and malformed bare/object digests on either side of a shared path", () => {
    const valid = digest("c");
    const invalid: unknown[] = [[], ["nested"], 7, null, {}, { mode: "x" }, { sha256: valid, link: "target" }, { sha256: 42 }, { link: 42 }];
    const malformed = ["a".repeat(63), "a".repeat(65), "A".repeat(64), "z".repeat(64), "ab".repeat(31) + "!", ""];
    for (const value of [...invalid, ...malformed.flatMap((bad) => [bad, { sha256: bad }])]) {
      for (const [before, after] of [[value, valid], [valid, value]]) {
        expect(() => compareSnapshots({ shared: before as string }, { shared: after as string })).toThrow(/shared/);
      }
    }
  });
  it("treats a bare digest and { sha256 } as the same file and reports real changes", () => {
    const digest = "a".repeat(64);
    expect(compareSnapshots({ "a.ts": { sha256: digest } }, { "a.ts": digest })).toEqual({
      same: true,
      changed: [],
      added: [],
      removed: [],
    });
    const result = compareSnapshots(
      { "a.ts": { sha256: digest }, "gone.ts": { sha256: "b".repeat(64) }, link: { link: "/tmp/x" } },
      { "a.ts": { sha256: "c".repeat(64) }, "new.ts": { sha256: "d".repeat(64) }, link: { link: "/tmp/y" } },
    );
    expect(result).toEqual({ same: false, changed: ["a.ts", "link"], added: ["new.ts"], removed: ["gone.ts"] });
  });

  it("rejects malformed snapshot entries instead of comparing them loosely", () => {
    expect(() => compareSnapshots({ a: { sha256: "nope" } }, { a: "a".repeat(64) })).toThrow(/Invalid snapshot digest/);
    expect(() => compareSnapshots({ a: 5 as unknown as string }, { a: "a".repeat(64) })).toThrow(/Unsupported snapshot entry/);
  });

  it("skips only the requested directory, not plus-prefixed or longer sibling names", () => {
    const root = tempRoot();
    for (const relative of ["generated/drop.txt", "+ generated/keep.txt", "generated-extra/keep.txt"]) {
      const file = path.join(root, relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, relative);
    }
    expect(Object.keys(snapshotTree(root, { skip: ["generated"] })).sort()).toEqual([
      "+ generated/keep.txt", "generated-extra/keep.txt",
    ]);
  });

  it.skipIf(process.platform === "win32")("walks a tree with symlinks relative to another root", () => {
    const parent = tempRoot();
    const root = path.join(parent, "tree");
    fs.mkdirSync(path.join(root, "nested"), { recursive: true });
    fs.writeFileSync(path.join(root, "one.txt"), "one");
    fs.writeFileSync(path.join(root, "nested", "two.txt"), "two");
    fs.symlinkSync("one.txt", path.join(root, "nested", "link.txt"));
    const snapshot = snapshotTree(root, { relativeTo: parent });
    expect(Object.keys(snapshot).sort()).toEqual(["tree/nested/link.txt", "tree/nested/two.txt", "tree/one.txt"]);
    expect(snapshot["tree/nested/link.txt"]).toEqual({ link: "one.txt" });
    expect(snapshot["tree/one.txt"]).toEqual({ sha256: sha256(Buffer.from("one")) });
  });
});

describe("required check ledger", () => {
  it("does not let an unobserved required check pass", () => {
    const ledger = createCheckLedger();
    ledger.add("observed", "pass", { at: 1 });
    expect(ledger.ok()).toBe(true);
    ledger.add("missing", "unobserved");
    expect(ledger.toJSON().ok).toBe(false);
  });

  it("rejects duplicate names and unknown statuses, and records failures", () => {
    const ledger = createCheckLedger();
    ledger.add("one", "fail", "why");
    expect(ledger.ok()).toBe(false);
    expect(() => ledger.add("one", "pass")).toThrow(/Duplicate check/);
    expect(() => ledger.add("two", "maybe" as "pass")).toThrow(/Invalid check status/);
  });
});

describe("persisted session ingestion strictness", () => {
  it("selects raw custom_message entries and never a projection that dropped the entry type", () => {
    const raw: Array<Record<string, unknown>> = [
      {
        type: "custom_message",
        id: "a1",
        customType: "pi-fabric-prewalk-armed",
        content: "x",
        display: false,
        details: { mode: "in-place", model: "m" },
        timestamp: "2026-09-20T00:00:00.000Z",
      },
      {
        type: "custom_message",
        id: "b2",
        customType: "pi-fabric-prewalk-continue",
        content: "y",
        display: false,
        details: { continuationId: "c" },
        timestamp: "2026-09-20T00:00:01.000Z",
      },
      { type: "message", id: "c3", message: { role: "custom", customType: "other-extension", content: "decoy" } },
    ];
    const selected = persistedPrewalkMessages(raw);
    expect(selected.map((message) => message.customType)).toEqual([
      "pi-fabric-prewalk-armed",
      "pi-fabric-prewalk-continue",
    ]);
    expect(selected.map((message) => message.id)).toEqual(["a1", "b2"]);
    // The scratch verifier stripped `type` from its projection; the strict
    // selector must treat that as no evidence, not as a matching message.
    const stripped = raw.map(({ type: _type, ...rest }) => rest);
    expect(persistedPrewalkMessages(stripped)).toEqual([]);
  });
});

describe("recording completeness options", () => {
  const sessionStart = { type: "session_start", sessionId: "s-1", at: 1 };
  const sessionEnd = { type: "session_shutdown", at: 2 };

  it("accepts an aborted assistant only when explicitly allowed", () => {
    const events = [
      { type: "session", id: "s-1" },
      { type: "message_end", message: { role: "assistant", stopReason: "aborted" } },
      { type: "agent_settled" },
    ];
    expect(() => assertCompleteRecording(events, [sessionStart, sessionEnd])).toThrow(/Assistant did not complete: aborted/);
    expect(() => assertCompleteRecording(events, [sessionStart, sessionEnd], { allowAborted: true })).not.toThrow();
  });

  it("supports rpc recordings without a synthetic session header", () => {
    const events = [{ type: "agent_settled" }];
    expect(() => assertCompleteRecording(events, [sessionStart, sessionEnd])).toThrow(/session header or agent_settled missing/);
    expect(() =>
      assertCompleteRecording(events, [sessionStart, sessionEnd], { requireSessionHeader: false, sessionId: "s-1" }),
    ).not.toThrow();
    expect(() =>
      assertCompleteRecording(events, [sessionStart, sessionEnd], { requireSessionHeader: false, sessionId: "other" }),
    ).toThrow(/Mismatched live\/telemetry session identity/);
  });
});

describe("prewalk status parsing", () => {
  it("parses the indented status text structurally", () => {
    const text = [
      "state: armed",
      "planRequired: true",
      "runtime:",
      "  entry:",
      "    path: /repo/dist/index.js",
      `    loadedSha256: ${'a'.repeat(64)}`,
      `    diskSha256: ${'a'.repeat(64)}`,
      "    stale: false",
      "  lazyRuntime:",
      "    path: /repo/dist/fabric-runtime-state.js",
      `    loadedSha256: ${'b'.repeat(64)}`,
      `    diskSha256: ${'c'.repeat(64)}`,
      "    stale: true",
    ].join("\n");
    const parsed = parseIndentedStatus(text) as {
      state: string;
      runtime: { entry: { loadedSha256: string }; lazyRuntime: { stale: string } };
    };
    expect(parsed.state).toBe("armed");
    expect(parsed.runtime.entry.loadedSha256).toBe("a".repeat(64));
    expect(parsed.runtime.lazyRuntime.stale).toBe("true");
  });

  it("rejects malformed status text", () => {
    expect(() => parseIndentedStatus("state: armed\nnot a status line")).toThrow(/Malformed status line/);
    expect(() => parseIndentedStatus(5 as unknown as string)).toThrow(/must be a string/);
  });

  const statusText = (dist = "/repo/dist") => [
    "state: armed",
    "planRequired: false",
    "runtime:",
    "  entry:",
    `    path: ${dist}/index.js`,
    `    loadedSha256: ${"a".repeat(64)}`,
    `    diskSha256: ${"a".repeat(64)}`,
    "    stale: false",
    "  lazyRuntime:",
    `    path: ${dist}/fabric-runtime-state.js`,
    `    loadedSha256: ${"b".repeat(64)}`,
    `    diskSha256: ${"b".repeat(64)}`,
    "    stale: false",
  ].join("\n");

  it("recognizes bare, JSON-enveloped and batch-nested status observations", () => {
    expect(extractPrewalkStatus(statusText())?.state).toBe("armed");
    expect(extractPrewalkStatus(statusText(undefined))?.runtime).toBeDefined();
    const json = JSON.stringify({ status: { state: "armed", runtime: { entry: { path: "/repo/dist/index.js" } } } });
    expect(extractPrewalkStatus(json)?.state).toBe("armed");
    // The real child envelope: a descriptions list, the status block, then an
    // appendix that repeats unrelated text.
    const batched = [
      "descriptions:",
      "  - name: plan",
      "    inputSchema:",
      "      required:",
      "        - outcome",
      'helper: "<multi-line string, see section: helper>"',
      "status:",
      ...statusText().split("\n").map((line) => `  ${line}`),
      "",
      "--- helper (100 chars) ---",
      "not a status line",
      statusText("/quoted/not-runtime"),
    ].join("\n");
    expect(extractPrewalkStatus(batched)?.state).toBe("armed");
  });

  it("never treats quoted, descriptive or malformed text as an observation", () => {
    const quoted = ["helper: |", ...statusText().split("\n").map((line) => `  ${line}`)].join("\n");
    expect(extractPrewalkStatus(quoted)).toBeNull();
    const descriptive = ["descriptions:", "  runtime:", "    entry: /dist/index.js", "", "<plain status text>"].join("\n");
    expect(extractPrewalkStatus(descriptive)).toBeNull();
    const malformed = ["status:", ...statusText().split("\n").map((line) => `  ${line}`), "  malformed line"].join("\n");
    expect(extractPrewalkStatus(malformed)).toBeNull();
    expect(extractPrewalkStatus("state: armed\nplanRequired: false")).toBeNull();
    expect(extractPrewalkStatus(5 as unknown as string)).toBeNull();
  });
});

describe("check axes", () => {
  it("keeps lifecycle, scope and artifact verdicts separate", () => {
    const ledger = createCheckLedger();
    ledger.add("lifecycle-ok", "pass");
    ledger.add("task-verification", "fail", "tests failed: 4 of 7", "artifact");
    ledger.add("scope-no-writes-after-marker", "pass", null, "scope");
    expect(ledger.axes()).toEqual({
      lifecycle: [{ name: "lifecycle-ok", status: "pass" }],
      artifact: [{ name: "task-verification", status: "fail" }],
      scope: [{ name: "scope-no-writes-after-marker", status: "pass" }],
    });
    expect(ledger.ok()).toBe(false);
    expect(ledger.toJSON().axes.artifact).toEqual([{ name: "task-verification", status: "fail" }]);
    expect(() => ledger.add("bad-axis", "pass", null, "")).toThrow(/Invalid check axis/);
  });
});

describe("task-check contract", () => {
  it("accepts only a relative Node test module inside the work directory", () => {
    expect(parseTaskCheckSpec('{"testFile":"tests/a.test.mjs"}')).toEqual({
      testFile: "tests/a.test.mjs",
      artifact: "tests/a.test.mjs",
    });
    expect(parseTaskCheckSpec('{"testFile":"tests/a.test.mjs","artifact":"src/a.ts"}').artifact).toBe("src/a.ts");
    for (const spec of [
      '{"testFile":"/etc/passwd"}',
      '{"testFile":"../a.test.mjs"}',
      '{"testFile":"tests/../../a.test.mjs"}',
      '{"testFile":"a.txt"}',
      '{"testFile":"tests/a.test.mjs","command":"rm -rf /"}',
      "not json",
    ]) {
      expect(() => parseTaskCheckSpec(spec)).toThrow();
    }
  });

  it("treats a failing suite as an artifact failure and re-binds the verified content", () => {
    const sha = "a".repeat(64);
    const receipt = {
      exitCode: 1,
      counts: { tests: 7, pass: 3, fail: 4 },
      ok: false,
      artifact: { path: "tests/a.test.mjs", sha256After: sha, unchangedDuringCheck: true },
    };
    expect(taskCheckReceiptProblems(receipt, { artifactSha256: sha }).join("\n")).toContain("tests failed: 4 of 7");
    expect(taskCheckReceiptProblems(receipt, { artifactSha256: "b".repeat(64) }).join("\n")).toContain(
      "artifact content changed after the check ran",
    );
    expect(taskCheckReceiptProblems(receipt, { artifactSha256: null }).join("\n")).toContain(
      "artifact missing at re-verification",
    );
    const passing = { ...receipt, exitCode: 0, ok: true, counts: { tests: 7, pass: 7, fail: 0 } };
    expect(taskCheckReceiptProblems(passing, { artifactSha256: sha })).toEqual([]);
    expect(taskCheckReceiptProblems({ ...passing, counts: { tests: 0, pass: 0, fail: 0 } }, {})).toContain(
      "no tests were executed",
    );
    expect(
      taskCheckReceiptProblems({ ...passing, artifact: { ...passing.artifact, unchangedDuringCheck: false } }, {}),
    ).toContain("artifact changed while its check ran");
    expect(taskCheckReceiptProblems("nope", {})).toEqual(["receipt is not an object"]);
  });
});

describe("request contract helpers", () => {
  const contract = {
    markers: { task: "TASK", plan: "PLAN", continuation: "CONT", steer1: "S1", steer2: "S2" },
    exactlyOnce: ["plan", "continuation"],
    present: ["task"],
    ordered: ["steer1", "steer2"],
    absentBefore: ["continuation"],
  };
  const record = (requestIndex: number, model: string, sha: string, matches: Record<string, number[][]>) =>
    ({ type: "request_context", requestIndex, model,
      requestEvidence: { layout: "context", truncated: false, matches, contractSha256: sha } });

  it("parses a contract and rejects unknown fields, markers, and overlaps", () => {
    const text = JSON.stringify({ markers: { task: "T", plan: "P" }, exactlyOnce: ["plan"], present: ["task"] });
    expect(parseRequestContract(text, "test")).toMatchObject({ markers: { task: "T", plan: "P" }, exactlyOnce: ["plan"], present: ["task"] });
    expect(() => parseRequestContract('{"markers":{"task":"T"},"extra":1}', "test")).toThrow(/unknown field/);
    expect(() => parseRequestContract('{"markers":{"bad name":"T"}}', "test")).toThrow(/invalid marker name/);
    expect(() => parseRequestContract('{"markers":{"task":"T"},"ordered":["missing"]}', "test")).toThrow(/undeclared marker/);
    expect(() => parseRequestContract('{"markers":{"task":"T"},"exactlyOnce":["task"],"present":["task"]}', "test")).toThrow(/both exactlyOnce and present/);
  });

  it("scans user and custom text parts without returning message text", () => {
    const messages = [
      { role: "system", content: "TASK" },
      { role: "assistant", content: "PLAN" },
      { role: "user", content: "say TASK now" },
      { role: "custom", content: [{ type: "text", text: "x TASK" }, { type: "image", data: "TASK" }] },
    ];
    const scan = scanRequestMessages(messages, { task: "TASK" });
    expect(scan).toEqual({ matches: { task: [[2, 0, 4], [3, 0, 2]] }, truncated: false });
    expect(JSON.stringify(scan)).not.toContain("say ");
    expect(scanRequestMessages(messages, { task: "TASK" }, { maxMatches: 1 }).truncated).toBe(true);
    expect(scanRequestMessages([{ role: "user", content: "TASK" }], { task: "TASK" }, { maxChars: 3 }).truncated).toBe(true);
  });

  it("reports evidence and payload problems", () => {
    const good = [
      record(1, "a/m", "sha", { task: [[0, 0, 0]], plan: [], continuation: [], steer1: [], steer2: [] }),
      record(2, "a/e", "sha", { task: [[1, 0, 0]], plan: [[1, 0, 1]], continuation: [[1, 0, 2]], steer1: [[2, 0, 0]], steer2: [] }),
      record(3, "a/e", "sha", { task: [[1, 0, 0]], plan: [[1, 0, 1]], continuation: [[1, 0, 2]], steer1: [[2, 0, 0]], steer2: [[3, 0, 0]] }),
    ];
    expect(requestContractEvidenceProblems(good, contract, "sha")).toEqual([]);
    expect(requestContractPayloadProblems(good, contract, "a/e")).toEqual([]);
    expect(requestContractEvidenceProblems(good, contract, "other")).toEqual([
      "request 1: contract sha256 mismatch",
      "request 2: contract sha256 mismatch",
      "request 3: contract sha256 mismatch",
    ]);
    const duplicated = [good[0]!, record(2, "a/e", "sha", { task: [[1, 0, 0]], plan: [[1, 0, 1], [1, 0, 2]], continuation: [[1, 0, 2]], steer1: [[2, 0, 0]], steer2: [] })];
    expect(requestContractPayloadProblems(duplicated, contract, "a/e")).toContain('request 2: "plan" appears 2 times (exactly once required)');
    const leaked = [record(1, "a/m", "sha", { task: [[0, 0, 0]], plan: [], continuation: [[0, 0, 1]], steer1: [], steer2: [] }), good[1]!];
    expect(requestContractPayloadProblems(leaked, contract, "a/e")).toContain('request 1: "continuation" present before the executor switch');
    const unordered = [good[0]!, record(2, "a/e", "sha", { task: [[1, 0, 0]], plan: [[1, 0, 1]], continuation: [[1, 0, 2]], steer1: [], steer2: [[2, 0, 0]] }), record(3, "a/e", "sha", { task: [[1, 0, 0]], plan: [[1, 0, 1]], continuation: [[1, 0, 2]], steer1: [[3, 0, 0]], steer2: [] })];
    expect(requestContractPayloadProblems(unordered, contract, "a/e")).toContain('ordered marker "steer2" appears before the previous ordered marker');
  });

  it("rejects malformed marker positions instead of trusting or crashing on them", () => {
    const malformed = [
      record(1, "a/m", "sha", { task: [[0, 0]], plan: [], continuation: [], steer1: [], steer2: [] }),
      record(2, "a/e", "sha", { task: [[1, -1, 0]], plan: [[1, 0, 1]], continuation: [[1, 0, 2]], steer1: [[2, 0, 0]], steer2: [] }),
      record(3, "a/e", "sha", { task: [[1, 0, 0]], plan: [[1, 0, 1]], continuation: [[1, 0, 2]], steer1: [[2, 0, 0]], steer2: [[3, 0, 0], [2, 0, 0]] }),
    ];
    expect(requestContractEvidenceProblems(malformed, contract, "sha")).toEqual([
      "request 1: marker task positions malformed",
      "request 2: marker task positions malformed",
      "request 3: marker steer2 positions malformed",
    ]);
    expect(requestContractPayloadProblems(malformed, contract, "a/e")).toEqual([
      'request 1: marker "task" positions malformed or unrecorded',
      'request 2: marker "task" positions malformed or unrecorded',
      'request 3: marker "steer2" positions malformed or unrecorded',
    ]);
  });
});

describe("canary telemetry recorder", () => {
  const loadRecorder = () => {
    const handlers = new Map<string, Array<(event: unknown, context: unknown) => unknown>>();
    const pi = {
      on: (type: string, handler: (event: unknown, context: unknown) => unknown) => {
        const list = handlers.get(type) ?? [];
        list.push(handler);
        handlers.set(type, list);
      },
      getActiveTools: () => ["read"],
    } as unknown as ExtensionAPI;
    const telemetryPath = path.join(tempRoot(), "telemetry.jsonl");
    process.env.PREWALK_CANARY_TELEMETRY = telemetryPath;
    recorder(pi);
    const fire = (type: string, event: unknown, context: unknown) => {
      for (const handler of handlers.get(type) ?? []) {
        expect(handler(event, context)).toBeUndefined();
      }
    };
    return { fire, telemetryPath };
  };

  const contextStub = (entries: unknown[] = []) =>
    ({
      sessionManager: {
        getSessionId: () => "session-1",
        getEntries: () => entries,
      },
      cwd: "/tmp",
      model: { provider: "openai-codex", id: "gpt-6-astra" },
      thinkingLevel: "high",
      abort: vi.fn(),
    }) as unknown as ExtensionContext;

  it("throws without the opt-in destination and records both projections passively", () => {
    delete process.env.PREWALK_CANARY_TELEMETRY;
    expect(() => recorder({} as ExtensionAPI)).toThrow(/PREWALK_CANARY_TELEMETRY/);

    const { fire, telemetryPath } = loadRecorder();
    const context = contextStub([
      {
        type: "message",
        id: "m1",
        parentId: null,
        timestamp: "2026-09-19T05:00:00.000Z",
        message: { role: "custom", customType: "pi-fabric-prewalk-armed", content: "decoy" },
      },
      {
        type: "custom_message",
        id: "a1",
        parentId: "m1",
        timestamp: "2026-09-19T05:00:01.000Z",
        customType: "pi-fabric-prewalk-armed",
        content: "armed",
        display: false,
        details: { mode: "in-place", model: "zro/glm-5.3" },
      },
      {
        type: "custom_message",
        id: "c1",
        parentId: "a1",
        timestamp: "2026-09-19T05:00:02.000Z",
        customType: "pi-fabric-prewalk-continue",
        content: "continue",
        display: false,
        details: { continuationId: "c-1", returnModel: "openai-codex/gpt-6-astra" },
      },
      {
        type: "custom_message",
        id: "o1",
        parentId: "c1",
        timestamp: "2026-09-19T05:00:03.000Z",
        customType: "other-extension",
        content: "ignored",
        display: false,
      },
    ]);
    fire("session_start", { reason: "startup" }, context);
    fire("before_agent_start", { prompt: "go" }, context);
    fire("message_end", {
      message: {
        role: "custom",
        customType: "pi-fabric-prewalk-armed",
        content: "armed",
        display: false,
        details: { mode: "in-place", model: "zro/glm-5.3" },
        timestamp: 1500,
      },
    }, context);
    fire("model_select", {
      model: { provider: "zro", id: "glm-5.3" },
      previousModel: { provider: "openai-codex", id: "gpt-6-astra" },
      source: "set",
    }, context);
    fire("session_compact_failed", {
      reason: "manual",
      errorMessage: "Compaction failed: Nothing to compact (session too small)",
      aborted: false,
      willRetry: false,
      fromExtension: false,
    }, context);
    fire("session_shutdown", { reason: "quit" }, context);

    const records = fs
      .readFileSync(telemetryPath, "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as Record<string, unknown>);
    const byType = (type: string) => records.filter((record) => record.type === type);
    expect(byType("session_start")[0]).toMatchObject({ sessionId: "session-1", model: "openai-codex/gpt-6-astra" });
    expect(byType("prewalk_message")[0]).toMatchObject({ source: "live", customType: "pi-fabric-prewalk-armed" });
    expect(byType("model_select")[0]).toMatchObject({ model: "zro/glm-5.3", previous: "openai-codex/gpt-6-astra" });
    expect(byType("compaction_failed")[0]).toMatchObject({
      error: "Compaction failed: Nothing to compact (session too small)",
    });
    const shutdown = byType("session_shutdown")[0] as {
      prewalkLive: Array<{ customType: string; details: unknown }>;
      prewalkPersisted: Array<{ id: string | null; customType: string; details: { continuationId?: string } }>;
    };
    expect(shutdown.prewalkLive).toEqual([
      { customType: "pi-fabric-prewalk-armed", details: { mode: "in-place", model: "zro/glm-5.3" } },
    ]);
    expect(shutdown.prewalkPersisted.map((message) => message.customType)).toEqual([
      "pi-fabric-prewalk-armed",
      "pi-fabric-prewalk-continue",
    ]);
    expect(shutdown.prewalkPersisted[0]?.id).toBe("a1");
    expect(shutdown.prewalkPersisted[1]?.details.continuationId).toBe("c-1");
  });

  it("records fixture request markers only when opted in, without leaking or mutating messages", () => {
    const messages = [
      { role: "system", content: "TASK-MARKER" },
      { role: "assistant", content: "TASK-MARKER" },
      { role: "user", content: "TASK-MARKER private unrelated text" },
    ];
    const before = JSON.stringify(messages);
    const context = contextStub();
    const ordinary = loadRecorder();
    ordinary.fire("context", { type: "context", messages }, context);
    expect(fs.existsSync(ordinary.telemetryPath)).toBe(false);
    const contractPath = path.join(tempRoot(), "contract.json");
    fs.writeFileSync(contractPath, JSON.stringify({ markers: { task: "TASK-MARKER" }, exactlyOnce: ["task"] }));
    process.env.PREWALK_CANARY_REQUEST_CONTRACT = contractPath;
    try {
      const optedIn = loadRecorder();
      optedIn.fire("context", { type: "context", messages }, context);
      const text = fs.readFileSync(optedIn.telemetryPath, "utf8");
      expect(JSON.parse(text)).toMatchObject({
        type: "request_context", requestIndex: 1, model: "openai-codex/gpt-6-astra",
        requestEvidence: { layout: "context", truncated: false, matches: { task: [[2, 0, 0]] } },
      });
      expect(JSON.parse(text).requestEvidence.contractSha256).toMatch(/^[0-9a-f]{64}$/);
      expect(text).not.toContain("TASK-MARKER");
      expect(text).not.toContain("private unrelated text");
      expect(JSON.stringify(messages)).toBe(before);
    } finally { delete process.env.PREWALK_CANARY_REQUEST_CONTRACT; }
  });

  it("records effective compaction settings and terminal abort metadata passively", () => {
    const { fire, telemetryPath } = loadRecorder();
    const context = contextStub();
    fire("session_before_compact", {
      reason: "manual", preparation: { settings: { reserveTokens: 16384, keepRecentTokens: 20000 }, tokensBefore: 31000 },
    }, context);
    fire("session_compact_failed", { reason: "manual", aborted: true, willRetry: false, fromExtension: true }, context);
    const records = fs.readFileSync(telemetryPath, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(records[0]).toMatchObject({ type: "compaction_attempt", model: "openai-codex/gpt-6-astra", tokensBefore: 31000,
      settings: { reserveTokens: 16384, keepRecentTokens: 20000 } });
    expect(records[1]).toMatchObject({ type: "compaction_failed", aborted: true, willRetry: false, fromExtension: true });
  });

  it("aborts the run past the turn cap", () => {
    const { fire } = loadRecorder();
    const context = contextStub();
    for (let turn = 1; turn <= 16; turn += 1) {
      fire("turn_start", { turnIndex: turn, timestamp: turn }, context);
    }
    expect((context as unknown as { abort: ReturnType<typeof vi.fn> }).abort).not.toHaveBeenCalled();
    fire("turn_start", { turnIndex: 17, timestamp: 17 }, context);
    expect((context as unknown as { abort: ReturnType<typeof vi.fn> }).abort).toHaveBeenCalledTimes(1);
  });

  it("reads a validated PREWALK_CANARY_MAX_TURNS and records the chosen cap", () => {
    process.env.PREWALK_CANARY_MAX_TURNS = "3";
    try {
      const { fire, telemetryPath } = loadRecorder();
      const context = contextStub();
      fire("session_start", { reason: "startup" }, context);
      for (let turn = 1; turn <= 3; turn += 1) {
        fire("turn_start", { turnIndex: turn, timestamp: turn }, context);
      }
      expect((context as unknown as { abort: ReturnType<typeof vi.fn> }).abort).not.toHaveBeenCalled();
      fire("turn_start", { turnIndex: 4, timestamp: 4 }, context);
      expect((context as unknown as { abort: ReturnType<typeof vi.fn> }).abort).toHaveBeenCalledTimes(1);
      const records = fs
        .readFileSync(telemetryPath, "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(records.find((record) => record.type === "session_start")).toMatchObject({ turnCap: 3 });
    } finally {
      delete process.env.PREWALK_CANARY_MAX_TURNS;
    }
  });

  it("rejects an invalid PREWALK_CANARY_MAX_TURNS", () => {
    process.env.PREWALK_CANARY_MAX_TURNS = "0";
    try {
      expect(() => loadRecorder()).toThrow(/positive integer/);
    } finally {
      delete process.env.PREWALK_CANARY_MAX_TURNS;
    }
  });
});

// Synthetic parser/CLI fixtures, not live performance evidence. No private archive is required.
const cells = ["off1", "on1", "on2", "off2"] as const;
const readGzJsonl = (file: string) => parseJsonLines(zlib.gunzipSync(fs.readFileSync(file)).toString("utf8"), file);
const mainModel = "fixture/main";
const executorModel = "fixture/executor";

const createRecoveryArchive = () => {
  const archiveDir = tempRoot();
  const files: Record<string, string | Buffer> = {
    "manifest.json": JSON.stringify({ head: "0".repeat(40), main: { model: mainModel }, prewalk: { model: executorModel } }),
  };
  const gzipJsonl = (events: unknown[]) => zlib.gzipSync(events.map((event) => JSON.stringify(event)).join("\n") + "\n");
  for (const cell of cells) {
    const isOn = cell.startsWith("on");
    const modelChanges = isOn ? [
      { type: "model_select", at: 1500, model: executorModel, previous: mainModel },
      { type: "model_select", at: 4500, model: mainModel, previous: executorModel },
    ] : [];
    const events = [
      { type: "session", version: 3, id: cell },
      assistantEvent(mainModel, 1000),
      ...(isOn ? [
        { type: "message_end", message: { role: "custom", customType: "pi-fabric-prewalk-armed", timestamp: 1400, details: { mode: "in-place" } } },
        assistantEvent(executorModel, 2000),
        assistantEvent(executorModel, 3000),
        { type: "message_end", message: { role: "custom", customType: "pi-fabric-prewalk-continue", timestamp: 4600, details: { continuationId: `fixture-${cell}` } } },
      ] : []),
      assistantEvent(mainModel, 5000),
      { type: "agent_settled" },
    ];
    const telemetry = [
      { type: "session_start", sessionId: cell, at: 900 },
      ...modelChanges,
      ...(isOn ? [{ type: "compaction_failed", at: 4700, reason: "prewalk-return", error: "Nothing to compact (session too small)" }] : []),
      { type: "session_shutdown", at: 6000 },
    ];
    // Independent expected totals for two assistantEvent records per model;
    // never call the analyzer to construct its own expected result.
    const totals: PerModelUsage = { requests: 2, input: 20, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 30, recordedCostEstimateUsd: 1 };
    files[`${cell}/events.jsonl.gz`] = gzipJsonl(events);
    files[`${cell}/telemetry.jsonl.gz`] = gzipJsonl(telemetry);
    files[`${cell}/result.json`] = JSON.stringify({
      perModel: { [mainModel]: totals, ...(isOn ? { [executorModel]: totals } : {}) },
      assistantTurns: isOn ? 4 : 2,
      finalModel: mainModel,
      modelChanges,
      quality: { success: true, passed: 1, total: 1 },
      changedProtected: [],
      wallMs: 5100,
    });
  }
  for (const [relative, content] of Object.entries(files)) {
    const file = path.join(archiveDir, relative);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, content);
  }
  fs.writeFileSync(path.join(archiveDir, "checksums.sha256"), Object.keys(files).sort()
    .map((relative) => `${sha256(fs.readFileSync(path.join(archiveDir, relative)))}  ${relative}`).join("\n") + "\n");
  return archiveDir;
};

const recoverArchive = (archiveDir: string, out: string) => spawnSync(process.execPath, [
  path.join(projectRoot, "bench/prewalk/recover-live-canary.mjs"), "--archive", archiveDir, "--out", out,
], { encoding: "utf8", timeout: 10_000 });

describe("portable recorded-canary recovery", () => {
  it("verifies every archived byte and recovers all four cells without changing the input", () => {
    const archiveDir = createRecoveryArchive();
    const before = snapshotTree(archiveDir);
    const out = tempRoot();
    const result = recoverArchive(archiveDir, out);
    expect(result.status, `${result.error ?? ""}\n${result.stderr}`).toBe(0);
    const recovered = JSON.parse(fs.readFileSync(path.join(out, "recovery.json"), "utf8")) as {
      verifiedArchiveFiles: number; cells: Array<{ cell: string }>;
    };
    expect(recovered.verifiedArchiveFiles).toBe(1 + cells.length * 3);
    expect(recovered.cells.map(({ cell }) => cell)).toEqual(cells);
    expect(compareSnapshots(before, snapshotTree(archiveDir))).toEqual({ same: true, changed: [], added: [], removed: [] });
  });

  it("rejects corrupted archived bytes before writing a recovery", () => {
    const archiveDir = createRecoveryArchive();
    fs.appendFileSync(path.join(archiveDir, "on1/events.jsonl.gz"), "corrupted");
    const out = tempRoot();
    const result = recoverArchive(archiveDir, out);
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Archive checksum mismatch: on1/events.jsonl.gz");
    expect(fs.readdirSync(out)).toEqual([]);
  });

  it("recovers armed+continue with exact continuation ids for ON cells only and matches recorded usage", () => {
    const archiveDir = createRecoveryArchive();
    const manifest = JSON.parse(fs.readFileSync(path.join(archiveDir, "manifest.json"), "utf8")) as {
      main: { model: string };
      prewalk: { model: string };
    };
    const expectedContinuationIds: Record<string, string> = { on1: "fixture-on1", on2: "fixture-on2" };
    for (const cell of cells) {
      const frozen = JSON.parse(fs.readFileSync(path.join(archiveDir, cell, "result.json"), "utf8")) as {
        perModel: Record<string, PerModelUsage>;
        assistantTurns: number;
        finalModel: string;
        modelChanges: Array<{ model: string; previous: string }>;
        quality: { success: boolean; passed: number; total: number };
        changedProtected: string[];
      };
      const liveEvents = readGzJsonl(path.join(archiveDir, cell, "events.jsonl.gz"));
      const telemetryEvents = readGzJsonl(path.join(archiveDir, cell, "telemetry.jsonl.gz"));
      expect(() => assertCompleteRecording(liveEvents, telemetryEvents)).not.toThrow();
      const analysis = analyzeCell({ name: cell, liveEvents, telemetryEvents });

      expect(frozen.quality.success).toBe(true);
      expect(frozen.changedProtected).toEqual([]);
      expect(frozen.finalModel).toBe(manifest.main.model);
      expect(analysis.assistantCount).toBe(frozen.assistantTurns);
      const frozenMatch = usageMatches(analysis.perModel, frozen.perModel);
      expect(frozenMatch.ok, JSON.stringify(frozenMatch.mismatches.slice(0, 3))).toBe(true);

      if (cell.startsWith("on")) {
        expect(frozen.modelChanges).toHaveLength(2);
        expect(frozen.modelChanges[0]).toMatchObject({ model: manifest.prewalk.model, previous: manifest.main.model });
        expect(frozen.modelChanges[1]).toMatchObject({ model: manifest.main.model, previous: manifest.prewalk.model });
        expect(analysis.prewalkMessages.map((message) => message.customType)).toEqual([
          "pi-fabric-prewalk-armed", "pi-fabric-prewalk-continue",
        ]);
        const details = analysis.prewalkMessages[1]?.details as { continuationId?: string };
        expect(details.continuationId).toBe(expectedContinuationIds[cell]);
        expect(analysis.phases).toMatchObject({ handoffAt: 1500, returnAt: 4500, preHandoffMainMs: 500, executorIntervalMs: 3000, executorAssistantSpanMs: 1000, returnMs: 1500 });
        expect(analysis.phases.compaction?.outcome).toBe("compaction_failed");
        expect(analysis.phases.compaction?.error).toContain("Nothing to compact");
        expect(analysis.phases.compaction?.attemptMs).toBeNull();
      } else {
        expect(frozen.modelChanges).toEqual([]);
        expect(analysis.prewalkMessages).toEqual([]);
        expect(analysis.phases.handoffAt).toBeNull();
        expect(analysis.phases.compaction).toBeNull();
      }
    }
  });
});

describe.skipIf(process.platform === "win32")("canary runner subprocess", () => {
  const runnerPath = path.join(projectRoot, "bench", "prewalk", "prewalk-canary-run.mjs");
  const fakePiSource = path.join(projectRoot, "bench", "prewalk", "fixtures", "fake-canary-pi.mjs");
  const spawnRunner = (options: {
    mode?: string;
    args?: string[];
    leadingArgs?: string[];
    reuseOutDir?: string;
    timeoutSeconds?: number;
    rpc?: boolean;
    rpcRuns?: number;
    turns?: number;
  } = {}) => {
    const root = tempRoot();
    const fakePi = path.join(root, "fake-pi");
    fs.copyFileSync(fakePiSource, fakePi);
    fs.chmodSync(fakePi, 0o755);
    const promptFile = path.join(root, "prompt.txt");
    fs.writeFileSync(promptFile, "probe\n");
    const cwd = path.join(root, "work");
    fs.mkdirSync(cwd, { recursive: true });
    const outDir = options.reuseOutDir ?? path.join(root, "cell");
    const result = spawnSync(
      process.execPath,
      [
        runnerPath,
        ...(options.leadingArgs ?? []),
        "--out",
        outDir,
        "--cwd",
        cwd,
        "--prompt-file",
        promptFile,
        "--pi-binary",
        fakePi,
        "--timeout-seconds",
        String(options.timeoutSeconds ?? 10),
        ...(options.rpc ? ["--rpc", "--rpc-runs", String(options.rpcRuns ?? 2)] : []),
        ...(options.turns === undefined ? [] : ["--turns", String(options.turns)]),
        ...(options.args ?? []),
      ],
      {
        cwd: projectRoot,
        env: { ...process.env, FAKE_PI_MODE: options.mode ?? "ok" },
        encoding: "utf8",
        timeout: 30_000,
      },
    );
    return { result, outDir };
  };
  const summaryLine = (result: { stdout: string }) =>
    JSON.parse((result.stdout.trim().split("\n").at(-1) ?? "{}")) as { ok: boolean; problems: string[] };

  it("preserves repeated extension flags at argument zero and later positions", () => {
    const { result, outDir } = spawnRunner({
      leadingArgs: ["--extension", "/tmp/first-provider"],
      args: ["--extension", "/tmp/last-provider"],
    });
    expect(result.status, result.stderr).toBe(0);
    expect(summaryLine(result).ok).toBe(true);
    const started = JSON.parse(fs.readFileSync(path.join(outDir, "started.json"), "utf8")) as { extensions: string[] };
    expect(started.extensions).toEqual([
      projectRoot, path.join(projectRoot, "bench", "prewalk", "prewalk-canary-telemetry.ts"),
      "/tmp/first-provider", "/tmp/last-provider",
    ]);
  });

  it("captures a complete run with aligned arrival lines and recorded runtime hashes", () => {
    const { result, outDir } = spawnRunner({ args: ["--extension", "/tmp/extra-provider"] });
    expect(result.status).toBe(0);
    expect(summaryLine(result).ok).toBe(true);
    const started = JSON.parse(fs.readFileSync(path.join(outDir, "started.json"), "utf8")) as {
      extensions: string[];
      runtimeHashes: Record<string, string | null>;
    };
    expect(started.extensions).toEqual([
      projectRoot,
      path.join(projectRoot, "bench", "prewalk", "prewalk-canary-telemetry.ts"),
      "/tmp/extra-provider",
    ]);
    expect(Object.keys(started.runtimeHashes)).toEqual([
      "dist/index.js",
      "dist/fabric-runtime-state.js",
      "bench/prewalk/prewalk-canary-telemetry.ts",
    ]);
    for (const hash of Object.values(started.runtimeHashes)) {
      expect(hash === null || /^[0-9a-f]{64}$/.test(hash)).toBe(true);
    }
    const events = fs.readFileSync(path.join(outDir, "events.jsonl"), "utf8").trim().split("\n");
    const arrivals = fs
      .readFileSync(path.join(outDir, "arrival.jsonl"), "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { lineIndex: number; arrivalMs: number });
    expect(arrivals).toHaveLength(events.length);
    expect(arrivals.map((arrival) => arrival.lineIndex)).toEqual(events.map((_, index) => index));
    expect(
      arrivals.every((arrival, index) => index === 0 || arrival.arrivalMs >= (arrivals[index - 1]?.arrivalMs ?? 0)),
    ).toBe(true);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      ok: boolean;
      telemetryShutdown: boolean;
      problems: string[];
    };
    expect(finished.ok).toBe(true);
    expect(finished.telemetryShutdown).toBe(true);
    expect(finished.problems).toEqual([]);
    expect(fs.existsSync(path.join(outDir, "stdout-tail.bin"))).toBe(false);
  }, 30_000);

  it("propagates nonzero pi exits and missing telemetry shutdown", () => {
    const failing = spawnRunner({ mode: "fail" });
    expect(failing.result.status).toBe(1);
    expect(JSON.stringify(summaryLine(failing.result).problems)).toContain("exited with code 3");
    const noShutdown = spawnRunner({ mode: "noshutdown" });
    expect(noShutdown.result.status).toBe(1);
    expect(JSON.stringify(summaryLine(noShutdown.result).problems)).toContain("no session_shutdown record");
    const badTelemetry = spawnRunner({ mode: "badtelemetry" });
    expect(badTelemetry.result.status).toBe(1);
    expect(JSON.stringify(summaryLine(badTelemetry.result).problems)).toContain("malformed telemetry line 1");
  }, 30_000);

  it("preserves unterminated stdout bytes exactly and excludes them from parsed lines", () => {
    const { result, outDir } = spawnRunner({ mode: "partial" });
    expect(result.status).toBe(1);
    expect(fs.readFileSync(path.join(outDir, "stdout-tail.bin")).toString()).toBe('{"type":"agent_settled","trunc');
    const events = fs.readFileSync(path.join(outDir, "events.jsonl"), "utf8").trim().split("\n");
    expect(events).toHaveLength(1);
    expect(summaryLine(result).problems.join(";")).toContain("unterminated");
  }, 30_000);

  it("kills the owned process group on timeout", () => {
    const { result, outDir } = spawnRunner({ mode: "hang", timeoutSeconds: 2 });
    expect(result.status).toBe(1);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      timedOut: boolean;
      problems: string[];
    };
    expect(finished.timedOut).toBe(true);
    expect(finished.problems.join(";")).toContain("timed out after 2s");
  }, 30_000);

  it("refuses to replay a started cell and validates arguments before spawning", () => {
    const first = spawnRunner();
    expect(first.result.status).toBe(0);
    const replay = spawnRunner({ reuseOutDir: first.outDir });
    expect(replay.result.status).toBe(1);
    expect(replay.result.stderr).toContain("already started");

    const root = tempRoot();
    const cwd = path.join(root, "work");
    fs.mkdirSync(cwd, { recursive: true });
    const promptFile = path.join(root, "prompt.txt");
    fs.writeFileSync(promptFile, "probe\n");
    const relative = spawnSync(
      process.execPath,
      [runnerPath, "--out", "relative-dir", "--cwd", cwd, "--prompt-file", promptFile],
      { cwd: projectRoot, encoding: "utf8", timeout: 10_000 },
    );
    expect(relative.status).toBe(1);
    expect(relative.stderr).toContain("absolute");
    const badTimeout = spawnSync(
      process.execPath,
      [runnerPath, "--out", path.join(root, "cell"), "--cwd", cwd, "--prompt-file", promptFile, "--timeout-seconds", "0"],
      { cwd: projectRoot, encoding: "utf8", timeout: 10_000 },
    );
    expect(badTimeout.status).toBe(1);
    expect(badTimeout.stderr).toContain("positive integer");
  }, 30_000);

  it("keeps an rpc session open until the expected runs settle and the queue drains", () => {
    const { result, outDir } = spawnRunner({ mode: "rpc-ok", rpc: true, rpcRuns: 2 });
    expect(result.status).toBe(0);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      ok: boolean;
      mode: string;
      settlements: number;
      problems: string[];
    };
    expect(finished.ok).toBe(true);
    expect(finished.mode).toBe("rpc");
    expect(finished.settlements).toBe(2);
    expect(finished.problems).toEqual([]);
    const rpc = JSON.parse(fs.readFileSync(path.join(outDir, "rpc.json"), "utf8")) as {
      settlements: number;
      state: { isStreaming: boolean; pendingMessageCount: number };
      commands: Array<{ command: string; success: boolean | null }>;
    };
    expect(rpc.settlements).toBe(2);
    expect(rpc.state.isStreaming).toBe(false);
    expect(rpc.state.pendingMessageCount).toBe(0);
    expect(rpc.commands.map((command) => command.command)).toEqual([
      "prompt",
      "get_state",
      "get_session_stats",
      "get_entries",
    ]);
    expect(rpc.commands.every((command) => command.success === true)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(outDir, "rpc-entries.json"), "utf8"))).toEqual([]);
    const started = JSON.parse(fs.readFileSync(path.join(outDir, "started.json"), "utf8")) as { mode: string; turns: number };
    expect(started.mode).toBe("rpc");
    expect(started.turns).toBe(16);
    // pid is EOF-driven: the fake records session_shutdown only when the
    // runner closes stdin after both runs settled.
    const telemetry = fs
      .readFileSync(path.join(outDir, "telemetry.jsonl"), "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { type: string });
    expect(telemetry.filter((record) => record.type === "session_shutdown")).toHaveLength(1);
  }, 30_000);

  it("fails the rpc run when the child exits before the expected runs", () => {
    const { result, outDir } = spawnRunner({ mode: "rpc-early-exit", rpc: true, rpcRuns: 2 });
    expect(result.status).toBe(1);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      ok: boolean;
      problems: string[];
    };
    expect(finished.ok).toBe(false);
    expect(finished.problems.join(";")).toContain("rpc recorded 1/2 agent settles");
  }, 30_000);

  it("fails the rpc run when get_state does not succeed", () => {
    const { result, outDir } = spawnRunner({ mode: "rpc-bad-state", rpc: true, rpcRuns: 2 });
    expect(result.status).toBe(1);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      ok: boolean;
      problems: string[];
    };
    expect(finished.problems.join(";")).toContain("rpc get_state failed: state unavailable");
  }, 30_000);

  it("does not accept an idle-looking state while the queue is not drained", () => {
    const { result, outDir } = spawnRunner({ mode: "rpc-pending", rpc: true, rpcRuns: 2, timeoutSeconds: 2 });
    expect(result.status).toBe(1);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      timedOut: boolean;
      problems: string[];
    };
    expect(finished.timedOut).toBe(true);
    expect(finished.problems.join(";")).toContain("rpc session was not idle with an empty queue");
  }, 30_000);

  it("records malformed and stray rpc stdout instead of trusting it", () => {
    const { result, outDir } = spawnRunner({ mode: "rpc-noise", rpc: true, rpcRuns: 2 });
    expect(result.status).toBe(1);
    const finished = JSON.parse(fs.readFileSync(path.join(outDir, "finished.json"), "utf8")) as {
      ok: boolean;
      problems: string[];
    };
    expect(finished.ok).toBe(false);
    expect(finished.problems.join(";")).toContain("malformed rpc stdout line");
    expect(finished.problems.join(";")).toContain("unexpected rpc response id");
  }, 30_000);
});
