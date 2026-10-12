import type {
  ExtensionAPI,
  ExtensionContext,
  SessionBeforeCompactEvent,
  SessionEntry,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import {
  applyCarryUpdate,
  COMPACTION_CARRY_ENTRY_TYPE,
  latestCarryItems,
} from "../src/compaction/carry.js";
import { compileFabricSummary, fabricCompactionVersion, registerCompactionHook } from "../src/compaction/hook.js";
import { MAX_PRESERVE_ITEM_CHARS, MAX_PRESERVE_ITEMS } from "../src/compaction/instructions.js";
import {
  CompactionOwnerObserver,
  compactionOwnerOf,
  observedCompactionOwner,
} from "../src/compaction/owner.js";
import { compactionPressure } from "../src/compaction/pressure.js";
import { compactAtConfiguredThreshold } from "../src/compaction/threshold.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { CompactController } from "../src/core/compact-controller.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { CompactProvider } from "../src/providers/compact-provider.js";

let ids = 0;
const nextId = (): string => `c${++ids}`;
const user = (text: string): SessionEntry => ({
  type: "message",
  id: nextId(),
  parentId: null,
  timestamp: "2024-01-01T00:00:00Z",
  message: { role: "user", content: text, timestamp: 1 },
}) as SessionEntry;
const assistant = (text: string): SessionEntry => ({
  type: "message",
  id: nextId(),
  parentId: null,
  timestamp: "2024-01-01T00:00:01Z",
  message: {
    role: "assistant",
    content: [{ type: "text", text }],
    api: "anthropic",
    provider: "anthropic",
    model: "m",
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    stopReason: "stop",
    timestamp: 2,
  },
}) as SessionEntry;
const carryEntry = (data: unknown): SessionEntry => ({
  type: "custom",
  id: nextId(),
  parentId: null,
  timestamp: "2024-01-01T00:00:02Z",
  customType: COMPACTION_CARRY_ENTRY_TYPE,
  data,
}) as SessionEntry;
const compactionEntry = (details: unknown, fromHook?: boolean): SessionEntry => ({
  type: "compaction",
  id: nextId(),
  parentId: null,
  timestamp: "2024-01-01T00:00:03Z",
  summary: "s",
  firstKeptEntryId: "",
  tokensBefore: 10,
  details,
  ...(fromHook === undefined ? {} : { fromHook }),
}) as SessionEntry;

const fabricDetails = (): unknown => {
  const result = compileFabricSummary([user("goal"), assistant("done")], 1000);
  if (!("compaction" in result)) throw new Error("expected compaction");
  return result.compaction.details;
};

const invocation = (
  branch: SessionEntry[],
  usage?: { tokens: number | null; contextWindow: number; percent: number | null },
): FabricInvocationContext => ({
  cwd: process.cwd(),
  signal: undefined,
  parentToolCallId: "t",
  nestedToolCallId: "n",
  extensionContext: {
    model: { provider: "anthropic", id: "sonnet", contextWindow: 100_000 },
    getContextUsage: () => usage,
    sessionManager: { getBranch: () => branch },
  } as unknown as ExtensionContext,
  update() {},
  activity() {},
});

describe("compaction config", () => {
  it("defaults the new keys and validates pressure bands as a pair", () => {
    expect(DEFAULT_FABRIC_CONFIG.compaction).toMatchObject({
      pressureBands: { warn: 0.6, urgent: 0.8 },
      outputReserveTokens: 0,
      repairOrphans: true,
    });
    const valid = normalizeFabricConfig({
      compaction: { pressureBands: { warn: 0.5, urgent: 0.9 }, outputReserveTokens: 12_000.7, repairOrphans: false },
    });
    expect(valid.compaction).toMatchObject({
      pressureBands: { warn: 0.5, urgent: 0.9 },
      outputReserveTokens: 12_000,
      repairOrphans: false,
    });
    for (const pressureBands of [
      { warn: 0.9, urgent: 0.5 },
      { warn: 0, urgent: 0.5 },
      { warn: 0.5, urgent: 1 },
      { warn: 0.85 },
      { warn: "0.5", urgent: 0.9 },
    ]) {
      expect(normalizeFabricConfig({ compaction: { pressureBands } }).compaction.pressureBands)
        .toEqual({ warn: 0.6, urgent: 0.8 });
    }
    expect(normalizeFabricConfig({ compaction: { pressureBands: { urgent: 0.95 } } }).compaction.pressureBands)
      .toEqual({ warn: 0.6, urgent: 0.95 });
    expect(normalizeFabricConfig({ compaction: { outputReserveTokens: -5 } }).compaction.outputReserveTokens).toBe(0);
  });
});

describe("compaction owner", () => {
  it("classifies committed entries and reads the latest on the branch", () => {
    expect(compactionOwnerOf(undefined)).toBe("none");
    expect(compactionOwnerOf({ details: fabricDetails(), fromHook: true })).toBe("fabric");
    expect(compactionOwnerOf({ details: { compactor: "other" }, fromHook: true })).toBe("external");
    expect(compactionOwnerOf({ details: { compactor: "fabric", version: 9 }, fromHook: true })).toBe("external");
    expect(compactionOwnerOf({})).toBe("pi");
    expect(observedCompactionOwner([user("x")])).toBe("none");
    expect(observedCompactionOwner([compactionEntry(fabricDetails(), true), compactionEntry(undefined)]))
      .toBe("pi");
  });

  it("warns once per session only when Fabric is enabled and lost", () => {
    const observer = new CompactionOwnerObserver();
    const external = { details: {}, fromHook: true };
    expect(observer.observe("s1", { details: fabricDetails(), fromHook: true }, true).warning).toBeUndefined();
    expect(observer.observe("s1", external, false).warning).toBeUndefined();
    const first = observer.observe("s1", external, true);
    expect(first.owner).toBe("external");
    expect(first.warning).toContain("load order");
    expect(first.warning).toContain("docs/compaction.md");
    expect(observer.observe("s1", {}, true).warning).toBeUndefined();
    observer.noteDeliberateYield();
    expect(observer.observe("s2", external, true).warning).toBeUndefined();
    expect(observer.observe("s2", external, true).warning).toContain("another extension");
  });
});

describe("compact.pressure", () => {
  it("reports bands, headroom, thresholds and owner", () => {
    const config = structuredClone(DEFAULT_FABRIC_CONFIG.compaction);
    config.thresholds["anthropic/sonnet"] = 0.7;
    const branch = [compactionEntry({}, true)];
    const read = (tokens: number | null, cfg = config) => compactionPressure(
      invocation(branch, { tokens, contextWindow: 100_000, percent: tokens === null ? null : tokens / 1000 })
        .extensionContext,
      cfg,
    );
    expect(read(50_000)).toEqual({
      tokens: 50_000,
      contextWindow: 100_000,
      fraction: 0.5,
      headroomTokens: 50_000,
      band: "ok",
      outputReserveTokens: 0,
      thresholdFraction: 0.7,
      owner: "external",
    });
    expect(read(60_000).band).toBe("warn");
    expect(read(80_000).band).toBe("urgent");
    expect(read(null)).toMatchObject({ tokens: null, fraction: null, headroomTokens: null, band: "unknown" });
    const reserved = { ...config, outputReserveTokens: 50_000 };
    expect(read(55_000, reserved)).toMatchObject({ band: "urgent", headroomTokens: 45_000, outputReserveTokens: 50_000 });

    const tokenConfig = structuredClone(config);
    tokenConfig.tokenThresholds["anthropic/sonnet"] = 90_000;
    const withTokens = read(10_000, tokenConfig);
    expect(withTokens.thresholdTokens).toBe(90_000);
    expect(withTokens.thresholdFraction).toBeUndefined();
  });

  it("is unknown without a host context", () => {
    expect(compactionPressure(undefined, DEFAULT_FABRIC_CONFIG.compaction)).toMatchObject({
      band: "unknown",
      contextWindow: null,
      owner: "none",
    });
  });
});

describe("headroom trigger", () => {
  const contextAt = (tokens: number): ExtensionContext => ({
    model: { provider: "anthropic", id: "sonnet" },
    getContextUsage: () => ({ tokens, contextWindow: 100_000, percent: tokens / 1000 }),
    compact: vi.fn((options) => options?.onComplete?.({} as never)),
    hasUI: false,
  } as unknown as ExtensionContext);

  it("compacts when headroom drops below the output reserve and reports the trigger", async () => {
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.compaction.outputReserveTokens = 30_000;
    const onTrigger = vi.fn();
    const below = contextAt(65_000);
    await expect(compactAtConfiguredThreshold(below, config, onTrigger)).resolves.toBe(false);
    expect(below.compact).not.toHaveBeenCalled();

    const breached = contextAt(75_000);
    // A token threshold that is not reached does not mask the reserve.
    config.compaction.tokenThresholds["anthropic/sonnet"] = 95_000;
    await expect(compactAtConfiguredThreshold(breached, config, onTrigger)).resolves.toBe(true);
    expect(onTrigger).toHaveBeenCalledWith("headroom", true);

    const controller = new CompactController();
    controller.noteAutoCompaction("headroom", true);
    expect(controller.status().lastAuto).toMatchObject({ trigger: "headroom", committed: true });
  });

  it("stays disabled at the default reserve", async () => {
    const context = contextAt(99_000);
    await expect(compactAtConfiguredThreshold(context, structuredClone(DEFAULT_FABRIC_CONFIG))).resolves.toBe(false);
  });

  it("does not defer Pi's threshold compaction once the reserve is breached", () => {
    let handler: ((event: SessionBeforeCompactEvent, context: ExtensionContext) => unknown) | undefined;
    const pi = {
      on(name: string, candidate: unknown) {
        if (name === "session_before_compact") handler = candidate as typeof handler;
      },
    } as unknown as ExtensionAPI;
    let reserve = 0;
    registerCompactionHook(pi, {
      getEngine: () => "pi",
      getThresholdTokens: () => 95_000,
      getOutputReserveTokens: () => reserve,
    });
    const context = { model: { provider: "anthropic", id: "sonnet", contextWindow: 100_000 } } as unknown as ExtensionContext;
    const event = { reason: "threshold", preparation: { tokensBefore: 80_000 }, branchEntries: [] } as unknown as SessionBeforeCompactEvent;
    expect(handler?.(event, context)).toEqual({ cancel: true });
    reserve = 30_000;
    expect(handler?.(event, context)).toBeUndefined();
  });

  it("notes a deliberate yield for the pi-vcc sentinel", () => {
    let handler: ((event: SessionBeforeCompactEvent, context: ExtensionContext) => unknown) | undefined;
    const pi = {
      on(name: string, candidate: unknown) {
        if (name === "session_before_compact") handler = candidate as typeof handler;
      },
    } as unknown as ExtensionAPI;
    const onYield = vi.fn();
    registerCompactionHook(pi, { getEngine: () => "fabric", onYield });
    handler?.({ customInstructions: "__pi_vcc__" } as SessionBeforeCompactEvent, {} as ExtensionContext);
    expect(onYield).toHaveBeenCalledOnce();
  });
});

describe("carry-forward focus", () => {
  it("applies clear, replace, remove, add in order with dedupe", () => {
    expect(applyCarryUpdate(["a", "b"], { add: ["b", "c"] })).toEqual(["a", "b", "c"]);
    expect(applyCarryUpdate(["a", "b"], { remove: ["a"], add: ["d"] })).toEqual(["b", "d"]);
    expect(applyCarryUpdate(["a"], { clear: true, add: ["z"] })).toEqual(["z"]);
    expect(applyCarryUpdate(["a"], { items: ["x", "x", "y"] })).toEqual(["x", "y"]);
  });

  it("fails closed on bounds", () => {
    const full = Array.from({ length: MAX_PRESERVE_ITEMS }, (_, index) => `item ${index}`);
    expect(() => applyCarryUpdate(full, { add: ["one more"] })).toThrow(/exceeds 16 items/);
    expect(() => applyCarryUpdate([], { add: ["x".repeat(MAX_PRESERVE_ITEM_CHARS + 1)] })).toThrow(/compact carry/);
    expect(() => applyCarryUpdate([], { add: ["   "] })).toThrow(/non-empty/);
    expect(() => applyCarryUpdate([], { add: ["\ud800"] })).toThrow(/surrogate/);
  });

  it("replays the latest entry, including an explicit clear, and rejects malformed data", () => {
    expect(latestCarryItems([carryEntry({ version: 1, items: ["a"] }), user("x")])).toEqual(["a"]);
    expect(latestCarryItems([carryEntry({ version: 1, items: ["a"] }), carryEntry({ version: 1, items: [] })]))
      .toEqual([]);
    expect(latestCarryItems([carryEntry({ version: 1, items: ["a"] }), carryEntry({ version: 2, items: ["b"] })]))
      .toEqual([]);
    expect(latestCarryItems([carryEntry({ version: 1, items: [1] })])).toEqual([]);
  });

  it("renders the carry list in every Fabric summary until cleared", () => {
    const first = [user("goal"), carryEntry({ version: 1, items: ["Auth regression is open"] }), assistant("step one")];
    const compiled = compileFabricSummary(first, 1000);
    if (!("compaction" in compiled)) throw new Error("expected compaction");
    expect(compiled.compaction.summary).toContain("[Carry Forward]");
    expect(compiled.compaction.summary).toContain("- Auth regression is open [carry:0]");
    expect(compiled.compaction.details?.sections).toContain("[Carry Forward]");
    expect(compiled.compaction.details?.carry).toEqual({ count: 1, renderedOmittedBytes: 0 });
    expect(fabricCompactionVersion(compiled.compaction.details)).toBe(2);

    const second = [
      ...first,
      compactionEntry(compiled.compaction.details, true),
      user("more"),
      assistant("step two"),
    ];
    const again = compileFabricSummary(second, 1000);
    if (!("compaction" in again)) throw new Error("expected compaction");
    expect(again.compaction.summary).toContain("- Auth regression is open [carry:0]");

    const cleared = compileFabricSummary([...second, carryEntry({ version: 1, items: [] }), user("go"), assistant("x")], 1000);
    if (!("compaction" in cleared)) throw new Error("expected compaction");
    expect(cleared.compaction.summary).not.toContain("[Carry Forward]");
    expect(cleared.compaction.details?.carry).toBeUndefined();
  });

  it("compact.carry reads, persists changes only, and fails closed without a session writer", async () => {
    const branch: SessionEntry[] = [user("goal")];
    const appended: Array<{ customType: string; data: unknown }> = [];
    const provider = new CompactProvider(new CompactController(), {
      appendEntry: (customType, data) => {
        appended.push({ customType, data });
        branch.push(carryEntry(data));
      },
    });
    const context = invocation(branch);
    expect(await provider.invoke("carry", {}, context)).toEqual({ items: [] });
    expect(await provider.invoke("carry", { add: ["fact"] }, context)).toEqual({ items: ["fact"] });
    expect(await provider.invoke("carry", { add: ["fact"] }, context)).toEqual({ items: ["fact"] });
    expect(appended).toEqual([{ customType: COMPACTION_CARRY_ENTRY_TYPE, data: { version: 1, items: ["fact"] } }]);
    expect(await provider.invoke("carry", {}, context)).toEqual({ items: ["fact"] });
    expect(await provider.invoke("carry", { clear: true }, context)).toEqual({ items: [] });
    expect(appended).toHaveLength(2);

    await expect(provider.invoke("carry", { add: [""] }, context)).rejects.toThrow(/Invalid compact.carry/);
    await expect(provider.invoke("carry", { bogus: true }, context)).rejects.toThrow(/Invalid compact.carry/);
    const readOnly = new CompactProvider(new CompactController());
    expect(await readOnly.invoke("carry", {}, context)).toEqual({ items: [] });
    await expect(readOnly.invoke("carry", { add: ["x"] }, context)).rejects.toThrow(/cannot persist/);
  });

  it("compact.status and compact.pressure report owner and reserve", async () => {
    const provider = new CompactProvider(new CompactController(), {
      config: () => ({ ...DEFAULT_FABRIC_CONFIG.compaction, outputReserveTokens: 8_000 }),
    });
    const context = invocation([compactionEntry(fabricDetails(), true)], { tokens: 70_000, contextWindow: 100_000, percent: 70 });
    expect(await provider.invoke("status", {}, context)).toEqual({ owner: "fabric", outputReserveTokens: 8_000 });
    expect(await provider.invoke("pressure", {}, context)).toMatchObject({ band: "warn", owner: "fabric", outputReserveTokens: 8_000 });
    const descriptors = await provider.list({}, context);
    expect(Object.fromEntries(descriptors.map((descriptor) => [descriptor.name, descriptor.risk]))).toEqual({
      request: "write",
      status: "read",
      pressure: "read",
      carry: "write",
      cancel: "write",
    });
  });
});
