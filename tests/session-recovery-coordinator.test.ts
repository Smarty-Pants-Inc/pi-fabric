// Keep reason: data safety and B2/B4/B5 trust boundaries, including the enqueue-before-receipt
// failure window. Markers/native-admission fixtures use real files. Adapter doubles do NOT
// qualify runtime native barriers, actual leases, storage fencing or cross-host recovery.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AssistantMessage, Model } from "@earendil-works/pi-ai";
import { convertResponsesMessages } from "@earendil-works/pi-ai/api/openai-responses-shared";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { convertToLlm, SessionManager, type ContextEvent } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { prepareSessionRecovery } from "../src/recovery/coordinator.js";
import { repairRecoveryContext } from "../src/recovery/context-repair.js";
import type {
  ContinuationRequest, PortableSessionBinding, RecoveryAcceptedInput, RecoveryEffectsSettlement,
  RecoveryInputSnapshot, RecoveryToolResult, SessionRecoveryAdapters, SettledRecoveryEffect,
} from "../src/recovery/l7-interfaces.js";

const resultHash = (message: RecoveryToolResult): string => createHash("sha256").update(JSON.stringify(message), "utf8").digest("hex");
const nativeResult = (message: RecoveryToolResult) => ({ message, source: "native-entry" as const, postHook: true });
const originalAssistant = (ids = ["native-call", "unstarted-call"]): AssistantMessage => ({
  role: "assistant", api: "openai-responses", provider: "openai", model: "recovery-test", timestamp: 1,
  stopReason: "aborted", errorMessage: "original interruption",
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  content: ids.map(id => ({ type: "toolCall", id, name: id === "unstarted-call" ? "write" : "fabric_exec", arguments: {} })),
});

const fixture = (callIds = ["native-call", "unstarted-call"]) => {
  // Preserve all raw fixtures, including interrupted tails and predecessor receipts.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-coordinator-"));
  const sessionFile = path.join(root, "session.jsonl"), stateDirectory = path.join(root, "state");
  fs.mkdirSync(stateDirectory);
  const raw = '{"type":"session","id":"session-1"}\n{"partial":';
  fs.writeFileSync(sessionFile, raw);
  const receiptFile = path.join(stateDirectory, "predecessor-receipt.json");
  fs.writeFileSync(receiptFile, '{"machineId":"machine-A","bootId":"boot-A"}\n');
  const receipt = fs.readFileSync(receiptFile, "utf8");
  const successorMachine = { machineId: "machine-B" }; // Physical metadata consumed by the B2 double, not Fabric transport identity.
  const binding: PortableSessionBinding = {
    sessionFile, stateDirectory, transportMachine: "portable-session-machine",
    evidence: {
      sessionId: "session-1", predecessorReceipt: { bytes: receipt, sha256: createHash("sha256").update(receipt).digest("hex") },
      predecessorPodId: "pod-A", predecessorBootId: "boot-A",
      successorPodId: "pod-B", successorBootId: "boot-B",
      exclusionProof: { kind: "b1-fence", observedAt: 1, detail: "native-driver-receipt" },
    },
  };
  const snapshotFile = path.join(stateDirectory, "input-history.json");
  const getSnapshot = (): RecoveryInputSnapshot => JSON.parse(fs.readFileSync(snapshotFile, "utf8"));
  const setSnapshot = (value: RecoveryInputSnapshot): void => writeJsonAtomic(snapshotFile, value, { durable: true });
  setSnapshot({
    sessionId: "session-1", complete: true, accepted: {
      inputId: "original-input", text: "Finish the authorized file and peer steps.",
      podId: "pod-A", bootId: "boot-A", settlement: "open", cancellation: "not-cancelled",
    },
  });
  let settlement: RecoveryEffectsSettlement = {
    uncertain: 0, settled: [
      { toolCallId: "native-call", key: "saved-effect-1", outcome: "committed", resultBytesHash: "b".repeat(64) },
      { toolCallId: "unstarted-call", key: "saved-effect-2", outcome: "not-committed" },
    ],
  };
  let result: RecoveryToolResult = {
    role: "toolResult", toolCallId: "native-call", toolName: "fabric_exec",
    content: [{ type: "text", text: "Original durable committed result" }], isError: false, timestamp: 1,
  };
  settlement = { ...settlement, settled: settlement.settled.map(effect => effect.outcome === "committed"
    ? { ...effect, resultBytesHash: resultHash(result) } : effect) };
  const manager = SessionManager.inMemory(root);
  manager.appendMessage(originalAssistant(callIds));
  const events: string[] = [], requests: ContinuationRequest[] = [], generations: string[] = [];
  const nativeFile = path.join(stateDirectory, "native-admissions.json");
  const adapters: SessionRecoveryAdapters = {
    maintenance: Object.fromEntries(["admitMaintenance", "beginMaintenance", "prepareMaintenance", "consumeMaintenance", "adoptMaintenance", "read", "listPending", "withMaintenanceAttempt", "assertMaintenanceGate", "cancelMaintenance", "reconcileAfterJoin", "captureTaskAbort", "releaseLostMaintenance"].map(name => [name, () => { throw new Error("maintenance not used by this fixture"); }])) as unknown as SessionRecoveryAdapters["maintenance"],
    identity: {
      validate: vi.fn(async () => {
        // Simulate B2 running on another machine with the same retained predecessor receipt.
        // This is NOT native old-writer exclusion evidence or a runtime qualification.
        expect(successorMachine.machineId).not.toBe(JSON.parse(receipt).machineId);
        expect(fs.readFileSync(receiptFile, "utf8")).toBe(binding.evidence.predecessorReceipt.bytes);
        events.push("identity"); return binding;
      }),
      verifyPredecessor: vi.fn(async () => { throw new Error("Parent lease custody owns predecessor verification"); }),
    },
    inputs: {
      readRecoveryInput: vi.fn(async () => { events.push("snapshot"); return getSnapshot(); }),
      admitContinuation: vi.fn(async request => {
        events.push("native-admission"); requests.push({ ...request });
        const existing = fs.existsSync(nativeFile)
          ? JSON.parse(fs.readFileSync(nativeFile, "utf8")) as { request: ContinuationRequest; admissionId: string }
          : undefined;
        if (existing) {
          expect(request).toEqual(existing.request);
          return { admissionId: existing.admissionId, existing: true };
        }
        const admissionId = "native-admission-1";
        writeJsonAtomic(nativeFile, { request, admissionId }, { durable: true });
        return { admissionId, existing: false };
      }),
    },
    effects: {
      settlePendingIntents: vi.fn(async (sessionId, generation) => {
        expect(sessionId).toBe("session-1"); events.push("settle"); generations.push(generation); return settlement;
      }),
      readToolResult: vi.fn(async (effect: SettledRecoveryEffect) => {
        events.push("result");
        if (effect.outcome === "not-committed") return nativeResult({
          role: "toolResult", toolCallId: effect.toolCallId, toolName: "write", isError: true, timestamp: 1,
          content: [{ type: "text", text: "Original operation was not committed; destination refused it." }],
        });
        return nativeResult(result);
      }),
    },
  };
  const options = {
    sessionId: "session-1", sessionFile, stateDirectory,
    activateSuccessor: vi.fn(async (validated: PortableSessionBinding) => {
      expect(validated).toEqual(binding); events.push("lease-activation");
    }),
  };
  const context = { sessionManager: manager } as unknown as Parameters<SessionRecoveryAdapters["identity"]["validate"]>[0];
  const prepare = () => prepareSessionRecovery(context, adapters, options);
  const markerFile = (): string => {
    const directory = path.join(stateDirectory, "recovery", "continuations");
    const files = fs.readdirSync(directory);
    expect(files).toHaveLength(1);
    return path.join(directory, files[0]!);
  };
  const marker = (): Record<string, unknown> => JSON.parse(fs.readFileSync(markerFile(), "utf8"));
  const changeInput = (change: Partial<RecoveryAcceptedInput>) => {
    const snapshot = getSnapshot();
    setSnapshot({ ...snapshot, accepted: { ...snapshot.accepted!, ...change } });
  };
  const preserved = () => {
    expect(fs.readFileSync(sessionFile, "utf8")).toBe(raw);
    expect(fs.readFileSync(receiptFile, "utf8")).toBe(receipt);
  };
  return {
    root, sessionFile, stateDirectory, binding, successorMachine, adapters, options, context, prepare, marker, markerFile,
    events, requests, generations, nativeFile, snapshotFile, changeInput, preserved, setSnapshot, getSnapshot, manager,
    result: () => result,
    setSettlement: (value: RecoveryEffectsSettlement) => { settlement = value; },
    setResult: (value: RecoveryToolResult) => { result = value; },
  };
};

describe("startup session recovery coordinator", () => {
  it("hashes the exact materialized native envelope and rejects a B4 hash mismatch before any receipt/admission", async () => {
    const f = fixture(["native-call"]);
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: "native-call", key: "K", outcome: "committed", resultBytesHash: "0".repeat(64) }] });
    await expect(f.prepare()).rejects.toThrow(/hash/i);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    expect(f.requests).toEqual([]);
    f.preserved();
  });

  it.each([false, true])("strictly transforms adapter-original even when its postHook flag is %s, then retains exact hash/source across restart", async postHook => {
    const f = fixture(["native-call"]), original = { ...f.result(), isError: true, content: [{ type: "text" as const, text: "secret original failure" }] };
    const transformed = { ...original, content: [{ type: "text" as const, text: "[redacted] original failure\n\u0000é" }] };
    const originalBytes = JSON.stringify(original);
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: "native-call", key: "K", outcome: "committed", resultBytesHash: resultHash(original) }] });
    vi.mocked(f.adapters.effects.readToolResult).mockResolvedValue({ message: original, source: "adapter-original", postHook });
    const transform = vi.fn(async (message: RecoveryToolResult, effect: SettledRecoveryEffect) => {
      expect(message).toEqual(original); expect(effect.key).toBe("K");
      // A live hook may mutate its argument; it must not own the adapter/raw history object.
      message.content = transformed.content;
      return message;
    });
    Object.assign(f.options, { transformToolResult: transform });
    const prepared = await f.prepare();
    expect(transform).toHaveBeenCalledTimes(1);
    expect(prepared.results).toEqual([transformed]);
    expect(prepared.results[0]?.isError).toBe(true);
    const receipts = [{ toolCallId: "native-call", source: "adapter-original", resultBytesHash: resultHash(transformed) }];
    expect(f.marker()).toMatchObject({ resultReceipts: receipts });
    expect(JSON.stringify(original)).toBe(originalBytes);
    const bytes = fs.readFileSync(f.markerFile(), "utf8");
    const restarted = await f.prepare();
    expect(restarted.results).toEqual(prepared.results);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    await restarted.admitContinuation(() => expect(f.marker().resultReceipts).toEqual(receipts));
    expect(f.requests).toHaveLength(1);
    f.preserved();
  });

  it.each(["missing", "throws", "wrong-call", "wrong-name", "wrong-timestamp", "lost-error", "malformed"] as const)("blocks adapter-original when strict transformation is %s without raw fallback", async fault => {
    const f = fixture(), original = { ...f.result(), isError: true };
    vi.mocked(f.adapters.effects.readToolResult).mockResolvedValue({ message: original, source: "adapter-original", postHook: true });
    const failure = new Error("strict hook refused");
    if (fault !== "missing") Object.assign(f.options, { transformToolResult: async () => {
      if (fault === "throws") throw failure;
      return { ...original, ...(fault === "wrong-call" ? { toolCallId: "other" } :
        fault === "wrong-name" ? { toolName: "other" } : fault === "wrong-timestamp" ? { timestamp: 2 } :
          fault === "lost-error" ? { isError: false } : { content: null }) };
    } });
    if (fault === "throws") await expect(f.prepare()).rejects.toBe(failure);
    else await expect(f.prepare()).rejects.toThrow(/hook|transform|result/i);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    expect(f.requests).toEqual([]); f.preserved();
  });

  it.each(["not-post-hook", "missing-flag", "unknown-source", "raw-message"] as const)("rejects %s result provenance", async fault => {
    const f = fixture();
    const envelope = { message: f.result(), source: "native-entry", postHook: true };
    vi.mocked(f.adapters.effects.readToolResult).mockResolvedValue((fault === "raw-message" ? f.result() :
      { ...envelope, ...(fault === "not-post-hook" ? { postHook: false } :
        fault === "missing-flag" ? { postHook: undefined } : { source: "unknown" }) }) as never);
    await expect(f.prepare()).rejects.toThrow(/result|hook|provenance/i);
    expect(f.requests).toEqual([]);
  });

  it("uses native-entry postHook=true without re-running hooks and records the checked hash before admission", async () => {
    const f = fixture(), transform = vi.fn(async () => { throw new Error("must not rerun native hooks"); });
    Object.assign(f.options, { transformToolResult: transform });
    const prepared = await f.prepare();
    const receipts = prepared.results.map(message => ({ toolCallId: message.toolCallId, source: "native-entry", resultBytesHash: resultHash(message) }));
    expect(f.marker().resultReceipts).toEqual(receipts);
    await prepared.admitContinuation(() => expect(f.marker().resultReceipts).toEqual(receipts));
    expect(transform).not.toHaveBeenCalled();
    f.preserved();
  });

  it.each(["hash", "source"] as const)("rejects changed retained result %s provenance on restart without overwriting the receipt", async fault => {
    const f = fixture(["native-call"]); await f.prepare();
    const bytes = fs.readFileSync(f.markerFile(), "utf8");
    if (fault === "hash") {
      const changed = { ...f.result(), content: [{ type: "text" as const, text: "different durable result" }] };
      f.setResult(changed);
      f.setSettlement({ uncertain: 0, settled: [{ toolCallId: "native-call", key: "K", outcome: "committed", resultBytesHash: resultHash(changed) }] });
    } else {
      vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => ({
        ...(await Promise.resolve(nativeResult({ ...f.result(), toolCallId: effect.toolCallId }))), source: "adapter-original",
      }));
      Object.assign(f.options, { transformToolResult: async (message: RecoveryToolResult) => message });
    }
    await expect(f.prepare()).rejects.toThrow(/receipt|provenance/i);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    expect(f.requests).toEqual([]);
  });

  it("durably unions newly settled calls after native continuation progress under the same generation/text/admission", async () => {
    const f = fixture(["native-call"]), first = f.result();
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: first.toolCallId, key: "K1", outcome: "committed", resultBytesHash: resultHash(first) }] });
    const initial = await f.prepare();
    await initial.admitContinuation(() => repairRecoveryContext(f.manager.buildSessionProjection().messages, initial.results));
    const originalMarker = f.marker();
    f.manager.appendMessage({ role: "user", content: initial.continuation!.text, timestamp: 2 });
    f.manager.appendMessage(originalAssistant(["second-call"]));
    const second = { ...first, toolCallId: "second-call", timestamp: 3, content: [{ type: "text" as const, text: "New distinct settled result" }] };
    const messages = [first, second];
    f.setSettlement({ uncertain: 0, settled: messages.map((message, index) => ({
      toolCallId: message.toolCallId, key: `K${index + 1}`, outcome: "committed", resultBytesHash: resultHash(message),
    })) });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => nativeResult(messages.find(message => message.toolCallId === effect.toolCallId)!));
    const sync = fs.fsyncSync, barriers: string[] = [];
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      barriers.push(fs.fstatSync(fd).isDirectory() ? "directory" : "file"); sync(fd);
    });
    const progressed = await f.prepare();
    expect(progressed.results).toEqual(messages);
    const receipts = messages.map(message => ({ toolCallId: message.toolCallId, source: "native-entry", resultBytesHash: resultHash(message) }));
    expect(f.marker()).toEqual({ ...originalMarker, resultReceipts: receipts });
    expect(barriers).toContain("file");
    if (process.platform !== "win32") expect(barriers).toContain("directory");
    expect(f.requests).toHaveLength(1); // New receipt is already durable BEFORE any second admission.
    const bytes = fs.readFileSync(f.markerFile(), "utf8");
    // Crash again after preparation but before admission: the expanded receipt is stable.
    const restarted = await f.prepare();
    expect(restarted.continuation).toEqual(initial.continuation);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    await expect(restarted.admitContinuation(() => {
      expect(f.marker().resultReceipts).toEqual(receipts);
      expect(repairRecoveryContext(f.manager.buildSessionProjection().messages, restarted.results)
        .filter(message => message.role === "toolResult")).toEqual(messages);
    })).resolves.toEqual({ admissionId: "native-admission-1", existing: true });
    expect(f.requests).toEqual([initial.continuation, initial.continuation]);
    expect(new Set(f.generations).size).toBe(1);
    expect(f.marker()).toEqual({ ...originalMarker, resultReceipts: receipts });
    f.preserved();
  });

  it.each([
    ["branch", false], ["branch", true], ["compaction", false], ["compaction", true],
  ] as const)("retains historical receipts without resurrecting calls after %s (B4 still returns history: %s)", async (view, returned) => {
    const f = fixture(["native-call"]), first = f.result();
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: first.toolCallId, key: "K1", outcome: "committed", resultBytesHash: resultHash(first) }] });
    const initial = await f.prepare(); await initial.admitContinuation(() => {});
    const saved = f.marker();
    if (view === "branch") f.manager.resetLeaf();
    const secondEntry = f.manager.appendMessage(originalAssistant(["second-call"]));
    if (view === "compaction") f.manager.appendCompaction("First result summarized", secondEntry, 100);
    const second = { ...first, toolCallId: "second-call", timestamp: 3 };
    const messages = returned ? [first, second] : [second];
    f.setSettlement({ uncertain: 0, settled: messages.map(message => ({
      toolCallId: message.toolCallId, key: message.toolCallId, outcome: "committed", resultBytesHash: resultHash(message),
    })) });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => nativeResult(messages.find(message => message.toolCallId === effect.toolCallId)!));
    const raw = f.manager.buildSessionProjection().messages, rawBytes = JSON.stringify(raw);
    const prepared = await f.prepare();
    expect(prepared.results).toEqual([second]);
    expect(repairRecoveryContext(raw, prepared.results).filter(message => message.role === "toolResult")).toEqual([second]);
    expect(JSON.stringify(raw)).toBe(rawBytes);
    expect(f.marker()).toEqual({ ...saved, resultReceipts: [
      { toolCallId: first.toolCallId, source: "native-entry", resultBytesHash: resultHash(first) },
      { toolCallId: second.toolCallId, source: "native-entry", resultBytesHash: resultHash(second) },
    ] });
    await expect(prepared.admitContinuation(() => {})).resolves.toEqual({ admissionId: "native-admission-1", existing: true });
    // Neither the native view nor settlement returns either historical call now.
    f.manager.appendCompaction("All calls summarized", null, 100);
    f.setSettlement({ uncertain: 0, settled: [] });
    const bytes = fs.readFileSync(f.markerFile(), "utf8"), empty = await f.prepare();
    expect(empty.results).toEqual([]);
    expect(empty.continuation).toEqual(initial.continuation);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    await expect(empty.admitContinuation(() => {})).resolves.toEqual({ admissionId: "native-admission-1", existing: true });
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    f.preserved();
  });

  it.each(["hash", "source", "timestamp"] as const)("revalidates a previously pinned invisible call's %s before adding any new receipt", async fault => {
    const f = fixture(["native-call"]), original = f.result();
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: original.toolCallId, key: "K1", outcome: "committed", resultBytesHash: resultHash(original) }] });
    await (await f.prepare()).admitContinuation(() => {});
    f.manager.resetLeaf(); f.manager.appendMessage(originalAssistant(["second-call"]));
    const first = { ...original, ...(fault === "hash" ? { content: [] } : fault === "timestamp" ? { timestamp: 99 } : {}) };
    const second = { ...original, toolCallId: "second-call", timestamp: 3 };
    // The new call is deliberately read first: a later historical conflict must not persist it.
    f.setSettlement({ uncertain: 0, settled: [second, first].map(message => ({
      toolCallId: message.toolCallId, key: message.toolCallId, outcome: "committed", resultBytesHash: resultHash(message),
    })) });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => ({
      ...nativeResult(effect.toolCallId === first.toolCallId ? first : second),
      source: fault === "source" && effect.toolCallId === first.toolCallId ? "adapter-original" : "native-entry",
    }));
    Object.assign(f.options, { transformToolResult: async (message: RecoveryToolResult) => message });
    const bytes = fs.readFileSync(f.markerFile(), "utf8");
    await expect(f.prepare()).rejects.toThrow(/receipt|provenance/i);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    expect(f.requests).toHaveLength(1); f.preserved();
  });

  it("accepts consistent same-K physical aliases but materializes only exact calls in the active native branch", async () => {
    const f = fixture(), nativeId = "native_call|fc_original";
    const abandoned = f.manager.getLeafId()!;
    f.manager.appendMessage(originalAssistant(["other-branch"]));
    f.manager.branch(abandoned);
    f.manager.appendMessage(originalAssistant([nativeId]));
    const ids = ["nested-physical", nativeId, "native_call", "other-branch"];
    const messages = ids.map(toolCallId => ({ ...f.result(), toolCallId }));
    expect(new Set(messages.map(resultHash)).size).toBe(ids.length); // Full hashes MUST differ with the alias ID.
    f.setSettlement({ uncertain: 0, settled: messages.map(message => ({
      toolCallId: message.toolCallId, key: "K", outcome: "committed", resultBytesHash: resultHash(message),
    })) });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => nativeResult(messages.find(message => message.toolCallId === effect.toolCallId)!));
    const raw = f.manager.buildSessionProjection().messages, before = JSON.stringify(raw);
    const prepared = await f.prepare();
    expect(prepared.results).toEqual([messages[1]]);
    const repaired = repairRecoveryContext(raw, prepared.results);
    expect(repaired.filter(message => message.role === "toolResult")).toEqual([messages[1]]);
    expect(JSON.stringify(raw)).toBe(before);
    expect(f.adapters.effects.readToolResult).toHaveBeenCalledTimes(4); // Even invisible aliases are checked for consistency.
    expect(f.marker().resultReceipts).toEqual([{ toolCallId: nativeId, source: "native-entry", resultBytesHash: resultHash(messages[1]!) }]);
    f.preserved();
  });

  it.each(["outcome", "content", "details", "error", "tool-name", "nested-id", "nested-timestamp", "extra-field"] as const)("blocks inconsistent same-K aliases (%s), including an invisible nested alias", async fault => {
    const f = fixture(), first = { ...f.result(), details: { id: "saved-effect", timestamp: 10 }, extra: "original" };
    const second = { ...first, toolCallId: "nested-only", timestamp: 2, ...(fault === "content" ? { content: [] } :
      fault === "details" ? { details: { contradictory: true } } : fault === "error" ? { isError: true } :
        fault === "tool-name" ? { toolName: "other-tool" } : fault === "nested-id" ? { details: { ...first.details, id: "changed" } } :
          fault === "nested-timestamp" ? { details: { ...first.details, timestamp: 99 } } : fault === "extra-field" ? { extra: "changed" } : {}) };
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: first.toolCallId, key: "K", outcome: "committed", resultBytesHash: resultHash(first) },
      { toolCallId: second.toolCallId, key: "K", outcome: fault === "outcome" ? "not-committed" : "committed", resultBytesHash: resultHash(second) },
    ] });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => nativeResult(effect.toolCallId === first.toolCallId ? first : second));
    await expect(f.prepare()).rejects.toThrow(/uncertain|conflict/i);
    expect(f.requests).toEqual([]); f.preserved();
  });

  it.each(["native-entry", "adapter-original"] as const)("allows same-K native aliases with distinct original timestamps via %s, retaining each full ID/timestamp hash across restart", async source => {
    const f = fixture(["first|fc_first", "second|fc_second"]);
    const messages = ["first|fc_first", "second|fc_second"].map((toolCallId, index) => ({ ...f.result(), toolCallId, timestamp: index + 1 }));
    f.setSettlement({ uncertain: 0, settled: messages.map(message => ({
      toolCallId: message.toolCallId, key: "K", outcome: "committed", resultBytesHash: resultHash(message),
    })) });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => ({
      ...nativeResult(messages.find(message => message.toolCallId === effect.toolCallId)!), source,
    }));
    const transform = vi.fn(async (message: RecoveryToolResult) => message);
    Object.assign(f.options, { transformToolResult: transform });
    const prepared = await f.prepare();
    expect(prepared.results).toEqual(messages);
    expect(messages[0]!.timestamp).toBe(1); expect(messages[1]!.timestamp).toBe(2);
    expect(new Set(messages.map(resultHash)).size).toBe(2);
    expect(f.marker().resultReceipts).toEqual(messages.map(message => ({ toolCallId: message.toolCallId, source, resultBytesHash: resultHash(message) })));
    expect(repairRecoveryContext(f.manager.buildSessionProjection().messages, prepared.results).filter(message => message.role === "toolResult")).toEqual(messages);
    await prepared.admitContinuation(() => {});
    const bytes = fs.readFileSync(f.markerFile(), "utf8"), restarted = await f.prepare();
    expect(restarted.results).toEqual(messages);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    await expect(restarted.admitContinuation(() => {})).resolves.toEqual({ admissionId: "native-admission-1", existing: true });
    expect(transform).toHaveBeenCalledTimes(source === "adapter-original" ? 4 : 0);
    f.preserved();
  });

  it("still checks a distinct-timestamp alias's complete native-entry hash including ID and timestamp", async () => {
    const f = fixture(["first", "second"]), first = { ...f.result(), toolCallId: "first", timestamp: 1 };
    const second = { ...first, toolCallId: "second", timestamp: 2 };
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: first.toolCallId, key: "K", outcome: "committed", resultBytesHash: resultHash(first) },
      { toolCallId: second.toolCallId, key: "K", outcome: "committed", resultBytesHash: resultHash({ ...second, timestamp: 1 }) },
    ] });
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect => nativeResult(effect.toolCallId === first.toolCallId ? first : second));
    await expect(f.prepare()).rejects.toThrow(/hash mismatch/i);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    expect(f.requests).toEqual([]); f.preserved();
  });

  it("blocks a partially settled native multi-call assistant before writing a result receipt", async () => {
    const f = fixture();
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: "native-call", key: "K", outcome: "committed", resultBytesHash: resultHash(f.result()) }] });
    await expect(f.prepare()).rejects.toThrow(/unsettled-aborted-call/);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    expect(f.requests).toEqual([]); f.preserved();
  });

  it("upgrades a legacy format-1 marker with durable result provenance without changing generation/text/admission", async () => {
    const f = fixture(); await (await f.prepare()).admitContinuation(() => {});
    const legacy = f.marker(); delete legacy.resultReceipts;
    writeJsonAtomic(f.markerFile(), legacy, { durable: true });
    const prepared = await f.prepare();
    expect(f.marker()).toMatchObject(legacy);
    expect(f.marker().resultReceipts).toHaveLength(2);
    await prepared.admitContinuation(() => expect(f.marker().resultReceipts).toHaveLength(2));
    expect(f.requests[1]).toEqual(f.requests[0]);
    f.preserved();
  });

  it("blocks a changed result receipt between preparation and admission before installing context", async () => {
    const f = fixture(), prepared = await f.prepare(), install = vi.fn();
    const marker = f.marker();
    const receipts = marker.resultReceipts as Array<{ resultBytesHash: string }>;
    receipts[0]!.resultBytesHash = "0".repeat(64);
    writeJsonAtomic(f.markerFile(), marker, { durable: true });
    await expect(prepared.admitContinuation(install)).rejects.toThrow(/marker/);
    expect(install).not.toHaveBeenCalled(); expect(f.requests).toEqual([]);
    f.preserved();
  });

  it("detaches and freezes materialized results so later adapter mutations cannot invalidate retained byte hashes", async () => {
    const f = fixture(), original = f.result(), prepared = await f.prepare();
    const bytes = JSON.stringify(prepared.results[0]);
    original.content = [{ type: "text", text: "late mutation" }];
    expect(JSON.stringify(prepared.results[0])).toBe(bytes);
    expect(Object.isFrozen(prepared.results[0])).toBe(true);
    expect(Object.isFrozen(prepared.results[0]!.content)).toBe(true);
    expect(f.marker().resultReceipts).toEqual(prepared.results.map(message => ({ toolCallId: message.toolCallId, source: "native-entry", resultBytesHash: resultHash(message) })));
    f.preserved();
  });

  it("does not revive a partially settled outer assistant by inventing a result for a nested alias", async () => {
    const f = fixture();
    const nested = { ...f.result(), toolCallId: "nested-only" };
    f.setSettlement({ uncertain: 0, settled: [{ toolCallId: nested.toolCallId, key: "K", outcome: "committed", resultBytesHash: resultHash(nested) }] });
    vi.mocked(f.adapters.effects.readToolResult).mockResolvedValue(nativeResult(nested));
    const prepared = await f.prepare();
    expect(prepared.results).toEqual([]);
    expect(repairRecoveryContext(f.manager.buildSessionProjection().messages, prepared.results).filter(message => message.role === "toolResult")).toEqual([]);
    f.preserved();
  });

  it("orders validated identity -> awaited parent lease callback -> complete history -> effects/results -> durable marker -> context -> B5", async () => {
    const f = fixture();
    let release!: () => void;
    f.options.activateSuccessor.mockImplementation(async () => {
      f.events.push("lease-activation"); await new Promise<void>(resolve => { release = resolve; });
    });
    const pending = f.prepare();
    await vi.waitFor(() => expect(release).toBeTypeOf("function"));
    expect(f.events).toEqual(["identity", "lease-activation"]);
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
    release();
    const prepared = await pending;
    expect(prepared.classification).toBe("interrupted");
    expect(prepared.binding).toEqual(f.binding);
    expect(prepared.results).toHaveLength(2);
    expect(prepared.results[0]).toMatchObject({ toolCallId: "native-call", content: [{ type: "text", text: "Original durable committed result" }] });
    expect(prepared.results[1]).toMatchObject({ toolCallId: "unstarted-call", isError: true });
    const before = f.marker();
    expect(before).toMatchObject({ originalInputId: "original-input", recoveryGeneration: f.generations[0] });
    expect(before.admissionId).toBeUndefined();
    const admitted = await prepared.admitContinuation(() => {
      expect(f.marker()).toEqual(before);
      expect(fs.existsSync(f.nativeFile)).toBe(false);
      f.events.push("context-install");
    });
    expect(admitted).toEqual({ admissionId: "native-admission-1", existing: false });
    expect(f.events).toEqual(["identity", "lease-activation", "snapshot", "settle", "result", "result", "snapshot", "context-install", "native-admission"]);
    expect(f.requests[0]).toEqual(prepared.continuation);
    expect(Object.keys(f.requests[0]!).sort()).toEqual(["originalInputId", "recoveryGeneration", "sessionId", "text"]);
    expect(f.marker()).toMatchObject({ admissionId: "native-admission-1" });
    f.preserved();
  });

  it("copies and deeply freezes the validated binding before an awaited activation can mutate the adapter's original object", async () => {
    const f = fixture(), original: PortableSessionBinding = JSON.parse(JSON.stringify(f.binding));
    const redirected = path.join(f.root, "unvalidated-root");
    f.options.activateSuccessor.mockImplementation(async validated => {
      expect(validated).toEqual(original);
      Object.assign(f.binding, { stateDirectory: redirected, sessionFile: path.join(redirected, "wrong.jsonl"), transportMachine: "wrong-transport" });
      Object.assign(f.binding.evidence, { sessionId: "wrong-session", successorBootId: "boot-C" });
      Object.assign(f.binding.evidence.predecessorReceipt, { bytes: "changed receipt" });
      Object.assign(f.binding.evidence.exclusionProof, { detail: "changed exclusion" });
      await Promise.resolve();
    });
    const prepared = await f.prepare();
    expect(prepared.binding).toEqual(original);
    expect(prepared.binding).not.toBe(f.binding);
    expect(Object.isFrozen(prepared.binding)).toBe(true);
    expect(Object.isFrozen(prepared.binding.evidence)).toBe(true);
    expect(Object.isFrozen(prepared.binding.evidence.predecessorReceipt)).toBe(true);
    expect(Object.isFrozen(prepared.binding.evidence.exclusionProof)).toBe(true);
    expect(f.marker()).toMatchObject({ stateDirectory: f.stateDirectory, sessionFile: f.sessionFile, sessionId: "session-1" });
    await prepared.admitContinuation(() => {});
    expect(fs.existsSync(redirected)).toBe(false);
    f.preserved();
  });

  it("materializes committed AND known-not-committed native results for an aborted multi-call assistant through actual provider conversion", async () => {
    const f = fixture(), unexecutedFile = path.join(f.root, "never-replayed.txt");
    const knownFailure: RecoveryToolResult = {
      role: "toolResult", toolCallId: "unstarted-call", toolName: "write", isError: true, timestamp: 2,
      content: [{ type: "text", text: "Original operation was not committed; destination refused it." }],
    };
    const committed: RecoveryToolResult = {
      role: "toolResult", toolCallId: "native-call", toolName: "fabric_exec", isError: false, timestamp: 2,
      content: [{ type: "text", text: "Original durable committed result" }],
    };
    vi.mocked(f.adapters.effects.readToolResult).mockImplementation(async effect =>
      nativeResult(effect.outcome === "not-committed" ? knownFailure : committed));
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: "native-call", key: "saved-effect-1", outcome: "committed", resultBytesHash: resultHash(committed) },
      { toolCallId: "unstarted-call", key: "saved-effect-2", outcome: "not-committed" },
    ] });
    const original: AssistantMessage = {
      role: "assistant", api: "openai-responses", provider: "openai", model: "recovery-test", timestamp: 1,
      stopReason: "aborted", errorMessage: "original interruption",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      content: [
        { type: "toolCall", id: "native-call", name: "fabric_exec", arguments: { code: "throw new Error('must never replay original program')" } },
        { type: "toolCall", id: "unstarted-call", name: "write", arguments: { path: unexecutedFile, content: "must not be recreated" } },
      ],
    };
    const raw: ContextEvent["messages"] = [{ role: "user", content: "original input", timestamp: 0 }, original];
    const rawBytes = JSON.stringify(raw), prepared = await f.prepare();
    expect(prepared.results).toEqual([committed, knownFailure]);
    expect(prepared.results[1]).toEqual(knownFailure);
    await prepared.admitContinuation(() => {
      const repaired = repairRecoveryContext(raw, prepared.results);
      expect(repaired.filter(message => message.role === "toolResult")).toEqual([committed, knownFailure]);
      expect(repaired.find(message => message.role === "toolResult" && message.toolCallId === "unstarted-call")).toMatchObject({ isError: true });
      const model: Model<"openai-responses"> = {
        id: "recovery-test", name: "Recovery test", api: "openai-responses", provider: "openai",
        baseUrl: "https://unused.invalid", reasoning: false, input: ["text", "image"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32000, maxTokens: 1024,
      };
      const converted = convertResponsesMessages(model, normalizeContext({ messages: convertToLlm(repaired) }), new Set(["openai"]));
      expect(converted.filter(item => item.type === "function_call")).toHaveLength(2);
      expect(converted.filter(item => item.type === "function_call_output")).toEqual([
        { type: "function_call_output", call_id: "native-call", output: "Original durable committed result" },
        { type: "function_call_output", call_id: "unstarted-call", output: "Original operation was not committed; destination refused it." },
      ]);
    });
    expect(original.stopReason).toBe("aborted");
    expect(JSON.stringify(raw)).toBe(rawBytes);
    expect(fs.existsSync(unexecutedFile)).toBe(false);
    expect(f.adapters.effects.settlePendingIntents).toHaveBeenCalledTimes(1);
    expect(f.adapters.effects.readToolResult).toHaveBeenCalledTimes(2);
    f.preserved();
  });

  it("flushes the marker file and its directory before allowing B5 admission", async () => {
    const f = fixture(), sync = fs.fsyncSync, barriers: string[] = [];
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      barriers.push(fs.fstatSync(fd).isDirectory() ? "directory" : "file");
      sync(fd);
    });
    const prepared = await f.prepare();
    expect(barriers).toContain("file");
    // The shared helper documents that Windows cannot confirm directory fsync.
    // This Linux/block-volume recovery profile makes no Windows durability claim.
    if (process.platform !== "win32") expect(barriers).toContain("directory");
    expect(f.requests).toEqual([]);
    await prepared.admitContinuation(() => {
      expect(f.marker().recoveryGeneration).toBe(prepared.continuation?.recoveryGeneration);
    });
  });

  it("reuses the durable marker's original text rather than regenerating retry text", async () => {
    const f = fixture(); await f.prepare();
    const marker: Record<string, unknown> = { ...f.marker(), text: "Previously persisted continuation text; do not regenerate." };
    writeJsonAtomic(f.markerFile(), marker, { durable: true });
    const prepared = await f.prepare();
    expect(prepared.continuation?.text).toBe(marker.text);
    await prepared.admitContinuation(() => {});
    expect(f.requests[0]?.text).toBe(marker.text);
    expect(f.requests[0]?.recoveryGeneration).toBe(marker.recoveryGeneration);
  });

  it("keeps path-like original input ids inside the separate hashed marker directory", async () => {
    const f = fixture(); f.changeInput({ inputId: "../../session.jsonl" });
    const prepared = await f.prepare();
    expect(path.basename(f.markerFile())).toMatch(/^[a-f0-9]{64}\.json$/);
    expect(prepared.continuation?.originalInputId).toBe("../../session.jsonl");
    f.preserved();
  });

  it("survives an injected crash AFTER native enqueue and BEFORE B3 receives the admission receipt", async () => {
    const f = fixture(), native = f.adapters.inputs.admitContinuation;
    vi.mocked(f.adapters.inputs.admitContinuation).mockImplementationOnce(async request => {
      await native(request); // The real fixture admission is already on disk; B3 has no returned id.
      expect(f.marker().admissionId).toBeUndefined();
      throw new Error("CRASH: native accepted, caller receipt not delivered");
    });
    const first = await f.prepare();
    await expect(first.admitContinuation(() => {})).rejects.toThrow("CRASH:");
    const bytes = fs.readFileSync(f.markerFile(), "utf8"), saved = f.marker();
    // A third physical machine/boot mounts the SAME retained state. This is a fixture,
    // not evidence that a real old writer was fenced or that a lease was acquired.
    expect(f.successorMachine.machineId).toBe("machine-B");
    f.successorMachine.machineId = "machine-C";
    Object.assign(f.binding.evidence, { predecessorPodId: "pod-B", predecessorBootId: "boot-B", successorPodId: "pod-C", successorBootId: "boot-C" });
    const second = await f.prepare();
    expect(second.binding.stateDirectory).toBe(first.binding.stateDirectory);
    expect(second.binding.transportMachine).toBe("portable-session-machine");
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    expect(second.continuation).toEqual(first.continuation);
    expect(second.continuation?.recoveryGeneration).toBe(saved.recoveryGeneration);
    await expect(second.admitContinuation(() => {})).resolves.toEqual({ admissionId: "native-admission-1", existing: true });
    expect(f.requests).toHaveLength(2);
    expect(f.requests[0]).toEqual(f.requests[1]);
    expect(new Set(f.generations).size).toBe(1);
    expect(JSON.parse(fs.readFileSync(f.nativeFile, "utf8")).admissionId).toBe("native-admission-1");
    f.preserved();
  });

  it("coalesces repeated admission calls and never repeats the context-install callback", async () => {
    const f = fixture(), prepared = await f.prepare(), install = vi.fn();
    const first = prepared.admitContinuation(install), second = prepared.admitContinuation(install);
    expect(await second).toEqual(await first);
    expect(install).toHaveBeenCalledTimes(1);
    expect(f.requests).toHaveLength(1);
  });

  it("reconciles a saved admission id but refuses a changed native admission id without overwriting its marker", async () => {
    const f = fixture();
    await (await f.prepare()).admitContinuation(() => {});
    const bytes = fs.readFileSync(f.markerFile(), "utf8");
    vi.mocked(f.adapters.inputs.admitContinuation).mockResolvedValue({ admissionId: "conflicting-native-id", existing: true });
    await expect((await f.prepare()).admitContinuation(() => {})).rejects.toThrow(/admission.*conflict/i);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    f.preserved();
  });

  it.each(["completed", "cancelled", "unchanged-boot", "clean"] as const)("settles issued intents but does not materialize, create a marker or admit %s history", async classification => {
    const f = fixture();
    if (classification === "clean") f.setSnapshot({ sessionId: "session-1", complete: true });
    if (classification === "completed") f.changeInput({ settlement: "completed" });
    if (classification === "cancelled") f.changeInput({ cancellation: "cancelled" });
    if (classification === "unchanged-boot") f.changeInput({ podId: "pod-B", bootId: "boot-B" });
    const prepared = await f.prepare();
    expect(prepared.classification).toBe(classification);
    expect(prepared.results).toEqual([]);
    expect(prepared.continuation).toBeUndefined();
    const install = vi.fn();
    expect(await prepared.admitContinuation(install)).toBeUndefined();
    expect(install).not.toHaveBeenCalled();
    expect(f.adapters.effects.settlePendingIntents).toHaveBeenCalledTimes(1);
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    f.preserved();
  });

  it.each(["cancelled", "completed", "unchanged-boot", "clean"] as const)("records each issued-intent outcome for %s history without restarting any slot or continuing", async classification => {
    for (const outcome of ["committed", "not-committed", "uncertain"] as const) {
      const f = fixture(["native-call"]);
      if (classification === "cancelled") f.changeInput({ cancellation: "cancelled" });
      if (classification === "completed") f.changeInput({ settlement: "completed" });
      if (classification === "unchanged-boot") f.changeInput({ podId: "pod-B", bootId: "boot-B" });
      if (classification === "clean") f.setSnapshot({ sessionId: "session-1", complete: true });
      // A retained B4 double owns one ALREADY-issued slot; settlement may record its
      // verdict but must not create/restart a slot. This does not qualify actual B4 IO.
      const issuedFile = path.join(f.stateDirectory, "issued-intents.json");
      const issued = { slots: [{ toolCallId: "native-call", key: "issued-K", status: "issued" }] };
      writeJsonAtomic(issuedFile, issued, { durable: true });
      vi.mocked(f.adapters.effects.settlePendingIntents).mockImplementation(async (sessionId, generation) => {
        expect(sessionId).toBe("session-1"); expect(generation).toMatch(/^[a-f0-9-]{36}$/);
        const retained = JSON.parse(fs.readFileSync(issuedFile, "utf8"));
        expect(retained).toEqual(issued);
        writeJsonAtomic(issuedFile, { slots: [{ ...retained.slots[0], status: outcome }] }, { durable: true });
        return { uncertain: outcome === "uncertain" ? 1 : 0, settled: [{
          toolCallId: "native-call", key: "issued-K", outcome,
          ...(outcome === "committed" ? { resultBytesHash: resultHash(f.result()) } : {}),
        }] };
      });
      const projection = vi.spyOn(f.manager, "buildSessionProjection").mockImplementation(() => {
        throw new Error("Non-interrupted settlement must not materialize a provider continuation");
      });
      const entries = JSON.stringify(f.manager.getEntries()), install = vi.fn();
      if (outcome === "uncertain") await expect(f.prepare()).rejects.toThrow(/uncertain/i);
      else {
        const prepared = await f.prepare();
        expect(prepared.classification).toBe(classification);
        expect(prepared.results).toEqual([]); expect(prepared.continuation).toBeUndefined();
        await expect(prepared.admitContinuation(install)).resolves.toBeUndefined();
      }
      expect(JSON.parse(fs.readFileSync(issuedFile, "utf8"))).toEqual({
        slots: [{ ...issued.slots[0], status: outcome }],
      });
      expect(f.adapters.effects.settlePendingIntents).toHaveBeenCalledTimes(1);
      expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
      expect(projection).not.toHaveBeenCalled(); expect(install).not.toHaveBeenCalled();
      expect(f.adapters.inputs.admitContinuation).not.toHaveBeenCalled();
      expect(JSON.stringify(f.manager.getEntries())).toBe(entries);
      expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
      f.preserved(); projection.mockRestore();
    }
  });

  it("settles a cancelled input using its retained generation without modifying the existing result/admission marker", async () => {
    const f = fixture(); await (await f.prepare()).admitContinuation(() => {});
    const bytes = fs.readFileSync(f.markerFile(), "utf8"), generation = f.marker().recoveryGeneration;
    f.changeInput({ cancellation: "cancelled" });
    vi.mocked(f.adapters.effects.settlePendingIntents).mockClear();
    vi.mocked(f.adapters.effects.readToolResult).mockClear();
    const cancelled = await f.prepare();
    expect(f.adapters.effects.settlePendingIntents).toHaveBeenCalledExactlyOnceWith("session-1", generation);
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(cancelled.results).toEqual([]); expect(cancelled.continuation).toBeUndefined();
    expect(await cancelled.admitContinuation(() => { throw new Error("must not install"); })).toBeUndefined();
    expect(f.requests).toHaveLength(1);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes); f.preserved();
  });

  it("durable cancellation takes precedence over completed settlement", async () => {
    const f = fixture(); f.changeInput({ settlement: "completed", cancellation: "cancelled" });
    expect((await f.prepare()).classification).toBe("cancelled");
    expect(f.requests).toEqual([]);
  });

  it.each(["cancelled", "completed"] as const)("rereads history immediately before admission and observes a racing %s", async state => {
    const f = fixture(), prepared = await f.prepare(), install = vi.fn();
    const saved = fs.readFileSync(f.markerFile(), "utf8");
    if (state === "cancelled") f.changeInput({ cancellation: "cancelled" });
    else f.changeInput({ settlement: "completed" });
    expect(await prepared.admitContinuation(install)).toBeUndefined();
    expect(install).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(saved);
    f.preserved();
  });

  it.each(["incomplete", "wrong-session", "unknown-cancel", "missing-cancel", "bad-settlement", "bad-accepted"] as const)("fails closed on %s history before effects", async fault => {
    const f = fixture();
    if (fault === "incomplete") f.setSnapshot({ ...f.getSnapshot(), complete: false });
    if (fault === "wrong-session") f.setSnapshot({ ...f.getSnapshot(), sessionId: "other" });
    if (fault === "unknown-cancel") f.changeInput({ cancellation: "unknown" });
    if (fault === "missing-cancel") f.changeInput({ cancellation: undefined } as unknown as Partial<RecoveryAcceptedInput>);
    if (fault === "bad-settlement") f.changeInput({ settlement: "not-a-native-state" } as unknown as Partial<RecoveryAcceptedInput>);
    if (fault === "bad-accepted") f.setSnapshot({ ...f.getSnapshot(), accepted: null } as unknown as RecoveryInputSnapshot);
    await expect(f.prepare()).rejects.toThrow();
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    f.preserved();
  });

  it("blocks unreadable/corrupt persisted input history instead of interpreting it as no cancellation", async () => {
    const f = fixture(); fs.writeFileSync(f.snapshotFile, '{"partial":');
    await expect(f.prepare()).rejects.toThrow();
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    f.preserved();
  });

  it.each(["unknown-cancel", "incomplete", "changed-input", "changed-text"] as const)("does not install context or admit if the final reread becomes %s", async fault => {
    const f = fixture(), prepared = await f.prepare(), install = vi.fn();
    if (fault === "unknown-cancel") f.changeInput({ cancellation: "unknown" });
    if (fault === "incomplete") f.setSnapshot({ ...f.getSnapshot(), complete: false });
    if (fault === "changed-input") f.changeInput({ inputId: "different-input" });
    if (fault === "changed-text") f.changeInput({ text: "changed original immutable input" });
    await expect(prepared.admitContinuation(install)).rejects.toThrow();
    expect(install).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    f.preserved();
  });

  it.each(["sessionId", "sessionFile", "stateDirectory", "validator-refused", "lease-refused"] as const)("does not read history/effects or mutate files on %s identity/lease failure", async fault => {
    const f = fixture();
    if (fault === "sessionId") Object.assign(f.binding.evidence, { sessionId: "wrong" });
    if (fault === "sessionFile") Object.assign(f.binding, { sessionFile: `${f.sessionFile}.wrong` });
    if (fault === "stateDirectory") Object.assign(f.binding, { stateDirectory: `${f.stateDirectory}.wrong` });
    if (fault === "validator-refused") vi.mocked(f.adapters.identity.validate).mockRejectedValue(new Error("old writer still has access"));
    if (fault === "lease-refused") f.options.activateSuccessor.mockRejectedValue(new Error("actual successor lease not acquired"));
    await expect(f.prepare()).rejects.toThrow();
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(f.adapters.inputs.admitContinuation).not.toHaveBeenCalled();
    if (fault !== "lease-refused") expect(f.options.activateSuccessor).not.toHaveBeenCalled();
    expect(fs.existsSync(f.nativeFile)).toBe(false);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    f.preserved();
  });

  it("uses B2's authoritative absolute REQUIRED root when the parent has no independently known state directory", async () => {
    const f = fixture(), { stateDirectory: _expected, ...options } = f.options;
    const prepared = await prepareSessionRecovery(f.context, f.adapters, options);
    expect(prepared.binding.stateDirectory).toBe(f.stateDirectory);
    expect(f.marker().stateDirectory).toBe(f.stateDirectory);
  });

  it("refuses a non-absolute B2 state root even when the parent has no expected root", async () => {
    const f = fixture(), { stateDirectory: _expected, ...options } = f.options;
    Object.assign(f.binding, { stateDirectory: "relative-root" });
    await expect(prepareSessionRecovery(f.context, f.adapters, options)).rejects.toThrow(/identity/i);
    expect(f.options.activateSuccessor).not.toHaveBeenCalled();
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
  });

  it.each(["lease-absent", "harness-asserted"] as const)("refuses %s evidence before activation/history/effects/admission or mutations even when B2 and native activation would succeed", async kind => {
    const f = fixture();
    // Runtime injection keeps this RED independent of the current production evidence union.
    // Lease expiry plus a harness stop assertion cannot substitute for B1's fence receipt.
    Object.assign(f.binding.evidence.exclusionProof, { kind, detail: "lease expired + harness record confirms old writer stopped" });
    const history = fs.readFileSync(f.snapshotFile, "utf8"), entries = JSON.stringify(f.manager.getEntries());
    const files = fs.readdirSync(f.stateDirectory).sort();
    const projection = vi.spyOn(f.manager, "buildSessionProjection");
    await expect(f.prepare()).rejects.toThrow(/exclusion|evidence|B1|fence/i);
    expect(f.adapters.identity.validate).toHaveBeenCalledExactlyOnceWith(f.context);
    expect(f.events).toEqual(["identity"]);
    expect(f.options.activateSuccessor).not.toHaveBeenCalled();
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(f.adapters.inputs.admitContinuation).not.toHaveBeenCalled();
    expect(projection).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    expect(fs.existsSync(f.nativeFile)).toBe(false);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    expect(fs.readdirSync(f.stateDirectory).sort()).toEqual(files);
    expect(fs.readFileSync(f.snapshotFile, "utf8")).toBe(history);
    expect(JSON.stringify(f.manager.getEntries())).toBe(entries);
    f.preserved();
  });

  it("allows the b1-fence receipt discriminator only through the same B2 identity and mandatory native activation", async () => {
    const f = fixture();
    // Proposed sole production kind; this DTO string does not authenticate a controller.
    // B2 and real native activation still own receipt validation and exclusion authority.
    Object.assign(f.binding.evidence.exclusionProof, { kind: "b1-fence" });
    const prepared = await f.prepare();
    expect(f.adapters.identity.validate).toHaveBeenCalledExactlyOnceWith(f.context);
    expect(f.options.activateSuccessor).toHaveBeenCalledExactlyOnceWith(prepared.binding);
    expect(prepared.binding).toEqual(f.binding);
    expect(prepared.classification).toBe("interrupted");
    expect(f.events).toEqual(["identity", "lease-activation", "snapshot", "settle", "result", "result"]);
    expect(f.marker().admissionId).toBeUndefined();
    expect(f.adapters.inputs.admitContinuation).not.toHaveBeenCalled();
    await expect(prepared.admitContinuation(() => { f.events.push("context-install"); }))
      .resolves.toEqual({ admissionId: "native-admission-1", existing: false });
    expect(f.events.slice(-3)).toEqual(["snapshot", "context-install", "native-admission"]);
    expect(f.requests).toEqual([prepared.continuation]);
    expect(f.marker()).toMatchObject({ admissionId: "native-admission-1" });
    f.preserved();
  });

  it("refuses b1-fence evidence before history/effects/admission or mutations when native activation is absent", async () => {
    const f = fixture();
    Object.assign(f.binding.evidence.exclusionProof, { kind: "b1-fence" });
    const history = fs.readFileSync(f.snapshotFile, "utf8");
    await expect(prepareSessionRecovery(f.context, f.adapters, {
      ...f.options, activateSuccessor: undefined,
    } as never)).rejects.toThrow(/activation/i);
    expect(f.adapters.identity.validate).toHaveBeenCalledExactlyOnceWith(f.context);
    expect(f.events).toEqual(["identity"]);
    expect(f.options.activateSuccessor).not.toHaveBeenCalled();
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(f.adapters.inputs.admitContinuation).not.toHaveBeenCalled();
    expect(fs.existsSync(f.nativeFile)).toBe(false);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    expect(fs.readFileSync(f.snapshotFile, "utf8")).toBe(history);
    f.preserved();
  });

  it.each(["uncertain-count", "uncertain-effect", "malformed", "missing-result-hash", "duplicate-key"] as const)("blocks continuation on %s settlement", async fault => {
    const f = fixture();
    const committed = { toolCallId: "native-call", key: "saved-effect-1", outcome: "committed" as const, resultBytesHash: "b".repeat(64) };
    if (fault === "uncertain-count") f.setSettlement({ uncertain: 1, settled: [committed] });
    if (fault === "uncertain-effect") f.setSettlement({ uncertain: 0, settled: [{ ...committed, outcome: "uncertain" }] });
    if (fault === "malformed") f.setSettlement({ uncertain: -1, settled: [] });
    if (fault === "missing-result-hash") f.setSettlement({ uncertain: 0, settled: [{ toolCallId: "native-call", key: "key", outcome: "committed" }] });
    if (fault === "duplicate-key") f.setSettlement({ uncertain: 0, settled: [committed, committed] });
    await expect(f.prepare()).rejects.toThrow();
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    f.preserved();
  });

  it("returns exactly one truthful native result for multiple committed nested effects, not a replayed program", async () => {
    const f = fixture(["native-call"]);
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: "native-call", key: "nested-1", outcome: "committed", resultBytesHash: resultHash(f.result()) },
      { toolCallId: "native-call", key: "nested-2", outcome: "committed", resultBytesHash: resultHash(f.result()) },
    ] });
    const prepared = await f.prepare();
    expect(prepared.results).toHaveLength(1);
    expect(f.adapters.effects.readToolResult).toHaveBeenCalledTimes(2);
    await prepared.admitContinuation(() => {});
    expect(f.requests).toHaveLength(1);
    f.preserved();
  });

  it("blocks an original native result with the wrong toolCallId", async () => {
    const f = fixture();
    f.setResult({ role: "toolResult", toolCallId: "regenerated-id", toolName: "fabric_exec", content: [], isError: false, timestamp: 1 });
    await expect(f.prepare()).rejects.toThrow(/result/i);
    expect(f.requests).toEqual([]);
  });

  it("blocks conflicting truthful-result claims for the same original native call", async () => {
    const f = fixture();
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: "native-call", key: "nested-1", outcome: "committed", resultBytesHash: "b".repeat(64) },
      { toolCallId: "native-call", key: "nested-2", outcome: "committed", resultBytesHash: "c".repeat(64) },
    ] });
    const conflicting = { ...f.result(), content: [] };
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: "native-call", key: "nested-1", outcome: "committed", resultBytesHash: resultHash(conflicting) },
      { toolCallId: "native-call", key: "nested-2", outcome: "committed", resultBytesHash: resultHash(f.result()) },
    ] });
    vi.mocked(f.adapters.effects.readToolResult).mockResolvedValueOnce(nativeResult(conflicting));
    await expect(f.prepare()).rejects.toThrow(/result.*conflict/i);
    expect(f.requests).toEqual([]);
  });

  it("refuses a marker corrupted between preparation and admission before context installation", async () => {
    const f = fixture(), prepared = await f.prepare(), install = vi.fn();
    fs.writeFileSync(f.markerFile(), '{"partial":');
    await expect(prepared.admitContinuation(install)).rejects.toThrow(/marker/i);
    expect(install).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe('{"partial":');
    f.preserved();
  });

  it.each(["invalid-json", "missing-generation", "wrong-input", "wrong-session-file", "bad-admission-id", "bad-result-receipt", "duplicate-result-receipt"] as const)("fails a %s durable marker without repairing/overwriting it", async fault => {
    const f = fixture(); await f.prepare();
    const marker = f.marker();
    if (fault === "missing-generation") delete marker.recoveryGeneration;
    if (fault === "wrong-input") marker.originalInputId = "another-input";
    if (fault === "wrong-session-file") marker.sessionFile = "another-file";
    if (fault === "bad-admission-id") marker.admissionId = "";
    if (fault === "bad-result-receipt") marker.resultReceipts = [{ toolCallId: "native-call", source: "raw", resultBytesHash: "0".repeat(64) }];
    if (fault === "duplicate-result-receipt") marker.resultReceipts = [
      { toolCallId: "native-call", source: "native-entry", resultBytesHash: "0".repeat(64) },
      { toolCallId: "native-call", source: "native-entry", resultBytesHash: "0".repeat(64) },
    ];
    const bytes = fault === "invalid-json" ? '{"partial":' : JSON.stringify(marker);
    fs.writeFileSync(f.markerFile(), bytes);
    vi.mocked(f.adapters.effects.settlePendingIntents).mockClear();
    await expect(f.prepare()).rejects.toThrow(/marker/i);
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(fs.readFileSync(f.markerFile(), "utf8")).toBe(bytes);
    expect(f.requests).toEqual([]);
    f.preserved();
  });

  it.each([{ admissionId: "", existing: true }, { admissionId: "id", existing: "unknown" }])("refuses malformed B5 admission %j", async admission => {
    const f = fixture(), prepared = await f.prepare();
    vi.mocked(f.adapters.inputs.admitContinuation).mockResolvedValue(admission as never);
    await expect(prepared.admitContinuation(() => {})).rejects.toThrow(/admission/i);
    expect(f.marker().admissionId).toBeUndefined();
  });

  it("never calls B5 if synchronous provider context installation throws", async () => {
    const f = fixture(), prepared = await f.prepare();
    await expect(prepared.admitContinuation(() => { throw new Error("context not installed"); })).rejects.toThrow("context not installed");
    expect(f.requests).toEqual([]);
    expect(f.marker().admissionId).toBeUndefined();
  });

  it("refuses asynchronous context installation rather than starting a turn before repair", async () => {
    const f = fixture(), prepared = await f.prepare();
    await expect(prepared.admitContinuation(async () => {})).rejects.toThrow(/synchronous/i);
    expect(f.requests).toEqual([]);
  });

  it("settled effects with no committed results still admit one continuation, without executing any effect", async () => {
    const f = fixture(); f.setSettlement({ settled: [], uncertain: 0 });
    const prepared = await f.prepare();
    expect(prepared.results).toEqual([]);
    await prepared.admitContinuation(() => {});
    expect(f.adapters.effects.readToolResult).not.toHaveBeenCalled();
    expect(f.requests).toHaveLength(1);
    f.preserved();
  });
});
