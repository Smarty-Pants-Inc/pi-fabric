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
import { convertToLlm, type ContextEvent } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { prepareSessionRecovery } from "../src/recovery/coordinator.js";
import { repairRecoveryContext } from "../src/recovery/context-repair.js";
import type {
  ContinuationRequest, PortableSessionBinding, RecoveryAcceptedInput, RecoveryEffectsSettlement,
  RecoveryInputSnapshot, RecoveryToolResult, SessionRecoveryAdapters, SettledRecoveryEffect,
} from "../src/recovery/l7-interfaces.js";

const fixture = () => {
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
      exclusionProof: { kind: "volume-detach", observedAt: 1, detail: "native-driver-receipt" },
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
  const events: string[] = [], requests: ContinuationRequest[] = [], generations: string[] = [];
  const nativeFile = path.join(stateDirectory, "native-admissions.json");
  const adapters: SessionRecoveryAdapters = {
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
      readToolResult: vi.fn(async (effect: SettledRecoveryEffect): Promise<RecoveryToolResult> => {
        events.push("result");
        if (effect.outcome === "not-committed") return {
          role: "toolResult", toolCallId: effect.toolCallId, toolName: "write", isError: true, timestamp: 1,
          content: [{ type: "text", text: "Original operation was not committed; destination refused it." }],
        };
        return result;
      }),
    },
  };
  const options = {
    sessionId: "session-1", sessionFile, stateDirectory,
    activateSuccessor: vi.fn(async (validated: PortableSessionBinding) => {
      expect(validated).toEqual(binding); events.push("lease-activation");
    }),
  };
  const context = {} as Parameters<SessionRecoveryAdapters["identity"]["validate"]>[0];
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
    events, requests, generations, nativeFile, snapshotFile, changeInput, preserved, setSnapshot, getSnapshot,
    setSettlement: (value: RecoveryEffectsSettlement) => { settlement = value; },
    setResult: (value: RecoveryToolResult) => { result = value; },
  };
};

describe("startup session recovery coordinator", () => {
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
      effect.outcome === "not-committed" ? knownFailure : committed);
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
    expect(prepared.results[1]).toBe(knownFailure);
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

  it.each(["completed", "cancelled", "unchanged-boot", "clean"] as const)("does not settle, create a marker or admit %s history", async classification => {
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
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    expect(f.requests).toEqual([]);
    expect(fs.existsSync(path.join(f.stateDirectory, "recovery"))).toBe(false);
    f.preserved();
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

  it.each(["sessionId", "sessionFile", "stateDirectory", "lease-absent-unproven", "validator-refused", "lease-refused"] as const)("does not read history/effects or mutate files on %s identity/lease failure", async fault => {
    const f = fixture();
    if (fault === "sessionId") Object.assign(f.binding.evidence, { sessionId: "wrong" });
    if (fault === "sessionFile") Object.assign(f.binding, { sessionFile: `${f.sessionFile}.wrong` });
    if (fault === "stateDirectory") Object.assign(f.binding, { stateDirectory: `${f.stateDirectory}.wrong` });
    if (fault === "lease-absent-unproven") {
      Object.assign(f.binding.evidence.exclusionProof, { kind: "lease-absent" });
      f.options.activateSuccessor.mockRejectedValue(new Error("B2 native physical exclusion chain unresolved"));
    }
    if (fault === "validator-refused") vi.mocked(f.adapters.identity.validate).mockRejectedValue(new Error("old writer still has access"));
    if (fault === "lease-refused") f.options.activateSuccessor.mockRejectedValue(new Error("actual successor lease not acquired"));
    await expect(f.prepare()).rejects.toThrow();
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
    expect(f.adapters.effects.settlePendingIntents).not.toHaveBeenCalled();
    if (fault !== "lease-refused" && fault !== "lease-absent-unproven") expect(f.options.activateSuccessor).not.toHaveBeenCalled();
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

  it("consumes the approved lease-absent evidence discriminator only through B2 and the mandatory native activation callback", async () => {
    const f = fixture();
    Object.assign(f.binding.evidence.exclusionProof, { kind: "lease-absent" });
    // This double checks choreography only. The real B2/lease integration must validate
    // physical exclusion; a descriptive lease-absent record itself is never that proof.
    const prepared = await f.prepare();
    expect(prepared.classification).toBe("interrupted");
    expect(f.events.slice(0, 3)).toEqual(["identity", "lease-activation", "snapshot"]);
  });

  it("never treats lease-absent evidence as sufficient when no native activation callback is provided", async () => {
    const f = fixture();
    Object.assign(f.binding.evidence.exclusionProof, { kind: "lease-absent" });
    await expect(prepareSessionRecovery(f.context, f.adapters, {
      ...f.options, activateSuccessor: undefined,
    } as never)).rejects.toThrow(/activation/i);
    expect(f.adapters.inputs.readRecoveryInput).not.toHaveBeenCalled();
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
    const f = fixture();
    f.setSettlement({ uncertain: 0, settled: [
      { toolCallId: "native-call", key: "nested-1", outcome: "committed", resultBytesHash: "b".repeat(64) },
      { toolCallId: "native-call", key: "nested-2", outcome: "committed", resultBytesHash: "b".repeat(64) },
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
    vi.mocked(f.adapters.effects.readToolResult).mockResolvedValueOnce({ role: "toolResult", toolCallId: "native-call", toolName: "fabric_exec", content: [], isError: false, timestamp: 2 });
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

  it.each(["invalid-json", "missing-generation", "wrong-input", "wrong-session-file", "bad-admission-id"] as const)("fails a %s durable marker without repairing/overwriting it", async fault => {
    const f = fixture(); await f.prepare();
    const marker = f.marker();
    if (fault === "missing-generation") delete marker.recoveryGeneration;
    if (fault === "wrong-input") marker.originalInputId = "another-input";
    if (fault === "wrong-session-file") marker.sessionFile = "another-file";
    if (fault === "bad-admission-id") marker.admissionId = "";
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
