import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { EffectStore } from "../src/effects/store.js";
import { createCertifiedEffectRecordPort } from "../src/effects/extension.js";
import { createMaintenanceRepairEngine, createMaintenanceRepairHost } from "../src/effects/maintenance-repair-host.js";
import type { BoundRepairOriginal, RepairNativeEntry, RepairNativeReader, RepairNativeScan } from "../src/effects/maintenance-repair.js";
import type { EffectIntent, EffectOutcome, EffectRecord, EffectToolResult, RecoveryOwner } from "../src/effects/contracts.js";
import type { NativeAcceptedRecoveryInput, RepairRef } from "../src/effects/repair-contracts.js";
import type { RecoveryDurableEntryResult } from "../src/recovery/l7-interfaces.js";
import { canonicalEncode, manifestDigest, mintEffectKey } from "../src/effects/slots.js";
import { admitBoundScriptedInput, admitContinuation, bindInputAdmission, commitRecoveryInputDelivery, ensureRecoveryGeneration,
  initializeNewRecoverySession, readRecoverySession, recordInputOutcome, recordRecoveryRepair,
  releaseRecoveryInputAfterJoin, selectRecoveryInput } from "../src/recovery/input-admission.js";
import type { AcceptedRecoveryInput, RecoveryAdmissionOwner } from "../src/recovery/input-admission.js";

// Keep reason: native/B6/B5 trust boundary and data safety. These tests exercise
// real B4 retention/qualification/resolution and actual B5 state transitions.
// Only the two labelled native/B6 boundaries below are substituted. No G2 claim.
const H = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const REQUIRED = { code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" };
const gate = () => {
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>(resolve => { enter = resolve; });
  const waiting = new Promise<void>(resolve => { release = resolve; });
  return { entered, release, async wait() { enter(); await waiting; } };
};
type NativeMessageEntry = RepairNativeEntry & { timestamp: string; message?: unknown };
interface Original {
  readonly selected: AcceptedRecoveryInput;
  readonly input: NativeAcceptedRecoveryInput;
  readonly generation: string | undefined;
  readonly carrier: string;
  released: boolean;
}

async function fixture() {
  // Every destructive test operation is confined to THIS allocated temp root.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "maintenance-repair-"));
  const sessionRoot = path.join(root, "session"), workspaceRoot = path.join(root, "workspace");
  fs.mkdirSync(sessionRoot, { mode: 0o700 }); fs.mkdirSync(workspaceRoot, { mode: 0o700 });
  const sessionId = `repair-${randomUUID()}`, file = path.join(root, "native.jsonl");
  const deadline = Date.now() + 600_000;
  const all: NativeMessageEntry[] = [];
  const committed = new Map<string, { seq: number; bytes: Uint8Array }>();
  const nativeResults = new Map<string, EffectToolResult>();
  const outcomes = new Map<string, EffectOutcome>();
  const sinks: string[] = [], drives: number[] = [], validators: RepairRef[] = [];
  let leaf: string | null = null, epoch = 0, initialized = false, nextSeq = 11;
  let beforeRead: (() => Promise<void>) | undefined;
  let readFailure: RecoveryDurableEntryResult | Error | undefined;
  let scanTransform: ((scan: RepairNativeScan) => RepairNativeScan) | undefined;
  const header = JSON.stringify({ type: "session", id: sessionId, version: 3, timestamp: "2026-10-29T00:00:00.000Z", cwd: workspaceRoot }) + "\n";
  const offsetOf = (index: number) => Buffer.byteLength(header) + all.slice(0, index).reduce((n, entry) => n + Buffer.byteLength(JSON.stringify(entry) + "\n"), 0);
  const deliveryId = (id: string) => "pi-native-entry:v1:" + H(JSON.stringify([sessionId, id]));
  const append = (id: string, message?: unknown, options: { parent?: string | null; correlation?: string; durable?: boolean; type?: string } = {}) => {
    let nativeMessage = message;
    if (message !== null && typeof message === "object" && "role" in message) {
      if (message.role === "assistant") nativeMessage = { ...message, api: "openai-completions", provider: "openai", model: "component-model",
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        stopReason: "content" in message && Array.isArray(message.content) && message.content.some(block => block?.type === "toolCall") ? "toolUse" : "stop", timestamp: 12345 };
      else if (message.role === "user") nativeMessage = { ...message, timestamp: 12344 };
    }
    const entry: NativeMessageEntry = { id, parentId: options.parent === undefined ? leaf : options.parent,
      type: options.type ?? "message", timestamp: "2026-10-29T00:00:01.000Z", ...(nativeMessage === undefined ? {} : { message: nativeMessage }) };
    if (all.some(value => value.id === id)) throw new Error("duplicate test native identity");
    all.push(entry); leaf = id;
    const bytes = Buffer.from(JSON.stringify(entry) + "\n"), seq = nextSeq++;
    if (options.durable !== false) committed.set(options.correlation ?? deliveryId(id), { seq, bytes: Uint8Array.from(bytes) });
    return { entry, bytes, seq };
  };
  // A can predate activation and have no B6 record. D, NOT A, starts protection.
  append("A", { role: "assistant", content: [{ type: "text", text: "pre-activation anchor" }] }, { durable: false });
  // probe:native-Pi-maintenance — retained exact serializer, branch ancestry,
  // scan/coordinates, post-hook join and epoch lifecycle. Not real Pi/fsync.
  const native: RepairNativeReader = {
    getLeafId: () => leaf,
    getBranch(from = leaf ?? "") {
      const branch: NativeMessageEntry[] = [];
      let current = all.find(entry => entry.id === from);
      while (current) { branch.push(current); current = current.parentId === null ? undefined : all.find(entry => entry.id === current!.parentId); }
      return clone(branch.reverse());
    },
    inspectRecoveryEntries() {
      const entries = all.map((entry, index) => {
        const bytes = Buffer.from(JSON.stringify(entry) + "\n");
        return { entryId: entry.id, location: { file, offset: offsetOf(index), length: bytes.length - 1 }, bytes: Uint8Array.from(bytes), terminated: true };
      });
      const scan: RepairNativeScan = { sessionId, file, bytes: Buffer.byteLength(header) + all.reduce((n, entry) => n + Buffer.byteLength(JSON.stringify(entry) + "\n"), 0), entries, issues: [] };
      return scanTransform ? scanTransform(scan) : scan;
    },
  };
  const open = () => {
    const thisEpoch = epoch; let live = true;
    const assertCurrent = () => {
      if (!live || epoch !== thisEpoch) throw Object.assign(new Error("native binding revoked"), { code: "RECOVERY_EPOCH_REVOKED" });
    };
    const owner: RecoveryOwner = { orgId: "repair-org", logicalSessionId: sessionId, sessionRoot, workspaceRoot, sessionFile: file,
      pvcUID: "retained-test-volume", exclusionReceipt: "probe:native-Pi-maintenance", signal: new AbortController().signal,
      attempt: { podUID: `pod-${epoch}`, bootId: `boot-${epoch}`, ownerIncarnation: `owner-${epoch}`, authorityRevision: String(epoch) }, assertLive: assertCurrent };
    const store = new EffectStore(owner);
    const readNativeToolResult = async (effect: { key: string; toolCallId: string; outcome: EffectOutcome }) => {
      assertCurrent();
      const record = await store.withSession("settle", tx => tx.read(effect.key));
      if (!record.transitions.some(row => row.state === "RESULT" && row.outcome === effect.outcome)) throw new Error("native join lacks the retained B4 outcome");
      const message = nativeResults.get(effect.toolCallId);
      return message === undefined ? undefined : clone(message);
    };
    const port = createCertifiedEffectRecordPort({ owner, store, readNativeToolResult,
      admissionForCall() { throw new Error("test only binds already-issued intents"); },
      async confirmNativeAssistant() { throw new Error("test does not dispatch new tools"); },
      assertManifestConstraints() { throw new Error("test does not regenerate preparation"); },
      additionalSinks: [{ kind: "maintenance-test", adapterVersion: "1", async reconcile(intent, context) {
        assertCurrent(); expect(context.owner).toBe(owner); sinks.push(intent.key);
        const outcome = outcomes.get(intent.key) ?? "committed";
        return { outcome, ...(outcome === "uncertain" ? { reason: "destination ACK unavailable" } : { result: {
          role: "toolResult", toolCallId: intent.tool.originalToolCallId, toolName: "write", content: [{ type: "text", text: "saved λ\nresult" }],
          details: { z: "é", a: "保留" }, isError: outcome === "not-committed",
        } }) };
      } }],
    });
    // probe:B6-committed-maintenance — committed bytes+seq read boundary. A
    // withheld record models no prior barrier; no registration/fsync is proved.
    const engine = createMaintenanceRepairEngine({ owner, store, native, assertCurrent, readNativeToolResult,
      durability: { async readDurableEntry(request) {
        assertCurrent(); const callback = beforeRead; beforeRead = undefined; if (callback) await callback();
        if (readFailure instanceof Error) throw readFailure;
        if (readFailure) return readFailure;
        const record = committed.get(request.deliveryId);
        return request.sessionId === sessionId && request.recordKind === "carrier" && record
          ? { found: true, seq: record.seq, bytes: Uint8Array.from(record.bytes) }
          : { found: false, reason: "not-found" };
      } },
    });
    const admissionOwner: RecoveryAdmissionOwner = { epoch: { processIncarnationId: `process-${epoch}`, bindingId: `binding-${epoch}`, epoch },
      podId: `pod-${epoch}`, bootId: `boot-${epoch}`, historyComplete: true, assertCurrent,
      reconsiderInput() { assertCurrent(); drives.push(epoch); },
      validateRepair(ref) { validators.push(clone(ref)); engine.resolveRepair(ref); },
    };
    if (!initialized) {
      initializeNewRecoverySession({ mode: "NEW", root: sessionRoot, sessionId, recovery: admissionOwner,
        assertNew() { assertCurrent(); if (initialized) throw new Error("not NEW"); } }); initialized = true;
    }
    const unbind = bindInputAdmission({ root: sessionRoot, sessionId, recovery: admissionOwner,
      mainAgent: { id: `session:${sessionId}`, sessionId, local: true,
        deliverAgent() { throw new Error("probe:native-Pi-maintenance: legacy native delivery is forbidden in REQUIRED"); } } });
    const ioAbort = new AbortController();
    const host = createMaintenanceRepairHost({ owner, engine, recordPort: port, native, admissionOwner, io: { signal: ioAbort.signal, deadlineMs: deadline } });
    return { owner, store, port, engine, host, admissionOwner, abortIo() { ioAbort.abort(); }, revoke() { live = false; },
      close() { engine.invalidateRepairResolution(); unbind(); live = false; } };
  };
  let runtime = open();
  const admit = async (pin = true): Promise<Original> => {
    const originalInputId = `input-${randomUUID()}`;
    const receipt = admitBoundScriptedInput({ sessionId, request: { from: { id: "sender", name: "Sender", kind: "agent" },
      message: "original λ\ninput é", delivery: "followUp", data: { scriptedInput: { version: 1, sessionId, inputId: originalInputId, deadlineAt: deadline } } } });
    const selection = await selectRecoveryInput({ sessionId, originalInputId, epoch: runtime.admissionOwner.epoch,
      expectedRevision: readRecoverySession(sessionId).revision });
    if (!selection.selected) throw new Error(selection.code);
    const generation = pin ? await ensureRecoveryGeneration(sessionId, originalInputId) : undefined;
    const selected = selection.input;
    expect(selected.admissionId).toBe(receipt.messageId); expect(selected.deliveryId).toBe(selected.admissionId);
    const carrier = `D-${originalInputId}`;
    const physical = append(carrier, { role: "user", content: [{ type: "text", text: selected.text }] }, { correlation: selected.deliveryId });
    await commitRecoveryInputDelivery(selected, { entryId: carrier, bytesSha256: H(physical.bytes), seq: physical.seq, executionBranchEntryId: "A" });
    return { selected, generation, carrier, released: false, input: { sessionId, originalInputId, admissionId: selected.admissionId,
      deliveryId: selected.deliveryId, kind: "initial", text: selected.text, payloadSha256: H(selected.text), deadlineMs: selected.deadlineMs, consumption: { kind: "round" } } };
  };
  const issue = async (original: Original, name = "write", parentToolCallId?: string) => {
    const toolCallId = `call-${randomUUID()}`, assistant = `assistant-${toolCallId}`;
    const args = { path: `${name}.txt`, content: "original λ\nbytes" };
    append(assistant, { role: "assistant", content: [{ type: "toolCall", id: toolCallId, name: "write", arguments: args }] });
    const slot = { id: name, ordinal: 0, kind: "maintenance-test", canonicalRef: "pi.write", adapterVersion: "1",
      canonicalTarget: path.join(workspaceRoot, `${name}.txt`), predecessors: [], canonicalArguments: args, constraints: {} };
    const tuple = [1, "repair-org", sessionId, original.input.originalInputId, slot.id, slot.kind] as const;
    const intent: EffectIntent = { format: "fabric.effect-intent", version: 1, key: mintEffectKey(tuple), keyTuple: tuple,
      manifestDigest: manifestDigest({ version: 1, slots: [slot] }), acceptedInput: { id: original.input.originalInputId,
        digest: original.input.payloadSha256, nativeEntryId: original.carrier }, slot,
      tool: { canonicalRef: "pi.write", adapterVersion: "1", originalToolCallId: toolCallId, nativeAssistantEntryId: assistant,
        ...(parentToolCallId === undefined ? {} : { parentToolCallId }) }, canonicalArguments: args, canonicalTarget: slot.canonicalTarget,
      payload: { encoding: "base64", bytes: Buffer.from(args.content).toString("base64"), digest: H(args.content) }, originAttempt: runtime.owner.attempt };
    await runtime.store.withSession("new-intent", tx => tx.bind(intent)); return intent;
  };
  const materialize = async (intent: EffectIntent, toolCallId = intent.tool.originalToolCallId) => {
    const result = await runtime.store.withSession("settle", tx => tx.read(intent.key).transitions.find(row => row.state === "RESULT"));
    const saved = result?.result;
    // This fixture's sink emits text blocks; validate the retained payload before
    // assigning the native result type, including readonly JSON object narrowing.
    if (!result || !saved || typeof saved !== "object" || Array.isArray(saved) ||
        !("content" in saved) || !Array.isArray(saved.content) ||
        !saved.content.every((block: unknown): block is { type: "text"; text: string } =>
          block !== null && typeof block === "object" && "type" in block && block.type === "text" &&
          "text" in block && typeof block.text === "string") || !("details" in saved)) {
      throw new Error("test requires actual saved sink result");
    }
    // Exact native property order differs from canonical adapter encoding.
    const message: EffectToolResult = { role: "toolResult", toolCallId, toolName: "write", content: clone(saved.content),
      details: clone(saved.details), isError: result.outcome === "not-committed", timestamp: nativeResults.size + 12345 };
    append(`result-${toolCallId}`, message); nativeResults.set(toolCallId, clone(message));
  };
  const settle = async (original: Original, intent?: EffectIntent) => {
    if (!original.generation) throw new Error("no test pin");
    const result = await runtime.port.settleOriginalPendingIntents(sessionId, original.input.originalInputId, original.generation);
    if (intent && result.uncertain === 0) await materialize(intent);
    return result;
  };
  const join = async (original: Original, kind: "lifecycle-interrupt" | "user-cancel" = "lifecycle-interrupt") => {
    append(`J-${randomUUID()}`, { role: "assistant", content: [{ type: "text", text: "joined λ\n保留" }] });
    const outcome = await recordInputOutcome({ sessionId, originalInputId: original.input.originalInputId, requestId: `outcome-${randomUUID()}`,
      kind, at: Date.now(), bootId: runtime.admissionOwner.bootId, deadlineMs: original.input.deadlineMs });
    expect(outcome.durable).toBe(true);
    if (!original.released) {
      await releaseRecoveryInputAfterJoin(original.selected, Promise.resolve(), async () => { runtime.admissionOwner.assertCurrent(); });
      original.released = true;
    }
  };
  const binding = (original: Original): BoundRepairOriginal => {
    const state = readRecoverySession(sessionId), input = state.inputs.find(row => row.kind === "round" && row.originalInputId === original.input.originalInputId)!;
    const pin = state.recoveryGenerationPins.find(row => row.originalInputId === input.originalInputId && row.admissionId === input.admissionId)!;
    return { input: original.input, originalAdmissionId: input.admissionId, originalPayloadSha256: H(input.round.message),
      originalDeadlineMs: input.deadlineAt, originalCarrierEntryId: input.delivery!.entryId, executionBranchEntryId: input.delivery!.executionBranchEntryId,
      recoveryGeneration: pin.recoveryGeneration, carriers: state.inputs.flatMap(row => row.delivery ? [{ entryId: row.delivery.entryId,
        deliveryId: row.admissionId, bytesSha256: row.delivery.bytesSha256, seq: row.delivery.seq }] : []) };
  };
  return { root, sessionRoot, sessionId, file, native, all, committed, nativeResults, outcomes, sinks, validators, drives, append, admit, issue, settle, materialize, join, binding,
    get runtime() { return runtime; }, get leaf() { return leaf!; },
    beforeRead(callback: () => Promise<void>) { beforeRead = callback; },
    failRead(failure: RecoveryDurableEntryResult | Error | undefined) { readFailure = failure; },
    transformScan(transform?: (scan: RepairNativeScan) => RepairNativeScan) { scanTransform = transform; },
    async reopen() { runtime.close(); epoch++; runtime = open(); },
    close() { try { runtime.close(); } finally { fs.rmSync(root, { recursive: true, force: true }); } },
  };
}
const using = async (run: (f: Awaited<ReturnType<typeof fixture>>) => Promise<void>) => {
  const f = await fixture(); try { await run(f); } finally { f.close(); }
};
const ready = async (f: Awaited<ReturnType<typeof fixture>>) => {
  const original = await f.admit(), intent = await f.issue(original);
  expect((await f.settle(original, intent)).uncertain).toBe(0); await f.join(original);
  return { original, intent };
};

describe("maintenance repair production over actual B5 (native/B6 component boundaries only)", () => {
  it("produces A != J with actual input/pin/deadline and distinct exact hash domains, without sink execution", () => using(async f => {
    const { original } = await ready(f), before = f.sinks.length;
    const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf);
    const view = f.runtime.host.resolveRepair(ref);
    expect(f.sinks).toHaveLength(before); expect(ref.recoveryGeneration).toBe(original.generation);
    expect(ref.recoveryGeneration).not.toBe("b5-durable-generation");
    expect(view.executionBranchEntryId).toBe("A"); expect(view.joinedLeafEntryId).not.toBe("A");
    expect(view.protectedEntryIds[0]).toBe(original.carrier); expect(view.protectedEntryIds).not.toContain("A");
    expect(view.settlements).toHaveLength(1); expect(Object.isFrozen(view.nativeEntries)).toBe(true);
    const branch = f.native.getBranch(f.leaf);
    const exact = H(JSON.stringify(branch.map(entry => [entry.id, H(JSON.stringify(entry) + "\n")])));
    expect(view.joinedHistorySha256).toBe(exact);
    expect(exact).not.toBe(H(JSON.stringify(branch.map(entry => [entry.id, H(canonicalEncode(entry) + "\n")]))));
    expect(exact).not.toBe(H(JSON.stringify(branch.map(entry => [entry.id, H(JSON.stringify(entry))]))));
    expect(ref.repairReceiptSha256).toBe(H(canonicalEncode(view)));
    expect(view.protectedRangeSha256).toBe(H(canonicalEncode(["fabric.b4.protected-range", 1, view.nativeEntries.map(row => [row.entryId, row.bytesSha256])])));
    for (const row of view.nativeEntries) {
      const exactBytes = Buffer.from(JSON.stringify(f.all.find(entry => entry.id === row.entryId)) + "\n");
      expect(row.length).toBe(exactBytes.length - 1); expect(row.bytesSha256).toBe(H(exactBytes));
      expect(row.b6Sequence).toBe(f.committed.get(row.deliveryId)!.seq);
    }
    expect(readRecoverySession(f.sessionId).repairs).toEqual([ref]); expect(f.validators).toEqual([ref]);
  }));

  it("implements the Q2 open delivered zero-intent path without making a no-original receipt", () => using(async f => {
    await expect(f.runtime.host.repairMaintenance({ sessionId: f.sessionId, originalInputId: "foreign", admissionId: "x", deliveryId: "x", kind: "initial",
      text: "x", payloadSha256: H("x"), deadlineMs: Date.now() + 1000, consumption: { kind: "round" } }, f.leaf)).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
    const original = await f.admit(); await f.join(original);
    const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf);
    expect(f.runtime.host.resolveRepair(ref).settlements).toEqual([]); expect(f.sinks).toEqual([]);
  }));

  it("missing existing pin holds, and a live input reservation holds before recording", () => using(async f => {
    const original = await f.admit(false); await f.join(original);
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    expect(readRecoverySession(f.sessionId).recoveryGenerationPins).toEqual([]);
    await ensureRecoveryGeneration(f.sessionId, original.input.originalInputId);
    const selection = await selectRecoveryInput({ sessionId: f.sessionId, originalInputId: original.input.originalInputId,
      epoch: f.runtime.admissionOwner.epoch, expectedRevision: readRecoverySession(f.sessionId).revision });
    expect(selection.selected).toBe(true);
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    expect(readRecoverySession(f.sessionId).repairs).toEqual([]);
  }));

  it("foreign original is CONFLICT; this original's uncertainty wins over forged occurrence identity", () => using(async f => {
    const original = await f.admit(), intent = await f.issue(original); f.outcomes.set(intent.key, "uncertain");
    expect((await f.settle(original)).uncertain).toBe(1); await f.join(original);
    await expect(f.runtime.host.repairMaintenance({ ...original.input, admissionId: "forged", payloadSha256: H("bad") }, f.leaf)).rejects.toMatchObject(REQUIRED);
    await expect(f.runtime.host.repairMaintenance({ ...original.input, originalInputId: "foreign" }, f.leaf)).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
  }));

  it("authenticates exact B5 occurrence fields, including continuation text independently of original text", () => using(async f => {
    const { original } = await ready(f);
    for (const change of [{ deliveryId: "forged" }, { admissionId: "forged" }, { text: "changed" }, { payloadSha256: H("changed") },
      { deadlineMs: original.input.deadlineMs + 1 }, { kind: "recovery" as const }, { consumption: { kind: "effect-read" as const, effectKey: "x" } }]) {
      await expect(f.runtime.host.repairMaintenance({ ...original.input, ...change }, f.leaf)).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    }
    expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
    const continuation = await admitContinuation({ sessionId: f.sessionId, originalInputId: original.input.originalInputId,
      recoveryGeneration: original.generation!, text: "different continuation λ" });
    const selected = await selectRecoveryInput({ sessionId: f.sessionId, originalInputId: original.input.originalInputId,
      admissionId: continuation.admissionId, epoch: f.runtime.admissionOwner.epoch, expectedRevision: readRecoverySession(f.sessionId).revision });
    if (!selected.selected) throw new Error(selected.code);
    const carrierId = "continuation-carrier";
    const carrier = f.append(carrierId, { role: "user", content: [{ type: "text", text: selected.input.text }] }, { correlation: selected.input.deliveryId });
    await commitRecoveryInputDelivery(selected.input, { entryId: carrierId, executionBranchEntryId: "A", bytesSha256: H(carrier.bytes), seq: carrier.seq });
    const resumed: Original = { ...original, selected: selected.input, released: false, input: { ...original.input, kind: "recovery",
      admissionId: selected.input.admissionId, deliveryId: selected.input.deliveryId, text: selected.input.text, payloadSha256: H(selected.input.text) } };
    await f.join(resumed);
    const ref = await f.runtime.host.repairMaintenance(resumed.input, f.leaf), view = f.runtime.host.resolveRepair(ref);
    expect(ref.executionBranchEntryId).toBe("A"); expect(view.nativeEntries.find(row => row.entryId === carrierId)!.deliveryId).toBe(continuation.admissionId);
    expect(view.originalInputId).toBe(original.input.originalInputId);
  }));

  it.each(["not-found", "store-unavailable", "superseded"] as const)("B6 %s is not commit proof and creates no final record", reason => using(async f => {
    const { original } = await ready(f); f.failRead({ found: false, reason });
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
    f.failRead(undefined); const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf);
    expect(f.runtime.host.resolveRepair(ref).settlements).toHaveLength(1);
  }));

  it("a registered-looking native entry without prior B6 barrier holds; exact bytes and original sequence are required", () => using(async f => {
    const { original } = await ready(f);
    const saved = f.committed.get(original.input.deliveryId)!; f.committed.delete(original.input.deliveryId);
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    f.committed.set(original.input.deliveryId, { ...saved, seq: saved.seq + 1 });
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    f.committed.set(original.input.deliveryId, { ...saved, bytes: Buffer.from("different\n") });
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    f.committed.set(original.input.deliveryId, saved);
    expect(await f.runtime.host.repairMaintenance(original.input, f.leaf)).toHaveProperty("repairReceiptSha256");
  }));

  it("same-looking materialization is not a join, and actual call arguments cannot be regenerated or substituted", () => using(async f => {
    const { original, intent } = await ready(f), callId = intent.tool.originalToolCallId;
    const actual = f.nativeResults.get(callId)!;
    f.nativeResults.delete(callId);
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    f.nativeResults.set(callId, { ...actual, details: { forged: true } });
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    f.nativeResults.set(callId, actual);
    const entry = f.all.find(row => row.id === intent.tool.nativeAssistantEntryId)!;
    const saved = clone(entry.message);
    const changed = clone(entry.message) as { content: Array<{ arguments: unknown }> };
    changed.content[0]!.arguments = { path: "different", content: "new bytes" }; entry.message = changed;
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    entry.message = saved;
    expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
    expect(await f.runtime.host.repairMaintenance(original.input, f.leaf)).toHaveProperty("repairReceiptSha256");
  }));

  it("cold and forged refs throw synchronously, then a new epoch prepares exact retained history without another sink", () => using(async f => {
    const { original } = await ready(f); const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf);
    const view = f.runtime.host.resolveRepair(ref), before = f.sinks.length;
    for (const bad of [{ ...ref, repairReceiptSha256: H("forged") }, { ...ref, executionBranchEntryId: f.leaf },
      { ...ref, recoveryGeneration: "forged" }, { ...ref, extra: true }]) expect(() => f.runtime.host.resolveRepair(bad)).toThrowError(expect.objectContaining(REQUIRED));
    let getterRan = false;
    const accessorRef = Object.defineProperty({ ...ref }, "repairReceiptSha256", { enumerable: true, get() { getterRan = true; return ref.repairReceiptSha256; } });
    expect(() => f.runtime.host.resolveRepair(accessorRef)).toThrowError(expect.objectContaining(REQUIRED)); expect(getterRan).toBe(false);
    await f.reopen(); expect(() => f.runtime.host.resolveRepair(ref)).toThrowError(expect.objectContaining(REQUIRED));
    await f.runtime.engine.prepareRepairResolution(ref);
    expect(f.runtime.host.resolveRepair(ref)).toEqual(view); expect(f.runtime.host.resolveRepair(ref)).not.toBeInstanceOf(Promise);
    expect(await f.runtime.host.repairMaintenance(original.input, f.leaf)).toEqual(ref); expect(f.sinks).toHaveLength(before);
    expect(f.validators).toEqual([ref, ref]); // Real B5 exact retry still invokes the void validator.
  }));

  it("new production at J2 refuses without destroying the old historical resolver", () => using(async f => {
    const { original } = await ready(f); const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf), oldJ = f.leaf;
    f.append("J2", { role: "assistant", content: [{ type: "text", text: "later" }] });
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    expect(f.runtime.host.resolveRepair(ref).joinedLeafEntryId).toBe(oldJ);
    await f.reopen(); await f.runtime.engine.prepareRepairResolution(ref);
    expect(f.runtime.host.resolveRepair(ref).joinedLeafEntryId).toBe(oldJ);
  }));

  it("cancel after B4 final leaves historical evidence, not an admission latch for later originals", () => using(async f => {
    const { original } = await ready(f);
    const ref = await f.runtime.engine.certifyRepair(f.binding(original), f.leaf);
    await f.join(original, "user-cancel");
    await expect(recordRecoveryRepair(f.sessionId, ref)).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    expect(f.runtime.host.resolveRepair(ref).originalInputId).toBe(original.input.originalInputId);
    const next = await f.admit(); await f.join(next);
    const nextRef = await f.runtime.host.repairMaintenance(next.input, f.leaf);
    expect(nextRef.recoveryGeneration).not.toBe(ref.recoveryGeneration);
  }));

  it("actual B5 I/O cancellation after B4 final returns no operation ref and preserves historical resolution", () => using(async f => {
    const { original } = await ready(f), realLink = fs.linkSync.bind(fs);
    const link = vi.spyOn(fs, "linkSync").mockImplementation((source, target) => {
      realLink(source, target);
      if (String(target).endsWith(`${path.sep}receipt.json`)) f.runtime.abortIo();
    });
    try { await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject({ code: "RECOVERY_ABORTED" }); }
    finally { link.mockRestore(); }
    expect(readRecoverySession(f.sessionId).repairs).toEqual([]);
    const saved = await f.runtime.store.withSession("settle", tx => tx.readMaintenanceRepair(original.input.originalInputId));
    const ref: RepairRef = { originalInputId: saved!.view.originalInputId, recoveryGeneration: saved!.view.recoveryGeneration,
      executionBranchEntryId: saved!.view.executionBranchEntryId, repairReceiptSha256: saved!.repairReceiptSha256 };
    expect(f.runtime.host.resolveRepair(ref).joinedLeafEntryId).toBe(f.leaf);
    await f.reopen(); expect(await f.runtime.host.repairMaintenance(original.input, f.leaf)).toEqual(ref);
  }));

  it("unrelated uncertain original and its concurrent frontier progress do not block this original", () => using(async f => {
    const old = await f.admit(), pending = await f.issue(old); f.outcomes.set(pending.key, "uncertain");
    await f.settle(old); await f.join(old, "user-cancel");
    const next = await f.admit(), intent = await f.issue(next); await f.settle(next, intent); await f.join(next);
    const paused = gate(); f.beforeRead(() => paused.wait());
    const repairing = f.runtime.host.repairMaintenance(next.input, f.leaf);
    try {
      await paused.entered;
      await f.runtime.store.withSession("settle", tx => tx.append(pending.key, { state: "ALIAS", toolCallId: "old-extra-alias",
        attempt: f.runtime.owner.attempt, at: "2026-10-29T00:00:04.000Z" }));
    } finally { paused.release(); }
    const ref = await repairing, view = f.runtime.host.resolveRepair(ref);
    expect(view.settlements.map(row => row.key)).toEqual([intent.key]);
    expect(await f.runtime.store.withSession("settle", tx => tx.read(pending.key).transitions.some(row => row.state === "RESULT"))).toBe(false);
    const stored = await f.runtime.store.withSession("settle", tx => tx.readMaintenanceRepair(next.input.originalInputId));
    expect(stored!.frontier.map(row => row.key)).toEqual([intent.key]);
  }));

  it("rechecks this original's exact frontier in the publication lock, without nested native/B5/store entry", () => using(async f => {
    const { original, intent } = await ready(f), paused = gate(); f.beforeRead(() => paused.wait());
    const repairing = f.runtime.host.repairMaintenance(original.input, f.leaf);
    const refusal = expect(repairing).rejects.toMatchObject(REQUIRED);
    try {
      await paused.entered;
      await f.runtime.store.withSession("settle", tx => tx.append(intent.key, { state: "ALIAS", toolCallId: "late-alias",
        attempt: f.runtime.owner.attempt, at: "2026-10-29T00:00:04.000Z" }));
    } finally { paused.release(); }
    await refusal;
    expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
  }));

  it("unions every retained off-branch alias call/result with D..J in native offset order", () => using(async f => {
    const original = await f.admit(), intent = await f.issue(original); await f.settle(original, intent);
    const offBranchResult = f.leaf, alias = "retained-alias", assistant = "alias-assistant";
    f.append(assistant, { role: "assistant", content: [{ type: "toolCall", id: alias, name: "write", arguments: intent.canonicalArguments }] }, { parent: original.carrier });
    await f.runtime.store.withSession("settle", tx => tx.append(intent.key, { state: "ALIAS", toolCallId: alias,
      attempt: f.runtime.owner.attempt, at: "2026-10-29T00:00:04.000Z", receipt: { nativeAssistantEntryId: assistant } }));
    await f.materialize(intent, alias); await f.join(original);
    expect(f.native.getBranch(f.leaf).map(row => row.id)).not.toContain(offBranchResult);
    const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf), view = f.runtime.host.resolveRepair(ref);
    expect(view.protectedEntryIds).toContain(offBranchResult); expect(view.settlements).toHaveLength(2);
    expect(view.nativeEntries.map(row => row.offset)).toEqual([...view.nativeEntries.map(row => row.offset)].sort((a, b) => a - b));
    expect(view.settlements[0]!.resultBytesHash).not.toBe(view.settlements[1]!.resultBytesHash);
  }));

  it("nested-parent materialization remains a truthful Q6 hold, never a fabricated parent return", () => using(async f => {
    const original = await f.admit(), intent = await f.issue(original, "nested", "fabric-parent"); await f.settle(original, intent); await f.join(original);
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
  }));

  it.each(["not-committed", "committed"] as const)("preserves definitive %s outcomes and requires real materialized bytes", outcome => using(async f => {
    const original = await f.admit(), intent = await f.issue(original); f.outcomes.set(intent.key, outcome);
    await f.settle(original); await f.join(original);
    await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject(REQUIRED);
    await f.materialize(intent); // Real saved original exists; only native boundary supplies metadata.
    const ref = await f.runtime.host.repairMaintenance(original.input, f.leaf);
    expect(f.runtime.host.resolveRepair(ref).settlements[0]!.outcome).toBe(outcome);
  }));

  it("rejects scan issues, unterminated/invalid UTF-8, forged ids and unsafe coordinates", () => using(async f => {
    const { original } = await ready(f);
    const transforms: Array<(scan: RepairNativeScan) => RepairNativeScan> = [
      scan => ({ ...scan, issues: [{ location: scan.entries[1]!.location, bytes: new Uint8Array(), reason: "commit-unproven" }] }),
      scan => ({ ...scan, entries: scan.entries.map((row, i) => i === 1 ? { ...row, terminated: false } : row) }),
      scan => ({ ...scan, entries: scan.entries.map((row, i) => i === 1 ? { ...row, entryId: "forged" } : row) }),
      scan => ({ ...scan, entries: scan.entries.map((row, i) => i === 1 ? { ...row, location: { ...row.location, offset: Number.MAX_SAFE_INTEGER } } : row) }),
      scan => ({ ...scan, entries: scan.entries.map((row, i) => { if (i !== 1) return row; const bytes = Uint8Array.from(row.bytes); bytes[1] = 255; return { ...row, bytes }; }) }),
    ];
    for (const transform of transforms) {
      f.transformScan(transform); await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toBeDefined();
      f.transformScan(undefined);
      expect(await f.runtime.store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
    }
  }));

  it("observed B6 read failure invalidates a prepared cache; native revocation never returns stale permission", () => using(async f => {
    const { original } = await ready(f), ref = await f.runtime.host.repairMaintenance(original.input, f.leaf);
    f.failRead(new Error("committed log unavailable"));
    await expect(f.runtime.engine.prepareRepairResolution(ref)).rejects.toThrow("committed log unavailable");
    expect(() => f.runtime.host.resolveRepair(ref)).toThrowError(expect.objectContaining(REQUIRED));
    f.failRead(undefined); await f.runtime.engine.prepareRepairResolution(ref);
    f.runtime.revoke(); expect(() => f.runtime.host.resolveRepair(ref)).toThrowError(expect.objectContaining({ code: "RECOVERY_EPOCH_REVOKED" }));
    await f.reopen(); expect(() => f.runtime.host.resolveRepair(ref)).toThrowError(expect.objectContaining(REQUIRED));
  }));

  it.each(["canonical-link-ACK", "namespace-barrier"] as const)("receipt %s failure exposes no ref and exact retained retry uses real barriers", failure => using(async f => {
    const { original } = await ready(f);
    const realLink = fs.linkSync.bind(fs), realSync = fs.fsyncSync.bind(fs);
    let canonicalLinked = false, injected = false;
    const links = vi.spyOn(fs, "linkSync").mockImplementation((source, target) => {
      realLink(source, target);
      if (String(target).endsWith(`${path.sep}receipt.json`)) {
        canonicalLinked = true;
        if (failure === "canonical-link-ACK" && !injected) { injected = true; throw new Error("receipt link ACK lost"); }
      }
    });
    const barriers = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (failure === "namespace-barrier" && canonicalLinked && !injected && fs.fstatSync(fd).isDirectory()) {
        injected = true; throw new Error("receipt namespace barrier unavailable");
      }
      realSync(fd);
    });
    try { await expect(f.runtime.host.repairMaintenance(original.input, f.leaf)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" }); }
    finally { links.mockRestore(); barriers.mockRestore(); }
    expect(injected).toBe(true); expect(readRecoverySession(f.sessionId).repairs).toEqual([]);
    const saved = await f.runtime.store.withSession("settle", tx => tx.readMaintenanceRepair(original.input.originalInputId));
    expect(saved).toBeDefined();
    const ref: RepairRef = { originalInputId: saved!.view.originalInputId, recoveryGeneration: saved!.view.recoveryGeneration,
      executionBranchEntryId: saved!.view.executionBranchEntryId, repairReceiptSha256: saved!.repairReceiptSha256 };
    expect(() => f.runtime.host.resolveRepair(ref)).toThrowError(expect.objectContaining(REQUIRED));
    await f.reopen();
    expect(await f.runtime.host.repairMaintenance(original.input, f.leaf)).toEqual(ref);
    expect(f.runtime.host.resolveRepair(ref).joinedLeafEntryId).toBe(f.leaf);
  }));

  it("unknown-ACK final receipt is requalified, while actual retained storage damage invalidates cache", () => using(async f => {
    const { original } = await ready(f);
    // B4 final exists; caller lost its ACK before B5 recording. This exercises
    // immutable exact retry, not a claim of killed-process/native/B6 recovery.
    const ref = await f.runtime.engine.certifyRepair(f.binding(original), f.leaf);
    await f.reopen(); await f.runtime.engine.prepareRepairResolution(ref);
    expect(await f.runtime.host.repairMaintenance(original.input, f.leaf)).toEqual(ref);
    const receipts = path.join(f.sessionRoot, "effects", "repairs-v1");
    const directory = fs.readdirSync(receipts).find(name => !name.endsWith(".tmp"))!;
    fs.writeFileSync(path.join(receipts, directory, "receipt.json"), "damaged retained receipt");
    await expect(f.runtime.engine.prepareRepairResolution(ref)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    expect(() => f.runtime.host.resolveRepair(ref)).toThrowError(expect.objectContaining(REQUIRED));
  }));
});
