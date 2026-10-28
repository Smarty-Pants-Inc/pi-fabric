import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { persistRecoveryAttemptCompletion, type RecoveryAttemptCompletionRequest } from "../src/recovery/attempt-completion.js";
import {
  createRecoveryAttemptLedger, type RecoveryAttemptIdentity, type RecoveryAttemptRequest, type RecoveryEvidenceBytes,
} from "../src/recovery/attempt-ledger.js";
import { openRecoveryDurabilityStore, type RecoveryNativeEntryVerifier } from "../src/recovery/durability-store.js";
import { openRecoveryJournal } from "../src/recovery/recovery-journal.js";
import type { RecoveryNativeEntryRegistrationRequest } from "../src/recovery/l7-interfaces.js";

// Trust boundary/data-safety integration tests. Native Pi remains an EXTERNAL owner: this
// path is descriptor metadata only, never created/opened/read/scanned/fsynced by the tests.
const NATIVE_FILE = "/native-descriptor-fixture/retained/session.jsonl";
const hash = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
const raw = (text: string): RecoveryEvidenceBytes => { const bytes = Uint8Array.from(Buffer.from(text)); return { bytes, sha256: hash(bytes) }; };
const bounds = () => ({ deadlineMs: Date.now() + 30_000 });
const identity = (attemptNumber = 1): RecoveryAttemptIdentity => ({
  sessionId: "session", acceptedInputId: "input", toolCallId: `call-${attemptNumber}`, effectKey: "effect",
  attemptId: `attempt-${attemptNumber}`, attemptNumber, hostId: "host", podId: "pod", bootId: "boot",
  ownerIncarnation: "incarnation", authorityRevision: "revision",
});
const trace = () => {
  const evidence = raw("raw trace prefix\nincluding an interrupted final li");
  return { traceId: "trace", storage: { storeId: "trace-fixture", objectKey: "raw-prefix", writerHostId: "host", receipt: "fixture-trace-commit" },
    retainedFrom: 0 as const, byteLength: evidence.bytes.length, ...evidence, interrupted: true };
};
const attempt = (id: RecoveryAttemptIdentity): RecoveryAttemptRequest => ({
  identity: id, toolVersion: "tool@1", intent: raw("immutable intent"), payload: raw("immutable payload"),
  destination: { kind: "fixture", target: "destination" }, observedAt: 10, traces: [trace()],
});
const closers: Array<{ close(): Promise<void> }> = [];
const forbiddenNativeOpens: string[] = [];
beforeEach(() => {
  const open = fs.openSync;
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    if (String(file).startsWith("/native-descriptor-fixture/")) {
      forbiddenNativeOpens.push(String(file));
      throw new Error("Native file I/O is forbidden");
    }
    return open(file, flags, mode);
  });
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const resource of closers.splice(0).reverse()) await resource.close();
  expect(forbiddenNativeOpens.splice(0)).toEqual([]);
  // Retain temporary journals/raw trace evidence; no trace deletion or cleanup.
});

async function fixture(verifier?: RecoveryNativeEntryVerifier) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-attempt-completion-"));
  const journalOptions = { directory: path.join(root, "attempts"), storeId: "attempt-store" };
  const nativeEvidence = new Map<string, Buffer>();
  const storeOptions = {
    directory: path.join(root, "barriers"), storeId: "barrier-store", sessionId: "session", sessionFile: NATIVE_FILE,
    // Explicit native-owner fixture: compare the exact descriptor against fixture commit
    // evidence. This does NOT establish live native flushing, which remains external.
    verifyNativeEntry: verifier ?? (async (request: RecoveryNativeEntryRegistrationRequest) => {
      const expected = nativeEvidence.get(request.entryId);
      return expected !== undefined && expected.equals(Buffer.from(request.bytes)) && hash(expected) === request.bytesSha256;
    }),
  };
  const journal = await openRecoveryJournal(journalOptions); closers.push(journal);
  const ledger = createRecoveryAttemptLedger(journal);
  const store = await openRecoveryDurabilityStore(storeOptions); closers.push(store);
  const request = (id = identity(), eventId = "result", entryId = `native-${id.attemptId}-${eventId}`): RecoveryAttemptCompletionRequest => {
    const message = { role: "toolResult", toolCallId: id.toolCallId, toolName: "fixture_tool",
      content: [{ type: "text", text: "post-hook native result, NOT the raw adapter bytes" }], isError: false, timestamp: 100,
      // Arbitrary tool details must NOT be treated as a universal recovery identity schema.
      details: { acceptedInputId: "opaque-other-input", effectKey: "opaque-other-effect", toolCallId: "opaque-other-call" } };
    const bytes = Buffer.from(`${JSON.stringify({ type: "message", id: entryId, parentId: "parent-entry", timestamp: "2026-01-01T00:00:00.000Z", message })}\n`);
    nativeEvidence.set(entryId, Buffer.from(bytes));
    const nativeEntry: RecoveryNativeEntryRegistrationRequest = {
      sessionId: id.sessionId, deliveryId: "pi-native-entry:v1:" + hash(JSON.stringify([id.sessionId, entryId])),
      recordKind: "carrier", entryId, location: { file: NATIVE_FILE, offset: 128, length: bytes.length - 1 }, bytes: Uint8Array.from(bytes),
      bytesSha256: hash(bytes), flushReceipt: { kind: "file-fsync", file: NATIVE_FILE, bytesThrough: 128 + bytes.length, namespaceDurable: true }, ...bounds(),
    };
    return { event: { identity: { ...id }, eventId, state: "committed", observedAt: 100, result: raw("raw committed adapter outcome"),
      receipt: raw("raw business receipt"), traces: [trace()] }, nativeEntry,
      nativeCorrelation: { acceptedInputId: id.acceptedInputId, effectKey: id.effectKey, attemptId: id.attemptId } };
  };
  await ledger.recordAttempt(attempt(identity()), bounds());
  return { root, journalOptions, storeOptions, journal, ledger, store, request };
}

const tuple = (request: RecoveryAttemptCompletionRequest) => ({
  sessionId: request.nativeEntry.sessionId, deliveryId: request.nativeEntry.deliveryId, recordKind: "carrier" as const,
});
const history = (context: Awaited<ReturnType<typeof fixture>>, id = identity()) => context.ledger.readAttempt(id.sessionId, id.attemptId)!;
const replaceMessage = (request: RecoveryAttemptCompletionRequest, patch: Record<string, unknown>) => {
  const entry = JSON.parse(Buffer.from(request.nativeEntry.bytes).toString("utf8"));
  Object.assign(entry.message, patch);
  const bytes = Buffer.from(`${JSON.stringify(entry)}\n`);
  Object.assign(request.nativeEntry, { bytes, bytesSha256: hash(bytes), location: { ...request.nativeEntry.location, length: bytes.length - 1 },
    flushReceipt: { kind: "file-fsync", file: NATIVE_FILE, bytesThrough: request.nativeEntry.location.offset + bytes.length, namespaceDurable: true } });
};

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

describe("host post-release attempt completion", () => {
  it("makes the caller's ACK eligible only after the raw event AND real native-entry barrier", async () => {
    const entered = deferred(), release = deferred();
    const context = await fixture(async () => { entered.resolve(); await release.promise; return true; });
    const request = context.request();
    const before = structuredClone(request);
    const acknowledge = vi.fn();
    const pending = persistRecoveryAttemptCompletion(context.ledger, context.store, request).then(result => {
      if (result.ackEligible) acknowledge(result);
      return result;
    });
    await entered.promise;
    expect(acknowledge).not.toHaveBeenCalled();
    expect(history(context).events).toHaveLength(1);
    expect(history(context).events[0]!.result).toEqual(request.event.result);
    expect(await context.store.readDurableEntry(tuple(request))).toEqual({ found: false, reason: "not-found" });
    release.resolve();
    const result = await pending;
    expect(result.ackEligible).toBe(true);
    if (!result.ackEligible) throw new Error("Expected completion eligibility");
    expect(result.attemptSeq).toBe(history(context).events[0]!.seq);
    expect(await context.store.readDurableEntry(tuple(request))).toEqual({ found: true, seq: result.barrierSeq, bytes: Uint8Array.from(request.nativeEntry.bytes) });
    expect(acknowledge).toHaveBeenCalledExactlyOnceWith(result);
    expect(request).toEqual(before);
  });

  it("normal exact repeat after reopen returns the same two seqs, without another evidence append, even if caller ACK failed", async () => {
    const context = await fixture(), request = context.request();
    const first = await persistRecoveryAttemptCompletion(context.ledger, context.store, request);
    expect(first.ackEligible).toBe(true);
    const beforeAttemptNames = fs.readdirSync(context.journalOptions.directory).filter(name => /^(raw|commit)-/.test(name));
    const beforeBarrierNames = fs.readdirSync(context.storeOptions.directory).filter(name => /^(raw|commit)-/.test(name));
    const acknowledge = () => { throw new Error("caller ACK transport failed AFTER barrier"); };
    expect(() => { if (first.ackEligible) acknowledge(); }).toThrow("caller ACK");
    await context.journal.close(); await context.store.close();
    const journal = await openRecoveryJournal({ ...context.journalOptions, required: true }); closers.push(journal);
    const ledger = createRecoveryAttemptLedger(journal);
    const store = await openRecoveryDurabilityStore({ ...context.storeOptions, required: true }); closers.push(store);
    const repeated = await persistRecoveryAttemptCompletion(ledger, store, request);
    expect(repeated).toEqual(first);
    expect(ledger.readAttempt("session", "attempt-1")!.events).toHaveLength(1);
    expect(ledger.readAttempt("session", "attempt-1")!.events[0]!.state).toBe("committed");
    expect(ledger.readAttempt("session", "attempt-1")!.events[0]!.result).toEqual(request.event.result);
    expect(fs.readdirSync(context.journalOptions.directory).filter(name => /^(raw|commit)-/.test(name))).toEqual(beforeAttemptNames);
    expect(fs.readdirSync(context.storeOptions.directory).filter(name => /^(raw|commit)-/.test(name))).toEqual(beforeBarrierNames);
  });

  it("does not make ACK eligible when raw-event journal fsync fails; reopen retains the complete unacknowledged raw event", async () => {
    const context = await fixture(), request = context.request();
    const register = vi.spyOn(context.store, "registerNativeEntry");
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw Object.assign(new Error("injected EIO"), { code: "EIO" }); });
    const result = await persistRecoveryAttemptCompletion(context.ledger, context.store, request);
    expect(result).toEqual({ ackEligible: false, stage: "attempt", reason: "store-unavailable" });
    expect(register).not.toHaveBeenCalled();
    sync.mockRestore();
    await context.journal.close();
    const journal = await openRecoveryJournal({ ...context.journalOptions, required: true }); closers.push(journal);
    const recovered = createRecoveryAttemptLedger(journal).readAttempt("session", "attempt-1")!;
    expect(recovered.events).toHaveLength(1);
    expect(recovered.events[0]!.result).toEqual(request.event.result);
    expect(recovered.events[0]!.state).toBe("committed");
  });

  it("does not make ACK eligible when the REAL durability barrier fsync fails, and keeps the raw committed outcome", async () => {
    const context = await fixture(), request = context.request();
    const register = context.store.registerNativeEntry.bind(context.store);
    let restoreSync: (() => void) | undefined;
    vi.spyOn(context.store, "registerNativeEntry").mockImplementation(async descriptor => {
      const result = await register(descriptor);
      expect(result.registered).toBe(true);
      const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw Object.assign(new Error("barrier EIO"), { code: "EIO" }); });
      restoreSync = () => sync.mockRestore();
      return result;
    });
    const result = await persistRecoveryAttemptCompletion(context.ledger, context.store, request);
    restoreSync?.();
    expect(result).toEqual({ ackEligible: false, stage: "barrier", reason: "store-unavailable", attemptSeq: history(context).events[0]!.seq });
    expect(history(context).events[0]!.state).toBe("committed");
    expect(history(context).events[0]!.result).toEqual(request.event.result);
    expect(await context.store.readDurableEntry(tuple(request))).toEqual({ found: false, reason: "store-unavailable" });
  });

  it("returns typed native-verifier refusal without a barrier or losing raw evidence", async () => {
    const context = await fixture(async () => false), request = context.request();
    const barrier = vi.spyOn(context.store, "awaitDurableAppend");
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toEqual({
      ackEligible: false, stage: "registration", reason: "conflict", attemptSeq: expect.any(Number),
    });
    expect(barrier).not.toHaveBeenCalled();
    expect(history(context).events[0]!.result).toEqual(request.event.result);
    expect(await context.store.readDurableEntry(tuple(request))).toEqual({ found: false, reason: "not-found" });
  });

  const invalidDescriptors: Array<[string, (request: RecoveryAttemptCompletionRequest) => void]> = [
    ["session", request => Object.assign(request.nativeEntry, { sessionId: "other-session" })],
    ["host input join", request => Object.assign(request.nativeCorrelation!, { acceptedInputId: "other-input" })],
    ["host effect join", request => Object.assign(request.nativeCorrelation!, { effectKey: "other-effect" })],
    ["host physical attempt join", request => Object.assign(request.nativeCorrelation!, { attemptId: "other-attempt" })],
    ["native toolCallId", request => replaceMessage(request, { toolCallId: "other-call" })],
    ["non-toolResult message", request => replaceMessage(request, { role: "assistant" })],
    ["entry ID", request => Object.assign(request.nativeEntry, { entryId: "other-entry" })],
    ["invalid serialized bytes", request => request.nativeEntry.bytes.fill(0)],
    ["missing descriptor bytes", request => Object.assign(request.nativeEntry, { bytes: undefined })],
  ];
  it.each(invalidDescriptors)("refuses a derivable %s mismatch AFTER retaining the raw outcome", async (_name, invalidate) => {
    const context = await fixture(), request = context.request();
    invalidate(request);
    const register = vi.spyOn(context.store, "registerNativeEntry");
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toEqual({
      ackEligible: false, stage: "registration", reason: "conflict", attemptSeq: expect.any(Number),
    });
    expect(register).not.toHaveBeenCalled();
    expect(history(context).events).toHaveLength(1);
    expect(history(context).events[0]!.result).toEqual(request.event.result);
    expect(history(context).events[0]!.state).toBe("committed");
  });

  it.each(["bytesSha256", "flushReceipt"] as const)("honors the real store's invalid %s refusal while retaining the raw outcome", async field => {
    const context = await fixture(), request = context.request();
    if (field === "bytesSha256") Object.assign(request.nativeEntry, { bytesSha256: "0".repeat(64) });
    else Object.assign(request.nativeEntry, { flushReceipt: { kind: "file-fsync", file: NATIVE_FILE, bytesThrough: 1, namespaceDurable: true } });
    const register = vi.spyOn(context.store, "registerNativeEntry"), barrier = vi.spyOn(context.store, "awaitDurableAppend");
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toMatchObject({ ackEligible: false, stage: "registration", reason: "conflict" });
    expect(register).toHaveBeenCalledOnce(); expect(barrier).not.toHaveBeenCalled();
    expect(history(context).events[0]!.result).toEqual(request.event.result);
  });

  it.each(["missing", "empty", "bad-digest"] as const)("never substitutes a receipt for %s raw result evidence", async invalid => {
    const context = await fixture(), request = context.request();
    if (invalid === "missing") Object.assign(request.event, { result: undefined });
    else if (invalid === "empty") Object.assign(request.event, { result: raw("") });
    else Object.assign(request.event.result, { sha256: "0".repeat(64) });
    const register = vi.spyOn(context.store, "registerNativeEntry");
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toEqual({ ackEligible: false, stage: "attempt", reason: "invalid-evidence" });
    expect(register).not.toHaveBeenCalled();
    expect(history(context).events).toHaveLength(0);
  });

  it("allows absent host joins rather than inventing input/effect parsing rules for arbitrary native details", async () => {
    const context = await fixture(), request = context.request();
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, { event: request.event, nativeEntry: request.nativeEntry })).toMatchObject({ ackEligible: true });
  });

  it("snapshots all caller buffers/metadata before the first await and preserves the original deadline/signal identity", async () => {
    const context = await fixture(), request = context.request();
    const controller = new AbortController(), replacement = new AbortController(); replacement.abort();
    Object.assign(request.nativeEntry, { signal: controller.signal });
    // Include Buffer subarray views, not only standalone Uint8Arrays.
    const resultBacking = Buffer.concat([Buffer.from("prefix"), Buffer.from(request.event.result.bytes), Buffer.from("suffix")]);
    Object.assign(request.event.result, { bytes: resultBacking.subarray(6, -6) });
    const nativeBacking = Buffer.concat([Buffer.from("prefix"), Buffer.from(request.nativeEntry.bytes), Buffer.from("suffix")]);
    Object.assign(request.nativeEntry, { bytes: nativeBacking.subarray(6, -6) });
    const expectedEvent = structuredClone(request.event), expectedNative = Uint8Array.from(request.nativeEntry.bytes);
    const originalDeadline = request.nativeEntry.deadlineMs;
    const gate = deferred(), append = context.ledger.appendEvent.bind(context.ledger);
    const appendSpy = vi.spyOn(context.ledger, "appendEvent").mockImplementation(async (event, options) => {
      await gate.promise;
      return append(event, options);
    });
    const register = vi.spyOn(context.store, "registerNativeEntry"), barrier = vi.spyOn(context.store, "awaitDurableAppend");
    const pending = persistRecoveryAttemptCompletion(context.ledger, context.store, request);
    request.event.result.bytes.fill(0); request.event.receipt!.bytes.fill(0); request.event.traces[0]!.bytes.fill(0);
    Object.assign(request.event.traces[0]!.storage, { receipt: "changed", objectKey: "changed" });
    Object.assign(request.event.identity, { sessionId: "changed", acceptedInputId: "changed", toolCallId: "changed", effectKey: "changed", attemptId: "changed" });
    Object.assign(request.event, { eventId: "changed", observedAt: 999, state: "failed" });
    request.nativeEntry.bytes.fill(0);
    Object.assign(request.nativeEntry.location, { file: "changed", offset: 0, length: 0 });
    Object.assign(request.nativeEntry.flushReceipt, { file: "changed", bytesThrough: 0 });
    Object.assign(request.nativeEntry, { sessionId: "changed", deliveryId: "changed", entryId: "changed", bytesSha256: "changed", deadlineMs: 0, signal: replacement.signal });
    Object.assign(request.nativeCorrelation!, { acceptedInputId: "changed", effectKey: "changed", attemptId: "changed" });
    gate.resolve();
    const result = await pending;
    expect(result.ackEligible).toBe(true);
    const event = history(context).events[0]!;
    expect({ ...event, seq: undefined, kind: undefined }).toEqual({ ...expectedEvent, seq: undefined, kind: undefined });
    expect(appendSpy.mock.calls[0]![1]).toEqual({ deadlineMs: originalDeadline, signal: controller.signal });
    expect(register.mock.calls[0]![0].signal).toBe(controller.signal);
    expect(register.mock.calls[0]![0].deadlineMs).toBe(originalDeadline);
    expect(register.mock.calls[0]![0].bytes).toEqual(expectedNative);
    expect(barrier.mock.calls[0]![0]).toEqual({ sessionId: "session", deliveryId: register.mock.calls[0]![0].deliveryId, recordKind: "carrier", deadlineMs: originalDeadline, signal: controller.signal });
  });

  for (const stop of ["deadline", "aborted"] as const) {
    it.each(["attempt", "registration", "barrier"] as const)(`refuses ACK when original ${stop} becomes effective after the %s await, retaining completed writes`, async stage => {
      const context = await fixture(), request = context.request();
      const controller = new AbortController(); Object.assign(request.nativeEntry, { signal: controller.signal });
      const finish = () => { if (stop === "aborted") controller.abort(); else vi.spyOn(Date, "now").mockReturnValue(request.nativeEntry.deadlineMs); };
      const realBarrier = context.store.awaitDurableAppend.bind(context.store);
      const barrier = vi.spyOn(context.store, "awaitDurableAppend");
      if (stage === "attempt") {
        const append = context.ledger.appendEvent.bind(context.ledger);
        vi.spyOn(context.ledger, "appendEvent").mockImplementation(async (event, options) => { const result = await append(event, options); finish(); return result; });
      } else if (stage === "registration") {
        const register = context.store.registerNativeEntry.bind(context.store);
        vi.spyOn(context.store, "registerNativeEntry").mockImplementation(async descriptor => { const result = await register(descriptor); finish(); return result; });
      } else {
        barrier.mockImplementation(async options => { const result = await realBarrier(options); finish(); return result; });
      }
      const result = await persistRecoveryAttemptCompletion(context.ledger, context.store, request);
      expect(result).toMatchObject({ ackEligible: false, stage: stage === "barrier" ? "barrier" : "registration", reason: stop, attemptSeq: expect.any(Number) });
      expect(history(context).events[0]!.result).toEqual(request.event.result);
      expect(history(context).events[0]!.state).toBe("committed");
      if (stage === "barrier") {
        expect(result).toHaveProperty("barrierSeq", expect.any(Number));
        expect(await context.store.readDurableEntry(tuple(request))).toMatchObject({ found: true });
      } else expect(barrier).not.toHaveBeenCalled();
    });
  }

  it.each(["deadline", "aborted"] as const)("does not refresh an already %s original budget", async reason => {
    const context = await fixture(), request = context.request(), controller = new AbortController();
    if (reason === "aborted") controller.abort();
    Object.assign(request.nativeEntry, { deadlineMs: reason === "deadline" ? Date.now() - 1 : request.nativeEntry.deadlineMs, signal: controller.signal });
    const register = vi.spyOn(context.store, "registerNativeEntry");
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toEqual({ ackEligible: false, stage: "attempt", reason });
    expect(register).not.toHaveBeenCalled();
    expect(history(context).events).toHaveLength(0);
    expect(history(context).attempt.state).toBe("intent");
  });

  it("does not collapse identical logical results from distinct physical attempts or distinct events", async () => {
    const context = await fixture(), secondIdentity = identity(2);
    await context.ledger.recordAttempt(attempt(secondIdentity), bounds());
    const requests = [context.request(identity(), "result-1"), context.request(secondIdentity, "result-1"), context.request(identity(), "result-2")];
    const results = [];
    for (const request of requests) results.push(await persistRecoveryAttemptCompletion(context.ledger, context.store, request));
    expect(results.every(result => result.ackEligible)).toBe(true);
    expect(new Set(results.map(result => "attemptSeq" in result ? result.attemptSeq : undefined)).size).toBe(3);
    expect(new Set(results.map(result => result.ackEligible ? result.barrierSeq : undefined)).size).toBe(3);
    expect(history(context).events).toHaveLength(2);
    expect(history(context, secondIdentity).events).toHaveLength(1);
    expect(context.ledger.entries()).toHaveLength(5);
    for (const request of requests) expect(await context.store.readDurableEntry(tuple(request))).toMatchObject({ found: true });
  });

  it("refuses changed bytes under the same immutable event ID without overwriting the original raw result", async () => {
    const context = await fixture(), request = context.request();
    const first = await persistRecoveryAttemptCompletion(context.ledger, context.store, request);
    const originalResult = structuredClone(request.event.result);
    Object.assign(request.event, { result: raw("different raw result") });
    const register = vi.spyOn(context.store, "registerNativeEntry");
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toEqual({ ackEligible: false, stage: "attempt", reason: "conflict" });
    expect(register).not.toHaveBeenCalled();
    expect(history(context).events).toHaveLength(1);
    expect(history(context).events[0]!.result).toEqual(originalResult);
    expect(first.ackEligible).toBe(true);
  });

  it("returns typed unavailable registration when the real store is closed, without undoing the raw business outcome", async () => {
    const context = await fixture(), request = context.request();
    await context.store.close();
    expect(await persistRecoveryAttemptCompletion(context.ledger, context.store, request)).toMatchObject({ ackEligible: false, stage: "registration", reason: "store-unavailable" });
    expect(history(context).events[0]!.state).toBe("committed");
    expect(history(context).events[0]!.result).toEqual(request.event.result);
  });
});
