// TEST ONLY: in-memory behavioral double. No native permission, durability or crash proof.
import type { RecoveryInputOutcomeRecord, RecoveryInputSnapshot } from "../../../src/recovery/input-admission.js";
import type { RepairRef } from "../../../src/recovery/maintenance-admission.js";
import { UNKNOWN_ACK_MESSAGE, type AdmissionHarness, type B5Admission } from "./types.js";

const copy = <T>(x: T): T => JSON.parse(JSON.stringify(x)) as T;
class FakeAdmissionError extends Error {
  override readonly name = "RecoveryAdmissionError";
  constructor(readonly code: string, message: string) { super(message); }
}
const fail = (code: string, message: string): never => { throw new FakeAdmissionError(code, message); };
let nextSession = 0;

export function createTestOnlyFakeHarness(): AdmissionHarness {
  const sessionId = `test-only-b5-${++nextSession}`, originalInputId = "original";
  const deadlineMs = Date.now() + 120_000;
  let required = false, bound = false, exists = false, joined = false, revision = 0;
  let generation: string | undefined, repair: RepairRef | undefined;
  let continuation: { admissionId: string; text: string } | undefined;
  let outcomes: RecoveryInputOutcomeRecord[] = [];
  let lostAck = false;
  const assertSession = (id: string) => {
    if (id !== sessionId || !bound) fail("RECOVERY_NOT_BOUND", "No REQUIRED binding");
  };
  const read = (id: string, io?: { deadlineMs?: number; signal?: AbortSignal }) => {
    assertSession(id);
    if (io?.signal?.aborted) fail("RECOVERY_ABORTED", "Recovery I/O was aborted");
    if (io?.deadlineMs !== undefined && Date.now() >= io.deadlineMs) fail("RECOVERY_DEADLINE", "Recovery I/O deadline expired");
    if (!exists) fail("RECOVERY_HISTORY_INCOMPLETE", "Required session ledger missing");
  };
  const write = () => {
    revision++;
    if (lostAck) { lostAck = false; throw new Error(UNKNOWN_ACK_MESSAGE); }
  };
  const terminal = () => outcomes.find(o => o.kind !== "lifecycle-interrupt");
  const original = (id: string) => {
    if (!joined || id !== originalInputId) fail("RECOVERY_HISTORY_INCOMPLETE", "Original is absent");
  };
  const pin = (g: string) => {
    if (!generation) fail("RECOVERY_HISTORY_INCOMPLETE", "Missing durable original generation pin");
    if (g !== generation) fail("RECOVERY_CONFLICT", "Recovery generation differs from original pin");
  };
  const recovery = { epoch: { processIncarnationId: "test-only-process", bindingId: "test-only-binding", epoch: 0 },
    podId: "original-pod", bootId: "test-only-boot", historyComplete: true,
    assertCurrent() {}, reconsiderInput() {},
  };
  const initialization = { mode: "NEW" as const, root: `test-only-memory://${sessionId}`, sessionId, recovery,
    assertNew() { if (required) fail("RECOVERY_CONFLICT", "Authoritative fixture identity is REQUIRED"); },
  };
  const maintenanceHistory = async () => {
    try { read(sessionId); return { ok: true as const }; }
    catch (error) {
      const code = error instanceof FakeAdmissionError && error.code === "RECOVERY_HISTORY_INCOMPLETE"
        ? "RECOVERY_HISTORY_INCOMPLETE" as const : "RECOVERY_MAINTENANCE_UNCERTAIN" as const;
      return { ok: false as const, code, message: error instanceof Error ? error.message : "Unknown failure" };
    }
  };
  const admission: B5Admission = {
    initializeNewRecoverySession(request, io) {
      if (request.mode !== "NEW" || request.sessionId !== sessionId) fail("RECOVERY_INVALID", "Explicit NEW required");
      if (io?.signal?.aborted) fail("RECOVERY_ABORTED", "Aborted");
      if (io?.deadlineMs !== undefined && Date.now() >= io.deadlineMs) fail("RECOVERY_DEADLINE", "Expired");
      if (bound || exists) fail("RECOVERY_CONFLICT", "NEW never overwrites or initializes a bound session");
      if (request.assertNew() !== undefined) fail("RECOVERY_CONFLICT", "NEW assertion must be synchronous");
      if (request.recovery.assertCurrent() !== undefined) fail("RECOVERY_EPOCH_REVOKED", "Current assertion must be synchronous");
      if (!request.recovery.historyComplete) fail("RECOVERY_HISTORY_INCOMPLETE", "Complete history required");
      exists = true; revision = 0;
      if (lostAck) { lostAck = false; throw new Error(UNKNOWN_ACK_MESSAGE); }
    },
    async readRecoveryInput(id, io) {
      try { read(id, io); }
      catch (error) {
        if (error instanceof FakeAdmissionError && error.code === "RECOVERY_HISTORY_INCOMPLETE") {
          return { sessionId, complete: false, revision: 0, queued: [], outcomes: [] };
        }
        throw error;
      }
      const last = outcomes.at(-1), end = terminal();
      const snapshot: RecoveryInputSnapshot = { sessionId, complete: true, revision, queued: [], outcomes: copy(outcomes),
        ...(joined && !end ? { accepted: { inputId: originalInputId, text: " exact original bytes K\n", podId: "original-pod",
          bootId: "boot-0", settlement: "open" as const, cancellation: "not-cancelled" as const,
          deadlineMs, admissionId: `test-only-original:${sessionId}`,
          delivery: { entryId: "original-entry", bytesSha256: "a".repeat(64), seq: 1, executionBranchEntryId: "anchor-A" },
          ...(last ? { lastOutcome: copy(last) } : {}),
        } } : {}),
      };
      return snapshot;
    },
    async ensureRecoveryGeneration(id, originalId, io) {
      read(id, io); original(originalId);
      if (generation) return generation;
      if (terminal()) return fail("RECOVERY_INPUT_TERMINAL", "Cannot allocate for terminal input");
      generation = `test-only-generation:${sessionId}`; write(); return generation;
    },
    async admitContinuation(request) {
      read(request.sessionId); original(request.originalInputId); pin(request.recoveryGeneration);
      if (continuation) {
        if (request.text !== continuation.text) fail("RECOVERY_CONFLICT", "Changed continuation under same generation");
        return { admissionId: continuation.admissionId, existing: true };
      }
      if (terminal()) fail("RECOVERY_INPUT_TERMINAL", "Terminal input");
      if (Date.now() >= deadlineMs) {
        const error = new FakeAdmissionError("SCRIPTED_INPUT_EXPIRED", "Original deadline expired");
        Object.defineProperty(error, "name", { value: "ScriptedInputAdmissionError" }); throw error;
      }
      if (!request.text.trim()) fail("SCRIPTED_INPUT_INVALID", "Continuation text required");
      continuation = { admissionId: `test-only-continuation:${sessionId}:${generation}`, text: request.text };
      write(); return { admissionId: continuation.admissionId, existing: false };
    },
    async recordRecoveryRepair(id, ref, io) {
      read(id, io); original(ref.originalInputId); pin(ref.recoveryGeneration);
      if (repair) {
        if (JSON.stringify(repair) !== JSON.stringify(ref)) fail("RECOVERY_CONFLICT", "Changed retained repair");
        return;
      }
      if (terminal() || ref.executionBranchEntryId !== "anchor-A" || ref.repairReceiptSha256 !== "a".repeat(64)) {
        fail("RECOVERY_CONFLICT", "Not the joined fixture repair");
      }
      repair = copy(ref); write();
    },
    async recordInputOutcome(request) {
      try {
        read(request.sessionId, request); original(request.originalInputId);
        if (request.kind === "settled") fail("RECOVERY_HISTORY_INCOMPLETE", "Fake does not authenticate native terminal witnesses");
        const old = outcomes.find(o => o.requestId === request.requestId);
        if (old) {
          const { revision: _r, ...payload } = old;
          const { deadlineMs: _d, signal: _s, ...offered } = request;
          if (JSON.stringify(payload) !== JSON.stringify(offered)) fail("RECOVERY_CONFLICT", "Changed outcome");
          return { durable: true, status: "existing", revision: old.revision };
        }
        const end = terminal();
        if (end) return { durable: true, status: "already-terminal", revision: end.revision, kind: end.kind as "user-cancel" | "settled" };
        const { deadlineMs: _d, signal: _s, ...payload } = request;
        outcomes.push({ ...copy(payload), revision: revision + 1 }); write();
        return { durable: true, status: "recorded", revision };
      } catch (error) {
        const code = error instanceof FakeAdmissionError ? error.code : "unknown";
        return { durable: false, reason: code === "RECOVERY_HISTORY_INCOMPLETE" ? "incomplete-history" :
          code === "RECOVERY_DEADLINE" ? "deadline" : code === "RECOVERY_ABORTED" ? "aborted" :
          code === "RECOVERY_CONFLICT" ? "conflict" : code === "RECOVERY_NOT_BOUND" ? "superseded" : "store-unavailable" };
      }
    },
    maintenance: {
      async read() { const status = await maintenanceHistory(); return status.ok ? { ok: true, value: { revision } } : status; },
      async listPending(_id, limit) {
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > 256) return { ok: false, code: "RECOVERY_MAINTENANCE_INVALID", message: "Limit must be 1..256" };
        const status = await maintenanceHistory(); return status.ok ? { ok: true, value: { revision, records: [] } } : status;
      },
    },
  };
  return {
    admission, sessionId, originalInputId, deadlineMs, initialization,
    requireIdentity() { required = true; },
    bindRequired() { if (bound) fail("RECOVERY_CONFLICT", "Already bound"); required = true; bound = true; },
    unbind() { bound = false; }, rebindRequired() { required = true; bound = true; },
    async seedJoinedOriginal() {
      read(sessionId); joined = true; revision += 6;
      outcomes = [{ sessionId, originalInputId, requestId: "joined-interruption", kind: "lifecycle-interrupt",
        at: Date.now(), bootId: "boot-0", revision: revision - 2 }];
    },
    repairFor(recoveryGeneration) { return { originalInputId, recoveryGeneration,
      executionBranchEntryId: "anchor-A", repairReceiptSha256: "a".repeat(64) }; },
    observe() {
      const pins = generation ? [{ originalInputId, admissionId: `test-only-original:${sessionId}`, recoveryGeneration: generation }] : [];
      return copy({ revision: exists ? revision : undefined, pins: exists ? pins : [],
        continuationIds: exists && continuation ? [continuation.admissionId] : [], repairs: exists && repair ? [repair] : [],
        storage: JSON.stringify(exists ? { revision, pins, continuation, repair, outcomes, joined } : null), nativeDeliveries: 0,
      });
    },
    removeLedger() { exists = false; },
    async withLostWriteAck(writeOperation) {
      lostAck = true;
      try { return await writeOperation(); } finally { lostAck = false; }
    },
    close() { bound = false; exists = false; },
  };
}
