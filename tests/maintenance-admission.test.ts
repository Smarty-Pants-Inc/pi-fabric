import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  admitBoundScriptedInput, admitContinuation, assertRecoveryInputPermission, bindInputAdmission,
  commitRecoveryInputDelivery, commitRecoverySession, ensureRecoveryGeneration, readRecoveryInput, readRecoverySession,
  recordInputOutcome, recordRecoveryRepair, recoveryCanonical, recoveryHash,
  releaseRecoveryInputAfterJoin, selectRecoveryInput, RecoveryAdmissionError,
  type AcceptedRecoveryInput, type InputAdmissionMainAgent, type RecoveryInputOutcomeRequest, type RecoveryAdmissionOwner,
} from "../src/recovery/input-admission.js";
import {
  RECOVERY_MAINTENANCE_CODES, RecoveryMaintenanceError, createRecoveryMaintenanceAdapter,
  type AcceptedRecoveryMaintenance, type MaintenanceCompletion, type MaintenanceOperation,
  type MaintenanceRecord, type MaintenanceRequest, type MaintenanceResult, type NativePlanRef, type RecoveryEpoch,
} from "../src/recovery/maintenance-admission.js";

// Keep: real fsync/rename/checksum/reopen, trusted native/B5 port boundary, data safety,
// unknown-ACK/crash and two race orders. Native/B6 WAL evidence callbacks here are boundary
// fixtures, NOT the B3 native writer or B6 qualification, and no physical host bar is claimed.
const cleanups: Array<() => void> = [];
afterEach(() => { vi.restoreAllMocks(); for (const close of cleanups.splice(0)) close(); });
const take = <T>(r: MaintenanceResult<T>): T => { if (!r.ok) throw new Error(`${r.code}: ${r.message}`); return r.value; };
const refused = (r: MaintenanceResult<unknown>, code: string) => expect(r).toMatchObject({ ok: false, code: code.startsWith("RECOVERY_") ? code : `RECOVERY_MAINTENANCE_${code}` });
const deferred = <T = void>() => { let resolve!: (value: T) => void; let reject!: (e: Error) => void; const promise = new Promise<T>((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const epoch = (id = "owner-a", n = 0): RecoveryEpoch => ({ processIncarnationId: "process-a", bindingId: id, epoch: n });
const D = recoveryHash("native history");
const fixture = (options: { complete?: boolean; root?: string; sessionId?: string; epoch?: RecoveryEpoch; owner?: RecoveryAdmissionOwner } = {}) => {
  const root = options.root ?? fs.mkdtempSync(path.join(os.tmpdir(), "maintenance-admission-"));
  const sessionId = options.sessionId ?? `session-${path.basename(root)}`;
  let currentEpoch = options.epoch ?? epoch(); let live = true; let drive: (() => Promise<void>) | undefined;
  const drives: string[] = []; const native: string[] = [];
  const mainAgent: InputAdmissionMainAgent = { id: `session:${sessionId}`, local: true, sessionId,
    deliverAgent: () => { throw new Error("REQUIRED admission/continuation must not submit through native Main"); } };
  let unbind: () => void;
  const bind = () => {
    unbind = bindInputAdmission({ root, sessionId, mainAgent, recovery: options.owner ?? {
      get epoch() { return currentEpoch; }, podId: `pod-${currentEpoch.bindingId}`, bootId: currentEpoch.processIncarnationId, historyComplete: options.complete ?? true,
      assertCurrent: () => { if (!live) throw new RecoveryAdmissionError("RECOVERY_EPOCH_REVOKED", "old writer positively excluded"); },
      reconsiderInput: async () => { drives.push("drive"); if (drive) await drive(); },
      validateRepair: ref => { expect(ref.repairReceiptSha256).toBe(D); },
    } }); cleanups.push(unbind);
  };
  bind();
  const api = createRecoveryMaintenanceAdapter();
  const io = () => ({ deadlineMs: Date.now() + 30_000 });
  const request = (requestId = "compact-1", operation: MaintenanceOperation = { kind: "compact", reason: "manual" }): MaintenanceRequest => ({
    version: 1, admissionKind: "maintenance", sessionId, requestId, epoch: currentEpoch, operation,
    baseLeafId: "branch-1", baseHistorySha256: D, viewSha256: D, issuedAtMs: Date.now(), deadlineMs: Date.now() + 60_000,
  });
  const key = (requestId = "compact-1") => ({ sessionId, requestId });
  const snapshot = () => readRecoverySession(sessionId);
  const admitRound = (inputId = "round-1", deadlineAt = Date.now() + 120_000) => admitBoundScriptedInput({ sessionId, request: {
    from: { id: "driver", name: "Driver", kind: "main" }, message: `real text ${inputId}`, delivery: "steer", priority: "interrupt", triggerTurn: true,
    data: { scriptedInput: { version: 1, inputId, sessionId, deadlineAt } },
  } });
  const select = async (inputId = "round-1") => {
    const selected = await selectRecoveryInput({ sessionId, originalInputId: inputId, epoch: currentEpoch, expectedRevision: snapshot().revision });
    if (!selected.selected) throw new Error(selected.code);
    return selected.input;
  };
  const delivery = async (input: AcceptedRecoveryInput) => {
    await commitRecoveryInputDelivery(input, { entryId: `input-${input.originalInputId}`, bytesSha256: D, seq: 10, executionBranchEntryId: "branch-1" });
  };
  const outcome = (originalInputId = "round-1", kind: RecoveryInputOutcomeRequest["kind"] = "lifecycle-interrupt", requestId = `${kind}-${originalInputId}`): RecoveryInputOutcomeRequest => ({
    sessionId, originalInputId, kind, requestId, at: Date.now(), bootId: currentEpoch.processIncarnationId, deadlineMs: Date.now() + 20_000,
    ...(kind === "settled" ? { terminalEvidence: { entryId: "witness", bytesSha256: D, seq: 12, inputEntryId: `input-${originalInputId}`,
      executionBranchEntryId: "branch-1", runId: "run-1", finalEntryIds: ["final-assistant"] } } : {}),
  });
  const begin = async (r = request()) => {
    const admitted = take(await api.admitMaintenance(r));
    return take(await api.beginMaintenance(key(r.requestId), currentEpoch, admitted.revision));
  };
  // Native test port: exact retained JSON+LF and stable B6 receipt references. B5 never
  // sees summary bytes, appends the native file, or fabricates those receipts itself.
  const plans = new Map<string, { ref: NativePlanRef; apply: () => MaintenanceCompletion }>();
  const plan = (requestId = "compact-1", outcome: MaintenanceCompletion["outcome"] = "completed", errorCode?: string) => {
    const old = plans.get(requestId); if (old) return old;
    const prepareId = `prepare-${requestId}`, checkpointId = `checkpoint-${requestId}`;
    const prepareBytes = `${JSON.stringify({ type: "pi-maintenance:prepare:v1", requestId, summary: "PRIVATE NATIVE SUMMARY", outcome })}\n`;
    const checkpointBytes = `${JSON.stringify({ type: "pi-maintenance:checkpoint:v1", requestId, parentId: "branch-1", outcome })}\n`;
    const append = (id: string, bytes: string) => {
      if (!native.includes(id)) { const fd = fs.openSync(path.join(root, "native-fixture.jsonl"), "a"); try { fs.writeSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); } native.push(id); }
    };
    append(prepareId, prepareBytes);
    const ref = { entryId: prepareId, bytesSha256: recoveryHash(prepareBytes), planSha256: recoveryHash(`plan:${prepareBytes}`) };
    const p = { ref, apply: () => {
      expect(snapshot().maintenance.find(e => e.record.request.requestId === requestId)?.record.state).toBe("PREPARED");
      append(checkpointId, checkpointBytes);
      return { outcome, ...(errorCode ? { errorCode } : {}), checkpointEntryId: checkpointId,
        entries: [{ entryId: prepareId, bytesSha256: ref.bytesSha256, seq: 30 }, { entryId: checkpointId, bytesSha256: recoveryHash(checkpointBytes), seq: 31 }] };
    } }; plans.set(requestId, p); return p;
  };
  const finish = async (requestId = "compact-1", opts: { joined?: Promise<void>; outcome?: MaintenanceCompletion["outcome"]; errorCode?: string; reconcile?: (record: MaintenanceRecord) => Promise<void>; taskAbortRequestId?: string } = {}) =>
    api.reconcileAfterJoin(key(requestId), currentEpoch, { ...io(), joined: opts.joined ?? Promise.resolve(),
      settle: async (_record, prepare) => { const p = plan(requestId, opts.outcome, opts.errorCode); take(await prepare(p.ref)); return p.apply(); },
      reconcile: opts.reconcile ?? (async () => {}), ...(opts.taskAbortRequestId ? { taskAbortRequestId: opts.taskAbortRequestId } : {}),
    });
  return { root, sessionId, mainAgent, api, key, io, request, snapshot, admitRound, select, delivery, outcome, begin, plan, finish, native, drives,
    get epoch() { return currentEpoch; }, advanceEpoch: () => { currentEpoch = { ...currentEpoch, epoch: currentEpoch.epoch + 1 }; }, setDrive: (callback: () => Promise<void>) => { drive = callback; },
    rebind: (next = epoch("owner-b")) => { unbind(); currentEpoch = next; bind(); }, revoke: () => { live = false; },
  };
};

describe.skipIf(process.platform === "win32")("durable maintenance admission and one-operation authority", () => {
  it("exports the closed refusal vocabulary and exact adapter methods", () => {
    expect(new Set(RECOVERY_MAINTENANCE_CODES).size).toBe(17);
    expect(Object.keys(createRecoveryMaintenanceAdapter())).toEqual(["admitMaintenance", "beginMaintenance", "prepareMaintenance", "consumeMaintenance", "adoptMaintenance", "read", "listPending", "withMaintenanceAttempt", "assertMaintenanceGate", "cancelMaintenance", "reconcileAfterJoin", "captureTaskAbort", "releaseLostMaintenance"]);
  });
  it("durably dedupes identity across unknown admission ACK, keeps the original deadline, and stores no native content", async () => {
    const f = fixture(); const r = f.request();
    const a = take(await f.api.admitMaintenance(r));
    const revision = f.snapshot().revision;
    const duplicate = take(await f.api.admitMaintenance(JSON.parse(JSON.stringify(r))));
    expect(duplicate).toEqual(a); expect(f.snapshot().revision).toBe(revision);
    refused(await f.api.admitMaintenance({ ...r, viewSha256: recoveryHash("changed") }), "CONFLICT");
    f.rebind();
    expect(take(await f.api.admitMaintenance(r))).toEqual(a);
    const p = f.plan();
    const pathName = path.join(f.root, "input-admission", "v1", "session", `${recoveryHash(f.sessionId)}.json`);
    expect(fs.readFileSync(pathName, "utf8")).not.toContain("PRIVATE NATIVE SUMMARY");
    expect(a.request.deadlineMs).toBe(r.deadlineMs); expect(p.ref.bytesSha256).toMatch(/^[a-f0-9]{64}$/);
  });
  it.each(["text", "message", "content", "instructions", "customInstructions", "customPrompt", "prompt", "label", "summary", "data"])("refuses %s even when empty/undefined, at every closed-schema level", async field => {
    const f = fixture();
    for (const value of ["", undefined]) {
      refused(await f.api.admitMaintenance({ ...f.request(), [field]: value } as MaintenanceRequest), "USER_TEXT");
      refused(await f.api.admitMaintenance({ ...f.request(), operation: { kind: "compact", reason: "manual", [field]: value } } as MaintenanceRequest), "USER_TEXT");
    }
    expect(f.snapshot().maintenance).toEqual([]);
  });
  it.each([
    { version: 2 }, { admissionKind: "input" }, { unexpected: true }, { baseHistorySha256: "bad" }, { issuedAtMs: 0 },
    { epoch: { processIncarnationId: "p", epoch: 0 } }, { operation: { kind: "tree-nav", targetEntryId: "pi-maintenance:prepare:v1", summarize: false } },
    { operation: { kind: "compact", reason: "model-switch" } }, { operation: { kind: "compact", reason: "manual", targetModel: undefined } },
    { operation: { kind: "tree-nav", targetEntryId: null, summarize: false, reason: "manual" } },
    { repair: undefined },
  ])("refuses malformed/unknown nested payload %j without mutation", async change => {
    const f = fixture(); refused(await f.api.admitMaintenance({ ...f.request(), ...change } as MaintenanceRequest), "INVALID"); expect(f.snapshot().revision).toBe(0);
  });
  it("never invokes getters while validating operations and rejects unknown kinds", async () => {
    const f = fixture(); let called = false;
    const operation = Object.defineProperty({}, "kind", { enumerable: true, get: () => { called = true; return "compact"; } });
    refused(await f.api.admitMaintenance({ ...f.request(), operation } as MaintenanceRequest), "INVALID"); expect(called).toBe(false);
    refused(await f.api.admitMaintenance({ ...f.request(), operation: { kind: "bug-report" } } as unknown as MaintenanceRequest), "KIND_MISMATCH");
  });
  it("pins the model-switch target and accepts inputless root/no-summary navigation", async () => {
    const f = fixture(); const r = f.request("switch", { kind: "compact", reason: "model-switch", targetModel: { provider: "physical", id: "smaller" } });
    const a = take(await f.api.admitMaintenance(r)); expect(a.request.operation).toEqual(r.operation);
    refused(await f.api.admitMaintenance({ ...r, operation: { kind: "compact", reason: "model-switch", targetModel: { provider: "physical", id: "changed" } } }), "CONFLICT");
    take(await f.finish("switch", { outcome: "noop" }));
    const t = await f.begin(f.request("root", { kind: "tree-nav", targetEntryId: null, summarize: false }));
    await f.api.withMaintenanceAttempt(t.token, async () => {
      expect(() => f.api.assertMaintenanceGate(t.token, "tree-summary", D)).toThrow(/purpose|operation/i);
    });
    take(await f.finish("root", { outcome: "noop" }));
  });
  it("allows one scoped attempt, rejects copied tokens, wrong purposes/views and every post-prepare inference", async () => {
    const f = fixture(); const b = await f.begin();
    refused(await f.api.beginMaintenance(f.key(), f.epoch, f.snapshot().revision), "TOKEN_REUSED");
    await expect(f.api.withMaintenanceAttempt({ ...b.token }, async () => {})).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_TOKEN_REUSED" });
    await f.api.withMaintenanceAttempt(b.token, async () => {
      f.api.assertMaintenanceGate(b.token, "compact-summary", D);
      expect(() => f.api.assertMaintenanceGate({ ...b.token }, "compact-summary", D)).toThrow(/copied|private/i);
      for (const purpose of ["turn", "bug-report", "unknown"] as const) expect(() => f.api.assertMaintenanceGate(b.token, purpose as "turn", D)).toThrow(/inference/i);
      expect(() => f.api.assertMaintenanceGate(b.token, "tree-summary", D)).toThrow(/operation/i);
      expect(() => f.api.assertMaintenanceGate(b.token, "compact-summary", recoveryHash("different"))).toThrow(/view/i);
      const p = f.plan(); take(await f.api.prepareMaintenance(b.token, p.ref));
      expect(() => f.api.assertMaintenanceGate(b.token, "compact-summary", D)).toThrow(/ACTIVE/i);
      expect(take(await f.api.prepareMaintenance(b.token, p.ref)).state).toBe("PREPARED");
      refused(await f.api.prepareMaintenance(b.token, { ...p.ref, planSha256: recoveryHash("different") }), "UNCERTAIN");
      refused(await f.api.consumeMaintenance(b.token, p.apply()), "UNCERTAIN");
    });
    await expect(f.api.withMaintenanceAttempt(b.token, async () => {})).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_TOKEN_REUSED" });
    const consumed = take(await f.finish());
    expect(take(await f.api.consumeMaintenance(b.token, consumed.completion!))).toEqual(consumed);
    refused(await f.api.consumeMaintenance(b.token, { ...consumed.completion!, outcome: "noop" }), "CONFLICT");
    expect(f.snapshot().outcomes).toEqual([]); // inputless maintenance emits no input outcome
  });
  it("refuses an unbound gate/adapter and revokes detached ALS descendants after the one attempt ends", async () => {
    const unbound = createRecoveryMaintenanceAdapter(); refused(await unbound.read({ sessionId: "missing", requestId: "missing" }), "UNBOUND");
    const f = fixture(); const b = await f.begin(); const later = deferred(); let descendant!: Promise<void>;
    await f.api.withMaintenanceAttempt(b.token, async () => {
      descendant = later.promise.then(() => { expect(() => f.api.assertMaintenanceGate(b.token, "compact-summary", D)).toThrow(/ended|live/i); });
    });
    later.resolve(); await descendant;
    expect(f.snapshot().maintenance[0]!.record.state).toBe("ADMITTED");
    refused(await f.api.beginMaintenance(f.key(), f.epoch, f.snapshot().revision), "TOKEN_REUSED");
    take(await f.finish("compact-1", { outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_INTERRUPTED" }));
  });
  it.each(["completed", "noop", "cancelled", "failed"] as const)("consumes truthful %s only after actual join, B6 receipts, then reconciliation/release", async outcome => {
    const f = fixture(); await f.begin(); const join = deferred(); const reconciled: string[] = [];
    const pending = f.finish("compact-1", { joined: join.promise, outcome, ...(outcome === "cancelled" || outcome === "failed" ? { errorCode: `RECOVERY_MAINTENANCE_${outcome.toUpperCase()}` } : {}),
      reconcile: async record => { expect(record.state).toBe("CONSUMED"); expect(f.snapshot().reservation?.kind).toBe("maintenance"); reconciled.push("after-consume"); } });
    await Promise.resolve(); expect(f.snapshot().maintenance[0]!.record.state).toBe("ACTIVE"); expect(f.native).toEqual([]);
    join.resolve(); const end = take(await pending);
    expect(end.completion?.outcome).toBe(outcome); expect(f.snapshot().reservation).toBeUndefined(); expect(reconciled).toEqual(["after-consume"]); expect(f.drives).toHaveLength(1);
    expect(take(await f.finish())).toEqual(end); expect(f.drives).toHaveLength(1); expect(f.native).toHaveLength(2);
  });
  it.each(["provider", "hook", "native-prepare", "join", "B6", "reconcile"])("does not strand ACTIVE or loop after %s failure", async phase => {
    const f = fixture(); const b = await f.begin();
    if (["provider", "hook", "native-prepare"].includes(phase)) {
      await expect(f.api.withMaintenanceAttempt(b.token, async () => { throw new RecoveryMaintenanceError(phase === "hook" ? "RECOVERY_MAINTENANCE_HOOK_CONTENT" : "RECOVERY_MAINTENANCE_UNCERTAIN", phase); })).rejects.toThrow(phase);
      expect(f.snapshot().maintenance[0]!.record.state).not.toBe("ACTIVE");
      take(await f.finish("compact-1", { outcome: "failed", errorCode: `failure:${phase}` }));
    } else {
      let settle = 0;
      const joined = phase === "join" ? Promise.reject(new Error("join incomplete")) : Promise.resolve();
      const edge = { ...f.io(), joined, settle: async (_r: MaintenanceRecord, prepare: (p: NativePlanRef) => Promise<MaintenanceResult>) => {
        settle++; if (phase === "B6") throw new Error("B6 unavailable"); const p = f.plan(); take(await prepare(p.ref)); return p.apply();
      }, reconcile: async () => { if (phase === "reconcile") throw new Error("reconcile failed"); } };
      refused(await f.api.reconcileAfterJoin(f.key(), f.epoch, edge), "UNCERTAIN");
      refused(await f.api.reconcileAfterJoin(f.key(), f.epoch, edge), "UNCERTAIN");
      expect(settle).toBe(phase === "join" ? 0 : 1); expect(f.snapshot().maintenance[0]!.record.state).not.toBe("ACTIVE"); expect(f.snapshot().reservation?.kind).toBe("maintenance"); expect(f.drives).toHaveLength(0);
    }
  });
});

describe.skipIf(process.platform === "win32")("one serialized input/outcome/maintenance arbiter", () => {
  it("maintenance wins first durable commit: select is non-blocking BUSY, winner release drives queued input exactly once", async () => {
    const f = fixture(); f.admitRound(); const r = f.request(); const a = take(await f.api.admitMaintenance(r));
    const selected = await selectRecoveryInput({ sessionId: f.sessionId, originalInputId: "round-1", epoch: f.epoch, expectedRevision: f.snapshot().revision });
    expect(selected).toMatchObject({ selected: false, code: "RECOVERY_MAINTENANCE_BUSY" });
    expect(f.snapshot().inputs).toHaveLength(1); expect(f.snapshot().outcomes).toEqual([]);
    take(await f.api.beginMaintenance(f.key(), f.epoch, a.revision));
    f.setDrive(async () => { await f.select(); });
    take(await f.finish()); take(await f.finish());
    expect(f.snapshot().reservation).toMatchObject({ kind: "input", originalInputId: "round-1" }); expect(f.drives).toHaveLength(1);
  });
  it("round wins first durable commit: maintenance reports BUSY and releases to one B5 deliver without blocking", async () => {
    const f = fixture(); f.admitRound(); const first = await f.select();
    refused(await f.api.admitMaintenance(f.request()), "BUSY"); expect(f.snapshot().maintenance).toEqual([]);
    f.setDrive(async () => { expect(await f.select()).toMatchObject({ admissionId: first.admissionId }); });
    take(await f.api.releaseLostMaintenance(f.key(), f.epoch)); take(await f.api.releaseLostMaintenance(f.key(), f.epoch));
    expect(f.drives).toHaveLength(1); expect(f.snapshot().reservation).toMatchObject({ kind: "input" });
  });
  it("accept/select/outcome revisions share one owner; no transaction can await or reenter", async () => {
    const f = fixture(); const r = f.request(); f.admitRound(); const revision = f.snapshot().revision;
    const a = take(await f.api.admitMaintenance(r)); expect(a.revision).toBeGreaterThan(revision);
    f.admitRound("queued"); refused(await f.api.beginMaintenance(f.key(), f.epoch, a.revision), "CONFLICT");
    expect(() => commitRecoverySession(f.sessionId, () => Promise.resolve())).toThrow(/cannot await/i);
    expect(() => commitRecoverySession(f.sessionId, () => readRecoverySession(f.sessionId))).toThrow(/Reentrant/i);
    expect(f.snapshot().inputs.map(i => i.originalInputId)).toEqual(["round-1", "queued"]);
  });
  it("one round at a time, including interrupt-priority arrivals, with storage-only original and continuation", async () => {
    const f = fixture(); const first = f.admitRound(); const selected = await f.select(); f.admitRound("queued");
    expect(await selectRecoveryInput({ sessionId: f.sessionId, originalInputId: "queued", epoch: f.epoch, expectedRevision: f.snapshot().revision })).toMatchObject({ code: "RECOVERY_INPUT_BUSY" });
    await f.delivery(selected); assertRecoveryInputPermission(selected);
    expect(await recordInputOutcome(f.outcome())).toMatchObject({ durable: true, status: "recorded" });
    const joined = deferred(); let restored = 0;
    const release = releaseRecoveryInputAfterJoin(selected, joined.promise, async () => { restored++; });
    expect(f.snapshot().reservation?.kind).toBe("input"); joined.resolve(); await release;
    const recoveryGeneration = await ensureRecoveryGeneration(f.sessionId, "round-1");
    const continuation = { sessionId: f.sessionId, originalInputId: "round-1", recoveryGeneration, text: "resume exact original" };
    const a = await admitContinuation(continuation); const b = await admitContinuation(continuation);
    expect(b).toEqual({ ...a, existing: true }); expect(a.existing).toBe(false); expect(restored).toBe(1);
    expect(f.snapshot().inputs.find(i => i.admissionId === a.admissionId)?.deadlineAt).toBe(first.deadlineAt);
    await expect(admitContinuation({ ...continuation, text: "different" })).rejects.toThrow(/Changed continuation/);
    expect((await readRecoveryInput(f.sessionId)).accepted).toMatchObject({ inputId: "round-1", settlement: "open", cancellation: "not-cancelled" });
    expect((await readRecoveryInput(f.sessionId)).queued.map(i => i.inputId)).toEqual(["queued"]);
  });
  it("recordInputOutcome is request-idempotent, terminal-winner ordered, and time is evidence not order", async () => {
    const f = fixture(); f.admitRound(); const selected = await f.select(); await f.delivery(selected);
    const cancelled = { ...f.outcome("round-1", "user-cancel", "E"), at: 100 };
    const first = await recordInputOutcome(cancelled);
    expect(first).toMatchObject({ durable: true, status: "recorded" });
    expect(await recordInputOutcome({ ...cancelled, deadlineMs: cancelled.deadlineMs - 1 })).toMatchObject({ durable: true, status: "existing", revision: "revision" in first ? first.revision : -1 });
    expect(await recordInputOutcome({ ...cancelled, at: 101 })).toEqual({ durable: false, reason: "conflict" });
    expect(await recordInputOutcome({ ...f.outcome("round-1", "settled"), at: 1 })).toMatchObject({ durable: true, status: "already-terminal", kind: "user-cancel" });
    expect(await recordInputOutcome(f.outcome())).toMatchObject({ durable: true, status: "already-terminal", kind: "user-cancel" });
    expect(f.snapshot().outcomes).toHaveLength(1);
    const other = fixture(); other.admitRound(); const s = await other.select(); await other.delivery(s);
    expect(await recordInputOutcome(other.outcome("round-1", "settled"))).toMatchObject({ status: "recorded" });
    expect(await recordInputOutcome(other.outcome("round-1", "user-cancel"))).toMatchObject({ status: "already-terminal", kind: "settled" });
  });
  it.each([false, true])("task E during idle compact after settled round (queued=%s) never targets stale input and closes atomically", async queued => {
    const f = fixture(); f.admitRound(); const stale = await f.select(); await f.delivery(stale);
    expect(await recordInputOutcome(f.outcome("round-1", "settled"))).toMatchObject({ status: "recorded" });
    await releaseRecoveryInputAfterJoin(stale, Promise.resolve(), async () => {}); f.drives.length = 0;
    if (queued) { f.admitRound("q1"); f.admitRound("q2"); }
    const b = await f.begin(); const stop = deferred(); const running = f.api.withMaintenanceAttempt(b.token, async signal => { await stop.promise; expect(signal.aborted).toBe(true); });
    const abort = take(await f.api.captureTaskAbort(f.sessionId, "task-E", f.epoch, Date.now(), f.epoch.processIncarnationId));
    expect(abort.originalInputIds).toEqual(queued ? ["q1", "q2"] : []);
    expect(f.snapshot().maintenance[0]!.record.state).not.toBe("ACTIVE");
    stop.resolve(); await running;
    take(await f.finish("compact-1", { outcome: "cancelled", errorCode: "RECOVERY_MAINTENANCE_CANCELLED", taskAbortRequestId: "task-E" }));
    const state = f.snapshot(); const record = state.maintenance[0]!.record;
    expect(state.outcomes.filter(o => o.kind === "user-cancel").map(o => o.originalInputId)).toEqual(queued ? ["q1", "q2"] : []);
    expect(state.outcomes.find(o => o.originalInputId === "round-1")!.kind).toBe("settled");
    for (const o of state.outcomes.filter(o => o.kind === "user-cancel")) expect(o.revision).toBe(record.revision);
    expect(record.state).toBe("CONSUMED"); expect(state.taskAborts[0]?.committed).toBe(true); expect(f.drives).toHaveLength(1);
  });
  it("late E finishes an already-prepared plan and cancels only captured real queued targets", async () => {
    const f = fixture(); f.admitRound("q1"); const b = await f.begin(); const p = f.plan();
    await f.api.withMaintenanceAttempt(b.token, async () => { take(await f.api.prepareMaintenance(b.token, p.ref)); });
    const abort = take(await f.api.captureTaskAbort(f.sessionId, "late-E", f.epoch, Date.now(), f.epoch.processIncarnationId));
    f.admitRound("later-than-E"); expect(abort.originalInputIds).toEqual(["q1"]);
    const c = take(await f.finish()); expect(c.completion?.outcome).toBe("completed");
    expect(f.snapshot().outcomes.map(o => [o.originalInputId, o.kind])).toEqual([["q1", "user-cancel"]]);
    expect((await readRecoveryInput(f.sessionId)).queued.map(i => i.inputId)).toEqual(["later-than-E"]);
  });
  it("operation-only cancel/Escape leaves all real queued inputs uncancelled and a late prepared plan cannot roll back", async () => {
    const f = fixture(); f.admitRound(); const b = await f.begin();
    take(await f.api.cancelMaintenance(f.key(), f.epoch));
    await expect(f.api.withMaintenanceAttempt(b.token, async () => {})).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_TOKEN_REUSED" });
    const c = take(await f.finish("compact-1", { outcome: "cancelled", errorCode: "RECOVERY_MAINTENANCE_CANCELLED" }));
    expect(c.completion?.outcome).toBe("cancelled"); expect(f.snapshot().outcomes).toEqual([]); expect(f.snapshot().inputs).toHaveLength(1);
  });
  it("tree navigation refuses any open logical input, compact requires native retained repair on its execution branch", async () => {
    const f = fixture(); f.admitRound(); const selected = await f.select(); await f.delivery(selected);
    await recordInputOutcome(f.outcome()); await releaseRecoveryInputAfterJoin(selected, Promise.resolve(), async () => {});
    refused(await f.api.admitMaintenance(f.request("tree", { kind: "tree-nav", targetEntryId: null, summarize: true })), "INPUT_SELECTED");
    refused(await f.api.admitMaintenance(f.request()), "REPAIR_REQUIRED");
    const recoveryGeneration = await ensureRecoveryGeneration(f.sessionId, "round-1");
    const repair = { originalInputId: "round-1", recoveryGeneration, executionBranchEntryId: "branch-1", repairReceiptSha256: D };
    refused(await f.api.admitMaintenance({ ...f.request(), repair }), "REPAIR_REQUIRED");
    await recordRecoveryRepair(f.sessionId, repair);
    refused(await f.api.admitMaintenance({ ...f.request(), repair, baseLeafId: "other-branch" }), "REPAIR_REQUIRED");
    take(await f.api.admitMaintenance({ ...f.request(), repair }));
  });
  it("one durable auto-retry slot is original-input keyed across different runIds and boots, before continuation", async () => {
    const f = fixture(); const deadline = Date.now() + 90_000; f.admitRound("round-1", deadline); const selected = await f.select(); await f.delivery(selected);
    await recordInputOutcome(f.outcome()); await releaseRecoveryInputAfterJoin(selected, Promise.resolve(), async () => {});
    const recoveryGeneration = await ensureRecoveryGeneration(f.sessionId, "round-1");
    const repair = { originalInputId: "round-1", recoveryGeneration, executionBranchEntryId: "branch-1", repairReceiptSha256: D }; await recordRecoveryRepair(f.sessionId, repair);
    const r = { ...f.request("retry-1", { kind: "auto-compact-retry", retryAnchor: "round-1" }), repair, deadlineMs: deadline };
    take(await f.api.admitMaintenance(r));
    expect(f.snapshot().retrySlots).toEqual([{ anchor: "round-1", requestId: "retry-1" }]);
    take(await f.finish("retry-1", { outcome: "failed", errorCode: "capacity-overflow" }));
    f.rebind(epoch("owner-b"));
    refused(await f.api.admitMaintenance({ ...r, requestId: "different-runId", epoch: f.epoch }), "RETRY_EXHAUSTED");
    expect(f.snapshot().retrySlots).toHaveLength(1); expect(f.snapshot().inputs.filter(i => i.kind === "continuation")).toEqual([]);
  });
  it("truly inputless retry resolves its originating maintenance; random anchors/deadline renewal refuse", async () => {
    const f = fixture(); const r = f.request("origin"); take(await f.api.admitMaintenance(r)); take(await f.finish("origin", { outcome: "failed", errorCode: "overflow" }));
    refused(await f.api.admitMaintenance(f.request("random", { kind: "auto-compact-retry", retryAnchor: "native-run-id" })), "REPAIR_REQUIRED");
    refused(await f.api.admitMaintenance({ ...f.request("retry", { kind: "auto-compact-retry", retryAnchor: "origin" }), deadlineMs: r.deadlineMs + 1 }), "DEADLINE");
    take(await f.api.admitMaintenance({ ...f.request("retry", { kind: "auto-compact-retry", retryAnchor: "origin" }), deadlineMs: r.deadlineMs }));
    take(await f.finish("retry", { outcome: "failed", errorCode: "overflow" }));
    refused(await f.api.admitMaintenance({ ...f.request("again", { kind: "auto-compact-retry", retryAnchor: "origin" }), deadlineMs: r.deadlineMs }), "RETRY_EXHAUSTED");
  });
});

describe.skipIf(process.platform === "win32")("bounded refusal, uncertainty and epoch/adoption failure paths", () => {
  it("rejects deadline/abort before any write, but exact accepted lookup survives expiry", async () => {
    const f = fixture(); const r = f.request();
    refused(await f.api.admitMaintenance(r, { deadlineMs: Date.now() - 1 }), "DEADLINE");
    refused(await f.api.admitMaintenance(r, { signal: AbortSignal.abort() }), "CANCELLED");
    const a = take(await f.api.admitMaintenance(r));
    vi.spyOn(Date, "now").mockReturnValue(r.deadlineMs + 1);
    expect(take(await f.api.admitMaintenance(r))).toEqual(a);
    refused(await f.api.beginMaintenance(f.key(), f.epoch, f.snapshot().revision), "DEADLINE");
  });
  it("all three epoch fields revoke even at same-process epoch zero, and old permits never reopen", async () => {
    const f = fixture(); const b = await f.begin();
    const next = epoch("replacement-same-process", 0); f.rebind(next);
    refused(await f.api.beginMaintenance(f.key(), b.token.epoch, f.snapshot().revision), "RECOVERY_EPOCH_REVOKED");
    await expect(f.api.withMaintenanceAttempt(b.token, async () => {})).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_TOKEN_REUSED" });
    const adopted = take(await f.api.adoptMaintenance(f.key(), next, f.snapshot().revision));
    expect(adopted.record.request.epoch).toEqual(b.token.epoch); expect(adopted.record.activeEpoch).toEqual(next); expect(adopted.record.permitRevision).toBeGreaterThan(b.record.permitRevision);
    await expect(f.api.withMaintenanceAttempt(adopted.token, async () => {})).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_INTERRUPTED" });
    take(await f.finish("compact-1", { outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_INTERRUPTED" }));
  });
  it.each(["ADMITTED", "ACTIVE", "PREPARED"] as const)("adopts %s settlement-only without summary, including after the original deadline", async state => {
    const f = fixture(); const r = f.request(); take(await f.api.admitMaintenance(r));
    if (state !== "ADMITTED") { const b = take(await f.api.beginMaintenance(f.key(), f.epoch, f.snapshot().revision)); if (state === "PREPARED") await f.api.withMaintenanceAttempt(b.token, async () => { take(await f.api.prepareMaintenance(b.token, f.plan().ref)); }); }
    f.rebind({ processIncarnationId: "process-b", bindingId: "owner-b", epoch: 0 });
    take(await f.api.adoptMaintenance(f.key(), f.epoch, f.snapshot().revision));
    const originalNow = Date.now(); vi.spyOn(Date, "now").mockReturnValue(r.deadlineMs + 10);
    const end = take(await f.api.reconcileAfterJoin(f.key(), f.epoch, { deadlineMs: r.deadlineMs + 60_000, joined: Promise.resolve(),
      settle: async (_record, prepare) => { const p = f.plan("compact-1", state === "PREPARED" ? "completed" : "failed", state === "PREPARED" ? undefined : "RECOVERY_MAINTENANCE_INTERRUPTED"); take(await prepare(p.ref)); return p.apply(); }, reconcile: async () => {},
    }));
    expect(end.state).toBe("CONSUMED"); expect(end.request.deadlineMs).toBe(r.deadlineMs); expect(originalNow).toBeLessThan(r.deadlineMs);
  });
  it("checks receiving positive exclusion and expected revision before adoption", async () => {
    const f = fixture(); await f.begin(); f.rebind(epoch("owner-b"));
    refused(await f.api.adoptMaintenance(f.key(), f.epoch, -1), "CONFLICT"); f.revoke();
    refused(await f.api.adoptMaintenance(f.key(), f.epoch, 2), "RECOVERY_EPOCH_REVOKED");
  });
  it("refuses truncated pending scans, duplicate/missing receipts, and corrupt or inaccessible ledger", async () => {
    const f = fixture(); await f.begin();
    refused(await f.api.listPending(f.sessionId, 0), "INVALID");
    expect(take(await f.api.listPending(f.sessionId, 1)).records).toHaveLength(1);
    const p = f.plan();
    const failed = await f.api.reconcileAfterJoin(f.key(), f.epoch, { ...f.io(), joined: Promise.resolve(),
      settle: async (_r, prepare) => { take(await prepare(p.ref)); const c = p.apply(); return { ...c, entries: [c.entries[0]!, c.entries[0]!] }; }, reconcile: async () => {},
    }); refused(failed, "UNCERTAIN"); expect(f.snapshot().maintenance[0]!.record.state).toBe("PREPARED");
    const file = path.join(f.root, "input-admission", "v1", "session", `${recoveryHash(f.sessionId)}.json`);
    const bytes = fs.readFileSync(file, "utf8"); fs.writeFileSync(file, bytes.slice(0, -7));
    refused(await f.api.read(f.key()), "UNCERTAIN"); expect((await readRecoveryInput(f.sessionId)).complete).toBe(false);
  });
  it("reads back rename-before-fsync unknown ACK before either dispatcher can run", async () => {
    const f = fixture(); const r = f.request(); const original = fs.renameSync;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => { original(...args); throw new Error("directory ACK lost"); });
    refused(await f.api.admitMaintenance(r), "UNCERTAIN"); spy.mockRestore();
    const a = take(await f.api.admitMaintenance(r)); expect(a.state).toBe("ADMITTED"); expect(f.snapshot().maintenance).toHaveLength(1);
    f.admitRound(); expect(await selectRecoveryInput({ sessionId: f.sessionId, originalInputId: "round-1", epoch: f.epoch, expectedRevision: f.snapshot().revision })).toMatchObject({ code: "RECOVERY_MAINTENANCE_BUSY" });
  });
  it("incomplete history never authorizes maintenance, cancellation, or a new continuation", async () => {
    const f = fixture(); f.admitRound(); await f.select();
    const recoveryGeneration = await ensureRecoveryGeneration(f.sessionId, "round-1");
    commitRecoverySession(f.sessionId, state => { state.complete = false; });
    refused(await f.api.admitMaintenance(f.request()), "UNCERTAIN");
    expect(await recordInputOutcome(f.outcome("round-1", "user-cancel"))).toEqual({ durable: false, reason: "incomplete-history" });
    await expect(admitContinuation({ sessionId: f.sessionId, originalInputId: "round-1", recoveryGeneration, text: "resume" })).rejects.toThrow(/complete/i);
  });
  it.each(["deadline", "aborted", "conflict", "incomplete-history", "store-unavailable", "superseded"] as const)("recordInputOutcome preserves the closed %s refusal", async reason => {
    const f = fixture(); f.admitRound(); let r = f.outcome("round-1", "user-cancel"); let spy: ReturnType<typeof vi.spyOn> | undefined;
    if (reason === "deadline") r = { ...r, deadlineMs: Date.now() - 1 };
    if (reason === "aborted") r = { ...r, signal: AbortSignal.abort() };
    if (reason === "conflict") { await recordInputOutcome(r); r = { ...r, at: r.at + 1 }; }
    if (reason === "incomplete-history") r = { ...r, originalInputId: "absent" };
    if (reason === "store-unavailable") spy = vi.spyOn(fs, "openSync").mockImplementation(() => { throw new Error("store unavailable"); });
    if (reason === "superseded") f.revoke();
    expect(await recordInputOutcome(r)).toEqual({ durable: false, reason }); spy?.mockRestore();
  });
});

describe.skipIf(process.platform === "win32")("late commit/abort reconciliation boundaries", () => {
  it("retains native owner prototype methods rather than spreading away its exclusion check", async () => {
    const owner = new class implements RecoveryAdmissionOwner {
      readonly epoch = epoch(); readonly podId = "native-pod"; readonly bootId = "native-boot"; readonly historyComplete = true;
      private readonly marker = "native-current";
      assertCurrent() { if (this.marker !== "native-current") throw new Error("native owner context lost"); }
      reconsiderInput() {}
    };
    const f = fixture({ owner }); await f.begin(); take(await f.finish("compact-1", { outcome: "noop" }));
  });
  it.each([false, true])("missing/asynchronous native repair validator (async=%s) cannot establish a repair reference", async asynchronous => {
    const owner: RecoveryAdmissionOwner = { epoch: epoch(), podId: "native-pod", bootId: "native-boot", historyComplete: true,
      assertCurrent() {}, reconsiderInput() {}, ...(asynchronous ? { validateRepair: async () => {} } : {}),
    };
    const f = fixture({ owner }); f.admitRound(); const input = await f.select(); await f.delivery(input);
    await recordInputOutcome(f.outcome()); await releaseRecoveryInputAfterJoin(input, Promise.resolve(), async () => {});
    const recoveryGeneration = await ensureRecoveryGeneration(f.sessionId, "round-1");
    await expect(recordRecoveryRepair(f.sessionId, { originalInputId: "round-1", recoveryGeneration, executionBranchEntryId: "branch-1", repairReceiptSha256: D })).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    expect(f.snapshot().repairs).toEqual([]);
  });
  it("rereads a native full-epoch getter so in-control revocation never retains an old compute permit", async () => {
    const f = fixture(); const b = await f.begin();
    await f.api.withMaintenanceAttempt(b.token, async () => {
      f.advanceEpoch();
      expect(() => f.api.assertMaintenanceGate(b.token, "compact-summary", D)).toThrow(expect.objectContaining({ code: "RECOVERY_EPOCH_REVOKED" }));
    });
    refused(await f.api.beginMaintenance(f.key(), b.token.epoch, f.snapshot().revision), "RECOVERY_EPOCH_REVOKED");
    const adopted = take(await f.api.adoptMaintenance(f.key(), f.epoch, f.snapshot().revision));
    expect(adopted.record.activeEpoch.epoch).toBe(b.token.epoch.epoch + 1); expect(adopted.record.request.epoch).toEqual(b.token.epoch);
    await expect(f.api.withMaintenanceAttempt(adopted.token, async () => {})).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_INTERRUPTED" });
    take(await f.finish("compact-1", { outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_INTERRUPTED" }));
  });
  it.each(["begin", "adopt"] as const)("reads back a committed %s after its lost ACK instead of stranding ACTIVE", async kind => {
    const f = fixture(); const r = f.request(); take(await f.api.admitMaintenance(r));
    if (kind === "adopt") f.rebind(epoch("receiving-owner"));
    const revision = f.snapshot().revision; const original = fs.renameSync;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => { original(...args); throw new Error("commit ACK lost"); });
    const begun = take(await (kind === "begin" ? f.api.beginMaintenance(f.key(), f.epoch, revision) : f.api.adoptMaintenance(f.key(), f.epoch, revision)));
    spy.mockRestore();
    expect(begun.record.state).toBe("ACTIVE"); expect(begun.record.revision).toBe(revision + 1);
    if (kind === "begin") await f.api.withMaintenanceAttempt(begun.token, async () => { f.api.assertMaintenanceGate(begun.token, "compact-summary", D); });
    else await expect(f.api.withMaintenanceAttempt(begun.token, async () => { throw new Error("no successor inference"); })).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_INTERRUPTED" });
    expect(f.snapshot().maintenance[0]!.record.state).toBe("ADMITTED");
    take(await f.finish("compact-1", { outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_INTERRUPTED" }));
  });
  it("parks a begin whose commit crosses the original deadline and never renews compute permission", async () => {
    const f = fixture(); const r = f.request(); const admitted = take(await f.api.admitMaintenance(r));
    let clock = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => clock);
    const rename = fs.renameSync; let first = true;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => { rename(...args); if (first) { first = false; clock = r.deadlineMs + 1; throw new Error("late commit ACK lost"); } });
    refused(await f.api.beginMaintenance(f.key(), f.epoch, admitted.revision), "UNCERTAIN"); spy.mockRestore();
    expect(f.snapshot().maintenance[0]!.record.state).toBe("ADMITTED");
    refused(await f.api.beginMaintenance(f.key(), f.epoch, f.snapshot().revision), "TOKEN_REUSED");
    take(await f.finish("compact-1", { outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_DEADLINE" }));
  });
  it("expiry between provider and prepare refuses both gates but still permits bounded control-only settlement", async () => {
    const f = fixture(); const b = await f.begin();
    await f.api.withMaintenanceAttempt(b.token, async () => {
      vi.spyOn(Date, "now").mockReturnValue(b.record.request.deadlineMs + 1);
      expect(() => f.api.assertMaintenanceGate(b.token, "compact-summary", D)).toThrow(expect.objectContaining({ code: "RECOVERY_MAINTENANCE_DEADLINE" }));
      refused(await f.api.prepareMaintenance(b.token, f.plan("compact-1", "failed", "RECOVERY_MAINTENANCE_DEADLINE").ref), "DEADLINE");
    });
    expect(f.snapshot().maintenance[0]!.record.state).toBe("ADMITTED");
    take(await f.finish("compact-1", { outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_DEADLINE" }));
  });
  it("an unrelated accept cannot change a selected permit, and a released occurrence cannot reauthorize its old token", async () => {
    const f = fixture(); f.admitRound(); const first = await f.select(); await f.delivery(first);
    f.admitRound("later"); const duplicate = await f.select(); expect(duplicate.revision).toBe(first.revision);
    expect(() => assertRecoveryInputPermission(first)).not.toThrow();
    await recordInputOutcome(f.outcome()); await releaseRecoveryInputAfterJoin(first, Promise.resolve(), async () => {});
    const next = await f.select(); expect(next.revision).toBeGreaterThan(first.revision);
    expect(() => assertRecoveryInputPermission(first)).toThrow(/reservation/i);
    expect(() => assertRecoveryInputPermission(next)).not.toThrow();
  });
  it("E while the prepared edge awaits B6 does not revoke the issued plan and cancels real targets atomically with consumption", async () => {
    const f = fixture(); f.admitRound("queued"); await f.begin(); const barrier = deferred(); const prepared = deferred();
    const pending = f.api.reconcileAfterJoin(f.key(), f.epoch, { ...f.io(), joined: Promise.resolve(),
      settle: async (_record, prepare) => { const p = f.plan(); take(await prepare(p.ref)); prepared.resolve(); await barrier.promise; return p.apply(); }, reconcile: async () => {},
    });
    await prepared.promise;
    const abort = take(await f.api.captureTaskAbort(f.sessionId, "E-during-B6", f.epoch, Date.now(), "boot-a", f.io()));
    expect(abort.originalInputIds).toEqual(["queued"]); expect(abort.committed).toBe(false);
    expect(f.snapshot().outcomes).toEqual([]); barrier.resolve(); const done = take(await pending);
    expect(done.completion!.outcome).toBe("completed"); const s = f.snapshot();
    expect(s.taskAborts[0]!.committed).toBe(true); expect(s.outcomes[0]!.revision).toBe(done.revision); expect(f.drives).toHaveLength(1);
  });
  it("E after CONSUMED but before release never revives ACTIVE or targets a settled round", async () => {
    const f = fixture(); f.admitRound(); const first = await f.select(); await f.delivery(first);
    await recordInputOutcome(f.outcome("round-1", "settled")); await releaseRecoveryInputAfterJoin(first, Promise.resolve(), async () => {});
    f.admitRound("queued"); await f.begin(); const drivesBefore = f.drives.length;
    take(await f.finish("compact-1", { reconcile: async () => {
      const abort = take(await f.api.captureTaskAbort(f.sessionId, "E-after-consume", f.epoch, Date.now(), "boot-a", f.io()));
      expect(abort).toMatchObject({ committed: true, originalInputIds: ["queued"] });
      expect(f.snapshot().maintenance[0]!.record.state).toBe("CONSUMED");
    } }));
    expect(f.snapshot().outcomes.filter(o => o.kind === "user-cancel").map(o => o.originalInputId)).toEqual(["queued"]);
    expect(f.drives.length - drivesBefore).toBe(1);
  });
  it("a lost consume ACK reconciles exact completion once without a second append, edge or release", async () => {
    const f = fixture(); await f.begin(); const rename = fs.renameSync; let lost = false;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
      rename(...args); const payload = JSON.parse(fs.readFileSync(args[1], "utf8")).payload;
      if (!lost && payload?.maintenance[0]?.record.state === "CONSUMED") { lost = true; throw new Error("consume ACK lost"); }
    });
    const done = take(await f.finish()); spy.mockRestore(); expect(lost).toBe(true); expect(done.state).toBe("CONSUMED");
    expect(take(await f.finish())).toEqual(done); expect(f.native).toHaveLength(2); expect(f.drives).toHaveLength(1);
  });
  it.each(["RECOVERY_MAINTENANCE_SELF_CALL", "RECOVERY_MAINTENANCE_HOOK_CONTENT", "RECOVERY_EXECUTION_BRANCH_REQUIRED"] as const)("preserves native %s refusal without ACTIVE leak, fallback permission or automatic replay", async code => {
    const f = fixture(); await f.begin(); let calls = 0;
    const edge = { ...f.io(), joined: Promise.resolve(), settle: async () => { calls++; throw new RecoveryMaintenanceError(code, "native refusal"); }, reconcile: async () => {} };
    refused(await f.api.reconcileAfterJoin(f.key(), f.epoch, edge), code);
    refused(await f.api.reconcileAfterJoin(f.key(), f.epoch, edge), code);
    expect(calls).toBe(1); expect(f.snapshot().maintenance[0]!.record.state).toBe("ADMITTED"); expect(f.snapshot().reservation?.kind).toBe("maintenance");
  });
});

// The child bundles THIS candidate's production modules, then exits via a real SIGKILL.
// No Bun worker, new writer exclusion scheme or process supervisor is introduced. Its
// parent Vitest command runs under the task's smarty-reap-owned bounded process group.
describe.runIf(process.platform === "linux")("process-loss maintenance ledger replay", () => {
  it.each(["ADMITTED", "ACTIVE", "PREPARED", "CONSUMED", "ADOPTED"] as const)("fresh process adopts %s after commit-before-ACK and a second crash without successor inference", async state => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "maintenance-process-")); const bundle = path.join(root, "worker.mjs");
    await build({ entryPoints: [fileURLToPath(new URL("./fixtures/maintenance-admission-worker.ts", import.meta.url))], outfile: bundle, bundle: true, format: "esm", platform: "node", target: "node24" });
    const run = (mode: string, owner: string) => new Promise<{ code: number | null; signal: string | null; stdout: string }>((resolve, reject) => {
      const child = spawn(process.execPath, [bundle, root, state, mode, owner], { stdio: ["ignore", "pipe", "pipe"] }); let stdout = "", stderr = "";
      child.stdout.on("data", b => { stdout += String(b); }); child.stderr.on("data", b => { stderr += String(b); }); child.once("error", reject);
      child.once("exit", (code, signal) => { if (code !== 0 && signal !== "SIGKILL") reject(new Error(stderr)); else resolve({ code, signal, stdout }); });
    });
    expect(await run("crash", "a")).toMatchObject({ signal: "SIGKILL" });
    if (state === "ADOPTED") expect(await run("adopt-crash", "b")).toMatchObject({ signal: "SIGKILL" });
    const recovered = await run("recover", "c"); expect(recovered.code).toBe(0);
    const result = JSON.parse(recovered.stdout) as { record: MaintenanceRecord; counts: { records: number; nativePlans: number; provider: number }; deadline: number };
    expect(result.record.state).toBe("CONSUMED"); expect(result.record.request.deadlineMs).toBe(result.deadline);
    expect(result.counts).toEqual({ records: 1, nativePlans: state === "PREPARED" || state === "CONSUMED" ? 1 : 0, provider: 0 });
    const retry = await run("lookup", "c"); expect(JSON.parse(retry.stdout).record).toEqual(result.record);
  }, 30_000);
});
