import { describe, expect, it } from "vitest";
import { UNKNOWN_ACK_MESSAGE, type AdmissionHarness, type AdmissionHarnessFactory } from "./types.js";

/** Registers callable Vitest cases. Every case gets a fresh isolated harness and finally closes it.
 * Keep: C1/C2 trust-boundary and acknowledged-work data safety; NOT native crash acceptance. */
export function runConformance(admission: AdmissionHarnessFactory): void {
  const test = (name: string, run: (h: AdmissionHarness) => Promise<void> | void) => it(name, async () => {
    const h = await admission.create();
    try { await run(h); } finally { h.close(); }
  });
  const error = async (operation: () => unknown, code: string) => {
    let caught: unknown;
    try { await operation(); } catch (failure) { caught = failure; }
    expect(caught).toBeInstanceOf(Error);
    expect(caught).toMatchObject({ name: "RecoveryAdmissionError", code, message: expect.any(String) });
    expect((caught as Error).message.length).toBeGreaterThan(0);
  };
  const open = (h: AdmissionHarness) => {
    h.admission.initializeNewRecoverySession(h.initialization);
    h.requireIdentity(); h.bindRequired();
  };
  const joined = async (h: AdmissionHarness) => { open(h); await h.seedJoinedOriginal(); };
  const continuation = (h: AdmissionHarness, recoveryGeneration: string) => ({
    sessionId: h.sessionId, originalInputId: h.originalInputId, recoveryGeneration, text: " exact continuation bytes\n",
  });
  const incomplete = async (h: AdmissionHarness) => {
    expect(await h.admission.readRecoveryInput(h.sessionId)).toEqual({
      sessionId: h.sessionId, complete: false, revision: 0, queued: [], outcomes: [],
    });
    for (const result of [
      await h.admission.maintenance.read({ sessionId: h.sessionId, requestId: "absent" }),
      await h.admission.maintenance.listPending(h.sessionId, 16),
    ]) {
      expect(result).toMatchObject({ ok: false, code: "RECOVERY_HISTORY_INCOMPLETE", message: expect.any(String) });
      expect("value" in result).toBe(false);
    }
  };

  describe(`l7-b5-admission-v1 / ${admission.name}`, () => {
    test("NEW requires positive synchronous authority and complete history; seals zero once, never overwrites", async h => {
      const before = h.observe();
      const denied = Object.assign(new Error("No positive NEW authority"), { name: "RecoveryAdmissionError", code: "RECOVERY_CONFLICT" });
      await expect(Promise.resolve().then(() => h.admission.initializeNewRecoverySession({
        ...h.initialization, assertNew() { throw denied; },
      }))).rejects.toBe(denied);
      expect(h.observe()).toEqual(before);
      await error(() => h.admission.initializeNewRecoverySession({ ...h.initialization,
        recovery: { ...h.initialization.recovery, historyComplete: false },
      }), "RECOVERY_HISTORY_INCOMPLETE");
      expect(h.observe()).toEqual(before);
      await error(() => h.admission.initializeNewRecoverySession({ ...h.initialization, assertNew: async () => {} }), "RECOVERY_CONFLICT");
      await error(() => h.admission.initializeNewRecoverySession({ ...h.initialization,
        recovery: { ...h.initialization.recovery, assertCurrent: async () => {} },
      }), "RECOVERY_EPOCH_REVOKED");
      expect(h.observe()).toEqual(before);
      h.admission.initializeNewRecoverySession(h.initialization);
      const sealed = h.observe();
      expect(sealed).toMatchObject({ revision: 0, pins: [], continuationIds: [], repairs: [], nativeDeliveries: 0 });
      await error(() => h.admission.initializeNewRecoverySession(h.initialization), "RECOVERY_CONFLICT");
      expect(h.observe()).toEqual(sealed);
      h.requireIdentity(); h.bindRequired();
      expect(await h.admission.readRecoveryInput(h.sessionId)).toEqual({
        sessionId: h.sessionId, complete: true, revision: 0, queued: [], outcomes: [],
      });
      expect(h.observe()).toEqual(sealed);
    });

    test("REQUIRED absence is incomplete, never an implicit NEW store", async h => {
      h.requireIdentity(); h.bindRequired(); const absent = h.observe();
      await incomplete(h);
      await error(() => h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId), "RECOVERY_HISTORY_INCOMPLETE");
      await error(() => h.admission.initializeNewRecoverySession(h.initialization), "RECOVERY_CONFLICT");
      h.unbind();
      await error(() => h.admission.initializeNewRecoverySession(h.initialization), "RECOVERY_CONFLICT");
      expect(h.observe()).toEqual(absent);
    });

    test("missing original pin refuses both continuation and repair with typed errors and no mutation", async h => {
      await joined(h); const before = h.observe();
      await error(() => h.admission.admitContinuation(continuation(h, "never-allocated")), "RECOVERY_HISTORY_INCOMPLETE");
      await error(() => h.admission.recordRecoveryRepair(h.sessionId, h.repairFor("never-allocated")), "RECOVERY_HISTORY_INCOMPLETE");
      expect(h.observe()).toEqual(before);
    });

    test("one pin identifies the original admission, survives a new full epoch, and rejects reminted generations", async h => {
      await joined(h);
      const original = (await h.admission.readRecoveryInput(h.sessionId)).accepted!;
      const before = h.observe();
      const generation = await h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId);
      expect(generation.trim().length).toBeGreaterThan(0);
      const pinned = h.observe();
      expect(pinned.revision).toBe(before.revision! + 1);
      expect(pinned.pins).toEqual([{ originalInputId: h.originalInputId, admissionId: original.admissionId, recoveryGeneration: generation }]);
      h.rebindRequired();
      expect(await h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId)).toBe(generation);
      expect((await h.admission.readRecoveryInput(h.sessionId)).accepted).toEqual(original);
      await error(() => h.admission.admitContinuation(continuation(h, `${generation}:reminted`)), "RECOVERY_CONFLICT");
      await error(() => h.admission.recordRecoveryRepair(h.sessionId, h.repairFor(`${generation}:reminted`)), "RECOVERY_CONFLICT");
      expect(h.observe()).toEqual(pinned);
    });

    test("unknown pin-write ACK is read back by ensure, not a caller remint or another revision", async h => {
      await joined(h); const before = h.observe();
      await expect(h.withLostWriteAck(() => h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId)))
        .rejects.toThrow(UNKNOWN_ACK_MESSAGE);
      const visible = h.observe();
      expect(visible.pins).toHaveLength(1); expect(visible.revision).toBe(before.revision! + 1);
      h.rebindRequired();
      expect(await h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId)).toBe(visible.pins[0]!.recoveryGeneration);
      expect(h.observe()).toEqual(visible);
    });

    test("unknown continuation-write ACK retries the exact retained admission and rejects changed bytes", async h => {
      await joined(h); const generation = await h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId);
      const request = continuation(h, generation), before = h.observe();
      await expect(h.withLostWriteAck(() => h.admission.admitContinuation(request))).rejects.toThrow(UNKNOWN_ACK_MESSAGE);
      const visible = h.observe();
      expect(visible.continuationIds).toHaveLength(1); expect(visible.revision).toBe(before.revision! + 1);
      h.rebindRequired();
      expect(await h.admission.admitContinuation(request)).toEqual({ admissionId: visible.continuationIds[0], existing: true });
      await error(() => h.admission.admitContinuation({ ...request, text: `${request.text}changed` }), "RECOVERY_CONFLICT");
      expect(h.observe()).toEqual(visible);
    });

    test("retained pin/repair/continuation retries survive terminal outcome without rearming work", async h => {
      await joined(h); const generation = await h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId);
      const repair = h.repairFor(generation), request = continuation(h, generation);
      await h.admission.recordRecoveryRepair(h.sessionId, repair);
      const first = await h.admission.admitContinuation(request); expect(first.existing).toBe(false);
      const outcome = await h.admission.recordInputOutcome({ sessionId: h.sessionId, originalInputId: h.originalInputId,
        requestId: "terminal-cancel", kind: "user-cancel", at: Date.now(), bootId: "test-boot",
        deadlineMs: Math.min(h.deadlineMs, Date.now() + 10_000) });
      expect(outcome).toMatchObject({ durable: true, status: "recorded" });
      const terminal = h.observe(); h.rebindRequired();
      expect(await h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId)).toBe(generation);
      expect(await h.admission.admitContinuation(request)).toEqual({ ...first, existing: true });
      await h.admission.recordRecoveryRepair(h.sessionId, repair);
      await error(() => h.admission.recordRecoveryRepair(h.sessionId, { ...repair, repairReceiptSha256: "f".repeat(64) }), "RECOVERY_CONFLICT");
      expect(h.observe()).toEqual(terminal);
    });

    test("unknown NEW seal ACK permits exact REQUIRED readback, never overwrite", async h => {
      await expect(h.withLostWriteAck(() => h.admission.initializeNewRecoverySession(h.initialization))).rejects.toThrow(UNKNOWN_ACK_MESSAGE);
      const visible = h.observe(); expect(visible.revision).toBe(0);
      await error(() => h.admission.initializeNewRecoverySession(h.initialization), "RECOVERY_CONFLICT");
      h.requireIdentity(); h.bindRequired();
      expect(await h.admission.readRecoveryInput(h.sessionId)).toEqual({
        sessionId: h.sessionId, complete: true, revision: 0, queued: [], outcomes: [],
      });
      expect(h.observe()).toEqual(visible);
    });

    test("loss after acknowledged work refuses all C1 mutations and NEW recovery without recreating history", async h => {
      await joined(h); h.removeLedger(); const lost = h.observe();
      await incomplete(h);
      await error(() => h.admission.ensureRecoveryGeneration(h.sessionId, h.originalInputId), "RECOVERY_HISTORY_INCOMPLETE");
      await error(() => h.admission.admitContinuation(continuation(h, "not-a-pin")), "RECOVERY_HISTORY_INCOMPLETE");
      await error(() => h.admission.recordRecoveryRepair(h.sessionId, h.repairFor("not-a-pin")), "RECOVERY_HISTORY_INCOMPLETE");
      await error(() => h.admission.initializeNewRecoverySession(h.initialization), "RECOVERY_CONFLICT");
      h.unbind();
      await error(() => h.admission.initializeNewRecoverySession(h.initialization), "RECOVERY_CONFLICT");
      expect(h.observe()).toEqual(lost); expect(lost.nativeDeliveries).toBe(0);
    });
  });
}
