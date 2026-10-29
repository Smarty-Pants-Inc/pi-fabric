import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as admission from "../src/recovery/input-admission.js";
import { B5_HASH, b5GenerationFixture } from "./helpers/b5-generation-fixture.js";
import type { RepairRef } from "../src/recovery/maintenance-admission.js";

const repairFor = (recoveryGeneration: string): RepairRef => ({ originalInputId: "original", recoveryGeneration,
  executionBranchEntryId: "anchor-A", repairReceiptSha256: B5_HASH });
const continuationFor = (sessionId: string, recoveryGeneration: string) => ({ sessionId, originalInputId: "original",
  recoveryGeneration, text: " exact continuation bytes\n" });
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const rewriteLedger = (file: string, change: (s: admission.RecoverySessionLedger) => void) => {
  const envelope = JSON.parse(fs.readFileSync(file, "utf8")) as { payload: admission.RecoverySessionLedger; sha256: string };
  change(envelope.payload); envelope.sha256 = admission.recoveryHash(admission.recoveryCanonical(envelope.payload));
  fs.writeFileSync(file, `${JSON.stringify(envelope)}\n`);
};

const cleanups: Array<() => void> = [];
afterEach(() => { vi.restoreAllMocks(); for (const close of cleanups.splice(0)) close(); });

describe.skipIf(process.platform === "win32")("B5 durable recovery generation pin (C1)", () => {
  it("REFUSES a reminted generation instead of admitting a second distinct continuation", async () => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal();
    // Keep this reproducer executable against the unmodified baseline: RED must be
    // the second admission, never a missing allocator import/setup failure.
    const allocator = (admission as unknown as {
      ensureRecoveryGeneration?: (sessionId: string, originalInputId: string) => Promise<string>;
    }).ensureRecoveryGeneration;
    const generation = allocator ? await allocator(f.sessionId, "original") : "baseline-first-generation";
    const request = { sessionId: f.sessionId, originalInputId: "original", recoveryGeneration: generation, text: "resume exact bytes" };
    const first = await admission.admitContinuation(request);
    expect(await admission.admitContinuation(request)).toEqual({ ...first, existing: true });
    const revision = f.snapshot().revision;
    const reminted = await admission.admitContinuation({ ...request, recoveryGeneration: `${generation}:reminted` })
      .then(value => ({ admitted: value }), error => ({ code: (error as admission.RecoveryAdmissionError).code }));
    expect({ reminted, continuations: f.snapshot().inputs.filter(i => i.kind === "continuation").length,
      revisionChange: f.snapshot().revision - revision, nativeAdmissions: f.nativeAdmissions },
    "different g must refuse without a second continuation, revision, or native admission").toEqual({
      reminted: { code: "RECOVERY_CONFLICT" }, continuations: 1, revisionChange: 0, nativeAdmissions: 0,
    });
  });

  it("allocates through the adapter only for the current nonterminal original, never a queue or incomplete history", async () => {
    const f = b5GenerationFixture(cleanups); const api = admission.createRecoveryInputAdapter();
    expect(api.ensureRecoveryGeneration).toBe(admission.ensureRecoveryGeneration);
    await expect(api.ensureRecoveryGeneration(f.sessionId, "absent")).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    f.admit();
    await expect(api.ensureRecoveryGeneration(f.sessionId, "original")).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await f.select(); f.admit("queued");
    const before = f.snapshot();
    await expect(api.ensureRecoveryGeneration(f.sessionId, "queued")).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(api.ensureRecoveryGeneration(f.sessionId, "original", { signal: AbortSignal.abort() })).rejects.toMatchObject({ code: "RECOVERY_ABORTED" });
    await expect(api.ensureRecoveryGeneration(f.sessionId, "original", { deadlineMs: Date.now() - 1 })).rejects.toMatchObject({ code: "RECOVERY_DEADLINE" });
    expect(f.snapshot()).toEqual(before);
    const generation = await api.ensureRecoveryGeneration(f.sessionId, "original");
    expect(generation).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    const pinned = f.snapshot();
    expect(pinned.revision).toBe(before.revision + 1);
    expect(pinned.recoveryGenerationPins).toEqual([{ originalInputId: "original", admissionId: pinned.inputs[0]!.admissionId, recoveryGeneration: generation }]);
    f.reopen();
    expect(await api.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
    expect(f.snapshot()).toEqual(pinned);

    const incomplete = b5GenerationFixture(cleanups, false); incomplete.admit();
    await expect(api.ensureRecoveryGeneration(incomplete.sessionId, "original")).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    expect(incomplete.snapshot().recoveryGenerationPins).toEqual([]);
    const terminal = b5GenerationFixture(cleanups); terminal.admit(); await terminal.select(); await terminal.outcome("user-cancel");
    const cancelled = terminal.snapshot();
    await expect(api.ensureRecoveryGeneration(terminal.sessionId, "original")).rejects.toMatchObject({ code: "RECOVERY_INPUT_TERMINAL" });
    expect(terminal.snapshot()).toEqual(cancelled);
  }, 60_000);

  it("reopens after durable pin/before settlement and during settlement/before projection without reminting or rearming", async () => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal();
    const api = admission.createRecoveryInputAdapter(); const original = clone(f.snapshot().inputs[0]!);
    const generation = await api.ensureRecoveryGeneration(f.sessionId, "original");
    const pin = clone(f.snapshot().recoveryGenerationPins); const pinnedRevision = f.snapshot().revision;
    // First crash cut: pin is durable, but no generation-dependent settlement,
    // receipt production, continuation, or coordinator projection has happened.
    f.reopen(); expect(await api.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
    expect(f.snapshot().revision).toBe(pinnedRevision); expect(f.snapshot().repairs).toEqual([]);
    expect(f.snapshot().inputs).toEqual([original]);

    // A real durable LOCAL destination fixture, not B4/native effect proof. Its
    // retained g/K/bytes are checked on repeat; no second destination write occurs.
    const destination = path.join(f.root, "destination-boundary.jsonl");
    const effect = { recoveryGeneration: generation, originalAdmissionId: original.admissionId, key: "K", bytes: original.round.message };
    const settle = (g: string) => {
      expect(g).toBe(generation);
      if (fs.existsSync(destination)) { expect(fs.readFileSync(destination, "utf8")).toBe(`${JSON.stringify(effect)}\n`); return; }
      const fd = fs.openSync(destination, "wx");
      try { fs.writeSync(fd, `${JSON.stringify(effect)}\n`); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    };
    settle(generation);
    // Second cut: issued work has settled under g; no coordinator marker exists.
    f.reopen(); expect(await api.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation); settle(generation);
    expect(fs.readFileSync(destination, "utf8").trim().split("\n")).toHaveLength(1);
    const repair = repairFor(generation); await admission.recordRecoveryRepair(f.sessionId, repair);
    const request = f.maintenanceRequest(repair); expect(await f.maintenance.admitMaintenance(request)).toMatchObject({ ok: true });
    const retrySlots = clone(f.snapshot().retrySlots);
    expect(retrySlots).toEqual([{ anchor: "original", requestId: "retry" }]);
    // Maintenance adoption is control-only; it must reuse g, not mint another g.
    f.reopen(); expect(await api.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
    expect(await f.maintenance.adoptMaintenance({ sessionId: f.sessionId, requestId: "retry" }, f.owner.epoch, f.snapshot().revision)).toMatchObject({ ok: true });
    expect(await f.finishMaintenance()).toMatchObject({ ok: true });
    const continuation = continuationFor(f.sessionId, generation); const first = await api.admitContinuation(continuation);
    f.reopen();
    expect(await api.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
    expect(await api.admitContinuation(continuation)).toEqual({ ...first, existing: true });
    expect(await f.maintenance.admitMaintenance(f.maintenanceRequest(repair, "another-run"))).toMatchObject({ ok: false, code: "RECOVERY_MAINTENANCE_RETRY_EXHAUSTED" });
    const state = f.snapshot();
    expect(state.inputs[0]).toEqual(original); expect(state.recoveryGenerationPins).toEqual(pin); expect(state.retrySlots).toEqual(retrySlots);
    expect(state.inputs.filter(i => i.kind === "continuation")).toHaveLength(1);
    expect(state.inputs[1]).toMatchObject({ deadlineAt: original.deadlineAt, podId: original.podId, bootId: original.bootId });
    expect(f.nativeAdmissions).toBe(0);
  }, 60_000);

  it.each(["lost-ACK", "abort-after-rename"] as const)("rereads an unknown pin-write %s at the post-rename boundary without reminting", async cut => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal(); const revision = f.snapshot().revision;
    const abort = new AbortController(); let committedGeneration: string | undefined;
    const rename = fs.renameSync;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
      rename(...args);
      if (String(args[1]) !== f.ledgerFile) return;
      const payload = JSON.parse(fs.readFileSync(f.ledgerFile, "utf8")).payload as admission.RecoverySessionLedger;
      committedGeneration = payload.recoveryGenerationPins[0]!.recoveryGeneration;
      if (cut === "lost-ACK") throw new Error("pin directory ACK lost after rename");
      abort.abort();
    });
    await expect(admission.ensureRecoveryGeneration(f.sessionId, "original", { signal: abort.signal })).rejects.toThrow(cut === "lost-ACK" ? /ACK lost/ : /aborted/);
    spy.mockRestore(); expect(committedGeneration).toBeDefined();
    f.reopen();
    expect(await admission.ensureRecoveryGeneration(f.sessionId, "original")).toBe(committedGeneration);
    const state = f.snapshot(); expect(state.revision).toBe(revision + 1); expect(state.recoveryGenerationPins).toHaveLength(1);
    expect(state.repairs).toEqual([]); expect(state.inputs).toHaveLength(1); expect(state.retrySlots).toEqual([]);
    f.reopen(); expect(await admission.ensureRecoveryGeneration(f.sessionId, "original")).toBe(committedGeneration);
    expect(f.snapshot()).toEqual(state);
  }, 60_000);

  it("rereads continuation commit-before-ACK, retains exact retries after terminal, and conflicts on changed bytes", async () => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal();
    const generation = await admission.ensureRecoveryGeneration(f.sessionId, "original");
    const repair = repairFor(generation); await admission.recordRecoveryRepair(f.sessionId, repair);
    const request = continuationFor(f.sessionId, generation); const revision = f.snapshot().revision;
    let retainedAdmissionId: string | undefined; const rename = fs.renameSync;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
      rename(...args); if (String(args[1]) !== f.ledgerFile) return;
      const payload = JSON.parse(fs.readFileSync(f.ledgerFile, "utf8")).payload as admission.RecoverySessionLedger;
      retainedAdmissionId = payload.inputs.find(i => i.kind === "continuation")?.admissionId;
      throw new Error("continuation commit ACK lost");
    });
    await expect(admission.admitContinuation(request)).rejects.toThrow(/ACK lost/); spy.mockRestore();
    expect(retainedAdmissionId).toBeDefined(); f.reopen();
    expect(await admission.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
    expect(await admission.admitContinuation(request)).toEqual({ admissionId: retainedAdmissionId, existing: true });
    expect(f.snapshot().revision).toBe(revision + 1);
    expect(await f.outcome("user-cancel")).toMatchObject({ durable: true, status: "recorded" }); f.reopen();
    const terminal = f.snapshot();
    // Exact g lookup/retries remain available even after the absolute deadline.
    const now = vi.spyOn(Date, "now").mockReturnValue(f.deadlineAt + 1);
    expect(await admission.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
    expect(await admission.admitContinuation(request)).toEqual({ admissionId: retainedAdmissionId, existing: true });
    await expect(admission.recordRecoveryRepair(f.sessionId, repair)).resolves.toBeUndefined();
    await expect(admission.admitContinuation({ ...request, text: `${request.text}changed` })).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(admission.admitContinuation({ ...request, recoveryGeneration: `${generation}:new-boot` })).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(admission.recordRecoveryRepair(f.sessionId, { ...repair, repairReceiptSha256: admission.recoveryHash("changed receipt") })).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(admission.recordRecoveryRepair(f.sessionId, { ...repair, recoveryGeneration: `${generation}:new-boot` })).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    now.mockRestore(); expect(f.snapshot()).toEqual(terminal);
  }, 60_000);

  it("missing/mismatched pins refuse continuation and repair without mutation and cannot bypass maintenance", async () => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal();
    const unpinned = f.snapshot(); const missing = "deliberately-unallocated-control";
    await expect(admission.admitContinuation(continuationFor(f.sessionId, missing))).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    await expect(admission.recordRecoveryRepair(f.sessionId, repairFor(missing))).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    expect(await f.maintenance.admitMaintenance(f.maintenanceRequest(repairFor(missing)))).toMatchObject({ ok: false, code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" });
    expect(f.snapshot()).toEqual(unpinned);
    const generation = await admission.ensureRecoveryGeneration(f.sessionId, "original");
    const pinned = f.snapshot(); const wrong = `${generation}:different`;
    await expect(admission.admitContinuation(continuationFor(f.sessionId, wrong))).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(admission.recordRecoveryRepair(f.sessionId, repairFor(wrong))).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(admission.recordRecoveryRepair(f.sessionId, { ...repairFor(generation), executionBranchEntryId: "not-A" })).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    await expect(admission.recordRecoveryRepair(f.sessionId, { ...repairFor(generation), extra: true } as RepairRef)).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    expect(f.snapshot()).toEqual(pinned);
    const repair = repairFor(generation); await admission.recordRecoveryRepair(f.sessionId, repair);
    const recorded = f.snapshot();
    await expect(admission.recordRecoveryRepair(f.sessionId, { ...repair, repairReceiptSha256: admission.recoveryHash("different") })).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    expect(await f.maintenance.admitMaintenance(f.maintenanceRequest(repairFor(wrong)))).toMatchObject({ ok: false, code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" });
    expect(f.snapshot()).toEqual(recorded);
  }, 60_000);

  it("checks pin schema, duplicate identities/generations, and retained-original admission identity instead of repairing them", async () => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal();
    const generation = await admission.ensureRecoveryGeneration(f.sessionId, "original"); f.admit("other-original");
    const bytes = fs.readFileSync(f.ledgerFile, "utf8");
    type Ledger = admission.RecoverySessionLedger;
    const mutants: Array<[string, (s: Ledger) => void]> = [
      ["missing field", s => { delete (s as unknown as Record<string, unknown>).recoveryGenerationPins; }],
      ["not an array", s => { (s as unknown as Record<string, unknown>).recoveryGenerationPins = {}; }],
      ["null pin", s => { s.recoveryGenerationPins = [null as unknown as admission.RecoveryGenerationPin]; }],
      ["missing generation", s => { delete (s.recoveryGenerationPins[0] as unknown as Record<string, unknown>).recoveryGeneration; }],
      ["blank generation", s => { s.recoveryGenerationPins = [{ ...s.recoveryGenerationPins[0]!, recoveryGeneration: " " }]; }],
      ["extra pin key", s => { Object.assign(s.recoveryGenerationPins[0]!, { epoch: 0 }); }],
      ["extra ledger key", s => { Object.assign(s, { recoveryGeneration: generation }); }],
      ["duplicate exact pin", s => { s.recoveryGenerationPins.push(clone(s.recoveryGenerationPins[0]!)); }],
      ["duplicate original with another generation", s => { s.recoveryGenerationPins.push({ ...s.recoveryGenerationPins[0]!, recoveryGeneration: `${generation}:second` }); }],
      ["unknown original", s => { s.recoveryGenerationPins = [{ ...s.recoveryGenerationPins[0]!, originalInputId: "absent" }]; }],
      ["retained-original admission-id mismatch", s => { s.recoveryGenerationPins = [{ ...s.recoveryGenerationPins[0]!, admissionId: "different-original-admission" }]; }],
      ["another original's admission-id", s => { s.recoveryGenerationPins = [{ ...s.recoveryGenerationPins[0]!, admissionId: s.inputs[1]!.admissionId }]; }],
      ["duplicate generation across originals", s => { s.recoveryGenerationPins.push({ originalInputId: "other-original", admissionId: s.inputs[1]!.admissionId, recoveryGeneration: generation }); }],
    ];
    for (const [name, mutate] of mutants) {
      fs.writeFileSync(f.ledgerFile, bytes); rewriteLedger(f.ledgerFile, mutate); f.reopen();
      const corrupted = fs.readFileSync(f.ledgerFile, "utf8");
      await expect(admission.ensureRecoveryGeneration(f.sessionId, "original"), name).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
      expect(fs.readFileSync(f.ledgerFile, "utf8"), `${name}: no implicit rewrite/remint`).toBe(corrupted);
    }
    fs.writeFileSync(f.ledgerFile, bytes);
    // The pins are actually checksum-covered, not merely validated when convenient.
    const envelope = JSON.parse(bytes); envelope.payload.recoveryGenerationPins[0].recoveryGeneration = "tampered-without-rehash";
    fs.writeFileSync(f.ledgerFile, JSON.stringify(envelope));
    await expect(admission.ensureRecoveryGeneration(f.sessionId, "original")).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    fs.writeFileSync(f.ledgerFile, bytes); expect(await admission.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
  }, 60_000);

  it("refuses absent/contradictory pin evidence in retained continuation, repair and maintenance history", async () => {
    const f = b5GenerationFixture(cleanups); await f.joinedOriginal();
    const generation = await admission.ensureRecoveryGeneration(f.sessionId, "original"); const repair = repairFor(generation);
    await admission.recordRecoveryRepair(f.sessionId, repair);
    expect(await f.maintenance.admitMaintenance(f.maintenanceRequest(repair))).toMatchObject({ ok: true });
    const continuation = continuationFor(f.sessionId, generation); await admission.admitContinuation(continuation);
    const bytes = fs.readFileSync(f.ledgerFile, "utf8");
    const mutants: Array<[string, (s: admission.RecoverySessionLedger) => void]> = [
      ["empty pins with prior evidence", s => { s.recoveryGenerationPins = []; }],
      ["changed pin with prior evidence", s => { s.recoveryGenerationPins = [{ ...s.recoveryGenerationPins[0]!, recoveryGeneration: `${generation}:reminted` }]; }],
      ["continuation admission used as original pin", s => { s.recoveryGenerationPins = [{ ...s.recoveryGenerationPins[0]!, admissionId: s.inputs[1]!.admissionId }]; }],
      ["continuation generation mismatch", s => { s.inputs[1] = { ...s.inputs[1]!, recoveryGeneration: `${generation}:wrong` }; }],
      ["continuation metadata mismatch", s => { const data = s.inputs[1]!.round.data as { scriptedContinuation: { recoveryGeneration: string } }; data.scriptedContinuation.recoveryGeneration = `${generation}:wrong`; }],
      ["repair generation mismatch", s => { s.repairs[0] = { ...repair, recoveryGeneration: `${generation}:wrong` }; }],
      ["maintenance generation mismatch", s => { Object.assign(s.maintenance[0]!.record.request, { repair: { ...repair, recoveryGeneration: `${generation}:wrong` } }); }],
      ["maintenance repair no longer retained", s => { s.repairs = []; }],
      ["original retry lost pin after references removed", s => { s.repairs = []; s.inputs.splice(1); s.recoveryGenerationPins = []; const request = s.maintenance[0]!.record.request as unknown as Record<string, unknown>; delete request.repair; }],
    ];
    for (const [name, mutate] of mutants) {
      fs.writeFileSync(f.ledgerFile, bytes); rewriteLedger(f.ledgerFile, mutate); f.reopen();
      const corrupted = fs.readFileSync(f.ledgerFile, "utf8");
      await expect(admission.ensureRecoveryGeneration(f.sessionId, "original"), name).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
      await expect(admission.admitContinuation(continuation), name).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
      await expect(admission.recordRecoveryRepair(f.sessionId, repair), name).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
      expect(await f.maintenance.read({ sessionId: f.sessionId, requestId: "retry" }), name).toMatchObject({ ok: false, code: "RECOVERY_MAINTENANCE_UNCERTAIN" });
      expect(fs.readFileSync(f.ledgerFile, "utf8"), name).toBe(corrupted);
    }
    fs.writeFileSync(f.ledgerFile, bytes); expect(await admission.ensureRecoveryGeneration(f.sessionId, "original")).toBe(generation);
  }, 60_000);

  it("leaves ordinary non-REQUIRED caller-generation continuation behavior unchanged", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "b5-ordinary-control-")); const sessionId = path.basename(root);
    const deadlineAt = Date.now() + 120_000; const journal = new Map<string, { queued: true; routed: "main"; messageId: string }>();
    const mainAgent: admission.InputAdmissionMainAgent = { id: `session:${sessionId}`, sessionId, local: true,
      deliverAgent(request) {
        const id = request.deliveryId!; const old = journal.get(id); if (old) return { ...old, duplicate: true };
        // Respect native's dedupe-before-data contract, including the deadline getter.
        void request.data; const result = { queued: true as const, routed: "main" as const, messageId: id };
        journal.set(id, result); return result;
      } };
    cleanups.push(admission.bindInputAdmission({ root, sessionId, mainAgent }));
    admission.admitBoundScriptedInput({ sessionId, request: { from: { id: "ordinary-driver", name: "Driver", kind: "main" },
      message: "ordinary original", delivery: "followUp", data: { scriptedInput: { version: 1, inputId: "original", sessionId, deadlineAt } } } });
    const first = continuationFor(sessionId, "ordinary-caller-g1"); const a = await admission.admitContinuation(first);
    expect(await admission.admitContinuation(first)).toEqual({ ...a, existing: true });
    const b = await admission.admitContinuation({ ...first, recoveryGeneration: "ordinary-caller-g2" });
    expect(b.existing).toBe(false); expect(b.admissionId).not.toBe(a.admissionId); expect(journal.size).toBe(3);
    await expect(admission.ensureRecoveryGeneration(sessionId, "original")).rejects.toMatchObject({ code: "RECOVERY_NOT_BOUND" });
  }, 60_000);
});
