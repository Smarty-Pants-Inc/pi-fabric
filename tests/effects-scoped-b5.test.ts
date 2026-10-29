import {
  appendFileSync, closeSync, fsyncSync, lstatSync, mkdirSync, mkdtempSync, openSync,
  readFileSync, readdirSync, realpathSync, rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { AcceptedInput, EffectIntent, EffectSlot, JsonValue, RecoveryOwner } from "../src/effects/contracts.js";
import { createCertifiedEffectRecordPort } from "../src/effects/extension.js";
import type { EffectFailpointName } from "../src/effects/failpoints.js";
import { prepareFileEffect } from "../src/effects/file.js";
import { canonicalEncode, manifestDigest, mintEffectKey, sha256Bytes } from "../src/effects/slots.js";
import { EffectStore } from "../src/effects/store.js";
import {
  admitBoundScriptedInput, admitContinuation, bindInputAdmission, commitRecoveryInputDelivery,
  ensureRecoveryGeneration, initializeNewRecoverySession, readRecoveryInput, readRecoverySession,
  recordInputOutcome, releaseRecoveryInputAfterJoin, selectRecoveryInput,
  type AcceptedRecoveryInput, type RecoveryAdmissionOwner, type RecoveryInputDelivery,
} from "../src/recovery/input-admission.js";

// Keep reasons: cross-component trust boundary + protection of unrelated durable
// bytes. Actual B5 admission/history/pins + actual B4 store/record port/file sink.
// Native owner/carrier/join evidence is explicitly probe:native-Pi, NOT native Pi
// or B6 certification. No fake B5 API, pin/repair validator, crypto mock, generic
// session transaction seeding, direct ledger-file writes, or context remapping.
// B4 historical issued-intent fixtures use its validated EffectStore.bind API;
// this does NOT prove live dispatch can admit KB past another pending KA.
// Main2 owns snapshot/pin/marker pre-write policy. This is NOT its caller driver,
// does not bind/duplicate C21/C22, and must not be reported as S3/G2 GREEN.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const cleanups: Array<() => void> = [];
afterEach(() => { for (const cleanup of cleanups.splice(0).reverse()) cleanup(); });

/** Full bytes, names, directories, modes and retained link counts. The B4 store
 * retains ordinary stages AND publication witnesses; neither may be omitted. */
function inventory(directory: string): unknown {
  const entries: Array<{ path: string; mode: number; links: number; bytes?: string }> = [];
  const visit = (file: string, relative: string) => {
    const stat = lstatSync(file);
    if (stat.isSymbolicLink() || (!stat.isFile() && !stat.isDirectory())) throw new Error("Unexpected test-owned inventory entry");
    entries.push({ path: relative, mode: stat.mode & 0o7777, links: stat.nlink,
      ...(stat.isFile() ? { bytes: readFileSync(file).toString("base64") } : {}) });
    if (stat.isDirectory()) for (const child of readdirSync(file).sort()) visit(join(file, child), `${relative}/${child}`);
  };
  visit(directory, ".");
  return entries;
}

function fixture(initialize = true) {
  const root = mkdtempSync(join(tmpdir(), "fabric-real-b5-scoped-b4-"));
  const sessionId = `real-b5-scoped:${basename(root)}`;
  for (const child of ["B5", "effects-session", "workspace"]) mkdirSync(join(root, child), { mode: 0o700 });
  const b5Root = realpathSync(join(root, "B5"));
  const sessionRoot = realpathSync(join(root, "effects-session"));
  const workspaceRoot = realpathSync(join(root, "workspace"));
  const sessionFile = join(root, "probe-native-Pi.jsonl");
  const deadlineAt = Date.now() + 15 * 60_000;
  let boot = 0;
  let live = true;
  let unbind = () => {};
  let ownerChecks = 0;
  let cutAt: number | undefined;
  let cutError: Error | undefined;
  let nativeDeliveryCalls = 0;
  let reconsiderations = 0;
  let sequence = 0;
  let effectBoundary: ((point: EffectFailpointName, details?: Record<string, unknown>) => void) | undefined;
  const mainAgent = { id: `session:${sessionId}`, local: true, sessionId,
    deliverAgent(): never {
      nativeDeliveryCalls += 1;
      throw new Error("probe:native-Pi: REQUIRED B5 admission must not deliver through ordinary Main");
    } };
  const makeNativeOwner = (): RecoveryAdmissionOwner => {
    const incarnation = boot;
    return {
      epoch: { processIncarnationId: `probe:native-Pi:process:${incarnation}`, bindingId: `probe:native-Pi:binding:${incarnation}`, epoch: 0 },
      podId: `probe:native-Pi:pod:${incarnation}`, bootId: `probe:native-Pi:boot:${incarnation}`, historyComplete: true,
      assertCurrent() {
        if (!live || incarnation !== boot) throw new Error("probe:native-Pi: old owner excluded");
        ownerChecks += 1;
        if (cutAt !== undefined && ownerChecks === cutAt) {
          cutAt = undefined;
          throw cutError!;
        }
      },
      reconsiderInput() { reconsiderations += 1; },
      // No validateRepair: this fixture is not a B3 receipt validator/producer.
    };
  };
  let nativeOwner = makeNativeOwner();
  if (initialize) initializeNewRecoverySession({ mode: "NEW", sessionId, root: b5Root, recovery: nativeOwner,
    assertNew() {
      if (boot !== 0 || !live) throw new Error("probe:native-Pi: no authoritative NEW grant");
    } });
  const bind = () => { unbind = bindInputAdmission({ sessionId, root: b5Root, mainAgent, recovery: nativeOwner }); };
  bind();
  const makeEffectOwner = (): RecoveryOwner => {
    const incarnation = boot;
    return { orgId: "real-b5-scoped-component-org", logicalSessionId: sessionId, sessionRoot, workspaceRoot, sessionFile,
      pvcUID: "probe:native-Pi:retained-volume", exclusionReceipt: `probe:native-Pi:excluded:${incarnation}`,
      attempt: { podUID: `probe:native-Pi:pod:${incarnation}`, bootId: `probe:native-Pi:boot:${incarnation}`,
        ownerIncarnation: `probe:native-Pi:owner:${incarnation}`, authorityRevision: `probe:native-Pi:authority:${incarnation}` },
      signal: new AbortController().signal,
      assertLive() { if (!live || incarnation !== boot) throw new Error("probe:native-Pi: old B4 owner excluded"); } };
  };
  let effectOwner = makeEffectOwner();
  let store = new EffectStore(effectOwner);
  const makePort = () => createCertifiedEffectRecordPort({ owner: effectOwner, store,
    admissionForCall(): never { throw new Error("Settlement must use stored original identity, not current context"); },
    async confirmNativeAssistant(): Promise<never> { throw new Error("Settlement must not synthesize a new native assistant"); },
    assertManifestConstraints(): never { throw new Error("Settlement must use the stored manifest, not prepare a new call"); },
    async failpoint(point, details) { effectBoundary?.(point, details); },
    // No producer, resolver, native result join or validateRepair double.
  });
  let port = makePort();
  cleanups.push(() => {
    live = false; unbind();
    // Only this fixture's mkdtemp root is ever deleted.
    rmSync(root, { recursive: true, force: true });
  });
  const state = () => readRecoverySession(sessionId);
  const admit = (originalInputId: string) => admitBoundScriptedInput({ sessionId, request: {
    from: { id: "scoped-real-input-driver", name: "Real API test driver", kind: "main" },
    message: ` exact retained ${originalInputId} payload: 雪\n`, delivery: "steer", priority: "interrupt", triggerTurn: true,
    data: { scriptedInput: { version: 1, inputId: originalInputId, sessionId, deadlineAt }, payload: { originalInputId, order: [2, 1] } },
  } });
  const select = async (originalInputId: string) => {
    const selected = await selectRecoveryInput({ sessionId, originalInputId, epoch: nativeOwner.epoch, expectedRevision: state().revision });
    if (!selected.selected) throw new Error(`Actual B5 selection refused: ${selected.code}`);
    return selected.input;
  };
  const appendProbe = (entryId: string, body: Record<string, unknown>) => {
    const bytes = JSON.stringify({ entryId, probe: "probe:native-Pi", ...body });
    appendFileSync(sessionFile, `${bytes}\n`, { mode: 0o600 });
    const fd = openSync(sessionFile, "r");
    try { fsyncSync(fd); } finally { closeSync(fd); }
    const parent = openSync(root, "r");
    try { fsyncSync(parent); } finally { closeSync(parent); }
    return { entryId, bytesSha256: sha256Bytes(bytes), seq: ++sequence };
  };
  const deliver = async (input: AcceptedRecoveryInput): Promise<RecoveryInputDelivery> => {
    const native = appendProbe(`probe:native-Pi:input:${input.originalInputId}`, { kind: "input", originalInputId: input.originalInputId,
      admissionId: input.admissionId, text: input.text, deadlineAt: input.deadlineMs });
    const evidence = { ...native, executionBranchEntryId: `probe:native-Pi:branch:${input.originalInputId}` };
    await commitRecoveryInputDelivery(input, evidence);
    return evidence;
  };
  const finish = async (input: AcceptedRecoveryInput, kind: "user-cancel" | "lifecycle-interrupt" | "settled") => {
    const original = state().inputs.find(row => row.kind === "round" && row.originalInputId === input.originalInputId)!;
    const terminal = kind === "settled" ? appendProbe(`probe:native-Pi:terminal:${input.originalInputId}`, { kind: "terminal", originalInputId: input.originalInputId }) : undefined;
    const outcome = await recordInputOutcome({ sessionId, originalInputId: input.originalInputId, requestId: `${kind}:${input.admissionId}:${boot}`,
      kind, at: Date.now(), bootId: nativeOwner.bootId, deadlineMs: Math.min(deadlineAt, Date.now() + 10_000),
      ...(terminal === undefined ? {} : { terminalEvidence: { ...terminal, inputEntryId: original.delivery!.entryId,
        executionBranchEntryId: original.delivery!.executionBranchEntryId, runId: `probe:native-Pi:run:${input.originalInputId}`,
        finalEntryIds: [terminal.entryId] } }) });
    expect(outcome).toMatchObject({ durable: true, status: "recorded" });
    // Explicit native join probe, not a fabricated B5 API. Actual release still
    // reads/checks the retained outcome and drives its real durable release edge.
    await releaseRecoveryInputAfterJoin(input, Promise.resolve(), async () => {});
  };
  const issued = async (originalInputId: string, slotId = "same-slot"): Promise<EffectIntent> => {
    const original = state().inputs.find(row => row.kind === "round" && row.originalInputId === originalInputId)!;
    if (!original?.delivery) throw new Error("Issued B4 fixture requires actual retained B5 admission/delivery");
    const args = { path: `${originalInputId}-${slotId}.txt`, content: `retained effect ${originalInputId}/${slotId}: 雪\n` };
    const selected: EffectSlot = { id: slotId, ordinal: 0, kind: "file-write", canonicalRef: "pi.write", adapterVersion: "1",
      canonicalTarget: join(workspaceRoot, args.path), canonicalArguments: args, constraints: {}, predecessors: [] };
    const manifest = { version: 1 as const, slots: [selected] };
    const accepted: AcceptedInput = { id: original.originalInputId, digest: sha256Bytes(canonicalEncode(original.round)),
      nativeEntryId: original.delivery.entryId, manifest, manifestDigest: manifestDigest(manifest) };
    const prepared = await prepareFileEffect(workspaceRoot, "write", args);
    const keyTuple = [1, effectOwner.orgId, sessionId, accepted.id, selected.id, selected.kind] as const;
    const payload = { encoding: "base64" as const, bytes: prepared.desiredBase64, digest: prepared.post.sha256! };
    const intent: EffectIntent = { format: "fabric.effect-intent", version: 1, key: mintEffectKey(keyTuple), keyTuple,
      manifestDigest: accepted.manifestDigest, acceptedInput: { id: accepted.id, digest: accepted.digest, nativeEntryId: accepted.nativeEntryId },
      slot: selected, tool: { canonicalRef: selected.canonicalRef, adapterVersion: "1", originalToolCallId: `retained-call:${originalInputId}:${slotId}`,
        nativeAssistantEntryId: `probe:native-Pi:assistant:${originalInputId}:${slotId}` }, canonicalArguments: args,
      canonicalTarget: selected.canonicalTarget, payload, originAttempt: effectOwner.attempt, adapterData: prepared as unknown as JsonValue,
      file: { preExists: prepared.pre.exists, preHash: prepared.pre.sha256, preMode: prepared.pre.mode,
        postHash: prepared.post.sha256!, postMode: prepared.post.mode!, desiredBytes: payload, preparedResult: prepared.preparedResult as unknown as JsonValue } };
    await store.withSession("new-intent", tx => tx.bind(intent));
    return intent;
  };
  return { root, sessionId, b5Root, sessionRoot, workspaceRoot, sessionFile, deadlineAt, state, admit, select, deliver, finish, issued,
    get nativeOwner() { return nativeOwner; }, get store() { return store; }, get port() { return port; },
    get nativeDeliveryCalls() { return nativeDeliveryCalls; }, get reconsiderations() { return reconsiderations; },
    setEffectBoundary(callback?: typeof effectBoundary) { effectBoundary = callback; },
    cutNativeOwnerAt(check: number, error: Error) { ownerChecks = 0; cutAt = check; cutError = error; },
    reopen() {
      unbind(); boot += 1; nativeOwner = makeNativeOwner(); bind();
      effectOwner = makeEffectOwner(); store = new EffectStore(effectOwner); port = makePort();
    },
    record(key: string) { return store.withSession("settle", tx => tx.read(key)); },
    async originalInventory(intent: EffectIntent) {
      let destination: string | null = null;
      try { destination = readFileSync(intent.canonicalTarget).toString("base64"); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      return { ledger: inventory(join(sessionRoot, "effects", intent.key)), destination };
    },
  };
}

type Fixture = ReturnType<typeof fixture>;
async function terminalAWithOpenB(f: Fixture, terminalKind: "user-cancel" | "settled" = "user-cancel") {
  const aReceipt = f.admit("A");
  const a = await f.select("A"); await f.deliver(a);
  const gA = await ensureRecoveryGeneration(f.sessionId, "A");
  const KA = await f.issued("A");
  await f.finish(a, terminalKind);
  const bReceipt = f.admit("B");
  const b = await f.select("B"); await f.deliver(b);
  const gB = await ensureRecoveryGeneration(f.sessionId, "B");
  const KB = await f.issued("B");
  await f.finish(b, "lifecycle-interrupt");
  expect(gA).toMatch(UUID); expect(gB).toMatch(UUID); expect(gA).not.toBe(gB);
  return { aReceipt, bReceipt, a, b, gA, gB, KA, KB };
}

// Explicit POSIX skip: the real retained B4 store/file sink require namespace
// fsync + no-follow guarantees. A platform skip is not passing real-port evidence.
describe.skipIf(process.platform === "win32")("actual B5 + real B4 scoped integration; probe:native-Pi boundary", () => {
  it.each(["user-cancel", "settled"] as const)("terminal-issued A (%s) + current OPEN B: preserve complete KB inventory, then settle B/gB", async terminalKind => {
    const f = fixture();
    const { aReceipt, bReceipt, a, b, gA, gB, KA, KB } = await terminalAWithOpenB(f, terminalKind);
    expect((await readRecoveryInput(f.sessionId)).accepted).toMatchObject({ inputId: "B", settlement: "open", admissionId: bReceipt.messageId });
    expect(f.state().currentInputId).toBe("B");
    expect(f.state().inputs.filter(row => row.kind === "round").map(row => [row.originalInputId, row.admissionId])).toEqual([["A", aReceipt.messageId], ["B", bReceipt.messageId]]);
    expect(KA.acceptedInput.id).toBe("A"); expect(KA.keyTuple[3]).toBe("A");
    expect(KB.acceptedInput.id).toBe("B"); expect(KB.keyTuple[3]).toBe("B");
    expect(KA.slot.id).toBe(KB.slot.id); expect(KA.key).not.toBe(KB.key);
    expect((await f.record(KA.key)).transitions).toEqual([]);
    expect((await f.record(KB.key)).transitions).toEqual([]);
    // Capture EVERY retained KB name/byte/mode/link, not a digest of selected rows.
    const beforeB = await f.originalInventory(KB);
    const b5Before = inventory(f.b5Root);
    const nativeBefore = readFileSync(f.sessionFile);
    const pins = f.state().recoveryGenerationPins;
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "A", gA)).toMatchObject({ uncertain: 0,
      settled: [{ key: KA.key, toolCallId: KA.tool.originalToolCallId, outcome: "committed" }] });
    expect(await f.originalInventory(KB)).toEqual(beforeB);
    expect(inventory(f.b5Root)).toEqual(b5Before);
    expect(readFileSync(f.sessionFile)).toEqual(nativeBefore);
    expect((await f.record(KA.key)).transitions.map(row => row.recoveryGeneration)).toEqual([gA, gA]);
    const afterA = await f.originalInventory(KA);
    f.reopen();
    expect(f.state().recoveryGenerationPins).toEqual(pins);
    expect(await ensureRecoveryGeneration(f.sessionId, "A")).toBe(gA);
    expect(await ensureRecoveryGeneration(f.sessionId, "B")).toBe(gB);
    expect(f.state().inputs.find(row => row.kind === "round" && row.originalInputId === "A")!.admissionId).toBe(a.admissionId);
    expect(f.state().inputs.find(row => row.kind === "round" && row.originalInputId === "B")!.admissionId).toBe(b.admissionId);
    expect(inventory(f.b5Root)).toEqual(b5Before);
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "B", gB)).toMatchObject({ uncertain: 0,
      settled: [{ key: KB.key, toolCallId: KB.tool.originalToolCallId, outcome: "committed" }] });
    expect(await f.originalInventory(KA)).toEqual(afterA);
    expect((await f.record(KB.key)).transitions.map(row => row.recoveryGeneration)).toEqual([gB, gB]);
    expect(readFileSync(KA.canonicalTarget, "utf8")).toBe("retained effect A/same-slot: 雪\n");
    expect(readFileSync(KB.canonicalTarget, "utf8")).toBe("retained effect B/same-slot: 雪\n");
    const settledBytes = inventory(f.sessionRoot);
    await f.port.settleOriginalPendingIntents(f.sessionId, "A", gA);
    await f.port.settleOriginalPendingIntents(f.sessionId, "B", gB);
    expect(inventory(f.sessionRoot)).toEqual(settledBytes);
    expect(inventory(f.b5Root)).toEqual(b5Before);
    expect(f.state().inputs.every(row => row.deadlineAt === f.deadlineAt)).toBe(true);
    expect(f.state().inputs.every(row => row.kind === "round")).toBe(true);
    expect(f.nativeDeliveryCalls).toBe(0);
    expect(f.reconsiderations).toBe(2);
  });

  it("SOURCE-BOUND LIMIT: native release after lifecycle interruption does not permit a second logical-OPEN pin", async () => {
    const f = fixture();
    f.admit("A"); const a = await f.select("A"); await f.deliver(a);
    const gA = await ensureRecoveryGeneration(f.sessionId, "A");
    f.admit("B");
    await f.finish(a, "lifecycle-interrupt");
    expect(f.state().reservation).toBeUndefined();
    expect(f.state().currentInputId).toBe("A");
    expect((await readRecoveryInput(f.sessionId)).accepted).toMatchObject({ inputId: "A", settlement: "open" });
    const before = inventory(f.b5Root);
    expect(await selectRecoveryInput({ sessionId: f.sessionId, originalInputId: "B", epoch: f.nativeOwner.epoch,
      expectedRevision: f.state().revision })).toMatchObject({ selected: false, code: "RECOVERY_INPUT_BUSY" });
    await expect(ensureRecoveryGeneration(f.sessionId, "B")).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    expect(inventory(f.b5Root)).toEqual(before);
    f.reopen();
    expect(await ensureRecoveryGeneration(f.sessionId, "A")).toBe(gA);
    expect(f.state().recoveryGenerationPins).toEqual([{ originalInputId: "A", admissionId: a.admissionId, recoveryGeneration: gA }]);
    expect(f.state().currentInputId).toBe("A");
    expect(inventory(f.b5Root)).toEqual(before);
    // No seeded dual-OPEN state and no claim this supplies the requested fixture.
  });

  it("actual B5 continuation API refuses cross-original, random and unpinned generations without mutation; correct B remains valid", async () => {
    const f = fixture();
    const { gA, gB } = await terminalAWithOpenB(f);
    await f.port.settleOriginalPendingIntents(f.sessionId, "A", gA);
    await f.port.settleOriginalPendingIntents(f.sessionId, "B", gB);
    f.admit("C");
    const before = inventory(f.root);
    for (const request of [{ originalInputId: "A", recoveryGeneration: gB }, { originalInputId: "B", recoveryGeneration: gA },
      { originalInputId: "B", recoveryGeneration: "caller-random-g2" }]) {
      await expect(admitContinuation({ sessionId: f.sessionId, ...request, text: "unchanged continuation" }))
        .rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
      expect(inventory(f.root)).toEqual(before);
    }
    await expect(admitContinuation({ sessionId: f.sessionId, originalInputId: "C", recoveryGeneration: gB, text: "unpinned" }))
      .rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    expect(inventory(f.root)).toEqual(before);
    const receipt = await admitContinuation({ sessionId: f.sessionId, originalInputId: "B", recoveryGeneration: gB, text: "unchanged continuation" });
    expect(receipt.existing).toBe(false);
    const retained = f.state();
    expect(retained.inputs.find(row => row.kind === "continuation")).toMatchObject({ originalInputId: "B", recoveryGeneration: gB, deadlineAt: f.deadlineAt, admissionId: receipt.admissionId });
    f.reopen();
    const beforeRetry = inventory(f.b5Root);
    expect(await admitContinuation({ sessionId: f.sessionId, originalInputId: "B", recoveryGeneration: gB, text: "unchanged continuation" }))
      .toEqual({ ...receipt, existing: true });
    expect(inventory(f.b5Root)).toEqual(beforeRetry);
    expect(f.state().recoveryGenerationPins).toEqual(retained.recoveryGenerationPins);
    // This proves actual API validation, NOT Main's missing pre-B4 ordering.
  });

  it("probe:native-Pi: unknown pin-write ACK reads back the exact durable UUID and never remints after reopen", async () => {
    const f = fixture();
    f.admit("A"); const a = await f.select("A"); await f.deliver(a);
    const cut = new Error("probe:native-Pi: lost pin-write ACK after durable save");
    // Source-pinned native-owner fault: commitRecoverySession checks owner at
    // enter, after read, after change, pre-save, pre-rename, then POST-save (#6).
    // No fs/crypto/API mock. If that path changes, durable readback below must
    // fail rather than manufacturing a pin in the driver.
    f.cutNativeOwnerAt(6, cut);
    await expect(ensureRecoveryGeneration(f.sessionId, "A")).rejects.toBe(cut);
    const persisted = f.state();
    expect(persisted.recoveryGenerationPins).toHaveLength(1);
    const pin = persisted.recoveryGenerationPins[0]!;
    expect(pin).toMatchObject({ originalInputId: "A", admissionId: a.admissionId });
    expect(pin.recoveryGeneration).toMatch(UUID);
    const before = inventory(f.root);
    f.reopen();
    expect(await ensureRecoveryGeneration(f.sessionId, "A")).toBe(pin.recoveryGeneration);
    expect(f.state()).toEqual(persisted);
    expect(inventory(f.root)).toEqual(before);
    expect(f.state().inputs).toHaveLength(1);
    expect(f.nativeDeliveryCalls).toBe(0);
  });

  it("real file settlement cut and unknown continuation ACK retain exact original pin/admission without redo or revival", async () => {
    const f = fixture();
    const { b, gA, gB, KA, KB } = await terminalAWithOpenB(f);
    const beforeB = await f.originalInventory(KB);
    let commits = 0;
    f.setEffectBoundary((point, details) => {
      if (point === "destination-committed" && details?.key === KA.key) {
        commits += 1;
        if (commits === 1) throw new Error("probe:native-Pi: crash boundary after real file commit, before effect RESULT");
      }
    });
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "A", gA)).toMatchObject({ uncertain: 1 });
    expect((await f.record(KA.key)).transitions.map(row => row.state)).toEqual(["ATTEMPT", "UNCERTAIN"]);
    expect(await f.originalInventory(KB)).toEqual(beforeB);
    const inode = lstatSync(KA.canonicalTarget).ino;
    const retainedPins = f.state().recoveryGenerationPins;
    f.reopen();
    expect(await ensureRecoveryGeneration(f.sessionId, "A")).toBe(gA);
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "A", gA)).toMatchObject({ uncertain: 0 });
    expect(lstatSync(KA.canonicalTarget).ino).toBe(inode); // Real post-state readback, not replacement.
    expect(await f.originalInventory(KB)).toEqual(beforeB);
    expect((await f.record(KA.key)).transitions.every(row => row.recoveryGeneration === gA)).toBe(true);
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "B", gB)).toMatchObject({ uncertain: 0 });
    const cut = new Error("probe:native-Pi: continuation persisted before ACK was lost");
    f.cutNativeOwnerAt(6, cut);
    await expect(admitContinuation({ sessionId: f.sessionId, originalInputId: "B", recoveryGeneration: gB, text: "resume exact B" })).rejects.toBe(cut);
    const retained = f.state();
    const continuation = retained.inputs.find(row => row.kind === "continuation")!;
    expect(continuation).toMatchObject({ originalInputId: "B", recoveryGeneration: gB, deadlineAt: b.deadlineMs });
    expect(continuation.admissionId).toBeTruthy();
    f.reopen();
    const beforeRetry = inventory(f.root);
    expect(await ensureRecoveryGeneration(f.sessionId, "B")).toBe(gB);
    expect(await admitContinuation({ sessionId: f.sessionId, originalInputId: "B", recoveryGeneration: gB, text: "resume exact B" }))
      .toEqual({ admissionId: continuation.admissionId, existing: true });
    expect(f.state()).toEqual(retained);
    expect(f.state().recoveryGenerationPins).toEqual(retainedPins);
    expect(inventory(f.root)).toEqual(beforeRetry);
    expect(f.nativeDeliveryCalls).toBe(0);
  });

  it("pending-only: terminal retained original with issued KA settles under its old pin with no accepted current input", async () => {
    const f = fixture();
    f.admit("A"); const a = await f.select("A"); await f.deliver(a);
    const gA = await ensureRecoveryGeneration(f.sessionId, "A");
    const KA = await f.issued("A");
    await f.finish(a, "user-cancel");
    const snapshot = await readRecoveryInput(f.sessionId);
    expect(snapshot.complete).toBe(true); expect(snapshot.accepted).toBeUndefined(); expect(snapshot.queued).toEqual([]);
    expect((await f.record(KA.key)).transitions).toEqual([]);
    const retained = f.state(); const native = readFileSync(f.sessionFile);
    f.reopen();
    expect(await ensureRecoveryGeneration(f.sessionId, "A")).toBe(gA);
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "A", gA)).toMatchObject({ uncertain: 0, settled: [{ key: KA.key, outcome: "committed" }] });
    expect(f.state()).toEqual(retained);
    expect(readFileSync(f.sessionFile)).toEqual(native);
    await expect(admitContinuation({ sessionId: f.sessionId, originalInputId: "A", recoveryGeneration: gA, text: "must not revive" }))
      .rejects.toMatchObject({ code: "RECOVERY_INPUT_TERMINAL" });
    expect(f.state()).toEqual(retained);
  });

  it("terminal-issued original without a retained pin refuses allocation rather than borrowing a generation to settle", async () => {
    const f = fixture();
    f.admit("A"); const a = await f.select("A"); await f.deliver(a);
    const KA = await f.issued("A");
    await f.finish(a, "user-cancel");
    expect(f.state().recoveryGenerationPins).toEqual([]);
    expect((await f.record(KA.key)).transitions).toEqual([]);
    const before = inventory(f.root);
    await expect(ensureRecoveryGeneration(f.sessionId, "A")).rejects.toMatchObject({ code: "RECOVERY_INPUT_TERMINAL" });
    expect(inventory(f.root)).toEqual(before);
    expect((await f.record(KA.key)).transitions).toEqual([]);
    // B4 is deliberately NOT called with a random/borrowed g. This checks only
    // the actual B5 allocator refusal, not an unimplemented Main caller hold.
  });

  it("open/delivered original with zero issued intents has an empty scoped result, not a fabricated repair receipt", async () => {
    const f = fixture();
    f.admit("A"); const a = await f.select("A"); await f.deliver(a);
    const generation = await ensureRecoveryGeneration(f.sessionId, "A");
    expect(generation).toMatch(UUID);
    expect((await readRecoveryInput(f.sessionId)).accepted).toMatchObject({ inputId: "A", settlement: "open", admissionId: a.admissionId });
    const before = inventory(f.root);
    expect(await f.port.settleOriginalPendingIntents(f.sessionId, "A", generation)).toEqual({ settled: [], uncertain: 0 });
    expect(inventory(f.root)).toEqual(before);
    expect(f.state().repairs).toEqual([]);
    expect(f.state().inputs).toHaveLength(1);
    // Q2 approves a real bound empty repair receipt. Only the additive B4
    // settlement's empty result is exercised here; no receipt producer is used.
  });

  it("missing REQUIRED history is incomplete, not clean empty, and does not authorize a fresh pin", async () => {
    const f = fixture(false); // Deliberately no NEW seal; no bytes removed or forged.
    const before = inventory(f.root);
    expect(await readRecoveryInput(f.sessionId)).toMatchObject({ complete: false, queued: [], outcomes: [] });
    expect(() => readRecoverySession(f.sessionId)).toThrowError(expect.objectContaining({ code: "RECOVERY_HISTORY_INCOMPLETE" }));
    await expect(ensureRecoveryGeneration(f.sessionId, "A")).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    expect(inventory(f.root)).toEqual(before);
    // Do not infer clean history from B4 emptiness or repair by initializing it.
  });

  it("clean NEW history is distinct from pending-only: reads establish emptiness without allocating or borrowing a generation", async () => {
    const f = fixture();
    const before = inventory(f.root);
    expect(await readRecoveryInput(f.sessionId)).toMatchObject({ complete: true, queued: [], outcomes: [] });
    expect((await readRecoveryInput(f.sessionId)).accepted).toBeUndefined();
    expect(await f.store.withSession("settle", tx => tx.list())).toEqual([]);
    expect(f.state().recoveryGenerationPins).toEqual([]);
    expect(f.state().inputs).toEqual([]);
    expect(inventory(f.root)).toEqual(before);
    // No settlement call: its generation-dependent API is not an empty-history
    // detector and this driver may not invent a random/borrowed pin to call it.
  });
});
