import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCertifiedEffectRecordPort } from "../../../src/effects/extension.js";
import { createMaintenanceRepairEngine, createMaintenanceRepairHost } from "../../../src/effects/maintenance-repair-host.js";
import type { RepairNativeEntry, RepairNativeReader, RepairNativeScan } from "../../../src/effects/maintenance-repair.js";
import { admitBoundScriptedInput, bindInputAdmission, commitRecoveryInputDelivery, ensureRecoveryGeneration,
  initializeNewRecoverySession, readRecoverySession, recordInputOutcome, recoveryOwner, releaseRecoveryInputAfterJoin,
  selectRecoveryInput } from "../../../src/recovery/input-admission.js";
import type { RecoveryAdmissionOwner } from "../../../src/recovery/input-admission.js";
import type { EffectIntent, EffectSink, RecoveryOwner } from "../../../src/effects/contracts.js";
import { EffectStore } from "../../../src/effects/store.js";
import { canonicalEncode, mintEffectKey, manifestDigest } from "../../../src/effects/slots.js";
import { prepareFileEffect } from "../../../src/effects/file.js";
import { prepareShellEffect, shellEffectCommand } from "../../../src/effects/shell.js";
import { adapterOriginalResult, canonicalCopy, copy, createSinkGate, fail, hash, immutable, INPUT, INPUT_LIFETIME_MS, intent, joinedBranch, originalResult, resolveScenario, SESSION, type RecoveryIdentity, type Scenario, type ScenarioSource, type ConformanceHarness } from "./fixtures.js";

/** Real B4 engine/store/host and actual B5 lifecycle. Only native-Pi/B6 are
 * explicitly substituted below. Ch1 identities are independent observations of
 * actual B5 selection, admission, delivery, deadline and generation allocation,
 * never copied from fixture identities or the B4 receipt. Component boundaries
 * are not real-native qualification or G2. Existing sink controls and all legacy
 * result-reader behavior remain unchanged. */
export interface RealOptions {
  /** Captured once for the self-owned original offer; reopen never renews it. */
  readonly now?: () => number;
  /** Actual, already delivered/joined B5 observations. The caller retains all
   * B5 initialization, binding, admission, pin, rebind and disposal custody. */
  readonly recoveryIdentity?: () => RecoveryIdentity;
}
export const createReal = async (source: ScenarioSource, options: RealOptions = {}): Promise<ConformanceHarness> => {
  const suppliedIdentity = options.recoveryIdentity ? immutable(copy(options.recoveryIdentity())) : undefined;
  const sessionId = suppliedIdentity?.acceptedInput.sessionId ?? SESSION;
  // B4 storage is always its own tree, never the external B5 receiver's root.
  const root = mkdtempSync(join(tmpdir(), "b4-contract-"));
  const sessionRoot = join(root, "session");
  const workspaceRoot = join(root, "workspace");
  mkdirSync(sessionRoot, { mode: 0o700 });
  mkdirSync(workspaceRoot, { mode: 0o700 });
  // These controls are populated only after independent actual B5 readback.
  let scenario: Scenario;
  const destination = new Set<string>();
  const observed: EffectIntent[] = [];
  const generations: string[] = [];
  let sinkGate: ReturnType<typeof createSinkGate>;
  let lostAck = false;
  let incarnation = 0;
  let initialized = false;
  let nativeEntries: RepairNativeEntry[] = [];
  let nativeReady = false;
  const nativeFile = join(root, "session.jsonl");
  const durableBytes = new Map<string, { seq: number; bytes: Uint8Array }>();
  const drives: number[] = [];
  let carrierEntry: RepairNativeEntry;
  let carrierDeliveryId: string;
  let carrierSequence = 0;
  let rawCarrier: Buffer;
  // Preserve the original self-owned serializer and seq=0. The external route
  // instead uses the driver's exact joinedBranch carrier and retained sequence.
  const userEntry = (text: string) => ({ id: "A", parentId: null, type: "message", timestamp: "2026-10-29T00:00:00.000Z",
    message: { role: "user", content: text } });
  // probe:native-Pi-contract — exact serializer/branch/scan substitute; no Pi
  // writer, fsync receipt, prepared REQUIRED open or actual exclusion is proved.
  const scanNative = (): RepairNativeScan => {
    const header = JSON.stringify({ type: "session", version: 3, id: sessionId, timestamp: "2026-10-29T00:00:00.000Z", cwd: workspaceRoot }) + "\n";
    let offset = Buffer.byteLength(header);
    durableBytes.clear();
    const entries = nativeEntries.map((entry, index) => {
      const bytes = Buffer.from(JSON.stringify(entry) + "\n");
      const deliveryId = entry.id === carrierEntry.id ? carrierDeliveryId
        : "pi-native-entry:v1:" + hash(JSON.stringify([sessionId, entry.id]));
      // The retained carrier is first; later entries have noncolliding monotonic
      // probe sequences without rewriting B5's seq=0 (own) or seq=1 (external).
      durableBytes.set(deliveryId, { seq: carrierSequence + index, bytes: Uint8Array.from(bytes) });
      const candidate = { entryId: entry.id, location: { file: nativeFile, offset, length: bytes.length - 1 }, bytes: Uint8Array.from(bytes), terminated: true };
      offset += bytes.length; return candidate;
    });
    return { sessionId, file: nativeFile, bytes: offset, entries, issues: [] };
  };
  const native: RepairNativeReader = {
    getLeafId: () => nativeEntries.at(-1)?.id ?? null,
    getBranch(leaf) {
      const end = leaf === undefined ? nativeEntries.length - 1 : nativeEntries.findIndex(entry => entry.id === leaf);
      return end < 0 ? [] : copy(nativeEntries.slice(0, end + 1));
    },
    inspectRecoveryEntries: scanNative,
  };
  const materializeNative = async (store: EffectStore) => {
    if (nativeReady) return;
    const records = await store.withSession("settle", tx => tx.list());
    nativeEntries = [copy(carrierEntry)];
    const aliases = records.flatMap(record => [{ record, id: record.intent.tool.originalToolCallId },
      ...record.transitions.filter(row => row.state === "ALIAS").map(row => ({ record, id: row.toolCallId }))]);
    if (aliases.length) nativeEntries.push({ id: "assistant", parentId: carrierEntry.id, type: "message", timestamp: "2026-10-29T00:00:01.000Z",
      message: { role: "assistant", content: aliases.map(({ record, id }) => ({ type: "toolCall", id,
        name: record.intent.tool.canonicalRef.startsWith("pi.") ? record.intent.tool.canonicalRef.slice(3) : record.intent.tool.canonicalRef,
        arguments: record.intent.canonicalArguments })) } } as RepairNativeEntry);
    for (const { record, id } of aliases) if (record.transitions.some(row => row.state === "RESULT")) {
      nativeEntries.push({ id: `result-${id}`, parentId: nativeEntries.at(-1)!.id, type: "message", timestamp: "2026-10-29T00:00:02.000Z",
        message: { ...originalResult(record.intent) as object, toolCallId: id } } as RepairNativeEntry);
    }
    nativeEntries.push({ id: "J", parentId: nativeEntries.at(-1)!.id, type: "message", timestamp: "2026-10-29T00:00:03.000Z",
      message: { role: "assistant", content: [{ type: "text", text: "joined λ" }] } } as RepairNativeEntry);
    scanNative(); nativeReady = true;
  };
  const sink = (kind: string): EffectSink => ({ kind, adapterVersion: "1",
    async reconcile(value, context) {
      observed.push(copy(value)); generations.push(context.recoveryGeneration);
      await sinkGate.enter();
      const outcome = scenario.outcomes?.[value.slot.id] ?? "committed";
      if (outcome === "committed") destination.add(value.key);
      if (lostAck) { lostAck = false; throw new Error("destination committed, reply lost"); }
      return { outcome, ...(outcome === "committed" ? { result: scenario.resultSource === "adapter-original" ? adapterOriginalResult(value) : originalResult(value) } : {}) };
    },
  });
  const open = () => {
    let live = true;
    const epoch = incarnation;
    // On external reopen the driver has already rebound B5. Capture that exact
    // owner object, not an equivalent owner or a snapshot of its epoch fields.
    const externalOwner = suppliedIdentity ? recoveryOwner(sessionId) : undefined;
    const assertCurrent = () => {
      if (!live || epoch !== incarnation) fail("RECOVERY_EPOCH_REVOKED");
      if (externalOwner && recoveryOwner(sessionId) !== externalOwner) fail("RECOVERY_EPOCH_REVOKED");
    };
    const owner: RecoveryOwner = {
      orgId: "contract-org", logicalSessionId: sessionId, sessionRoot, workspaceRoot, sessionFile: nativeFile,
      pvcUID: "fixture-pvc", exclusionReceipt: "probe:native-Pi-contract", signal: new AbortController().signal,
      attempt: { podUID: `pod-${incarnation}`, bootId: `boot-${incarnation}`, ownerIncarnation: `owner-${incarnation}`, authorityRevision: `${incarnation}` },
      assertLive: assertCurrent,
    };
    const store = new EffectStore(owner);
    const port = createCertifiedEffectRecordPort({ owner, store,
      admissionForCall() { throw new Error("conformance must not admit unissued work"); },
      async confirmNativeAssistant() { throw new Error("conformance must not invent a native barrier"); },
      assertManifestConstraints() { throw new Error("conformance must not prepare unissued work"); },
      additionalSinks: [sink("contract-file"), sink("contract-peer")],
      // probe:native-Pi-contract-result — preserve C16–C18/C35's exact legacy
      // result setup. This substituted post-hook join is not production proof.
      async readNativeToolResult(effect) {
        if (scenario.resultSource === "adapter-original") return undefined;
        const value = await store.withSession("settle", tx => tx.read(effect.key).intent);
        return { ...originalResult(value) as object, toolCallId: effect.toolCallId } as Awaited<ReturnType<NonNullable<Parameters<typeof createCertifiedEffectRecordPort>[0]["readNativeToolResult"]>>>;
      },
    });
    // probe:B6-committed-contract — bytes+seq map only; no real registration,
    // committed-log recovery, barrier or process-death persistence is implied.
    const engine = createMaintenanceRepairEngine({ owner, store, native, assertCurrent,
      readNativeToolResult: async effect => {
        if (scenario.resultSource === "adapter-original") return undefined;
        const value = await store.withSession("settle", tx => tx.read(effect.key).intent);
        return { ...originalResult(value) as object, toolCallId: effect.toolCallId } as Awaited<ReturnType<NonNullable<Parameters<typeof createCertifiedEffectRecordPort>[0]["readNativeToolResult"]>>>;
      },
      durability: { async readDurableEntry(request) {
        assertCurrent(); const saved = durableBytes.get(request.deliveryId);
        return request.sessionId === sessionId && request.recordKind === "carrier" && saved
          ? { found: true as const, seq: saved.seq, bytes: Uint8Array.from(saved.bytes) }
          : { found: false as const, reason: "not-found" as const };
      } },
    });
    const admissionOwner: RecoveryAdmissionOwner = externalOwner ?? {
      epoch: { processIncarnationId: `process-${epoch}`, bindingId: `binding-${epoch}`, epoch },
      podId: `pod-${epoch}`, bootId: `boot-${epoch}`, historyComplete: true, assertCurrent,
      reconsiderInput() { assertCurrent(); drives.push(epoch); },
      validateRepair(ref) { engine.resolveRepair(ref); },
    };
    let unbind: (() => void) | undefined;
    if (!suppliedIdentity) {
      if (!initialized) {
        initializeNewRecoverySession({ mode: "NEW", sessionId, root: sessionRoot, recovery: admissionOwner,
          assertNew() { assertCurrent(); if (initialized) fail("RECOVERY_CONFLICT"); } });
        initialized = true;
      }
      unbind = bindInputAdmission({ sessionId, root: sessionRoot, recovery: admissionOwner,
        mainAgent: { id: `session:${sessionId}`, sessionId, local: true,
          deliverAgent() { throw new Error("probe:native-Pi-contract: REQUIRED B5 must not call legacy Main delivery"); } } });
    }
    const host = createMaintenanceRepairHost({ owner, engine, recordPort: port, native, admissionOwner });
    const provider: ConformanceHarness["provider"] = { ...host,
      async repairMaintenance(input, leaf) { await materializeNative(store); return host.repairMaintenance(input, leaf); },
    };
    // Initial construction never produces a repair or calls validateRepair:
    // the external driver's validator cannot use h.provider until we return h.
    return { store, port, engine, provider, admissionOwner, close() { engine.invalidateRepairResolution(); unbind?.(); live = false; } };
  };
  let runtime = open();
  let expectedIdentity: RecoveryIdentity;
  let assertSelfOwnedSelection: ((identity: RecoveryIdentity) => void) | undefined;
  if (suppliedIdentity) {
    // Observe only: never initialize, bind, admit, select, allocate, recommit,
    // release or unbind the external driver's already joined B5 occurrence.
    expectedIdentity = suppliedIdentity;
    const input = suppliedIdentity.acceptedInput;
    const occurrence = readRecoverySession(sessionId).inputs.find(row =>
      row.originalInputId === input.originalInputId && row.admissionId === input.admissionId);
    if (!occurrence?.delivery) fail("External actual B5 delivery is absent");
    // Same actual identity and serializer as real-b5.ts; no canonical reorder,
    // timestamp repair, replacement commitment, or receipt-derived carrier.
    carrierEntry = copy(joinedBranch("J", suppliedIdentity)[0]!) as unknown as RepairNativeEntry;
    carrierDeliveryId = input.deliveryId;
    carrierSequence = occurrence.delivery.seq;
    rawCarrier = Buffer.from(JSON.stringify(carrierEntry) + "\n");
    nativeEntries = [copy(carrierEntry)];
    scanNative();
  } else {
    // Offer metadata is not an accepted identity. Actual B5 admits/selects it,
    // allocates g and commits delivery; reopen never repeats this bootstrap.
    const offer = { sessionId, inputId: INPUT, text: "original input",
      deadlineAt: (options.now ?? Date.now)() + INPUT_LIFETIME_MS };
    const admission = admitBoundScriptedInput({ sessionId: offer.sessionId, request: { from: { id: "contract-sender", name: "Contract sender", kind: "agent" },
      message: offer.text, delivery: "followUp", data: { scriptedInput: { version: 1, inputId: offer.inputId,
        sessionId: offer.sessionId, deadlineAt: offer.deadlineAt } } } });
    const selected = await selectRecoveryInput({ sessionId: offer.sessionId, originalInputId: offer.inputId,
      epoch: runtime.admissionOwner.epoch, expectedRevision: readRecoverySession(offer.sessionId).revision });
    if (!selected.selected) fail(selected.code);
    if (admission.messageId !== selected.input.admissionId || admission.deadlineAt !== selected.input.deadlineMs ||
        selected.input.deadlineMs !== offer.deadlineAt) fail("Actual B5 admission and selection observations disagree");
    const allocatedGeneration = await ensureRecoveryGeneration(selected.input.sessionId, selected.input.originalInputId);
    carrierEntry = userEntry(selected.input.text);
    carrierDeliveryId = selected.input.deliveryId;
    rawCarrier = Buffer.from(JSON.stringify(carrierEntry) + "\n");
    nativeEntries = [copy(carrierEntry)];
    scanNative(); // probe:B6-committed-contract readback precedes B5 delivery commitment.
    await commitRecoveryInputDelivery(selected.input, { entryId: carrierEntry.id, executionBranchEntryId: carrierEntry.id,
      bytesSha256: hash(rawCarrier), seq: carrierSequence });
    const interrupted = await recordInputOutcome({ sessionId: selected.input.sessionId, originalInputId: selected.input.originalInputId, kind: "lifecycle-interrupt",
      requestId: "contract-interrupt", at: Date.now(), bootId: runtime.admissionOwner.bootId, deadlineMs: selected.input.deadlineMs });
    if (!interrupted.durable) fail(interrupted.reason);
    // probe:native-Pi-contract — already-joined task boundary, not an actual Pi join/drain.
    await releaseRecoveryInputAfterJoin(selected.input, Promise.resolve(), async () => { runtime.admissionOwner.assertCurrent(); });
    expectedIdentity = immutable({
      acceptedInput: { sessionId: selected.input.sessionId, originalInputId: selected.input.originalInputId, admissionId: selected.input.admissionId,
        deliveryId: selected.input.deliveryId, kind: "initial", text: selected.input.text,
        payloadSha256: hash(selected.input.text), deadlineMs: offer.deadlineAt, consumption: { kind: "round" } },
      recoveryGeneration: allocatedGeneration, executionBranchEntryId: carrierEntry.id,
    });
    assertSelfOwnedSelection = identity => {
      const input = identity.acceptedInput;
      if (input.admissionId !== admission.messageId || input.admissionId !== selected.input.admissionId ||
          input.deliveryId !== selected.input.deliveryId || input.text !== selected.input.text ||
          input.deadlineMs !== admission.deadlineAt || input.deadlineMs !== selected.input.deadlineMs ||
          identity.recoveryGeneration !== allocatedGeneration) fail("Actual B5 admission/selection observations disagree");
    };
  }
  const recoveryIdentity = (): RecoveryIdentity => {
    const expected = expectedIdentity.acceptedInput;
    const state = readRecoverySession(sessionId);
    const original = state.inputs.find(row => row.kind === "round" && row.originalInputId === expected.originalInputId);
    const occurrence = state.inputs.find(row => row.originalInputId === expected.originalInputId && row.admissionId === expected.admissionId);
    const pin = state.recoveryGenerationPins.find(row => row.originalInputId === expected.originalInputId && row.admissionId === original?.admissionId);
    if (!state.complete || state.reservation?.kind === "input" || !original?.delivery || !occurrence?.delivery || !pin ||
        pin.recoveryGeneration !== expectedIdentity.recoveryGeneration || state.sessionId !== expected.sessionId ||
        expected.deliveryId !== occurrence.admissionId || expected.kind !== (occurrence.kind === "round" ? "initial" : "recovery") ||
        occurrence.round.message !== expected.text || hash(occurrence.round.message) !== expected.payloadSha256 ||
        original.deadlineAt !== expected.deadlineMs || occurrence.deadlineAt !== original.deadlineAt ||
        canonicalEncode(expected.consumption) !== canonicalEncode({ kind: "round" }) ||
        original.delivery.executionBranchEntryId !== expectedIdentity.executionBranchEntryId ||
        occurrence.delivery.executionBranchEntryId !== expectedIdentity.executionBranchEntryId ||
        original.delivery.entryId !== carrierEntry.id || occurrence.delivery.entryId !== carrierEntry.id ||
        original.delivery.bytesSha256 !== hash(rawCarrier) || occurrence.delivery.bytesSha256 !== hash(rawCarrier) ||
        original.delivery.seq !== carrierSequence || occurrence.delivery.seq !== carrierSequence) {
      return fail("Actual B5 selection/delivery/deadline/pin observations disagree");
    }
    const identity: RecoveryIdentity = {
      acceptedInput: { sessionId: state.sessionId, originalInputId: occurrence.originalInputId, admissionId: occurrence.admissionId,
        deliveryId: occurrence.admissionId, kind: occurrence.kind === "round" ? "initial" : "recovery", text: occurrence.round.message,
        payloadSha256: hash(occurrence.round.message), deadlineMs: original.deadlineAt, consumption: { kind: "round" } },
      recoveryGeneration: pin.recoveryGeneration, executionBranchEntryId: original.delivery.executionBranchEntryId,
    };
    assertSelfOwnedSelection?.(identity);
    return immutable(identity);
  };
  // Authenticate before callback resolution; then derive every scenario intent,
  // alias and committed-before-ACK destination key from these observations.
  recoveryIdentity();
  scenario = resolveScenario(source, { recoveryIdentity });
  for (const key of scenario.committedBeforeAck ?? []) destination.add(key);
  sinkGate = createSinkGate(scenario.stallSink);
  lostAck = scenario.lostAck === true;
  const issue = async (value: EffectIntent) => { await runtime.store.withSession("new-intent", tx => tx.bind(canonicalCopy(value))); };
  for (const value of scenario.intents ?? []) await issue(value);
  const target = join(workspaceRoot, "file.txt");
  if (scenario.fileState || scenario.shellState) {
    writeFileSync(target, "before", { mode: 0o644 });
    const shell = scenario.shellState !== undefined;
    const args = shell ? { command: shellEffectCommand("file.txt", hash("before")) } : { path: "file.txt", content: "after" };
    const prepared = shell
      ? await prepareShellEffect(workspaceRoot, args, { kind: "read-sha256-v1", path: "file.txt", sha256: hash("before") })
      : await prepareFileEffect(workspaceRoot, "write", args);
    const identity = recoveryIdentity();
    const base = intent("file", 0, [], false, identity);
    const slot = { ...base.slot, kind: shell ? "shell-check" : "file-write", canonicalRef: shell ? "pi.bash" : "pi.write", canonicalTarget: target, canonicalArguments: args };
    const tuple = [1, "contract-org", identity.acceptedInput.sessionId, base.acceptedInput.id, slot.id, slot.kind] as const;
    const text = prepared.format === "fabric-file-effect-v1" ? Buffer.from(prepared.desiredBase64, "base64") : Buffer.from(canonicalEncode(prepared));
    const payload = { encoding: "base64" as const, bytes: text.toString("base64"), digest: hash(text) };
    await issue({ ...base, key: mintEffectKey(tuple), keyTuple: tuple, slot, canonicalArguments: args,
      canonicalTarget: target, manifestDigest: manifestDigest({ version: 1, slots: [slot] }),
      tool: { ...base.tool, canonicalRef: slot.canonicalRef }, payload, adapterData: canonicalCopy(prepared) as unknown as EffectIntent["canonicalArguments"],
      ...(prepared.format !== "fabric-file-effect-v1" ? {} : { file: { preExists: prepared.pre.exists, preHash: prepared.pre.sha256,
        preMode: prepared.pre.mode, postHash: prepared.post.sha256!, postMode: prepared.post.mode!, desiredBytes: payload,
        preparedResult: canonicalCopy(prepared.preparedResult) as unknown as EffectIntent["canonicalArguments"] } }) });
    if (scenario.fileState === "post") writeFileSync(target, "after");
    if (scenario.fileState === "neither" || scenario.shellState === "changed") writeFileSync(target, "foreign");
  }
  if (scenario.alias) await runtime.store.withSession("settle", tx => tx.append(scenario.alias!.key, {
    state: "ALIAS", toolCallId: scenario.alias!.toolCallId, attempt: runtime.store.owner.attempt, at: "2026-10-29T00:00:00.000Z",
  }));
  if (scenario.corrupt) {
    const key = scenario.intents?.[0]?.key;
    if (!key) fail("fixture requires an intent to corrupt");
    writeFileSync(join(sessionRoot, "effects", key, "intent.json"), "corrupt");
  }
  const harness: ConformanceHarness = { provider: runtime.provider, recoveryIdentity, issue,
    async reopen() {
      runtime.close(); incarnation++; runtime = open();
      // Publish the replacement before preparing retained refs: an external
      // B5 validator routes through h.provider, not the invalidated old engine.
      harness.provider = runtime.provider;
      recoveryIdentity(); // Same original deadline, delivery and genuine pin.
      for (const ref of readRecoverySession(sessionId).repairs) await runtime.engine.prepareRepairResolution(ref);
    },
    destination: () => [...destination], observedIntents: () => copy(observed), generations: () => [...generations],
    async ledgerDigest() {
      const files: Array<[string, string]> = [];
      const walk = (directory: string) => {
        for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
          const file = join(directory, entry.name);
          if (entry.isDirectory()) walk(file); else files.push([file.slice(root.length), hash(readFileSync(file))]);
        }
      };
      walk(sessionRoot); return hash(canonicalEncode(files));
    },
    joinedBranch: () => copy(nativeEntries.map(entry => ({ ...entry }))), sinkGate,
    advanceJoinedLeaf(nextLeafId = "J2") {
      nativeEntries.push({ id: nextLeafId, parentId: nativeEntries.at(-1)!.id, type: "message", timestamp: "2026-10-29T00:00:04.000Z",
        message: { role: "assistant", content: [{ type: "text", text: "later joined λ" }] } } as RepairNativeEntry);
      scanNative();
    },
    fileSnapshot() { const stat = statSync(target); return { bytes: readFileSync(target, "utf8"), identity: `${stat.dev}:${stat.ino}` }; },
    async dispose() { try { sinkGate.release(); runtime.close(); } finally { rmSync(root, { recursive: true, force: true }); } },
  };
  return harness;
};
