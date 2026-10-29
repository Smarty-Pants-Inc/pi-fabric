import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCertifiedEffectRecordPort } from "../../../src/effects/extension.js";
import { createMaintenanceRepairEngine, createMaintenanceRepairHost } from "../../../src/effects/maintenance-repair-host.js";
import type { RepairNativeEntry, RepairNativeReader, RepairNativeScan } from "../../../src/effects/maintenance-repair.js";
import { admitBoundScriptedInput, bindInputAdmission, commitRecoveryInputDelivery, ensureRecoveryGeneration,
  initializeNewRecoverySession, readRecoverySession, recordInputOutcome, releaseRecoveryInputAfterJoin,
  selectRecoveryInput } from "../../../src/recovery/input-admission.js";
import type { RecoveryAdmissionOwner } from "../../../src/recovery/input-admission.js";
import type { EffectIntent, EffectSink, RecoveryOwner } from "../../../src/effects/contracts.js";
import { EffectStore } from "../../../src/effects/store.js";
import { canonicalEncode, mintEffectKey, manifestDigest } from "../../../src/effects/slots.js";
import { prepareFileEffect } from "../../../src/effects/file.js";
import { prepareShellEffect, shellEffectCommand } from "../../../src/effects/shell.js";
import { adapterOriginalResult, canonicalCopy, copy, createSinkGate, fail, hash, immutable, INPUT, INPUT_LIFETIME_MS, intent, originalResult, SESSION, type RecoveryIdentity, type Scenario, type ConformanceHarness } from "./fixtures.js";

/** Real B4 engine/store/host and actual B5 lifecycle. Only native-Pi/B6 are
 * explicitly substituted below. Ch1 identities are independent observations of
 * actual B5 selection, admission, delivery, deadline and generation allocation,
 * never copied from fixture identities or the B4 receipt. Component boundaries
 * are not real-native qualification or G2. Existing sink controls and all legacy
 * result-reader behavior remain unchanged. */
export interface RealOptions {
  /** Captured once for the original offer; reopen never renews its deadline. */
  readonly now?: () => number;
}
export const createReal = async (scenario: Scenario, options: RealOptions = {}): Promise<ConformanceHarness> => {
  const root = mkdtempSync(join(tmpdir(), "b4-contract-"));
  const sessionRoot = join(root, "session");
  const workspaceRoot = join(root, "workspace");
  mkdirSync(sessionRoot, { mode: 0o700 });
  mkdirSync(workspaceRoot, { mode: 0o700 });
  const destination = new Set(scenario.committedBeforeAck ?? []);
  const observed: EffectIntent[] = [];
  const generations: string[] = [];
  const sinkGate = createSinkGate(scenario.stallSink);
  let lostAck = scenario.lostAck === true;
  let incarnation = 0;
  let initialized = false;
  let nativeEntries: RepairNativeEntry[] = [];
  let nativeReady = false;
  const nativeFile = join(root, "session.jsonl");
  const durableBytes = new Map<string, { seq: number; bytes: Uint8Array }>();
  const drives: number[] = [];
  const userEntry = (text: string) => ({ id: "A", parentId: null, type: "message", timestamp: "2026-10-29T00:00:00.000Z",
    message: { role: "user", content: text } });
  // probe:native-Pi-contract — exact serializer/branch/scan substitute; no Pi
  // writer, fsync receipt, prepared REQUIRED open or actual exclusion is proved.
  const scanNative = (): RepairNativeScan => {
    const header = JSON.stringify({ type: "session", version: 3, id: SESSION, timestamp: "2026-10-29T00:00:00.000Z", cwd: workspaceRoot }) + "\n";
    let offset = Buffer.byteLength(header);
    durableBytes.clear();
    const entries = nativeEntries.map((entry, seq) => {
      const bytes = Buffer.from(JSON.stringify(entry) + "\n");
      const deliveryId = entry.id === "A" ? readRecoverySession(SESSION).inputs.find(row => row.kind === "round")!.admissionId
        : "pi-native-entry:v1:" + hash(JSON.stringify([SESSION, entry.id]));
      durableBytes.set(deliveryId, { seq, bytes: Uint8Array.from(bytes) });
      const candidate = { entryId: entry.id, location: { file: nativeFile, offset, length: bytes.length - 1 }, bytes: Uint8Array.from(bytes), terminated: true };
      offset += bytes.length; return candidate;
    });
    return { sessionId: SESSION, file: nativeFile, bytes: offset, entries, issues: [] };
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
    nativeEntries = [userEntry(recoveryIdentity().acceptedInput.text)];
    const aliases = records.flatMap(record => [{ record, id: record.intent.tool.originalToolCallId },
      ...record.transitions.filter(row => row.state === "ALIAS").map(row => ({ record, id: row.toolCallId }))]);
    if (aliases.length) nativeEntries.push({ id: "assistant", parentId: "A", type: "message", timestamp: "2026-10-29T00:00:01.000Z",
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
    const assertCurrent = () => { if (!live || epoch !== incarnation) fail("RECOVERY_EPOCH_REVOKED"); };
    const owner: RecoveryOwner = {
      orgId: "contract-org", logicalSessionId: SESSION, sessionRoot, workspaceRoot, sessionFile: nativeFile,
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
        return request.sessionId === SESSION && request.recordKind === "carrier" && saved
          ? { found: true as const, seq: saved.seq, bytes: Uint8Array.from(saved.bytes) }
          : { found: false as const, reason: "not-found" as const };
      } },
    });
    const admissionOwner: RecoveryAdmissionOwner = { epoch: { processIncarnationId: `process-${epoch}`, bindingId: `binding-${epoch}`, epoch },
      podId: `pod-${epoch}`, bootId: `boot-${epoch}`, historyComplete: true, assertCurrent,
      reconsiderInput() { assertCurrent(); drives.push(epoch); },
      validateRepair(ref) { engine.resolveRepair(ref); },
    };
    if (!initialized) {
      initializeNewRecoverySession({ mode: "NEW", sessionId: SESSION, root: sessionRoot, recovery: admissionOwner,
        assertNew() { assertCurrent(); if (initialized) fail("RECOVERY_CONFLICT"); } });
      initialized = true;
    }
    const unbind = bindInputAdmission({ sessionId: SESSION, root: sessionRoot, recovery: admissionOwner,
      mainAgent: { id: `session:${SESSION}`, sessionId: SESSION, local: true,
        deliverAgent() { throw new Error("probe:native-Pi-contract: REQUIRED B5 must not call legacy Main delivery"); } } });
    const host = createMaintenanceRepairHost({ owner, engine, recordPort: port, native, admissionOwner });
    const provider: ConformanceHarness["provider"] = { ...host,
      async repairMaintenance(input, leaf) { await materializeNative(store); return host.repairMaintenance(input, leaf); },
    };
    return { store, port, engine, provider, admissionOwner, close() { engine.invalidateRepairResolution(); unbind(); live = false; } };
  };
  let runtime = open();
  // Offer metadata is not an accepted identity. Actual B5 admits/selects it and
  // allocates g; the accessor below independently rereads retained evidence.
  const offer = { sessionId: SESSION, inputId: INPUT, text: "original input",
    deadlineAt: (options.now ?? Date.now)() + INPUT_LIFETIME_MS };
  const admission = admitBoundScriptedInput({ sessionId: offer.sessionId, request: { from: { id: "contract-sender", name: "Contract sender", kind: "agent" },
    message: offer.text, delivery: "followUp", data: { scriptedInput: { version: 1, inputId: offer.inputId,
      sessionId: offer.sessionId, deadlineAt: offer.deadlineAt } } } });
  const selected = await selectRecoveryInput({ sessionId: offer.sessionId, originalInputId: offer.inputId,
    epoch: runtime.admissionOwner.epoch, expectedRevision: readRecoverySession(offer.sessionId).revision });
  if (!selected.selected) fail(selected.code);
  if (admission.messageId !== selected.input.admissionId || admission.deadlineAt !== selected.input.deadlineMs) {
    fail("Actual B5 admission and selection observations disagree");
  }
  const allocatedGeneration = await ensureRecoveryGeneration(selected.input.sessionId, selected.input.originalInputId);
  nativeEntries = [userEntry(selected.input.text)];
  scanNative(); // probe:B6-committed-contract readback exists before B5 delivery commitment.
  const rawCarrier = Buffer.from(JSON.stringify(nativeEntries[0]) + "\n");
  await commitRecoveryInputDelivery(selected.input, { entryId: "A", executionBranchEntryId: "A", bytesSha256: hash(rawCarrier), seq: 0 });
  const interrupted = await recordInputOutcome({ sessionId: selected.input.sessionId, originalInputId: selected.input.originalInputId, kind: "lifecycle-interrupt",
    requestId: "contract-interrupt", at: Date.now(), bootId: runtime.admissionOwner.bootId, deadlineMs: selected.input.deadlineMs });
  if (!interrupted.durable) fail(interrupted.reason);
  // probe:native-Pi-contract — already-joined task boundary, not an actual Pi join/drain.
  await releaseRecoveryInputAfterJoin(selected.input, Promise.resolve(), async () => { runtime.admissionOwner.assertCurrent(); });
  const recoveryIdentity = (): RecoveryIdentity => {
    const state = readRecoverySession(selected.input.sessionId);
    const original = state.inputs.find(row => row.kind === "round" && row.originalInputId === selected.input.originalInputId);
    const occurrence = state.inputs.find(row => row.originalInputId === selected.input.originalInputId && row.admissionId === selected.input.admissionId);
    const pin = state.recoveryGenerationPins.find(row => row.originalInputId === selected.input.originalInputId && row.admissionId === original?.admissionId);
    if (!original?.delivery || !occurrence?.delivery || !pin || pin.recoveryGeneration !== allocatedGeneration ||
        occurrence.admissionId !== admission.messageId || selected.input.deliveryId !== occurrence.admissionId ||
        occurrence.round.message !== selected.input.text || original.deadlineAt !== selected.input.deadlineMs ||
        occurrence.delivery.bytesSha256 !== hash(rawCarrier) || occurrence.delivery.seq !== 0 ||
        occurrence.delivery.entryId !== nativeEntries[0]?.id) {
      return fail("Actual B5 selection/delivery/deadline/pin observations disagree");
    }
    const identity: RecoveryIdentity = {
      acceptedInput: { sessionId: state.sessionId, originalInputId: occurrence.originalInputId, admissionId: occurrence.admissionId,
        deliveryId: selected.input.deliveryId, kind: occurrence.kind === "round" ? "initial" : "recovery", text: selected.input.text,
        payloadSha256: hash(selected.input.text), deadlineMs: original.deadlineAt, consumption: { kind: "round" } },
      recoveryGeneration: pin.recoveryGeneration, executionBranchEntryId: original.delivery.executionBranchEntryId,
    };
    return immutable(identity);
  };
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
      for (const ref of readRecoverySession(SESSION).repairs) await runtime.engine.prepareRepairResolution(ref);
      harness.provider = runtime.provider;
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
