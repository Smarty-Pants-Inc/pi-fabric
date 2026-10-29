import { Buffer } from "node:buffer";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  EffectContractError, type AcceptedInput, type EffectIntent, type EffectSink,
  type EffectSlot, type EffectToolResult, type EffectTransitionInput, type JsonValue, type RecoveryOwner,
} from "../src/effects/contracts.js";
import { createCertifiedEffectRecordPort, type CertifiedEffectRecordOptions } from "../src/effects/extension.js";
import { prepareFileEffect } from "../src/effects/file.js";
import {
  bindEffectReconciler, createEffectReconciler, reconcileOriginalPending, reconcilePending,
  settleOriginalPendingIntents,
} from "../src/effects/reconcile.js";
import { canonicalEncode, manifestDigest, mintEffectKey, sha256Bytes } from "../src/effects/slots.js";
import { EffectStore } from "../src/effects/store.js";

// Data-safety/trust-boundary component tests: real retained EffectStore,
// reconciliation and record-port projection, not B5 pin policy or G2 evidence.
// No B5 is simulated. Only the injected native reader/assistant acknowledgement
// boundaries below carry probe: labels; genuine native/B6 qualification is RED.
const roots: string[] = [];
const at = "2026-01-02T03:04:05.000Z";
const g1 = "opaque-generation-for-O1";
const g2 = "different-opaque-generation-for-O2";

afterEach(() => {
  // Deletion is confined to directories allocated by this file's mkdtempSync.
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function provision() {
  const root = mkdtempSync(join(tmpdir(), "fabric-scoped-settlement-"));
  roots.push(root);
  mkdirSync(join(root, "session"), { mode: 0o700 });
  mkdirSync(join(root, "workspace"), { mode: 0o700 });
  const controller = new AbortController();
  const owner: RecoveryOwner = {
    orgId: "component-org", logicalSessionId: `component-session:${root}`,
    sessionRoot: realpathSync(join(root, "session")), workspaceRoot: realpathSync(join(root, "workspace")),
    sessionFile: join(root, "native.jsonl"), pvcUID: "component-volume", exclusionReceipt: "component-exclusive-owner",
    attempt: { podUID: "component-pod", bootId: "component-boot", ownerIncarnation: "component-owner", authorityRevision: "component-revision" },
    signal: controller.signal,
    assertLive(permission) {
      expect(["settle", "new-intent"]).toContain(permission);
      if (controller.signal.aborted) throw new EffectContractError("EFFECT_OWNER_REVOKED", "Component owner was revoked");
    },
  };
  return { owner, store: new EffectStore(owner), controller };
}

function slot(owner: RecoveryOwner, id: string, ordinal = 0, predecessors: readonly string[] = [], kind = "component-effect"): EffectSlot {
  return {
    id, ordinal, predecessors, kind, canonicalRef: "pi.write", adapterVersion: "1",
    canonicalTarget: join(owner.workspaceRoot, `${id}.txt`),
    canonicalArguments: { path: `${id}.txt`, content: `immutable:${id}` }, constraints: {},
  };
}

function input(id: string, slots: readonly EffectSlot[]): AcceptedInput {
  const manifest = { version: 1 as const, slots };
  return { id, digest: sha256Bytes(`original:${id}`), nativeEntryId: `original-carrier:${id}`,
    manifest, manifestDigest: manifestDigest(manifest) };
}

function intent(owner: RecoveryOwner, accepted: AcceptedInput, selected = accepted.manifest.slots[0]!): EffectIntent {
  const keyTuple = [1, owner.orgId, owner.logicalSessionId, accepted.id, selected.id, selected.kind] as const;
  const bytes = Buffer.from(canonicalEncode(selected.canonicalArguments), "utf8");
  return {
    format: "fabric.effect-intent", version: 1, key: mintEffectKey(keyTuple), keyTuple,
    manifestDigest: accepted.manifestDigest,
    acceptedInput: { id: accepted.id, digest: accepted.digest, nativeEntryId: accepted.nativeEntryId },
    slot: selected, tool: { canonicalRef: selected.canonicalRef, adapterVersion: selected.adapterVersion,
      originalToolCallId: `call:${accepted.id}:${selected.id}`, nativeAssistantEntryId: `assistant:${accepted.id}:${selected.id}` },
    canonicalArguments: selected.canonicalArguments, canonicalTarget: selected.canonicalTarget,
    payload: { encoding: "base64", bytes: bytes.toString("base64"), digest: sha256Bytes(bytes) }, originAttempt: owner.attempt,
  };
}

function transition(owner: RecoveryOwner, toolCallId: string, body: Pick<EffectTransitionInput, "state"> & Partial<EffectTransitionInput>): EffectTransitionInput {
  return { toolCallId, attempt: owner.attempt, at, ...body };
}

function adapterOriginal(record: EffectIntent): JsonValue {
  return { role: "toolResult", toolCallId: record.tool.originalToolCallId, toolName: "write", isError: false,
    content: [{ type: "text", text: `saved:${record.key}` }], details: { original: true } };
}

async function bind(store: EffectStore, records: readonly EffectIntent[]) {
  await store.withSession("new-intent", tx => { for (const record of records) tx.bind(record); });
}

async function terminal(owner: RecoveryOwner, store: EffectStore, record: EffectIntent, aliases: readonly string[] = [], outcome: "committed" | "not-committed" = "committed") {
  const result = adapterOriginal(record);
  await store.withSession("settle", tx => {
    for (const toolCallId of aliases) tx.append(record.key, transition(owner, toolCallId, { state: "ALIAS" }));
    tx.append(record.key, transition(owner, record.tool.originalToolCallId, { state: "ATTEMPT", recoveryGeneration: "retained-terminal-generation" }));
    tx.append(record.key, transition(owner, record.tool.originalToolCallId, { state: "RESULT", outcome, result,
      resultBytesHash: sha256Bytes(canonicalEncode(result)), recoveryGeneration: "retained-terminal-generation" }));
  });
}

const ledger = (store: EffectStore) => store.withSession("settle", tx => tx.list());

// Include retained publication stages/witnesses as well as canonical records:
// an ambiguity/terminal/empty call must not create an unreported write either.
function diskSnapshot(directory: string): Record<string, string> {
  const files: Record<string, string> = {};
  const visit = (path: string, prefix: string) => {
    for (const entry of readdirSync(path, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const name = `${prefix}${entry.name}`;
      if (entry.isDirectory()) visit(join(path, entry.name), `${name}/`);
      else files[name] = readFileSync(join(path, entry.name)).toString("base64");
    }
  };
  visit(directory, "");
  return files;
}

const sink = (run: EffectSink["reconcile"]): EffectSink => ({ kind: "component-effect", adapterVersion: "1", reconcile: run });

function recordPort(owner: RecoveryOwner, store: EffectStore, accepted: AcceptedInput, readNativeToolResult?: CertifiedEffectRecordOptions["readNativeToolResult"]) {
  return createCertifiedEffectRecordPort({
    owner, store,
    admissionForCall(call) {
      expect(call.context.cwd).toBe(owner.workspaceRoot);
      return { acceptedInput: accepted, nativeAssistantEntryId: `probe:B6:assistant:${call.toolCallId}` };
    },
    async confirmNativeAssistant(call) {
      // probe:B6: component acknowledgement only, not real native durability.
      expect(call.nativeAssistantEntryId).toBe(`probe:B6:assistant:${call.toolCallId}`);
      await owner.assertLive("new-intent");
    },
    assertManifestConstraints(selected, prepared) {
      expect(accepted.manifest.slots.some(candidate => canonicalEncode(candidate) === canonicalEncode(selected))).toBe(true);
      expect(prepared.workspaceRoot).toBe(owner.workspaceRoot);
    },
    ...(readNativeToolResult === undefined ? {} : { readNativeToolResult }),
  });
}

function newWrite(port: ReturnType<typeof recordPort>, owner: RecoveryOwner, selected: EffectSlot) {
  const args = selected.canonicalArguments as Record<string, unknown>;
  const prepare = vi.fn(() => prepareFileEffect(owner.workspaceRoot, "write", args));
  const run = () => port.execute({ canonicalRef: "pi.write", adapterVersion: "1", kind: "file-write",
    canonicalTarget: selected.canonicalTarget, canonicalArguments: args, toolCallId: "new-physical-write",
    context: { cwd: owner.workspaceRoot } as ExtensionContext, prepare });
  return { prepare, run };
}

// One event-driven timeout is only a missing-lock-release safety net, not polling.
async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Scoped native projection did not release the store lock")), 3_000);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}

describe.skipIf(process.platform === "win32")("original-scoped settlement (retained POSIX component)", () => {
  it("leaves O1 uncertain at g1 while O2 completes at g2 without unrelated writes, projection or generation stamping", async () => {
    const { owner, store } = provision();
    const peerBody = { text: "exact original payload", deadline: "2026-01-02T04:04:05.000Z" };
    const selected = { ...slot(owner, "peer"), canonicalTarget: "logical-recipient:original" };
    const base = intent(owner, input("O1", [selected]));
    const bytes = Buffer.from(canonicalEncode(peerBody), "utf8");
    const first: EffectIntent = { ...base,
      payload: { encoding: "base64", bytes: bytes.toString("base64"), digest: sha256Bytes(bytes) },
      message: { logicalRecipient: selected.canonicalTarget, immutableBody: peerBody, bodyDigest: sha256Bytes(bytes),
        firstOfferedAt: at, originalDeadlineAt: peerBody.deadline, envelopeIdentity: { id: "retained-envelope" } } };
    const second = intent(owner, input("O2", [slot(owner, "second")]));
    await bind(store, [first, second]);
    const observations: Array<[string, string]> = [];
    const effects = sink(async (record, context) => {
      observations.push([record.acceptedInput.id, context.recoveryGeneration]);
      expect(record).toEqual(record.acceptedInput.id === "O1" ? first : second);
      if (record.acceptedInput.id === "O1") return { outcome: "uncertain", reason: "Recipient outcome is not definitive" };
      const result = adapterOriginal(record);
      return { outcome: "committed", result };
    });
    const uncertain = await reconcileOriginalPending(owner, store, [effects], "O1", g1);
    expect(uncertain.uncertain).toBe(1);
    const beforeO2 = await store.withSession("settle", tx => tx.read(first.key));
    const originalFiles = diskSnapshot(join(owner.sessionRoot, "effects", first.key));
    const completed = await createEffectReconciler({ owner, store, sinks: [effects] }).reconcileOriginalPending("O2", g2);
    expect(completed.uncertain).toBe(0);
    expect(completed.settled).toEqual([expect.objectContaining({ key: second.key, outcome: "committed" })]);
    expect(completed.repairs.map(row => row.key)).toEqual([second.key]);
    expect(completed.ledgerFrontier.map(row => row.key)).toEqual([second.key]);
    expect(completed.unresolved).toEqual([]);
    expect(observations).toEqual([["O1", g1], ["O2", g2]]);
    const after = await ledger(store);
    expect(after.find(row => row.intent.key === first.key)).toEqual(beforeO2);
    expect(diskSnapshot(join(owner.sessionRoot, "effects", first.key))).toEqual(originalFiles);
    for (const record of after) {
      expect(record.intent).toEqual(record.intent.key === first.key ? first : second);
      expect(record.transitions.map(row => row.recoveryGeneration)).toEqual([record.intent.key === first.key ? g1 : g2, record.intent.key === first.key ? g1 : g2]);
    }
  });

  it("retains and traverses predecessors inside the selected original, including out-of-ordinal dependencies", async () => {
    const { owner, store } = provision();
    const predecessor = slot(owner, "predecessor", 10);
    const dependent = slot(owner, "dependent", 0, [predecessor.id]);
    const accepted = input("O2", [dependent, predecessor]);
    const first = intent(owner, accepted, predecessor);
    const second = intent(owner, accepted, dependent);
    const foreign = intent(owner, input("O1", [slot(owner, predecessor.id)]));
    await bind(store, [second, foreign, first]);
    const foreignBefore = await store.withSession("settle", tx => tx.read(foreign.key));
    const calls: string[] = [];
    const effects = sink(async (record, context) => {
      expect(context.recoveryGeneration).toBe(g2);
      expect(record.acceptedInput.id).toBe("O2");
      calls.push(record.slot.id);
      return { outcome: "committed", result: adapterOriginal(record) };
    });
    const result = await reconcileOriginalPending(owner, store, [effects], "O2", g2);
    expect(calls).toEqual(["predecessor", "dependent"]);
    expect(result.settled.map(row => row.key)).toEqual([first.key, second.key]);
    expect(result.uncertain).toBe(0);
    expect(await store.withSession("settle", tx => tx.read(foreign.key))).toEqual(foreignBefore);
  });

  it("does not satisfy a missing predecessor from another original, but projects terminal results before dependency lookup", async () => {
    const { owner, store } = provision();
    const predecessor = slot(owner, "shared-slot", 0);
    const dependent = slot(owner, "dependent", 1, [predecessor.id]);
    const foreign = intent(owner, input("O1", [predecessor]));
    const selected = intent(owner, input("O2", [predecessor, dependent]), dependent);
    const historical = intent(owner, input("O3", [predecessor, dependent]), dependent);
    await bind(store, [foreign, selected, historical]);
    await terminal(owner, store, foreign);
    await terminal(owner, store, historical, ["historical-alias"], "not-committed");
    const historicalFiles = diskSnapshot(join(owner.sessionRoot, "effects", historical.key));
    const calls = vi.fn<EffectSink["reconcile"]>(async record => ({ outcome: "committed", result: adapterOriginal(record) }));
    const pending = await reconcileOriginalPending(owner, store, [sink(calls)], "O2", g2);
    expect(pending.uncertain).toBe(1);
    expect(pending.unresolved).toEqual([{ key: selected.key, reason: "Predecessor shared-slot has no committed result" }]);
    expect(calls).not.toHaveBeenCalled();
    const projected = await reconcileOriginalPending(owner, store, [sink(calls)], "O3", "historical-read-generation");
    expect(projected.uncertain).toBe(0);
    expect(projected.settled.map(row => [row.toolCallId, row.outcome])).toEqual([
      [historical.tool.originalToolCallId, "not-committed"], ["historical-alias", "not-committed"],
    ]);
    expect(diskSnapshot(join(owner.sessionRoot, "effects", historical.key))).toEqual(historicalFiles);
    expect(calls).not.toHaveBeenCalled();
  });

  it("rejects legacy multi-original ambiguity before ATTEMPT, sink, outcome or native projection writes", async () => {
    const { owner, store } = provision();
    const first = intent(owner, input("O1", [slot(owner, "first", 1)]));
    const second = intent(owner, input("O2", [slot(owner, "second", 2)]));
    const historicalInput = input("O3", [slot(owner, "historical", 0)]);
    const historical = intent(owner, historicalInput);
    await bind(store, [historical, first, second]);
    await terminal(owner, store, historical);
    const before = diskSnapshot(owner.sessionRoot);
    const calls = vi.fn<EffectSink["reconcile"]>(async record => ({ outcome: "committed", result: adapterOriginal(record) }));
    await expect(reconcilePending(owner, store, [sink(calls)], g1)).rejects.toBeInstanceOf(EffectContractError);
    await expect(createEffectReconciler({ owner, store, sinks: [sink(calls)] }).settlePendingIntents(owner.logicalSessionId, g1))
      .rejects.toMatchObject({ code: "EFFECT_ORIGINAL_SCOPE_REQUIRED" });
    const native = vi.fn<NonNullable<CertifiedEffectRecordOptions["readNativeToolResult"]>>(async () => {
      throw new Error("probe:native-Pi: ambiguity must refuse before projecting even a terminal record");
    });
    const port = recordPort(owner, store, historicalInput, native);
    await expect(port.reconcilePending(g1)).rejects.toMatchObject({ code: "EFFECT_ORIGINAL_SCOPE_REQUIRED" });
    expect(calls).not.toHaveBeenCalled();
    expect(native).not.toHaveBeenCalled();
    expect(diskSnapshot(owner.sessionRoot)).toEqual(before);
    expect((await ledger(store)).filter(row => row.intent.key !== historical.key).every(row => row.transitions.length === 0)).toBe(true);
  });

  it("keeps empty, terminal-only and single-pending-original legacy behavior without generation-dependent terminal writes", async () => {
    const { owner, store } = provision();
    await ledger(store); // Provision/validate the existing empty retained namespace.
    const beforeEmpty = diskSnapshot(owner.sessionRoot);
    const calls = vi.fn<EffectSink["reconcile"]>(async record => ({ outcome: "committed", result: adapterOriginal(record) }));
    const recovery = createEffectReconciler({ owner, store, sinks: [sink(calls)] });
    expect(await recovery.reconcilePending(g1)).toEqual({ settled: [], repairs: [], unresolved: [], ledgerFrontier: [], uncertain: 0 });
    expect(await recovery.reconcileOriginalPending("absent-original", g2)).toEqual({ settled: [], repairs: [], unresolved: [], ledgerFrontier: [], uncertain: 0 });
    expect(await recovery.settleOriginalPendingIntents(owner.logicalSessionId, "absent-original", g2)).toEqual({ settled: [], uncertain: 0 });
    expect(diskSnapshot(owner.sessionRoot)).toEqual(beforeEmpty);
    const historical = intent(owner, input("O1", [slot(owner, "historical", 0)]));
    const pending = intent(owner, input("O2", [slot(owner, "pending", 1)]));
    await bind(store, [historical, pending]);
    await terminal(owner, store, historical, ["retained-alias"]);
    const beforeHistorical = diskSnapshot(join(owner.sessionRoot, "effects", historical.key));
    const result = await recovery.reconcilePending(g2);
    expect(result.uncertain).toBe(0);
    expect(result.settled.map(row => row.toolCallId)).toEqual([historical.tool.originalToolCallId, "retained-alias", pending.tool.originalToolCallId]);
    expect(calls).toHaveBeenCalledTimes(1);
    expect(diskSnapshot(join(owner.sessionRoot, "effects", historical.key))).toEqual(beforeHistorical);
    const beforeTerminal = diskSnapshot(owner.sessionRoot);
    const projected = await recovery.settlePendingIntents(owner.logicalSessionId, "not-written-on-terminal-results");
    expect(projected).toEqual({ settled: result.settled, uncertain: 0 });
    expect(calls).toHaveBeenCalledTimes(1);
    expect(diskSnapshot(owner.sessionRoot)).toEqual(beforeTerminal);
    const pendingRecord = await store.withSession("settle", tx => tx.read(pending.key));
    expect(pendingRecord.transitions.map(row => row.recoveryGeneration)).toEqual([g2, g2]);
  });

  it("uses the explicitly bound scoped API, refuses wrong sessions and selectors, and never falls back to an ambient store", async () => {
    const { owner, store } = provision();
    const record = intent(owner, input("O2", [slot(owner, "one")]));
    await bind(store, [record]);
    const effects = sink(async row => ({ outcome: "committed", result: adapterOriginal(row) }));
    const recovery = createEffectReconciler({ owner, store, sinks: [effects] });
    const before = diskSnapshot(owner.sessionRoot);
    await expect(recovery.settleOriginalPendingIntents("wrong-session", "O2", g2)).rejects.toMatchObject({ code: "EFFECT_SESSION_MISMATCH" });
    await expect(recovery.reconcileOriginalPending("", g2)).rejects.toMatchObject({ code: "EFFECT_ORIGINAL_INPUT" });
    await expect(recovery.reconcileOriginalPending("O2", "")).rejects.toMatchObject({ code: "EFFECT_RECOVERY_GENERATION" });
    await expect(settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2)).rejects.toMatchObject({ code: "EFFECT_SESSION_UNBOUND" });
    expect(diskSnapshot(owner.sessionRoot)).toEqual(before);
    const unbind = bindEffectReconciler({ owner, store, sinks: [effects] });
    try {
      expect(await settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2)).toMatchObject({ uncertain: 0,
        settled: [{ key: record.key, toolCallId: record.tool.originalToolCallId, outcome: "committed" }] });
    } finally { unbind(); }
    await expect(settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2)).rejects.toMatchObject({ code: "EFFECT_SESSION_UNBOUND" });
  });

  it("probe:native-Pi: reuses outside-lock projection for each scoped physical alias with exact native JSON bytes and explicit postHook", async () => {
    const { owner, store } = provision();
    const accepted = input("O2", [slot(owner, "native")]);
    const selected = intent(owner, accepted);
    const foreign = intent(owner, input("O1", [slot(owner, "foreign")]));
    await bind(store, [selected, foreign]);
    await terminal(owner, store, selected, ["second-physical-alias"]);
    await terminal(owner, store, foreign);
    const foreignBefore = diskSnapshot(join(owner.sessionRoot, "effects", foreign.key));
    const messages = new Map<string, EffectToolResult>([
      [selected.tool.originalToolCallId, { timestamp: 123_456, details: { z: "雪", a: true }, role: "toolResult",
        toolCallId: selected.tool.originalToolCallId, toolName: "write", content: [{ type: "text", text: "post-hook original" }], isError: false }],
      ["second-physical-alias", { content: [{ type: "text", text: "post-hook alias" }], isError: true,
        toolCallId: "second-physical-alias", toolName: "write", timestamp: 234_567, role: "toolResult", details: { z: 2, a: 1 } }],
    ]);
    const readNativeToolResult = vi.fn<NonNullable<CertifiedEffectRecordOptions["readNativeToolResult"]>>(async effect => {
      // Re-enter the SAME lock: this cannot finish if projection is inside it.
      const saved = await store.withSession("settle", tx => tx.read(effect.key));
      expect(saved.intent.acceptedInput.id).toBe("O2");
      const message = messages.get(effect.toolCallId);
      if (!message) throw new Error("probe:native-Pi: unexpected physical alias");
      return message;
    });
    const port = recordPort(owner, store, accepted, readNativeToolResult);
    const result = await bounded(port.reconcileOriginalPending("O2", g2));
    expect(result.uncertain).toBe(0);
    expect(result.settled).toHaveLength(2);
    const adapterHash = sha256Bytes(canonicalEncode(adapterOriginal(selected)));
    for (const effect of result.settled) {
      const message = messages.get(effect.toolCallId)!;
      expect(effect.resultBytesHash).toBe(sha256Bytes(JSON.stringify(message)));
      expect(effect.resultBytesHash).not.toBe(adapterHash);
      expect(effect.resultBytesHash).not.toBe(sha256Bytes(canonicalEncode(message)));
      const read = await bounded(port.readToolResult(effect));
      expect(read).toEqual({ source: "native-entry", postHook: true, message });
      expect(JSON.stringify(read.message)).toBe(JSON.stringify(message));
    }
    expect(new Set(result.settled.map(row => row.resultBytesHash)).size).toBe(2);
    expect(result.repairs.map(row => row.resultBytesHash)).toEqual([adapterHash, adapterHash]);
    expect(result.ledgerFrontier.map(row => row.key)).toEqual([selected.key]);
    expect(diskSnapshot(join(owner.sessionRoot, "effects", foreign.key))).toEqual(foreignBefore);
    await expect(port.readToolResult({ ...result.settled[0]!, resultBytesHash: adapterHash })).rejects.toMatchObject({ code: "EFFECT_RESULT_UNAVAILABLE" });
  });

  it("preserves scoped adapter-original identity/hash and absence of timestamp when no native join exists", async () => {
    const { owner, store } = provision();
    const accepted = input("O2", [slot(owner, "adapter")]);
    const selected = intent(owner, accepted);
    await bind(store, [selected]);
    await terminal(owner, store, selected, ["adapter-alias"]);
    const port = recordPort(owner, store, accepted);
    const result = await port.settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2);
    for (const effect of result.settled) {
      expect(effect.resultBytesHash).toBe(sha256Bytes(canonicalEncode(adapterOriginal(selected))));
      const original = await port.readToolResult(effect);
      expect(original).toEqual({ source: "adapter-original", postHook: false, message: adapterOriginal(selected) });
      expect(Object.hasOwn(original.message, "timestamp")).toBe(false);
      expect(original.message.toolCallId).toBe(selected.tool.originalToolCallId);
    }
  });

  it("probe:native-Pi: propagates scoped reader errors and does not let O2 success clear O1's new-intent failure bookkeeping", async () => {
    const { owner, store } = provision();
    const first = intent(owner, input("O1", [slot(owner, "first")]));
    const existingSlot = slot(owner, "second");
    const newSlot = slot(owner, "new-file", 1, [], "file-write");
    const accepted = input("O2", [existingSlot, newSlot]);
    const second = intent(owner, accepted, existingSlot);
    await bind(store, [first, second]);
    await terminal(owner, store, first);
    await terminal(owner, store, second);
    const readerFailure = new Error("probe:native-Pi: committed native reader unavailable");
    let failFirst = true;
    const native = vi.fn<NonNullable<CertifiedEffectRecordOptions["readNativeToolResult"]>>(async effect => {
      await store.withSession("settle", tx => tx.read(effect.key));
      if (effect.key === first.key && failFirst) throw readerFailure;
      return undefined;
    });
    const port = recordPort(owner, store, accepted, native);
    const before = diskSnapshot(owner.sessionRoot);
    await expect(bounded(port.settleOriginalPendingIntents(owner.logicalSessionId, "O1", g1))).rejects.toBe(readerFailure);
    expect(await port.settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2)).toMatchObject({ uncertain: 0 });
    const write = newWrite(port, owner, newSlot);
    await expect(write.run()).rejects.toMatchObject({ code: "EFFECT_SETTLEMENT_REQUIRED" });
    expect(write.prepare).not.toHaveBeenCalled();
    expect(diskSnapshot(owner.sessionRoot)).toEqual(before);
    failFirst = false;
    expect(await port.settleOriginalPendingIntents(owner.logicalSessionId, "O1", g1)).toMatchObject({ uncertain: 0 });
    const written = await write.run();
    expect(written.isError).not.toBe(true);
    expect(write.prepare).toHaveBeenCalledTimes(1);
    expect(readFileSync(newSlot.canonicalTarget, "utf8")).toBe("immutable:new-file");
  });

  it("probe:native-Pi: a newer scoped success cannot clear an in-flight reader or its later failure", async () => {
    const { owner, store } = provision();
    const existingSlot = slot(owner, "historical");
    const newSlot = slot(owner, "after-reader", 1, [], "file-write");
    const accepted = input("O2", [existingSlot, newSlot]);
    const selected = intent(owner, accepted, existingSlot);
    await bind(store, [selected]);
    await terminal(owner, store, selected);
    let entered!: () => void;
    let release!: () => void;
    const readerEntered = new Promise<void>(resolve => { entered = resolve; });
    const readerReleased = new Promise<void>(resolve => { release = resolve; });
    const readerFailure = new Error("probe:native-Pi: older projection failed after the newer projection finished");
    let reads = 0;
    const port = recordPort(owner, store, accepted, async effect => {
      await store.withSession("settle", tx => tx.read(effect.key));
      reads += 1;
      if (reads === 1) {
        entered();
        await readerReleased;
        throw readerFailure;
      }
      return undefined;
    });
    const first = port.settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2);
    const firstRejected = expect(first).rejects.toBe(readerFailure);
    const write = newWrite(port, owner, newSlot);
    try {
      await bounded(readerEntered);
      expect(await bounded(port.settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2))).toMatchObject({ uncertain: 0 });
      await expect(write.run()).rejects.toMatchObject({ code: "EFFECT_SETTLEMENT_REQUIRED" });
      expect(write.prepare).not.toHaveBeenCalled();
    } finally {
      release();
      await bounded(firstRejected);
    }
    await expect(write.run()).rejects.toMatchObject({ code: "EFFECT_SETTLEMENT_REQUIRED" });
    expect(await port.settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2)).toMatchObject({ uncertain: 0 });
    await write.run();
    expect(readFileSync(newSlot.canonicalTarget, "utf8")).toBe("immutable:after-reader");
  });

  it("never turns scoped success into permission for a new effect to obscure another original's pending destination", async () => {
    const { owner, store } = provision();
    const newSlot = slot(owner, "shared-destination", 1, [], "file-write");
    const pendingSlot = { ...slot(owner, "pending"), canonicalTarget: newSlot.canonicalTarget };
    const first = intent(owner, input("O1", [pendingSlot]));
    const existingSlot = slot(owner, "historical");
    const accepted = input("O2", [existingSlot, newSlot]);
    const second = intent(owner, accepted, existingSlot);
    await bind(store, [first, second]);
    await terminal(owner, store, second);
    const before = diskSnapshot(owner.sessionRoot);
    const port = recordPort(owner, store, accepted);
    expect(await port.settleOriginalPendingIntents(owner.logicalSessionId, "O2", g2)).toMatchObject({ uncertain: 0 });
    const write = newWrite(port, owner, newSlot);
    await expect(write.run()).rejects.toMatchObject({ code: "EFFECT_PENDING_INTENT" });
    expect(write.prepare).not.toHaveBeenCalled();
    expect(diskSnapshot(owner.sessionRoot)).toEqual(before);
    expect((await store.withSession("settle", tx => tx.read(first.key))).transitions).toEqual([]);
  });
});
