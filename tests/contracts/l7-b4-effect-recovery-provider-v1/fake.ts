import type { EffectIntent, EffectOutcome } from "../../../src/effects/contracts.js";
import { canonicalEncode, intentBindingDigest } from "../../../src/effects/slots.js";
import type { AcceptedRecoveryInput, B4EffectRecoveryProviderV1, RepairRef, RepairReceiptView, SettledRecoveryEffect } from "./contract.js";
import { accepted, adapterOriginalResult, canonicalCopy, copy, createSinkGate, fail, GENERATION, hash, immutable, intent, joinedBranch, joinedEvidence, originalResult, SESSION,
  type ConformanceHarness, type Scenario } from "./fixtures.js";

export interface FakeOptions {
  readonly sessionId?: string;
  readonly acceptedInput?: Readonly<AcceptedRecoveryInput>;
  readonly executionBranchEntryId?: string;
  readonly joinedLeafEntryId?: string;
  /** Receipt evidence supplied by the consumer's B5 fixture; never checked by B4 settlement. */
  readonly pin?: string;
}
export interface FakeHarness extends Omit<ConformanceHarness, "provider"> {
  readonly provider: B4EffectRecoveryProviderV1;
}

/** Producer-owned TEST ONLY. In-memory behavior model, never a durability,
 * native receipt, B1 witness or peer sink implementation. No production exports. */
export async function createFake(scenario: Scenario = {}, options: FakeOptions = {}): Promise<FakeHarness> {
  const sessionId = options.sessionId ?? options.acceptedInput?.sessionId ?? SESSION;
  const acceptedInput = immutable(copy(options.acceptedInput ?? { ...accepted, sessionId }));
  if (acceptedInput.sessionId !== sessionId) throw new Error("Fake session and accepted input must agree");
  const identity = { acceptedInput, executionBranchEntryId: options.executionBranchEntryId ?? "A" };
  const sinkGate = createSinkGate(scenario.stallSink);
  const resultFor = scenario.resultSource === "adapter-original" ? adapterOriginalResult : originalResult;
  const intents = new Map<string, EffectIntent>();
  const outcomes = new Map<string, EffectOutcome>();
  const destination = new Set(scenario.committedBeforeAck ?? []);
  const observed: EffectIntent[] = [];
  const generations: string[] = [];
  // Receipt fixture input only. Pin allocation/validation belongs to B5/Main,
  // not this provider; no hidden pin check before settlement or repair.
  const receiptGeneration = options.pin ?? GENERATION;
  let fileBytes = scenario.fileState === "post" ? "after" : scenario.fileState === "neither" || scenario.shellState === "changed" ? "foreign" : "before";
  let fileIdentity = "original-inode";
  let joinedLeaf = options.joinedLeafEntryId ?? "J";
  let lostAck = scenario.lostAck === true;
  let receipt: Readonly<RepairReceiptView> | undefined;
  let ref: RepairRef | undefined;
  let tail: Promise<unknown> = Promise.resolve();
  const issue = async (value: EffectIntent): Promise<void> => {
    const previous = intents.get(value.key);
    if (previous && intentBindingDigest(previous) !== intentBindingDigest(value)) fail("EFFECT_SLOT_CONFLICT");
    intents.set(value.key, immutable(canonicalCopy(value)));
  };
  for (const value of scenario.intents ?? []) await issue(value);
  if (scenario.fileState || scenario.shellState) await issue(intent("file", 0, [], false, identity));
  const rows = (): SettledRecoveryEffect[] => [...outcomes].flatMap(([key, outcome]) => {
    const value = intents.get(key)!;
    const ids = [value.tool.originalToolCallId,
      ...(scenario.alias?.key === key ? [scenario.alias.toolCallId] : [])];
    return ids.map(toolCallId => ({ key, toolCallId, outcome,
      ...(outcome === "committed" ? { resultBytesHash: hash(JSON.stringify({
        ...resultFor(value) as object, toolCallId: scenario.resultSource === "adapter-original" ? value.tool.originalToolCallId : toolCallId,
      })) } : {}) }));
  });
  const provider: B4EffectRecoveryProviderV1 = {
    settlePendingIntents(sessionId, generation) {
      const run = async () => {
        if (sessionId !== acceptedInput.sessionId) fail("EFFECT_SESSION_MISMATCH");
        if (typeof generation !== "string" || !generation.trim()) fail("EFFECT_RECOVERY_GENERATION");
        if (scenario.corrupt) return { settled: [], uncertain: 1 };
        if (!intents.size) return { settled: [], uncertain: 0 };
        const visiting = new Set<string>();
        const visit = async (value: EffectIntent): Promise<void> => {
          if (outcomes.has(value.key) && outcomes.get(value.key) !== "uncertain") return;
          if (visiting.has(value.key)) fail("EFFECT_LEDGER_CORRUPT");
          visiting.add(value.key);
          let outcome: EffectOutcome = scenario.outcomes?.[value.slot.id] ?? "committed";
          if (value.slot.id === "file" && (scenario.fileState || scenario.shellState)) {
            if (fileBytes === "foreign") outcome = "uncertain";
            else if (scenario.fileState && fileBytes === "before") { fileBytes = "after"; fileIdentity = "new-inode"; }
          }
          for (const predecessor of value.slot.predecessors) {
            const dependency = [...intents.values()].find(item => item.acceptedInput.id === value.acceptedInput.id && item.slot.id === predecessor);
            if (dependency) await visit(dependency);
            if (!dependency || outcomes.get(dependency.key) !== "committed") outcome = "uncertain";
          }
          if (outcome !== "uncertain") {
            generations.push(generation);
            observed.push(copy(value));
            await sinkGate.enter();
            if (outcome === "committed") destination.add(value.key);
            if (lostAck) { lostAck = false; outcome = "uncertain"; }
          }
          outcomes.set(value.key, outcome);
          visiting.delete(value.key);
        };
        for (const value of [...intents.values()].sort((a, b) => a.slot.ordinal - b.slot.ordinal || a.key.localeCompare(b.key))) await visit(value);
        return { settled: rows(), uncertain: [...outcomes.values()].filter(value => value === "uncertain").length };
      };
      const result = tail.then(run);
      tail = result.catch(() => {});
      return result;
    },
    async readToolResult(effect) {
      const value = intents.get(effect.key);
      if (!value || effect.outcome === "uncertain" || !effect.resultBytesHash) fail("EFFECT_RESULT_UNAVAILABLE");
      if (effect.toolCallId !== value.tool.originalToolCallId &&
        !(scenario.alias?.key === effect.key && scenario.alias.toolCallId === effect.toolCallId)) fail("EFFECT_CALL_ALIAS_MISMATCH");
      if (value.tool.parentToolCallId) fail("EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR");
      if (outcomes.get(effect.key) !== effect.outcome) fail("EFFECT_RESULT_OUTCOME_MISMATCH");
      const source = scenario.resultSource ?? "native-entry";
      const message = { ...resultFor(value) as object, toolCallId: source === "adapter-original" ? value.tool.originalToolCallId : effect.toolCallId };
      if (hash(JSON.stringify(message)) !== effect.resultBytesHash) fail("EFFECT_RESULT_UNAVAILABLE");
      return { message: message as Awaited<ReturnType<B4EffectRecoveryProviderV1["readToolResult"]>>["message"], source, postHook: source === "native-entry" };
    },
    async repairMaintenance(input, executionLeafId) {
      if (canonicalEncode(input) !== canonicalEncode(acceptedInput)) fail("RECOVERY_CONFLICT");
      if (executionLeafId !== joinedLeaf || [...intents.keys()].some(key => !outcomes.has(key) || outcomes.get(key) === "uncertain")) fail("RECOVERY_MAINTENANCE_REPAIR_REQUIRED");
      if (receipt) {
        if (receipt.joinedLeafEntryId !== joinedLeaf) fail("RECOVERY_MAINTENANCE_REPAIR_REQUIRED");
        return ref!;
      }
      receipt = immutable(joinedEvidence(rows(), joinedLeaf, identity, receiptGeneration));
      ref = immutable({ originalInputId: acceptedInput.originalInputId, recoveryGeneration: receiptGeneration, executionBranchEntryId: identity.executionBranchEntryId,
        repairReceiptSha256: hash(canonicalEncode(receipt)) });
      return ref;
    },
    resolveRepair(request) {
      if (!receipt || !ref || canonicalEncode(ref) !== canonicalEncode(request)) fail("RECOVERY_MAINTENANCE_REPAIR_REQUIRED");
      return receipt;
    },
  };
  return { provider, issue, async reopen() { /* Same retained model state; NOT a process-crash proof. */ },
    destination: () => [...destination], observedIntents: () => copy(observed), generations: () => [...generations],
    async ledgerDigest() { return hash(canonicalEncode({ intents: [...intents.values()], outcomes: [...outcomes], receipt: receipt ?? null })); },
    joinedBranch: () => copy(joinedBranch(joinedLeaf, identity)), sinkGate,
    advanceJoinedLeaf(nextLeafId = `${joinedLeaf}2`) { joinedLeaf = nextLeafId; },
    fileSnapshot() { return { bytes: fileBytes, identity: fileIdentity }; }, async dispose() { sinkGate.release(); } };
}

/** Full-provider accessor for consumers that do not need retained-state controls.
 * No Partial cast; all four methods are available in the exported return type. */
export async function createFakeProvider(scenario: Scenario = {}, options: FakeOptions = {}): Promise<B4EffectRecoveryProviderV1> {
  return (await createFake(scenario, options)).provider;
}
