import type { EffectIntent, EffectOutcome } from "../../../src/effects/contracts.js";
import { canonicalEncode, intentBindingDigest } from "../../../src/effects/slots.js";
import type { B4EffectRecoveryProviderV1, RepairRef, RepairReceiptView, SettledRecoveryEffect } from "./contract.js";
import { accepted, canonicalCopy, copy, fail, GENERATION, hash, immutable, INPUT, intent, joinedEvidence, originalResult, SESSION,
  type ConformanceFactory } from "./fixtures.js";

/** Producer-owned TEST ONLY. In-memory behavior model, never a durability,
 * native receipt, B1 witness or peer sink implementation. No production exports. */
export const createFake: ConformanceFactory = async scenario => {
  const intents = new Map<string, EffectIntent>();
  const outcomes = new Map<string, EffectOutcome>();
  const destination = new Set(scenario.committedBeforeAck ?? []);
  const observed: EffectIntent[] = [];
  const generations: string[] = [];
  const pin = scenario.pin === undefined ? GENERATION : scenario.pin;
  let fileBytes = scenario.fileState === "post" ? "after" : scenario.fileState === "neither" || scenario.shellState === "changed" ? "foreign" : "before";
  let fileIdentity = "original-inode";
  let joinedLeaf = "J";
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
  if (scenario.fileState || scenario.shellState) await issue(intent("file"));
  const rows = (): SettledRecoveryEffect[] => [...outcomes].flatMap(([key, outcome]) => {
    const value = intents.get(key)!;
    const ids = [value.tool.originalToolCallId,
      ...(scenario.alias?.key === key ? [scenario.alias.toolCallId] : [])];
    return ids.map(toolCallId => ({ key, toolCallId, outcome,
      ...(outcome === "committed" ? { resultBytesHash: hash(JSON.stringify({
        ...originalResult(value) as object, toolCallId,
      })) } : {}) }));
  });
  const provider: B4EffectRecoveryProviderV1 = {
    settlePendingIntents(sessionId, generation) {
      const run = async () => {
        if (sessionId !== SESSION) fail("EFFECT_SESSION_MISMATCH");
        if (typeof generation !== "string" || !generation.trim()) fail("EFFECT_RECOVERY_GENERATION");
        if (intents.size && !pin) fail("RECOVERY_HISTORY_INCOMPLETE");
        if (intents.size && generation !== pin) fail("RECOVERY_CONFLICT");
        if (scenario.corrupt) return { settled: [], uncertain: 1 };
        if (!intents.size) return { settled: [], uncertain: 0 };
        const visiting = new Set<string>();
        const visit = (value: EffectIntent): void => {
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
            if (dependency) visit(dependency);
            if (!dependency || outcomes.get(dependency.key) !== "committed") outcome = "uncertain";
          }
          if (outcome !== "uncertain") {
            generations.push(generation);
            observed.push(copy(value));
            if (outcome === "committed") destination.add(value.key);
            if (lostAck) { lostAck = false; outcome = "uncertain"; }
          }
          outcomes.set(value.key, outcome);
          visiting.delete(value.key);
        };
        for (const value of [...intents.values()].sort((a, b) => a.slot.ordinal - b.slot.ordinal || a.key.localeCompare(b.key))) visit(value);
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
      const message = { ...originalResult(value) as object, toolCallId: effect.toolCallId };
      if (hash(JSON.stringify(message)) !== effect.resultBytesHash) fail("EFFECT_RESULT_UNAVAILABLE");
      return { message: message as Awaited<ReturnType<B4EffectRecoveryProviderV1["readToolResult"]>>["message"], source: "native-entry", postHook: true };
    },
    async repairMaintenance(input, executionLeafId) {
      if (input.sessionId !== SESSION || input.originalInputId !== INPUT || canonicalEncode(input) !== canonicalEncode(accepted)) fail("RECOVERY_CONFLICT");
      if (!pin) fail("RECOVERY_HISTORY_INCOMPLETE");
      if (executionLeafId !== joinedLeaf || [...intents.keys()].some(key => !outcomes.has(key) || outcomes.get(key) === "uncertain")) fail("RECOVERY_MAINTENANCE_REPAIR_REQUIRED");
      if (receipt) {
        if (receipt.joinedLeafEntryId !== joinedLeaf) fail("RECOVERY_MAINTENANCE_REPAIR_REQUIRED");
        return ref!;
      }
      receipt = immutable({ ...joinedEvidence(rows(), joinedLeaf), recoveryGeneration: pin });
      ref = immutable({ originalInputId: INPUT, recoveryGeneration: pin, executionBranchEntryId: "A",
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
    advanceJoinedLeaf() { joinedLeaf = "J2"; },
    fileSnapshot() { return { bytes: fileBytes, identity: fileIdentity }; }, async dispose() {} };
};
