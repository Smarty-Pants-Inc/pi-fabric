import type {
  EffectIntent, EffectRecord, EffectRepair, EffectResult, EffectSink, EffectSinkResult, EffectTransition,
  EffectToolResult, ReadToolResult, ReconcilePendingResult, RecoveryOwner, SettledEffect, SettlePendingIntents, SettlePendingIntentsResult,
} from "./contracts.js";
import { EffectContractError } from "./contracts.js";
import { canonicalEncode, sha256Bytes } from "./slots.js";
import { EffectStore } from "./store.js";

export interface EffectReconcilerOptions {
  readonly owner: RecoveryOwner;
  readonly store: EffectStore;
  readonly sinks: readonly EffectSink[];
}

const resultTransition = (record: EffectRecord): EffectTransition | undefined =>
  record.transitions.find(transition => transition.state === "RESULT");
const explain = (error: unknown): string => error instanceof Error ? error.message : String(error);
const sinkIdentity = (kind: string, version: string): string => canonicalEncode([kind, version]);
const checkGeneration = (generation: string): void => {
  if (typeof generation !== "string" || generation.trim().length === 0) {
    throw new EffectContractError("EFFECT_RECOVERY_GENERATION", "Recovery generation must be a nonempty string");
  }
  canonicalEncode(generation);
};

/** No input admission, prompt submission, JS replay, or independent startup hook.
 * The session-wide store lock also prevents a later path mutation obscuring a
 * pending intent while its sink is queried. B3 owns cross-process exclusion. */
export const reconcilePending = async (
  owner: RecoveryOwner,
  store: EffectStore,
  sinks: readonly EffectSink[],
  recoveryGeneration: string,
): Promise<ReconcilePendingResult> => {
  checkGeneration(recoveryGeneration);
  if (store.owner !== owner) {
    throw new EffectContractError("EFFECT_OWNER_MISMATCH", "Reconciliation must use the store's bound recovery capability");
  }
  const sinkMap = new Map<string, EffectSink>();
  for (const sink of sinks) {
    const id = sinkIdentity(sink.kind, sink.adapterVersion);
    if (sinkMap.has(id)) throw new EffectContractError("EFFECT_SINK_AMBIGUOUS", `Duplicate sink ${id}`);
    sinkMap.set(id, sink);
  }
  // Capability failures remain refusals; they must not masquerade as an empty ledger.
  await owner.assertLive("settle");
  return store.withSession("settle", async transaction => {
    const settled: SettlePendingIntentsResult["settled"] = [];
    const repairs: EffectRepair[] = [];
    const unresolved: Array<{ key: string; reason: string }> = [];
    const ledgerFrontier: Array<{ key: string; sequence: number; digest: string }> = [];
    let records: readonly EffectRecord[];
    try {
      records = transaction.list();
    } catch (error) {
      // Do not skip corrupt directories and then report that no work is pending.
      return { settled, repairs, ledgerFrontier, unresolved: [{ key: "ledger", reason: explain(error) }], uncertain: 1 };
    }
    const byKey = new Map(records.map(record => [record.intent.key, record]));
    const bySlot = new Map(records.map(record => [
      canonicalEncode([record.intent.acceptedInput.id, record.intent.slot.id]), record.intent.key,
    ]));
    const outcomes = new Map<string, EffectSinkResult>();
    const visiting = new Set<string>();

    const project = (record: EffectRecord, outcome: EffectSinkResult): void => {
      const intent = record.intent;
      const aliases = new Map<string, string | undefined>([[intent.tool.originalToolCallId, intent.tool.parentToolCallId]]);
      for (const transition of record.transitions) {
        if (!aliases.has(transition.toolCallId)) aliases.set(transition.toolCallId, transition.parentToolCallId);
      }
      for (const [toolCallId, parentToolCallId] of aliases) {
        settled.push({ toolCallId, key: intent.key, outcome: outcome.outcome,
          ...(outcome.resultBytesHash !== undefined ? { resultBytesHash: outcome.resultBytesHash } : {}) });
        // A nested physical alias is a join reference, NOT an orphan native result.
        // B3 groups these by parent and labels an interrupted parent truthfully;
        // we never manufacture the unevaluated fabric_exec return value.
        repairs.push({ key: intent.key, toolCallId,
          ...(parentToolCallId !== undefined ? { parentToolCallId } : {}),
          argumentsDigest: sha256Bytes(canonicalEncode(intent.canonicalArguments)),
          outcome: outcome.outcome,
          ...(outcome.result !== undefined ? { result: outcome.result } : {}),
          ...(outcome.resultBytesHash !== undefined ? { resultBytesHash: outcome.resultBytesHash } : {}) });
      }
      const tail = record.transitions[record.transitions.length - 1];
      ledgerFrontier.push({ key: intent.key, sequence: tail?.sequence ?? 0, digest: tail?.digest ?? record.intentDigest });
      if (outcome.outcome === "uncertain") unresolved.push({ key: intent.key, reason: outcome.reason ?? "Sink state is uncertain" });
      outcomes.set(intent.key, outcome);
    };

    const appendOutcome = (intent: EffectIntent, outcome: EffectSinkResult): EffectRecord => transaction.append(intent.key, {
      state: outcome.outcome === "uncertain" ? "UNCERTAIN" : "RESULT",
      toolCallId: intent.tool.originalToolCallId,
      ...(intent.tool.parentToolCallId !== undefined ? { parentToolCallId: intent.tool.parentToolCallId } : {}),
      attempt: owner.attempt,
      at: new Date().toISOString(),
      recoveryGeneration,
      outcome: outcome.outcome,
      ...(outcome.result !== undefined ? { result: outcome.result } : {}),
      ...(outcome.resultBytesHash !== undefined ? { resultBytesHash: outcome.resultBytesHash } : {}),
      ...(outcome.receipt !== undefined ? { receipt: outcome.receipt } : {}),
      ...(outcome.reason !== undefined ? { reason: outcome.reason } : {}),
    });

    const visit = async (record: EffectRecord): Promise<void> => {
      const { intent } = record;
      if (outcomes.has(intent.key)) return;
      if (visiting.has(intent.key)) throw new EffectContractError("EFFECT_LEDGER_CORRUPT", "Cyclic stored effect predecessors");
      visiting.add(intent.key);
      const previous = resultTransition(record);
      if (previous) {
        project(record, { outcome: previous.outcome!,
          ...(previous.result !== undefined ? { result: previous.result } : {}),
          ...(previous.resultBytesHash !== undefined ? { resultBytesHash: previous.resultBytesHash } : {}),
          ...(previous.receipt !== undefined ? { receipt: previous.receipt } : {}) });
        visiting.delete(intent.key);
        return;
      }
      let outcome: EffectSinkResult | undefined;
      for (const predecessor of intent.slot.predecessors) {
        const key = bySlot.get(canonicalEncode([intent.acceptedInput.id, predecessor]));
        const dependency = key === undefined ? undefined : byKey.get(key);
        if (dependency) await visit(dependency);
        if (key === undefined || outcomes.get(key)?.outcome !== "committed") {
          outcome = { outcome: "uncertain", reason: `Predecessor ${predecessor} has no committed result` };
          break;
        }
      }
      const sink = sinkMap.get(sinkIdentity(intent.slot.kind, intent.tool.adapterVersion));
      if (!outcome && !sink) outcome = { outcome: "uncertain", reason: `Unsupported effect adapter ${intent.slot.kind}@${intent.tool.adapterVersion}` };
      if (!outcome && sink) {
        // Persist attempt evidence BEFORE any possible sink action. Sink errors are
        // not proof of absence, so retain the intent and classify as uncertainty.
        await owner.assertLive("settle");
        transaction.append(intent.key, {
          state: "ATTEMPT", toolCallId: intent.tool.originalToolCallId,
          ...(intent.tool.parentToolCallId !== undefined ? { parentToolCallId: intent.tool.parentToolCallId } : {}),
          attempt: owner.attempt, at: new Date().toISOString(), recoveryGeneration,
        });
        try {
          outcome = await sink.reconcile(intent, { owner, recoveryGeneration });
          if (!["committed", "not-committed", "uncertain"].includes(outcome.outcome)) {
            throw new EffectContractError("EFFECT_SINK_RESULT", "Sink returned an invalid outcome");
          }
          if (outcome.outcome === "committed" && outcome.result === undefined) {
            throw new EffectContractError("EFFECT_SINK_RESULT", "Committed sink outcome has no original result");
          }
          if (outcome.result !== undefined) {
            const resultBytesHash = sha256Bytes(canonicalEncode(outcome.result));
            if (outcome.resultBytesHash !== undefined && resultBytesHash !== outcome.resultBytesHash) {
              throw new EffectContractError("EFFECT_RESULT_HASH", "Sink result bytes do not match their hash");
            }
            outcome = { ...outcome, resultBytesHash };
          } else if (outcome.resultBytesHash !== undefined) {
            throw new EffectContractError("EFFECT_RESULT_HASH", "Sink result hash has no corresponding result bytes");
          }
          // Snapshot/check the COMPLETE response before a durable RESULT append.
          canonicalEncode(outcome);
        } catch (error) {
          outcome = { outcome: "uncertain", reason: explain(error) };
        }
      }
      try {
        const updated = appendOutcome(intent, outcome!);
        project(updated, outcome!);
      } catch (error) {
        // A failed result barrier can follow a real destination commit. Never
        // acknowledge it or redo here. The next owner must reread/reconcile it.
        project(record, { outcome: "uncertain", reason: `Outcome durability failed: ${explain(error)}` });
        throw error;
      }
      visiting.delete(intent.key);
    };

    for (const record of [...records].sort((a, b) => a.intent.slot.ordinal - b.intent.slot.ordinal || a.intent.key.localeCompare(b.intent.key))) {
      try {
        await visit(record);
      } catch (error) {
        if (!outcomes.has(record.intent.key)) project(record, { outcome: "uncertain", reason: explain(error) });
        // A capability/store failure closes the rest of this settlement pass.
        for (const pending of records) {
          if (!outcomes.has(pending.intent.key)) project(pending, { outcome: "uncertain", reason: "Settlement stopped after a ledger/authority failure" });
        }
        break;
      }
    }
    return { settled, repairs, unresolved, ledgerFrontier, uncertain: unresolved.length };
  });
};

/** Read only an acknowledged original result under a verified physical alias.
 * Parent IDs are join references, never aliases authorizing a fabricated parent
 * return. In particular this cannot reconstruct an unevaluated JS final value. */
const readResultRecord = async (
  owner: RecoveryOwner, store: EffectStore, key: string, toolCallId: string, resultBytesHash: string,
) => {
  if (store.owner !== owner) throw new EffectContractError("EFFECT_OWNER_MISMATCH", "Result lookup must use the bound recovery capability");
  return store.withSession("settle", transaction => {
    const record = transaction.read(key);
    if (record.intent.tool.originalToolCallId !== toolCallId && !record.transitions.some(row => row.toolCallId === toolCallId)) {
      throw new EffectContractError("EFFECT_CALL_ALIAS_MISMATCH", "Tool call is not a recorded physical alias for this effect");
    }
    const result = resultTransition(record);
    if (!result || result.result === undefined || result.resultBytesHash !== resultBytesHash || sha256Bytes(canonicalEncode(result.result)) !== resultBytesHash) {
      throw new EffectContractError("EFFECT_RESULT_UNAVAILABLE", "No acknowledged original result matches this call/hash");
    }
    return { record, result };
  });
};
export const readEffectResult = async (
  owner: RecoveryOwner, store: EffectStore, key: string, toolCallId: string, resultBytesHash: string,
): Promise<EffectResult> => (await readResultRecord(owner, store, key, toolCallId, resultBytesHash)).result.result!;

/** B3's readToolResult(effect) is bound to one session runtime. Return only a
 * complete original native message; do not invent IDs, timestamps, parent
 * fabric_exec returns, or error flags from an adapter's content-only result. */
export const readToolResult = async (
  owner: RecoveryOwner, store: EffectStore, effect: SettledEffect,
): Promise<EffectToolResult> => {
  if (effect.outcome === "uncertain" || effect.resultBytesHash === undefined) {
    throw new EffectContractError("EFFECT_RESULT_UNAVAILABLE", "An uncertain or unhashed effect has no repairable original native result");
  }
  const { record, result } = await readResultRecord(owner, store, effect.key, effect.toolCallId, effect.resultBytesHash);
  if (result.outcome !== effect.outcome) throw new EffectContractError("EFFECT_RESULT_OUTCOME_MISMATCH", "Settlement descriptor differs from the original durable outcome");
  const parent = record.intent.tool.originalToolCallId === effect.toolCallId ? record.intent.tool.parentToolCallId :
    record.transitions.find(row => row.toolCallId === effect.toolCallId)?.parentToolCallId;
  if (parent !== undefined) throw new EffectContractError("EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR", "Nested results require labelled parent repair, not an orphan native tool result");
  const value = result.result!;
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new EffectContractError("EFFECT_NATIVE_RESULT_INVALID", "Stored result is not an original native tool result");
  }
  const message = value as Record<string, unknown>;
  if (message.role !== "toolResult" || message.toolCallId !== effect.toolCallId || typeof message.toolName !== "string" ||
    message.toolName.length === 0 || typeof message.isError !== "boolean" || typeof message.timestamp !== "number" ||
    !Number.isFinite(message.timestamp) || !Array.isArray(message.content) || !message.content.every(block => {
      if (block === null || typeof block !== "object" || Array.isArray(block)) return false;
      const item = block as Record<string, unknown>;
      return (item.type === "text" && typeof item.text === "string" && (item.textSignature === undefined || typeof item.textSignature === "string")) ||
        (item.type === "image" && typeof item.data === "string" && typeof item.mimeType === "string");
    })) {
    throw new EffectContractError("EFFECT_NATIVE_RESULT_INVALID", "Stored result lacks the exact original native call/content/error/timestamp fields");
  }
  return value as unknown as EffectToolResult;
};

/** Explicit session binding, not a second store discovered from cwd/home/temp. */
export const createEffectReconciler = (options: EffectReconcilerOptions) => {
  const run = (generation: string) => reconcilePending(options.owner, options.store, options.sinks, generation);
  const settle: SettlePendingIntents = async (sessionId, recoveryGeneration) => {
    if (sessionId !== options.owner.logicalSessionId) {
      throw new EffectContractError("EFFECT_SESSION_MISMATCH", "Cannot settle another logical session");
    }
    const { settled, uncertain } = await run(recoveryGeneration);
    return { settled, uncertain };
  };
  const read: ReadToolResult = effect => readToolResult(options.owner, options.store, effect);
  return { reconcilePending: run, settlePendingIntents: settle, readToolResult: read };
};

const bindings = new Map<string, ReturnType<typeof createEffectReconciler>>();
/** B3 calls once after provisioning and before admitting any model work. */
export const bindEffectReconciler = (options: EffectReconcilerOptions): (() => void) => {
  const id = options.owner.logicalSessionId;
  if (bindings.has(id)) throw new EffectContractError("EFFECT_SESSION_ALREADY_BOUND", `Effect session ${id} already has a runtime`);
  const runtime = createEffectReconciler(options);
  bindings.set(id, runtime);
  return () => { if (bindings.get(id) === runtime) bindings.delete(id); };
};

/** Exact two-argument B3 interface; never creates a fallback runtime/store. */
export const settlePendingIntents: SettlePendingIntents = async (sessionId, recoveryGeneration) => {
  const runtime = bindings.get(sessionId);
  if (!runtime) throw new EffectContractError("EFFECT_SESSION_UNBOUND", `Effect session ${sessionId} is not bound`);
  return runtime.settlePendingIntents(sessionId, recoveryGeneration);
};
