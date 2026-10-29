import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { requireNamespaceBarrier, syncPathNamespaceDurable, writeJsonAtomic } from "../core/atomic-write.js";
import { copyFabricPrincipal, type FabricPrincipal } from "../host-compatibility.js";
import type { FabricAgentMessageDelivery, FabricAgentMessageResult, FabricMainAgentDeliveryRequest, FabricSteerPriority, MainAgentController } from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { MaintenanceLedgerEntry, RecoveryEpoch, RepairRef } from "./maintenance-admission.js";

/** Opt-in wire metadata. deadlineAt is the ORIGINAL absolute Unix-millisecond deadline. */
export interface ScriptedInput {
  readonly version: 1;
  readonly inputId: string;
  readonly sessionId: string;
  readonly deadlineAt: number;
}
export type InputAdmissionMainAgent = Pick<MainAgentController, "id" | "local" | "sessionId" | "deliverAgent">;
export interface PreparedScriptedInput {
  idempotencyKey: string;
  message: string;
  data: unknown;
  /** Immutable host-captured attribution; never recovered from data or other metadata. */
  readonly principal?: FabricPrincipal | undefined;
  deadlineAt: number;
  delivery: FabricAgentMessageDelivery;
  priority?: FabricSteerPriority;
  /** Materialized Main default: omitted input and explicit true are the same policy. */
  triggerTurn: boolean;
  /** Saved native admission only; duplicate receipt, not processing/effect completion. */
  result?: FabricAgentMessageResult;
}
export interface PendingScriptedInput extends PreparedScriptedInput {
  from: MeshIdentity;
  targetId: string;
  published: boolean;
}
export interface Binding {
  sessionId: string; root: string; mainAgent: InputAdmissionMainAgent;
  /** REQUIRED-only owner supplied after B2/native positive single-writer exclusion. */
  recovery?: RecoveryAdmissionOwner;
}
interface DeliveryPolicy {
  /** Required; no delivery default. triggerTurn alone defaults to true, as in native Main. */
  delivery: FabricAgentMessageDelivery;
  priority?: FabricSteerPriority;
  triggerTurn?: boolean;
}
interface Round extends DeliveryPolicy {
  from: MeshIdentity; targetId: string; message: string; data: unknown; triggerTurn: boolean;
  readonly principal?: FabricPrincipal | undefined;
}
interface RecordPayload {
  role: "sender" | "receiver";
  deliveryId: string;
  round: Round;
  published?: true;
  /** Native admission receipt, NOT task/effect completion or a broker ACK. */
  result?: FabricAgentMessageResult;
}
interface SenderKey { root: string; from: MeshIdentity; sessionId: string; inputId: string }

export class ScriptedInputAdmissionError extends Error {
  override readonly name = "ScriptedInputAdmissionError";
  constructor(readonly code: string, message: string) { super(message); }
}
const fail = (code: string, message: string): never => { throw new ScriptedInputAdmissionError(`SCRIPTED_INPUT_${code}`, message); };
const notDurable = (platform: NodeJS.Platform): Error =>
  new ScriptedInputAdmissionError("SCRIPTED_INPUT_NOT_DURABLE", `Scripted input namespace durability is unavailable on ${platform}`);
const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value) &&
  (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
const identifier = (value: unknown, name: string): string => nonempty(value) ? value : fail("INVALID", `${name} must be a nonempty string`);
const exactKeys = (value: Record<string, unknown>, keys: readonly string[]): boolean =>
  Reflect.ownKeys(value).every(key => typeof key === "string" && keys.includes(key));
const hash = (bytes: string): string => createHash("sha256").update(bytes).digest("hex");

/** Strict canonical JSON: never silently omit bytes, invoke toJSON/getters, or accept cycles. */
const canonical = (value: unknown): string => {
  const ancestors = new Set<object>();
  const visit = (item: unknown, depth: number): unknown => {
    if (item === null || typeof item === "string" || typeof item === "boolean") return item;
    if (typeof item === "number" && Number.isFinite(item)) return item;
    if (depth > 100 || (!Array.isArray(item) && !object(item)) || ancestors.has(item)) {
      return fail("INVALID", "Scripted input needs lossless, acyclic JSON data");
    }
    ancestors.add(item);
    try {
      if (Array.isArray(item)) {
        if (Reflect.ownKeys(item).length !== item.length + 1) return fail("INVALID", "Scripted input needs dense JSON arrays");
        return Array.from({ length: item.length }, (_, index) => {
          const descriptor = Object.getOwnPropertyDescriptor(item, String(index));
          if (!descriptor?.enumerable || !("value" in descriptor)) return fail("INVALID", "Scripted input needs plain JSON values");
          return visit(descriptor.value, depth + 1);
        });
      }
      const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      const keys = Reflect.ownKeys(item);
      if (keys.some(key => typeof key !== "string")) return fail("INVALID", "Scripted input cannot serialize JSON symbol keys");
      for (const key of (keys as string[]).sort()) {
        const descriptor = Object.getOwnPropertyDescriptor(item, key)!;
        if (!descriptor.enumerable || !("value" in descriptor)) return fail("INVALID", "Scripted input needs plain JSON values");
        result[key] = visit(descriptor.value, depth + 1);
      }
      return result;
    } finally { ancestors.delete(item); }
  };
  return JSON.stringify(visit(value, 0));
};
const snapshot = <T>(value: T): T => JSON.parse(canonical(value)) as T;

/** Ordinary messages are untouched; the presence of malformed opt-in metadata is a refusal. */
export function readScriptedInput(data: unknown): ScriptedInput | undefined {
  if (data === null || (typeof data !== "object" && typeof data !== "function") || !("scriptedInput" in data)) return undefined;
  const descriptor = Object.getOwnPropertyDescriptor(data, "scriptedInput");
  const metadata: unknown = descriptor && "value" in descriptor ? descriptor.value : undefined;
  if (!object(data) || !object(metadata) || !exactKeys(metadata, ["version", "inputId", "sessionId", "deadlineAt"]) ||
      Object.values(Object.getOwnPropertyDescriptors(metadata)).some(field => !field.enumerable || !("value" in field))) {
    return fail("INVALID", "Malformed data.scriptedInput metadata");
  }
  if (metadata.version !== 1 || !nonempty(metadata.inputId) || !nonempty(metadata.sessionId) ||
      typeof metadata.deadlineAt !== "number" || !Number.isSafeInteger(metadata.deadlineAt) || metadata.deadlineAt <= 0) {
    return fail("INVALID", "Malformed data.scriptedInput: version 1, inputId, sessionId and an absolute deadlineAt are required");
  }
  return Object.freeze({ version: 1, inputId: metadata.inputId, sessionId: metadata.sessionId, deadlineAt: metadata.deadlineAt });
}
const metadataOf = (round: Round): ScriptedInput => readScriptedInput(round.data) ?? fail("INVALID", "data.scriptedInput is required");
const sender = (from: MeshIdentity): MeshIdentity => {
  const copy = snapshot(from);
  if (!object(copy) || !nonempty(copy.id) || !nonempty(copy.name) || !["main", "agent", "actor"].includes(copy.kind) ||
      (copy.sessionId !== undefined && !nonempty(copy.sessionId)) || (copy.verified !== undefined && copy.verified !== "bridge")) {
    return fail("INVALID", "Scripted input needs a fully attributed sender id, name and kind");
  }
  return copy;
};
/** The native host field is optional, but an invalid supplied claim must never become UNKNOWN. */
const principalSnapshot = (value: unknown): FabricPrincipal | undefined => {
  if (value === undefined) return undefined;
  const copy = snapshot(value);
  if (!object(copy) || !exactKeys(copy, ["id", "binding"])) return fail("INVALID", "Scripted input needs a valid native principal id and binding");
  return copyFabricPrincipal(copy) ?? fail("INVALID", "Scripted input needs a valid native principal id and binding");
};
const makeRound = (options: { from: MeshIdentity; targetId: string; message: string; data: unknown; principal?: FabricPrincipal | undefined } & DeliveryPolicy): Round => {
  const principal = principalSnapshot(options.principal);
  const data = snapshot(options.data);
  const metadata = readScriptedInput(data) ?? fail("INVALID", "data.scriptedInput is required");
  const targetId = `session:${metadata.sessionId}`;
  if (options.targetId !== targetId && options.targetId !== metadata.sessionId) return fail("INVALID", "Scripted input recipient target must match scriptedInput.sessionId");
  if (!nonempty(options.message)) return fail("INVALID", "Scripted input message must not be empty");
  if (options.delivery !== "steer" && options.delivery !== "followUp") return fail("INVALID", "Scripted input delivery must be steer or followUp (no default)");
  if (options.priority !== undefined && (options.priority !== "interrupt" || options.delivery !== "steer" || options.triggerTurn === false)) {
    return fail("INVALID", "Scripted input priority requires a triggering interrupt steer");
  }
  if (options.triggerTurn !== undefined && typeof options.triggerTurn !== "boolean") return fail("INVALID", "Scripted input triggerTurn must be boolean");
  // Keep the EXACT offered text and delivery policy, not Main's presentation-time trim/defaults.
  return { from: sender(options.from), targetId, message: options.message, data, delivery: options.delivery,
    ...(principal === undefined ? {} : { principal }),
    ...(options.priority === undefined ? {} : { priority: options.priority }),
    triggerTurn: options.triggerTurn ?? true };
};
const deliveryIdFor = (fromId: string, sessionId: string, inputId: string): string =>
  `scripted-input:v1:${hash(canonical([1, fromId, sessionId, inputId]))}`;
const roundId = (round: Round): string => {
  const metadata = metadataOf(round);
  return deliveryIdFor(round.from.id, metadata.sessionId, metadata.inputId);
};
const base = (root: string): string => path.join(path.resolve(identifier(root, "root")), "input-admission", "v1");
const directory = (root: string, role: RecordPayload["role"], identity: string): string => path.join(base(root), role, hash(identity));
const recordPath = (root: string, role: RecordPayload["role"], round: Round): string =>
  path.join(directory(root, role, role === "sender" ? round.from.id : metadataOf(round).sessionId), `${roundId(round).split(":").at(-1)!}.json`);
const nativeResult = (value: unknown, deadlineAt: number): FabricAgentMessageResult => {
  if (!object(value) || value.queued !== true || value.routed !== "main" || !nonempty(value.messageId) ||
      (value.deadlineAt !== undefined && value.deadlineAt !== deadlineAt) ||
      ["acknowledged", "triggered"].some(key => value[key] !== undefined && typeof value[key] !== "boolean") ||
      ["duplicate", "halted", "coalesced", "stalled"].some(key => value[key] !== undefined && value[key] !== true) ||
      ["reason", "notice", "replacedMessageId"].some(key => value[key] !== undefined && typeof value[key] !== "string")) {
    return fail("INVALID", "Expected an authenticated native Main admission result, not a publish/broker ACK");
  }
  return { ...snapshot(value), deadlineAt } as unknown as FabricAgentMessageResult;
};

// All entrypoints assume the existing session/root single-writer fence. This module adds
// no ownership protocol, scheduler, transport, post-append barrier or effect-completion ledger.
const save = (file: string, payload: RecordPayload): void => {
  requireNamespaceBarrier(notDurable);
  writeJsonAtomic(file, { version: 1, payload, sha256: hash(canonical(payload)) }, { durable: true });
};
const load = (file: string, role: RecordPayload["role"]): RecordPayload | undefined => {
  requireNamespaceBarrier(notDurable);
  let fd: number;
  try { fd = fs.openSync(file, "r"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  let value: unknown;
  try {
    const stat = fs.fstatSync(fd);
    const bytes = fs.readFileSync(fd, "utf8");
    // A rename can be visible after a failed directory fsync. Every read owes fresh
    // file + full namespace barriers, even if this is a duplicate or saved result.
    fs.fsyncSync(fd);
    syncPathNamespaceDurable(file, stat, notDurable);
    try { value = JSON.parse(bytes); } catch { return fail("CORRUPT", `Corrupt scripted input JSON: ${file}`); }
  } finally { fs.closeSync(fd); }
  try {
    if (!object(value) || value.version !== 1 || !exactKeys(value, ["version", "payload", "sha256"]) ||
        typeof value.sha256 !== "string" || !object(value.payload) || hash(canonical(value.payload)) !== value.sha256) {
      return fail("CORRUPT", `Corrupt scripted input checksum/envelope: ${file}`);
    }
    const payload = value.payload;
    if (payload.role !== role || !exactKeys(payload, ["role", "deliveryId", "round", "published", "result"]) ||
        !object(payload.round) || !exactKeys(payload.round, ["from", "targetId", "message", "data", "delivery", "priority", "triggerTurn", "principal"]) ||
        (payload.published !== undefined && (role !== "sender" || payload.published !== true))) {
      return fail("CORRUPT", `Corrupt scripted input record: ${file}`);
    }
    const round = makeRound(payload.round as unknown as Round);
    if (canonical(round) !== canonical(payload.round) || payload.deliveryId !== roundId(round) ||
        path.basename(file) !== `${roundId(round).split(":").at(-1)!}.json`) return fail("CORRUPT", `Corrupt scripted input immutable binding: ${file}`);
    if (payload.result !== undefined) nativeResult(payload.result, metadataOf(round).deadlineAt);
    return payload as unknown as RecordPayload;
  } catch (error) {
    if (error instanceof ScriptedInputAdmissionError && error.code === "SCRIPTED_INPUT_CORRUPT") throw error;
    return fail("CORRUPT", `Corrupt scripted input immutable envelope: ${file}`);
  }
};
const files = (root: string, role: RecordPayload["role"], identity: string): string[] => {
  requireNamespaceBarrier(notDurable);
  const dir = directory(root, role, identity);
  try { return fs.readdirSync(dir).filter(name => name.endsWith(".json")).sort().map(name => path.join(dir, name)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
const sameRound = (saved: RecordPayload, round: Round): void => {
  if (canonical(saved.round) !== canonical(round)) fail("CONFLICT", `Scripted input immutable round conflict for ${roundId(round)}`);
};
const prepared = (payload: RecordPayload): PreparedScriptedInput => ({
  idempotencyKey: payload.deliveryId, message: payload.round.message, data: snapshot(payload.round.data), deadlineAt: metadataOf(payload.round).deadlineAt,
  delivery: payload.round.delivery,
  ...(payload.round.principal === undefined ? {} : { principal: principalSnapshot(payload.round.principal) }),
  ...(payload.round.priority === undefined ? {} : { priority: payload.round.priority }),
  triggerTurn: payload.round.triggerTurn,
  ...(payload.result === undefined ? {} : { result: duplicate(payload.result) }),
});

/** Persist the immutable outbox intent BEFORE the caller can publish. No driver replay is needed. */
export function prepareScriptedInput(options: { root: string; from: MeshIdentity; targetId: string; message: string; data: unknown; principal?: FabricPrincipal | undefined } & DeliveryPolicy): PreparedScriptedInput {
  const round = makeRound(options);
  const file = recordPath(options.root, "sender", round);
  const previous = load(file, "sender");
  if (previous) sameRound(previous, round);
  const payload: RecordPayload = previous ?? { role: "sender", deliveryId: roundId(round), round };
  if (!previous) save(file, payload);
  return prepared(payload);
}
/** One finite startup outbox scan. Published-but-unacknowledged and expired offers remain visible. */
export function listPendingScriptedInputs(options: { root: string; from: MeshIdentity }): PendingScriptedInput[] {
  const from = sender(options.from);
  return files(options.root, "sender", from.id).flatMap(file => {
    const payload = load(file, "sender") ?? fail("HISTORY_MISSING", `Sender intent disappeared: ${file}`);
    if (canonical(payload.round.from) !== canonical(from)) return fail("CONFLICT", "Scripted input immutable sender conflict");
    return payload.result ? [] : [{ ...prepared(payload), from: snapshot(payload.round.from), targetId: payload.round.targetId, published: payload.published === true }];
  });
}
const senderRecord = (options: SenderKey): { file: string; payload: RecordPayload } => {
  const from = sender(options.from);
  const sessionId = identifier(options.sessionId, "sessionId");
  const inputId = identifier(options.inputId, "inputId");
  const id = deliveryIdFor(from.id, sessionId, inputId);
  const file = path.join(directory(options.root, "sender", from.id), `${id.split(":").at(-1)!}.json`);
  const payload = load(file, "sender") ?? fail("HISTORY_MISSING", "Missing scripted input sender intent history");
  if (canonical(from) !== canonical(payload.round.from)) return fail("CONFLICT", "Scripted input immutable sender conflict");
  return { file, payload };
};
/** Broker publish observation only. This does NOT retire the outbox or claim native processing. */
export function markScriptedInputPublished(options: SenderKey): void {
  const { file, payload } = senderRecord(options);
  if (!payload.published) save(file, { ...payload, published: true });
}
/** Only a host-authenticated native Main receipt retires an intent; callers must verify its origin. */
export function acknowledgeScriptedInput(options: SenderKey & { result: FabricAgentMessageResult }): void {
  const { file, payload } = senderRecord(options);
  const result = nativeResult(options.result, metadataOf(payload.round).deadlineAt);
  if (payload.result) {
    if (payload.result.messageId !== result.messageId) fail("CONFLICT", "Scripted input acknowledgement receipt conflict");
    return;
  }
  save(file, { ...payload, published: true, result });
}

const validateBinding = (binding: Binding): void => {
  identifier(binding.sessionId, "sessionId"); base(binding.root);
  if (!binding.mainAgent.local || binding.mainAgent.id !== `session:${binding.sessionId}` ||
      (binding.mainAgent.sessionId !== undefined && binding.mainAgent.sessionId !== binding.sessionId)) {
    fail("INVALID", "Scripted input binding must identify this local Main session");
  }
};
const duplicate = (result: FabricAgentMessageResult): FabricAgentMessageResult => ({ ...snapshot(result), duplicate: true, triggered: false });

/**
 * Main's stable deliveryId journal is the ONLY native-admission deduper. It checks
 * duplicate ids BEFORE reading data, and reads data BEFORE appending/sending a new item.
 * The accessor pins that boundary: an expired new admission (or lookup-only original)
 * throws before any effect, while an existing native id returns even after expiry.
 * Never deliver first and then reject on deadline, or mark a pending intent admitted.
 */
const deliverNative = (mainAgent: InputAdmissionMainAgent, payload: RecordPayload,
  request: FabricMainAgentDeliveryRequest, lookupOnly = false): FabricAgentMessageResult => {
  const metadata = metadataOf(payload.round);
  // Spreading retains the router's private symbol-keyed native wake admission as well
  // as its host-hydrated verification/source/outcome fields. Principal comes only from
  // the immutable round, including UNKNOWN on legacy records. Data grants none.
  const native: FabricMainAgentDeliveryRequest = { ...request, from: snapshot(payload.round.from), message: payload.round.message,
    principal: principalSnapshot(payload.round.principal),
    delivery: payload.round.delivery, triggerTurn: payload.round.triggerTurn, deliveryId: payload.deliveryId };
  Object.defineProperty(native, "data", { enumerable: true, get: () => {
    if (lookupOnly) return fail("NOT_ADMITTED", "Original scripted input is not admitted in native Main history");
    if (Date.now() >= metadata.deadlineAt) return fail("EXPIRED", `Scripted input deadline expired at ${metadata.deadlineAt}`);
    return payload.round.data;
  } });
  return nativeResult(mainAgent.deliverAgent(native), metadata.deadlineAt);
};

/** Synchronous durable receiver intent -> native journal -> durable result -> ACK. */
export function admitScriptedInput(options: Binding & { request: FabricMainAgentDeliveryRequest }): FabricAgentMessageResult {
  validateBinding(options);
  const metadata = readScriptedInput(options.request.data) ?? fail("INVALID", "data.scriptedInput is required for keyed admission");
  if (metadata.sessionId !== options.sessionId) return fail("INVALID", "scriptedInput.sessionId does not match the receiving session");
  if (options.request.delivery !== "steer" && options.request.delivery !== "followUp") return fail("INVALID", "Scripted input delivery must be steer or followUp");
  const round = makeRound({ ...options.request, delivery: options.request.delivery, targetId: `session:${options.sessionId}`, data: options.request.data });
  const recoveryBinding = bindings.get(options.sessionId);
  if (recoveryBinding?.recovery) {
    if (path.resolve(options.root) !== recoveryBinding.root || options.mainAgent !== recoveryBinding.mainAgent) return fail("INVALID", "Recovery admission must use the current bound root and Main");
    return admitRecoveryRound(recoveryBinding, round);
  }
  const file = recordPath(options.root, "receiver", round);
  const previous = load(file, "receiver");
  if (previous) sameRound(previous, round); // Conflict MUST precede the deadline check.
  if (previous?.result) return duplicate(previous.result);
  const payload: RecordPayload = previous ?? { role: "receiver", deliveryId: roundId(round), round };
  if (!previous) {
    if (Date.now() >= metadata.deadlineAt) return fail("EXPIRED", `Scripted input deadline expired at ${metadata.deadlineAt}`);
    save(file, payload); // Pending is not admission: a cut here must still retry native.
  }
  const result = deliverNative(options.mainAgent, payload, options.request);
  save(file, { ...payload, result }); // Native has already journaled under this SAME stable id.
  return result;
}

const bindings = new Map<string, Binding>();
/** B3 calls after native journal open and existing ownership/exclusion checks; cleanup before close. */
export function bindInputAdmission(options: Binding): () => void {
  validateBinding(options);
  if (bindings.has(options.sessionId)) return fail("ALREADY_BOUND", `Input admission already bound for session ${options.sessionId}`);
  if (options.recovery) validateRecoveryOwner(options.recovery);
  // This trusted native port may have prototype methods and a live epoch getter.
  // Do not spread/snapshot it into a stale identity; only accepted capabilities carry
  // detached full-epoch values. Each gate rereads the current native identity.
  const binding = { ...options, root: path.resolve(options.root) };
  bindings.set(options.sessionId, binding);
  return () => { if (bindings.get(binding.sessionId) === binding) bindings.delete(binding.sessionId); };
}
const bound = (sessionId: string): Binding => bindings.get(identifier(sessionId, "sessionId")) ??
  fail("NOT_BOUND", `Input admission is not bound for session ${sessionId}`);
/** Narrow control-orphan seam: request attribution is supplied by the native router/control host. */
export function admitBoundScriptedInput(options: { sessionId: string; request: FabricMainAgentDeliveryRequest }): FabricAgentMessageResult {
  return admitScriptedInput({ ...bound(options.sessionId), request: options.request });
}

const admittedOriginal = (binding: Binding, originalInputId: string): RecordPayload => {
  const matches = files(binding.root, "receiver", binding.sessionId).flatMap(file => {
    const payload = load(file, "receiver") ?? fail("HISTORY_MISSING", "Original input history disappeared");
    const metadata = metadataOf(payload.round);
    if (metadata.sessionId !== binding.sessionId) return fail("CORRUPT", "Original input session history is corrupt");
    return metadata.inputId === originalInputId ? [{ file, payload }] : [];
  });
  if (!matches.length) return fail("HISTORY_MISSING", "Original input must resolve admitted input history in this session");
  // Sender is intentionally absent from B3's key: never guess if two senders reused an inputId.
  if (matches.length !== 1) return fail("AMBIGUOUS", "Original scripted inputId is ambiguous across senders");
  const { file, payload } = matches[0]!;
  if (payload.result) return payload;
  // Recover a lost response without admitting an unaccepted original just to manufacture history.
  const result = deliverNative(binding.mainAgent, payload, {
    from: payload.round.from, message: payload.round.message, delivery: "followUp",
  }, true);
  const admitted = { ...payload, result };
  save(file, admitted);
  return admitted;
};

/**
 * Existing-only orphan-claim recovery. No receiver intent or native input is created.
 * A pending intent is reconciled only if native already knows its stable deliveryId.
 */
export function findScriptedInputAdmission(options: { sessionId: string; request: FabricMainAgentDeliveryRequest }): FabricAgentMessageResult | undefined {
  const binding = bound(options.sessionId);
  validateBinding(binding);
  if (binding.recovery) return findRecoveryRound(binding, options.request);
  const metadata = readScriptedInput(options.request.data) ?? fail("INVALID", "data.scriptedInput is required for admission lookup");
  if (metadata.sessionId !== binding.sessionId) return fail("INVALID", "scriptedInput.sessionId does not match the receiving session");
  if (options.request.delivery !== "steer" && options.request.delivery !== "followUp") return fail("INVALID", "Scripted input delivery must be steer or followUp");
  const round = makeRound({ ...options.request, delivery: options.request.delivery, targetId: `session:${binding.sessionId}`, data: options.request.data });
  const file = recordPath(binding.root, "receiver", round);
  const payload = load(file, "receiver");
  if (!payload) return undefined;
  sameRound(payload, round);
  if (payload.result) return duplicate(payload.result);
  let result: FabricAgentMessageResult;
  try { result = deliverNative(binding.mainAgent, payload, options.request, true); }
  catch (error) {
    if (error instanceof ScriptedInputAdmissionError && error.code === "SCRIPTED_INPUT_NOT_ADMITTED") return undefined;
    throw error;
  }
  save(file, { ...payload, result });
  return duplicate(result);
}

/** Admission only. Native session consumption is the processing carrier, not effect completion. */
export async function admitContinuation(options: {
  sessionId: string; originalInputId: string; recoveryGeneration: string; text: string;
}): Promise<{ admissionId: string; existing: boolean }> {
  const binding = bound(options.sessionId);
  identifier(options.originalInputId, "originalInputId"); identifier(options.recoveryGeneration, "recoveryGeneration");
  if (binding.recovery) return admitRecoveryContinuation(binding, options);
  const original = admittedOriginal(binding, options.originalInputId);
  const inputId = `scripted-continuation:v1:${hash(canonical([1, options.sessionId, options.originalInputId, options.recoveryGeneration]))}`;
  const data = {
    scriptedInput: { version: 1, inputId, sessionId: binding.sessionId, deadlineAt: metadataOf(original.round).deadlineAt },
    scriptedContinuation: { version: 1, originalInputId: options.originalInputId, recoveryGeneration: options.recoveryGeneration },
  };
  const request: FabricMainAgentDeliveryRequest = {
    from: { id: `session:${binding.sessionId}`, name: "Main", kind: "main", sessionId: binding.sessionId },
    message: options.text, data, delivery: "followUp", triggerTurn: true, source: "fabric-host",
  };
  const result = admitScriptedInput({ ...binding, request });
  return { admissionId: deliveryIdFor(request.from.id, binding.sessionId, inputId), existing: result.duplicate === true };
}

// REQUIRED admission/history. The existing external positive single-writer exclusion is a
// prerequisite, NOT something writeJsonAtomic supplies. One synchronous owner below orders
// accept/select/delivery/outcome/maintenance/retry revisions; never await in its callback.
export interface RecoveryIo { readonly deadlineMs?: number; readonly signal?: AbortSignal }
export interface RecoveryAdmissionOwner {
  /** Native's current full identity; a getter supports in-control epoch revocation. */
  readonly epoch: RecoveryEpoch;
  readonly podId: string;
  readonly bootId: string;
  /** Explicit native evidence that input/outcome capture covered the session from admission.
   * Legacy receiver records cannot be promoted to complete history by absence of a cancel. */
  readonly historyComplete: boolean;
  /** Synchronous positive exclusion/current-binding check, repeated before durable save. */
  assertCurrent(): void;
  /** Existing B3 event path, invoked once AFTER actual join/reconciliation, outside the owner. */
  reconsiderInput(): void | Promise<void>;
  /** Validates the retained B3 repair receipt; never a public-JSON permission grant. */
  validateRepair?(ref: RepairRef): void;
}
export interface AcceptedRecoveryInput {
  readonly admissionKind: "input";
  readonly sessionId: string; readonly originalInputId: string; readonly inputId: string;
  readonly admissionId: string; readonly deliveryId: string; readonly text: string;
  readonly epoch: RecoveryEpoch; readonly deadlineMs: number; readonly revision: number;
  readonly recoveryGeneration?: string;
  readonly executionBranchEntryId?: string;
}
export interface RecoveryInputDelivery {
  readonly entryId: string; readonly bytesSha256: string; readonly seq: number;
  readonly executionBranchEntryId: string;
}
export interface RecoveryInputRecord {
  readonly kind: "round" | "continuation"; readonly originalInputId: string;
  readonly admissionId: string; readonly round: Round; readonly deadlineAt: number;
  readonly podId: string; readonly bootId: string; readonly acceptedRevision: number;
  readonly recoveryGeneration?: string;
  delivery?: RecoveryInputDelivery;
}
export interface RecoveryTerminalEvidence {
  readonly entryId: string; readonly bytesSha256: string; readonly seq: number;
  readonly inputEntryId: string; readonly runId: string; readonly executionBranchEntryId: string;
  readonly finalEntryIds: readonly string[];
}
export interface RecoveryInputOutcomePayload {
  readonly sessionId: string; readonly requestId: string; readonly originalInputId: string;
  readonly kind: "user-cancel" | "lifecycle-interrupt" | "settled";
  readonly at: number; readonly bootId: string; readonly terminalEvidence?: RecoveryTerminalEvidence;
}
export interface RecoveryInputOutcomeRequest extends RecoveryInputOutcomePayload {
  readonly deadlineMs: number; readonly signal?: AbortSignal;
}
export interface RecoveryInputOutcomeRecord extends RecoveryInputOutcomePayload { readonly revision: number }
export type RecoveryInputOutcomeResult =
  | { readonly durable: true; readonly status: "recorded" | "existing"; readonly revision: number }
  | { readonly durable: true; readonly status: "already-terminal"; readonly revision: number; readonly kind: "user-cancel" | "settled" }
  | { readonly durable: false; readonly reason: "conflict" | "deadline" | "aborted" | "store-unavailable" | "incomplete-history" | "superseded" };
export interface RecoveryTaskAbort {
  readonly requestId: string; readonly at: number; readonly bootId: string;
  readonly originalInputIds: readonly string[]; readonly maintenanceRequestId: string;
  committed: boolean;
}
export interface RecoveryGenerationPin {
  readonly originalInputId: string;
  /** Immutable ORIGINAL round admission, never a continuation or a native epoch. */
  readonly admissionId: string;
  readonly recoveryGeneration: string;
}
export interface RecoverySessionLedger {
  readonly version: 1; readonly sessionId: string;
  revision: number; complete: boolean;
  inputs: RecoveryInputRecord[];
  outcomes: RecoveryInputOutcomeRecord[];
  /** One open logical input, distinct from a selected live native occurrence. */
  currentInputId?: string;
  reservation?: { kind: "input"; originalInputId: string; admissionId: string; epoch: RecoveryEpoch; revision: number } | { kind: "maintenance"; requestId: string };
  maintenance: MaintenanceLedgerEntry[];
  retrySlots: Array<{ anchor: string; requestId: string }>;
  repairs: RepairRef[];
  /** One durable generation per original/admission, retained through terminal outcome. */
  recoveryGenerationPins: RecoveryGenerationPin[];
  taskAborts: RecoveryTaskAbort[];
  driveReceipts: string[];
}
export interface RecoveryInputSnapshot {
  readonly sessionId: string; readonly complete: boolean; readonly revision: number;
  readonly accepted?: {
    readonly inputId: string; readonly text: string; readonly podId: string; readonly bootId: string;
    readonly settlement: "open" | "completed" | "cancelled";
    readonly cancellation: "not-cancelled" | "cancelled" | "unknown";
    readonly deadlineMs: number; readonly admissionId: string;
    readonly delivery?: RecoveryInputDelivery; readonly lastOutcome?: RecoveryInputOutcomeRecord;
  };
  readonly queued: readonly { readonly inputId: string; readonly admissionId: string; readonly deadlineMs: number }[];
  readonly outcomes: readonly RecoveryInputOutcomeRecord[];
}
export type RecoveryInputSelectionResult =
  | { readonly selected: true; readonly input: AcceptedRecoveryInput; readonly revision: number; readonly existing: boolean }
  | { readonly selected: false; readonly code: "RECOVERY_MAINTENANCE_BUSY" | "RECOVERY_INPUT_BUSY" | "RECOVERY_INPUT_TERMINAL" | "RECOVERY_REVISION_CONFLICT"; readonly revision: number };
export interface RecoveryInputSelectionRequest extends RecoveryIo {
  readonly sessionId: string; readonly originalInputId: string; readonly epoch: RecoveryEpoch;
  readonly expectedRevision: number; readonly admissionId?: string;
}
export interface RecoveryInputAdapter {
  readRecoveryInput(sessionId: string, io?: RecoveryIo): Promise<RecoveryInputSnapshot>;
  /** Call before settlement/repair production; an unknown write ACK must reread this pin. */
  ensureRecoveryGeneration(sessionId: string, originalInputId: string, io?: RecoveryIo): Promise<string>;
  admitContinuation(request: { sessionId: string; originalInputId: string; recoveryGeneration: string; text: string }): Promise<{ admissionId: string; existing: boolean }>;
  recordInputOutcome(request: RecoveryInputOutcomeRequest): Promise<RecoveryInputOutcomeResult>;
  selectRecoveryInput(request: RecoveryInputSelectionRequest): Promise<RecoveryInputSelectionResult>;
  commitRecoveryInputDelivery(input: AcceptedRecoveryInput, evidence: RecoveryInputDelivery, io?: RecoveryIo): Promise<void>;
  releaseRecoveryInputAfterJoin(input: AcceptedRecoveryInput, joined: Promise<void>, reconcile: () => Promise<void>, io?: RecoveryIo): Promise<void>;
}
export class RecoveryAdmissionError extends Error {
  override readonly name = "RecoveryAdmissionError";
  constructor(readonly code: string, message: string) { super(message); }
}
const recoveryFail = (code: string, message: string): never => { throw new RecoveryAdmissionError(code, message); };
export const recoveryCanonical = canonical;
export const recoveryHash = hash;
const validEpoch = (epoch: unknown): epoch is RecoveryEpoch => object(epoch) && exactKeys(epoch, ["processIncarnationId", "bindingId", "epoch"]) &&
  nonempty(epoch.processIncarnationId) && nonempty(epoch.bindingId) && Number.isSafeInteger(epoch.epoch) && (epoch.epoch as number) >= 0;
const validHash = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const validTime = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;
function validateRecoveryOwner(owner: RecoveryAdmissionOwner): void {
  if (!validEpoch(owner.epoch) || !nonempty(owner.podId) || !nonempty(owner.bootId) || typeof owner.historyComplete !== "boolean" ||
      typeof owner.assertCurrent !== "function" || typeof owner.reconsiderInput !== "function") recoveryFail("RECOVERY_INVALID", "Malformed native recovery admission owner");
  checkOwner(owner);
}
function checkOwner(owner: RecoveryAdmissionOwner): void {
  const returned: unknown = owner.assertCurrent();
  if (returned !== undefined) recoveryFail("RECOVERY_EPOCH_REVOKED", "Positive exclusion/current-binding check must be synchronous");
}
export function recoveryOwner(sessionId: string): RecoveryAdmissionOwner {
  const owner = bindings.get(sessionId)?.recovery;
  if (!owner) return recoveryFail("RECOVERY_NOT_BOUND", "No REQUIRED admission owner is bound to this session");
  checkOwner(owner);
  return owner;
}
export function assertRecoveryEpoch(sessionId: string, epoch: RecoveryEpoch): void {
  if (!validEpoch(epoch) || canonical(recoveryOwner(sessionId).epoch) !== canonical(epoch)) recoveryFail("RECOVERY_EPOCH_REVOKED", "Full process/binding/epoch identity was revoked");
}
function checkIo(io?: RecoveryIo): void {
  if (io?.signal?.aborted) recoveryFail("RECOVERY_ABORTED", "Recovery I/O was aborted");
  if (io?.deadlineMs !== undefined && (!validTime(io.deadlineMs) || Date.now() >= io.deadlineMs)) recoveryFail("RECOVERY_DEADLINE", "Recovery I/O deadline expired");
}
const ledgerPath = (binding: Binding): string => path.join(base(binding.root), "session", `${hash(binding.sessionId)}.json`);
const emptyLedger = (binding: Binding): RecoverySessionLedger => ({ version: 1, sessionId: binding.sessionId, revision: 0,
  complete: binding.recovery!.historyComplete && files(binding.root, "receiver", binding.sessionId).length === 0,
  inputs: [], outcomes: [], maintenance: [], retrySlots: [], repairs: [], recoveryGenerationPins: [], taskAborts: [], driveReceipts: [] });
const checkedLedger = (value: unknown, sessionId: string): RecoverySessionLedger => {
  if (!object(value) || value.version !== 1 || value.sessionId !== sessionId || !Number.isSafeInteger(value.revision) || (value.revision as number) < 0 ||
      typeof value.complete !== "boolean" || !exactKeys(value, ["version", "sessionId", "revision", "complete", "inputs", "outcomes", "currentInputId", "reservation", "maintenance", "retrySlots", "repairs", "recoveryGenerationPins", "taskAborts", "driveReceipts"]) ||
      ["inputs", "outcomes", "maintenance", "retrySlots", "repairs", "recoveryGenerationPins", "taskAborts", "driveReceipts"].some(key => !Array.isArray(value[key]))) {
    return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt serialized session revision/history");
  }
  const s = value as unknown as RecoverySessionLedger;
  const ids = new Set<string>();
  for (const i of s.inputs) {
    if (!object(i) || !exactKeys(i, ["kind", "originalInputId", "admissionId", "round", "deadlineAt", "podId", "bootId", "acceptedRevision", "recoveryGeneration", "delivery"]) ||
        !["round", "continuation"].includes(i.kind) || !nonempty(i.originalInputId) || !nonempty(i.admissionId) || ids.has(i.admissionId) ||
        !nonempty(i.podId) || !nonempty(i.bootId) || !validTime(i.deadlineAt) || !Number.isSafeInteger(i.acceptedRevision) || i.acceptedRevision < 1 || i.acceptedRevision > s.revision ||
        i.admissionId.startsWith("pi-native-entry:v1:")) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt input occurrence history");
    const round = makeRound(i.round);
    if (canonical(round) !== canonical(i.round) || metadataOf(round).sessionId !== sessionId || metadataOf(round).deadlineAt !== i.deadlineAt || roundId(round) !== i.admissionId) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt immutable accepted input");
    }
    ids.add(i.admissionId);
  }
  const originals = new Map<string, RecoveryInputRecord>();
  for (const i of s.inputs.filter(i => i.kind === "round")) {
    if (originals.has(i.originalInputId) || i.originalInputId !== metadataOf(i.round).inputId || Reflect.has(i, "recoveryGeneration")) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Ambiguous original admission/generation history");
    }
    originals.set(i.originalInputId, i);
  }
  const pins = new Map<string, RecoveryGenerationPin>(); const generations = new Set<string>();
  for (const pin of s.recoveryGenerationPins) {
    if (!object(pin) || !exactKeys(pin, ["originalInputId", "admissionId", "recoveryGeneration"]) ||
        !nonempty(pin.originalInputId) || !nonempty(pin.admissionId) || !nonempty(pin.recoveryGeneration) ||
        originals.get(pin.originalInputId)?.admissionId !== pin.admissionId || pins.has(pin.originalInputId) || generations.has(pin.recoveryGeneration)) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt or ambiguous recovery generation pin identity");
    }
    pins.set(pin.originalInputId, pin); generations.add(pin.recoveryGeneration);
  }
  const continuations = new Set<string>();
  for (const i of s.inputs.filter(i => i.kind === "continuation")) {
    const original = originals.get(i.originalInputId); const pin = pins.get(i.originalInputId);
    if (!original || !pin || i.recoveryGeneration !== pin.recoveryGeneration || continuations.has(i.originalInputId) ||
        i.deadlineAt !== original.deadlineAt || i.podId !== original.podId || i.bootId !== original.bootId ||
        canonical(i.round) !== canonical(continuationRound(sessionId, original, pin.recoveryGeneration, i.round.message))) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Continuation is inconsistent with its original generation pin");
    }
    continuations.add(i.originalInputId);
  }
  const repairIds = new Set<string>();
  const checkRepair = (ref: RepairRef): void => {
    if (!object(ref) || !exactKeys(ref, ["originalInputId", "recoveryGeneration", "executionBranchEntryId", "repairReceiptSha256"]) ||
        !nonempty(ref.originalInputId) || !nonempty(ref.recoveryGeneration) || !nonempty(ref.executionBranchEntryId) || !validHash(ref.repairReceiptSha256) ||
        pins.get(ref.originalInputId)?.recoveryGeneration !== ref.recoveryGeneration ||
        originals.get(ref.originalInputId)?.delivery?.executionBranchEntryId !== ref.executionBranchEntryId) {
      recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Repair is inconsistent with its original generation pin/branch");
    }
  };
  for (const ref of s.repairs) {
    checkRepair(ref);
    if (repairIds.has(ref.originalInputId)) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Ambiguous retained repair history");
    repairIds.add(ref.originalInputId);
  }
  if (s.currentInputId && !originals.has(s.currentInputId)) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Missing selected original history");
  const requestIds = new Set<string>();
  for (const e of s.maintenance) {
    if (!object(e) || typeof e.settlementOnly !== "boolean" || typeof e.cancelRequested !== "boolean" || !object(e.record) ||
        !object(e.record.request) || e.record.request.sessionId !== sessionId || !nonempty(e.record.request.requestId) || requestIds.has(e.record.request.requestId) ||
        !["ADMITTED", "ACTIVE", "PREPARED", "CONSUMED"].includes(e.record.state) || !validEpoch(e.record.activeEpoch) ||
        !Number.isSafeInteger(e.record.permitRevision) || e.record.permitRevision < 0 || !Number.isSafeInteger(e.record.revision) || e.record.revision > s.revision ||
        !validHash(e.record.operationSha256) || e.record.operationSha256 !== hash(canonical(e.record.request.operation)) ||
        (["PREPARED", "CONSUMED"].includes(e.record.state) && !e.record.plan) || (e.record.state === "CONSUMED" && !e.record.completion)) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt maintenance state/plan history");
    }
    if (Reflect.has(e.record.request, "repair")) {
      const ref = e.record.request.repair!; checkRepair(ref);
      if (!s.repairs.some(retained => canonical(retained) === canonical(ref))) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Maintenance repair is absent from retained generation history");
    }
    requestIds.add(e.record.request.requestId);
  }
  for (const slot of s.retrySlots) {
    if (!object(slot) || !exactKeys(slot, ["anchor", "requestId"]) || !nonempty(slot.anchor) || !nonempty(slot.requestId) ||
        (originals.has(slot.anchor) && (!pins.has(slot.anchor) || !s.maintenance.some(e => e.record.request.requestId === slot.requestId && e.record.request.repair?.originalInputId === slot.anchor)))) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Retry slot is missing its original generation history");
    }
  }
  if (s.reservation && (s.reservation.kind === "maintenance" ? !requestIds.has(s.reservation.requestId) : s.reservation.kind !== "input" || !ids.has(s.reservation.admissionId) || !validEpoch(s.reservation.epoch) || !Number.isSafeInteger(s.reservation.revision) || s.reservation.revision < 1 || s.reservation.revision > s.revision)) {
    return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt session reservation");
  }
  return s;
};
function loadLedger(binding: Binding): RecoverySessionLedger {
  requireNamespaceBarrier(notDurable);
  const file = ledgerPath(binding);
  let fd: number;
  try { fd = fs.openSync(file, "r"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return emptyLedger(binding); throw error; }
  let envelope: unknown;
  try {
    const stat = fs.fstatSync(fd);
    if (stat.size > 64 * 1024 * 1024) recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Bounded session ledger read would truncate history");
    const bytes = fs.readFileSync(fd, "utf8");
    fs.fsyncSync(fd); syncPathNamespaceDurable(file, stat, notDurable);
    try { envelope = JSON.parse(bytes); } catch { recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Corrupt session ledger JSON"); }
  } finally { fs.closeSync(fd); }
  if (!object(envelope) || !exactKeys(envelope, ["version", "payload", "sha256"]) || envelope.version !== 1 || !validHash(envelope.sha256) ||
      hash(canonical(envelope.payload)) !== envelope.sha256) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Session ledger checksum conflict");
  return checkedLedger(envelope.payload, binding.sessionId);
}
const commitOwners = new Set<string>();
/** Internal shared short transaction used by maintenance-admission. No async callbacks:
 * every load/check/save is synchronous under the existing positively excluded writer.
 * Every operation rereads + barriers, so unknown ACK/rename visibility must reconcile
 * before either dispatcher can act. No second maintenance mutex or cached CAS exists. */
export function commitRecoverySession<T>(sessionId: string, change: (state: RecoverySessionLedger, nextRevision: number) => T, io?: RecoveryIo): T {
  io = { deadlineMs: io?.deadlineMs ?? Date.now() + 10_000, ...(io?.signal ? { signal: io.signal } : {}) };
  checkIo(io);
  const binding = bindings.get(sessionId);
  const owner = recoveryOwner(sessionId);
  if (!binding) return recoveryFail("RECOVERY_NOT_BOUND", "Missing bound session");
  const key = ledgerPath(binding);
  if (commitOwners.has(key)) return recoveryFail("RECOVERY_REVISION_CONFLICT", "Reentrant session commit owner");
  commitOwners.add(key);
  try {
    const s = loadLedger(binding); checkIo(io); checkOwner(owner);
    const previous = canonical(s);
    const next = s.revision + 1;
    if (!Number.isSafeInteger(next)) recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Session revision exhausted");
    const output = change(s, next);
    if (output !== null && typeof output === "object" && "then" in output) recoveryFail("RECOVERY_REVISION_CONFLICT", "A session transaction cannot await");
    checkOwner(owner); checkIo(io);
    if (canonical(s) !== previous) {
      s.revision = next;
      requireNamespaceBarrier(notDurable);
      writeJsonAtomic(key, { version: 1, payload: s, sha256: hash(canonical(s)) }, { durable: true });
      // A deadline/abort after the commit is unknown-ACK, not absence. A retry rereads it.
      checkOwner(owner); checkIo(io);
    }
    return output;
  } finally { commitOwners.delete(key); }
}
export function readRecoverySession(sessionId: string, io?: RecoveryIo): RecoverySessionLedger {
  return commitRecoverySession(sessionId, state => snapshot(state), io);
}
const terminalFor = (s: RecoverySessionLedger, originalInputId: string): RecoveryInputOutcomeRecord | undefined =>
  s.outcomes.find(o => o.originalInputId === originalInputId && o.kind !== "lifecycle-interrupt");
const originalFor = (s: RecoverySessionLedger, originalInputId: string): RecoveryInputRecord =>
  s.inputs.find(i => i.kind === "round" && i.originalInputId === originalInputId) ?? recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Original input is absent from retained accepted history");
const continuationRound = (sessionId: string, original: RecoveryInputRecord, recoveryGeneration: string, text: string): Round =>
  makeRound({ from: { id: `session:${sessionId}`, name: "Main", kind: "main", sessionId }, targetId: `session:${sessionId}`,
    message: text, delivery: "followUp", triggerTurn: true,
    data: { scriptedInput: { version: 1, inputId: `scripted-continuation:v1:${hash(canonical([1, sessionId, original.originalInputId, recoveryGeneration]))}`,
      sessionId, deadlineAt: original.deadlineAt },
    scriptedContinuation: { version: 1, originalInputId: original.originalInputId, recoveryGeneration } } });
const requireGenerationPin = (s: RecoverySessionLedger, original: RecoveryInputRecord, generation: string): void => {
  const pin = s.recoveryGenerationPins.find(p => p.originalInputId === original.originalInputId && p.admissionId === original.admissionId);
  if (!pin) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Missing durable original recovery generation pin");
  if (pin.recoveryGeneration !== generation) recoveryFail("RECOVERY_CONFLICT", "Recovery generation differs from the durable original pin");
};
/** The only REQUIRED generation allocator. Mint inside the shared synchronous owner,
 * persist with its file/namespace barriers before exposing g, and never infer/backfill
 * a pin from a caller UUID, continuation, receipt, boot, or absent marker. */
export async function ensureRecoveryGeneration(sessionId: string, originalInputId: string, io?: RecoveryIo): Promise<string> {
  if (!nonempty(originalInputId)) return recoveryFail("RECOVERY_CONFLICT", "Original input identity is required");
  return commitRecoverySession(sessionId, s => {
    const original = originalFor(s, originalInputId);
    const old = s.recoveryGenerationPins.find(p => p.originalInputId === originalInputId && p.admissionId === original.admissionId);
    if (old) return old.recoveryGeneration; // Also retained after terminal outcome.
    if (!s.complete || s.inputs.some(i => i.originalInputId === originalInputId && i.recoveryGeneration !== undefined) ||
        s.repairs.some(r => r.originalInputId === originalInputId) || s.maintenance.some(e => e.record.request.repair?.originalInputId === originalInputId)) {
      return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Cannot allocate over incomplete or prior generation history");
    }
    if (terminalFor(s, originalInputId)) return recoveryFail("RECOVERY_INPUT_TERMINAL", "Cannot allocate a new generation for a terminal original");
    if (s.currentInputId !== originalInputId) return recoveryFail("RECOVERY_CONFLICT", "Only the current open original can allocate a generation");
    const generation = randomUUID();
    if (s.recoveryGenerationPins.some(p => p.recoveryGeneration === generation)) return recoveryFail("RECOVERY_CONFLICT", "Recovery generation identity collision");
    s.recoveryGenerationPins.push({ originalInputId, admissionId: original.admissionId, recoveryGeneration: generation });
    return generation;
  }, io);
}
const queuedReceipt = (record: RecoveryInputRecord, existing: boolean): FabricAgentMessageResult => ({
  queued: true, routed: "main", messageId: record.admissionId, deadlineAt: record.deadlineAt,
  ...(existing ? { duplicate: true, triggered: false } : {}),
});
/** REQUIRED receiver ACK is durable B5 QUEUE admission, not native carrier commitment.
 * Only the selected controlled native occurrence may append/deliver after recovery.
 * Ordinary native Main admission/receipt semantics above are intentionally unchanged. */
function admitRecoveryRound(binding: Binding, round: Round): FabricAgentMessageResult {
  const m = metadataOf(round);
  const receipt = commitRecoverySession(binding.sessionId, (s, revision) => {
    const admissionId = roundId(round);
    const old = s.inputs.find(i => i.admissionId === admissionId);
    if (old) {
      if (canonical(old.round) !== canonical(round)) fail("CONFLICT", "Changed immutable recovery input");
      return queuedReceipt(old, true);
    }
    if (s.inputs.some(i => i.kind === "round" && i.originalInputId === m.inputId)) fail("AMBIGUOUS", "Original inputId is already owned by another sender");
    if (Date.now() >= m.deadlineAt) fail("EXPIRED", "Original input deadline expired before durable admission");
    const owner = recoveryOwner(binding.sessionId);
    const record: RecoveryInputRecord = { kind: "round", originalInputId: m.inputId, admissionId, round: snapshot(round), deadlineAt: m.deadlineAt,
      podId: owner.podId, bootId: owner.bootId, acceptedRevision: revision };
    s.inputs.push(record);
    return queuedReceipt(record, false);
  });
  return receipt;
}
function findRecoveryRound(binding: Binding, request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult | undefined {
  if (request.delivery !== "steer" && request.delivery !== "followUp") return fail("INVALID", "Recovery input lookup needs a delivery kind");
  const round = makeRound({ ...request, data: request.data, delivery: request.delivery, targetId: `session:${binding.sessionId}` });
  const record = readRecoverySession(binding.sessionId).inputs.find(i => i.admissionId === roundId(round));
  if (!record) return undefined;
  if (canonical(record.round) !== canonical(round)) return fail("CONFLICT", "Changed immutable recovery input lookup");
  return queuedReceipt(record, true);
}
function admitRecoveryContinuation(binding: Binding, request: { originalInputId: string; recoveryGeneration: string; text: string }): { admissionId: string; existing: boolean } {
  if (!nonempty(request.text)) return fail("INVALID", "Continuation text is required");
  return commitRecoverySession(binding.sessionId, (s, revision) => {
    if (!s.complete) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Continuation requires complete original/outcome history");
    const original = originalFor(s, request.originalInputId);
    requireGenerationPin(s, original, request.recoveryGeneration);
    const round = continuationRound(binding.sessionId, original, request.recoveryGeneration, request.text);
    const admissionId = roundId(round);
    const old = s.inputs.find(i => i.admissionId === admissionId);
    if (old) {
      if (canonical(old.round) !== canonical(round)) recoveryFail("RECOVERY_CONFLICT", "Changed continuation under the same generation");
      return { admissionId, existing: true };
    }
    if (terminalFor(s, request.originalInputId)) return recoveryFail("RECOVERY_INPUT_TERMINAL", "A terminal input cannot receive a new continuation");
    if (Date.now() >= original.deadlineAt) return fail("EXPIRED", "Continuation cannot renew its original deadline");
    s.inputs.push({ kind: "continuation", originalInputId: request.originalInputId, admissionId, round, deadlineAt: original.deadlineAt,
      podId: original.podId, bootId: original.bootId, acceptedRevision: revision, recoveryGeneration: request.recoveryGeneration });
    return { admissionId, existing: false };
  });
}
const outcomePayload = (request: RecoveryInputOutcomeRequest | RecoveryInputOutcomePayload): RecoveryInputOutcomePayload => ({
  sessionId: request.sessionId, requestId: request.requestId, originalInputId: request.originalInputId, kind: request.kind, at: request.at, bootId: request.bootId,
  ...(request.terminalEvidence === undefined ? {} : { terminalEvidence: snapshot(request.terminalEvidence) }),
});
function validateOutcome(request: RecoveryInputOutcomePayload): void {
  if (!nonempty(request.sessionId) || !nonempty(request.requestId) || !nonempty(request.originalInputId) || !validTime(request.at) || !nonempty(request.bootId) ||
      !["user-cancel", "lifecycle-interrupt", "settled"].includes(request.kind)) recoveryFail("RECOVERY_CONFLICT", "Malformed immutable input outcome");
  const e = request.terminalEvidence;
  if (request.kind === "settled" && (!e || !nonempty(e.entryId) || !validHash(e.bytesSha256) || !Number.isSafeInteger(e.seq) || e.seq < 0 ||
      !nonempty(e.inputEntryId) || !nonempty(e.runId) || !nonempty(e.executionBranchEntryId) || !Array.isArray(e.finalEntryIds) || !e.finalEntryIds.length || e.finalEntryIds.some(id => !nonempty(id)))) {
    recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Settlement requires validated native terminal witness/B6 references");
  }
  if (request.kind !== "settled" && e !== undefined) recoveryFail("RECOVERY_CONFLICT", "Only settled outcomes carry terminal evidence");
}
/** Same transaction primitive used by E+maintenance consumption. Never invoke externally
 * without the session revision owner; outcome at is evidence, revision orders winners. */
export function applyInputOutcome(s: RecoverySessionLedger, revision: number, payload: RecoveryInputOutcomePayload): RecoveryInputOutcomeResult {
  validateOutcome(payload);
  if (!s.complete || payload.sessionId !== s.sessionId) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Complete session input history is required");
  originalFor(s, payload.originalInputId);
  if (payload.kind === "settled" && !s.inputs.some(i => i.originalInputId === payload.originalInputId && i.delivery?.entryId === payload.terminalEvidence!.inputEntryId &&
      i.delivery.executionBranchEntryId === payload.terminalEvidence!.executionBranchEntryId)) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Terminal witness does not match a committed native input/branch");
  const old = s.outcomes.find(o => o.requestId === payload.requestId);
  if (old) {
    const { revision: oldRevision, ...oldPayload } = old;
    if (canonical(oldPayload) !== canonical(payload)) return recoveryFail("RECOVERY_CONFLICT", "Changed outcome under the same requestId");
    return { durable: true, status: "existing", revision: oldRevision };
  }
  const terminal = terminalFor(s, payload.originalInputId);
  if (terminal) return { durable: true, status: "already-terminal", revision: terminal.revision, kind: terminal.kind as "user-cancel" | "settled" };
  s.outcomes.push({ ...snapshot(payload), revision });
  if (payload.kind !== "lifecycle-interrupt" && s.currentInputId === payload.originalInputId) delete s.currentInputId;
  // This does NOT release the live native occurrence. Only its actual-join edge does.
  return { durable: true, status: "recorded", revision };
}
export async function recordInputOutcome(request: RecoveryInputOutcomeRequest): Promise<RecoveryInputOutcomeResult> {
  try {
    if (!object(request) || !exactKeys(request, ["sessionId", "requestId", "originalInputId", "kind", "at", "bootId", "terminalEvidence", "deadlineMs", "signal"]) ||
        Object.values(Object.getOwnPropertyDescriptors(request)).some(d => !d.enumerable || !("value" in d))) return { durable: false, reason: "conflict" };
    const payload = outcomePayload(request); validateOutcome(payload);
    if (!validTime(request.deadlineMs)) return { durable: false, reason: "deadline" };
    return commitRecoverySession(request.sessionId, (s, revision) => {
      const original = originalFor(s, request.originalInputId);
      // No new computation is authorized by a late recovery-I/O outcome write.
      if (Date.now() < original.deadlineAt && request.deadlineMs > original.deadlineAt) return recoveryFail("RECOVERY_DEADLINE", "Outcome attempt cannot extend its original input deadline");
      return applyInputOutcome(s, revision, payload);
    }, request);
  } catch (error) {
    const code = error instanceof RecoveryAdmissionError ? error.code : undefined;
    const reason = code === "RECOVERY_CONFLICT" ? "conflict" : code === "RECOVERY_DEADLINE" ? "deadline" : code === "RECOVERY_ABORTED" ? "aborted" :
      code === "RECOVERY_HISTORY_INCOMPLETE" ? "incomplete-history" : ["RECOVERY_EPOCH_REVOKED", "RECOVERY_NOT_BOUND"].includes(code ?? "") ? "superseded" : "store-unavailable";
    return { durable: false, reason };
  }
}
export async function readRecoveryInput(sessionId: string, io?: RecoveryIo): Promise<RecoveryInputSnapshot> {
  try {
    const s = readRecoverySession(sessionId, io);
    const id = s.currentInputId ?? (s.reservation?.kind === "input" ? s.reservation.originalInputId : undefined);
    const selected = id ? originalFor(s, id) : undefined;
    const terminal = id ? terminalFor(s, id) : undefined;
    const last = id ? s.outcomes.filter(o => o.originalInputId === id).at(-1) : undefined;
    return {
      sessionId, complete: s.complete, revision: s.revision,
      ...(selected ? { accepted: { inputId: selected.originalInputId, text: selected.round.message, podId: selected.podId, bootId: selected.bootId,
        settlement: terminal?.kind === "settled" ? "completed" as const : terminal?.kind === "user-cancel" ? "cancelled" as const : "open" as const,
        cancellation: terminal?.kind === "user-cancel" ? "cancelled" as const : s.complete ? "not-cancelled" as const : "unknown" as const,
        deadlineMs: selected.deadlineAt, admissionId: selected.admissionId,
        ...(selected.delivery ? { delivery: snapshot(selected.delivery) } : {}), ...(last ? { lastOutcome: snapshot(last) } : {}) } } : {}),
      queued: s.inputs.filter(i => i.kind === "round" && i.originalInputId !== id && !terminalFor(s, i.originalInputId)).map(i => ({ inputId: i.originalInputId, admissionId: i.admissionId, deadlineMs: i.deadlineAt })),
      outcomes: snapshot(s.outcomes),
    };
  } catch (error) {
    // Incomplete/corrupt history is never treated as clean/no-cancel. Deadline/exclusion
    // errors still propagate, so the caller cannot mistake a stale binding for a read.
    if (error instanceof RecoveryAdmissionError && error.code === "RECOVERY_HISTORY_INCOMPLETE") return { sessionId, complete: false, revision: 0, queued: [], outcomes: [] };
    throw error;
  }
}
export async function selectRecoveryInput(request: RecoveryInputSelectionRequest): Promise<RecoveryInputSelectionResult> {
  return commitRecoverySession(request.sessionId, (s, revision) => {
    assertRecoveryEpoch(request.sessionId, request.epoch);
    if (!s.complete) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Cannot select from incomplete history");
    // Deliberately NON-BLOCKING. The native/B5 race is decided only by this durable owner.
    if (s.reservation?.kind === "maintenance") return { selected: false, code: "RECOVERY_MAINTENANCE_BUSY", revision: s.revision };
    if (s.reservation?.kind === "input" && s.reservation.originalInputId !== request.originalInputId || s.currentInputId && s.currentInputId !== request.originalInputId) {
      return { selected: false, code: "RECOVERY_INPUT_BUSY", revision: s.revision };
    }
    if (terminalFor(s, request.originalInputId)) return { selected: false, code: "RECOVERY_INPUT_TERMINAL", revision: s.revision };
    const firstOpen = s.currentInputId ?? s.inputs.find(i => i.kind === "round" && !terminalFor(s, i.originalInputId))?.originalInputId;
    if (firstOpen !== request.originalInputId) return { selected: false, code: "RECOVERY_INPUT_BUSY", revision: s.revision };
    const original = originalFor(s, request.originalInputId);
    const occurrence = request.admissionId ? s.inputs.find(i => i.admissionId === request.admissionId && i.originalInputId === request.originalInputId) :
      s.inputs.filter(i => i.originalInputId === request.originalInputId).at(-1)!;
    if (!occurrence) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Selected occurrence is absent from history");
    const existing = s.reservation?.kind === "input" && s.reservation.admissionId === occurrence.admissionId && canonical(s.reservation.epoch) === canonical(request.epoch);
    if (!existing && s.reservation?.kind === "input" && canonical(s.reservation.epoch) === canonical(request.epoch)) return { selected: false, code: "RECOVERY_INPUT_BUSY", revision: s.revision };
    if (!existing && s.revision !== request.expectedRevision) return { selected: false, code: "RECOVERY_REVISION_CONFLICT", revision: s.revision };
    // Selection may finalize/settle expired work. Provider permission separately enforces deadline.
    if (!existing) {
      s.currentInputId = original.originalInputId;
      s.reservation = { kind: "input", originalInputId: original.originalInputId, admissionId: occurrence.admissionId, epoch: snapshot(request.epoch), revision };
    }
    const input: AcceptedRecoveryInput = { admissionKind: "input", sessionId: s.sessionId, originalInputId: original.originalInputId,
      inputId: metadataOf(occurrence.round).inputId, admissionId: occurrence.admissionId, deliveryId: occurrence.admissionId, text: occurrence.round.message,
      epoch: snapshot(request.epoch), deadlineMs: original.deadlineAt, revision: existing && s.reservation?.kind === "input" ? s.reservation.revision : revision,
      ...(occurrence.recoveryGeneration ? { recoveryGeneration: occurrence.recoveryGeneration } : {}),
      ...(original.delivery ? { executionBranchEntryId: original.delivery.executionBranchEntryId } : {}) };
    return { selected: true, input, revision: existing ? s.revision : revision, existing: !!existing };
  }, request);
}
function selectedOccurrence(s: RecoverySessionLedger, input: AcceptedRecoveryInput): RecoveryInputRecord {
  assertRecoveryEpoch(input.sessionId, input.epoch);
  const r = s.reservation;
  if (r?.kind !== "input" || r.admissionId !== input.admissionId || r.originalInputId !== input.originalInputId || canonical(r.epoch) !== canonical(input.epoch) || r.revision !== input.revision) {
    return recoveryFail("RECOVERY_EPOCH_REVOKED", "Input does not own the current durable native reservation");
  }
  return s.inputs.find(i => i.admissionId === input.admissionId) ?? recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Missing occurrence history");
}
export async function commitRecoveryInputDelivery(input: AcceptedRecoveryInput, evidence: RecoveryInputDelivery, io?: RecoveryIo): Promise<void> {
  if (!nonempty(evidence.entryId) || !validHash(evidence.bytesSha256) || !Number.isSafeInteger(evidence.seq) || evidence.seq < 0 || !nonempty(evidence.executionBranchEntryId)) {
    return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Native delivery requires exact entry/hash/branch/B6 references");
  }
  commitRecoverySession(input.sessionId, s => {
    const occurrence = selectedOccurrence(s, input);
    if (occurrence.delivery && canonical(occurrence.delivery) !== canonical(evidence)) return recoveryFail("RECOVERY_CONFLICT", "Changed native delivery commitment");
    if (!occurrence.delivery) occurrence.delivery = snapshot(evidence);
  }, io);
}
export function assertRecoveryInputPermission(input: AcceptedRecoveryInput): void {
  const s = readRecoverySession(input.sessionId);
  const occurrence = selectedOccurrence(s, input);
  if (!occurrence.delivery || terminalFor(s, input.originalInputId)) recoveryFail("RECOVERY_INPUT_TERMINAL", "No committed open native occurrence permits inference");
  if (Date.now() >= input.deadlineMs) recoveryFail("RECOVERY_DEADLINE", "Original input deadline expired");
}
const inputEdges = new WeakMap<RecoveryAdmissionOwner, Map<string, Promise<void>>>();
export async function releaseRecoveryInputAfterJoin(input: AcceptedRecoveryInput, joined: Promise<void>, reconcile: () => Promise<void>, io?: RecoveryIo): Promise<void> {
  const owner = recoveryOwner(input.sessionId);
  let memo = inputEdges.get(owner); if (!memo) { memo = new Map(); inputEdges.set(owner, memo); }
  const key = canonical([input.sessionId, input.admissionId, input.epoch]);
  const prior = memo.get(key); if (prior) return prior;
  const edge = Promise.resolve().then(async () => {
    await joined;
    const s = readRecoverySession(input.sessionId, io); selectedOccurrence(s, input);
    if (!s.outcomes.some(o => o.originalInputId === input.originalInputId)) recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Actual join is not a durable input outcome");
    // Release this native occurrence only after actual join, while logical L work stays open.
    commitRecoverySession(input.sessionId, state => { selectedOccurrence(state, input); delete state.reservation; }, io);
    await reconcile();
    assertRecoveryEpoch(input.sessionId, input.epoch);
    await driveRecoveryInputOnce(input.sessionId, `input-release:${key}`, io);
  });
  memo.set(key, edge);
  return edge;
}
export async function driveRecoveryInputOnce(sessionId: string, key: string, io?: RecoveryIo): Promise<void> {
  const owner = recoveryOwner(sessionId);
  const drive = commitRecoverySession(sessionId, s => {
    if (s.driveReceipts.includes(key)) return false;
    s.driveReceipts.push(key);
    return true;
  }, io);
  if (drive) await owner.reconsiderInput();
}
export async function recordRecoveryRepair(sessionId: string, ref: RepairRef, io?: RecoveryIo): Promise<void> {
  const owner = recoveryOwner(sessionId);
  if (!owner.validateRepair) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "No native repair receipt validator is bound");
  if (!object(ref) || !exactKeys(ref, ["originalInputId", "recoveryGeneration", "executionBranchEntryId", "repairReceiptSha256"]) ||
      Object.values(Object.getOwnPropertyDescriptors(ref)).some(d => !d.enumerable || !("value" in d)) ||
      !nonempty(ref.originalInputId) || !nonempty(ref.recoveryGeneration) || !nonempty(ref.executionBranchEntryId) || !validHash(ref.repairReceiptSha256)) return recoveryFail("RECOVERY_CONFLICT", "Invalid repair reference");
  commitRecoverySession(sessionId, s => {
    if (!s.complete) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Repair requires complete original/outcome history");
    const original = originalFor(s, ref.originalInputId);
    requireGenerationPin(s, original, ref.recoveryGeneration);
    const old = s.repairs.find(r => r.originalInputId === ref.originalInputId && r.recoveryGeneration === ref.recoveryGeneration);
    if (old && canonical(old) !== canonical(ref)) return recoveryFail("RECOVERY_CONFLICT", "Changed repair receipt under the same generation");
    // This trusted native port is synchronous validation only, never receipt production,
    // provider/effect work, or a public JSON grant. Check the pin before invoking it.
    const verified: unknown = owner.validateRepair!(snapshot(ref));
    if (verified !== undefined) return recoveryFail("RECOVERY_HISTORY_INCOMPLETE", "Repair receipt validation must complete synchronously before admission");
    if (old) return; // Exact retained retry, including after a terminal outcome/rebind.
    if (terminalFor(s, ref.originalInputId) || s.currentInputId !== ref.originalInputId || s.reservation?.kind === "input" ||
        !original.delivery || original.delivery.executionBranchEntryId !== ref.executionBranchEntryId) return recoveryFail("RECOVERY_CONFLICT", "Repair must belong to the joined open original execution branch");
    s.repairs.push(snapshot(ref));
  }, io);
}
export function createRecoveryInputAdapter(): RecoveryInputAdapter {
  return { readRecoveryInput, ensureRecoveryGeneration, admitContinuation, recordInputOutcome, selectRecoveryInput, commitRecoveryInputDelivery, releaseRecoveryInputAfterJoin };
}
