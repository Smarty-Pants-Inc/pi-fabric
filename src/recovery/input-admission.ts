import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { requireNamespaceBarrier, syncPathNamespaceDurable, writeJsonAtomic } from "../core/atomic-write.js";
import { copyFabricPrincipal, type FabricPrincipal } from "../host-compatibility.js";
import type { FabricAgentMessageDelivery, FabricAgentMessageResult, FabricMainAgentDeliveryRequest, FabricSteerPriority, MainAgentController } from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";

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
interface Binding { sessionId: string; root: string; mainAgent: InputAdmissionMainAgent }
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
