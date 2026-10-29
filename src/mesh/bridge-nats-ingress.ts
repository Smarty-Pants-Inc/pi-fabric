// INTERNAL native actor ingress. Public mesh labels are lookup hints, never credentials.
// The ledger's sole writer publishes receipts; this module only builds/checks bounded JSON and
// reads the host-pinned projection. Same-UID disk writers/trusted host code are outside A′'s boundary.
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { NativeActorBinding, NativeActorContext } from "../actors/native-handler.js";
import { processSender } from "../scope.js";
import { residentHostId } from "../residency/protocol.js";
import {
  BRIDGE_NATS_CONTROL_TOPIC, BRIDGE_NATS_MACHINE, BRIDGE_NATS_SESSION_ID, bridgeNatsMappedCommandId,
  type BridgeNatsCommandRecord,
} from "./bridge-nats-types.js";
import { controlBodyFingerprint } from "./control-idempotency.js";
import type { MeshEvent } from "./event-log.js";

export const NATIVE_INGRESS_DIRECTORY = "native-ingress";
export const NATIVE_INGRESS_MAX_BYTES = 16 * 1024;
const MAX_SEMANTIC_BYTES = 1024 * 1024;
const COMMAND_ID = /^[0-9a-f]{64}$/;
// Keep the existing control-plane effective deadline ceiling; no new timeout authority.
const MAX_CONTROL_TIMEOUT_MS = 24 * 60 * 60 * 1_000 + 60_000;
const COMMAND_FIELDS = new Set([
  "version", "commandId", "targetId", "operation", "replyTo", "ownerIncarnation", "message", "data", "wakeCause",
  "triggerTurn", "priority", "binding", "bindingProvenance", "cancelCommandId", "requestCreatedAt", "requestedAt", "deadlineAt", "bridge",
]);
const EVENT_FIELDS = new Set([
  "id", "sequence", "createdAt", "topic", "kind", "from", "to", "principal", "verification", "sender", "text", "data", "dedupeKey", "dedupeFingerprint",
]);
const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const text = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 512 && !/[\u0000-\u001f\u007f]/.test(value);
const fail = (reason: string): never => { throw new Error(`Native actor ingress refused: ${reason}`); };
const hash = (value: string): string => createHash("sha256").update(value, "utf8").digest("hex");

/** Strict, key-sorted JSON, including integer-looking keys. No toJSON/getter/undefined repair. */
const canonical = (value: unknown, depth = 0): string => {
  if (depth > 128) return fail("JSON nesting exceeds bound");
  if (value === null || typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length) return fail("non-JSON array");
    return `[${Array.from({ length: value.length }, (_, index) => {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor || !("value" in descriptor)) return fail("non-JSON array element");
      return canonical(descriptor.value, depth + 1);
    }).join(",")}]`;
  }
  if (object(value) && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)) {
    if (Object.getOwnPropertySymbols(value).length) return fail("non-JSON symbol");
    return `{${Object.keys(value).sort().map(key => {
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      if (!("value" in descriptor)) return fail("non-JSON accessor");
      return `${JSON.stringify(key)}:${canonical(descriptor.value, depth + 1)}`;
    }).join(",")}}`;
  }
  return fail("non-JSON value");
};
const boundedJson = (value: unknown, max = MAX_SEMANTIC_BYTES): string => {
  const json = canonical(value);
  if (Buffer.byteLength(json, "utf8") > max) return fail("JSON exceeds byte bound");
  return json;
};
const freeze = <T>(value: T): T => {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const child of Object.values(value)) freeze(child);
  }
  return value;
};
const keysOnly = (value: Record<string, unknown>, allowed: ReadonlySet<string>): void => {
  if (Object.keys(value).some(key => !allowed.has(key))) fail("unsupported semantic field");
};
const canonicalSender = (from: unknown): NativeActorContext["sender"] => {
  if (!object(from) || from.kind !== "main" || typeof from.id !== "string" || !BRIDGE_NATS_SESSION_ID.test(from.id)) {
    return fail("sender is not a canonical Main");
  }
  keysOnly(from, new Set(["id", "kind", "name", "sessionId", "verified"]));
  const sessionId = from.id.slice(8);
  if (from.sessionId !== undefined && from.sessionId !== sessionId) return fail("inconsistent sender session");
  return { id: from.id, kind: "main", sessionId };
};

/** Canonical FINAL MeshBridge/StoreBridgeSide semantics. Only local id/sequence/time/name vary.
 * Optional session metadata is independently derived, not a credential. Store-generated dedupe
 * metadata is included and checked, not silently discarded. Unsupported outer semantics refuse.
 */
const semantic = (event: unknown): Record<string, unknown> => {
  if (!object(event)) return fail("event is not an object");
  keysOnly(event, EVENT_FIELDS);
  const sender = canonicalSender(event.from);
  if (!object(event.from) || event.from.verified !== "bridge" || event.verification !== "bridge" ||
      event.topic !== BRIDGE_NATS_CONTROL_TOPIC || !object(event.data) || !text(event.to) || !object(event.sender)) return fail("not final bridge control");
  const data = event.data;
  keysOnly(data, COMMAND_FIELDS);
  if (data.version !== 1 || typeof data.commandId !== "string" || !COMMAND_ID.test(data.commandId) || !text(data.targetId) ||
      (data.operation !== "ask" && data.operation !== "cancel") || event.kind !== data.operation || !text(data.replyTo) ||
      typeof data.requestedAt !== "number" || !Number.isFinite(data.requestedAt) || typeof data.deadlineAt !== "number" ||
      !Number.isFinite(data.deadlineAt) || data.deadlineAt < data.requestedAt || !object(data.bridge) ||
      !text(data.bridge.from) || !text(data.bridge.id)) return fail("malformed control semantics");
  keysOnly(data.bridge, new Set(["from", "id"]));
  if (data.operation === "ask" ? typeof data.message !== "string" || data.cancelCommandId !== undefined
    : typeof data.cancelCommandId !== "string" || !COMMAND_ID.test(data.cancelCommandId)) return fail("malformed ask/cancel");
  if (data.ownerIncarnation !== undefined && !text(data.ownerIncarnation)) return fail("malformed owner incarnation");
  if (event.text !== undefined) return fail("unsupported control text");
  if (event.principal !== undefined && (!object(event.principal) || !text(event.principal.id) ||
      !["herdr-client", "voice-call", "org-agent"].includes(String(event.principal.binding)) ||
      Object.keys(event.principal).some(key => key !== "id" && key !== "binding"))) return fail("malformed principal");
  const dedupeKey = `bridge-publish:${hash(JSON.stringify([data.bridge.from, sender.id, data.commandId]))}`;
  const dedupeFingerprint = controlBodyFingerprint(data, event.principal);
  if (event.dedupeKey !== dedupeKey || event.dedupeFingerprint !== dedupeFingerprint) return fail("different publication metadata");
  return { topic: event.topic, kind: event.kind, from: { ...sender, verified: "bridge" }, to: event.to,
    ...(event.principal !== undefined ? { principal: event.principal } : {}), verification: "bridge", sender: event.sender, data,
    dedupeKey, dedupeFingerprint };
};

export interface NativeIngressReceipt {
  readonly v: 1;
  readonly link: string;
  readonly commandId: string;
  readonly sourceCommandId: string;
  readonly sourceEventId: string;
  readonly sender: NativeActorContext["sender"];
  readonly sourceMachine: string;
  readonly targetId: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation?: string;
  readonly operation: "ask" | "cancel";
  readonly cancelCommandId?: string;
  readonly sourceCancelCommandId?: string;
  readonly replyTo: string;
  readonly deadlineAt: number;
  readonly expiresAt: number;
  readonly eventDigest: string;
}
const RECEIPT_FIELDS = new Set([
  "v", "link", "commandId", "sourceCommandId", "sourceEventId", "sender", "sourceMachine", "targetId", "ownerHostId", "ownerIdentityId",
  "ownerIncarnation", "operation", "cancelCommandId", "sourceCancelCommandId", "replyTo", "deadlineAt", "expiresAt", "eventDigest",
]);
const checkedReceipt = (value: unknown): NativeIngressReceipt => {
  if (!object(value)) return fail("receipt is not an object");
  keysOnly(value, RECEIPT_FIELDS);
  const sender = canonicalSender(value.sender);
  if (!object(value.sender) || Object.keys(value.sender).length !== 3 || value.sender.sessionId !== sender.sessionId || value.v !== 1 ||
      !text(value.link) || !text(value.sourceCommandId) || !text(value.sourceEventId) || !text(value.targetId) ||
      !text(value.ownerHostId) || !text(value.ownerIdentityId) || !text(value.replyTo) || typeof value.commandId !== "string" ||
      value.commandId !== bridgeNatsMappedCommandId(value.link, "toFile", sender.id, value.sourceCommandId) ||
      typeof value.sourceMachine !== "string" || !BRIDGE_NATS_MACHINE.test(value.sourceMachine) ||
      typeof value.deadlineAt !== "number" || !Number.isFinite(value.deadlineAt) || typeof value.expiresAt !== "number" ||
      !Number.isFinite(value.expiresAt) || value.expiresAt < value.deadlineAt || typeof value.eventDigest !== "string" ||
      !COMMAND_ID.test(value.eventDigest) || (value.operation !== "ask" && value.operation !== "cancel") ||
      (value.ownerIncarnation !== undefined && !text(value.ownerIncarnation))) return fail("malformed receipt");
  if (value.operation === "cancel") {
    if (!text(value.sourceCancelCommandId) || value.cancelCommandId !==
        bridgeNatsMappedCommandId(value.link, "toFile", sender.id, value.sourceCancelCommandId)) return fail("invalid cancellation mapping");
  } else if (value.cancelCommandId !== undefined || value.sourceCancelCommandId !== undefined) return fail("unexpected cancellation");
  return freeze(value as unknown as NativeIngressReceipt);
};

/** Producer recipe, called only on a retained, durable claim by its ledger owner.
 * Capture the same process-local authority stamp MeshStore.publish adds at final append. It is
 * bound, not treated as remote identity; neither a peer nor an event body chooses this stamp.
 */
export const nativeIngressReceiptForClaim = (record: BridgeNatsCommandRecord, link: string): NativeIngressReceipt => {
  if (record.direction !== "toFile" || record.origin !== "control" || !record.resolvedBody ||
      (record.operation !== "ask" && record.operation !== "cancel")) return fail("claim has no retained ingress evidence");
  const sender = canonicalSender(record.from);
  const data: Record<string, unknown> = { ...record.resolvedBody, bridge: { from: link, id: record.sourceEventId } };
  if (data.commandId !== record.commandId || data.targetId !== record.targetId || data.operation !== record.operation ||
      data.replyTo !== record.replyTo || data.deadlineAt !== record.deadlineAt || data.ownerIncarnation !== record.ownerIncarnation ||
      data.cancelCommandId !== record.cancelCommandId || record.requesterId !== sender.id) return fail("retained claim differs");
  const event = { topic: BRIDGE_NATS_CONTROL_TOPIC, kind: record.operation, from: { ...sender, verified: "bridge" },
    to: record.ownerHostId, ...(record.principal !== undefined ? { principal: record.principal } : {}), verification: "bridge", sender: processSender(), data,
    dedupeKey: `bridge-publish:${hash(JSON.stringify([link, sender.id, record.commandId]))}`,
    dedupeFingerprint: controlBodyFingerprint(data, record.principal) };
  return checkedReceipt({ v: 1, link, commandId: record.commandId, sourceCommandId: record.originalCommandId,
    sourceEventId: record.sourceEventId, sender, sourceMachine: record.sourceMachine, targetId: record.targetId,
    ownerHostId: record.ownerHostId, ownerIdentityId: record.ownerIdentityId,
    ...(record.ownerIncarnation !== undefined ? { ownerIncarnation: record.ownerIncarnation } : {}), operation: record.operation,
    ...(record.cancelCommandId !== undefined ? { cancelCommandId: record.cancelCommandId, sourceCancelCommandId: record.originalCancelCommandId } : {}),
    replyTo: record.replyTo, deadlineAt: Math.min(record.deadlineAt, Number(data.requestedAt) + MAX_CONTROL_TIMEOUT_MS),
    expiresAt: record.expiresAt, eventDigest: hash(boundedJson(semantic(event))) });
};

/** Corruption detection only, NOT origin authentication or protection against same-UID writers. */
export const encodeNativeIngressReceipt = (receipt: NativeIngressReceipt): string =>
  `${boundedJson({ ...checkedReceipt(receipt), h: hash(boundedJson(receipt)) }, NATIVE_INGRESS_MAX_BYTES - 1)}\n`;

/** Private directory/file checks also used by the sole ledger writer. No symlink ancestry. */
export const checkNativeIngressDirectory = (ledgerDir: string, allowMissingProjection = false): string => {
  if (!path.isAbsolute(ledgerDir) || process.geteuid === undefined || !fs.constants.O_NOFOLLOW || !fs.constants.O_DIRECTORY) {
    return fail("unsupported or unpinned receipt directory");
  }
  const dir = path.join(ledgerDir, NATIVE_INGRESS_DIRECTORY);
  let current = path.parse(dir).root;
  for (const part of dir.slice(current.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    const stat = fs.lstatSync(current, { throwIfNoEntry: false });
    if (!stat && allowMissingProjection && current === dir) return dir;
    if (!stat || !stat.isDirectory() || stat.isSymbolicLink()) return fail("receipt path is not a real directory");
    if ((current === path.resolve(ledgerDir) || current === dir) &&
        (stat.uid !== process.geteuid() || (stat.mode & 0o7777) !== 0o700)) return fail("unsafe receipt directory");
  }
  return dir;
};

/** Bounded no-follow read through one fd, with descriptor/path identity checks. Never repairs. */
export const readNativeIngressReceipt = (ledgerDir: string, commandId: string): NativeIngressReceipt => {
  if (!COMMAND_ID.test(commandId)) return fail("invalid receipt lookup id");
  const dir = checkNativeIngressDirectory(ledgerDir);
  const file = path.join(dir, `${commandId}.json`);
  const before = fs.lstatSync(file);
  if (!before.isFile() || before.isSymbolicLink()) return fail("receipt is not a regular file");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.dev !== before.dev || stat.ino !== before.ino || stat.uid !== process.geteuid!() ||
        (stat.mode & 0o7777) !== 0o600 || stat.size < 1 || stat.size > NATIVE_INGRESS_MAX_BYTES) return fail("unsafe receipt file");
    const bytes = Buffer.alloc(stat.size + 1);
    let length = 0;
    while (length < bytes.length) {
      const count = fs.readSync(fd, bytes, length, bytes.length - length, null);
      if (!count) break;
      length += count;
    }
    const after = fs.fstatSync(fd), named = fs.lstatSync(file);
    if (length !== stat.size || after.size !== stat.size || after.mtimeMs !== stat.mtimeMs || after.ctimeMs !== stat.ctimeMs ||
        named.dev !== stat.dev || named.ino !== stat.ino || !named.isFile()) return fail("receipt changed while reading");
    const parsed: unknown = JSON.parse(bytes.subarray(0, length).toString("utf8"));
    if (!object(parsed)) return fail("receipt is not an object");
    boundedJson(parsed, NATIVE_INGRESS_MAX_BYTES);
    const { h, ...contents } = parsed;
    if (typeof h !== "string" || h !== hash(boundedJson(contents))) return fail("receipt corruption checksum differs");
    const receipt = checkedReceipt(contents);
    if (receipt.commandId !== commandId) return fail("different receipt id");
    return receipt;
  } finally { fs.closeSync(fd); }
};

// No mint-from-context export, no serialized trusted state, and no public native-actors re-export.
declare const admissionBrand: unique symbol;
export interface NativeControlAdmission { readonly [admissionBrand]: true }
export interface NativeActorIngress {
  requiresNativeAdmission(targetId: string): boolean;
  admitNativeControl(event: MeshEvent): NativeControlAdmission;
}
interface Admission {
  readonly binding: NativeActorBinding;
  readonly receipt: NativeIngressReceipt;
  readonly request: string | undefined;
  readonly now: () => number;
  state: "admitted" | "claimed" | "consumed";
}
const admissions = new WeakMap<NativeControlAdmission, Admission>();
const requestKey = (expected: { actorId: string; message: string; data: unknown }): string =>
  boundedJson({ actorId: expected.actorId, message: expected.message, ...(expected.data !== undefined ? { data: expected.data } : {}) });
const live = (admission: Admission): void => {
  const now = admission.now();
  if (!Number.isFinite(now) || now > admission.receipt.deadlineAt || now > admission.receipt.expiresAt) fail("expired admission");
};

export const createNativeActorIngress = (options: {
  readonly bindings: readonly NativeActorBinding[];
  readonly isNativeActor?: (id: string) => boolean;
  readonly now?: () => number;
}): NativeActorIngress => {
  const bindings = new Map<string, NativeActorBinding>();
  for (const input of options.bindings) {
    const binding = freeze(JSON.parse(boundedJson(input)) as NativeActorBinding);
    if (!/^[0-9a-f]{32}$/.test(binding.actorId) || !BRIDGE_NATS_SESSION_ID.test(binding.rootId) || binding.scope !== "session" ||
        !object(binding.ingress) || !text(binding.ingress.link) || !path.isAbsolute(binding.ingress.ledgerDir) || bindings.has(binding.actorId)) {
      fail("invalid/duplicate native binding");
    }
    bindings.set(binding.actorId, binding);
  }
  const now = options.now ?? Date.now, isNativeActor = options.isNativeActor;
  return Object.freeze({
    requiresNativeAdmission: (id: string): boolean => bindings.has(id) || isNativeActor?.(id) === true,
    admitNativeControl(event: MeshEvent): NativeControlAdmission {
      // Snapshot once; mutation of the genuine event or caller-owned data cannot change issued proof.
      const checked = freeze(JSON.parse(boundedJson(event)) as MeshEvent);
      const body = semantic(checked);
      const data = checked.data as Record<string, unknown>;
      const binding = bindings.get(data.targetId as string);
      if (!binding) return fail("native target has no pinned binding");
      const receipt = readNativeIngressReceipt(binding.ingress.ledgerDir, data.commandId as string);
      const sender = canonicalSender(checked.from);
      const stamp = data.bridge as Record<string, unknown>;
      if (receipt.link !== binding.ingress.link || stamp.from !== receipt.link || stamp.id !== receipt.sourceEventId ||
          receipt.eventDigest !== hash(boundedJson(body)) || receipt.targetId !== binding.actorId ||
          receipt.ownerIdentityId !== residentHostId(binding.rootId) || receipt.ownerHostId !== residentHostId(binding.rootId) ||
          receipt.ownerHostId !== checked.to || receipt.sender.id !== sender.id ||
          receipt.operation !== data.operation || receipt.replyTo !== data.replyTo || receipt.ownerIncarnation !== data.ownerIncarnation ||
          receipt.deadlineAt !== Math.min(data.deadlineAt as number, (data.requestedAt as number) + MAX_CONTROL_TIMEOUT_MS)) {
        return fail("event differs from pinned ingress receipt");
      }
      if (receipt.operation === "cancel") {
        const original = readNativeIngressReceipt(binding.ingress.ledgerDir, receipt.cancelCommandId!);
        if (original.operation !== "ask" || original.link !== receipt.link || original.sender.id !== receipt.sender.id ||
            original.sourceMachine !== receipt.sourceMachine || original.targetId !== receipt.targetId ||
            original.sourceCommandId !== receipt.sourceCancelCommandId || original.ownerHostId !== receipt.ownerHostId ||
            original.ownerIdentityId !== receipt.ownerIdentityId || original.replyTo !== receipt.replyTo || now() > original.expiresAt) {
          return fail("cancel differs from original requester/command lineage");
        }
      }
      const handle = Object.freeze({}) as NativeControlAdmission;
      const admission: Admission = { binding, receipt, now, state: "admitted",
        request: receipt.operation === "ask" ? requestKey({ actorId: binding.actorId, message: data.message as string, data: data.data }) : undefined };
      live(admission);
      admissions.set(handle, admission);
      return handle;
    },
  });
};

/** INTERNAL: bind one genuine ask handle to exactly one enqueue, before any queue mutation. */
export const claimNativeAskAdmission = (handle: NativeControlAdmission,
  expected: { actorId: string; message: string; data: unknown }): void => {
  const admission = admissions.get(handle);
  if (!admission || admission.state !== "admitted" || admission.request === undefined || admission.request !== requestKey(expected)) {
    return fail("invalid, altered or reused enqueue admission");
  }
  live(admission);
  admission.state = "claimed";
};

/** INTERNAL: callback entry only, after queue ownership/binding checks; never serialize this handle. */
export const consumeNativeAskAdmission = (handle: NativeControlAdmission,
  expected: { actorId: string; message: string; data: unknown }, signal: AbortSignal): NativeActorContext => {
  const admission = admissions.get(handle);
  if (!admission || admission.state !== "claimed" || admission.request !== requestKey(expected) || !signal || signal.aborted) {
    return fail("invalid, altered, cancelled or consumed callback admission");
  }
  live(admission);
  admission.state = "consumed";
  const r = admission.receipt;
  return Object.freeze({ actorId: admission.binding.actorId, source: "nats-bridge", sender: Object.freeze({ ...r.sender }),
    sourceMachine: r.sourceMachine, link: r.link, commandId: r.commandId, sourceCommandId: r.sourceCommandId,
    sourceEventId: r.sourceEventId, deadlineAt: r.deadlineAt, signal });
};
