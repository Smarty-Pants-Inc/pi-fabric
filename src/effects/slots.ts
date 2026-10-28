import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { types } from "node:util";
import { EffectContractError } from "./contracts.js";
import type {
  AcceptedInput,
  EffectIntent,
  EffectKeyTuple,
  EffectManifest,
  EffectPayload,
  EffectRecord,
  EffectSlot,
  JsonValue,
} from "./contracts.js";

const HASH = /^[0-9a-f]{64}$/;
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const CANONICAL_ERROR = "EFFECT_CANONICAL_INVALID";
const MANIFEST_ERROR = "EFFECT_MANIFEST_INVALID";
const INTENT_ERROR = "EFFECT_INTENT_INVALID";
const SLOT_ERROR = "EFFECT_SLOT_INVALID";

type JsonObject = Record<string, unknown>;

function fail(code: string, message: string): never {
  throw new EffectContractError(code, message);
}

/** UTF-8 must not replace an unpaired UTF-16 surrogate with U+FFFD. No NFC/NFD
 * normalization is performed: distinct, valid Unicode strings remain distinct. */
function assertUnicode(value: string, code: string): void {
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) fail(code, "Unpaired high surrogate");
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      fail(code, "Unpaired low surrogate");
    }
  }
}

function assertString(value: unknown, name: string, code: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) fail(code, `${name} must be a nonempty string`);
  assertUnicode(value, code);
}

function assertHash(value: unknown, name: string, code: string): asserts value is string {
  if (typeof value !== "string" || !HASH.test(value)) fail(code, `${name} must be a lowercase SHA-256 hex digest`);
}

function assertOrdinal(value: unknown, name: string, code: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || Object.is(value, -0)) {
    fail(code, `${name} must be a nonnegative safe integer`);
  }
}

// A void guard preserves readonly element types instead of narrowing to any[].
function requireArray(value: unknown, name: string, code: string): void {
  if (!Array.isArray(value)) fail(code, `${name} must be an array`);
}

/** Only own enumerable data fields are JSON. In particular, do not call toJSON,
 * getters, or Proxy traps while deciding which fields to hash. */
function assertShape(
  value: unknown,
  required: readonly string[],
  optional: readonly string[],
  name: string,
  code: string,
): asserts value is JsonObject {
  if (value === null || typeof value !== "object" || types.isProxy(value)) fail(code, `${name} must be a plain object`);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) fail(code, `${name} must be a plain object`);
  const allowed = new Set([...required, ...optional]);
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !allowed.has(key)) fail(code, `${name} has an unknown field`);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !("value" in descriptor) || !descriptor.enumerable) {
      fail(code, `${name}.${key} must be an enumerable data field`);
    }
  }
  for (const key of required) {
    if (!Object.hasOwn(value, key)) fail(code, `${name}.${key} is required`);
  }
}

/** canonicalEncode v1: compact JSON, UTF-16 lexical key order, ordered dense
 * arrays and finite JSON primitives. Like JSON number serialization, -0 is 0.
 * Repeated acyclic references are fine; cycles, exotic objects and all silent
 * JSON.stringify omissions/conversions are rejected. */
export function canonicalEncode(value: unknown): string {
  const active = new Set<object>();
  function encode(current: unknown): string {
    if (current === null) return "null";
    switch (typeof current) {
      case "boolean":
        return current ? "true" : "false";
      case "number":
        if (!Number.isFinite(current)) fail(CANONICAL_ERROR, "Non-finite number is not JSON");
        return JSON.stringify(current);
      case "string":
        assertUnicode(current, CANONICAL_ERROR);
        return JSON.stringify(current);
      case "object":
        break;
      default:
        fail(CANONICAL_ERROR, `Unsupported JSON value: ${typeof current}`);
    }
    if (types.isProxy(current)) fail(CANONICAL_ERROR, "Proxies are not canonical JSON");
    if (active.has(current)) fail(CANONICAL_ERROR, "Cyclic JSON value");
    const array = Array.isArray(current);
    const prototype = Object.getPrototypeOf(current);
    if (array ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null) {
      fail(CANONICAL_ERROR, "Nonplain object is not canonical JSON");
    }
    active.add(current);
    try {
      const keys = Reflect.ownKeys(current);
      if (array) {
        // Checking the key count first also avoids walking a huge sparse array.
        if (keys.length !== current.length + 1) fail(CANONICAL_ERROR, "Sparse or decorated array");
        for (const key of keys) {
          if (key === "length") continue;
          if (typeof key !== "string") fail(CANONICAL_ERROR, "Symbol array property");
          const index = Number(key);
          if (!Number.isSafeInteger(index) || index < 0 || index >= current.length || String(index) !== key) {
            fail(CANONICAL_ERROR, "Non-index array property");
          }
        }
        const elements: string[] = [];
        for (let index = 0; index < current.length; index++) {
          const descriptor = Object.getOwnPropertyDescriptor(current, String(index));
          if (!descriptor || !("value" in descriptor) || !descriptor.enumerable) {
            fail(CANONICAL_ERROR, "Sparse array or array accessor/nonenumerable element");
          }
          elements.push(encode(descriptor.value));
        }
        return `[${elements.join(",")}]`;
      }
      const fields: string[] = [];
      const names: string[] = [];
      for (const key of keys) {
        if (typeof key !== "string") fail(CANONICAL_ERROR, "Symbol object property");
        assertUnicode(key, CANONICAL_ERROR);
        names.push(key);
      }
      names.sort();
      for (const key of names) {
        const descriptor = Object.getOwnPropertyDescriptor(current, key);
        if (!descriptor || !("value" in descriptor) || !descriptor.enumerable) {
          fail(CANONICAL_ERROR, "Object accessor/nonenumerable property");
        }
        fields.push(`${JSON.stringify(key)}:${encode(descriptor.value)}`);
      }
      return `{${fields.join(",")}}`;
    } finally {
      active.delete(current);
    }
  }
  try {
    return encode(value);
  } catch (error) {
    if (error instanceof EffectContractError) throw error;
    // Includes excessive nesting; never leak an untyped serialization failure.
    throw new EffectContractError(CANONICAL_ERROR, `Cannot encode canonical JSON: ${error instanceof Error ? error.message : "unknown failure"}`);
  }
}

/** Hash actual bytes, or the exact UTF-8 encoding of a valid Unicode string. */
export function sha256Bytes(bytes: string | Uint8Array): string {
  if (typeof bytes === "string") {
    assertUnicode(bytes, CANONICAL_ERROR);
  } else if (types.isProxy(bytes) || !(bytes instanceof Uint8Array)) {
    fail(CANONICAL_ERROR, "SHA-256 input must be a string or Uint8Array");
  }
  try {
    return createHash("sha256").update(bytes).digest("hex");
  } catch {
    fail(CANONICAL_ERROR, "Cannot hash the supplied byte buffer");
  }
}

function validateTuple(tuple: EffectKeyTuple): void {
  canonicalEncode(tuple);
  if (!Array.isArray(tuple) || tuple.length !== 6 || tuple[0] !== 1) {
    fail("EFFECT_KEY_INVALID", "Effect key tuple must be [1, orgId, logicalSessionId, acceptedInputId, effectSlotId, effectKind]");
  }
  for (let index = 1; index < tuple.length; index++) {
    assertString(tuple[index], `keyTuple[${index}]`, "EFFECT_KEY_INVALID");
  }
}

export function mintEffectKey(tuple: EffectKeyTuple): string {
  validateTuple(tuple);
  return sha256Bytes(canonicalEncode(tuple));
}

function validateSlot(slot: EffectSlot, code: string): void {
  assertShape(slot, ["id", "ordinal", "kind", "canonicalRef", "adapterVersion", "canonicalTarget", "predecessors", "canonicalArguments", "constraints"], ["phase"], "slot", code);
  for (const name of ["id", "kind", "canonicalRef", "adapterVersion", "canonicalTarget"] as const) {
    assertString(slot[name], `slot.${name}`, code);
  }
  assertOrdinal(slot.ordinal, "slot.ordinal", code);
  if (Object.hasOwn(slot, "phase")) assertString(slot.phase, "slot.phase", code);
  requireArray(slot.predecessors, "slot.predecessors", code);
  const seen = new Set<string>();
  for (const predecessor of slot.predecessors) {
    assertString(predecessor, "slot.predecessors entry", code);
    if (predecessor === slot.id || seen.has(predecessor)) fail(code, "Duplicate or self predecessor");
    seen.add(predecessor);
  }
}

function validateManifest(manifest: EffectManifest): void {
  canonicalEncode(manifest);
  assertShape(manifest, ["version", "slots"], [], "manifest", MANIFEST_ERROR);
  if (manifest.version !== 1) fail("EFFECT_MANIFEST_VERSION_UNSUPPORTED", "Only effect manifest v1 is supported");
  requireArray(manifest.slots, "manifest.slots", MANIFEST_ERROR);
  const slots = new Map<string, EffectSlot>();
  const ordinals = new Set<number>();
  for (const slot of manifest.slots) {
    validateSlot(slot, MANIFEST_ERROR);
    if (slots.has(slot.id) || ordinals.has(slot.ordinal)) fail(MANIFEST_ERROR, "Manifest slot IDs and ordinals must be unique");
    slots.set(slot.id, slot);
    ordinals.add(slot.ordinal);
  }
  const successors = new Map<string, string[]>();
  const remaining = new Map<string, number>();
  const ready: string[] = [];
  for (const slot of manifest.slots) {
    remaining.set(slot.id, slot.predecessors.length);
    if (slot.predecessors.length === 0) ready.push(slot.id);
    for (const predecessor of slot.predecessors) {
      if (!slots.has(predecessor)) fail(MANIFEST_ERROR, `Unknown predecessor ${predecessor}`);
      const next = successors.get(predecessor) ?? [];
      next.push(slot.id);
      successors.set(predecessor, next);
    }
  }
  for (let index = 0; index < ready.length; index++) {
    for (const successor of successors.get(ready[index]!) ?? []) {
      const count = remaining.get(successor)! - 1;
      remaining.set(successor, count);
      if (count === 0) ready.push(successor);
    }
  }
  if (ready.length !== slots.size) fail(MANIFEST_ERROR, "Manifest predecessor graph contains a cycle");
}

export function manifestDigest(manifest: EffectManifest): string {
  validateManifest(manifest);
  return sha256Bytes(canonicalEncode(manifest));
}

/** Node's decoder alone is permissive (whitespace, URL alphabet, missing padding
 * and nonzero pad bits). Require standard RFC 4648 base64 and an exact roundtrip. */
function validatePayload(payload: EffectPayload, name: string): Buffer {
  assertShape(payload, ["encoding", "bytes", "digest"], [], name, INTENT_ERROR);
  if (payload.encoding !== "base64" || typeof payload.bytes !== "string" || !BASE64.test(payload.bytes)) {
    fail(INTENT_ERROR, `${name} must contain canonical base64`);
  }
  const decoded = Buffer.from(payload.bytes, "base64");
  if (decoded.toString("base64") !== payload.bytes) fail(INTENT_ERROR, `${name} has noncanonical base64 pad bits`);
  assertHash(payload.digest, `${name}.digest`, INTENT_ERROR);
  if (sha256Bytes(decoded) !== payload.digest) fail(INTENT_ERROR, `${name} digest does not match its bytes`);
  return decoded;
}

function validateMode(mode: unknown, name: string): void {
  assertOrdinal(mode, name, INTENT_ERROR);
  if (mode > 0o7777) fail(INTENT_ERROR, `${name} must contain only permission/special mode bits`);
}

/** Validates the standalone binding and all inline content hashes. Full manifest
 * membership/digest verification needs AcceptedInput and is done by the resolver.
 * No adapter registry is imported here: version authority is the admitted manifest. */
export function validateIntent(intent: EffectIntent): void {
  canonicalEncode(intent);
  assertShape(intent, ["format", "version", "key", "keyTuple", "manifestDigest", "acceptedInput", "slot", "tool", "canonicalArguments", "payload", "canonicalTarget", "originAttempt"], ["file", "message", "adapterData"], "intent", INTENT_ERROR);
  if (intent.format !== "fabric.effect-intent" || intent.version !== 1) fail(INTENT_ERROR, "Unsupported effect intent format/version");
  validateTuple(intent.keyTuple);
  assertHash(intent.key, "intent.key", INTENT_ERROR);
  if (mintEffectKey(intent.keyTuple) !== intent.key) fail(INTENT_ERROR, "Effect key does not match its complete tuple");
  assertHash(intent.manifestDigest, "intent.manifestDigest", INTENT_ERROR);
  assertShape(intent.acceptedInput, ["id", "digest", "nativeEntryId"], [], "intent.acceptedInput", INTENT_ERROR);
  assertString(intent.acceptedInput.id, "acceptedInput.id", INTENT_ERROR);
  assertHash(intent.acceptedInput.digest, "acceptedInput.digest", INTENT_ERROR);
  assertString(intent.acceptedInput.nativeEntryId, "acceptedInput.nativeEntryId", INTENT_ERROR);
  validateSlot(intent.slot, INTENT_ERROR);
  if (intent.keyTuple[3] !== intent.acceptedInput.id || intent.keyTuple[4] !== intent.slot.id || intent.keyTuple[5] !== intent.slot.kind) {
    fail(INTENT_ERROR, "Effect tuple does not identify the accepted input and manifest slot");
  }
  assertShape(intent.tool, ["canonicalRef", "adapterVersion", "originalToolCallId", "nativeAssistantEntryId"], ["parentToolCallId"], "intent.tool", INTENT_ERROR);
  for (const name of ["canonicalRef", "adapterVersion", "originalToolCallId", "nativeAssistantEntryId"] as const) {
    assertString(intent.tool[name], `tool.${name}`, INTENT_ERROR);
  }
  if (Object.hasOwn(intent.tool, "parentToolCallId")) assertString(intent.tool.parentToolCallId, "tool.parentToolCallId", INTENT_ERROR);
  if (intent.tool.canonicalRef !== intent.slot.canonicalRef || intent.tool.adapterVersion !== intent.slot.adapterVersion) {
    fail(INTENT_ERROR, "Tool identity/version does not match the manifest slot");
  }
  assertString(intent.canonicalTarget, "intent.canonicalTarget", INTENT_ERROR);
  if (intent.canonicalTarget !== intent.slot.canonicalTarget || canonicalEncode(intent.canonicalArguments) !== canonicalEncode(intent.slot.canonicalArguments)) {
    fail(INTENT_ERROR, "Intent target/arguments do not match the manifest slot");
  }
  validatePayload(intent.payload, "intent.payload");
  assertShape(intent.originAttempt, ["podUID", "bootId", "ownerIncarnation", "authorityRevision"], [], "intent.originAttempt", INTENT_ERROR);
  for (const name of ["podUID", "bootId", "ownerIncarnation", "authorityRevision"] as const) {
    assertString(intent.originAttempt[name], `originAttempt.${name}`, INTENT_ERROR);
  }
  if (Object.hasOwn(intent, "file") && Object.hasOwn(intent, "message")) fail(INTENT_ERROR, "An intent cannot be both a file and a peer effect");
  if (Object.hasOwn(intent, "file")) {
    const file = intent.file;
    assertShape(file, ["preExists", "preHash", "preMode", "postHash", "postMode", "desiredBytes", "preparedResult"], [], "intent.file", INTENT_ERROR);
    if (typeof file.preExists !== "boolean") fail(INTENT_ERROR, "file.preExists must be boolean");
    if (file.preExists) {
      assertHash(file.preHash, "file.preHash", INTENT_ERROR);
      validateMode(file.preMode, "file.preMode");
    } else if (file.preHash !== null || file.preMode !== null) {
      fail(INTENT_ERROR, "An absent pre-file must have null preHash/preMode");
    }
    assertHash(file.postHash, "file.postHash", INTENT_ERROR);
    validateMode(file.postMode, "file.postMode");
    validatePayload(file.desiredBytes, "file.desiredBytes");
    const desired = file.desiredBytes;
    if (file.postHash !== desired.digest || desired.digest !== intent.payload.digest || desired.bytes !== intent.payload.bytes) {
      fail(INTENT_ERROR, "File desired bytes, postHash and intent payload disagree");
    }
  }
  if (Object.hasOwn(intent, "message")) {
    const message = intent.message;
    assertShape(message, ["logicalRecipient", "immutableBody", "bodyDigest", "firstOfferedAt", "originalDeadlineAt", "envelopeIdentity"], [], "intent.message", INTENT_ERROR);
    assertString(message.logicalRecipient, "message.logicalRecipient", INTENT_ERROR);
    if (message.logicalRecipient !== intent.canonicalTarget) fail(INTENT_ERROR, "Peer recipient does not match the canonical target");
    assertHash(message.bodyDigest, "message.bodyDigest", INTENT_ERROR);
    const body = canonicalEncode(message.immutableBody);
    if (sha256Bytes(body) !== message.bodyDigest || message.bodyDigest !== intent.payload.digest || Buffer.from(body, "utf8").toString("base64") !== intent.payload.bytes) {
      fail(INTENT_ERROR, "Peer body, bodyDigest and intent payload disagree");
    }
    assertString(message.firstOfferedAt, "message.firstOfferedAt", INTENT_ERROR);
    assertString(message.originalDeadlineAt, "message.originalDeadlineAt", INTENT_ERROR);
    const offered = Date.parse(message.firstOfferedAt);
    const deadline = Date.parse(message.originalDeadlineAt);
    if (!Number.isFinite(offered) || !Number.isFinite(deadline) || deadline < offered) {
      fail(INTENT_ERROR, "Peer delivery horizon must be finite and preserve deadline order");
    }
  }
}

/** Only declared business fields participate. Native carriers, physical call
 * aliases (including parent IDs), and origin attempt are deliberately excluded.
 * Nested JSON in file/message is business data, not recursively scrubbed. */
export function intentBindingDigest(intent: EffectIntent): string {
  validateIntent(intent);
  return sha256Bytes(canonicalEncode({
    format: intent.format,
    version: intent.version,
    key: intent.key,
    keyTuple: intent.keyTuple,
    manifestDigest: intent.manifestDigest,
    acceptedInput: { id: intent.acceptedInput.id, digest: intent.acceptedInput.digest },
    slot: intent.slot,
    tool: { canonicalRef: intent.tool.canonicalRef, adapterVersion: intent.tool.adapterVersion },
    canonicalArguments: intent.canonicalArguments,
    canonicalTarget: intent.canonicalTarget,
    payload: intent.payload,
    ...(intent.file === undefined ? {} : { file: intent.file }),
    ...(intent.message === undefined ? {} : { message: intent.message }),
    ...(intent.adapterData === undefined ? {} : { adapterData: intent.adapterData }),
  }));
}

function original(record: EffectRecord): NonNullable<EffectContractError["original"]> {
  // ALIAS/ATTEMPT transitions after a saved outcome do not erase that outcome.
  const disposition = [...record.transitions].reverse().find((transition) => transition.state === "RESULT" || transition.state === "UNCERTAIN");
  const state = disposition?.state ?? record.transitions.at(-1)?.state ?? "INTENT";
  return {
    key: record.intent.key,
    state,
    ...(disposition?.result === undefined ? {} : { result: disposition.result }),
  };
}

function conflict(record: EffectRecord, message: string): never {
  throw new EffectContractError("EFFECT_SLOT_CONFLICT", message, original(record));
}

function predecessorCommitted(record: EffectRecord | undefined): boolean {
  if (!record) return false;
  for (let index = record.transitions.length - 1; index >= 0; index--) {
    const transition = record.transitions[index]!;
    if (transition.state === "UNCERTAIN") return false;
    if (transition.state === "RESULT") return transition.outcome === "committed";
  }
  return false;
}

/** Pure, fail-closed selection only: the caller must supply session-scoped records
 * with verified transition chains, serialize binding/dispatch, hold the B3 owner
 * capability and enforce adapter-specific constraints at the commit boundary.
 * Neither ordinal nor any special property inside model arguments is a selector. */
export function resolveEffectSlot({ acceptedInput, kind, canonicalRef, adapterVersion, canonicalTarget, canonicalArguments, phase, records }: {
  acceptedInput: AcceptedInput;
  kind: string;
  canonicalRef: string;
  adapterVersion: string;
  canonicalTarget: string;
  canonicalArguments: JsonValue;
  phase?: string;
  records: readonly EffectRecord[];
}): EffectSlot {
  canonicalEncode(acceptedInput);
  assertShape(acceptedInput, ["id", "digest", "nativeEntryId", "manifestDigest", "manifest"], ["receivedKey", "logicalRecipient", "firstOfferedAt", "originalDeadlineAt", "cancellationRef", "settlementRef"], "acceptedInput", SLOT_ERROR);
  assertString(acceptedInput.id, "acceptedInput.id", SLOT_ERROR);
  assertHash(acceptedInput.digest, "acceptedInput.digest", SLOT_ERROR);
  assertString(acceptedInput.nativeEntryId, "acceptedInput.nativeEntryId", SLOT_ERROR);
  assertHash(acceptedInput.manifestDigest, "acceptedInput.manifestDigest", SLOT_ERROR);
  for (const name of ["receivedKey", "logicalRecipient", "firstOfferedAt", "originalDeadlineAt", "cancellationRef", "settlementRef"] as const) {
    if (Object.hasOwn(acceptedInput, name)) assertString(acceptedInput[name], `acceptedInput.${name}`, SLOT_ERROR);
  }
  if (manifestDigest(acceptedInput.manifest) !== acceptedInput.manifestDigest) fail(MANIFEST_ERROR, "Accepted manifest digest does not match its bytes");
  for (const [name, value] of [["kind", kind], ["canonicalRef", canonicalRef], ["adapterVersion", adapterVersion], ["canonicalTarget", canonicalTarget]] as const) {
    assertString(value, name, SLOT_ERROR);
  }
  if (phase !== undefined) assertString(phase, "trusted phase", SLOT_ERROR);
  const argumentBytes = canonicalEncode(canonicalArguments);
  canonicalEncode(records);
  requireArray(records, "records", SLOT_ERROR);

  const slots = new Map(acceptedInput.manifest.slots.map((slot) => [slot.id, slot]));
  const bound = new Map<string, EffectRecord>();
  let namespace: string | undefined;
  for (const record of records) {
    assertShape(record, ["intent", "intentDigest", "transitions"], [], "record", SLOT_ERROR);
    validateIntent(record.intent);
    requireArray(record.transitions, "record.transitions", SLOT_ERROR);
    // The store, not this selector, owns sequence/previousDigest/digest validation.
    for (const transition of record.transitions) {
      if (transition === null || typeof transition !== "object" || transition.format !== "fabric.effect-transition" || transition.version !== 1 || transition.key !== record.intent.key || !["INTENT", "ATTEMPT", "ALIAS", "RESULT", "UNCERTAIN"].includes(transition.state)) {
        fail(SLOT_ERROR, "Invalid record transition identity/state/version");
      }
      if (transition.state === "RESULT" && !["committed", "not-committed", "uncertain"].includes(transition.outcome ?? "")) {
        fail(SLOT_ERROR, "RESULT requires a definitive outcome or explicit uncertainty");
      }
    }
    assertHash(record.intentDigest, "record.intentDigest", SLOT_ERROR);
    if (sha256Bytes(canonicalEncode(record.intent)) !== record.intentDigest) fail(SLOT_ERROR, "Record intent digest does not match its bytes");
    if (record.intent.acceptedInput.id !== acceptedInput.id) continue;
    if (record.intent.acceptedInput.digest !== acceptedInput.digest || record.intent.manifestDigest !== acceptedInput.manifestDigest) {
      conflict(record, "Accepted input or manifest differs from its historical binding");
    }
    const slot = slots.get(record.intent.slot.id);
    if (!slot || canonicalEncode(slot) !== canonicalEncode(record.intent.slot)) conflict(record, "Historical slot differs from the admitted manifest");
    const recordNamespace = canonicalEncode(record.intent.keyTuple.slice(1, 3));
    if (namespace !== undefined && namespace !== recordNamespace) fail(SLOT_ERROR, "Records span multiple org/session namespaces for this accepted input");
    namespace = recordNamespace;
    if (bound.has(slot.id)) conflict(bound.get(slot.id)!, "Multiple records bind the same manifest slot");
    bound.set(slot.id, record);
  }

  const sameOperation = (slot: EffectSlot): boolean => slot.kind === kind && slot.canonicalRef === canonicalRef && slot.phase === phase;
  const sameRequest = (slot: EffectSlot): boolean => sameOperation(slot) && slot.adapterVersion === adapterVersion && slot.canonicalTarget === canonicalTarget && canonicalEncode(slot.canonicalArguments) === argumentBytes;

  // Historical exact requests win BEFORE looking at any unbound slot or its
  // predecessors. In particular a later file RESULT never remints an old write.
  const historical = [...bound.values()].filter((record) => sameRequest(record.intent.slot));
  if (historical.length > 1) fail("EFFECT_SLOT_AMBIGUOUS", "Several historical bindings match; a trusted manifest phase is required");
  if (historical.length === 1) return slots.get(historical[0]!.intent.slot.id)!;

  const operations = acceptedInput.manifest.slots.filter(sameOperation);
  if (operations.length > 0 && !operations.some((slot) => slot.adapterVersion === adapterVersion)) {
    fail("EFFECT_ADAPTER_VERSION_UNSUPPORTED", "Adapter version is not authorized by the admitted manifest");
  }
  const candidates = operations.filter(sameRequest);
  if (candidates.length > 1) fail("EFFECT_SLOT_AMBIGUOUS", "Indistinguishable manifest slots require a trusted phase; completion/ordinal is not authority");
  if (candidates.length === 0) {
    const family = [...bound.values()].filter((record) => sameOperation(record.intent.slot) && record.intent.slot.adapterVersion === adapterVersion);
    const sameTarget = family.filter((record) => record.intent.canonicalTarget === canonicalTarget);
    const originals = sameTarget.length > 0 ? sameTarget : family;
    if (originals.length === 1) conflict(originals[0]!, "Changed request for a bound effect slot");
    if (originals.length > 1) fail("EFFECT_SLOT_AMBIGUOUS", "Changed request does not uniquely identify an original bound slot");
    fail("EFFECT_SLOT_NOT_FOUND", "Request/target/phase is not authorized by the admitted manifest");
  }
  const slot = candidates[0]!;
  const existing = bound.get(slot.id);
  if (existing) conflict(existing, "Changed request for a bound effect slot");
  if (acceptedInput.cancellationRef !== undefined || acceptedInput.settlementRef !== undefined) {
    fail("EFFECT_INPUT_CLOSED", "A cancelled or settled input cannot authorize an unstarted effect slot");
  }
  for (const predecessor of slot.predecessors) {
    const record = bound.get(predecessor);
    if (!predecessorCommitted(record)) {
      throw new EffectContractError("EFFECT_SLOT_PREDECESSOR", `Slot ${slot.id} requires committed predecessor ${predecessor}`, record === undefined ? undefined : original(record));
    }
  }
  return slot;
}
