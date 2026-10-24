import type { MeshEvent } from "../mesh/event-log.js";
import type { FabricMessageSender } from "../protocol.js";
import type { FabricActorActivation } from "./types.js";
type MeshActivationMetadata = Omit<Extract<FabricActorActivation, { kind: "mesh" }>, "data">;

/** Bounded newest-event metadata; older identities survive as an untrusted range. */
export interface CoalescedMeshEvent {
  id: string;
  sequence: number;
  topic: string;
  kind: string;
  event?: string;
  action?: string;
  from?: MeshEvent["from"];
  sender?: FabricMessageSender;
  principal?: MeshEvent["principal"];
  /** Original external provenance survives without a full payload. */
  origin?: "external";
  /** Original external input or conflicting attribution; rendering also checks the sender. */
  untrusted?: true;
  payload?: MeshEvent;
  dataOmitted?: true;
  metadataTruncated?: true;
  /** Genuine actor activation facts, not a mesh-sequence-derived substitute. */
  activation?: MeshActivationMetadata;
  /** Distinct omitted events (a lower bound for overlapping sparse histories). */
  omittedBefore?: { count: number; firstSequence: number; lastSequence: number; countIsLowerBound?: true };
}

interface CoalescedEventSource {
  payload: unknown;
  sender?: FabricMessageSender | undefined;
  provenance?: { principal?: MeshEvent["principal"] } | undefined;
  coalesced?: unknown;
  activation?: FabricActorActivation | undefined;
}

const MAX_FULL_EVENTS = 50;
const MAX_FULL_EVENT_BYTES = 64 * 1024;
const MAX_METADATA_RECORDS = 100;
const MAX_METADATA_BYTES = 64 * 1024;
const MAX_METADATA_STRING = 256;
const TRUNCATION_MARKER = "…[truncated]";
type Omitted = NonNullable<CoalescedMeshEvent["omittedBefore"]>;
// Only our own immutable output is cached. Arbitrary checkpoint/input objects may
// change; never give them an unchecked fast path or cache their authority.
const normalized = new WeakSet<CoalescedMeshEvent>();
const ownedPayloads = new WeakSet<MeshEvent>();
const payloadBytes = new WeakMap<MeshEvent, number>();
const object = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const identity = (value: unknown): value is Pick<MeshEvent, "id" | "sequence" | "topic" | "kind"> => {
  const event = object(value);
  return typeof event?.id === "string" && event.id.length > 0 &&
    typeof event.sequence === "number" && Number.isSafeInteger(event.sequence) && event.sequence >= 0 &&
    typeof event.topic === "string" && typeof event.kind === "string";
};
const jsonBytes = (value: unknown): number => {
  try {
    const json = JSON.stringify(value);
    return json === undefined ? Infinity : Buffer.byteLength(json, "utf8");
  } catch { return Infinity; }
};

/** Reject an entire oversized attribution, never turn a truncated prefix into authority. */
const boundedAttribution = <T>(value: T): T | undefined => {
  let nodes = 0;
  const fits = (part: unknown, depth: number): boolean => {
    if (++nodes > 128 || depth > 6) return false;
    if (typeof part === "string") return part.length <= MAX_METADATA_STRING;
    if (part === null || typeof part === "boolean" || typeof part === "number" || part === undefined) return true;
    if (Array.isArray(part)) return part.length <= 64 && part.every(item => fits(item, depth + 1));
    const record = object(part);
    if (!record) return false;
    const keys = Object.keys(record);
    return keys.length <= 16 && keys.every(key => key.length <= MAX_METADATA_STRING && fits(record[key], depth + 1));
  };
  return fits(value, 0) && jsonBytes(value) <= 4 * 1024 ? structuredClone(value) : undefined;
};

const boundedRecord = (record: CoalescedMeshEvent): CoalescedMeshEvent => {
  let changed = record.metadataTruncated === true;
  const text = (value: string): string => {
    if (value.length <= MAX_METADATA_STRING) return value;
    changed = true;
    return value.slice(0, MAX_METADATA_STRING - TRUNCATION_MARKER.length) + TRUNCATION_MARKER;
  };
  // These are conspicuous non-identities, not prefixes that could pass an exact
  // read-back check. The primary MeshEvent is never modified. Read by sequence
  // alone when either identity field was omitted.
  const id = record.id.length <= MAX_METADATA_STRING ? record.id
    : (changed = true, `⟪event id omitted; sequence ${record.sequence}⟫`);
  const topic = record.topic.length <= MAX_METADATA_STRING ? record.topic
    : (changed = true, "⟪topic omitted; read by sequence only⟫");
  const result: CoalescedMeshEvent = {
    id, sequence: record.sequence, topic, kind: text(record.kind),
    ...(record.event !== undefined ? { event: text(record.event) } : {}),
    ...(record.action !== undefined ? { action: text(record.action) } : {}),
    ...(record.origin === "external" ? { origin: "external" } : {}),
    ...(record.untrusted ? { untrusted: true } : {}),
    ...(record.payload ? { payload: record.payload } : { dataOmitted: true }),
  };
  for (const key of ["from", "sender", "principal", "activation"] as const) {
    if (record[key] === undefined) continue;
    const value = boundedAttribution(record[key]);
    if (value === undefined) changed = true;
    else Object.assign(result, { [key]: value });
  }
  if (changed) {
    result.metadataTruncated = true;
    result.untrusted = true;
    delete result.sender;
    delete result.principal;
    delete result.payload;
    result.dataOmitted = true;
  }
  return result;
};

const original = (source: CoalescedEventSource): CoalescedMeshEvent | undefined => {
  if (!identity(source.payload)) return undefined;
  const event = source.payload as MeshEvent;
  const data = object(event.data);
  const action = object(data?.payload)?.action;
  // Queue lineage may already have been downgraded by a steering input. Never
  // reconstruct its principal from the old payload after that downgrade.
  const principal = source.provenance ? source.provenance.principal
    : event.verification === "mesh" || event.verification === "bridge" ? event.principal : undefined;
  return boundedRecord({
    id: event.id, sequence: event.sequence, topic: event.topic, kind: event.kind,
    ...(typeof data?.event === "string" ? { event: data.event } : {}),
    ...(typeof action === "string" ? { action } : {}),
    ...(source.activation?.kind === "mesh" ? { activation: {
      kind: "mesh", id: source.activation.id, source: source.activation.source,
      sequence: source.activation.sequence, createdAt: source.activation.createdAt, topic: source.activation.topic,
    } } : {}),
    ...(event.from ? { from: event.from } : {}),
    // Queue attribution is authoritative; never borrow another collapsed event's stamp.
    ...(source.sender !== undefined ? { sender: source.sender } : {}),
    ...(principal ? { principal } : {}),
    ...(event.untrusted === true || event.origin === "external" ? { origin: "external", untrusted: true } : {}),
    payload: event,
  });
};

const restored = (value: unknown): CoalescedMeshEvent | undefined => {
  if (!identity(value)) return undefined;
  const record = value as CoalescedMeshEvent;
  if (normalized.has(record)) return record;
  const payload = identity(record.payload) && record.payload.id === record.id &&
    record.payload.sequence === record.sequence && record.payload.topic === record.topic && record.payload.kind === record.kind
    ? record.payload : undefined;
  const data = object(payload?.data);
  const event = record.event ?? data?.event;
  const action = record.action ?? object(data?.payload)?.action;
  return boundedRecord({
    id: record.id, sequence: record.sequence, topic: record.topic, kind: record.kind,
    ...(typeof event === "string" ? { event } : {}),
    ...(typeof action === "string" ? { action } : {}),
    ...(record.from ? { from: record.from } : {}),
    ...(record.sender !== undefined ? { sender: record.sender } : {}),
    ...(record.principal ? { principal: record.principal } : {}),
    ...(record.metadataTruncated ? { metadataTruncated: true } : {}),
    ...(record.activation ? { activation: record.activation } : {}),
    ...(record.origin === "external" || payload?.untrusted === true ? { origin: "external", untrusted: true }
      : record.untrusted === true ? { untrusted: true } : {}),
    ...(payload ? { payload } : { dataOmitted: true }),
  });
};

const omittedRange = (value: unknown): Omitted | undefined => {
  const range = object(value);
  if (!range) return undefined;
  const valid = (n: unknown): n is number => typeof n === "number" && Number.isSafeInteger(n) && n >= 0;
  return valid(range.count) && range.count > 0 && valid(range.firstSequence) && valid(range.lastSequence) &&
    range.firstSequence <= range.lastSequence
    ? { count: Math.min(range.count, range.lastSequence - range.firstSequence + 1),
      firstSequence: range.firstSequence, lastSequence: range.lastSequence,
      ...(range.countIsLowerBound === true ? { countIsLowerBound: true } : {}) } : undefined;
};

/** Exact for disjoint ranges and append bursts; overlapping sparse ranges give
 * a conservative lower bound, not double counting and not an unbounded ID ledger. */
const unionOmitted = (a: Omitted | undefined, b: Omitted, sameCheckpoint = false): Omitted => {
  if (!a) return { ...b };
  const overlap = Math.max(0, Math.min(a.lastSequence, b.lastSequence) - Math.max(a.firstSequence, b.firstSequence) + 1);
  const identical = a.count === b.count && a.firstSequence === b.firstSequence && a.lastSequence === b.lastSequence;
  const aDense = a.count === a.lastSequence - a.firstSequence + 1;
  const bDense = b.count === b.lastSequence - b.firstSequence + 1;
  const knownUnion = aDense && bDense || aDense && a.firstSequence <= b.firstSequence && a.lastSequence >= b.lastSequence ||
    bDense && b.firstSequence <= a.firstSequence && b.lastSequence >= a.lastSequence;
  return { count: Math.min(Number.MAX_SAFE_INTEGER, a.count + b.count - Math.min(a.count, b.count, overlap)),
    firstSequence: Math.min(a.firstSequence, b.firstSequence), lastSequence: Math.max(a.lastSequence, b.lastSequence),
    ...(a.countIsLowerBound || b.countIsLowerBound || overlap > 0 && !(identical && sameCheckpoint) && !knownUnion
      ? { countIsLowerBound: true } : {}) };
};
const metadataOnly = (record: CoalescedMeshEvent): Omit<CoalescedMeshEvent, "payload"> => {
  const { payload: _payload, ...metadata } = record;
  return metadata;
};
// Contribution to JSON.stringify(metadataArray, null, 2), including indentation
// and separator. Reserve a possible actor-specific render-time untrusted marker
// too, so restored foreign stamps cannot push a full metadata budget over its cap.
// Array brackets cost two more bytes.
const metadataCost = (record: CoalescedMeshEvent): number => {
  const json = JSON.stringify({ ...metadataOnly(record), untrusted: true }, null, 2);
  let lines = 1;
  for (let i = 0; i < json.length; i++) if (json.charCodeAt(i) === 10) lines++;
  return Buffer.byteLength(json, "utf8") + 2 * lines + 2;
};

/**
 * Union originals and any already-collapsed histories, by event ID and mesh sequence.
 * Never mutate an input array/record: enqueue checkpoint rollback keeps shallow snapshots.
 *
 * Full-event copies (including the unchanged primary outside this array) share 50 slots
 * and 64 KiB of JSON. An oversized primary remains compatible, but leaves no history
 * payload budget. Metadata retains at most the newest 100 records and 64 KiB of
 * rendered JSON (excluding only full-event payloads). Older events become an
 * untrusted sequence-range summary. All processing on subsequent merges is bounded.
 */
export const mergeCoalescedMeshEvents = (primary: unknown, ...sources: CoalescedEventSource[]): CoalescedMeshEvent[] => {
  const byId = new Map<string, CoalescedMeshEvent>();
  let omitted: Omitted | undefined;
  let omittedExternal = false;
  // Equal three-number sparse summaries can conceal divergent members. Only
  // identical represented checkpoints justify treating that overlap as exact.
  // This transient comparison is used only on duplicate summary merges, never
  // for the ordinary one-summary append path; no persistent identity ledger.
  const summarizedSources = new Map<string, unknown[]>();
  const sameHistory = (a: unknown, b: unknown): boolean => {
    if (a === b) return true;
    try { return JSON.stringify(a) === JSON.stringify(b); } catch { return false; }
  };
  const add = (record: CoalescedMeshEvent | undefined): void => {
    if (!record) return;
    const prior = byId.get(record.id);
    if (!prior) { byId.set(record.id, record); return; }
    // A duplicate may restore a previously omitted full payload, but can never wash
    // away an untrusted flag or disagreement in its original authority metadata.
    const disputed = JSON.stringify(prior.sender) !== JSON.stringify(record.sender) ||
      JSON.stringify(prior.principal) !== JSON.stringify(record.principal);
    const payload = prior.payload ?? record.payload;
    const merged: CoalescedMeshEvent = {
      ...record, ...prior,
      ...(prior.origin === "external" || record.origin === "external" ? { origin: "external" } : {}),
      ...(prior.untrusted || record.untrusted || disputed ? { untrusted: true } : {}),
      ...(prior.metadataTruncated || record.metadataTruncated ? { metadataTruncated: true } : {}),
      ...(payload ? { payload } : {}),
    };
    if (disputed || merged.metadataTruncated || prior.omittedBefore || record.omittedBefore) {
      delete merged.sender;
      delete merged.principal;
      delete merged.payload;
      merged.dataOmitted = true;
    }
    byId.set(record.id, merged);
  };
  for (const source of sources) {
    if (Array.isArray(source.coalesced)) for (const value of source.coalesced) {
      const record = restored(value);
      if (!record) continue;
      const range = omittedRange(object(value)?.omittedBefore);
      if (range && range.lastSequence < record.sequence) {
        const key = `${range.firstSequence}:${range.lastSequence}:${range.count}`;
        const histories = summarizedSources.get(key) ?? [];
        const duplicate = histories.some(history => sameHistory(history, source.coalesced));
        omitted = unionOmitted(omitted, range, duplicate);
        histories.push(source.coalesced);
        summarizedSources.set(key, histories);
        omittedExternal ||= record.origin === "external";
      }
      add(record);
    }
    add(original(source));
  }
  const records = [...byId.values()].sort((a, b) => a.sequence - b.sequence || a.id.localeCompare(b.id));
  const omit = (record: CoalescedMeshEvent): void => {
    omitted = unionOmitted(omitted, { count: 1, firstSequence: record.sequence, lastSequence: record.sequence });
    omittedExternal ||= record.origin === "external";
  };
  let start = 0;
  // A summarized prefix cannot be resurrected by replaying an old original or a
  // second copy of its checkpoint. Range counts never add covered sequences twice.
  while (start < records.length - 1 && omitted && records[start]!.sequence <= omitted.lastSequence) omit(records[start++]!);
  while (records.length - start > MAX_METADATA_RECORDS) omit(records[start++]!);

  let count = 1;
  let bytes = jsonBytes(primary);
  const bounded = records.map(record => {
    const { payload: _payload, dataOmitted: _omitted, omittedBefore: _range, ...metadata } = record;
    return { ...metadata, dataOmitted: true } as CoalescedMeshEvent;
  });
  // Prefer the newest whole full-event copies. Reuse only copies already owned
  // by this module, instead of deep-cloning up to 49 payloads on every admission.
  for (let index = records.length - 1; index >= start; index--) {
    const payload = records[index]!.payload;
    if (!payload || count >= MAX_FULL_EVENTS || records[index]!.metadataTruncated) continue;
    const size = payloadBytes.get(payload) ?? jsonBytes(payload);
    if (bytes + size > MAX_FULL_EVENT_BYTES) continue;
    const record = bounded[index]!;
    delete record.dataOmitted;
    const copy = ownedPayloads.has(payload) ? payload : structuredClone(payload);
    ownedPayloads.add(copy);
    payloadBytes.set(copy, size);
    record.payload = copy;
    count++;
    bytes += size;
  }
  const carrier = (record: CoalescedMeshEvent): CoalescedMeshEvent => {
    if (!omitted) return record;
    const { sender: _sender, principal: _principal, payload: _payload, ...metadata } = record;
    return { ...metadata, omittedBefore: { ...omitted }, untrusted: true, dataOmitted: true,
      ...(omittedExternal ? { origin: "external" } : {}) };
  };
  const costs = bounded.map(metadataCost);
  let total = 2;
  for (let index = start; index < bounded.length; index++) total += costs[index]!;
  while (start < bounded.length - 1 && total - costs[start]! + metadataCost(carrier(bounded[start]!)) > MAX_METADATA_BYTES) {
    total -= costs[start]!;
    omit(records[start++]!);
  }
  const result = bounded.slice(start);
  if (result.length) result[0] = carrier(result[0]!);
  for (const record of result) normalized.add(record);
  return result;
};

/** Whether an immutable activation already captured every identity and its attribution. */
export const coalescedMeshHistoryCaptured = (current: CoalescedEventSource, incoming: CoalescedEventSource): boolean => {
  const metadata = (records: CoalescedMeshEvent[]): string => JSON.stringify(records.map(record => [
    record.id, record.sequence, record.topic, record.kind, record.event, record.action,
    record.from, record.sender, record.principal, record.origin, record.untrusted,
    record.metadataTruncated, record.omittedBefore, record.activation,
  ]));
  return metadata(mergeCoalescedMeshEvents(current.payload, current)) ===
    metadata(mergeCoalescedMeshEvents(current.payload, current, incoming));
};

/** Legacy unmerged items stay unmerged; present histories are validated and re-bounded. */
export const restoreCoalescedMeshEvents = (source: CoalescedEventSource): CoalescedMeshEvent[] | undefined =>
  Array.isArray(source.coalesced) && source.coalesced.length > 0
    ? mergeCoalescedMeshEvents(source.payload, source) : undefined;
