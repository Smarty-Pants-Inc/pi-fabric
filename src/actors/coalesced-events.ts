import type { MeshEvent } from "../mesh/event-log.js";
import type { FabricMessageSender } from "../protocol.js";

/** Every collapsed identity survives, even when its full event no longer fits. */
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
}

interface CoalescedEventSource {
  payload: unknown;
  sender?: FabricMessageSender | undefined;
  provenance?: { principal?: MeshEvent["principal"] } | undefined;
  coalesced?: unknown;
}

const MAX_FULL_EVENTS = 50;
const MAX_FULL_EVENT_BYTES = 64 * 1024;
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

const original = (source: CoalescedEventSource): CoalescedMeshEvent | undefined => {
  if (!identity(source.payload)) return undefined;
  const event = source.payload as MeshEvent;
  const data = object(event.data);
  const action = object(data?.payload)?.action;
  // Queue lineage may already have been downgraded by a steering input. Never
  // reconstruct its principal from the old payload after that downgrade.
  const principal = source.provenance ? source.provenance.principal
    : event.verification === "mesh" || event.verification === "bridge" ? event.principal : undefined;
  return {
    id: event.id, sequence: event.sequence, topic: event.topic, kind: event.kind,
    ...(typeof data?.event === "string" ? { event: data.event } : {}),
    ...(typeof action === "string" ? { action } : {}),
    ...(event.from ? { from: structuredClone(event.from) } : {}),
    // Queue attribution is authoritative; never borrow another collapsed event's stamp.
    ...(source.sender !== undefined ? { sender: structuredClone(source.sender) } : {}),
    ...(principal ? { principal: structuredClone(principal) } : {}),
    ...(event.untrusted === true || event.origin === "external" ? { origin: "external", untrusted: true } : {}),
    payload: event,
  };
};

const restored = (value: unknown): CoalescedMeshEvent | undefined => {
  if (!identity(value)) return undefined;
  const record = value as CoalescedMeshEvent;
  const payload = identity(record.payload) && record.payload.id === record.id &&
    record.payload.sequence === record.sequence && record.payload.topic === record.topic && record.payload.kind === record.kind
    ? record.payload : undefined;
  const data = object(payload?.data);
  const event = record.event ?? data?.event;
  const action = record.action ?? object(data?.payload)?.action;
  return {
    id: record.id, sequence: record.sequence, topic: record.topic, kind: record.kind,
    ...(typeof event === "string" ? { event } : {}),
    ...(typeof action === "string" ? { action } : {}),
    ...(record.from ? { from: structuredClone(record.from) } : {}),
    ...(record.sender !== undefined ? { sender: structuredClone(record.sender) } : {}),
    ...(record.principal ? { principal: structuredClone(record.principal) } : {}),
    ...(record.origin === "external" || payload?.untrusted === true ? { origin: "external", untrusted: true }
      : record.untrusted === true ? { untrusted: true } : {}),
    ...(payload ? { payload } : { dataOmitted: true }),
  };
};

/**
 * Union originals and any already-collapsed histories, by event ID and mesh sequence.
 * Never mutate an input array/record: enqueue checkpoint rollback keeps shallow snapshots.
 *
 * Full-event copies (including the unchanged primary outside this array) share 50 slots
 * and 64 KiB of JSON. An oversized primary remains compatible, but leaves no history
 * payload budget. Metadata is intentionally linear and is never count-truncated.
 */
export const mergeCoalescedMeshEvents = (primary: unknown, ...sources: CoalescedEventSource[]): CoalescedMeshEvent[] => {
  const byId = new Map<string, CoalescedMeshEvent>();
  const add = (record: CoalescedMeshEvent | undefined): void => {
    if (!record) return;
    const prior = byId.get(record.id);
    if (!prior) { byId.set(record.id, record); return; }
    // A duplicate may restore a previously omitted full payload, but can never wash
    // away an untrusted flag or disagreement in its original authority metadata.
    const disputed = JSON.stringify(prior.sender) !== JSON.stringify(record.sender) ||
      JSON.stringify(prior.principal) !== JSON.stringify(record.principal);
    const payload = prior.payload ?? record.payload;
    byId.set(record.id, {
      ...record, ...prior,
      ...(prior.origin === "external" || record.origin === "external" ? { origin: "external" } : {}),
      ...(prior.untrusted || record.untrusted || disputed ? { untrusted: true } : {}),
      ...(payload ? { payload } : {}),
    });
  };
  for (const source of sources) {
    if (Array.isArray(source.coalesced)) for (const record of source.coalesced) add(restored(record));
    add(original(source));
  }
  const records = [...byId.values()].sort((a, b) => a.sequence - b.sequence || a.id.localeCompare(b.id));
  let count = 1;
  let bytes = jsonBytes(primary);
  // Prefer the newest full events. Missing older events remain individually readable
  // through mesh.read({ after: record.sequence - 1, topic: record.topic, limit: 1 }).
  const bounded = records.map(record => {
    const { payload: _payload, dataOmitted: _omitted, ...metadata } = record;
    return { ...metadata, dataOmitted: true } as CoalescedMeshEvent;
  });
  for (let index = records.length - 1; index >= 0; index--) {
    const payload = records[index]!.payload;
    if (!payload || count >= MAX_FULL_EVENTS) continue;
    const size = jsonBytes(payload);
    if (bytes + size > MAX_FULL_EVENT_BYTES) continue;
    const record = bounded[index]!;
    delete record.dataOmitted;
    record.payload = structuredClone(payload);
    count++;
    bytes += size;
  }
  return bounded;
};

/** Whether an immutable activation already captured every identity and its attribution. */
export const coalescedMeshHistoryCaptured = (current: CoalescedEventSource, incoming: CoalescedEventSource): boolean => {
  const metadata = (records: CoalescedMeshEvent[]): string => JSON.stringify(records.map(record => [
    record.id, record.sequence, record.topic, record.kind, record.event, record.action,
    record.from, record.sender, record.principal, record.origin, record.untrusted,
  ]));
  return metadata(mergeCoalescedMeshEvents(current.payload, current)) ===
    metadata(mergeCoalescedMeshEvents(current.payload, current, incoming));
};

/** Legacy unmerged items stay unmerged; present histories are validated and re-bounded. */
export const restoreCoalescedMeshEvents = (source: CoalescedEventSource): CoalescedMeshEvent[] | undefined =>
  Array.isArray(source.coalesced) && source.coalesced.length > 0
    ? mergeCoalescedMeshEvents(source.payload, source) : undefined;
