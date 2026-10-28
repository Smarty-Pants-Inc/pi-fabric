import { createHash } from "node:crypto";
import type { RecoveryJournal, RecoveryJournalRecord } from "./recovery-journal.js";

/** Separate from native carrier/barrier keys; this log is evidence, not an effect deduper. */
export const RECOVERY_ATTEMPT_NAMESPACE = "recovery-attempt:v1:";

export interface RecoveryEvidenceBytes {
  readonly bytes: Uint8Array;
  /** Lower-case SHA-256 of exactly bytes, without normalization or a trailing newline. */
  readonly sha256: string;
}

export interface RecoveryAttemptIdentity {
  readonly sessionId: string;
  readonly acceptedInputId: string;
  readonly toolCallId: string;
  readonly effectKey: string;
  /** A new ID AND ordinal for each physical try, including identical same-key retries. */
  readonly attemptId: string;
  readonly attemptNumber: number;
  readonly hostId: string;
  readonly podId: string;
  readonly bootId: string;
  readonly ownerIncarnation: string;
  readonly authorityRevision: string;
}

export interface RecoveryAttemptTraceReference {
  readonly traceId: string;
  readonly storage: {
    readonly storeId: string;
    readonly objectKey: string;
    readonly writerHostId: string;
    /** Actual trace-store commit/provenance reference supplied by its owner, not a filename. */
    readonly receipt: string;
  };
  /** Always the entire raw captured prefix, including an interrupted/invalid final line. */
  readonly retainedFrom: 0;
  readonly byteLength: number;
  readonly sha256: string;
  /** Retained exact raw bytes, not a parsed transcript or only a hash/path. */
  readonly bytes: Uint8Array;
  readonly interrupted: boolean;
}

export interface RecoveryAttemptRequest {
  readonly identity: RecoveryAttemptIdentity;
  readonly toolVersion: string;
  readonly intent: RecoveryEvidenceBytes;
  readonly payload: RecoveryEvidenceBytes;
  readonly destination: { readonly kind: string; readonly target: string };
  readonly observedAt: number;
  readonly traces: readonly RecoveryAttemptTraceReference[];
}

export type RecoveryAttemptState = "intent" | "dispatched" | "committed" | "not-committed" | "uncertain" | "unsupported" | "failed";

export interface RecoveryAttemptEventRequest {
  readonly identity: RecoveryAttemptIdentity;
  /** Stable only for reconciliation of THIS event, never reused for a different observation. */
  readonly eventId: string;
  readonly observedAt: number;
  readonly state: RecoveryAttemptState;
  readonly result?: RecoveryEvidenceBytes;
  readonly receipt?: RecoveryEvidenceBytes;
  readonly traces: readonly RecoveryAttemptTraceReference[];
}

export interface RecoveryAttemptRecord extends RecoveryAttemptRequest {
  readonly kind: "attempt";
  readonly state: "intent";
  readonly seq: number;
}

export interface RecoveryAttemptEvent extends RecoveryAttemptEventRequest {
  readonly kind: "event";
  readonly seq: number;
}

export type RecoveryAttemptRecordOrEvent = RecoveryAttemptRecord | RecoveryAttemptEvent;
export interface RecoveryAttemptHistory {
  readonly attempt: RecoveryAttemptRecord;
  /** No latest-only projection: every observation/result/receipt remains independently readable. */
  readonly events: readonly RecoveryAttemptEvent[];
}
export interface RecoveryAttemptAppendOptions { readonly deadlineMs: number; readonly signal?: AbortSignal }
export interface RecoveryAttemptAppendReceipt { readonly seq: number; readonly existing: boolean }
export interface RecoveryAttemptLedger {
  recordAttempt(request: RecoveryAttemptRequest, options: RecoveryAttemptAppendOptions): Promise<RecoveryAttemptAppendReceipt>;
  appendEvent(request: RecoveryAttemptEventRequest, options: RecoveryAttemptAppendOptions): Promise<RecoveryAttemptAppendReceipt>;
  readAttempt(sessionId: string, attemptId: string): RecoveryAttemptHistory | undefined;
  entries(): readonly RecoveryAttemptRecordOrEvent[];
}

export class RecoveryAttemptLedgerError extends Error {
  constructor(readonly reason: "invalid-evidence" | "conflict" | "not-found" | "corrupt", message: string) {
    super(message);
    this.name = "RecoveryAttemptLedgerError";
  }
}

const fail = (message: string): never => { throw new RecoveryAttemptLedgerError("invalid-evidence", message); };
const text = (value: string, name: string): string => typeof value === "string" && value.trim().length > 0 ? value : fail(`Missing ${name}`);
const digest = (value: string): string => /^[a-f0-9]{64}$/.test(value) ? value : fail("Invalid SHA-256");
const integer = (value: number, name: string, minimum = 0): number => Number.isSafeInteger(value) && value >= minimum ? value : fail(`Invalid ${name}`);
const timestamp = (value: number): number => Number.isFinite(value) && value >= 0 ? value : fail("Invalid observation time");
const hash = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const same = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);

function evidence(value: RecoveryEvidenceBytes): RecoveryEvidenceBytes {
  if (!value || !(value.bytes instanceof Uint8Array)) fail("Evidence requires exact bytes");
  const bytes = Uint8Array.from(value.bytes), sha256 = digest(value.sha256);
  if (hash(bytes) !== sha256) fail("Evidence digest mismatch");
  return { bytes, sha256 };
}

function identity(value: RecoveryAttemptIdentity): RecoveryAttemptIdentity {
  return {
    sessionId: text(value.sessionId, "sessionId"), acceptedInputId: text(value.acceptedInputId, "acceptedInputId"),
    toolCallId: text(value.toolCallId, "toolCallId"), effectKey: text(value.effectKey, "effectKey"),
    attemptId: text(value.attemptId, "attemptId"), attemptNumber: integer(value.attemptNumber, "attemptNumber", 1),
    hostId: text(value.hostId, "hostId"), podId: text(value.podId, "podId"), bootId: text(value.bootId, "bootId"),
    ownerIncarnation: text(value.ownerIncarnation, "ownerIncarnation"), authorityRevision: text(value.authorityRevision, "authorityRevision"),
  };
}

function traces(values: readonly RecoveryAttemptTraceReference[]): readonly RecoveryAttemptTraceReference[] {
  if (!Array.isArray(values)) fail("Trace references required");
  const ids = new Set<string>();
  return values.map(value => {
    const traceId = text(value.traceId, "traceId");
    if (ids.has(traceId)) fail("Duplicate trace reference in one observation");
    ids.add(traceId);
    if (value.retainedFrom !== 0 || typeof value.interrupted !== "boolean") fail("Full raw trace prefix required");
    const raw = evidence(value);
    if (raw.bytes.length !== value.byteLength) fail("Raw trace byte length mismatch");
    return { traceId, storage: {
      storeId: text(value.storage.storeId, "trace storeId"), objectKey: text(value.storage.objectKey, "trace objectKey"),
      writerHostId: text(value.storage.writerHostId, "trace writerHostId"), receipt: text(value.storage.receipt, "trace storage receipt"),
    }, retainedFrom: 0, byteLength: integer(value.byteLength, "trace byteLength"), ...raw, interrupted: value.interrupted };
  });
}

function attempt(value: RecoveryAttemptRequest): RecoveryAttemptRequest {
  const copiedTraces = traces(value.traces);
  if (copiedTraces.length === 0) fail("Attempt requires a raw trace storage reference");
  return { identity: identity(value.identity), toolVersion: text(value.toolVersion, "toolVersion"), intent: evidence(value.intent),
    payload: evidence(value.payload), destination: { kind: text(value.destination.kind, "destination kind"), target: text(value.destination.target, "destination target") },
    observedAt: timestamp(value.observedAt), traces: copiedTraces };
}

function event(value: RecoveryAttemptEventRequest): RecoveryAttemptEventRequest {
  if (!["intent", "dispatched", "committed", "not-committed", "uncertain", "unsupported", "failed"].includes(value.state)) fail("Invalid attempt state");
  if (value.state === "committed" && !value.result && !value.receipt) fail("Committed observation requires exact result or receipt");
  if (value.state === "committed" && value.traces.length === 0) fail("Committed observation requires a retained raw trace prefix");
  return { identity: identity(value.identity), eventId: text(value.eventId, "eventId"), observedAt: timestamp(value.observedAt), state: value.state,
    ...(value.result ? { result: evidence(value.result) } : {}), ...(value.receipt ? { receipt: evidence(value.receipt) } : {}), traces: traces(value.traces) };
}

const attemptKey = (id: RecoveryAttemptIdentity): string => RECOVERY_ATTEMPT_NAMESPACE + JSON.stringify([id.sessionId, id.attemptId, "attempt"]);
const eventKey = (value: RecoveryAttemptEventRequest): string => RECOVERY_ATTEMPT_NAMESPACE + JSON.stringify([value.identity.sessionId, value.identity.attemptId, "event", value.eventId]);
const packed = (value: RecoveryEvidenceBytes) => ({ base64: Buffer.from(value.bytes).toString("base64"), sha256: value.sha256 });
function encoded(kind: "attempt" | "event", value: RecoveryAttemptRequest | RecoveryAttemptEventRequest): Uint8Array {
  const body = kind === "attempt"
    ? { ...value, intent: packed((value as RecoveryAttemptRequest).intent), payload: packed((value as RecoveryAttemptRequest).payload), state: "intent" }
    : { ...value, ...("result" in value && value.result ? { result: packed(value.result) } : {}), ...("receipt" in value && value.receipt ? { receipt: packed(value.receipt) } : {}) };
  return Buffer.from(JSON.stringify({ version: 1, kind, ...body, traces: value.traces.map(ref => ({ ...ref, bytes: Buffer.from(ref.bytes).toString("base64") })) }), "utf8");
}

function unpacked(value: { base64: string; sha256: string }): RecoveryEvidenceBytes {
  if (!value || typeof value.base64 !== "string" || Buffer.from(value.base64, "base64").toString("base64") !== value.base64) fail("Invalid evidence encoding");
  return evidence({ bytes: Buffer.from(value.base64, "base64"), sha256: value.sha256 });
}

/** Undefined is a different journal namespace. Malformed attempt-domain rows are NEVER skipped. */
export function decodeRecoveryAttemptRecord(record: RecoveryJournalRecord): RecoveryAttemptRecordOrEvent | undefined {
  if (!record.key.startsWith(RECOVERY_ATTEMPT_NAMESPACE)) return undefined;
  try {
    const value = JSON.parse(Buffer.from(record.bytes).toString("utf8"));
    integer(record.seq, "journal sequence");
    if (value.version !== 1 || (value.kind !== "attempt" && value.kind !== "event")) fail("Unknown attempt record format");
    if (!Array.isArray(value.traces)) fail("Trace references required");
    const decodedTraces = value.traces.map((ref: RecoveryAttemptTraceReference & { bytes: string }) => ({ ...ref, bytes: unpacked({ base64: ref.bytes, sha256: ref.sha256 }).bytes }));
    const request = value.kind === "attempt"
      ? attempt({ ...value, traces: decodedTraces, intent: unpacked(value.intent), payload: unpacked(value.payload) })
      : event({ ...value, traces: decodedTraces, ...(value.result ? { result: unpacked(value.result) } : {}), ...(value.receipt ? { receipt: unpacked(value.receipt) } : {}) });
    const key = value.kind === "attempt" ? attemptKey(request.identity) : eventKey(request as RecoveryAttemptEventRequest);
    if (key !== record.key || !Buffer.from(encoded(value.kind, request)).equals(Buffer.from(record.bytes))) fail("Noncanonical or conflicting attempt record");
    return value.kind === "attempt"
      ? { kind: "attempt", state: "intent", seq: record.seq, ...request as RecoveryAttemptRequest }
      : { kind: "event", seq: record.seq, ...request as RecoveryAttemptEventRequest };
  } catch (error) {
    throw new RecoveryAttemptLedgerError("corrupt", `Invalid retained attempt record ${record.key}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

const binding = (value: RecoveryAttemptRequest) => ({ sessionId: value.identity.sessionId, acceptedInputId: value.identity.acceptedInputId,
  effectKey: value.identity.effectKey, toolVersion: value.toolVersion, intent: packed(value.intent), payload: packed(value.payload), destination: value.destination });

/** Checks retained relationships too, so reopening cannot silently accept orphan/conflicting rows. */
export function validateRecoveryAttemptRecords(records: readonly RecoveryAttemptRecordOrEvent[]): void {
  const attempts = new Map<string, RecoveryAttemptRecord>();
  const logical = new Map<string, RecoveryAttemptRecord>();
  const traceHistory = new Map<string, RecoveryAttemptTraceReference>();
  const eventIds = new Set<string>();
  const conflict = (message: string): never => { throw new RecoveryAttemptLedgerError("conflict", message); };
  for (const row of records) {
    const id = row.identity, key = attemptKey(id);
    if (row.kind === "attempt") {
      if (attempts.has(key)) conflict("Repeated physical attempt record");
      const logicalKey = JSON.stringify([id.sessionId, id.effectKey]);
      const original = logical.get(logicalKey);
      if (original && !same(binding(original), binding(row))) conflict("Changed immutable logical intent/payload/destination");
      for (const prior of attempts.values()) {
        if (prior.identity.sessionId === id.sessionId && prior.identity.effectKey === id.effectKey && prior.identity.attemptNumber === id.attemptNumber) conflict("Physical attempt ordinal reused");
      }
      logical.set(logicalKey, row); attempts.set(key, row);
    } else {
      const original = attempts.get(key), uniqueEvent = eventKey(row);
      if (!original || !same(original.identity, id)) conflict("Orphan event or changed physical identity");
      if (eventIds.has(uniqueEvent)) conflict("Repeated event identity");
      eventIds.add(uniqueEvent);
    }
    for (const ref of row.traces) {
      const traceKey = JSON.stringify([id.sessionId, id.attemptId, ref.traceId]), prior = traceHistory.get(traceKey);
      if (prior && (prior.storage.storeId !== ref.storage.storeId || prior.storage.objectKey !== ref.storage.objectKey || prior.storage.writerHostId !== ref.storage.writerHostId ||
        ref.byteLength < prior.byteLength || !Buffer.from(ref.bytes.subarray(0, prior.byteLength)).equals(Buffer.from(prior.bytes)))) conflict("Raw trace prefix replaced or shortened");
      traceHistory.set(traceKey, ref);
    }
  }
}

/** No file access here: journal owns durability, trace owner owns raw storage, native owns Pi. */
const journalQueues = new WeakMap<RecoveryJournal, Promise<void>>();

export function createRecoveryAttemptLedger(journal: RecoveryJournal): RecoveryAttemptLedger {
  const read = (): RecoveryAttemptRecordOrEvent[] => {
    const rows = journal.entries().flatMap(row => {
      const parsed = decodeRecoveryAttemptRecord(row);
      return parsed ? [parsed] : [];
    });
    validateRecoveryAttemptRecords(rows);
    return rows;
  };
  const append = (kind: "attempt" | "event", request: RecoveryAttemptRequest | RecoveryAttemptEventRequest, options: RecoveryAttemptAppendOptions): Promise<RecoveryAttemptAppendReceipt> => {
    const bytes = encoded(kind, request), key = kind === "attempt" ? attemptKey(request.identity) : eventKey(request as RecoveryAttemptEventRequest);
    // Serialize validation + append in this facade, not just writes; a changed binding cannot race.
    const operation = (journalQueues.get(journal) ?? Promise.resolve()).then(async () => {
      const existing = journal.get(key), rows = read();
      if (existing && !Buffer.from(existing.bytes).equals(Buffer.from(bytes))) throw new RecoveryAttemptLedgerError("conflict", "Attempt/event identity already bound to different bytes");
      if (!existing) {
        const seq = rows.reduce((max, row) => Math.max(max, row.seq), 0) + 1;
        const candidate = decodeRecoveryAttemptRecord({ key, bytes, seq })!;
        if (kind === "event" && !rows.some(row => row.kind === "attempt" && attemptKey(row.identity) === attemptKey(request.identity))) throw new RecoveryAttemptLedgerError("not-found", "Physical attempt must be recorded before its event");
        validateRecoveryAttemptRecords([...rows, candidate]);
      }
      return journal.append(key, bytes, options);
    });
    journalQueues.set(journal, operation.then(() => {}, () => {}));
    return operation;
  };
  return {
    recordAttempt: (request, options) => {
      // Snapshot before the first await or queue wait, including all mutable byte arrays.
      const copied = attempt(request);
      return append("attempt", copied, { ...options });
    },
    appendEvent: (request, options) => append("event", event(request), { ...options }),
    readAttempt: (sessionId, attemptId) => {
      text(sessionId, "sessionId"); text(attemptId, "attemptId");
      const rows = read(), original = rows.find(row => row.kind === "attempt" && row.identity.sessionId === sessionId && row.identity.attemptId === attemptId);
      return original?.kind === "attempt" ? { attempt: original, events: rows.filter((row): row is RecoveryAttemptEvent => row.kind === "event" && row.identity.sessionId === sessionId && row.identity.attemptId === attemptId) } : undefined;
    },
    entries: read,
  };
}
