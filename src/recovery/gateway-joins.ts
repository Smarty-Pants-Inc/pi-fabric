import { createHash } from "node:crypto";
import type { RecoveryJournal } from "./recovery-journal.js";

/** Structural port: attemptId identifies the physical invocation, including a model
 * invocation before it has produced ANY tool call/effect. No tool/effect is invented. */
export interface GatewayAttemptCorrelation {
  readonly sessionId: string;
  readonly acceptedInputId: string;
  readonly toolCallId?: string;
  readonly effectKey?: string;
  readonly attemptId: string;
  readonly hostId: string;
  readonly podId: string;
  readonly bootId: string;
  readonly ownerIncarnation: string;
  readonly authorityRevision: string | number;
}

export type GatewayCorrelation =
  | { readonly kind: "exact"; readonly value: GatewayAttemptCorrelation }
  | { readonly kind: "missing"; readonly reason: string }
  | { readonly kind: "ambiguous"; readonly reason: string; readonly candidates: readonly GatewayAttemptCorrelation[] };

/** JSON source data, not a billing estimate, normalized status or computed duration. */
export type GatewaySourceData = null | boolean | number | string |
  readonly GatewaySourceData[] | { readonly [key: string]: GatewaySourceData };

export interface GatewayRequestIdentity {
  readonly sourceId: string;
  /** Source-issued ID for ONE physical gateway/upstream invocation. Retries get new IDs. */
  readonly gatewayRequestId: string;
  readonly providerAttemptId?: string;
}

export interface GatewayRequestRow extends GatewayRequestIdentity {
  readonly scopeId: string;
  /** Immutable source event identity. Lifecycle updates use NEW sourceRecordIds. */
  readonly sourceRecordId: string;
  readonly retryOf?: { readonly gatewayRequestId: string; readonly providerAttemptId?: string };
  readonly correlation: GatewayCorrelation;
  readonly stage: "started" | "updated" | "finished" | "interrupted";
  /** Exact source value/units; null means unknown, never the import time. */
  readonly requestStartedAt: string | number | null;
  readonly timestamps: Readonly<Record<string, string | number | null>>;
  readonly status: GatewaySourceData;
  readonly billing: GatewaySourceData;
  /** Optional source provenance, route/group identifiers and native-unit conventions. */
  readonly metadata?: GatewaySourceData;
}

export interface GatewaySourceSnapshot {
  readonly snapshotId: string;
  /** Complete enumeration of ALL invocation observations for the frozen scope,
   * including starts with no response, failures, cancellations and every retry. */
  readonly complete: boolean;
  readonly reason?: string;
  readonly rows: readonly GatewayRequestRow[];
}

/** Capacity-owned source. This module implements the consumer, NOT a live gateway feed.
 * The normal route must retain dispatch/start records before waiting for a response. */
export interface GatewayCapacitySource {
  readonly sourceId: string;
  readGatewayRequests(request: {
    readonly scopeId: string;
    readonly deadlineMs: number;
    readonly signal?: AbortSignal;
  }): Promise<GatewaySourceSnapshot>;
}

export interface GatewaySourceExpectation {
  readonly sourceId: string;
  readonly scopeId: string;
  readonly captureId: string;
}
export interface GatewayCaptureRequest extends GatewaySourceExpectation {
  readonly deadlineMs: number;
  readonly signal?: AbortSignal;
}
export interface GatewaySourceObservation extends GatewaySourceExpectation {
  readonly state: "complete" | "incomplete" | "unavailable" | "missing";
  readonly snapshotId?: string;
  readonly reason?: string;
  readonly sourceRecordIds: readonly string[];
}
export type GatewayJoinRecord =
  | { readonly seq: number; readonly kind: "request"; readonly row: GatewayRequestRow }
  | { readonly seq: number; readonly kind: "source"; readonly observation: GatewaySourceObservation };

type Payload =
  | { readonly format: 1; readonly kind: "request"; readonly row: GatewayRequestRow }
  | { readonly format: 1; readonly kind: "source"; readonly observation: GatewaySourceObservation };
type AppendOptions = { readonly deadlineMs: number; readonly signal?: AbortSignal };
export interface GatewayJoinReceipt { readonly seq: number; readonly existing: boolean }
export interface GatewayCaptureReceipt {
  readonly requests: readonly GatewayJoinReceipt[];
  /** Written AFTER the request rows. A crash prefix without this receipt is not complete. */
  readonly source: GatewayJoinReceipt;
}
export type GatewayRequestJoinStatus = "matched" | "missing-correlation" | "ambiguous-correlation" |
  "unmatched-attempt" | "identity-mismatch" | "ambiguous-attempt";
export interface GatewayRequestJoin {
  readonly seq: number;
  readonly row: GatewayRequestRow;
  readonly status: GatewayRequestJoinStatus;
  readonly attemptIndexes: readonly number[];
}
export interface GatewayJoinInventory {
  /** One row per retained observation, not only successful/fully correlated requests. */
  readonly requests: readonly GatewayRequestJoin[];
  readonly attempts: readonly {
    readonly correlation: GatewayAttemptCorrelation;
    readonly status: "matched" | "missing-request" | "ambiguous-attempt";
    readonly sourceRecordIds: readonly string[];
  }[];
  readonly sources: readonly {
    readonly expected: GatewaySourceExpectation;
    readonly status: "complete" | "incomplete" | "unavailable" | "missing";
    readonly observation?: GatewaySourceObservation;
  }[];
  readonly sourceHistory: readonly GatewaySourceObservation[];
  /** Explicit expected IDs and declared retry parents that are absent from retained rows. */
  readonly missingRequests: readonly GatewayRequestIdentity[];
  /** Retained invocation rows not covered by a complete source enumeration. */
  readonly unsealedSourceRecordIds: readonly string[];
  readonly complete: boolean;
}

export class GatewayJoinError extends Error {
  constructor(readonly reason: "invalid-record" | "record-conflict" | "request-conflict" | "source-conflict", message: string) {
    super(message);
    this.name = "GatewayJoinError";
  }
}

const PREFIX = "recovery:gateway-join:v1:";
const queues = new WeakMap<RecoveryJournal, Promise<void>>();
const requiredIdentity = ["sessionId", "acceptedInputId", "attemptId", "hostId", "podId", "bootId", "ownerIncarnation"] as const;
const optionalIdentity = ["toolCallId", "effectKey"] as const;
function invalid(message: string): never { throw new GatewayJoinError("invalid-record", message); }
function text(value: unknown): value is string { return typeof value === "string" && value.length > 0; }
function object(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
function timestamp(value: unknown): boolean {
  return value === null || typeof value === "string" || (typeof value === "number" && Number.isFinite(value));
}

/** Reject lossy JSON (NaN, undefined, accessors, cycles, exotic objects) rather than
 * silently changing source evidence. Sorted keys make import replay order-independent. */
function canonical(value: unknown, ancestors = new Set<object>()): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value) && !Object.is(value, -0)) return JSON.stringify(value);
  if (!object(value) && !Array.isArray(value)) return invalid("Gateway evidence must be lossless JSON");
  const item = value as object;
  if (ancestors.has(item)) return invalid("Cyclic gateway evidence");
  if (Object.getOwnPropertySymbols(item).length) return invalid("Symbol gateway evidence fields");
  const proto = Object.getPrototypeOf(item);
  if (!Array.isArray(item) && proto !== Object.prototype && proto !== null) return invalid("Non-plain gateway evidence");
  ancestors.add(item);
  try {
    const descriptors = Object.getOwnPropertyDescriptors(item);
    for (const [name, descriptor] of Object.entries(descriptors)) {
      if (Array.isArray(item) && name === "length") continue;
      if (!("value" in descriptor) || !descriptor.enumerable) return invalid("Non-data gateway evidence field");
    }
    if (Array.isArray(value)) {
      if (Object.keys(value).length !== value.length) return invalid("Sparse or decorated gateway evidence array");
      return `[${Array.from({ length: value.length }, (_, i) => canonical(value[i], ancestors)).join(",")}]`;
    }
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${canonical(record[key], ancestors)}`).join(",")}}`;
  } finally { ancestors.delete(item); }
}
function detach<T>(value: T): T { return JSON.parse(canonical(value)) as T; }
function validateIdentity(value: unknown): asserts value is GatewayAttemptCorrelation {
  if (!object(value) || requiredIdentity.some(key => !text(value[key])) ||
    optionalIdentity.some(key => value[key] !== undefined && !text(value[key])) ||
    !(text(value.authorityRevision) || (typeof value.authorityRevision === "number" && Number.isSafeInteger(value.authorityRevision) && value.authorityRevision >= 0))) {
    invalid("Missing or malformed gateway runtime identity");
  }
}
function validateRequestIdentity(value: unknown): asserts value is GatewayRequestIdentity {
  if (!object(value) || !text(value.sourceId) || !text(value.gatewayRequestId) ||
    (value.providerAttemptId !== undefined && !text(value.providerAttemptId))) invalid("Missing gateway request identity");
}
function validateRow(row: GatewayRequestRow): void {
  validateRequestIdentity(row);
  if (!text(row.scopeId) || !text(row.sourceRecordId) || !timestamp(row.requestStartedAt) ||
    !["started", "updated", "finished", "interrupted"].includes(row.stage) || !object(row.timestamps) ||
    !Object.values(row.timestamps).every(timestamp) || row.status === undefined || row.billing === undefined) invalid("Malformed gateway source row");
  if (row.retryOf !== undefined && (!object(row.retryOf) || !text(row.retryOf.gatewayRequestId) ||
    (row.retryOf.providerAttemptId !== undefined && !text(row.retryOf.providerAttemptId)) || row.retryOf.gatewayRequestId === row.gatewayRequestId)) invalid("Malformed gateway retry identity");
  const correlation = row.correlation;
  if (!object(correlation)) invalid("Missing gateway correlation state");
  if (correlation.kind === "exact") validateIdentity(correlation.value);
  else if (correlation.kind === "missing") { if (!text(correlation.reason)) invalid("Missing correlation reason"); }
  else if (correlation.kind === "ambiguous") {
    if (!text(correlation.reason) || !Array.isArray(correlation.candidates) || correlation.candidates.length < 2) invalid("Malformed ambiguous correlation");
    correlation.candidates.forEach(validateIdentity);
  } else invalid("Unknown gateway correlation state");
}
function validateExpectation(value: GatewaySourceExpectation): void {
  if (!object(value) || !text(value.sourceId) || !text(value.scopeId) || !text(value.captureId)) invalid("Malformed source expectation");
}
function validateObservation(value: GatewaySourceObservation): void {
  validateExpectation(value);
  if (!["complete", "incomplete", "unavailable", "missing"].includes(value.state) ||
    !Array.isArray(value.sourceRecordIds) || !value.sourceRecordIds.every(text) ||
    new Set(value.sourceRecordIds).size !== value.sourceRecordIds.length ||
    ((value.state === "complete" || value.state === "incomplete") && !text(value.snapshotId)) ||
    (value.state !== "complete" && !text(value.reason))) invalid("Malformed gateway source observation");
}
function payloadKey(payload: Payload): string {
  const tuple = payload.kind === "request" ? ["request", payload.row.sourceId, payload.row.sourceRecordId] :
    ["source", payload.observation.sourceId, payload.observation.scopeId, payload.observation.captureId];
  return PREFIX + createHash("sha256").update(canonical(tuple)).digest("hex");
}
function toPayload(record: GatewayJoinRecord): Payload {
  return record.kind === "request" ? { format: 1, kind: "request", row: record.row } :
    { format: 1, kind: "source", observation: record.observation };
}
function sameIdentity(a: GatewayAttemptCorrelation, b: GatewayAttemptCorrelation): boolean {
  return [...requiredIdentity, ...optionalIdentity, "authorityRevision" as const].every(key => a[key] === b[key]);
}
function sameSource(a: GatewaySourceExpectation, b: GatewaySourceExpectation): boolean {
  return a.sourceId === b.sourceId && a.scopeId === b.scopeId && a.captureId === b.captureId;
}
class RecordIndex {
  private readonly records = new Map<string, string>();
  private readonly invocations = new Map<string, { binding: string; providerAttemptId: string | undefined }>();
  constructor(retained: readonly GatewayJoinRecord[] = []) {
    retained.forEach(record => this.add(toPayload(record)));
  }
  add(payload: Payload): void {
    const key = payloadKey(payload), bytes = canonical(payload), previous = this.records.get(key);
    if (previous !== undefined && previous !== bytes) {
      throw new GatewayJoinError("record-conflict", "Immutable gateway source record/capture changed");
    }
    if (payload.kind === "request") {
      const row = payload.row, invocation = canonical([row.sourceId, row.gatewayRequestId]);
      const binding = canonical([row.scopeId, row.requestStartedAt, row.correlation, row.retryOf ?? null]);
      const prior = this.invocations.get(invocation);
      // Status, billing, event timestamps and a newly learned provider ID may advance.
      // The gateway invocation, original start and runtime binding may not be restamped.
      if (prior && (prior.binding !== binding || (prior.providerAttemptId !== undefined &&
        row.providerAttemptId !== undefined && prior.providerAttemptId !== row.providerAttemptId))) {
        throw new GatewayJoinError("request-conflict", "Physical gateway request identity rebound");
      }
      this.invocations.set(invocation, { binding, providerAttemptId: row.providerAttemptId ?? prior?.providerAttemptId });
    }
    this.records.set(key, bytes);
  }
}
function serialized<T>(journal: RecoveryJournal, work: () => Promise<T>): Promise<T> {
  const previous = queues.get(journal) ?? Promise.resolve();
  const next = previous.then(work);
  queues.set(journal, next.then(() => {}, () => {}));
  return next;
}

/** Append-only consumer. Owns neither the journal's lifecycle nor native Pi files. */
export class GatewayJoinCapture {
  constructor(private readonly journal: RecoveryJournal) {}

  /** Always reads the journal, so an unavailable store cannot become an empty inventory. */
  records(): readonly GatewayJoinRecord[] {
    const records: GatewayJoinRecord[] = [];
    for (const record of this.journal.entries()) {
      if (!record.key.startsWith(PREFIX)) continue; // Other recovery owners share this journal.
      let payload: Payload;
      try {
        payload = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(record.bytes)) as Payload;
        if (!object(payload) || payload.format !== 1) invalid("Unknown retained gateway format");
        if (payload.kind === "request") validateRow(payload.row);
        else if (payload.kind === "source") validateObservation(payload.observation);
        else invalid("Unknown retained gateway record");
        if (payloadKey(payload) !== record.key || canonical(payload) !== new TextDecoder().decode(record.bytes)) invalid("Gateway record key/bytes mismatch");
      } catch (error) {
        if (error instanceof GatewayJoinError) throw error;
        throw new GatewayJoinError("invalid-record", "Unreadable retained gateway evidence");
      }
      records.push(payload.kind === "request" ? { seq: record.seq, kind: "request", row: payload.row } :
        { seq: record.seq, kind: "source", observation: payload.observation });
    }
    // Detect contradictory imports even when inserted by another journal client.
    new RecordIndex(records);
    return records;
  }

  async append(row: GatewayRequestRow, options: AppendOptions): Promise<GatewayJoinReceipt> {
    const saved = detach(row); // BEFORE the async queue, not after a caller can mutate it.
    validateRow(saved);
    const bound = { ...options };
    const payload: Payload = { format: 1, kind: "request", row: saved };
    return serialized(this.journal, async () => {
      new RecordIndex(this.records()).add(payload);
      return this.appendPayload(payload, bound);
    });
  }

  /** Exact imports are idempotent. Changed source IDs/capture IDs never replace history.
   * Preflight the whole snapshot; storage failures can still leave a retained prefix. */
  async importSnapshot(request: GatewayCaptureRequest, snapshot: GatewaySourceSnapshot): Promise<GatewayCaptureReceipt> {
    const bound = { ...request };
    validateExpectation(bound);
    const saved = detach(snapshot);
    if (!object(saved) || !text(saved.snapshotId) || typeof saved.complete !== "boolean" || !Array.isArray(saved.rows) ||
      (!saved.complete && !text(saved.reason))) invalid("Malformed source snapshot");
    const ids = new Set<string>();
    for (const row of saved.rows) {
      validateRow(row);
      if (row.sourceId !== bound.sourceId || row.scopeId !== bound.scopeId) throw new GatewayJoinError("source-conflict", "Row outside requested source/scope");
      if (ids.has(row.sourceRecordId)) invalid("Duplicate source record in snapshot");
      ids.add(row.sourceRecordId);
    }
    const observation: GatewaySourceObservation = {
      sourceId: bound.sourceId, scopeId: bound.scopeId, captureId: bound.captureId,
      state: saved.complete ? "complete" : "incomplete", snapshotId: saved.snapshotId,
      ...(saved.reason === undefined ? {} : { reason: saved.reason }), sourceRecordIds: [...ids].sort(),
    };
    validateObservation(observation);
    const source: Payload = { format: 1, kind: "source", observation };
    return serialized(this.journal, async () => {
      const retained = new RecordIndex(this.records());
      retained.add(source); // Changed capture refuses BEFORE adding any new rows.
      for (const row of saved.rows) retained.add({ format: 1, kind: "request", row });
      const requests: GatewayJoinReceipt[] = [];
      for (const row of saved.rows) requests.push(await this.appendPayload({ format: 1, kind: "request", row }, bound));
      return { requests, source: await this.appendPayload(source, bound) };
    });
  }

  async captureFrom(source: GatewayCapacitySource | undefined, request: GatewayCaptureRequest): Promise<GatewayCaptureReceipt> {
    const bound = { ...request };
    validateExpectation(bound);
    if (source && source.sourceId !== bound.sourceId) throw new GatewayJoinError("source-conflict", "Unexpected gateway source identity");
    let snapshot: GatewaySourceSnapshot | undefined;
    let reason = "No gateway source configured";
    if (source) {
      try {
        snapshot = await source.readGatewayRequests({ scopeId: bound.scopeId, deadlineMs: bound.deadlineMs,
          ...(bound.signal === undefined ? {} : { signal: bound.signal }) });
        if (!snapshot) invalid("Source returned no snapshot");
      } catch (error) {
        reason = error instanceof Error ? error.message || "Gateway source unavailable" : "Gateway source unavailable";
      }
    }
    if (snapshot) return this.importSnapshot(bound, snapshot);
    const observation: GatewaySourceObservation = { sourceId: bound.sourceId, scopeId: bound.scopeId,
      captureId: bound.captureId, state: source ? "unavailable" : "missing", reason, sourceRecordIds: [] };
    return serialized(this.journal, async () => {
      const payload: Payload = { format: 1, kind: "source", observation };
      new RecordIndex(this.records()).add(payload);
      return { requests: [], source: await this.appendPayload(payload, bound) };
    });
  }

  inventory(input: {
    readonly attempts: readonly GatewayAttemptCorrelation[];
    readonly expectedSources: readonly GatewaySourceExpectation[];
    readonly expectedRequests?: readonly GatewayRequestIdentity[];
  }): GatewayJoinInventory {
    const attempts = detach(input.attempts);
    attempts.forEach(validateIdentity);
    const expected = detach(input.expectedSources);
    expected.forEach(validateExpectation);
    const expectedRequests = detach(input.expectedRequests ?? []);
    expectedRequests.forEach(validateRequestIdentity);
    const records = this.records();
    const sourceHistory = records.flatMap(record => record.kind === "source" ? [record.observation] : []);
    const requests: GatewayRequestJoin[] = records.flatMap(record => {
      if (record.kind !== "request") return [];
      const correlation = record.row.correlation;
      const indexes = correlation.kind === "exact" ? attempts.flatMap((attempt, index) => sameIdentity(attempt, correlation.value) ? [index] : []) : [];
      let status: GatewayRequestJoinStatus;
      if (correlation.kind === "missing") status = "missing-correlation";
      else if (correlation.kind === "ambiguous") status = "ambiguous-correlation";
      else if (indexes.length > 1) status = "ambiguous-attempt";
      else if (indexes.length === 1) status = "matched";
      else status = attempts.some(attempt => attempt.attemptId === correlation.value.attemptId) ? "identity-mismatch" : "unmatched-attempt";
      return [{ seq: record.seq, row: record.row, status, attemptIndexes: indexes }];
    });
    const sources = expected.map(item => {
      const observation = sourceHistory.find(source => sameSource(item, source));
      let status: GatewaySourceObservation["state"] = observation?.state ?? "missing";
      if (observation?.state === "complete" && observation.sourceRecordIds.some(id => !requests.some(join =>
        join.row.sourceId === item.sourceId && join.row.scopeId === item.scopeId && join.row.sourceRecordId === id))) status = "incomplete";
      return { expected: item, status, ...(observation ? { observation } : {}) };
    });
    const attemptJoins = attempts.map((correlation, index) => {
      const joined = requests.filter(request => request.attemptIndexes.includes(index));
      const status = joined.some(request => request.status === "ambiguous-attempt") ? "ambiguous-attempt" as const :
        joined.length ? "matched" as const : "missing-request" as const;
      return { correlation, status, sourceRecordIds: joined.map(request => request.row.sourceRecordId) };
    });
    const requiredRequests = [...expectedRequests, ...requests.flatMap(({ row }) => row.retryOf ? [{
      sourceId: row.sourceId, gatewayRequestId: row.retryOf.gatewayRequestId,
      ...(row.retryOf.providerAttemptId === undefined ? {} : { providerAttemptId: row.retryOf.providerAttemptId }),
    }] : [])];
    const absent = new Map<string, GatewayRequestIdentity>();
    for (const expected of requiredRequests) {
      if (!requests.some(({ row }) => row.sourceId === expected.sourceId && row.gatewayRequestId === expected.gatewayRequestId &&
        (expected.providerAttemptId === undefined || row.providerAttemptId === expected.providerAttemptId))) absent.set(canonical(expected), expected);
    }
    const missingRequests = [...absent.values()];
    const unsealedSourceRecordIds = requests.filter(({ row }) => !sources.some(source => source.status === "complete" &&
      source.expected.sourceId === row.sourceId && source.expected.scopeId === row.scopeId &&
      source.observation?.sourceRecordIds.includes(row.sourceRecordId))).map(({ row }) => row.sourceRecordId);
    return { requests, attempts: attemptJoins, sources, sourceHistory, missingRequests, unsealedSourceRecordIds,
      complete: sources.length > 0 && sources.every(source => source.status === "complete") &&
        requests.every(request => request.status === "matched") && attemptJoins.every(attempt => attempt.status === "matched") &&
        missingRequests.length === 0 && unsealedSourceRecordIds.length === 0 };
  }

  private appendPayload(payload: Payload, options: AppendOptions): Promise<GatewayJoinReceipt> {
    return this.journal.append(payloadKey(payload), new TextEncoder().encode(canonical(payload)), options);
  }
}
