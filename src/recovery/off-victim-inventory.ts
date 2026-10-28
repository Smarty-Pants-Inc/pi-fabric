import { createHash } from "node:crypto";
import {
  decodeRecoveryAttemptRecord, validateRecoveryAttemptRecords,
  type RecoveryAttemptRecordOrEvent, type RecoveryAttemptTraceReference,
} from "./attempt-ledger.js";
import type { RecoveryJournalRecord } from "./recovery-journal.js";

/** Frozen independently of observed successes. Include sessions/attempts with no returned row. */
export interface RecoveryInventoryCohort {
  readonly cohortId: string;
  readonly frozenAt: number;
  readonly sessions: readonly {
    readonly sessionId: string;
    readonly attempts: readonly {
      readonly attemptId: string;
      readonly eventIds: readonly string[];
      readonly traceIds: readonly string[];
    }[];
  }[];
}

export interface RecoveryOffVictimReadPolicy {
  readonly observerHostId: string;
  readonly victimHostId: string;
  /** Source must refuse rather than retry against the victim, including indirect dependencies. */
  readonly allowVictimFallback: false;
}

export interface RecoveryOffVictimProvenance {
  readonly sourceHostId: string;
  readonly sourceId: string;
  readonly storeId: string;
  /** Real source read/commit provenance, not a basename or a caller's topology guess. */
  readonly receipt: string;
  /** Complete source-owner assertion of dependencies for THIS read, including sourceHostId. */
  readonly dependencyHostIds: readonly string[];
}

/** A closed, independently retained enumeration, not a count inferred from the returned rows.
 * The source owner must seal only after all writers/attempts are accounted for. A fixture seal
 * proves comparison behavior, NOT that a live source performed that protocol. */
export interface RecoveryInventoryTraceManifestEntry {
  readonly sessionId: string;
  readonly attemptId: string;
  readonly traceId: string;
  readonly storeId: string;
  readonly objectKey: string;
  /** Full sealed raw object, not merely its last successfully parsed line. */
  readonly byteLength: number;
  readonly sha256: string;
}

export interface RecoveryInventorySourceManifest {
  readonly cohortSha256: string;
  readonly sealed: boolean;
  readonly sealReceipt: string;
  readonly entries: readonly { readonly seq: number; readonly key: string; readonly sha256: string }[];
  /** Complete independent raw-object enumeration, including unreferenced/unknown objects. */
  readonly traces: readonly RecoveryInventoryTraceManifestEntry[];
}

export type RecoveryExternalRead<T> =
  | { readonly status: "present"; readonly value: T; readonly provenance: RecoveryOffVictimProvenance }
  | { readonly status: "missing" | "unknown"; readonly detail: string; readonly provenance?: RecoveryOffVictimProvenance };

/** This is an explicit external-only port. There is intentionally no local/victim journal,
 * pathname-to-host heuristic or default filesystem reader. The implementation MUST honor
 * allowVictimFallback:false. Supplied provenance is checked, never topology-certified. */
export interface RecoveryOffVictimSource {
  readonly sourceHostId: string;
  readonly sourceId: string;
  readManifest(policy: RecoveryOffVictimReadPolicy): Promise<RecoveryExternalRead<RecoveryInventorySourceManifest>>;
  readJournal(policy: RecoveryOffVictimReadPolicy): Promise<RecoveryExternalRead<{ readonly complete: boolean; readonly records: readonly RecoveryJournalRecord[] }>>;
  readTrace(reference: RecoveryAttemptTraceReference, policy: RecoveryOffVictimReadPolicy): Promise<RecoveryExternalRead<Uint8Array>>;
}

export type RecoveryInventoryAvailability = "present" | "missing" | "unknown";
export interface RecoveryInventoryAttemptRow {
  readonly sessionId: string;
  readonly attemptId: string;
  readonly expected: boolean;
  readonly availability: RecoveryInventoryAvailability;
  readonly record?: RecoveryAttemptRecordOrEvent;
}
export interface RecoveryInventoryEventRow extends RecoveryInventoryAttemptRow { readonly eventId: string }
export interface RecoveryInventoryTraceRow {
  readonly sessionId: string;
  readonly attemptId: string;
  readonly traceId: string;
  readonly eventId?: string;
  readonly seq?: number;
  readonly expected: boolean;
  readonly availability: RecoveryInventoryAvailability;
  readonly reference?: RecoveryAttemptTraceReference;
  readonly sourceManifestEntry?: RecoveryInventoryTraceManifestEntry;
  /** Exact externally read raw prefix, including invalid UTF-8 and interrupted final lines. */
  readonly bytes?: Uint8Array;
  readonly provenance?: RecoveryOffVictimProvenance;
  readonly detail?: string;
}
export interface RecoveryOffVictimInventory {
  readonly cohort: RecoveryInventoryCohort;
  readonly cohortSha256: string;
  readonly observerHostId: string;
  readonly victimHostId: string;
  readonly sourceId?: string;
  /** Evidence enumeration/comparison only; NOT installed topology or effect correctness. */
  readonly complete: boolean;
  readonly topologyQualification: "not-established-by-inventory";
  readonly manifest?: RecoveryInventorySourceManifest;
  readonly manifestProvenance?: RecoveryOffVictimProvenance;
  readonly journalProvenance?: RecoveryOffVictimProvenance;
  readonly sessions: readonly { readonly sessionId: string; readonly expected: boolean; readonly availability: RecoveryInventoryAvailability }[];
  readonly attempts: readonly RecoveryInventoryAttemptRow[];
  readonly events: readonly RecoveryInventoryEventRow[];
  readonly traces: readonly RecoveryInventoryTraceRow[];
  /** All returned journal bytes, including foreign namespaces and corrupt/unknown records. */
  readonly journalRecords: readonly RecoveryJournalRecord[];
  readonly unknownRecords: readonly { readonly record: RecoveryJournalRecord; readonly detail: string }[];
  readonly issues: readonly string[];
}

const sha256 = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const validText = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
const validDigest = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const id = (sessionId: string, attemptId: string): string => JSON.stringify([sessionId, attemptId]);
const traceId = (sessionId: string, attemptId: string, name: string): string => JSON.stringify([sessionId, attemptId, name]);
const copyTrace = (value: RecoveryAttemptTraceReference): RecoveryAttemptTraceReference => ({ ...value, storage: { ...value.storage }, bytes: Uint8Array.from(value.bytes) });
const copyProvenance = (value: RecoveryOffVictimProvenance): RecoveryOffVictimProvenance => ({ ...value, dependencyHostIds: [...value.dependencyHostIds] });
const errorText = (error: unknown): string => error instanceof Error ? error.message : String(error);

function copyCohort(value: RecoveryInventoryCohort): RecoveryInventoryCohort {
  if (!validText(value.cohortId) || !Number.isFinite(value.frozenAt) || value.frozenAt < 0 || !Array.isArray(value.sessions) || value.sessions.length === 0) throw new Error("A nonempty frozen recovery cohort is required");
  const sessionIds = new Set<string>();
  const sessions = value.sessions.map(session => {
    if (!validText(session.sessionId) || sessionIds.has(session.sessionId) || !Array.isArray(session.attempts)) throw new Error("Invalid or repeated frozen session");
    sessionIds.add(session.sessionId);
    const attemptIds = new Set<string>();
    const attempts = session.attempts.map((attempt: RecoveryInventoryCohort["sessions"][number]["attempts"][number]) => {
      if (!validText(attempt.attemptId) || attemptIds.has(attempt.attemptId)) throw new Error("Invalid or repeated frozen attempt");
      attemptIds.add(attempt.attemptId);
      const names = (values: readonly string[]): readonly string[] => {
        if (!Array.isArray(values) || values.some(value => !validText(value)) || new Set(values).size !== values.length) throw new Error("Invalid or repeated frozen event/trace identity");
        return Object.freeze([...values]);
      };
      return Object.freeze({ attemptId: attempt.attemptId, eventIds: names(attempt.eventIds), traceIds: names(attempt.traceIds) });
    });
    return Object.freeze({ sessionId: session.sessionId, attempts: Object.freeze(attempts) });
  });
  return Object.freeze({ cohortId: value.cohortId, frozenAt: value.frozenAt, sessions: Object.freeze(sessions) });
}

export function recoveryInventoryCohortSha256(cohort: RecoveryInventoryCohort): string {
  return sha256(Buffer.from(JSON.stringify(copyCohort(cohort)), "utf8"));
}

/** Fail closed while retaining the denominator and every obtainable byte/unknown. No effect
 * dispatch, replay, cleanup, native-file access, driver reconnect or live host test occurs. */
export async function buildRecoveryOffVictimInventory(options: {
  readonly observerHostId: string;
  readonly victimHostId: string;
  readonly cohort: RecoveryInventoryCohort;
  readonly source?: RecoveryOffVictimSource;
}): Promise<RecoveryOffVictimInventory> {
  const { observerHostId, victimHostId, source } = options;
  if (!validText(observerHostId) || !validText(victimHostId) || observerHostId === victimHostId) throw new Error("Inventory observer must be distinct from victim");
  if (source && (!validText(source.sourceId) || !validText(source.sourceHostId) || source.sourceHostId === victimHostId)) throw new Error("External inventory source must be distinct from victim");
  const sourceId = source?.sourceId, sourceHostId = source?.sourceHostId;
  const cohort = copyCohort(options.cohort), cohortSha256 = recoveryInventoryCohortSha256(cohort);
  const policy: RecoveryOffVictimReadPolicy = Object.freeze({ observerHostId, victimHostId, allowVictimFallback: false });
  const issues: string[] = [], records: RecoveryJournalRecord[] = [], unknownRecords: { record: RecoveryJournalRecord; detail: string }[] = [];
  const rows: RecoveryAttemptRecordOrEvent[] = [];
  let manifest: RecoveryInventorySourceManifest | undefined, manifestProvenance: RecoveryOffVictimProvenance | undefined, journalProvenance: RecoveryOffVictimProvenance | undefined;
  let authoritative = false, journalComplete = false;
  const provenanceValid = (value: RecoveryOffVictimProvenance): boolean => !!value && validText(value.sourceHostId) && value.sourceHostId !== victimHostId && value.sourceHostId === sourceHostId &&
    value.sourceId === sourceId && validText(value.storeId) && validText(value.receipt) && Array.isArray(value.dependencyHostIds) && value.dependencyHostIds.length > 0 &&
    value.dependencyHostIds.every(validText) && value.dependencyHostIds.includes(value.sourceHostId) && !value.dependencyHostIds.includes(victimHostId);
  if (!source) issues.push("External source unavailable; no victim fallback permitted");
  else {
    try {
      const read = await source.readManifest(policy);
      if (read.status !== "present") issues.push(`Source manifest ${read.status}: ${read.detail}`);
      else if (!provenanceValid(read.provenance)) issues.push("Invalid or victim-dependent source manifest provenance");
      else {
        manifestProvenance = copyProvenance(read.provenance);
        manifest = { cohortSha256: read.value.cohortSha256, sealed: read.value.sealed, sealReceipt: read.value.sealReceipt, entries: read.value.entries.map(entry => ({ ...entry })), traces: read.value.traces.map(entry => ({ ...entry })) };
        if (manifest.cohortSha256 !== cohortSha256 || manifest.sealed !== true || !validText(manifest.sealReceipt)) issues.push("Source manifest unsealed, unproven, or bound to a different frozen cohort");
        else authoritative = true;
      }
    } catch (error) { issues.push(`Source manifest unknown: ${errorText(error)}`); }
    try {
      const read = await source.readJournal(policy);
      if (read.status !== "present") issues.push(`External journal ${read.status}: ${read.detail}`);
      else {
        // Keep obtainable bytes even when their provenance/completeness is invalid.
        for (const record of read.value.records) records.push({ seq: record.seq, key: record.key, bytes: Uint8Array.from(record.bytes) });
        journalComplete = read.value.complete === true && provenanceValid(read.provenance);
        if (!journalComplete) issues.push("External journal incomplete or provenance invalid/victim-dependent");
        else journalProvenance = copyProvenance(read.provenance);
      }
    } catch (error) { issues.push(`External journal unknown: ${errorText(error)}`); }
  }
  if (manifest && journalProvenance && manifestProvenance?.storeId !== journalProvenance.storeId) {
    issues.push("Manifest and journal store identity differ"); authoritative = false;
  }
  const manifestByKey = new Map<string, { readonly seq: number; readonly key: string; readonly sha256: string }>();
  const manifestSeqs = new Set<number>();
  let previousManifestSeq = 0;
  if (manifest) for (const entry of manifest.entries) {
    if (entry.seq !== previousManifestSeq + 1) {
      issues.push(`Noncontiguous source manifest sequence: expected ${previousManifestSeq + 1}, received ${entry.seq}`); authoritative = false;
    }
    previousManifestSeq = entry.seq;
    if (!validText(entry.key) || !Number.isSafeInteger(entry.seq) || entry.seq < 0 || !validDigest(entry.sha256) || manifestByKey.has(entry.key) || manifestSeqs.has(entry.seq)) {
      issues.push("Source manifest has invalid/repeated record identity"); authoritative = false;
    }
    manifestByKey.set(entry.key, entry); manifestSeqs.add(entry.seq);
  }
  const manifestTraces = new Map<string, RecoveryInventoryTraceManifestEntry>();
  if (manifest) for (const entry of manifest.traces) {
    const key = traceId(entry.sessionId, entry.attemptId, entry.traceId);
    if (![entry.sessionId, entry.attemptId, entry.traceId, entry.storeId, entry.objectKey].every(validText) ||
      !Number.isSafeInteger(entry.byteLength) || entry.byteLength < 0 || !validDigest(entry.sha256) || manifestTraces.has(key)) {
      issues.push("Source manifest has invalid/repeated raw trace identity"); authoritative = false;
    }
    manifestTraces.set(key, entry);
  }
  const returnedKeys = new Set<string>(), returnedSeqs = new Set<number>();
  let previousSeq = 0;
  for (const record of records) {
    if (record.seq !== previousSeq + 1) {
      issues.push(`Noncontiguous journal sequence: expected ${previousSeq + 1}, received ${record.seq}`); authoritative = false;
    }
    if (returnedKeys.has(record.key) || returnedSeqs.has(record.seq) || !Number.isSafeInteger(record.seq) || record.seq <= previousSeq) {
      issues.push(`Repeated, unordered or invalid journal record ${record.key}`); authoritative = false;
    }
    returnedKeys.add(record.key); returnedSeqs.add(record.seq); previousSeq = record.seq;
    const sealed = manifestByKey.get(record.key);
    if (!sealed || sealed.seq !== record.seq || sealed.sha256 !== sha256(record.bytes)) {
      issues.push(`Journal record absent from or different to source manifest: ${record.key}`); authoritative = false;
    }
    try {
      const decoded = decodeRecoveryAttemptRecord(record);
      if (decoded) rows.push(decoded);
    } catch (error) {
      unknownRecords.push({ record, detail: errorText(error) }); issues.push(`Unknown attempt-domain record ${record.key}`);
    }
  }
  for (const entry of manifestByKey.values()) if (!returnedKeys.has(entry.key)) { issues.push(`Missing sealed journal record ${entry.key}`); authoritative = false; }
  try { validateRecoveryAttemptRecords(rows); } catch (error) { issues.push(`Retained attempt relationships invalid: ${errorText(error)}`); authoritative = false; }
  authoritative = authoritative && journalComplete && unknownRecords.length === 0;

  const attempts: RecoveryInventoryAttemptRow[] = [], events: RecoveryInventoryEventRow[] = [], traceRows: RecoveryInventoryTraceRow[] = [];
  const expectedAttempts = new Map<string, RecoveryInventoryCohort["sessions"][number]["attempts"][number]>();
  const expectedSessions = new Set(cohort.sessions.map(session => session.sessionId));
  for (const session of cohort.sessions) for (const attempt of session.attempts) expectedAttempts.set(id(session.sessionId, attempt.attemptId), attempt);
  const presentAttempts = new Set<string>(), presentEvents = new Set<string>(), presentTraces = new Set<string>(), presentSessions = new Set<string>();
  const latestTraces = new Map<string, { sessionId: string; attemptId: string; reference: RecoveryAttemptTraceReference }>();
  for (const row of rows) {
    const identity = row.identity, attemptId = id(identity.sessionId, identity.attemptId), expected = expectedAttempts.get(attemptId);
    presentSessions.add(identity.sessionId);
    if (row.kind === "attempt") {
      presentAttempts.add(attemptId);
      attempts.push({ sessionId: identity.sessionId, attemptId: identity.attemptId, expected: !!expected, availability: "present", record: row });
      if (!expected) issues.push(`Unexpected physical attempt ${attemptId}`);
    } else {
      const eventKey = traceId(identity.sessionId, identity.attemptId, row.eventId), expectedEvent = !!expected?.eventIds.includes(row.eventId);
      presentEvents.add(eventKey);
      events.push({ sessionId: identity.sessionId, attemptId: identity.attemptId, eventId: row.eventId, expected: expectedEvent, availability: "present", record: row });
      if (!expectedEvent) issues.push(`Unexpected attempt event ${eventKey}`);
    }
    for (const reference of row.traces) {
      const ref = copyTrace(reference), key = traceId(identity.sessionId, identity.attemptId, ref.traceId), expectedTrace = !!expected?.traceIds.includes(ref.traceId);
      presentTraces.add(key);
      latestTraces.set(key, { sessionId: identity.sessionId, attemptId: identity.attemptId, reference: ref });
      const base = { sessionId: identity.sessionId, attemptId: identity.attemptId, traceId: ref.traceId, ...(row.kind === "event" ? { eventId: row.eventId } : {}), seq: row.seq, expected: expectedTrace, reference: ref };
      if (!expectedTrace) issues.push(`Unexpected trace ${key}`);
      if (!source) { traceRows.push({ ...base, availability: "unknown", detail: "External trace source unavailable" }); continue; }
      try {
        // Give the source a detached request; it cannot mutate retained output or prior rows.
        const read = await source.readTrace(copyTrace(ref), policy);
        if (read.status !== "present") {
          traceRows.push({ ...base, availability: read.status, detail: read.detail }); issues.push(`Trace ${key} ${read.status}: ${read.detail}`);
        } else {
          const bytes = Uint8Array.from(read.value), validProvenance = provenanceValid(read.provenance) && read.provenance.storeId === ref.storage.storeId;
          const validBytes = bytes.length === ref.byteLength && sha256(bytes) === ref.sha256 && Buffer.from(bytes).equals(Buffer.from(ref.bytes));
          const detail = !validProvenance ? "Invalid/victim-dependent trace provenance" : !validBytes ? "Exact raw trace prefix mismatch" : undefined;
          traceRows.push({ ...base, availability: detail ? "unknown" : "present", bytes, ...(validProvenance ? { provenance: copyProvenance(read.provenance) } : {}), ...(detail ? { detail } : {}) });
          if (detail) issues.push(`Trace ${key}: ${detail}`);
        }
      } catch (error) {
        const detail = errorText(error); traceRows.push({ ...base, availability: "unknown", detail }); issues.push(`Trace ${key} unknown: ${detail}`);
      }
    }
  }
  if (manifest) {
    for (const [key, latest] of latestTraces) {
      const sealed = manifestTraces.get(key), ref = latest.reference;
      if (!sealed) { issues.push(`Raw trace absent from source enumeration ${key}`); authoritative = false; }
      else if (sealed.storeId !== ref.storage.storeId || sealed.objectKey !== ref.storage.objectKey || sealed.byteLength !== ref.byteLength || sealed.sha256 !== ref.sha256) {
        issues.push(`Full sealed raw trace differs from latest retained prefix ${key}`); authoritative = false;
        traceRows.push({ sessionId: latest.sessionId, attemptId: latest.attemptId, traceId: ref.traceId,
          expected: !!expectedAttempts.get(id(latest.sessionId, latest.attemptId))?.traceIds.includes(ref.traceId), availability: "unknown",
          sourceManifestEntry: { ...sealed }, detail: "Full source raw object not covered by retained trace evidence" });
      }
    }
    for (const [key, sealed] of manifestTraces) if (!latestTraces.has(key)) {
      issues.push(`Unreferenced sealed raw trace ${key}`); authoritative = false;
      presentSessions.add(sealed.sessionId); presentTraces.add(key);
      const attemptKey = id(sealed.sessionId, sealed.attemptId);
      if (!presentAttempts.has(attemptKey)) {
        attempts.push({ sessionId: sealed.sessionId, attemptId: sealed.attemptId, expected: expectedAttempts.has(attemptKey), availability: "unknown" });
        presentAttempts.add(attemptKey);
      }
      traceRows.push({ sessionId: sealed.sessionId, attemptId: sealed.attemptId, traceId: sealed.traceId,
        expected: !!expectedAttempts.get(id(sealed.sessionId, sealed.attemptId))?.traceIds.includes(sealed.traceId), availability: "unknown",
        sourceManifestEntry: { ...sealed }, detail: "Source raw object has no retained attempt/event reference" });
    }
  }
  for (const session of cohort.sessions) for (const expected of session.attempts) {
    const key = id(session.sessionId, expected.attemptId), availability = authoritative ? "missing" : "unknown";
    if (!presentAttempts.has(key)) { attempts.push({ sessionId: session.sessionId, attemptId: expected.attemptId, expected: true, availability }); issues.push(`Expected attempt ${key} ${availability}`); }
    for (const eventId of expected.eventIds) if (!presentEvents.has(traceId(session.sessionId, expected.attemptId, eventId))) {
      events.push({ sessionId: session.sessionId, attemptId: expected.attemptId, eventId, expected: true, availability }); issues.push(`Expected event ${key}/${eventId} ${availability}`);
    }
    for (const name of expected.traceIds) if (!presentTraces.has(traceId(session.sessionId, expected.attemptId, name))) {
      traceRows.push({ sessionId: session.sessionId, attemptId: expected.attemptId, traceId: name, expected: true, availability }); issues.push(`Expected trace ${key}/${name} ${availability}`);
    }
  }
  const sessions: { sessionId: string; expected: boolean; availability: RecoveryInventoryAvailability }[] = cohort.sessions.map(session => ({ sessionId: session.sessionId, expected: true, availability: presentSessions.has(session.sessionId) ? "present" : authoritative ? "missing" : "unknown" }));
  for (const sessionId of presentSessions) if (!expectedSessions.has(sessionId)) { sessions.push({ sessionId, expected: false, availability: "present" }); issues.push(`Unexpected session ${sessionId}`); }
  for (const session of sessions) if (session.availability !== "present") issues.push(`Expected session ${session.sessionId} ${session.availability}`);
  return {
    cohort, cohortSha256, observerHostId, victimHostId, ...(sourceId ? { sourceId } : {}), complete: authoritative && issues.length === 0,
    topologyQualification: "not-established-by-inventory", ...(manifest ? { manifest } : {}), ...(manifestProvenance ? { manifestProvenance } : {}), ...(journalProvenance ? { journalProvenance } : {}),
    sessions, attempts, events, traces: traceRows, journalRecords: records, unknownRecords, issues,
  };
}
