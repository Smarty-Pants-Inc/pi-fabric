import { JetStreamApiError, type ConsumerInfo, type JetStreamManager, type StoredMsg } from "@nats-io/jetstream";
import { leaseValue, presenceValue } from "../../topology/participant-view.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../../topology/types.js";
import type { TransportKvEntry } from "../types.js";
import { transportDurableName, transportInboxFilter, transportInboxStream, validateMachine } from "./subjects.js";

// This capability deliberately excludes JS clients, KV handles and every mutation API.
type ReadOnlyManager = {
  consumers: Pick<JetStreamManager["consumers"], "list">;
  streams: Pick<JetStreamManager["streams"], "info" | "getMessage">;
};
const DEFAULT_OLDER_THAN_MS = 24 * 60 * 60_000;
const TERMINAL = new Set(["completed", "failed", "cancelled", "stopped", "timed_out", "done", "aborted"]);
const objectValue = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Deliberately small duration grammar; no floating point, zero, signed values or overflow. */
export function parseStaleConsumerAge(value = "24h"): number {
  const match = /^([1-9][0-9]*)(ms|s|m|h|d)$/.exec(value);
  const units: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 };
  const ms = match && match[0] === value ? Number(match[1]) * units[match[2]!]! : NaN;
  if (!Number.isSafeInteger(ms) || ms <= 0) throw new Error("--older-than requires a positive integer duration (ms, s, m, h or d)");
  return ms;
}

const timestampMs = (value: unknown): number | undefined => {
  // nats.js 3.4 types SequenceInfo.last_active as Nanos, but the broker sends an RFC3339 timestamp.
  if (typeof value !== "string" || value.trim() !== value ||
    !/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(value)) return undefined;
  const ms = Date.parse(value);
  const day = Date.parse(`${value.slice(0, 10)}T00:00:00Z`);
  // Date.parse normalizes impossible month days (e.g. February 31); do not age those into candidates.
  return Number.isFinite(ms) && Number.isFinite(day) && new Date(day).toISOString().slice(0, 10) === value.slice(0, 10) ? ms : undefined;
};

/** Broker delivery/ACK timestamps, NOT pull/request activity or process liveness telemetry. */
export function staleConsumerActivity(info: Pick<ConsumerInfo, "created" | "delivered" | "ack_floor">) {
  const stamps = (["delivered", "ack_floor"] as const).flatMap(source => {
    const value: unknown = info[source]?.last_active;
    return value === undefined || value === null ? [] : [{ source, value, ms: timestampMs(value) }];
  });
  // A malformed activity field must not turn an active consumer into an old created-time candidate.
  if (stamps.some(stamp => stamp.ms === undefined)) return undefined;
  const latest = stamps.sort((a, b) => b.ms! - a.ms!)[0];
  if (latest) return { timestamp: latest.value as string, ms: latest.ms!, source: latest.source };
  const ms = timestampMs(info.created);
  return ms === undefined ? undefined : { timestamp: info.created, ms, source: "created" as const };
}

type Diagnostic = { key?: string; reason: string };
type Snapshot<T> = {
  summary: { stream: string; filter: string; last_seq: number | null; complete: boolean; diagnostics: Diagnostic[] };
  rows: TransportKvEntry<T>[];
};

/** Sequence-bounded raw KV enumeration: INFO + MSG.GET only, never KV keys/watch (which create consumers). */
async function kvSnapshot<T>(manager: ReadOnlyManager, machine: string, bucket: string, kind: "p" | "h", now: number,
  validate: (entry: TransportKvEntry<unknown>) => T | undefined): Promise<Snapshot<T>> {
  const stream = `KV_${bucket}`;
  const prefix = `$KV.${bucket}.`;
  const filter = `${prefix}${machine}.${kind}.>`;
  const result: Snapshot<T> = { summary: { stream, filter, last_seq: null, complete: false, diagnostics: [] }, rows: [] };
  const diagnostics = result.summary.diagnostics;
  let info;
  try { info = await manager.streams.info(stream); }
  catch (error) {
    if (!(error instanceof JetStreamApiError) || error.apiError().err_code !== 10059) throw error;
    diagnostics.push({ reason: "stream-not-found; absence is unknown" });
    return result;
  }
  const last = info.state.last_seq;
  result.summary.last_seq = last;
  const ttl = info.config.max_age / 1_000_000;
  if (!Number.isFinite(ttl) || ttl <= 0 || info.config.max_msgs_per_subject !== 1 ||
    info.config.subjects.length !== 1 || info.config.subjects[0] !== `${prefix}>` || info.config.mirror || info.config.sources?.length) {
    diagnostics.push({ reason: "unexpected-KV-stream-policy; absence is unknown" });
    return result;
  }
  if (!Number.isSafeInteger(last) || last < 0 || !Number.isSafeInteger(info.state.first_seq) || info.state.first_seq < 0) {
    throw new Error(`invalid sequence bounds for ${stream}`);
  }
  const latest = new Map<string, StoredMsg>();
  let seq = Math.max(1, info.state.first_seq);
  while (seq <= last) {
    // The SDK's MsgRequest type omits NextMsgRequest; a named query retains its supported next_by_subj field.
    const query = { seq, next_by_subj: filter };
    let msg: StoredMsg | null;
    try { msg = await manager.streams.getMessage(stream, query); }
    catch (error) {
      // 3.4 normally maps 10037 to null. Accept that exact API error too, never a generic 404,
      // permission error, unavailable stream or request failure as evidence of filter exhaustion.
      if (!(error instanceof JetStreamApiError) || error.apiError().err_code !== 10037) throw error;
      break;
    }
    if (!msg || msg.seq > last) break;
    if (!Number.isSafeInteger(msg.seq) || msg.seq < seq || !msg.subject.startsWith(`${prefix}${machine}.${kind}.`)) {
      throw new Error(`invalid sequence or subject from ${stream}`);
    }
    latest.set(msg.subject, msg);
    seq = msg.seq + 1;
  }
  for (const msg of latest.values()) {
    const key = msg.subject.slice(prefix.length);
    const operation = msg.header.get("KV-Operation");
    const marker = msg.header.get("Nats-Marker-Reason");
    if (operation === "DEL" || operation === "PURGE" || marker === "MaxAge") continue;
    const storedAt = timestampMs(msg.timestamp);
    if (storedAt === undefined || storedAt > now || operation && operation !== "PUT" || marker) {
      diagnostics.push({ key, reason: "invalid-KV-metadata" });
      continue;
    }
    // An already expired value, even if retained during expiry processing, cannot establish live presence.
    if (storedAt + ttl <= now) continue;
    let wrapper: unknown;
    try { wrapper = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(msg.data)); }
    catch { diagnostics.push({ key, reason: "malformed-KV-wrapper" }); continue; }
    if (!objectValue(wrapper) || typeof wrapper.owner !== "string" || !wrapper.owner || !("value" in wrapper)) {
      diagnostics.push({ key, reason: "malformed-KV-wrapper" });
      continue;
    }
    const entry = { key, machine, owner: wrapper.owner, value: wrapper.value, revision: msg.seq };
    const value = validate(entry);
    if (!value) { diagnostics.push({ key, reason: "invalid-KV-identity-or-value" }); continue; }
    result.rows.push({ ...entry, value });
  }
  // Writes concurrent with the scan make negative observations uncertain. Do not chase a moving tail.
  const after = await manager.streams.info(stream);
  result.summary.complete = after.created === info.created && after.state.last_seq === last &&
    after.config.max_age === info.config.max_age && JSON.stringify(after.config) === JSON.stringify(info.config);
  if (!result.summary.complete) diagnostics.push({ reason: "stream-changed-during-snapshot; absence is unknown" });
  return result;
}

function hostPresence(hostId: string, machine: string, now: number,
  presence: Snapshot<FabricParticipantRecord>, leases: Snapshot<FabricKvHostLease>) {
  const records = presence.rows.filter(entry => entry.machine === machine && entry.value.ownerHostId === hostId);
  const active = records.filter(entry => !TERMINAL.has(entry.value.status));
  const lease = leases.rows.find(entry => entry.machine === machine && entry.value.id === hostId);
  const snapshotsKnown = [presence, leases].every(snapshot => snapshot.summary.complete && snapshot.summary.diagnostics.length === 0);
  const validLease = lease && lease.value.expiresAt > lease.value.updatedAt && lease.value.updatedAt <= now;
  const leaseLive = !!validLease && lease!.value.expiresAt > now;
  const hostLease = lease && !validLease ? "unknown" : leaseLive ? "live" : lease ? "expired" :
    leases.summary.complete && leases.summary.diagnostics.length === 0 ? "not-observed" : "unknown";
  const matched = leaseLive && active.some(entry => {
    const record = entry.value, owner = lease!.value;
    return entry.owner === lease!.owner && record.ownerIncarnation === owner.incarnation &&
      record.ownerIdentityId === owner.identityId && record.rootId === owner.rootId;
  });
  // Presence without its matching, unexpired lease is NOT a live-host assertion. It still vetoes candidacy.
  const live: boolean | null = matched ? true : !snapshotsKnown || active.length > 0 || hostLease === "unknown" ? null : false;
  return {
    row_exists: records.length > 0 ? true : presence.summary.complete && presence.summary.diagnostics.length === 0 ? false : null,
    unexpired_rows: records.length,
    nonterminal_rows: active.length,
    live,
    host_lease: hostLease,
  };
}

/** All aged, identity-validated per-host durables, including ineligible rows and exclusion diagnostics. */
export async function reportStaleConsumers(manager: ReadOnlyManager, machine: string,
  olderThanMs = DEFAULT_OLDER_THAN_MS, now = Date.now()) {
  validateMachine(machine);
  if (machine.trim() !== machine) throw new Error("invalid machine token");
  if (!Number.isSafeInteger(olderThanMs) || olderThanMs <= 0) throw new Error("invalid older-than duration");
  if (!Number.isSafeInteger(now) || !Number.isFinite(new Date(now).getTime())) throw new Error("invalid observation time");
  const stream = transportInboxStream(machine);
  const consumers: ConsumerInfo[] = [];
  const diagnostics: { consumer: string; reason: string }[] = [];
  let ignored = 0;
  for await (const info of manager.consumers.list(stream)) {
    if (!/^rt_[0-9a-f]{32}$/.test(info.name)) { ignored++; continue; }
    let description: unknown;
    try { description = JSON.parse(info.config.description ?? ""); } catch { /* diagnosed below */ }
    if (!objectValue(description) || typeof description.fabricHostId !== "string" || !description.fabricHostId ||
      description.machine !== machine || transportDurableName(description.fabricHostId) !== info.name ||
      info.config.durable_name !== info.name || info.stream_name !== stream ||
      info.config.filter_subject !== transportInboxFilter(machine, description.fabricHostId) || info.config.filter_subjects?.length) {
      diagnostics.push({ consumer: info.name, reason: "invalid-host-description-durable-or-filter" });
      continue;
    }
    consumers.push(info);
  }
  const presence = await kvSnapshot(manager, machine, "fabric_presence", "p", now, presenceValue);
  const leases = await kvSnapshot(manager, machine, "fabric_leases", "h", now, leaseValue);
  const rows = consumers.flatMap(info => {
    const activity = staleConsumerActivity(info);
    if (!activity || activity.ms > now) {
      diagnostics.push({ consumer: info.name, reason: "invalid-or-future-last-active" });
      return [];
    }
    const age = now - activity.ms;
    if (age < olderThanMs) return [];
    const hostId = (JSON.parse(info.config.description!) as { fabricHostId: string }).fabricHostId;
    const observed = hostPresence(hostId, machine, now, presence, leases);
    const reasons: string[] = [];
    if (![info.num_pending, info.num_ack_pending].every(count => Number.isSafeInteger(count) && count >= 0)) reasons.push("invalid-pending-counts");
    if (info.num_pending !== 0) reasons.push("pending");
    if (info.num_ack_pending !== 0) reasons.push("ack-pending");
    if (observed.nonterminal_rows > 0) reasons.push("unexpired-nonterminal-presence");
    if (observed.host_lease === "live") reasons.push("live-host-lease");
    if (observed.live === null || observed.host_lease === "unknown") reasons.push("presence-or-lease-unknown");
    return [{
      durable_name: info.name, fabricHostId: hostId, machine, filter_subject: info.config.filter_subject,
      created: info.created, last_active: activity.timestamp, last_active_source: activity.source, age_ms: age,
      num_pending: info.num_pending, num_ack_pending: info.num_ack_pending,
      presence: observed, eligible: reasons.length === 0, reasons,
    }];
  }).sort((a, b) => a.durable_name.localeCompare(b.durable_name));
  return {
    ok: true, mode: "report-only", disposition: "none", machine, stream, observed_at: new Date(now).toISOString(),
    older_than_ms: olderThanMs, selection: "all-aged-validated-per-host-durables", consumers: rows,
    eligible_count: rows.filter(row => row.eligible).length, ignored_consumers: ignored, diagnostics,
    snapshots: { presence: presence.summary, leases: leases.summary },
    last_active_semantics: "Latest broker delivered/ack_floor last_active, else created; not pull/request telemetry.",
    presence_semantics: "row_exists indicates a validated, broker-TTL-unexpired presence row, including terminal rows. live requires nonterminal presence and a matching owner/identity/root/incarnation lease, unexpired by broker TTL and expiresAt. Nonterminal presence alone is not host liveness; it conservatively vetoes eligibility. Malformed rows make negative observations unknown.",
    limitation: "Sequence-bounded, non-atomic read-only snapshots, not proof of host death or authorization to delete. Eligibility requires no pending or ACK-pending messages, no unexpired nonterminal presence, no live lease and no unknown snapshot evidence. No inbox payloads read; no mutation, pull, ACK, KV watch or keys API.",
  };
}
