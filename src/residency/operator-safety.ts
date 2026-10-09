import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import { processStartTime } from "./process-identity.js";
import { ResidentActorAuthorizationError, type ResidentHostConfig } from "./protocol.js";
import type { MeshStateEntry, MeshStore } from "../mesh/store.js";
import { participantFilePresent, readParticipantFile } from "../topology/participant-files.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, STATE_LEASE_RENEW_MS, type FabricHostLease } from "../topology/host-leases.js";

export interface ResidentOperatorEvidence {
  rootId: string;
  mainSessionId: string;
  lastLeaseTime: number | null;
  leaseExpiresAt: number | null;
  liveLease: boolean;
  /** A live root lease that this root's own resident host writes, with no live Main session in it (smarty-dev#7817). */
  residentRenewedLease?: true;
  /** With --main-stopped: the tool's own snapshot of the root's participants, their pids and the owner lease. */
  toolEvidence?: MainToolEvidence;
  /** With --main-stopped: what the root participant shows. */
  mainLiveness?: string;
  operatorCheck: string;
}

/** The process that may renew a Main-less root's lease itself: the resident host of that root. */
export interface ResidentLeaseWriter { pid: number; host: string; startedAt: number }

const operatorCheck = "Check the root's Main session and last lease time; run herdr agent list / ps to confirm that no live Main serves this root. A wrong confirmation can interrupt a live Main's actor.";
const refuse = (evidence: ResidentOperatorEvidence, reason: string): never => {
  throw new ResidentActorAuthorizationError(`${reason}; ${JSON.stringify(evidence)}; only after the manual check use --confirm-dead-root ${evidence.rootId}`);
};

/** Report lease facts only. An absent/expired lease never proves that Main is dead. */
export function readResidentOperatorEvidence(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">,
  resident?: ResidentLeaseWriter, options: MainStoppedOptions = {}): ResidentOperatorEvidence {
  const evidence: ResidentOperatorEvidence = { rootId: config.rootId, mainSessionId: config.sessionId,
    lastLeaseTime: null, leaseExpiresAt: null, liveLease: false, operatorCheck };
  // smarty-dev#7817: only a live lease proven to be the resident's own heartbeat (its exact writer
  // incarnation, no live Main session) is set aside; any other live lease, or doubt, is a live Main.
  let mainLive = false;
  let selfIncarnation: number | undefined;
  let selfWindow = 0;
  let rootLease: FabricHostLease | undefined;
  const record = (updatedAt: number, expiresAt: number, self = false) => {
    evidence.lastLeaseTime = Math.max(evidence.lastLeaseTime ?? updatedAt, updatedAt);
    evidence.leaseExpiresAt = Math.max(evidence.leaseExpiresAt ?? expiresAt, expiresAt);
    if (expiresAt < Date.now()) return;
    evidence.liveLease = true;
    if (self) evidence.residentRenewedLease = true; else mainLive = true;
  };
  const file = hostLeasePath(config.meshRoot, config.rootId);
  let present = false;
  try { fs.lstatSync(file); present = true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") refuse(evidence, "root lease is unreadable"); }
  if (present) {
    const lease = readHostLeaseCurrent(config.meshRoot, config.rootId);
    if (!lease || lease.rootId !== config.rootId || lease.identityId !== config.rootId) {
      refuse(evidence, "root lease is unreadable or invalid");
    }
    const writer = lease!.writer;
    const self = resident !== undefined && writer !== undefined && writer.pid === resident.pid &&
      writer.host === resident.host && writer.startedAt === resident.startedAt &&
      (lease!.session === undefined || lease!.session.expiresAt < Date.now());
    rootLease = lease!;
    if (self) { selfIncarnation = lease!.startedAt; selfWindow = Math.max(0, lease!.expiresAt - lease!.updatedAt); }
    record(lease!.updatedAt, lease!.expiresAt, self);
  }
  const shared = mesh.get("topology/hosts/" + createHash("sha256").update(config.rootId).digest("hex"), { fresh: true });
  if (shared) {
    const value = shared.value as { id?: string; rootId?: string; expiresAt?: number; identity?: { id?: string } };
    if (!value || value.id !== config.rootId || value.rootId !== config.rootId ||
        value.identity?.id !== config.rootId || !Number.isFinite(value.expiresAt)) refuse(evidence, "shared root lease is invalid");
    const lease = hostEntryLiveness(shared, new Map());
    // The shared record of the same incarnation as the resident's own lease is that same heartbeat.
    record(lease.updatedAt, lease.expiresAt, selfIncarnation !== undefined &&
      (value as { startedAt?: number }).startedAt === selfIncarnation);
  }
  // smarty-dev#7817: no automatic Main-dead proof (that is smarty-dev#7956). A resident-renewed lease
  // is set aside only on the operator's --main-stopped assertion, and the assertion never overrides a
  // live observation: any fresh, reloading or doubtful root participant, or a live owner lease, refuses.
  if (!mainLive && options.mainStopped) {
    const { verdict, snapshot } = rootParticipantVerdict(config, mesh, rootLease, selfWindow, resident);
    evidence.mainLiveness = verdict;
    evidence.toolEvidence = snapshot;
    if (!verdict.startsWith("absent:") && !verdict.startsWith("stale:")) mainLive = true;
  }
  if (!options.mainStopped && evidence.residentRenewedLease) mainLive = true;
  evidence.liveLease = mainLive;
  if (mainLive) delete evidence.residentRenewedLease;
  return evidence;
}

/** A live Main republishes its root participant at least every STATE_LEASE_RENEW_MS; a record newer
 * than twice that (never less than the lease window) is fresh, a live observation. */
export const ROOT_PARTICIPANT_FRESH_MS = 2 * STATE_LEASE_RENEW_MS;

export interface MainStoppedOptions {
  /** The operator asserts the root's Main process is gone (--main-stopped). */
  mainStopped?: boolean;
}

/** A pid on this host, checked by the tool: /proc/<pid> exists, and its start time against the record. */
export interface MainPidCheck { pid: number; host: string; onThisHost: boolean; alive: boolean | null; startMatches: boolean | null }

/** What the tool itself observed at removal time (--main-stopped is an operator attestation, not this). */
export interface MainToolEvidence {
  capturedAt: string;
  host: string;
  rootLease: { present: boolean; updatedAt?: number; expiresAt?: number; sessionExpiresAt?: number;
    writer?: { pid: number; host: string; release: string; startedAt: number; isResident: boolean; check?: MainPidCheck } };
  participants: Array<{ source: "file" | "state"; ownerHostId: string | null; status: string | null; lastSeen: number | null;
    pid: number | null; host: string | null; startTime: string | null; release: string | null; ownerLeaseExpiresAt: number | null;
    identity: string }>;
}

const CLK_TCK = 100;
/** /proc/<pid> exists on this host; its start time (from /proc/<pid>/stat) within 10 s of the recorded one. */
const checkPid = (pid: number, host: string, startedAt: number | undefined): MainPidCheck => {
  const onThisHost = host === os.hostname();
  if (!onThisHost || process.platform !== "linux" || !Number.isSafeInteger(pid) || pid <= 0) {
    return { pid, host, onThisHost, alive: null, startMatches: null };
  }
  const alive = fs.existsSync(`/proc/${pid}`);
  let startMatches: boolean | null = null;
  if (alive && startedAt !== undefined) {
    try {
      const ticks = Number(processStartTime(pid));
      const uptime = Number(fs.readFileSync("/proc/uptime", "utf8").split(/\s+/)[0]);
      if (Number.isFinite(ticks) && Number.isFinite(uptime)) {
        startMatches = Math.abs(Date.now() - (uptime - ticks / CLK_TCK) * 1000 - startedAt) <= 10_000;
      }
    } catch { /* unknown */ }
  }
  return { pid, host, onThisHost, alive, startMatches };
};

/** The recorded Main process is gone from this host ("gone: ..."), or why that cannot be verified. */
const mainProcessGone = (main: { pid?: unknown; host?: unknown; startTime?: unknown } | undefined): string => {
  if (process.platform !== "linux") return "unknown: Main process identity unavailable (non-Linux; smarty-dev#7956)";
  if (!main || typeof main.pid !== "number" || !Number.isSafeInteger(main.pid) || main.pid <= 0) {
    return "unknown: Main process identity unavailable (no pid in the root participant record)";
  }
  if (main.host !== os.hostname()) return `unknown: Main process identity unavailable (recorded on host ${String(main.host)}, not this host)`;
  if (processStartTime(process.pid) === undefined) return "unknown: Main process identity unavailable (/proc is unreadable)";
  let exists: boolean;
  try { fs.statSync(`/proc/${main.pid}`); exists = true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") return "unknown: Main process identity unavailable (/proc is unreadable)";
    exists = false;
  }
  if (!exists) return `gone: Main pid ${main.pid} does not exist on this host`;
  if (typeof main.startTime !== "string") return `unknown: Main pid ${main.pid} exists and the record has no start time`;
  const current = processStartTime(main.pid);
  if (current === undefined) return "unknown: Main process identity unavailable (/proc is unreadable)";
  if (current === main.startTime) return `live: Main process ${main.pid} is alive on this host`;
  return `gone: Main pid ${main.pid} was reused (start time ${current}, recorded ${main.startTime})`;
};

/** What the root participant shows; "absent:" and "stale:" are the only answers that do not refuse.
 * The snapshot is the tool's own evidence for the audit record. A participant's process (its owner
 * lease writer) that is alive on this host refuses, whatever the operator's attestation says. */
function rootParticipantVerdict(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">,
  rootLease: FabricHostLease | undefined, window: number, resident?: ResidentLeaseWriter): { verdict: string; snapshot: MainToolEvidence } {
  const now = Date.now();
  const fresh = Math.max(ROOT_PARTICIPANT_FRESH_MS, window);
  const isResident = (writer: { pid: number; host: string; startedAt: number }): boolean => resident !== undefined &&
    writer.pid === resident.pid && writer.host === resident.host && writer.startedAt === resident.startedAt;
  const writerOf = (lease: FabricHostLease | undefined) => lease?.writer ? {
    pid: lease.writer.pid, host: lease.writer.host, release: lease.writer.releaseSha, startedAt: lease.writer.startedAt,
    isResident: isResident(lease.writer),
    ...(isResident(lease.writer) ? {} : { check: checkPid(lease.writer.pid, lease.writer.host, lease.writer.startedAt) }),
  } : undefined;
  const rootWriter = writerOf(rootLease);
  const snapshot: MainToolEvidence = { capturedAt: new Date(now).toISOString(), host: os.hostname(),
    rootLease: { present: rootLease !== undefined,
      ...(rootLease ? { updatedAt: rootLease.updatedAt, expiresAt: rootLease.expiresAt } : {}),
      ...(rootLease?.session ? { sessionExpiresAt: rootLease.session.expiresAt } : {}),
      ...(rootWriter ? { writer: rootWriter } : {}) },
    participants: [] };
  const done = (verdict: string) => ({ verdict, snapshot });
  const key = "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex");
  let entries: Array<[ "file" | "state", MeshStateEntry ]>;
  try {
    const file = readParticipantFile(config.meshRoot, key);
    if (!file && participantFilePresent(config.meshRoot, key)) return done("unknown: root participant record is unreadable");
    const state = mesh.get(key, { fresh: true });
    entries = [...(file ? [["file", file] as ["file", MeshStateEntry]] : []), ...(state ? [["state", state] as ["state", MeshStateEntry]] : [])];
  } catch (error) { return done(`unknown: root participant record is unreadable (${error instanceof Error ? error.message : String(error)})`); }
  let verdict: string | undefined;
  const first = (candidate: string) => { verdict ??= candidate; };
  for (const [source, entry] of entries) {
    const value = entry.value as { id?: unknown; ownerHostId?: unknown; status?: unknown } | null;
    const ownerHostId = value && typeof value === "object" && typeof value.ownerHostId === "string" ? value.ownerHostId : null;
    const ownerLease = ownerHostId === null ? undefined
      : ownerHostId === config.rootId ? rootLease : readHostLeaseCurrent(config.meshRoot, ownerHostId);
    const writer = ownerHostId === config.rootId ? rootWriter : writerOf(ownerLease);
    const main = value && typeof value === "object" ? (value as { mainProcess?: unknown }).mainProcess as
      { pid?: unknown; host?: unknown; startTime?: unknown } | undefined : undefined;
    const identity = mainProcessGone(main);
    snapshot.participants.push({ source, ownerHostId, status: typeof value?.status === "string" ? value.status : null,
      lastSeen: Number.isFinite(entry.updatedAt) ? entry.updatedAt : null,
      pid: typeof main?.pid === "number" ? main.pid : null, host: typeof main?.host === "string" ? main.host : null,
      startTime: typeof main?.startTime === "string" ? main.startTime : null, release: writer?.release ?? null,
      ownerLeaseExpiresAt: (ownerHostId === config.rootId ? rootLease?.session?.expiresAt : ownerLease?.expiresAt) ?? null,
      identity });
    if (!value || typeof value !== "object" || value.id !== config.rootId || ownerHostId === null ||
        !Number.isFinite(entry.updatedAt)) { first("unknown: root participant record is invalid"); continue; }
    // The tool verifies the recorded Main process is gone (or its pid reused); unknown identity refuses.
    if (!identity.startsWith("gone:")) { first(identity); continue; }
    if (value.status === "reloading") { first("unknown: root participant is reloading"); continue; }
    if (now - entry.updatedAt <= fresh) { first("live: root participant is fresh"); continue; }
    // The root lease may be the resident's heartbeat; the Main's own liveness is its session in it.
    const ownerUntil = ownerHostId === config.rootId ? rootLease?.session?.expiresAt : ownerLease?.expiresAt;
    if (ownerHostId !== config.rootId && ownerLease === undefined) { first("unknown: participant owner lease is unreadable"); continue; }
    if (ownerUntil !== undefined && ownerUntil >= now) first("live: participant owner lease is live");
  }
  if (rootWriter?.check?.alive === true) first(`live: root lease writer process ${rootWriter.pid} is alive on this host`);
  return done(verdict ?? (entries.length ? "stale: root participant is stale and its owner lease expired" : "absent: no root participant"));
}

export const MAIN_STOPPED_REQUIRED = "the root's Main may be running; confirm it is stopped and pass --main-stopped (automatic proof: smarty-dev#7956)";

/** `mainStopped` undefined: the action needs no Main-stopped assertion (stop); false/true: it does (remove). */
export function assertResidentOperatorConfirmed(evidence: ResidentOperatorEvidence, confirmation?: string, dryRun = false,
  mainStopped?: boolean): void {
  if (dryRun && confirmation === undefined) return;
  if (confirmation !== evidence.rootId) refuse(evidence, confirmation === undefined
    ? "Missing --confirm-dead-root: operator confirmation is required"
    : "Mismatched --confirm-dead-root: value must equal the selected resident's root id exactly");
  if (!dryRun && mainStopped === false) refuse(evidence, MAIN_STOPPED_REQUIRED);
  if (!dryRun && evidence.liveLease) refuse(evidence, "Main has a live root lease; confirmation cannot override a live owner lease" +
    (evidence.mainLiveness ? ` (${evidence.mainLiveness})` : ""));
}
