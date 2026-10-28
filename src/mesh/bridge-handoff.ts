import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import { BRIDGE_HANDOFF_PREFIX, bridgeHandoffOf, bridgeHandoffScope, type BridgeHandoff, isCanonicalRootId, validBridgeName } from "./bridge.js";
import type { MeshBatchOperation, MeshBatchView, MeshIdentity, MeshStateEntry, MeshStore } from "./store.js";
import { type FabricHostLease, hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, readHostLeases } from "../topology/host-leases.js";
import { participantFilePresent, readParticipantFile, readParticipantFiles, removeParticipantFileIf } from "../topology/participant-files.js";
import { ROOT_ID_PREFIX } from "../topology/root-inbox.js";

/**
 * The hub's handoff command (smarty-dev#9074): `mesh-bridge handoff --mesh ROOT --session ID --link NAME`.
 *
 * A Main moved from the hub to a remote host keeps its session id; its dead native records on the
 * hub reserve that id, so the hub's bridge refuses the moved Main forever. Run on the hub, after the
 * hub's Pi for that session stopped, this command verifies that no native holds the identity (no
 * live host lease, Main session lease or reload lease; no live process recorded for it on this host;
 * no other link holding a live mirror of it), retires the dead native's own records (host record,
 * root participant record and file), and writes, in the same state commit, a handoff record
 * "native identity <id> handed off to link <link>" with its time and operator. The bridge then lets
 * that one link mirror the identity (see bridgeHandoffScope in bridge.ts).
 *
 * Authentication is local write access to the hub's mesh: the bridge writes only mirror records
 * and bridged events into it, and agents cannot write topology/ keys through the mesh provider,
 * so no remote party can write a handoff.
 */

const HOST_PREFIX = "topology/hosts/";
const PARTICIPANT_PREFIX = "topology/participants/";
const keyFor = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");

/** A command argument the operator must fix (exit 2). */
export class BridgeHandoffUsageError extends Error {}

/** The hub still holds the identity, or it changed while the command ran: nothing was written. */
export class BridgeHandoffRefusedError extends Error {
  constructor(readonly reason: string, readonly evidence: BridgeHandoffEvidence) {
    super(reason);
  }
}

export interface BridgeHandoffEvidence {
  checkedAt: string;
  hub: string;
  id: string;
  host: { remoteHost: string | null; startedAt: number | null; expiresAt: number; version: number } | null;
  lease: { expiresAt: number; sessionExpiresAt: number | null; reloadUntil: number | null; writer: string | null } | null;
  participants: Array<{ source: "state" | "file"; remoteHost: string | null; status: string | null; updatedAt: number; process: string | null }>;
}

export interface BridgeHandoffRequest {
  /** The session id (`<session id>` or `session:<session id>`). */
  session: string;
  /** The bridge link (the hub bridge's --remote name) that may present the identity. */
  link: string;
  operator: string;
  now?: () => number;
  hostname?: string;
}

export interface BridgeHandoffResult {
  record: BridgeHandoff;
  /** The dead records this handoff removed. */
  retired: string[];
  /** The handoff it replaced, if any. */
  previous?: BridgeHandoff;
  evidence: BridgeHandoffEvidence;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const markOf = (value: unknown): string | undefined =>
  isObject(value) && typeof value.remoteHost === "string" ? value.remoteHost : undefined;
const iso = (at: number): string => Number.isFinite(at) ? new Date(at).toISOString() : String(at);

/** The canonical root id for a session argument, or a usage error. */
export const handoffRootId = (session: string): string => {
  const id = session.startsWith(ROOT_ID_PREFIX) ? session : ROOT_ID_PREFIX + session;
  if (!isCanonicalRootId(id)) throw new BridgeHandoffUsageError(`Not a Main session id: ${session}`);
  return id;
};

// The present/absent answer of a file, failing closed: anything but ENOENT counts as present.
const present = (file: string): boolean => {
  try { fs.lstatSync(file); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
};

// /proc/<pid>/stat field 22: the process start time in clock ticks since boot.
const startTicks = (pid: number): string | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
  } catch { return undefined; }
};

type ProcessVerdict = { held: boolean; detail: string };

// Whether /proc/<pid> exists: true, false (ENOENT), or undefined (cannot tell).
const procExists = (pid: number): boolean | undefined => {
  try { fs.statSync(`/proc/${pid}`); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT" ? false : undefined; }
};

/** A process recorded for the identity holds it unless it is proven gone from this host. */
const processVerdict = (
  pid: unknown, host: unknown, hostname: string, sameProcess: (pid: number) => boolean | undefined,
): ProcessVerdict => {
  if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) return { held: true, detail: "recorded process id is invalid" };
  if (host !== hostname) return { held: true, detail: `pid ${pid} was recorded on host ${String(host)}, not this host; cannot verify it stopped` };
  if (process.platform !== "linux") return { held: true, detail: `pid ${pid}: cannot verify a process on ${process.platform}` };
  const exists = procExists(pid);
  if (exists === false) return { held: false, detail: `pid ${pid} does not exist on this host` };
  if (exists === undefined) return { held: true, detail: `pid ${pid}: /proc is unreadable` };
  const same = sameProcess(pid);
  if (same === false) return { held: false, detail: `pid ${pid} was reused by another process` };
  return { held: true, detail: same ? `process ${pid} is alive on this host` : `pid ${pid} exists and cannot be told from the recorded process` };
};

// A root participant's Main process (smarty-dev#7817): pid, host and /proc start ticks. An older
// writer recorded none; then the leases alone decide.
const mainProcessVerdict = (main: unknown, hostname: string): ProcessVerdict | undefined => {
  if (main === undefined) return undefined;
  if (!isObject(main)) return { held: true, detail: "recorded Main process is malformed" };
  return processVerdict(main.pid, main.host, hostname, (pid) => {
    if (typeof main.startTime !== "string") return undefined;
    const current = startTicks(pid);
    return current === undefined ? undefined : current === main.startTime;
  });
};

// A host lease's writer (census data): pid, host and its start time in epoch ms (10 s tolerance).
const writerVerdict = (writer: NonNullable<FabricHostLease["writer"]>, hostname: string): ProcessVerdict =>
  processVerdict(writer.pid, writer.host, hostname, (pid) => {
    const ticks = Number(startTicks(pid));
    let uptime: number;
    try { uptime = Number(fs.readFileSync("/proc/uptime", "utf8").split(/\s+/)[0]); } catch { return undefined; }
    if (!Number.isFinite(ticks) || !Number.isFinite(uptime)) return undefined;
    return Math.abs(Date.now() - (uptime - ticks / 100) * 1000 - writer.startedAt) <= 10_000;
  });

interface Observation {
  hostEntry: MeshStateEntry | undefined;
  participantEntry: MeshStateEntry | undefined;
  fileEntry: MeshStateEntry | undefined;
  filePresent: boolean;
  lease: FabricHostLease | undefined;
  leasePresent: boolean;
}

interface Assessment {
  refusals: string[];
  retire: { host?: MeshStateEntry; participant?: MeshStateEntry; file?: MeshStateEntry };
  name?: string;
  evidence: BridgeHandoffEvidence;
}

/** Whether the hub still holds `id`, and which dead records a handoff to `link` retires. */
const assess = (id: string, link: string, now: number, hostname: string, seen: Observation): Assessment => {
  const refusals: string[] = [];
  const retire: Assessment["retire"] = {};
  let name: string | undefined;
  const evidence: BridgeHandoffEvidence = { checkedAt: iso(now), hub: hostname, id, host: null, lease: null, participants: [] };
  const { hostEntry, lease } = seen;
  if (seen.leasePresent && !lease) refusals.push("the host lease file is unreadable or invalid");
  const hostMark = hostEntry ? markOf(hostEntry.value) : undefined;
  if (hostEntry) {
    const value = hostEntry.value;
    if (!isObject(value) || value.id !== id) refusals.push("the host record is malformed");
    else {
      const until = hostEntryLiveness(hostEntry, new Map(lease ? [[id, lease]] : [])).expiresAt;
      evidence.host = { remoteHost: hostMark ?? null, startedAt: typeof value.startedAt === "number" ? value.startedAt : null,
        expiresAt: until, version: hostEntry.version };
      if (hostMark === undefined) {
        if (until >= now) refusals.push(`the native host lease is live until ${iso(until)}`);
        else retire.host = hostEntry;
      } else if (hostMark !== link) {
        if (until >= now) refusals.push(`bridge link ${hostMark} holds a live mirror of it until ${iso(until)}; stop the Main there first`);
        else retire.host = hostEntry;
      }
    }
  }
  // The lease file is a native's unless the host record is a mirror; census or session data, which
  // a mirror lease never carries, marks it a native's anyway.
  if (lease && (hostMark === undefined || lease.writer !== undefined || lease.session !== undefined)) {
    const until = Math.max(lease.expiresAt, lease.session?.expiresAt ?? -Infinity, lease.reloadUntil ?? -Infinity);
    const writer = lease.writer ? writerVerdict(lease.writer, hostname) : undefined;
    evidence.lease = { expiresAt: lease.expiresAt, sessionExpiresAt: lease.session?.expiresAt ?? null,
      reloadUntil: lease.reloadUntil ?? null, writer: writer?.detail ?? null };
    if (until >= now) refusals.push(`the native host lease file is live until ${iso(until)}`);
    if (writer?.held) refusals.push(`host lease writer: ${writer.detail}`);
  }
  if (seen.filePresent && !seen.fileEntry) refusals.push("the root participant file is unreadable or invalid");
  for (const [source, entry] of [["file", seen.fileEntry], ["state", seen.participantEntry]] as const) {
    if (!entry) continue;
    const value = entry.value;
    if (!isObject(value) || value.id !== id) { refusals.push(`the ${source} root participant record is malformed`); continue; }
    // A participant file is always a native's own.
    const mark = source === "file" ? undefined : markOf(value);
    const main = mark === undefined ? mainProcessVerdict(value.mainProcess, hostname) : undefined;
    evidence.participants.push({ source, remoteHost: mark ?? null, status: typeof value.status === "string" ? value.status : null,
      updatedAt: entry.updatedAt, process: main?.detail ?? null });
    if (typeof value.name === "string" && value.name && (mark === undefined || mark === link)) name ??= value.name;
    if (mark === link) continue;                                  // this link's own mirror stays
    if (mark !== undefined) { retire.participant = entry; continue; } // another link's: its host was judged above
    if (value.kind !== "root" || value.ownerHostId !== id || value.ownerIdentityId !== id) {
      refusals.push(`the ${source} participant record is not this root's own`);
    }
    if (typeof value.reloadUntil === "number" && value.reloadUntil >= now) refusals.push(`the Main is reloading until ${iso(value.reloadUntil)}`);
    if (main?.held) refusals.push(`Main process: ${main.detail}`);
    if (source === "file") retire.file = entry;
    else retire.participant = entry;
  }
  return { refusals, retire, ...(name ? { name } : {}), evidence };
};

const observe = (store: MeshStore, id: string, view?: MeshBatchView): Observation => {
  const participantKey = keyFor(PARTICIPANT_PREFIX, id);
  const get = (key: string): MeshStateEntry | undefined => view ? view.get(key) : store.get(key, { fresh: true });
  return {
    hostEntry: get(keyFor(HOST_PREFIX, id)),
    participantEntry: get(participantKey),
    fileEntry: readParticipantFile(store.root, participantKey),
    filePresent: participantFilePresent(store.root, participantKey),
    lease: readHostLeaseCurrent(store.root, id),
    leasePresent: present(hostLeasePath(store.root, id)),
  };
};

const commandIdentity = (hostname: string): MeshIdentity => ({ id: `bridge-handoff:${hostname}`, name: "mesh-bridge handoff", kind: "main" });

/**
 * Hand the dead native identity of `request.session` over to bridge link `request.link`. Throws
 * BridgeHandoffRefusedError, with what it observed, when the hub still holds the identity; then
 * nothing is written (a dead participant file it already removed stays removed).
 */
export const handOffNativeIdentity = async (store: MeshStore, request: BridgeHandoffRequest): Promise<BridgeHandoffResult> => {
  const id = handoffRootId(request.session);
  if (!validBridgeName(request.link)) throw new BridgeHandoffUsageError(`Invalid bridge link name: ${request.link}`);
  const operator = request.operator.trim();
  if (!operator) throw new BridgeHandoffUsageError("An operator name is required");
  const now = request.now ?? Date.now;
  const hostname = request.hostname ?? os.hostname();
  const judge = (view?: MeshBatchView): Assessment => {
    const assessment = assess(id, request.link, now(), hostname, observe(store, id, view));
    if (assessment.refusals.length) throw new BridgeHandoffRefusedError(assessment.refusals.join("; "), assessment.evidence);
    return assessment;
  };
  const fileRetired: string[] = [];
  // The native's own file first, under its key lock and judged again there: if the state commit
  // below then refuses, only a dead native's file is gone (the host reaper's own work).
  // What it observed before retiring anything: the evidence it reports and the name it records.
  const first = judge();
  const file = first.retire.file;
  if (file) {
    const removed = await removeParticipantFileIf({ root: store.root, custody: (operation) => store.custody(operation) }, file.key,
      (current) => current.version === file.version && current.updatedAt === file.updatedAt && !assess(id, request.link, now(), hostname,
        observe(store, id)).refusals.length);
    if (!removed) throw new BridgeHandoffRefusedError("the root participant file changed during the handoff; nothing was written, retry",
      assess(id, request.link, now(), hostname, observe(store, id)).evidence);
    fileRetired.push(`participant file ${file.key}`);
  }
  let result: BridgeHandoffResult | undefined;
  const handoffKey = keyFor(BRIDGE_HANDOFF_PREFIX, id);
  await store.writeBatch({
    identity: commandIdentity(hostname),
    lockTiming: { operation: "bridge-handoff-grant" },
    ops: [],
    // Judged again on the commit's own snapshot, so a native that came back since is never retired.
    prepare: (view) => {
      const assessment = judge(view);
      if (assessment.retire.file) throw new BridgeHandoffRefusedError("a root participant file appeared during the handoff; nothing was written",
        assessment.evidence);
      const existing = view.get(handoffKey);
      const previous = existing ? bridgeHandoffOf(existing) : undefined;
      const name = assessment.name ?? first.name ?? previous?.name;
      const at = now();
      const record: BridgeHandoff = { format: 1, id, sessionId: id.slice(ROOT_ID_PREFIX.length), ...(name ? { name } : {}),
        link: request.link, handedOffAt: at, operator, hub: hostname };
      // A backend may run prepare again on a newer snapshot: everything below is rebuilt each time.
      const retired = [...fileRetired];
      const ops: MeshBatchOperation[] = [];
      for (const [what, entry] of [["host record", assessment.retire.host], ["participant record", assessment.retire.participant]] as const) {
        if (!entry) continue;
        ops.push({ kind: "delete", key: entry.key, ifVersion: entry.version, onConflict: "abort" });
        retired.push(`${what} ${entry.key}${markOf(entry.value) ? ` (lapsed mirror of link ${markOf(entry.value)})` : ""}`);
      }
      ops.push({ kind: "put", key: handoffKey, value: record, ifVersion: view.version(handoffKey), onConflict: "abort" });
      result = { record, retired, ...(previous ? { previous } : {}), evidence: first.evidence };
      return ops;
    },
  });
  if (!result) throw new Error("The handoff commit did not run");
  return result;
};

/** Remove the handoff of `session`; returns what it removed, if anything. */
export const revokeHandoff = async (store: MeshStore, session: string, hostname = os.hostname()): Promise<unknown> => {
  const key = keyFor(BRIDGE_HANDOFF_PREFIX, handoffRootId(session));
  let removed: unknown;
  await store.writeBatch({
    identity: commandIdentity(hostname),
    lockTiming: { operation: "bridge-handoff-revoke" },
    ops: [],
    prepare: (view) => {
      const entry = view.get(key);
      removed = entry?.value;
      return entry ? [{ kind: "delete", key, ifVersion: entry.version, onConflict: "abort" }] : [];
    },
  });
  return removed;
};

/** Every handoff on the hub, and whether a native voided it (or it is malformed: in force nowhere). */
export const listHandoffs = (store: MeshStore, now = Date.now()): Array<{ handoff: BridgeHandoff | unknown; inForce: boolean }> => {
  const entries = store.listAll(BRIDGE_HANDOFF_PREFIX, { fresh: true });
  const scope = bridgeHandoffScope(entries, store.listAll(HOST_PREFIX, { fresh: true }),
    [...store.listAll(PARTICIPANT_PREFIX, { fresh: true }), ...readParticipantFiles(store.root, { maxAgeMs: 0 })],
    () => readHostLeases(store.root), now, "");
  return entries.map((entry) => {
    const handoff = bridgeHandoffOf(entry);
    return { handoff: handoff ?? entry.value, inForce: handoff !== undefined && !scope.voided.includes(entry) };
  });
};
