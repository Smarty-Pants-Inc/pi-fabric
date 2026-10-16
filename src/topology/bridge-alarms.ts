import { createHash } from "node:crypto";
import { emitMeshJournalAlarm, processIncarnation, readPhysicalHostIdentity, validProcessIncarnation } from "../core/atomic-write.js";
import { meshAlarmRecipient, type MeshIdentity, type MeshStore } from "../mesh/store.js";
import { hostMirrorLease, MIRROR_FRESH_MS, readHostLeases, type BridgeLeaseProcess } from "./host-leases.js";

/** Mirror observations already renew before their 15 s cap, even without event traffic.
 * Three missed freshness intervals are a stalled link; no new timer or probe traffic. */
export const BRIDGE_LEASE_ALARM_AFTER_MS = 3 * MIRROR_FRESH_MS;
const NAME = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
interface Observation { process: BridgeLeaseProcess; observedAt: number; expiresAt: number }
interface Episode { observedAt: number; sinceMs: number }
const episodes = new WeakMap<MeshStore, Map<string, Episode>>();

/** Called only on the existing committed owner presence pass. Reads existing lease files;
 * publishes owner alarms, but never changes leases, cursors, topology or bridge processes.
 * Empty links have no mirrored owner to lose and intentionally remain timer/probe-free. */
export const bridgeLeaseAlarms = async (mesh: MeshStore, identity: MeshIdentity, now = Date.now()): Promise<void> => {
  const physical = readPhysicalHostIdentity();
  if (!physical) return; // Unverified locality must not turn a foreign pid into a local bridge.
  const leases = readHostLeases(mesh.root);
  const links = new Map<string, Observation>();
  for (const entry of mesh.listAll("topology/hosts/", { fresh: true })) {
    const host = entry.value;
    if (!object(host) || typeof host.id !== "string" || host.rootId !== host.id ||
      entry.key !== `topology/hosts/${createHash("sha256").update(host.id).digest("hex")}` ||
      !object(host.identity) || host.identity.id !== host.id || entry.updatedBy.id !== host.id ||
      typeof host.remoteHost !== "string" || !NAME.test(host.remoteHost) ||
      typeof host.startedAt !== "number" || typeof host.expiresAt !== "number" ||
      (host.incarnationToken !== undefined && typeof host.incarnationToken !== "string")) continue;
    const mirror = hostMirrorLease(leases, {
      id: host.id, rootId: host.id, identity: { id: host.id }, startedAt: host.startedAt,
      expiresAt: host.expiresAt, remoteHost: host.remoteHost,
      ...(typeof host.incarnationToken === "string" ? { incarnationToken: host.incarnationToken } : {}),
    });
    const writer = mirror?.bridgeProcess;
    const link = host.bridgeVia ?? host.remoteHost;
    if (!mirror || !writer || typeof link !== "string" || !NAME.test(link) || writer.link !== link ||
      writer.machineId !== physical.machineId || writer.bootId !== physical.bootId ||
      !Number.isSafeInteger(writer.pid) || writer.pid <= 0 || typeof writer.incarnation !== "string" ||
      !validProcessIncarnation(writer.incarnation) || !Number.isFinite(mirror.observedAt) || mirror.observedAt > now ||
      !(mirror.sourceExpiresAt > mirror.sourceObservedAt)) continue;
    const key = `${link}:${writer.pid}:${writer.incarnation}`;
    const previous = links.get(key);
    if (!previous || mirror.observedAt > previous.observedAt) links.set(key, {
      process: writer, observedAt: mirror.observedAt,
      expiresAt: Math.min(leases.get(host.id)!.expiresAt, mirror.observedAt + MIRROR_FRESH_MS),
    });
  }
  let active = episodes.get(mesh);
  if (!active) { active = new Map(); episodes.set(mesh, active); }
  for (const key of active.keys()) if (!links.has(key)) active.delete(key);
  for (const [key, observation] of links) {
    const { process: writer, observedAt, expiresAt } = observation;
    // Fresh native creation identity, not kill(pid, 0), hostname, or a copied source pid.
    if (await processIncarnation(writer.pid) !== writer.incarnation) { active.delete(key); continue; }
    const episode = active.get(key);
    const fresh = expiresAt > now;
    if (!episode && (fresh || now - observedAt <= BRIDGE_LEASE_ALARM_AFTER_MS)) continue;
    if (episode && !fresh) continue;
    const state = fresh ? "recovered" : "outage";
    const sinceMs = episode?.sinceMs ?? expiresAt;
    const sampleAgeMs = Math.max(0, now - observedAt);
    const lapsedMs = Math.max(0, now - expiresAt);
    const data = { link: writer.link, pid: writer.pid, lastObservedAt: observedAt, sampleAgeMs, lapsedMs, sinceMs };
    const text = fresh ? `Bridge link ${writer.link} recovered; mirrored lease renewed`
      : `Bridge link ${writer.link} stalled: mirrored lease lapsed ${lapsedMs} ms ago (sample age ${sampleAgeMs} ms); bridge process ${writer.pid} is running`;
    const to = meshAlarmRecipient();
    // Publish's durable dedupe covers concurrent/replacement owner sweeps, without a new
    // shared-state write or custody lock. Recovery re-arms only on actual fresh authority.
    if (to) await mesh.publish({ topic: "ops.owner", kind: fresh ? "bridge.lease.recovered" : "bridge.lease.alarm",
      from: identity, to, text, data,
      dedupeKey: `bridge-lease:${key}:${episode?.observedAt ?? observedAt}:${state}` });
    else emitMeshJournalAlarm({ label: `mesh bridge ${writer.link}`, state,
      durationMs: Math.max(0, now - sinceMs), reason: "bridge-lease-stale", ...data });
    if (fresh) active.delete(key);
    else active.set(key, { observedAt, sinceMs });
  }
};
