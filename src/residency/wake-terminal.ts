import path from "node:path";
import { createHash } from "node:crypto";
import { assertMeshStateReadable, type MeshStore } from "../mesh/store.js";
import { ParticipantDirectory } from "../topology/participant-directory.js";
import { hostLeasePath, readHostLeaseCurrent } from "../topology/host-leases.js";
import { participantFilePresent } from "../topology/participant-files.js";
import { isLiveLegacyRootEntry } from "../topology/legacy-root-liveness.js";
import { acquireMainPublicationFence } from "../topology/main-publication-fence.js";
import { canonicalResidentWakeConfig, readWakeJson, retainedRoutesAt } from "./wake-index.js";

/** Terminal disposition is irreversible for this delivery: a late heartbeat is
 * not Main-death proof. Wait at least one minute AND three actual host-lease TTLs
 * beyond expiry (default native TTL is 15 s; longer/reload leases scale with it). */
export const RESIDENT_WAKE_TERMINAL_MIN_GRACE_MS = 60_000;
const RESIDENT_WAKE_TERMINAL_GRACE_TTLS = 3;

const nativeRootLeasePastGrace = (meshRoot: string, rootId: string, now: number): boolean => {
  const lease = readHostLeaseCurrent(meshRoot, rootId);
  // Bridge/transport projections do not share the native Main writer's
  // lease/publication protocol; their expiry is not fenced absence.
  // A missing lease (even after clean close/reaping) carries no expiry evidence.
  // Without consuming a native close proof, retain retry rather than infer death.
  if (!lease || lease.id !== rootId || lease.rootId !== rootId || lease.identityId !== rootId ||
      lease.mirror !== undefined || lease.mirrorLease === 1 ||
      !Number.isSafeInteger(now) || now <= 0 || !Number.isSafeInteger(lease.updatedAt) || lease.updatedAt <= 0 ||
      !Number.isSafeInteger(lease.expiresAt) || lease.expiresAt <= lease.updatedAt ||
      (lease.reloadUntil !== undefined && (!Number.isSafeInteger(lease.reloadUntil) ||
        lease.reloadUntil <= 0 || lease.reloadUntil > lease.expiresAt))) return false;
  const grace = Math.max(RESIDENT_WAKE_TERMINAL_MIN_GRACE_MS,
    RESIDENT_WAKE_TERMINAL_GRACE_TTLS * (lease.expiresAt - lease.updatedAt));
  const terminalAfter = lease.expiresAt + grace;
  // Overflow, malformed clocks and the exact boundary cannot authorize an ACK.
  if (!Number.isSafeInteger(terminalAfter) || now <= terminalAfter) return false;
  // The shared reader validates the lease schema but drops unrecognized mirrorLease
  // values. Only ABSENT raw provenance can certify a native lease, never a dropped
  // discriminator. Read raw LAST and require congruent identity/timing, including
  // optional fields the reader rejects: two different snapshots must not combine
  // an old expiry with new provenance or hide invalid incarnation/reload metadata.
  // This check runs on observation AND again under both terminal fences.
  const raw = readWakeJson<Record<string, unknown>>(hostLeasePath(meshRoot, rootId));
  if (!raw || typeof raw !== "object" || Array.isArray(raw) || raw.format !== 1 ||
      Object.hasOwn(raw, "mirrorLease") || Object.hasOwn(raw, "mirror")) return false;
  return (["id", "rootId", "identityId", "updatedAt", "expiresAt", "startedAt", "incarnationToken", "reloadUntil"] as const)
    .every(field => raw[field] === lease[field]) &&
    canonicalResidentWakeConfig({ session: raw.session }) === canonicalResidentWakeConfig({ session: lease.session });
};

/** Exceptional-path only: owner.json describes the resident, never its originating Main. */
export function residentWakeTerminalRoot(mesh: Pick<MeshStore, "root" | "listAll">, root: string): string | undefined {
  try {
    const routes = retainedRoutesAt(root);
    if (!routes || typeof routes.rootId !== "string" || !routes.rootId.length) return;
    const config = JSON.parse(routes.configJson);
    // FILE observations cannot certify absence in a transport-owned directory.
    if (config.mesh?.transport === "nats") return;
    const store = mesh as MeshStore;
    if (typeof store.get !== "function" || typeof store.stateToken !== "function") return;
    assertMeshStateReadable(mesh.root);
    const rootId = routes.rootId;
    const now = Date.now();
    if (!nativeRootLeasePastGrace(mesh.root, rootId, now)) return;
    // A read-only directory is never started: no heartbeat, registration or timer.
    // Use the resident's identity so legacy Main advertisements are not excluded as self.
    const directory = new ParticipantDirectory(store, { enabled: true, hostId: routes.hostId,
      rootId: routes.hostId, identity: { id: routes.hostId, name: "Fabric resident host", kind: "agent" }, reapDeadHosts: false });
    if (directory.get(rootId, now, { fresh: true })) return;
    const key = `topology/participants/${createHash("sha256").update(rootId).digest("hex")}`;
    const shared = store.get(key, { fresh: true });
    const file = readWakeJson<{ value?: unknown }>(path.join(mesh.root, "participants", `${key.slice(key.lastIndexOf("/") + 1)}.json`));
    if (participantFilePresent(mesh.root, key) && !file) return;
    if (shared || file) {
      const known = directory.lastKnown(rootId, now);
      // Invalid/ambiguous presence or a missing owner is unknown, not an expired Main.
      if (!known || !Number.isFinite(known.lapsedMs) || known.participant.kind !== "root" ||
          known.participant.rootId !== rootId || known.participant.ownerHostId !== rootId ||
          known.participant.ownerIdentityId !== rootId || known.participant.remoteHost !== undefined) return;
      const { local: _local, stale: _stale, bridgeStale: _bridgeStale, ...record } = known.participant;
      if (record.status === "reloading" && (record.reloadUntil ?? Infinity) >= now) return;
      for (const entry of [shared, file]) if (entry &&
          canonicalResidentWakeConfig(entry.value) !== canonicalResidentWakeConfig(record)) return;
    }
    const sessionId = typeof config.sessionId === "string" ? config.sessionId : rootId.startsWith("session:") ? rootId.slice(8) : undefined;
    if (sessionId !== undefined) {
      const legacy = store.get(`sessions/${sessionId}`, { fresh: true });
      // Ignore only a well-formed, expired legacy advertisement.
      if (legacy && (!isLiveLegacyRootEntry(legacy, -Infinity, mesh.root) || isLiveLegacyRootEntry(legacy, now, mesh.root))) return;
    }
    // This is only an observation/prefilter. Terminal callers MUST retain native
    // publication AND lease custody through their final check, receipt and ACK.
    if (directory.get(rootId, Date.now(), { fresh: true })) return;
    if (!nativeRootLeasePastGrace(mesh.root, rootId, Date.now())) return;
    return rootId;
  } catch { return; } // Unknown liveness/configuration must retain the existing retry.
}

/** No global mesh custody here. Main publication probes this kernel fence without
 * waiting; native lease claims/renewals and presence/session commits all enter the
 * root host's lease gate. Take no participant key while holding that gate (native
 * order is participant key -> lease gate -> shared state), then keep BOTH fences
 * through the caller's receipt I/O and short keyed mesh ACK. Busy/unknown refuses. */
export async function withResidentWakeTerminalRoot<T>(mesh: Pick<MeshStore, "root" | "listAll">,
  root: string, operation: (rootId: string) => Promise<T>): Promise<T | undefined> {
  // Do not fence an already-live Main just to discover it is live.
  const rootId = residentWakeTerminalRoot(mesh, root);
  const store = mesh as MeshStore;
  if (rootId === undefined || typeof store.leaseCustody !== "function") return;
  const release = await acquireMainPublicationFence(mesh.root, rootId);
  try {
    return await store.leaseCustody(hostLeasePath(mesh.root, rootId), async () => {
      if (residentWakeTerminalRoot(mesh, root) !== rootId) return;
      return operation(rootId);
    }, 0);
  } finally { release(); }
}
