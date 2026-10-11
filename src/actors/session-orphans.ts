import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ownProcessIncarnation } from "../core/atomic-write.js";
import { withStateFence } from "../mesh/commit-outbox.js";
import { assertMeshStateReadable, censusRecordAlive, type MeshIdentity, type MeshStateEntry, type MeshStore } from "../mesh/store.js";
import { residentProcessAlive } from "../residency/process-identity.js";
import { hostLeasePath, readHostLeaseCurrent, type FabricHostLease } from "../topology/host-leases.js";
import { isLiveLegacyRootEntry } from "../topology/legacy-root-liveness.js";
import { prepareParticipantFileLock, withParticipantFileTryLock } from "../topology/participant-files.js";
import { ActorRegistryStore } from "./registry-store.js";
import { FABRIC_ACTOR_HOST_EVENTS, type FabricActorInfo, type FabricActorSessionOrphan } from "./types.js";

/** Restart/reload grace, in addition to requiring positive owner death evidence. */
export const SESSION_ACTOR_ORPHAN_GRACE_MS = 120_000;
const SESSION_ACTOR_ORPHAN_TEXT = "session actor orphaned: its Main moved or ended; re-run activation.py in the new Main";
/** Display-only metadata, not stored identities or durable dedupe keys. Never split a JSON escape at the bound. */
const alarmLabel = (value: string): string => {
  let label = "";
  for (const character of value.replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g, "")) {
    const escaped = JSON.stringify(character).slice(1, -1);
    if (label.length + escaped.length > 128) break;
    label += escaped;
  }
  return label;
};
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");
const participantKey = (id: string): string => `topology/participants/${digest(id)}`;
const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const time = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value > 0;
const strings = (value: unknown): string[] => Array.isArray(value) ? value.filter((v): v is string => typeof v === "string") : [];
const until = (lease: FabricHostLease): number => Math.max(lease.expiresAt, lease.reloadUntil ?? 0, lease.session?.expiresAt ?? 0);

/** Strict destructive read: invalid optional liveness/owner evidence is doubt too. */
const leaseOf = (meshRoot: string, id: string): FabricHostLease | undefined => {
  const lease = readHostLeaseCurrent(meshRoot, id);
  if (!lease) return undefined;
  const raw = record(JSON.parse(fs.readFileSync(hostLeasePath(meshRoot, id), "utf8")));
  if (!raw || (raw.session !== undefined && !lease.session) || (raw.writer !== undefined && !lease.writer) ||
      !time(lease.updatedAt) || !time(lease.expiresAt)) return undefined;
  // Both reads must name the SAME lease generation. A renewal or replacement
  // between the strict parser and optional-field validation vetoes the verdict.
  for (const key of ["id", "rootId", "identityId", "updatedAt", "expiresAt", "startedAt", "reloadUntil", "session", "writer"] as const) {
    if (JSON.stringify(raw[key]) !== JSON.stringify(lease[key])) return undefined;
  }
  return lease;
};

/** Missing presence is absence; an unreadable/invalid record or owner lease is NOT absence. */
const noLiveParticipant = (mesh: MeshStore, id: string, now: number, deadOwner?: string): boolean => {
  const key = participantKey(id);
  const entries: MeshStateEntry[] = [];
  try {
    const file = path.join(mesh.root, "participants", `${digest(id)}.json`);
    try {
      const raw = record(JSON.parse(fs.readFileSync(file, "utf8")));
      if (!raw || raw.format !== 1 || raw.key !== key || !time(raw.updatedAt) ||
          typeof raw.version !== "number" || !record(raw.updatedBy)) return false;
      entries.push(raw as unknown as MeshStateEntry);
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false; }
    const shared = mesh.get(key, { fresh: true });
    if (shared) entries.push(shared);
    for (const entry of entries) {
      const value = record(entry.value);
      if (!value || value.id !== id || typeof value.ownerHostId !== "string" ||
          typeof value.ownerIdentityId !== "string" || typeof value.rootId !== "string" ||
          !time(entry.updatedAt) || now - entry.updatedAt < SESSION_ACTOR_ORPHAN_GRACE_MS) return false;
      // Staleness is not death authority for a different owner or bridged root.
      if (value.ownerHostId !== deadOwner || value.ownerIdentityId !== deadOwner || value.rootId !== deadOwner ||
          (value.remoteHost !== undefined && value.remoteHost !== os.hostname())) return false;
      if (value.reloadUntil !== undefined && !time(value.reloadUntil)) return false;
      if (typeof value.reloadUntil === "number" && value.reloadUntil >= now) return false;
      const owner = leaseOf(mesh.root, value.ownerHostId);
      if (!owner) {
        // The host reaper removes long-dead leases. Only the exact Main whose
        // process/lineage was positively proved dead can qualify that absence.
        try { fs.statSync(hostLeasePath(mesh.root, value.ownerHostId)); return false; }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false; }
        if (value.ownerHostId !== deadOwner || value.ownerIdentityId !== deadOwner || value.rootId !== deadOwner) return false;
      } else if (owner.identityId !== value.ownerIdentityId || owner.rootId !== value.rootId || until(owner) >= now) return false;
    }
    return true;
  } catch { return false; }
};

export interface SessionActorRootGone {
  oldHost: string;
  reason: string;
  leadName?: string;
  role?: string;
}

const rootMetadata = (mesh: MeshStore, rootId: string, lease?: FabricHostLease): Omit<SessionActorRootGone, "reason"> => {
  let participant: Record<string, unknown> | undefined;
  try {
    const file = record(JSON.parse(fs.readFileSync(path.join(mesh.root, "participants", `${digest(rootId)}.json`), "utf8")));
    const entry = file?.format === 1 && file.key === participantKey(rootId) ? file : mesh.get(participantKey(rootId), { fresh: true });
    const value = record(entry?.value);
    if (value?.id === rootId && value.kind === "root" && value.rootId === rootId &&
        value.ownerHostId === rootId && value.ownerIdentityId === rootId) participant = value;
  } catch {
    const value = record(mesh.get(participantKey(rootId), { fresh: true })?.value);
    if (value?.id === rootId && value.kind === "root" && value.rootId === rootId &&
        value.ownerHostId === rootId && value.ownerIdentityId === rootId) participant = value;
  }
  let owner: Record<string, unknown> | undefined;
  try {
    const value = record(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", `${encodeURIComponent(rootId.slice(8))}.owner.json`), "utf8")));
    if (value?.rootId === rootId && value.sessionId === rootId.slice(8) && value.ownerIdentityId === rootId &&
        Number.isSafeInteger(value.pid) && (value.pid as number) > 0) owner = value;
  } catch { /* Attribution may be unknown; it never supplies death authority. */ }
  const leadName = participant?.name ?? owner?.name;
  const host = lease?.writer?.host ?? owner?.host;
  return { oldHost: typeof host === "string" && host.length > 0 ? host : "unknown",
    ...(typeof leadName === "string" && leadName.length > 0 ? { leadName } : {}),
    ...(typeof participant?.role === "string" ? { role: participant.role } : {}) };
};

/** Uncached, conservative death verdict. Lease expiry alone never proves a process died. */
export const sessionActorRootGone = (
  mesh: MeshStore, rootId: string, lineageAlive?: (rootId: string) => boolean, now = Date.now(),
): SessionActorRootGone | undefined => {
  try {
    assertMeshStateReadable(mesh.root);
    if (!time(now) || !rootId.startsWith("session:")) return undefined;
    const sessionId = rootId.slice(8);
    const lease = leaseOf(mesh.root, rootId);
    if (!lease) {
      try { fs.statSync(hostLeasePath(mesh.root, rootId)); return undefined; }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return undefined; }
    }
    if (lease && (lease.rootId !== rootId || lease.identityId !== rootId ||
        now - Math.max(until(lease), lease.updatedAt) < SESSION_ACTOR_ORPHAN_GRACE_MS)) return undefined;
    // Native inbox records are not bridged; nevertheless historical records without
    // a host are NOT assumed local. A matching local lease writer qualifies them.
    // A fresh/replaced inbox also fences a root whose old lease still names a dead PID.
    let owner: Record<string, unknown> | undefined;
    const ownerPath = path.join(mesh.root, "main-followups", `${encodeURIComponent(sessionId)}.owner.json`);
    try {
      owner = record(JSON.parse(fs.readFileSync(ownerPath, "utf8")));
      if (!owner || owner.rootId !== rootId || owner.sessionId !== sessionId || owner.ownerIdentityId !== rootId ||
          !Number.isSafeInteger(owner.pid) || (owner.pid as number) <= 0 ||
          (owner.processStartedAt !== undefined && (typeof owner.processStartedAt !== "string" || !/^\d+$/.test(owner.processStartedAt))) ||
          now - fs.statSync(ownerPath).mtimeMs < SESSION_ACTOR_ORPHAN_GRACE_MS) return undefined;
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return undefined; }
    const writer = lease?.writer;
    const ownerHost = owner?.host ?? (writer && writer.pid === owner?.pid ? writer.host : undefined);
    // Inbox activation and lease renewal are independent writes. An old inbox
    // cannot qualify a present live/unknown writer, even with the same reused PID.
    if (writer && (writer.host !== os.hostname() ||
        (owner && (writer.pid !== owner.pid || writer.host !== ownerHost)) ||
        censusRecordAlive(writer.pid, writer))) return undefined;
    let reason: string | undefined;
    // The directory's clean-close proof is lease independent; preserve its conservative
    // raw-presence policy. Recheck the receipt's age, not just the actor's last update.
    if (lineageAlive?.(rootId) === false) {
      const closure = mesh.get(`topology/lineage-closures/${digest(rootId)}`, { fresh: true });
      const value = record(closure?.value);
      if (value?.format === 1 && value.rootId === rootId && value.ownerHostId === rootId &&
          value.ownerIdentityId === rootId && closure?.updatedBy.id === rootId && closure.updatedBy.kind === "main" &&
          time(value.closedAt) && now - value.closedAt >= SESSION_ACTOR_ORPHAN_GRACE_MS) {
        reason = "Main lineage closed";
      }
    }
    if (!reason && owner && (ownerHost !== os.hostname() || residentProcessAlive(owner.pid as number, owner.processStartedAt as string | undefined))) return undefined;
    if (!reason && writer?.host === os.hostname() && !censusRecordAlive(writer.pid, writer)) reason = "Main owner process is dead";
    // Missing (not invalid) leases are expected after host reaping. A host-qualified
    // inbox with valid kernel start evidence and an old mtime still proves death.
    if (!reason && owner && ownerHost === os.hostname() && typeof owner.processStartedAt === "string" &&
        !residentProcessAlive(owner.pid as number, owner.processStartedAt)) reason = "Main inbox owner incarnation is dead";
    if (!reason || !noLiveParticipant(mesh, rootId, now, rootId)) return undefined;
    const legacy = mesh.get(`sessions/${sessionId}`, { fresh: true });
    const legacyValue = record(legacy?.value);
    if (legacy && (!legacyValue || legacyValue.id !== rootId || legacyValue.sessionId !== sessionId ||
        typeof legacyValue.cwd !== "string" || !time(legacyValue.startedAt) ||
        (legacyValue.status !== "idle" && legacyValue.status !== "running") || isLiveLegacyRootEntry(legacy, now, mesh.root) ||
        legacy.updatedBy.id !== rootId || !time(legacy.updatedAt) ||
        now - legacy.updatedAt < SESSION_ACTOR_ORPHAN_GRACE_MS)) return undefined;
    return { ...rootMetadata(mesh, rootId, lease), reason };
  } catch { return undefined; }
};

/** Foreign session registries are observed directly, NEVER loaded into an ActorManager. */
export class SessionActorOrphans {
  #pending: Promise<number> | undefined;
  #closed = false;
  readonly #stores = new Map<string, ActorRegistryStore>();
  constructor(readonly mesh: MeshStore, readonly identity: MeshIdentity, readonly ownSessionId: string | undefined,
    readonly actorRoot: string, readonly lineageAlive?: (rootId: string) => boolean) {}

  #registries(): Array<{ sessionId: string; store: ActorRegistryStore }> {
    try {
      return fs.readdirSync(this.actorRoot, { withFileTypes: true }).flatMap(entry => {
        if (!entry.isDirectory() || entry.name === this.ownSessionId) return [];
        const root = path.join(this.actorRoot, entry.name);
        if (!fs.existsSync(path.join(root, "actors.json"))) return [];
        let store = this.#stores.get(root);
        if (!store) { store = new ActorRegistryStore(root); this.#stores.set(root, store); }
        return [{ sessionId: entry.name, store }];
      });
    } catch { return []; }
  }

  #rows(store: ActorRegistryStore): ReturnType<ActorRegistryStore["snapshot"]>["actors"] {
    const snapshot = store.snapshot();
    const parsed = record(JSON.parse(snapshot.bytes ?? "null"));
    // Never compact away malformed rows as a side effect of a truth repair.
    if (parsed?.format !== 1 || !Array.isArray(parsed.actors) || parsed.actors.length !== snapshot.actors.length) return [];
    return snapshot.actors;
  }

  list(): FabricActorInfo[] {
    return this.#registries().flatMap(({ sessionId, store }) => {
      try { return this.#rows(store).flatMap(row => {
        if (row.status !== "stopped" || !record(row.sessionOrphan)) return [];
        const view = this.#view(row, sessionId, store);
        return view ? [view] : [];
      }); } catch { return []; }
    });
  }

  resolve(id: string): FabricActorInfo | undefined {
    const rows = this.list();
    const exact = rows.find(row => row.id === id);
    if (exact) return exact;
    const matches = rows.filter(row => row.name === id || row.id.startsWith(id));
    if (matches.length > 1) throw new Error(`Ambiguous Fabric actor: ${id}`);
    return matches[0];
  }

  #view(row: Record<string, unknown>, sessionId: string, store: ActorRegistryStore): FabricActorInfo | undefined {
    if (typeof row.id !== "string" || !/^[a-f0-9]{32}$/.test(row.id) || typeof row.name !== "string" ||
        row.rootId !== `session:${sessionId}` || row.residency !== "session" || !time(row.createdAt) || !time(row.updatedAt)) return undefined;
    const instructions = store.instructions(row);
    if (typeof instructions !== "string") return undefined;
    return {
      id: row.id, name: row.name, scope: "session", rootId: row.rootId, ownerSessionId: sessionId,
      ...(typeof row.project === "string" ? { project: row.project } : {}),
      status: "stopped", sessionOrphan: row.sessionOrphan as unknown as FabricActorSessionOrphan,
      ...(typeof row.lastError === "string" ? { lastError: row.lastError } : {}),
      instructionsDigest: digest(instructions), instructionsLength: instructions.length,
      runner: row.runner === "claude" ? "claude" : "pi", residency: "session",
      events: strings(row.events).filter((event): event is FabricActorInfo["events"][number] => (FABRIC_ACTOR_HOST_EVENTS as readonly string[]).includes(event)),
      topics: strings(row.topics), delivery: row.delivery === "steer" || row.delivery === "followUp" || row.delivery === "nextTurn" ? row.delivery : "mailbox",
      responseMode: row.responseMode === "directive" ? "directive" : "text", triggerTurn: row.triggerTurn === true,
      coalesce: row.coalesce !== false, filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null },
      queued: 0, messages: store.messageCount(row), createdAt: row.createdAt, updatedAt: row.updatedAt,
      ...(typeof row.model === "string" ? { model: row.model } : {}),
      sessionFile: path.join(this.actorRoot, sessionId, row.id, "session.jsonl"),
    };
  }

  reconcile(): Promise<number> {
    if (this.#closed) return Promise.resolve(0);
    if (this.#pending) return this.#pending;
    const pending = this.#reconcile().finally(() => { if (this.#pending === pending) this.#pending = undefined; });
    return this.#pending = pending;
  }

  async #reconcile(): Promise<number> {
    let stopped = 0;
    for (const { sessionId, store } of this.#registries()) {
      // A persisted stop is also the alarm outbox. A crash/failed publication
      // leaves it pending; the mesh's durable dedupe receipt makes retry safe.
      try {
        for (const row of this.#rows(store)) {
          if (this.#closed) return stopped;
          if (row.rootId === `session:${sessionId}` && row.residency === "session" && row.status === "stopped" &&
              record(row.sessionOrphan)?.alarmPublishedAt === undefined && row.sessionOrphan) await this.#publishAlarm(store, row);
        }
      } catch { /* Keep unreadable/pending alarms for the next existing event/read. */ }
      let ids: string[];
      try { ids = this.#rows(store).filter(row => row.rootId === `session:${sessionId}` && row.residency === "session" &&
        row.status !== "stopped" && !row.removal && !row.sessionOrphan && typeof row.name === "string" &&
        /^[a-f0-9]{32}$/.test(row.id) && time(row.updatedAt) && Date.now() - row.updatedAt >= SESSION_ACTOR_ORPHAN_GRACE_MS).map(row => row.id); }
      catch { continue; }
      for (const id of ids) {
        if (this.#closed) return stopped;
        const rootId = `session:${sessionId}`;
        if (!sessionActorRootGone(this.mesh, rootId, this.lineageAlive) || !this.#actorAbsent(sessionId, id)) continue;
        try {
          const incarnation = await ownProcessIncarnation();
          const keys = [participantKey(rootId), participantKey(id)].sort();
          for (const key of keys) await prepareParticipantFileLock(this.mesh, key);
          const snapshot = store.snapshot();
          const current = this.#rows(store).find(row => row.id === id);
          if (!current || current.rootId !== rootId || current.residency !== "session" || current.status === "stopped" ||
              current.removal || current.sessionOrphan || !time(current.updatedAt) || Date.now() - current.updatedAt < SESSION_ACTOR_ORPHAN_GRACE_MS) continue;
          const gone = sessionActorRootGone(this.mesh, rootId, this.lineageAlive);
          if (!gone) continue;
          const text = SESSION_ACTOR_ORPHAN_TEXT;
          const marker: FabricActorSessionOrphan = { ...gone, oldRoot: rootId, lastUpdated: current.updatedAt, orphanedAt: Date.now() };
          const row = { ...current, status: "stopped", lastError: `root-gone: ${text}`, sessionOrphan: marker, updatedAt: marker.orphanedAt };
          const prepared = store.prepare(snapshot.actors.map(before => before.id === id ? row : before), { durable: true }, snapshot);
          let committed: boolean;
          try {
            // Global custody order matches adoption: registry -> state -> zero-wait keys.
            // A successor registry generation, resumed lineage or new live presence vetoes.
            committed = await ActorRegistryStore.withLocks([store], () => withStateFence(this.mesh, this.identity, () =>
              withParticipantFileTryLock(this.mesh, keys[0]!, incarnation, () =>
                withParticipantFileTryLock(this.mesh, keys[1]!, incarnation, () => {
                  if (this.#closed || !prepared.valid() || !sessionActorRootGone(this.mesh, rootId, this.lineageAlive) ||
                      !this.#actorAbsent(sessionId, id)) return false;
                  prepared.commit();
                  return true;
                })), 0));
          } finally { prepared.dispose(); }
          if (!committed) continue;
          stopped++;
          await this.#publishAlarm(store, row);
        } catch { /* Busy fence, corrupt evidence or I/O failure: retry only on the next event/read. */ }
      }
    }
    return stopped;
  }

  #actorAbsent(sessionId: string, id: string): boolean {
    const legacy = this.mesh.get(`actors/${sessionId}/${id}`, { fresh: true });
    const value = record(legacy?.value);
    if (legacy && (!value || value.id !== id || value.rootId !== `session:${sessionId}` || !time(legacy.updatedAt) ||
        typeof value.status !== "string" || !["idle", "stopped", "running", "queued", "preparing", "waiting", "failed", "failing-preparation"].includes(value.status))) return false;
    // An idle/stopped advertisement of the positively dead Main is not live
    // execution presence. Recent active advertisements retain the restart grace.
    if (legacy && value?.status !== "idle" && value?.status !== "stopped" &&
        Date.now() - legacy.updatedAt < SESSION_ACTOR_ORPHAN_GRACE_MS) return false;
    return noLiveParticipant(this.mesh, id, Date.now(), `session:${sessionId}`);
  }

  async #publishAlarm(store: ActorRegistryStore, row: Record<string, unknown> & { id: string }): Promise<void> {
    const marker = record(row.sessionOrphan);
    if (!marker || typeof marker.oldRoot !== "string" || typeof row.name !== "string" || !time(marker.orphanedAt)) return;
    const text = SESSION_ACTOR_ORPHAN_TEXT;
    // Whitelist attribution fields: a persisted marker cannot override the name or display line.
    const metadata = Object.fromEntries(["oldHost", "reason", "leadName", "role"].flatMap(key => {
      const value = marker[key];
      return typeof value === "string" ? [[key, alarmLabel(value)]] : [];
    }));
    await this.mesh.publish({ from: this.identity, topic: "ops.owner", kind: "actor.session.orphaned",
      dedupeKey: `session-actor-orphan:${row.id}:${marker.oldRoot}`, text,
      data: { ...metadata, actorId: alarmLabel(row.id), name: alarmLabel(row.name),
        oldRoot: alarmLabel(marker.oldRoot), rootId: alarmLabel(marker.oldRoot),
        ...(time(marker.lastUpdated) ? { lastUpdated: marker.lastUpdated } : {}), orphanedAt: marker.orphanedAt, line: text,
        ...(typeof row.project === "string" ? { project: alarmLabel(row.project) } : {}),
        ...(typeof row.role === "string" ? { role: alarmLabel(row.role) } : {}) } });
    // A receipt is durable before acknowledging it. Preserve every concurrent
    // registry mutation; never stamp a successor or a reopened row.
    await store.update(current => {
      const before = current.find(candidate => candidate.id === row.id);
      const orphan = record(before?.sessionOrphan);
      if (!before || before.rootId !== marker.oldRoot || before.status !== "stopped" || !orphan ||
          orphan.oldRoot !== marker.oldRoot || orphan.orphanedAt !== marker.orphanedAt || orphan.alarmPublishedAt !== undefined) return undefined;
      return { actors: current.map(candidate => candidate.id === row.id
        ? { ...candidate, sessionOrphan: { ...orphan, alarmPublishedAt: Date.now() } } : candidate), durable: true, value: true };
    });
  }

  async close(): Promise<void> { this.#closed = true; await this.#pending; }
}
