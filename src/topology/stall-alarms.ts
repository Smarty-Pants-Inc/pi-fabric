import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { SessionReceiptManager } from "../core/session-receipts.js";
import { confirmedMainInboxIds } from "./root-inbox-shared.js";
import { processStartTime, residentProcessAlive } from "../residency/process-identity.js";
import type { HeldAgentMessage, MainAgentController } from "../main-agent.js";
import type { MeshIdentity, MeshStore } from "../mesh/store.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "./types.js";

export interface StallAlarmOptions {
  rootPresenceAlarmMs: number;
  undeliveredAlarmMs: number;
  rootGoneTtlMs: number;
}
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const absentKey = (rootId: string): string => `topology/root-absence/${hash(rootId)}`;
const mailbox = (meshRoot: string): string => path.join(meshRoot, "main-followups");
const ownerFile = (meshRoot: string, sessionId: string): string => path.join(mailbox(meshRoot), `${encodeURIComponent(sessionId)}.owner.json`);
const routeFile = (meshRoot: string, id: string): string => path.join(mailbox(meshRoot), "routes", `${hash(id)}.json`);
const successorFile = (meshRoot: string, rootId: string): string => path.join(mailbox(meshRoot), "successors", `${hash(rootId)}.json`);
const read = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
};
const names = (directory: string): string[] => {
  try { return fs.readdirSync(directory); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
interface InboxOwner {
  rootId: string; sessionId: string; sessionFile?: string | undefined;
  ownerIdentityId: string; pid: number; host?: string; name?: string; processStartedAt?: string | undefined; retired?: true; activationId?: string;
}
interface InboxRoute { oldRoot: string; newRoot?: string; messageId: string; item?: HeldAgentMessage; done?: true }
interface Successor { oldRoot: string; newRoot?: string; activationId?: string }

/** Explicit native activation, not a name/cwd inference. Each runtime captures its
 * own token: resuming a root reopens fresh delivery, never the predecessor runtime.
 * Per-message move tombstones live separately and are never reset here. */
export const registerMainInbox = (meshRoot: string, identity: MeshIdentity, sessionId: string, sessionFile?: string): string => {
  const activationId = randomUUID();
  writeJsonAtomic(ownerFile(meshRoot, sessionId), { rootId: identity.id, sessionId, sessionFile, activationId,
    ownerIdentityId: identity.id, pid: process.pid, host: os.hostname(), name: identity.name, processStartedAt: processStartTime(process.pid) } satisfies InboxOwner, { durable: true });
  writeJsonAtomic(successorFile(meshRoot, identity.id), { oldRoot: identity.id, activationId } satisfies Successor, { durable: true });
  return activationId;
};
/** Called only after the old Main/control drainer has closed on an actual native session change. */
export const recordMainSuccessor = async (mesh: MeshStore, oldRoot: string, oldSessionId: string, newRoot: string): Promise<void> => {
  if (oldRoot === newRoot) return;
  await mesh.custody(() => {
    const owner = read<InboxOwner>(ownerFile(mesh.root, oldSessionId));
    if (!owner || owner.rootId !== oldRoot || owner.pid !== process.pid) throw new Error("Cannot rotate an unowned Main inbox");
    const prior = read<Successor>(successorFile(mesh.root, oldRoot));
    if (prior?.newRoot && prior.newRoot !== newRoot) throw new Error("Main inbox already has a different successor");
    writeJsonAtomic(successorFile(mesh.root, oldRoot), { oldRoot, newRoot, activationId: owner.activationId }, { durable: true });
    writeJsonAtomic(ownerFile(mesh.root, oldSessionId), { ...owner, retired: true }, { durable: true });
  });
};

/** Pi's native replacement shuts down the old extension runtime before starting the
 * new one. Bind its explicit targetSessionFile, never infer succession from a label. */
export const stageMainSuccessor = async (mesh: MeshStore, oldRoot: string, sessionId: string, targetSessionFile: string): Promise<void> => {
  await mesh.custody(() => {
    const file = ownerFile(mesh.root, sessionId);
    const owner = read<InboxOwner>(file);
    if (!owner || owner.rootId !== oldRoot || owner.pid !== process.pid) throw new Error("Cannot retire an unowned Main inbox");
    // Resuming the current native session keeps its root; it is not a rotation.
    if (owner.sessionFile && path.resolve(owner.sessionFile) === path.resolve(targetSessionFile)) return;
    writeJsonAtomic(path.join(mailbox(mesh.root), "rotations", `${hash(path.resolve(targetSessionFile))}.json`),
      { oldRoot, sessionId, activationId: owner.activationId, targetSessionFile: path.resolve(targetSessionFile) }, { durable: true });
    writeJsonAtomic(file, { ...owner, retired: true }, { durable: true });
  });
};
export const confirmMainSuccessor = async (mesh: MeshStore, newRoot: string, sessionFile?: string): Promise<boolean> => {
  if (!sessionFile) return false;
  return mesh.custody(() => {
    const intentFile = path.join(mailbox(mesh.root), "rotations", `${hash(path.resolve(sessionFile))}.json`);
    const intent = read<{ oldRoot: string; sessionId: string; activationId?: string; targetSessionFile: string }>(intentFile);
    if (!intent) return false;
    const owner = read<InboxOwner>(ownerFile(mesh.root, intent.sessionId));
    // An interrupted/canceled switch cannot authorize a different activation.
    if (!owner?.retired || owner.activationId !== intent.activationId) return false;
    if (intent.targetSessionFile !== path.resolve(sessionFile) || owner.rootId !== intent.oldRoot || newRoot === intent.oldRoot) throw new Error("Invalid Main rotation intent");
    const file = successorFile(mesh.root, intent.oldRoot);
    const prior = read<Successor>(file);
    if (prior?.newRoot && prior.newRoot !== newRoot) throw new Error("Main inbox already has a different successor");
    writeJsonAtomic(file, { oldRoot: intent.oldRoot, newRoot, activationId: owner.activationId }, { durable: true });
    // An intent authorizes this switch only, not a later return to the same file.
    fs.unlinkSync(intentFile);
    return true;
  });
};

export const mainInboxActive = (meshRoot: string, rootId: string, activationId?: string): boolean => {
  const current = read<Successor>(successorFile(meshRoot, rootId));
  return !current?.newRoot && (activationId === undefined || current?.activationId === activationId);
};

/** Permanent per-message claim: a resumed predecessor must never replay a moved message. */
export const mainInboxOwns = (meshRoot: string, rootId: string, id: string): boolean => {
  const route = read<InboxRoute>(routeFile(meshRoot, id));
  return !route || route.newRoot === rootId;
};

const absence = async (mesh: MeshStore, identity: MeshIdentity, rootId: string, live: boolean, now: number): Promise<number | undefined> => {
  const key = absentKey(rootId);
  const current = mesh.get(key, { fresh: true });
  if (live) {
    if (current) await mesh.writeBatch({ identity, ops: [{ kind: "delete", key, ifVersion: current.version, onConflict: "skip" }] });
    return undefined;
  }
  if (current) return (current.value as { firstAbsentAt: number }).firstAbsentAt;
  await mesh.writeBatch({ identity, ops: [], prepare: view => view.get(key) ? [] :
    [{ kind: "put", key, ifVersion: view.version(key), value: { rootId, firstAbsentAt: now } }] });
  return (mesh.get(key, { fresh: true })!.value as { firstAbsentAt: number }).firstAbsentAt;
};

/** Owning host's committed presence pass, never a new timer. Absence is an alert,
 * NOT adoption authority: lineageAlive's positive-closure policy is unchanged. */
export const rootPresenceAlarms = async (mesh: MeshStore, identity: MeshIdentity, hostId: string,
  participants: readonly FabricParticipantInfo[], thresholdMs = 15 * 60_000, now = Date.now()): Promise<void> => {
  const live = new Set(participants.filter(p => p.kind === "root" && !p.stale).map(p => p.id));
  // Re-arm even if the old members have since disappeared or changed owners.
  for (const entry of mesh.listAll("topology/root-absence/", { fresh: true })) {
    const rootId = (entry.value as { rootId: string }).rootId;
    if (live.has(rootId)) await absence(mesh, identity, rootId, true, now);
  }
  // The local reaper also owns stale native rows left by a dead publishing runtime.
  const roots = new Set(participants.filter(p => p.kind !== "root" && p.remoteHost === undefined &&
    (p.ownerHostId === hostId || p.stale)).map(p => p.rootId));
  for (const rootId of roots) {
    const since = await absence(mesh, identity, rootId, live.has(rootId), now);
    if (since === undefined || now - since <= thresholdMs) continue;
    const members = participants.filter(p => p.kind !== "root" && p.rootId === rootId);
    const byKind: Record<string, number> = Object.create(null), byStatus: Record<string, number> = Object.create(null);
    for (const member of members) { byKind[member.kind] = (byKind[member.kind] ?? 0) + 1; byStatus[member.status] = (byStatus[member.status] ?? 0) + 1; }
    await mesh.publish({ topic: "ops.owner", kind: "root.presence.alarm", from: identity, to: rootId,
      dedupeKey: `root-presence:${rootId}:${since}`, text: `Root ${rootId} absent since ${new Date(since).toISOString()}; ${members.length} members remain`,
      data: { rootId, firstAbsentAt: since, byKind, byStatus } });
  }
};

/** Held messages one alarm lists by id; its heldCount still counts them all. */
const PROVIDER_DEAD_LISTED_MESSAGES = 50;
/** Alarms per mesh publishBatch call; the loop publishes batches until every address is covered. */
const PROVIDER_DEAD_ALARM_BATCH = 50;

/**
 * smarty-dev#8742: a Main whose provider rejects every request as non-retryable reaches ops at
 * once, on the inbox-age alarm's path: ops.owner, addressed to the Main's owner and to the sender
 * of every held message (Main holds up to 200), one inbox.age.alarm per address however many of
 * its messages are held. PR #841 review: no held message's sender is left for the age pass.
 * An address's alarm reuses the age pass's dedupe key for its first held message, so it crosses
 * the existing mesh bridge to remote senders and the age pass never repeats it; the age pass
 * still reports that address's other messages if they stay unread past mesh.undeliveredAlarmMs.
 */
/**
 * pi-fabric#939 P2-b: the one recipient of the agent-admission alarm. An owner alarm is never published without one,
 * and never to a name ("org", "org-kate"): only an exact `session:<uuid>` address. The configured owner identity
 * first, else this Main's own session id (the manager's owner); undefined means log locally and skip the publish.
 */
const SESSION_ADDRESS = /^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const admissionAlarmRecipient = (ownerIdentityId: string | undefined, identity: MeshIdentity | undefined): string | undefined =>
  [ownerIdentityId?.trim(), identity?.kind === "main" ? identity.id : undefined]
    .find((candidate): candidate is string => typeof candidate === "string" && SESSION_ADDRESS.test(candidate));

export const providerDeadAlarms = async (mesh: MeshStore, identity: MeshIdentity, rootId: string,
  event: ProviderDeadAlarmEvent, ownerIdentityId?: string): Promise<void> => {
  const owners = [...new Set([identity.id, ...(ownerIdentityId ? [ownerIdentityId] : [])])];
  const fields = providerDeadAlarmFields(event);
  const reason = fields.kind === "unclassified" ? "provider-dead (unclassified)" : `provider-dead (non-retryable: ${fields.error})`;
  for (const to of owners) await mesh.publish({
    topic: "ops.owner", kind: "fabric.main.provider-dead", from: identity, to,
    dedupeKey: `provider-dead:${rootId}:${event.since}:${to}`,
    text: `Main ${rootId} ${reason}: ${fields.cause}; ${event.held.length} held message(s) unread`,
    data: { rootId, ...fields, held: event.held.slice(0, PROVIDER_DEAD_LISTED_MESSAGES), heldCount: event.held.length } });
  // Deduplicated per address: each sender once, listing its own held messages; owners list all.
  const byAddress = new Map<string, Array<ProviderDeadAlarmEvent["held"][number]>>();
  for (const item of event.held) {
    for (const to of new Set([item.from, ...owners])) {
      const items = byAddress.get(to);
      if (items) items.push(item); else byAddress.set(to, [item]);
    }
  }
  const alarms = [...byAddress].map(([to, items]) => {
    const first = items[0]!;
    return { topic: "ops.owner", kind: "inbox.age.alarm", from: identity, to, dedupeKey: `inbox-age:${first.id}:${to}`,
      text: items.length === 1 ? `Undelivered Main message ${first.id} to ${rootId}: ${reason}`
        : `${items.length} undelivered Main messages to ${rootId}, first ${first.id}: ${reason}`,
      data: { messageId: first.id, rootId, sentAt: first.sentAt, reason: "provider-dead", error: fields.error,
        messageIds: items.slice(0, PROVIDER_DEAD_LISTED_MESSAGES).map(item => item.id), heldCount: items.length } };
  });
  // A batch may commit only a prefix: continue from the first uncommitted alarm.
  for (let next = 0; next < alarms.length;) {
    const committed = await mesh.publishBatch(alarms.slice(next, next + PROVIDER_DEAD_ALARM_BATCH));
    if (!committed.length) throw new Error("Provider-dead alarm batch committed nothing");
    next += committed.length;
  }
};

/** What a provider-dead alarm may carry: the shape of FabricMainProviderDeadEvent, nothing else. */
export interface ProviderDeadAlarmEvent {
  kind?: "non-retryable" | "unclassified";
  error: string; status?: number; type?: string; code?: string; failures?: number;
  since: string; cause: string;
  held: ReadonlyArray<{ id: string; from: string; sentAt: number }>;
}

/**
 * smarty-dev#8742 security review: alarm data is an explicit allowlist (status, known type, fixed
 * code, Fabric's own cause), so a field an event happens to carry can never ride along to ops.owner,
 * held messages' senders or the mesh bridge. Provider message text is never one of them.
 */
export const providerDeadAlarmFields = (event: Omit<ProviderDeadAlarmEvent, "held">) => ({
  kind: event.kind ?? "non-retryable", error: event.error, since: event.since, cause: event.cause,
  ...(event.status === undefined ? {} : { status: event.status }),
  ...(event.type === undefined ? {} : { type: event.type }),
  ...(event.code === undefined ? {} : { code: event.code }),
  ...(event.failures === undefined ? {} : { failures: event.failures }),
});

export class MainInboxMaintenance {
  readonly #receipts = new Map<string, SessionReceiptManager>();
  constructor(readonly mesh: MeshStore, readonly identity: MeshIdentity, readonly participants: FabricParticipantSource,
    readonly main: MainAgentController, readonly options: StallAlarmOptions) {}

  async run(now = Date.now()): Promise<void> {
    const ownerFor = (rootId: string): InboxOwner | undefined => {
      for (const name of names(mailbox(this.mesh.root)).filter(name => name.endsWith(".owner.json"))) {
        const owner = read<InboxOwner>(path.join(mailbox(this.mesh.root), name));
        if (owner?.rootId === rootId) return owner;
      }
      return undefined;
    };
    const confirmed = (owner: InboxOwner): Set<string> => {
      const key = JSON.stringify([owner.sessionId, owner.sessionFile]);
      let manager = this.#receipts.get(key);
      if (!manager) {
        manager = { getEntries: () => [], getSessionFile: () => owner.sessionFile, isPersisted: () => true };
        this.#receipts.set(key, manager);
      }
      return confirmedMainInboxIds(manager, owner.sessionId);
    };
    const dropJournalItem = (owner: InboxOwner, id: string): void => {
      const file = path.join(mailbox(this.mesh.root), `${encodeURIComponent(owner.sessionId)}.json`);
      const journal = read<{ version: 1; items: HeldAgentMessage[] }>(file);
      if (journal?.items.some(item => item.id === id)) writeJsonAtomic(file,
        { ...journal, items: journal.items.filter(item => item.id !== id) }, { durable: true });
    };
    const alarm = async (item: HeldAgentMessage, rootId: string, ownerIdentityId: string): Promise<void> => {
      if (now - item.sentAt <= this.options.undeliveredAlarmMs) return;
      for (const to of new Set([item.from.id, ownerIdentityId])) await this.mesh.publish({
        topic: "ops.owner", kind: "inbox.age.alarm", from: this.identity, to,
        dedupeKey: `inbox-age:${item.id}:${to}`, text: `Undelivered Main message ${item.id} to ${rootId}`,
        data: { messageId: item.id, rootId, sentAt: item.sentAt } });
    };
    this.main.confirmInbox();
    const roots = this.participants.list({ kinds: ["root"], scope: "project", fresh: true });
    const live = new Set(roots.filter(p => !p.stale).map(p => p.id));
    for (const name of names(mailbox(this.mesh.root)).filter(name => name.endsWith(".owner.json"))) {
      const owner = read<InboxOwner>(path.join(mailbox(this.mesh.root), name));
      if (!owner || typeof owner.rootId !== "string" || typeof owner.sessionId !== "string" || !Number.isSafeInteger(owner.pid)) throw new Error("Invalid Main inbox owner");
      const journal = read<{ items: HeldAgentMessage[] }>(path.join(mailbox(this.mesh.root), `${encodeURIComponent(owner.sessionId)}.json`));
      if (!journal?.items?.length) continue;
      // Only the owning Main monitors a live inbox; any surviving Main may recover a dead one.
      if (live.has(owner.rootId) && owner.rootId !== this.identity.id) continue;
      const since = await absence(this.mesh, this.identity, owner.rootId, live.has(owner.rootId), now);
      const ids = confirmed(owner);
      for (const item of journal.items) {
        // nextTurn is passive session context, not a Main steer/followUp to inherit.
        if (item.deliverAs === "nextTurn") continue;
        if (ids.has(item.id) || (item.chain && ids.has(item.chain))) continue;
        const previousRoute = read<InboxRoute>(routeFile(this.mesh.root, item.id));
        if (previousRoute && previousRoute.newRoot !== owner.rootId) continue;
        await alarm(item, owner.rootId, owner.ownerIdentityId);
        // A lapsed lease is not permission to move a still-running writer's volatile queue.
        if (live.has(owner.rootId) || (!owner.retired && residentProcessAlive(owner.pid, owner.processStartedAt))) continue;
        await this.mesh.custody(() => {
          const currentOwner = read<InboxOwner>(ownerFile(this.mesh.root, owner.sessionId));
          if (!currentOwner || JSON.stringify(currentOwner) !== JSON.stringify(owner) ||
            (!currentOwner.retired && residentProcessAlive(currentOwner.pid, currentOwner.processStartedAt))) return;
          // Native confirmation can publish succession without changing a retired
          // owner. Decide reroute OR TTL disposal under custody, holding the lock
          // through the durable claim and source removal: no pre-lock absence of
          // a successor can authorize destructive no-destination custody.
          const successor = read<Successor>(successorFile(this.mesh.root, owner.rootId));
          const newRoot = successor?.oldRoot === owner.rootId ? successor.newRoot : undefined;
          if (!newRoot && (since === undefined || now - since <= this.options.rootGoneTtlMs)) return;
          const file = routeFile(this.mesh.root, item.id);
          const prior = read<InboxRoute>(file);
          if (prior && prior.newRoot !== owner.rootId) return;
          writeJsonAtomic(file, { ...(newRoot ? { newRoot } : {}),
            messageId: item.id, item, oldRoot: prior && !prior.done ? prior.oldRoot : owner.rootId } satisfies InboxRoute, { durable: true });
          // Move, not copy: the durable claim owns the payload before the old journal
          // loses it. A crash here is recovered from that claim, never from both inboxes.
          dropJournalItem(owner, item.id);
        });
      }
    }
    for (const name of names(path.join(mailbox(this.mesh.root), "routes")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))) {
      const file = path.join(mailbox(this.mesh.root), "routes", name);
      const pending = read<InboxRoute>(file)!;
      if (pending.done) continue;
      const item = pending.item;
      if (!item || pending.messageId !== item.id) throw new Error("Invalid pending Main inbox claim");
      const target = pending.newRoot;
      const since = target ? await absence(this.mesh, this.identity, target, live.has(target), now) : undefined;
      const observedOwner = target ? ownerFor(target) : undefined;
      const observedIds = observedOwner ? confirmed(observedOwner) : undefined;
      if (target && !observedIds?.has(item.id) && !(item.chain && observedIds?.has(item.chain))) {
        await alarm(item, target, observedOwner?.ownerIdentityId ?? target);
      }
      // Claims are durable inbox custody, even before the target has a journal.
      // Serialize recovery AND journal admission against root activation and other
      // drainers under the file custody lock (smarty-dev#6477 L5); publishing happens
      // afterwards, outside custody.
      const route = await this.mesh.custody((): InboxRoute | undefined => {
        const current = read<InboxRoute>(file);
        if (!current || JSON.stringify(current) !== JSON.stringify(pending)) return;
        if (!target) return current;
        const owner = ownerFor(target);
        const ids = owner ? confirmed(owner) : undefined;
        if (ids?.has(item.id) || (item.chain && ids?.has(item.chain))) return current;
        if (target === this.identity.id && mainInboxActive(this.mesh.root, target) && !owner?.retired) {
          this.main.receiveInboxItem(item);
          return current;
        }
        // A lapsed lease is still not a death proof, including for claim payloads.
        if (live.has(target) || (owner && !owner.retired && residentProcessAlive(owner.pid, owner.processStartedAt))) return;
        const successor = read<Successor>(successorFile(this.mesh.root, target));
        const next = successor?.oldRoot === target ? successor.newRoot : undefined;
        if (!next && (since === undefined || now - since <= this.options.rootGoneTtlMs)) return;
        const { newRoot: _target, ...custody } = current;
        const recovered: InboxRoute = { ...custody, ...(next ? { newRoot: next } : {}) };
        writeJsonAtomic(file, recovered, { durable: true });
        if (owner) dropJournalItem(owner, item.id);
        if (!next) return recovered;
        if (next === this.identity.id && mainInboxActive(this.mesh.root, next)) {
          this.main.receiveInboxItem(item);
          return recovered;
        }
        return undefined; // The named live successor (or a later recovery pass) drains.
      });
      if (!route) continue;
      const text = route.newRoot ? `rerouted: ${route.oldRoot} -> ${route.newRoot}` : "undeliverable: root gone";
      await this.mesh.publish({ topic: "fleet.work.inbox-receipts", kind: route.newRoot ? "rerouted" : "undeliverable",
        from: this.identity, to: item.from.id, text, dedupeKey: `inbox-disposition:${item.id}:${route.oldRoot}:${route.newRoot ?? "gone"}`,
        data: { messageId: item.id, oldRoot: route.oldRoot, newRoot: route.newRoot } });
      await this.mesh.custody(() => {
        const current = read<InboxRoute>(file);
        if (!current || JSON.stringify(current) !== JSON.stringify(route)) return;
        const { item: _payload, ...claim } = route;
        writeJsonAtomic(file, { ...claim, done: true }, { durable: true });
      });
    }
  }
}
