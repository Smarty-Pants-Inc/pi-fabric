import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { SessionReceiptManager } from "../core/session-receipts.js";
import { confirmedMainInboxIds } from "./root-inbox.js";
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
  ownerIdentityId: string; pid: number; processStartedAt?: string | undefined; retired?: true;
}
interface InboxRoute { oldRoot: string; newRoot?: string; messageId: string; item?: HeldAgentMessage; done?: true }
interface Successor { oldRoot: string; newRoot: string }

/** Root-owned metadata, not a name/cwd inference. Reload does not retire an inbox. */
export const registerMainInbox = (meshRoot: string, identity: MeshIdentity, sessionId: string, sessionFile?: string): void => {
  writeJsonAtomic(ownerFile(meshRoot, sessionId), { rootId: identity.id, sessionId, sessionFile,
    ownerIdentityId: identity.id, pid: process.pid, processStartedAt: processStartTime(process.pid) } satisfies InboxOwner, { durable: true });
};
/** Called only after the old Main/control drainer has closed on an actual native session change. */
export const recordMainSuccessor = async (mesh: MeshStore, oldRoot: string, oldSessionId: string, newRoot: string): Promise<void> => {
  if (oldRoot === newRoot) return;
  await mesh.exclusive(() => {
    const owner = read<InboxOwner>(ownerFile(mesh.root, oldSessionId));
    if (!owner || owner.rootId !== oldRoot || owner.pid !== process.pid) throw new Error("Cannot rotate an unowned Main inbox");
    const prior = read<Successor>(successorFile(mesh.root, oldRoot));
    if (prior && prior.newRoot !== newRoot) throw new Error("Main inbox already has a different successor");
    writeJsonAtomic(successorFile(mesh.root, oldRoot), { oldRoot, newRoot }, { durable: true });
    writeJsonAtomic(ownerFile(mesh.root, oldSessionId), { ...owner, retired: true }, { durable: true });
  });
};

/** Pi's native replacement shuts down the old extension runtime before starting the
 * new one. Bind its explicit targetSessionFile, never infer succession from a label. */
export const stageMainSuccessor = async (mesh: MeshStore, oldRoot: string, sessionId: string, targetSessionFile: string): Promise<void> => {
  await mesh.exclusive(() => {
    const file = ownerFile(mesh.root, sessionId);
    const owner = read<InboxOwner>(file);
    if (!owner || owner.rootId !== oldRoot || owner.pid !== process.pid) throw new Error("Cannot retire an unowned Main inbox");
    // Resuming the current native session keeps its root; it is not a rotation.
    if (owner.sessionFile && path.resolve(owner.sessionFile) === path.resolve(targetSessionFile)) return;
    writeJsonAtomic(path.join(mailbox(mesh.root), "rotations", `${hash(path.resolve(targetSessionFile))}.json`),
      { oldRoot, sessionId, targetSessionFile: path.resolve(targetSessionFile) }, { durable: true });
    writeJsonAtomic(file, { ...owner, retired: true }, { durable: true });
  });
};
export const confirmMainSuccessor = async (mesh: MeshStore, newRoot: string, sessionFile?: string): Promise<boolean> => {
  if (!sessionFile) return false;
  return mesh.exclusive(() => {
    const intent = read<{ oldRoot: string; sessionId: string; targetSessionFile: string }>(path.join(mailbox(mesh.root), "rotations", `${hash(path.resolve(sessionFile))}.json`));
    if (!intent) return false;
    const owner = read<InboxOwner>(ownerFile(mesh.root, intent.sessionId));
    if (intent.targetSessionFile !== path.resolve(sessionFile) || !owner?.retired || owner.rootId !== intent.oldRoot || newRoot === intent.oldRoot) throw new Error("Invalid Main rotation intent");
    const file = successorFile(mesh.root, intent.oldRoot);
    const prior = read<Successor>(file);
    if (prior && prior.newRoot !== newRoot) throw new Error("Main inbox already has a different successor");
    writeJsonAtomic(file, { oldRoot: intent.oldRoot, newRoot }, { durable: true });
    return true;
  });
};

export const mainInboxActive = (meshRoot: string, rootId: string): boolean =>
  read<Successor>(successorFile(meshRoot, rootId)) === undefined;

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

export class MainInboxMaintenance {
  readonly #receipts = new Map<string, SessionReceiptManager>();
  constructor(readonly mesh: MeshStore, readonly identity: MeshIdentity, readonly participants: FabricParticipantSource,
    readonly main: MainAgentController, readonly options: StallAlarmOptions) {}

  async run(now = Date.now()): Promise<void> {
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
      const receiptKey = JSON.stringify([owner.sessionId, owner.sessionFile]);
      let manager = this.#receipts.get(receiptKey);
      if (!manager) {
        manager = { getEntries: () => [], getSessionFile: () => owner.sessionFile, isPersisted: () => true };
        this.#receipts.set(receiptKey, manager);
      }
      const ids = confirmedMainInboxIds(manager, owner.sessionId);
      for (const item of journal.items) {
        // nextTurn is passive session context, not a Main steer/followUp to inherit.
        if (item.deliverAs === "nextTurn") continue;
        if (ids.has(item.id) || (item.chain && ids.has(item.chain))) continue;
        const previousRoute = read<InboxRoute>(routeFile(this.mesh.root, item.id));
        if (previousRoute && previousRoute.newRoot !== owner.rootId) continue;
        if (now - item.sentAt > this.options.undeliveredAlarmMs) {
          for (const to of new Set([item.from.id, owner.ownerIdentityId])) await this.mesh.publish({
            topic: "ops.owner", kind: "inbox.age.alarm", from: this.identity, to,
            dedupeKey: `inbox-age:${item.id}:${to}`, text: `Undelivered Main message ${item.id} to ${owner.rootId}`,
            data: { messageId: item.id, rootId: owner.rootId, sentAt: item.sentAt } });
        }
        // A lapsed lease is not permission to move a still-running writer's volatile queue.
        if (live.has(owner.rootId) || (!owner.retired && residentProcessAlive(owner.pid, owner.processStartedAt))) continue;
        const successor = read<Successor>(successorFile(this.mesh.root, owner.rootId));
        const newRoot = successor?.oldRoot === owner.rootId ? successor.newRoot : undefined;
        if (!newRoot && (since === undefined || now - since <= this.options.rootGoneTtlMs)) continue;
        await this.mesh.exclusive(() => {
          const currentOwner = read<InboxOwner>(ownerFile(this.mesh.root, owner.sessionId));
          if (!currentOwner || JSON.stringify(currentOwner) !== JSON.stringify(owner) ||
            (!currentOwner.retired && residentProcessAlive(currentOwner.pid, currentOwner.processStartedAt))) return;
          const file = routeFile(this.mesh.root, item.id);
          const prior = read<InboxRoute>(file);
          if (prior && prior.newRoot !== owner.rootId) return;
          writeJsonAtomic(file, { oldRoot: owner.rootId, ...(newRoot ? { newRoot } : {}),
            messageId: item.id, item } satisfies InboxRoute, { durable: true });
          // Move, not copy: the durable claim owns the payload before the old journal
          // loses it. A crash here is recovered from that claim, never from both inboxes.
          const sourceFile = path.join(mailbox(this.mesh.root), `${encodeURIComponent(owner.sessionId)}.json`);
          const source = read<{ version: 1; items: HeldAgentMessage[] }>(sourceFile);
          if (source) writeJsonAtomic(sourceFile, { ...source, items: source.items.filter(carrier => carrier.id !== item.id) }, { durable: true });
        });
      }
    }
    for (const name of names(path.join(mailbox(this.mesh.root), "routes")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))) {
      const file = path.join(mailbox(this.mesh.root), "routes", name);
      const route = read<InboxRoute>(file)!;
      if (route.done || (route.newRoot && route.newRoot !== this.identity.id)) continue;
      const item = route.item;
      if (!item || route.messageId !== item.id) throw new Error("Invalid pending Main inbox claim");
      if (route.newRoot) this.main.receiveInboxItem(item);
      const text = route.newRoot ? `rerouted: ${route.oldRoot} -> ${route.newRoot}` : "undeliverable: root gone";
      await this.mesh.publish({ topic: "fleet.work.inbox-receipts", kind: route.newRoot ? "rerouted" : "undeliverable",
        from: this.identity, to: item.from.id, text, dedupeKey: `inbox-disposition:${item.id}:${route.oldRoot}:${route.newRoot ?? "gone"}`,
        data: { messageId: item.id, oldRoot: route.oldRoot, newRoot: route.newRoot } });
      await this.mesh.exclusive(() => {
        const current = read<InboxRoute>(file);
        if (!current || current.newRoot !== route.newRoot || current.oldRoot !== route.oldRoot) return;
        const { item: _payload, ...claim } = route;
        writeJsonAtomic(file, { ...claim, done: true }, { durable: true });
      });
    }
  }
}
