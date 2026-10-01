import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { Readable, Writable } from "node:stream";
import { readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import { hostLeaseExpiry, readHostLeases, removeHostLease, STATE_LEASE_RENEW_MS, writeHostLease } from "../topology/host-leases.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../topology/types.js";
import { ROOT_ID_PREFIX } from "../topology/root-inbox.js";
import { participantFilePresent, readParticipantFiles } from "../topology/participant-files.js";
import type { MeshEvent, MeshIdentity, MeshStateEntry, MeshStore } from "./store.js";

/**
 * Fabric mesh bridge v1 (smarty-dev#2004). Each host keeps its own mesh; one bridge process on the
 * hub (Dev1) links it to one remote host's mesh over a stdio transport (ssh to a forced command
 * that runs `mesh-bridge agent`). No new store or protocol: the bridge republishes allow-listed
 * mesh events and mirrors each side's live root presence into the other side.
 *
 * Trust: the hub side is trusted; the remote side is not. An event from the remote crosses only
 * when its sender is a live participant native to the remote (and no hub record uses its id), and
 * its recipient is native to the hub. Mirrored records never replace a native record.
 */

export const BRIDGE_PROTOCOL_VERSION = 1;
const HOST_PREFIX = "topology/hosts/";
const PARTICIPANT_PREFIX = "topology/participants/";
/** A mirrored host lease lasts this long past its last renewal, so a dead bridge lapses it. */
export const BRIDGE_LEASE_MS = 15_000;
const DEFAULT_POLL_MS = 250;
const DEFAULT_PRESENCE_MS = 5_000;
/** A bridge call the remote has not answered by then closes the transport. */
export const DEFAULT_CALL_TIMEOUT_MS = 30_000;
/** stop() waits at most this long for the loop and for each remote step of the withdrawal. */
const DEFAULT_STOP_MS = 5_000;
const LOCK_RETRY_MIN_MS = 100;
const LOCK_RETRY_MAX_MS = 2_000;
const MESH_LOCK_TIMEOUT_CODE = "FABRIC_MESH_LOCK_TIMEOUT";

// Only the store's exact typed timeout is transient; messages never grant a retry.
const isMeshLockTimeout = (error: unknown): boolean =>
  error instanceof Error && "code" in error && error.code === MESH_LOCK_TIMEOUT_CODE;
const MAX_RPC_LINE_BYTES = 16 * 1024 * 1024;
/** A read page's event bytes stay under this, well inside one frame with its JSON envelope. */
export const BRIDGE_PAGE_BYTES = 8 * 1024 * 1024;
const NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;

const keyFor = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isIdentity = (value: unknown): value is MeshIdentity =>
  isObject(value) && typeof value.id === "string" && value.id.length > 0 && typeof value.name === "string" &&
  (value.kind === "main" || value.kind === "actor" || value.kind === "agent");

export const validBridgeName = (name: string): boolean => NAME_PATTERN.test(name);

/** v1 allow-list: control commands and acks, work events, and owner wakes. */
export const isBridgedTopic = (event: Pick<MeshEvent, "topic" | "kind">): boolean =>
  event.topic === "fabric.control.command" ||
  event.topic === "fabric.control.ack" ||
  event.topic.startsWith("fleet.work.") ||
  (event.topic === "ops.owner" && event.kind === "pr.wake");

/** The stamp a bridged event carries; an event with one is never forwarded again (no loops). */
export const bridgeStampOf = (event: Pick<MeshEvent, "data">): { from: string; id: string } | undefined => {
  const stamp = isObject(event.data) ? event.data.bridge : undefined;
  return isObject(stamp) && typeof stamp.from === "string" && typeof stamp.id === "string"
    ? { from: stamp.from, id: stamp.id }
    : undefined;
};

const hasBridgeField = (event: Pick<MeshEvent, "data">): boolean =>
  isObject(event.data) && Object.hasOwn(event.data, "bridge");

export interface BridgeHost {
  record: FabricHostRecord;
  /** The effective lease expiry on the source (the later of state and file lease). */
  expiresAt: number;
}

export interface BridgePresence {
  hosts: BridgeHost[];
  /** Root participants of those hosts. */
  participants: FabricParticipantRecord[];
  /** Every id and identity id of native records, live or not: a peer may never claim them. */
  reserved: string[];
}

export interface BridgeSkip { id: string; sequence: number; topic: string; bytes: number }

export interface BridgeRead {
  /** Allow-listed events after the cursor, in sequence order. */
  events: MeshEvent[];
  /** Allow-listed events too large for one frame, passed over (the bridge logs each). */
  skipped?: BridgeSkip[];
  /** The last sequence read (the cursor to resume from). */
  through: number;
}

export interface BridgePublish {
  topic: string;
  kind: string;
  from: MeshIdentity;
  to: string;
  text?: string;
  data: Record<string, unknown> & { bridge: { from: string; id: string } };
}

/** One side of the bridge: the local mesh directly, or the remote one through the agent. */
export interface BridgeSide {
  latestSequence(): Promise<number>;
  read(after: number): Promise<BridgeRead>;
  presence(): Promise<BridgePresence>;
  /** Publish a bridged event; with `held`, only if this link holds each of those ids at commit. */
  publish(event: BridgePublish, held?: string[]): Promise<{ sequence: number }>;
  /** Whether this link holds a live mirror answering to the id (the hub side only). */
  holds?(id: string): boolean;
  /** Whether this link has a mirror record for the id, even if its lease lapsed (hub only). */
  mirrored?(id: string): boolean;
  /** Replace this side's mirror of the peer's presence. */
  mirror(presence: Pick<BridgePresence, "hosts" | "participants">): Promise<void>;
  /** Original ids of events the peer bridged into this side after a sequence, one bounded page. */
  bridgedIds(after: number): Promise<BridgedIds>;
  /** End the transport and fail pending calls (a remote side only). */
  close?(error?: Error): void;
  /** The peer's records this side holds now (the hub side only): the link's current authority. */
  owned?(): Promise<Pick<BridgePresence, "hosts" | "participants">>;
  /** Fence every mirror write, wait for one in flight, then delete this link's mirrors. */
  withdraw?(): Promise<void>;
}

export interface BridgedIds { ids: string[]; through: number; done: boolean }
/** One bridgedIds page holds at most this many ids (about 2 MiB on the wire). */
const BRIDGED_IDS_PAGE = 50_000;

const remoteHostOf = (value: unknown): string | undefined =>
  isObject(value) && typeof value.remoteHost === "string" ? value.remoteHost : undefined;

const hostOf = (key: string, value: unknown): FabricHostRecord | undefined => {
  if (!isObject(value) || value.format !== 1 || typeof value.id !== "string") return undefined;
  if (key !== keyFor(HOST_PREFIX, value.id) || typeof value.rootId !== "string" || !isIdentity(value.identity)) return undefined;
  if (typeof value.startedAt !== "number" || typeof value.updatedAt !== "number" || typeof value.expiresAt !== "number") return undefined;
  return value as unknown as FabricHostRecord;
};

const participantOf = (key: string, value: unknown): FabricParticipantRecord | undefined => {
  if (!isObject(value) || value.format !== 1 || typeof value.id !== "string") return undefined;
  if (key !== keyFor(PARTICIPANT_PREFIX, value.id) || typeof value.ownerHostId !== "string") return undefined;
  if (typeof value.ownerIdentityId !== "string" || typeof value.rootId !== "string") return undefined;
  return value as unknown as FabricParticipantRecord;
};

/** Host lease fields that change on every renewal; participant updatedAt is source activity. */
const settled = (value: Record<string, unknown>): string =>
  JSON.stringify({ ...value, updatedAt: undefined, expiresAt: undefined });

/**
 * A bridge side over a mesh store on this host. `peer` names the other side: it is the
 * `remoteHost` mark on mirrored records and the `bridge.from` of events it bridges in.
 */
export class StoreBridgeSide implements BridgeSide {
  constructor(
    readonly store: MeshStore,
    readonly peer: string,
    readonly now: () => number = Date.now,
  ) {
    if (!validBridgeName(peer)) throw new Error(`Invalid bridge peer name: ${peer}`);
  }

  async latestSequence(): Promise<number> {
    return this.store.latestSequence();
  }

  // A page fits one transport frame (security review F4): it ends before the event that would
  // pass the byte budget, and `through` stays at the last event it covers. One event that alone
  // passes the budget is skipped with evidence, never a stall of the cursor.
  async read(after: number, pageBytes = BRIDGE_PAGE_BYTES): Promise<BridgeRead> {
    const page = this.store.read({ after, limit: this.store.maxReadEvents });
    const events: MeshEvent[] = [];
    const skipped: BridgeSkip[] = [];
    let bytes = 0;
    let through = after;
    for (const event of page) {
      if (isBridgedTopic(event) && !hasBridgeField(event)) {
        const size = Buffer.byteLength(JSON.stringify(event), "utf8");
        if (size > pageBytes) {
          skipped.push({ id: event.id, sequence: event.sequence, topic: event.topic, bytes: size });
        } else {
          if (bytes + size > pageBytes) break;
          bytes += size;
          events.push(event);
        }
      }
      through = event.sequence;
    }
    return { events, through, ...(skipped.length ? { skipped } : {}) };
  }

  async presence(): Promise<BridgePresence> {
    const now = this.now();
    const leases = readHostLeases(this.store.root);
    const reserved = new Set<string>();
    const hosts: BridgeHost[] = [];
    // Every id not mirrored by this link is reserved: natives, live or not, and other links'
    // mirrors (security review F1/F2). Only natives are this side's presence.
    for (const entry of this.store.listAll(HOST_PREFIX, { fresh: true })) {
      const mark = remoteHostOf(entry.value);
      if (mark === this.peer) continue;
      const host = hostOf(entry.key, entry.value);
      if (!host) {
        if (isObject(entry.value) && typeof entry.value.id === "string") reserved.add(entry.value.id);
        continue;
      }
      reserved.add(host.id).add(host.identity.id).add(host.rootId);
      if (mark !== undefined || entry.updatedBy.id !== host.identity.id) continue;
      const expiresAt = hostLeaseExpiry(leases, host);
      // File-only heartbeats leave the state record old. Carry the effective lease's
      // renewal time too, so the peer measures its TTL rather than the state's age.
      const lease = leases.get(host.id);
      const record = lease && lease.rootId === host.rootId && lease.identityId === host.identity.id &&
        lease.expiresAt >= host.expiresAt
        ? { ...host, updatedAt: lease.updatedAt, expiresAt }
        : host;
      if (expiresAt > now) hosts.push({ record, expiresAt });
    }
    const live = new Map(hosts.map((host) => [host.record.id, host.record]));
    const participants = new Map<string, { record: FabricParticipantRecord; updatedAt: number }>();
    for (const entry of this.#participantEntries()) {
      const mark = remoteHostOf(entry.value);
      if (mark === this.peer) continue;
      const participant = participantOf(entry.key, entry.value);
      if (!participant) {
        if (isObject(entry.value) && typeof entry.value.id === "string") reserved.add(entry.value.id);
        continue;
      }
      reserved.add(participant.id).add(participant.ownerIdentityId).add(participant.rootId);
      if (typeof participant.name === "string" && participant.name) reserved.add(participant.name);
      // Labels are minted per mesh and collide by design; they are never addresses across.
      if (typeof participant.sessionId === "string" && participant.sessionId) reserved.add(participant.sessionId);
      const owner = live.get(participant.ownerHostId);
      if (
        mark === undefined && participant.kind === "root" && owner && entry.updatedBy.id === participant.ownerIdentityId &&
        owner.identity.id === participant.ownerIdentityId && owner.rootId === participant.rootId &&
        (participants.get(participant.id)?.updatedAt ?? -Infinity) < entry.updatedAt
      ) participants.set(participant.id, { record: participant, updatedAt: entry.updatedAt });
    }
    return { hosts, participants: [...participants.values()].map(({ record }) => record), reserved: [...reserved] };
  }

  // Participant entries of both kinds: the shared state's (natives and mirrors) and the natives'
  // own files (smarty-dev#2004; under the participants-files policy natives are only there).
  #participantEntries(): MeshStateEntry[] {
    return [...this.store.listAll(PARTICIPANT_PREFIX, { fresh: true }), ...readParticipantFiles(this.store.root, { maxAgeMs: 0 })];
  }

  async publish(event: BridgePublish, held: string[] = []): Promise<{ sequence: number }> {
    const checked = checkBridgePublish(event);
    const data = { ...checked.data, bridge: { from: this.peer, id: checked.data.bridge.id } };
    const published = await this.store.publish({
      ...checked,
      from: { ...checked.from, verified: "bridge" },
      // Evaluated under the mesh lock that commits the event, so the ownership it checks is the
      // ownership at commit: a native takeover before it refuses the event (security review
      // round 3, F2). Every state writer takes the same lock. Under the participants-files policy a
      // native's first file is written without it; its host record, which reserves the root id,
      // still goes through the lock, and a mirror never outranks a native file (#142 S2).
      data: () => {
        for (const id of held) {
          if (!this.holds(id)) throw new BridgeOwnershipError(`${id} is no longer bound to bridge link ${this.peer}`);
        }
        return data;
      },
    });
    return { sequence: published.sequence };
  }

  /**
   * Whether this link holds `id` now. Run under the mesh lock that commits a bridged event, so it
   * decides on the ownership at commit (security review rounds 3 and 4, F2). Any record for the
   * id that is not this link's live mirror (a native, another link's, or a reserved id anywhere
   * on this side) is a denial, never a gap to fill from another mirror: `id` must be this
   * link's host, with a live lease, and its root, if present, must be this link's too.
   */
  holds(id: string): boolean {
    const now = this.now();
    const hostEntry = this.store.get(keyFor(HOST_PREFIX, id), { fresh: true });
    const participantEntry = this.store.get(keyFor(PARTICIPANT_PREFIX, id), { fresh: true });
    if (!hostEntry || remoteHostOf(hostEntry.value) !== this.peer) return false;
    if (participantEntry && remoteHostOf(participantEntry.value) !== this.peer) return false;
    // A native's own file, readable or not (fail closed, security pass S3 on #142).
    if (participantFilePresent(this.store.root, keyFor(PARTICIPANT_PREFIX, id))) return false;
    const host = hostOf(hostEntry.key, hostEntry.value);
    if (!host || host.id !== id || host.identity.id !== id || host.rootId !== id || hostEntry.updatedBy.id !== id) return false;
    if (hostLeaseExpiry(readHostLeases(this.store.root), host) <= now) return false;
    if (participantEntry) {
      const participant = participantOf(participantEntry.key, participantEntry.value);
      if (!participant || participant.ownerHostId !== id || participant.ownerIdentityId !== id) return false;
    }
    return !this.#reservedNow().has(id);
  }

  mirrored(id: string): boolean {
    return remoteHostOf(this.store.get(keyFor(HOST_PREFIX, id), { fresh: true })?.value) === this.peer;
  }

  // Every id this side holds that is not this link's mirror, read now.
  #reservedNow(): Set<string> {
    const reserved = new Set<string>();
    for (const entries of [this.store.listAll(HOST_PREFIX, { fresh: true }), this.#participantEntries()]) {
      for (const entry of entries) {
        if (remoteHostOf(entry.value) === this.peer || !isObject(entry.value)) continue;
        const value = entry.value;
        for (const field of ["id", "rootId", "ownerHostId", "ownerIdentityId", "sessionId", "name"]) {
          if (typeof value[field] === "string" && value[field]) reserved.add(value[field] as string);
        }
        if (isIdentity(value.identity)) reserved.add(value.identity.id);
      }
    }
    return reserved;
  }


  #fenced = false;
  #inflight: Promise<void> = Promise.resolve();

  /**
   * Mirror the peer's presence. Calls run one at a time; after withdraw() no call writes again,
   * and each write checks the fence, so a mirror in flight cannot restore a record or lease after
   * the withdrawal (security review round 2, F3).
   */
  mirror(presence: Pick<BridgePresence, "hosts" | "participants">): Promise<void> {
    if (this.#fenced) return Promise.resolve();
    const run = this.#inflight.then(() => this.#mirror(presence, false));
    this.#inflight = run.catch(() => undefined);
    return run;
  }

  async withdraw(): Promise<void> {
    this.#fenced = true;
    await this.#inflight;
    await this.#mirror({ hosts: [], participants: [] }, true);
  }

  async owned(): Promise<Pick<BridgePresence, "hosts" | "participants">> {
    const now = this.now();
    const leases = readHostLeases(this.store.root);
    const hosts: BridgeHost[] = [];
    for (const entry of this.store.listAll(HOST_PREFIX, { fresh: true })) {
      if (remoteHostOf(entry.value) !== this.peer) continue;
      const host = hostOf(entry.key, entry.value);
      if (!host || entry.updatedBy.id !== host.identity.id) continue;
      const expiresAt = hostLeaseExpiry(leases, host);
      if (expiresAt > now) hosts.push({ record: host, expiresAt });
    }
    const live = new Map(hosts.map((host) => [host.record.id, host.record]));
    const participants: FabricParticipantRecord[] = [];
    for (const entry of this.store.listAll(PARTICIPANT_PREFIX, { fresh: true })) {
      if (remoteHostOf(entry.value) !== this.peer) continue;
      const participant = participantOf(entry.key, entry.value);
      const owner = participant && live.get(participant.ownerHostId);
      if (
        participant && owner && participant.kind === "root" && entry.updatedBy.id === participant.ownerIdentityId &&
        owner.identity.id === participant.ownerIdentityId && owner.rootId === participant.rootId
      ) participants.push(participant);
    }
    return { hosts, participants };
  }

  async #mirror(presence: Pick<BridgePresence, "hosts" | "participants">, final: boolean): Promise<void> {
    const halted = (): boolean => this.#fenced && !final;
    const now = this.now();
    const own = new Set((await this.presence()).reserved);
    const wanted = new Map<string, { value: Record<string, unknown>; identity: MeshIdentity }>();
    const leases: Array<{ id: string; rootId: string; identityId: string; expiresAt: number }> = [];
    const hosts = new Map<string, FabricHostRecord>();
    for (const { record, expiresAt } of presence.hosts) {
      if (own.has(record.id) || own.has(record.identity.id) || own.has(record.rootId)) continue;
      // Mirror a still-live observation for one source TTL from this side's sync,
      // not until the source's absolute expiry. Even a final observation just before
      // the source expires can therefore extend a stopped host by at most one TTL.
      const ttl = Math.min(BRIDGE_LEASE_MS, expiresAt - record.updatedAt);
      if (expiresAt <= now || !Number.isFinite(ttl) || ttl <= 0) continue;
      const until = now + ttl;
      hosts.set(record.id, record);
      wanted.set(keyFor(HOST_PREFIX, record.id), {
        value: { ...record, updatedAt: now, expiresAt: until, remoteHost: this.peer },
        identity: record.identity,
      });
      leases.push({ id: record.id, rootId: record.rootId, identityId: record.identity.id, expiresAt: until });
    }
    for (const participant of presence.participants) {
      const owner = hosts.get(participant.ownerHostId);
      if (!owner || participant.kind !== "root" || own.has(participant.id)) continue;
      if (owner.identity.id !== participant.ownerIdentityId || owner.rootId !== participant.rootId) continue;
      wanted.set(keyFor(PARTICIPANT_PREFIX, participant.id), {
        value: { ...participant, remoteHost: this.peer },
        identity: owner.identity,
      });
    }
    const mirrored = new Map<string, { value: unknown; version: number }>();
    for (const prefix of [HOST_PREFIX, PARTICIPANT_PREFIX]) {
      for (const entry of this.store.listAll(prefix, { fresh: true })) {
        mirrored.set(entry.key, { value: entry.value, version: entry.version });
      }
    }
    for (const [key, { value, identity }] of wanted) {
      const existing = mirrored.get(key);
      // Never replace a native record, or another bridge's mirror (anti-spoofing); a native may be
      // only in its own file (smarty-dev#2004).
      if (existing && remoteHostOf(existing.value) !== this.peer) continue;
      if (key.startsWith(PARTICIPANT_PREFIX) && participantFilePresent(this.store.root, key)) continue;
      if (
        existing && isObject(existing.value) && settled(existing.value) === settled(value) &&
        (key.startsWith(HOST_PREFIX)
          ? typeof existing.value.updatedAt !== "number" || now - existing.value.updatedAt < STATE_LEASE_RENEW_MS
          : existing.value.updatedAt === value.updatedAt)
      ) continue;
      if (halted()) return;
      await this.#put(key, value, identity, existing?.version);
    }
    // A lease renews only a host record this link holds now, as written: never another owner's.
    for (const lease of leases) {
      if (halted()) return;
      const entry = this.store.get(keyFor(HOST_PREFIX, lease.id), { fresh: true });
      const held = entry && remoteHostOf(entry.value) === this.peer ? hostOf(entry.key, entry.value) : undefined;
      if (!held || held.identity.id !== lease.identityId || held.rootId !== lease.rootId) continue;
      writeHostLease(this.store.root, { ...lease, updatedAt: now });
    }
    for (const [key, { value, version }] of mirrored) {
      if (halted()) return;
      if (wanted.has(key) || remoteHostOf(value) !== this.peer) continue;
      if (key.startsWith(HOST_PREFIX) && isObject(value) && typeof value.id === "string") {
        removeHostLease(this.store.root, value.id);
      }
      await this.store.delete({ key, ifVersion: version }).catch(ignoreConflict);
    }
  }

  // Compare-and-swap only: a mirror never replaces a record written since it looked. A deleted
  // key keeps its tombstone version, so creating it again swaps against that version, once the
  // key is seen still absent.
  async #put(key: string, value: unknown, identity: MeshIdentity, version: number | undefined): Promise<void> {
    try {
      await this.store.put({ key, value, identity, ifVersion: version ?? 0 });
    } catch (error) {
      const found = version === undefined && error instanceof Error
        ? /compare-and-swap failed .* found (\d+)$/.exec(error.message)?.[1]
        : undefined;
      if (found === undefined || this.store.get(key, { fresh: true })) return ignoreConflict(error);
      await this.store.put({ key, value, identity, ifVersion: Number(found) }).catch(ignoreConflict);
    }
  }

  async bridgedIds(after: number): Promise<BridgedIds> {
    const ids: string[] = [];
    let cursor = after;
    while (true) {
      const page = this.store.read({ after: cursor, limit: this.store.maxReadEvents });
      for (const event of page) {
        const stamp = bridgeStampOf(event);
        if (stamp?.from === this.peer) ids.push(stamp.id);
      }
      if (page.length < this.store.maxReadEvents) return { ids, through: page.at(-1)?.sequence ?? cursor, done: true };
      cursor = page.at(-1)!.sequence;
      if (ids.length >= BRIDGED_IDS_PAGE) return { ids, through: cursor, done: false };
    }
  }

}

/** An event refused at commit: its sender or target is no longer bound to this link. */
export class BridgeOwnershipError extends Error {}

const ignoreConflict = (error: unknown): void => {
  // A concurrent writer changed the key: the next presence round decides again.
  if (error instanceof Error && /compare-and-swap failed/.test(error.message)) return;
  throw error;
};

/** The shape and allow-list check a side applies to every event it is asked to publish. */
export const checkBridgePublish = (input: unknown): BridgePublish => {
  if (!isObject(input)) throw new Error("Bridge publish is not an object");
  const { topic, kind, from, to, text, data } = input;
  if (typeof topic !== "string" || typeof kind !== "string" || !isBridgedTopic({ topic, kind })) {
    throw new Error(`Bridge topic is not allowed: ${String(topic)}`);
  }
  if (!isIdentity(from)) throw new Error("Bridge publish has no sender identity");
  if (typeof to !== "string" || !to.trim()) throw new Error("Bridge publish has no recipient");
  if (text !== undefined && typeof text !== "string") throw new Error("Bridge publish text is not a string");
  if (!isObject(data) || !bridgeStampOf({ data })) throw new Error("Bridge publish has no bridge stamp");
  return {
    topic, kind, to,
    from: { id: from.id, name: from.name, kind: from.kind, ...(typeof from.sessionId === "string" ? { sessionId: from.sessionId } : {}) },
    ...(text !== undefined ? { text } : {}),
    data: data as BridgePublish["data"],
  };
};

// ---------------------------------------------------------------------------------------------
// Stdio transport: one JSON request or response per line.

type RpcOp = "hello" | "latestSequence" | "read" | "presence" | "publish" | "mirror" | "bridgedIds";

interface RpcRequest { id: number; op: RpcOp; args?: unknown }

const lines = (input: Readable, onLine: (line: string) => void, onEnd: (error?: Error) => void): void => {
  let buffered = "";
  input.setEncoding("utf8");
  input.on("data", (chunk: string) => {
    buffered += chunk;
    let newline: number;
    while ((newline = buffered.indexOf("\n")) >= 0) {
      const line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      if (line.length > MAX_RPC_LINE_BYTES) {
        // A complete line over the limit fails the transport too.
        input.destroy();
        onEnd(new Error("Bridge transport line is too long"));
        return;
      }
      if (line.trim()) onLine(line);
    }
    if (buffered.length > MAX_RPC_LINE_BYTES) {
      input.destroy();
      onEnd(new Error("Bridge transport line is too long"));
    }
  });
  input.on("end", () => onEnd());
  input.on("close", () => onEnd());
  input.on("error", (error) => onEnd(error));
};

/**
 * The remote end of the transport (`mesh-bridge agent`), run by the forced ssh command. It serves
 * only the side operations above against its own mesh, and applies the same allow-list and the
 * pinned peer name to everything it is asked to write. Resolves when the input ends.
 */
export const serveBridgeAgent = (side: StoreBridgeSide, input: Readable, output: Writable): Promise<void> =>
  new Promise((resolve) => {
    let queue = Promise.resolve();
    const reply = (value: unknown): void => {
      output.write(`${JSON.stringify(value)}\n`);
    };
    lines(
      input,
      (line) => {
        queue = queue.then(async () => {
          let request: RpcRequest;
          try {
            request = JSON.parse(line) as RpcRequest;
          } catch {
            reply({ id: null, ok: false, error: "Malformed bridge request" });
            return;
          }
          try {
            reply({ id: request.id, ok: true, result: await dispatch(side, request) });
          } catch (error) {
            reply({
              id: request.id, ok: false, error: error instanceof Error ? error.message : String(error),
              ...(isMeshLockTimeout(error) ? { code: MESH_LOCK_TIMEOUT_CODE } : {}),
            });
          }
        });
      },
      () => void queue.then(() => resolve()),
    );
  });

const numberArg = (args: unknown, name: string): number => {
  const value = isObject(args) ? args[name] : undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new Error(`Bridge argument ${name} is invalid`);
  return value;
};

const presenceArg = (args: unknown): Pick<BridgePresence, "hosts" | "participants"> => {
  if (!isObject(args) || !Array.isArray(args.hosts) || !Array.isArray(args.participants)) {
    throw new Error("Bridge presence is invalid");
  }
  const hosts = args.hosts.flatMap((host): BridgeHost[] => {
    if (!isObject(host) || typeof host.expiresAt !== "number" || !isObject(host.record)) return [];
    const record = hostOf(keyFor(HOST_PREFIX, String(host.record.id)), host.record);
    return record && remoteHostOf(record) === undefined ? [{ record, expiresAt: host.expiresAt }] : [];
  });
  const participants = args.participants.flatMap((value): FabricParticipantRecord[] => {
    const record = isObject(value) ? participantOf(keyFor(PARTICIPANT_PREFIX, String(value.id)), value) : undefined;
    return record && remoteHostOf(record) === undefined ? [record] : [];
  });
  return { hosts, participants };
};

const dispatch = async (side: StoreBridgeSide, request: RpcRequest): Promise<unknown> => {
  switch (request.op) {
    case "hello": return { version: BRIDGE_PROTOCOL_VERSION };
    case "latestSequence": return side.latestSequence();
    case "read": return side.read(numberArg(request.args, "after"));
    case "presence": return { ...boundPresence(await side.presence()).presence, reserved: [] };
    case "publish": return side.publish(checkBridgePublish(request.args));
    case "mirror": return side.mirror(presenceArg(request.args));
    case "bridgedIds": return side.bridgedIds(numberArg(request.args, "after"));
    default: throw new Error(`Unknown bridge operation: ${String(request.op)}`);
  }
};

/** The hub's view of the remote side, over the transport's stdio. */
export class RemoteBridgeSide implements BridgeSide {
  #next = 1;
  #closed: Error | undefined;
  readonly #pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
  readonly #onClosed: (error: Error) => void;
  readonly closed: Promise<Error>;

  /**
   * Every call has a deadline: a remote that stops answering closes the transport, so an
   * untrusted peer never decides whether the bridge can stop (security review F3).
   */
  constructor(readonly input: Readable, readonly output: Writable, readonly callTimeoutMs = DEFAULT_CALL_TIMEOUT_MS) {
    let onClosed!: (error: Error) => void;
    this.closed = new Promise((resolve) => (onClosed = resolve));
    this.#onClosed = onClosed;
    lines(
      input,
      (line) => {
        let response: { id?: unknown; ok?: unknown; result?: unknown; error?: unknown; code?: unknown };
        try {
          response = JSON.parse(line) as typeof response;
        } catch {
          return;
        }
        const pending = typeof response.id === "number" ? this.#pending.get(response.id) : undefined;
        if (!pending) return;
        this.#pending.delete(response.id as number);
        if (response.ok === true) pending.resolve(response.result);
        else {
          const error = new Error(`Bridge agent: ${String(response.error)}`);
          // The protocol carries just this safe store code, not arbitrary peer error properties.
          if (response.code === MESH_LOCK_TIMEOUT_CODE) Object.assign(error, { code: MESH_LOCK_TIMEOUT_CODE });
          pending.reject(error);
        }
      },
      (error) => this.close(error ?? new Error("Bridge transport closed")),
    );
  }

  /** Fail every pending and later call, and end the transport's streams. */
  close(error: Error = new Error("Bridge transport closed")): void {
    if (this.#closed) return;
    this.#closed = error;
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
    this.#onClosed(error);
    this.output.end();
    this.input.destroy();
  }

  #call<T>(op: RpcOp, args?: unknown): Promise<T> {
    if (this.#closed) return Promise.reject(this.#closed);
    const id = this.#next++;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(
        () => this.close(new Error(`Bridge agent did not answer ${op} within ${this.callTimeoutMs} ms`)),
        this.callTimeoutMs,
      );
      const settle = <V>(done: (value: V) => void) => (value: V): void => {
        clearTimeout(timer);
        done(value);
      };
      this.#pending.set(id, { resolve: settle(resolve as (value: unknown) => void), reject: settle(reject) });
      this.output.write(`${JSON.stringify({ id, op, ...(args === undefined ? {} : { args }) })}\n`);
    });
  }

  async hello(): Promise<void> {
    const reply = await this.#call<{ version?: unknown }>("hello");
    if (reply?.version !== BRIDGE_PROTOCOL_VERSION) throw new Error(`Bridge agent speaks protocol ${String(reply?.version)}`);
  }

  latestSequence(): Promise<number> { return this.#call("latestSequence"); }
  read(after: number): Promise<BridgeRead> { return this.#call("read", { after }); }
  presence(): Promise<BridgePresence> { return this.#call("presence"); }
  publish(event: BridgePublish): Promise<{ sequence: number }> { return this.#call("publish", event); }
  mirror(presence: Pick<BridgePresence, "hosts" | "participants">): Promise<void> { return this.#call("mirror", presence); }
  bridgedIds(after: number): Promise<BridgedIds> { return this.#call("bridgedIds", { after }); }
}

// ---------------------------------------------------------------------------------------------
// The bridge loop.

interface DirectionCursor {
  /** The last source sequence handled. */
  after: number;
  /** The destination sequence of the last event bridged in (restart dedupe starts after it). */
  mark: number;
}

interface CursorFile {
  format: 1;
  local: string;
  remote: string;
  toRemote: DirectionCursor;
  toLocal: DirectionCursor;
}

export interface MeshBridgeOptions {
  /** This (hub) side's name, as the remote sees it. */
  localName: string;
  /** The remote side's name, as the hub sees it. */
  remoteName: string;
  local: BridgeSide;
  remote: BridgeSide;
  cursorPath: string;
  pollMs?: number;
  presenceMs?: number;
  /** Bound on each wait in stop(); the remote cannot hold a stop longer. */
  stopMs?: number;
  log?: (message: string) => void;
}

export interface BridgeStepResult {
  toRemote: number;
  toLocal: number;
  dropped: number;
}

// Where the hub routes to: the peer's canonical ids only. A label, session id or name is never
// an address across, since a hub recipient's session name is not in any record the bridge can
// reserve (security review round 2, F1).
const remoteAddresses = (presence: Pick<BridgePresence, "hosts" | "participants">): Set<string> => {
  const names = new Set<string>();
  for (const { record } of presence.hosts) names.add(record.id).add(record.identity.id);
  for (const participant of presence.participants) names.add(participant.id).add(participant.rootId);
  return names;
};

// Where the hub accepts events from the peer: its own natives, by id or unique alias.
const addresses = (presence: Pick<BridgePresence, "hosts" | "participants">): Set<string> => {
  const names = remoteAddresses(presence);
  for (const participant of presence.participants) {
    for (const alias of [participant.label, participant.sessionId]) if (alias) names.add(alias);
  }
  return names;
};

/**
 * A root's canonical id (`session:<session id>`); its host id and identity id are the same. An
 * admitted remote id must have this form. A native root never accepts a name in this namespace
 * other than its own id (RootInbox, ROOT_ID_PREFIX), and every native participant's id and name
 * is reserved, so no hub recipient but the remote owner answers to an admitted id.
 */
const CANONICAL_ID = new RegExp(`^${ROOT_ID_PREFIX}[A-Za-z0-9-]{8,128}$`);

/**
 * The remote's presence as this link admits it: hosts and roots none of whose ids or aliases
 * are reserved on the hub (natives, live or not, and other links' mirrors), and roots under an
 * admitted host. Mirroring, routing and sender checks all use this snapshot, never the peer's
 * raw claims (security review F1/F2).
 */
export const admitPresence = (
  claimed: Pick<BridgePresence, "hosts" | "participants">,
  reserved: ReadonlySet<string>,
): Pick<BridgePresence, "hosts" | "participants"> => {
  // v1 bridges Main roots only, whose host id, identity id and root id are one id (security
  // review round 4, F2): a host that splits them is refused, so one id names one owner.
  const hosts = claimed.hosts.filter(({ record }) =>
    record.identity.id === record.id && record.rootId === record.id &&
    CANONICAL_ID.test(record.id) && !reserved.has(record.id));
  const admitted = new Map(hosts.map(({ record }) => [record.id, record]));
  const participants = claimed.participants.filter((participant) => {
    const owner = admitted.get(participant.ownerHostId);
    return participant.kind === "root" && owner !== undefined && participant.id === owner.id &&
      owner.identity.id === participant.ownerIdentityId && owner.rootId === participant.rootId &&
      ![participant.id, participant.rootId, participant.ownerIdentityId, participant.sessionId]
        .some((id) => typeof id === "string" && reserved.has(id));
  });
  return { hosts, participants };
};

/**
 * A presence snapshot that fits one frame (security review round 2, F4): whole hosts with their
 * roots, in order, until the budget. Hosts past it are left out, so their mirrors lapse and
 * senders get the normal lapsed error; the bridge logs how many.
 */
export const boundPresence = (
  presence: Pick<BridgePresence, "hosts" | "participants">,
  budget = BRIDGE_PAGE_BYTES,
): { presence: Pick<BridgePresence, "hosts" | "participants">; dropped: number } => {
  const roots = new Map<string, FabricParticipantRecord[]>();
  for (const participant of presence.participants) {
    roots.set(participant.ownerHostId, [...(roots.get(participant.ownerHostId) ?? []), participant]);
  }
  const hosts: BridgeHost[] = [];
  const participants: FabricParticipantRecord[] = [];
  let bytes = 0;
  let dropped = 0;
  for (const host of presence.hosts) {
    const own = roots.get(host.record.id) ?? [];
    const size = Buffer.byteLength(JSON.stringify([host, own]), "utf8");
    if (bytes + size > budget) {
      dropped += 1;
      continue;
    }
    bytes += size;
    hosts.push(host);
    participants.push(...own);
  }
  return { presence: { hosts, participants }, dropped };
};

const senders = (presence: Pick<BridgePresence, "hosts" | "participants">): Set<string> => {
  const ids = new Set<string>();
  for (const { record } of presence.hosts) ids.add(record.identity.id);
  for (const participant of presence.participants) ids.add(participant.id).add(participant.ownerIdentityId);
  return ids;
};

export class MeshBridge {
  #cursor: CursorFile | undefined;
  #seen = { toRemote: new Set<string>(), toLocal: new Set<string>() };
  #presenceAt = Number.NEGATIVE_INFINITY;
  #presenceSync: Promise<void> | undefined;
  #stopped = false;
  #wake: (() => void) | undefined;
  /** The loop's current pass, so stop() can fence it before the final withdrawal. */
  #running: Promise<void> = Promise.resolve();

  constructor(readonly options: MeshBridgeOptions) {
    for (const name of [options.localName, options.remoteName]) {
      if (!validBridgeName(name)) throw new Error(`Invalid bridge name: ${name}`);
    }
    if (options.localName === options.remoteName) throw new Error("Bridge side names must differ");
  }

  #log(message: string): void {
    this.options.log?.(message);
  }

  /** Load the cursor (or start at both heads) and the ids bridged since the last save. */
  async start(): Promise<void> {
    const { local, remote, localName, remoteName } = this.options;
    let saved: CursorFile | undefined;
    try {
      const value = JSON.parse(readFileRetrying(this.options.cursorPath)) as CursorFile;
      if (value?.format === 1 && value.local === localName && value.remote === remoteName) saved = value;
      else this.#log(`cursor ${this.options.cursorPath} is for another bridge; starting at the heads`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    if (saved) {
      this.#cursor = saved;
    } else {
      const [localHead, remoteHead] = await Promise.all([local.latestSequence(), remote.latestSequence()]);
      this.#cursor = {
        format: 1, local: localName, remote: remoteName,
        toRemote: { after: localHead, mark: remoteHead },
        toLocal: { after: remoteHead, mark: localHead },
      };
      this.#save();
    }
    const [toRemote, toLocal] = await Promise.all([
      allBridgedIds(remote, this.#cursor.toRemote.mark),
      allBridgedIds(local, this.#cursor.toLocal.mark),
    ]);
    this.#seen = { toRemote, toLocal };
  }

  #save(): void {
    fs.mkdirSync(path.dirname(this.options.cursorPath), { recursive: true, mode: 0o700 });
    writeJsonAtomic(this.options.cursorPath, this.#cursor, { mode: 0o600 });
  }

  /** Mirror each side's live roots into the other; concurrent callers share one pass. */
  syncPresence(): Promise<void> {
    return this.#presenceSync ??= this.#syncPresence().finally(() => { this.#presenceSync = undefined; });
  }

  async #syncPresence(): Promise<void> {
    const syncedAt = Date.now();
    const [local, claimed] = await Promise.all([this.options.local.presence(), this.options.remote.presence()]);
    const remote = admitPresence(claimed, new Set(local.reserved));
    if (this.#stopped) return;
    const outbound = boundPresence(local);
    if (outbound.dropped) this.#log(`presence: ${outbound.dropped} hosts pass the frame budget and are not mirrored`);
    // Both settle before a failure is reported, so no local write outlives this pass.
    const results = await Promise.allSettled([this.options.remote.mirror(outbound.presence), this.options.local.mirror(remote)]);
    // A transient failure must not hide a simultaneous permanent/transport failure.
    const failures = results.filter((result) => result.status === "rejected");
    const failure = failures.find((result) => !isMeshLockTimeout(result.reason)) ?? failures[0];
    if (failure) throw failure.reason;
    // A lock-delayed write must not make an old snapshot look freshly observed.
    this.#presenceAt = syncedAt;
  }

  /**
   * Who may be reached and who may speak across, read after the mirror step and again for every
   * page: only the peer's records this side holds now, still clear of every reserved hub id. A
   * claim refused or lost in this pass is never trusted (security review round 2, F1/F2).
   */
  async #authority(refreshIfStale = true): Promise<{ local: BridgePresence; remote: Pick<BridgePresence, "hosts" | "participants"> }> {
    const { local } = this.options;
    if (!local.owned) throw new Error("The hub side of a bridge must report the records it holds");
    // A page/read/write can wait on the mesh lock longer than a mirrored lease.
    // Refresh once before denying stale authority, sharing the normal presence pass.
    // Store lock and transport call timeouts bound it; no retry loop or fresh-message RPC.
    if (refreshIfStale && Date.now() - this.#presenceAt > DEFAULT_PRESENCE_MS) await this.syncPresence();
    const [presence, owned] = await Promise.all([local.presence(), local.owned()]);
    return { local: presence, remote: admitPresence(owned, new Set(presence.reserved)) };
  }

  /** One pass: presence when due, then every new event in both directions. */
  async step(): Promise<BridgeStepResult> {
    if (!this.#cursor) await this.start();
    if (Date.now() - this.#presenceAt >= (this.options.presenceMs ?? DEFAULT_PRESENCE_MS)) {
      await this.syncPresence();
    }
    if (this.#stopped) return { toRemote: 0, toLocal: 0, dropped: 0 };
    const toRemote = await this.#forward("toRemote", this.options.local, this.options.remote, async (refreshIfStale = true) => {
      const { remote } = await this.#authority(refreshIfStale);
      return { recipients: remoteAddresses(remote) };
    });
    const toLocal = await this.#forward("toLocal", this.options.remote, this.options.local, async (refreshIfStale = true) => {
      const { local, remote } = await this.#authority(refreshIfStale);
      // The remote is untrusted: its sender (and an ack's target) must be bound to this link.
      return { recipients: addresses(local), senders: senders(remote), reserved: new Set(local.reserved) };
    });
    return { toRemote: toRemote.forwarded, toLocal: toLocal.forwarded, dropped: toRemote.dropped + toLocal.dropped };
  }

  async #forward(
    direction: "toRemote" | "toLocal",
    source: BridgeSide,
    target: BridgeSide,
    authority: (refreshIfStale?: boolean) => Promise<{ recipients: Set<string>; senders?: Set<string>; reserved?: Set<string> }>,
  ): Promise<{ forwarded: number; dropped: number }> {
    const cursor = this.#cursor![direction];
    const seen = this.#seen[direction];
    let forwarded = 0;
    let dropped = 0;
    while (true) {
      const start = cursor.after;
      const page = await source.read(start);
      // Even filtered/skip-only reads can carry this drain past the mirrors' lease.
      // Renew only when due, independently of whether the page needs routing authority.
      if (Date.now() - this.#presenceAt >= (this.options.presenceMs ?? DEFAULT_PRESENCE_MS)) await this.syncPresence();
      // Authority reads are deliberately canonical (copied-marker ABA), so idle pages
      // avoid them; every page with events retains the same fresh authority checks.
      let rules = page.events.length ? await authority() : { recipients: new Set<string>() };
      for (const skip of Array.isArray(page.skipped) ? page.skipped : []) {
        dropped += 1;
        this.#log(`${direction}: skipped ${skip.topic} ${skip.id} (sequence ${skip.sequence}): ${skip.bytes} bytes pass the ${BRIDGE_PAGE_BYTES}-byte frame budget`);
      }
      for (const event of page.events) {
        if (event.sequence <= cursor.after) continue;
        let refreshed = false;
        const refresh = async (): Promise<void> => {
          refreshed = true;
          await this.syncPresence();
          // This event gets one refresh, even if that pass itself waited on a lock.
          rules = await authority(false);
        };
        let reason = this.#refusal(event, rules, direction);
        const lapsedId = reason === "sender is not a live participant of the remote" ? event.from.id
          : reason === "ack target is not bound to this link" && isObject(event.data) && typeof event.data.targetId === "string" ? event.data.targetId
          : reason === "not addressed across" && direction === "toRemote" ? event.to : undefined;
        if (lapsedId && this.options.local.mirrored?.(lapsedId)) {
          await refresh();
          reason = this.#refusal(event, rules, direction);
        }
        if (reason) {
          if (reason !== "not addressed across") {
            dropped += 1;
            this.#log(`${direction}: dropped ${event.topic} ${event.id} from ${event.from.id}: ${reason}`);
          }
        } else if (this.#stopped) {
          return { forwarded, dropped };
        } else if (!seen.has(event.id)) {
          // A long backlog must not outlast the mirrors' 15 s lease: renew presence when due.
          if (Date.now() - this.#presenceAt >= (this.options.presenceMs ?? DEFAULT_PRESENCE_MS)) await this.syncPresence();
          if (this.#stopped) return { forwarded, dropped };
          const data = isObject(event.data) ? event.data : {};
          try {
            // Revalidated for each event after the awaits before it: an inbound event commits only
            // while its sender (and an ack's target) is still this link's; an outbound one is sent
            // only while its recipient is (security review round 3, F2).
            const held = direction === "toLocal"
              ? [event.from.id, ...(event.topic === "fabric.control.ack" && isObject(event.data) && typeof event.data.targetId === "string" ? [event.data.targetId] : [])]
              : [];
            const publish = async (): Promise<{ sequence: number }> => {
              if (direction === "toRemote" && this.options.local.holds && !this.options.local.holds(event.to!)) {
                throw new BridgeOwnershipError(`${event.to} is no longer bound to bridge link ${this.options.remoteName}`);
              }
              // Recheck the captured destination at the last seam, including after a refresh.
              const destinationRefusal = this.#destinationRefusal(event, direction);
              if (destinationRefusal) throw new BridgeOwnershipError(destinationRefusal);
              return target.publish({
                topic: event.topic, kind: event.kind, from: event.from, to: event.to!,
                ...(event.text !== undefined ? { text: event.text } : {}),
                data: { ...data, bridge: { from: direction === "toRemote" ? this.options.localName : this.options.remoteName, id: event.id } },
              }, held);
            };
            let published: { sequence: number };
            try {
              published = await publish();
            } catch (error) {
              // Ownership errors happen before append, under the publish lock. A mirror can
              // lapse during that wait: refresh once, reread authority, then retry once.
              const ids = direction === "toLocal" ? held : [event.to!];
              if (!(error instanceof BridgeOwnershipError) || refreshed || this.#destinationRefusal(event, direction) ||
                  !ids.some((id) => this.options.local.mirrored?.(id))) throw error;
              await refresh();
              if (this.#stopped) return { forwarded, dropped };
              if (this.#refusal(event, rules, direction)) throw error;
              published = await publish();
            }
            seen.add(event.id);
            cursor.mark = Math.max(cursor.mark, published.sequence);
            cursor.after = event.sequence;
            forwarded += 1;
            this.#save();
            // Only this newly inserted ID is covered by the checkpoint; recovery IDs stay.
            seen.delete(event.id);
          } catch (error) {
            // Too large or refused by the far side: never retried, or it would stall the source.
            if (!isPermanent(error)) throw error;
            dropped += 1;
            this.#log(`${direction}: refused ${event.topic} ${event.id}: ${(error as Error).message}`);
          }
        }
        cursor.after = event.sequence;
      }
      // The page may end in events the allow-list filtered out; a page that moved may have more.
      if (page.through <= start) break;
      cursor.after = Math.max(cursor.after, page.through);
      this.#save();
    }
    return { forwarded, dropped };
  }

  #destinationRefusal(event: MeshEvent, direction: "toRemote" | "toLocal"): string | undefined {
    // This is control-plane routing metadata, not a business field on work events or ACKs.
    // Absence alone is legacy compatibility; an explicitly present undefined is malformed.
    if (event.topic !== "fabric.control.command" || !isObject(event.data) ||
        !Object.hasOwn(event.data, "destinationRemoteHost")) return undefined;
    const destination = event.data.destinationRemoteHost;
    if (destination === null) return "command destination is native and must not cross a bridge";
    if (typeof destination !== "string") return "command destinationRemoteHost is not a string or null";
    const intended = direction === "toRemote" ? this.options.remoteName : this.options.localName;
    if (destination !== intended) return `command destination is not bound to bridge link ${intended}`;
    return undefined;
  }

  #refusal(event: MeshEvent, rules: { recipients: Set<string>; senders?: Set<string>; reserved?: Set<string> }, direction: "toRemote" | "toLocal"): string | undefined {
    if (!isBridgedTopic(event) || hasBridgeField(event)) return "not allowed";
    if (!event.to || !rules.recipients.has(event.to)) return "not addressed across";
    if (event.data !== undefined && !isObject(event.data)) return "data is not an object";
    const destinationRefusal = this.#destinationRefusal(event, direction);
    if (destinationRefusal) return destinationRefusal;
    if (!isIdentity(event.from)) return "no sender identity";
    if (rules.reserved?.has(event.from.id)) return "sender claims a hub identity";
    if (rules.senders && !rules.senders.has(event.from.id)) return "sender is not a live participant of the remote";
    if (rules.senders && event.topic === "fabric.control.ack") {
      const target = isObject(event.data) ? event.data.targetId : undefined;
      if (typeof target !== "string" || !rules.senders.has(target)) return "ack target is not bound to this link";
    }
    return undefined;
  }

  /** Step until stop() or a permanent/transport failure. Lock contention retries the pass. */
  async run(): Promise<void> {
    let started = false;
    let retryMs = LOCK_RETRY_MIN_MS;
    while (!this.#stopped) {
      const pass = started ? this.step() : this.start();
      // stop() needs a settlement fence, not a second unhandled rejection of a failed pass.
      this.#running = pass.then(() => undefined, () => undefined);
      let delay: number;
      try {
        await pass;
        if (!started) {
          started = true;
          retryMs = LOCK_RETRY_MIN_MS;
          continue;
        }
        retryMs = LOCK_RETRY_MIN_MS;
        delay = this.options.pollMs ?? DEFAULT_POLL_MS;
      } catch (error) {
        if (this.#stopped) return;
        if (!isMeshLockTimeout(error)) throw error;
        delay = retryMs;
        retryMs = Math.min(retryMs * 2, LOCK_RETRY_MAX_MS);
        this.#log(`mesh lock timeout; retrying in ${delay} ms`);
      }
      if (this.#stopped) break;
      await new Promise<void>((resolve) => {
        const wake = (): void => {
          clearTimeout(timer);
          this.#wake = undefined;
          resolve();
        };
        const timer = setTimeout(wake, delay);
        this.#wake = wake;
      });
    }
  }

  /**
   * Stop the loop and withdraw both mirrors, so peers see the lapse at once. Each wait on the
   * remote is bounded, and the local withdrawal never depends on it (security review F3): the
   * loop is fenced first (a silent remote's transport is closed, which fails its pending calls),
   * so no pass in flight can write a mirror after the withdrawal.
   */
  async stop(): Promise<void> {
    const bound = this.options.stopMs ?? DEFAULT_STOP_MS;
    const within = (work: Promise<unknown>): Promise<unknown> =>
      Promise.race([work.catch(() => undefined), new Promise((resolve) => setTimeout(resolve, bound).unref?.())]);
    this.#stopped = true;
    this.#wake?.();
    await within(this.#running);
    await within(this.options.remote.mirror({ hosts: [], participants: [] }));
    this.options.remote.close?.(new Error("Bridge stopped"));
    await within(this.#running);
    const { local } = this.options;
    await (local.withdraw ? local.withdraw() : local.mirror({ hosts: [], participants: [] })).catch((error: unknown) => {
      this.#log(`local withdrawal failed: ${error instanceof Error ? error.message : String(error)}`);
    });
  }
}

const allBridgedIds = async (side: BridgeSide, after: number): Promise<Set<string>> => {
  const ids = new Set<string>();
  let cursor = after;
  for (;;) {
    const page = await side.bridgedIds(cursor);
    if (!page || !Array.isArray(page.ids) || typeof page.through !== "number") throw new Error("Bridge ids page is invalid");
    for (const id of page.ids) if (typeof id === "string") ids.add(id);
    if (page.done || page.through <= cursor) return ids;
    cursor = page.through;
  }
};

const isPermanent = (error: unknown): boolean =>
  !isMeshLockTimeout(error) && (error instanceof BridgeOwnershipError ||
    error instanceof Error && /exceeds|not allowed|no bridge stamp|no recipient|no sender|Invalid mesh topic/i.test(error.message));
