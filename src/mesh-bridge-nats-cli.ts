// nats-link (smarty-dev#15310, A' lane L5): run and inspect one file-mesh <-> NATS link.
//
//   nats-link start    --config FILE
//   nats-link dry-run  --config FILE
//   nats-link withdraw --config FILE
//   nats-link status   --config FILE
//   nats-link receipt  --config FILE (--command ID [--after SEQ] | --session ID [--nats])
//   nats-link roots    [--config FILE] [--servers URLS] [--machine M] [--creds FILE] [--ca FILE]
//                      [--include-stale] [--kind root|actor|agent|provider]... [--timeout-ms N]
//
// start checks the allow-list against org.json's principal seats, takes the link's state-directory lock,
// then runs the link in the foreground under the service manager. dry-run prints what each allow entry
// resolves to now and starts nothing. withdraw is the rollback: it stops the running link and withdraws its
// mirrors on both sides. status, receipt, roots and dry-run only read: they write no mesh state or event, no
// link state and nothing to NATS. One JSON object (roots: one array) on stdout.
// Exit 0 ok, 1 not ok or unknown (never 0 on missing data), 2 usage or config. Contract: docs/mesh-bridge-nats.md.
import fs from "node:fs";
import path from "node:path";
import { readMeshStateMovedMarker } from "./mesh/backend-fence.js";
import { listHandoffs } from "./mesh/bridge-handoff.js";
import {
  assertFabricMeshRoot,
  assertNatsLinkPlatform,
  describeNatsLinkConfig,
  loadNatsLinkConfig,
  type NatsLinkConfig,
  NatsLinkConfigError,
  NatsLinkPlatformError,
  natsLinkAllowListConfig,
  natsLinkStatePaths,
  preflightNatsLinkConfig,
} from "./mesh/bridge-nats-config.js";
import { type NatsRootInfo, type NatsRootsSource, NatsRootsUnavailableError, natsRootsSourceFromEnv, readNatsRoots } from "./mesh/bridge-nats-roots.js";
import { acquireNatsLinkLock, NatsLinkBusyError, natsLinkRootsSource, type NatsLinkWithdrawOptions, withdrawNatsLink } from "./mesh/bridge-nats-ops.js";
import {
  BridgeNatsAllowListError,
  bridgeNatsAllowKey,
  bridgeNatsDryRun,
  bridgeNatsFilePresenceRecord,
  BridgeNatsSeatPinsError,
  createBridgeNatsIdentityGate,
  loadBridgeNatsSeatPins,
} from "./mesh/bridge-nats-identity.js";
import type { BridgeNatsAllowEntry, BridgeNatsPresenceRecord, BridgeNatsPresenceView, BridgeNatsSide } from "./mesh/bridge-nats-types.js";
import { type NatsLinkCounterName, readNatsLinkCounters } from "./mesh/bridge-nats-counters.js";
import type { MeshEvent } from "./mesh/event-log.js";
import { MeshStore } from "./mesh/store.js";
import { ParticipantDirectory } from "./topology/participant-directory.js";
import type { FabricParticipantInfo, FabricParticipantKind } from "./topology/types.js";

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts).
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;
/** start's bound on a requested stop: inside systemd's TimeoutStopSec=10 (docs/mesh-bridge.md). */
const STOP_BUDGET_MS = 5_000;
const KINDS: readonly FabricParticipantKind[] = ["root", "actor", "agent", "provider"];
const SESSION = /^(?:session:)?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/;

export const USAGE = `usage:
  nats-link start    --config FILE
  nats-link dry-run  --config FILE
  nats-link withdraw --config FILE
  nats-link status   --config FILE
  nats-link receipt  --config FILE (--command ID [--after SEQ] | --session ID [--nats])
  nats-link roots    [--config FILE] [--servers URLS] [--machine M] [--creds FILE] [--ca FILE] [--include-stale] [--kind KIND]... [--timeout-ms N]`;

class UsageError extends Error {}

type Flags = { values: Map<string, string[]>; booleans: Set<string> };

const VALUED: Record<string, readonly string[]> = {
  start: ["config"],
  "dry-run": ["config"],
  withdraw: ["config"],
  status: ["config"],
  receipt: ["config", "command", "session", "after"],
  roots: ["config", "servers", "machine", "creds", "ca", "kind", "timeout-ms"],
};
const BOOLEAN: Record<string, readonly string[]> = { receipt: ["nats"], roots: ["include-stale"] };
const REPEATABLE = new Set(["kind"]);

const parseFlags = (mode: string, argv: readonly string[]): Flags => {
  const values = new Map<string, string[]>();
  const booleans = new Set<string>();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index]!;
    const name = token.startsWith("--") ? token.slice(2) : "";
    if ((BOOLEAN[mode] ?? []).includes(name) && !booleans.has(name)) {
      booleans.add(name);
      continue;
    }
    const value = argv[index + 1];
    if (!(VALUED[mode] ?? []).includes(name) || value === undefined || value.startsWith("--") ||
      (values.has(name) && !REPEATABLE.has(name))) {
      throw new UsageError(`bad argument ${JSON.stringify(token).slice(0, 80)}`);
    }
    values.set(name, [...values.get(name) ?? [], value]);
    index += 1;
  }
  return { values, booleans };
};

const one = (flags: Flags, name: string): string | undefined => flags.values.get(name)?.[0];
const required = (flags: Flags, name: string): string => {
  const value = one(flags, name);
  if (!value) throw new UsageError(`--${name} is required`);
  return value;
};

// ---------------------------------------------------------------- start

/** A running link, as `startNatsLink` (bridge-nats-link.ts, lane L2) returns it. */
export interface NatsLinkHandle {
  /** Settles when the link ends by itself: resolves on a clean end, rejects with the reason it failed. */
  readonly closed: Promise<void>;
  /** Stop sync and forwarding, fence mirror writes, close NATS. Idempotent. */
  stop(): Promise<void>;
}
export interface NatsLinkStartOptions {
  readonly signal: AbortSignal;
  readonly log: (line: string) => void;
}
export type NatsLinkStarter = (config: NatsLinkConfig, options: NatsLinkStartOptions) => Promise<NatsLinkHandle>;

// The link (lane L2) loads only for `start`: status, receipt and roots never pay for the bridge graph.
// A build without it fails start by name, never silently.
export const loadNatsLinkStarter = async (): Promise<NatsLinkStarter> => {
  let module: { startNatsLink?: unknown };
  try {
    module = await import("./mesh/bridge-nats-link.js") as { startNatsLink?: unknown };
  } catch (error) {
    throw new Error("this build has no NATS link module (bridge-nats-link)", { cause: error });
  }
  if (typeof module.startNatsLink !== "function") throw new Error("bridge-nats-link does not export startNatsLink");
  return module.startNatsLink as NatsLinkStarter;
};

const within = async (work: Promise<unknown>, ms: number): Promise<boolean> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work.then(() => true, () => true), new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), ms);
      timer.unref?.();
    })]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
};

/**
 * Run one link until `signal` aborts (exit 0) or the link ends by itself (exit 1, so the service manager
 * restarts it). A requested stop waits at most STOP_BUDGET_MS for the link to stop.
 */
export const runNatsLink = async (
  config: NatsLinkConfig,
  signal: AbortSignal,
  starter: NatsLinkStarter,
  log: (line: string) => void,
): Promise<number> => {
  if (signal.aborted) return 0;
  const handle = await starter(config, { signal, log });
  let removeAbort: (() => void) | undefined;
  const aborted = new Promise<"aborted">((resolve) => {
    if (signal.aborted) { resolve("aborted"); return; }
    const onAbort = (): void => resolve("aborted");
    signal.addEventListener("abort", onAbort, { once: true });
    removeAbort = () => signal.removeEventListener("abort", onAbort);
  });
  try {
    const outcome = await Promise.race([
      handle.closed.then(() => "closed" as const, (error: unknown) => error instanceof Error ? error : new Error(String(error))),
      aborted,
    ]);
    if (outcome === "aborted" || signal.aborted) return 0;
    log(`stopped: ${outcome === "closed" ? "the link closed" : outcome.message}`);
    return 1;
  } finally {
    removeAbort?.();
    if (!await within(handle.stop(), STOP_BUDGET_MS)) log(`stop did not finish within ${STOP_BUDGET_MS} ms`);
  }
};

// ---------------------------------------------------------------- read-only file mesh

const readOnly = (): Error => new Error("nats-link opens the mesh read-only here");
/** Every write refused: status and receipt only read (as fabric-participants). */
class ReadOnlyMeshStore extends MeshStore {
  override async publish(): Promise<never> { throw readOnly(); }
  override async put(): Promise<never> { throw readOnly(); }
  override async delete(): Promise<never> { throw readOnly(); }
  override async writeBatch(): Promise<never> { throw readOnly(); }
  override async confirmWritable(): Promise<never> { throw readOnly(); }
}

const openReadOnly = (config: NatsLinkConfig): MeshStore => {
  const root = config.file.mesh;
  assertFabricMeshRoot(root);
  // A root switched to SQLite is read there, whatever this environment selects (as fabric-participants).
  const stateBackend = config.file.stateBackend ?? (readMeshStateMovedMarker(root) !== undefined ? "sqlite" : undefined);
  return new ReadOnlyMeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, {
    lockProtocol: config.file.lockProtocol,
    ...(stateBackend === undefined ? {} : { stateBackend }),
    ...(config.file.maxStateBytes === undefined ? {} : { maxStateBytes: config.file.maxStateBytes }),
  });
};

type ParticipantRow = FabricParticipantInfo & { remoteHost?: string; bridgeVia?: string };

/** Every participant on the file mesh, stale ones included, as agents.members lists them. */
const fileParticipants = (store: MeshStore): ParticipantRow[] => {
  const id = `nats-link:${process.pid}:${Date.now()}`;
  const directory = new ParticipantDirectory(store, {
    enabled: true, hostId: id, rootId: id, identity: { id, name: "nats-link", kind: "main" }, reapDeadHosts: false,
  });
  return directory.list({ scope: "project", fresh: true, includeStale: true }) as ParticipantRow[];
};

const sideEntries = (config: NatsLinkConfig, side: BridgeNatsSide): BridgeNatsAllowEntry[] =>
  natsLinkAllowListConfig(config).entries.filter((entry) => entry.side === side);

/** A root entry names a root by id; an actor entry by owner root and name (and its id, when pinned). */
const allowMatches = (entry: BridgeNatsAllowEntry, row: Pick<ParticipantRow, "id" | "kind" | "rootId" | "name">): boolean =>
  entry.kind === "root" ? row.kind === "root" && row.id === entry.id
    : row.kind === "actor" && row.rootId === entry.rootId && row.name === entry.name && (entry.id === undefined || row.id === entry.id);

const participantSummary = (row: ParticipantRow): Record<string, unknown> => ({
  id: row.id, kind: row.kind, name: row.name, sessionId: row.sessionId ?? null, rootId: row.rootId,
  remoteHost: row.remoteHost ?? null, ownerIncarnation: row.ownerIncarnation ?? null, status: row.status,
  live: row.stale !== true, ...(row.bridgeStale ? { bridgeStale: true } : {}),
});

const readText = (file: string): string | undefined => {
  try { return fs.readFileSync(file, "utf8"); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
};

// ---------------------------------------------------------------- presence views (for L3's gate)

/** A participant kind as the identity a crossing event carries: a root participant is a "main". */
const meshKind = (kind: string): BridgeNatsPresenceRecord["kind"] => kind === "root" ? "main" : kind === "actor" ? "actor" : "agent";

const presenceView = (side: BridgeNatsSide, records: readonly BridgeNatsPresenceRecord[]): BridgeNatsPresenceView => ({
  side,
  records: (id) => records.filter((record) => record.id === id),
  list: () => records,
});

/**
 * The file mesh as L3's gate reads it: every record, live or not, built by L3's one builder
 * (bridgeNatsFilePresenceRecord), which keeps the stored remoteHost and bridgeVia apart: a spoke link's
 * re-export of another link's mirror is never taken for that spoke's direct mirror (L3 audit P0).
 */
export const filePresenceView = (rows: readonly ParticipantRow[], observedAt: number): BridgeNatsPresenceView =>
  presenceView("file", rows.map((row) => bridgeNatsFilePresenceRecord(row, { live: row.stale !== true, observedAt })));

/** NATS KV presence as L3's gate reads it: the machine comes from each record's KV key. */
export const natsPresenceView = (roots: readonly NatsRootInfo[], observedAt: number): BridgeNatsPresenceView =>
  presenceView("nats", roots.map((root) => ({
    id: root.id, name: root.name, kind: meshKind(root.kind), rootId: root.rootId,
    ...(root.sessionId ? { sessionId: root.sessionId } : {}),
    ownerHostId: root.ownerHostId, ownerIdentityId: root.ownerIdentityId,
    ...(root.ownerIncarnation ? { ownerIncarnation: root.ownerIncarnation } : {}),
    machine: root.machine, live: root.live, observedAt,
  })));

/**
 * The org.json seat check `start` makes before it opens anything: L3's gate refuses an allow entry that
 * contradicts a principal's bound session. Fails closed on a missing, unsafe or malformed org.json.
 */
const identityGate = (config: NatsLinkConfig) =>
  createBridgeNatsIdentityGate(natsLinkAllowListConfig(config), { pins: loadBridgeNatsSeatPins(config.orgJson) });

/**
 * dry-run: for every allow entry, what it resolves to on its home side right now and what the link would
 * mirror (L3's bridgeNatsDryRun). Reads the file mesh and NATS presence only; writes nothing.
 */
export const natsLinkDryRun = async (
  config: NatsLinkConfig,
  options: { readRoots?: typeof readNatsRoots; now?: () => number } = {},
): Promise<ReturnType<typeof bridgeNatsDryRun>> => {
  const gate = identityGate(config);
  const store = openReadOnly(config);
  let rows: ParticipantRow[];
  try { rows = fileParticipants(store); } finally { store.closeState(); }
  const roots = await (options.readRoots ?? readNatsRoots)(natsLinkRootsSource(config), { includeStale: true, kinds: ["root", "actor"] });
  const now = (options.now ?? Date.now)();
  return bridgeNatsDryRun(gate, { file: filePresenceView(rows, now), nats: natsPresenceView(roots, now) }, now);
};

// ---------------------------------------------------------------- status

export const natsLinkStatus = async (config: NatsLinkConfig, now = Date.now()): Promise<Record<string, unknown>> => {
  const paths = natsLinkStatePaths(config);
  const problems: string[] = [];
  let cursor: Record<string, unknown> = { path: paths.cursor, present: false };
  const cursorText = readText(paths.cursor);
  if (cursorText === undefined) {
    problems.push("no cursor: the link has never started with this stateDir");
  } else {
    try {
      const value = JSON.parse(cursorText) as { local?: unknown; remote?: unknown; toRemote?: { after?: unknown }; toLocal?: { after?: unknown } };
      const modified = fs.statSync(paths.cursor).mtimeMs;
      cursor = { path: paths.cursor, present: true, toRemote: value.toRemote?.after ?? null, toLocal: value.toLocal?.after ?? null,
        modifiedAt: new Date(modified).toISOString(), ageMs: Math.max(0, Math.round(now - modified)) };
      if (value.local !== config.name || value.remote !== config.link) problems.push("cursor names another link (local/remote differ from name/link)");
    } catch {
      problems.push("cursor unreadable");
    }
  }
  const stallText = readText(paths.stallReason)?.trim();
  if (stallText) problems.push(`stalled: ${stallText.slice(0, 500)}`);

  const store = openReadOnly(config);
  let rows: ParticipantRow[];
  try { rows = fileParticipants(store); } finally { store.closeState(); }
  const mirrored = rows.filter((row) => row.remoteHost === config.link);
  // A mirror this link holds that the allow-list does not name: the link wrote an identity it must not
  // carry. Never expected; reported as a problem so a canary or an operator sees it at once.
  for (const row of mirrored) {
    if (!sideEntries(config, "nats").some((entry) => allowMatches(entry, row))) problems.push(`mirror not on allow.nats: ${row.id}`);
  }
  // Each listed identity as the file mesh shows it: a hub identity native there (a spoke root as its spoke
  // link's mirror), a NATS one as this link's mirror.
  const listed = (entries: readonly BridgeNatsAllowEntry[]) => entries.map((entry) => {
    const { viaSpoke, republishTopics } = pr2Fields(entry);
    const row = rows.find((candidate) => allowMatches(entry, candidate) &&
      (entry.side === "file" ? candidate.remoteHost === viaSpoke && candidate.bridgeVia === undefined
        : candidate.remoteHost === config.link));
    return { identity: bridgeNatsAllowKey(entry), ...(row ? { id: row.id } : {}), present: row !== undefined,
      live: row !== undefined && row.stale !== true, send: entry.send, receive: entry.receive,
      ...(viaSpoke === undefined ? {} : { viaSpoke }), ...(republishTopics === undefined ? {} : { republishTopics }) };
  });
  const allow = { file: listed(sideEntries(config, "file")), nats: listed(sideEntries(config, "nats")) };

  // The running link's crossing counters (PR 2): what each leg carried and refused, cumulative across restarts.
  const snapshot = readNatsLinkCounters(config.stateDir);
  if (snapshot && snapshot.link !== config.link) problems.push("counters name another link");
  const count = (name: NatsLinkCounterName): number => snapshot?.counters[name] ?? 0;
  const counters = snapshot
    ? { present: true, since: snapshot.since, startedAt: snapshot.startedAt, updatedAt: snapshot.updatedAt, pid: snapshot.pid,
      ageMs: Math.max(0, Math.round(now - Date.parse(snapshot.updatedAt))), values: snapshot.counters, last: snapshot.last }
    : { present: false };
  // The config refuses republishTopics without nats.events, so a listed sender here is read from the stream.
  const republishing = sideEntries(config, "nats").filter((entry) => pr2Fields(entry).republishTopics?.length);
  const republish = {
    enabled: config.nats.events,
    senders: republishing.map((entry) => ({ identity: bridgeNatsAllowKey(entry), topics: pr2Fields(entry).republishTopics })),
    accepted: count("republish.accepted"), duplicate: count("republish.duplicate"), refused: count("republish.refused"),
  };
  const relay = {
    spokes: config.relay?.spokes ?? [],
    spokeRoots: sideEntries(config, "file").flatMap((entry) => {
      const { viaSpoke } = pr2Fields(entry);
      return viaSpoke === undefined ? [] : [{ identity: bridgeNatsAllowKey(entry), viaSpoke }];
    }),
    toNats: count("relay.toNats"), toFile: count("relay.toFile"), refused: count("relay.refused"),
  };
  return {
    ok: problems.length === 0,
    at: new Date(now).toISOString(),
    config: describeNatsLinkConfig(config),
    cursor,
    stall: { path: paths.stallReason, reason: stallText || null },
    mirrors: mirrored.map(participantSummary),
    allow,
    counters,
    republish,
    relay,
    problems,
  };
};

/**
 * The PR 2 allow fields of a root entry (L3's parser owns them): `republishTopics` on a NATS root (N4: its
 * fleet.work/ops events republished into the file mesh) and `viaSpoke` on a file root that lives on a spoke.
 */
const pr2Fields = (entry: BridgeNatsAllowEntry): { viaSpoke?: string; republishTopics?: readonly string[] } =>
  entry.kind !== "root" ? {} : {
    ...(entry.viaSpoke === undefined ? {} : { viaSpoke: entry.viaSpoke }),
    ...(entry.republishTopics === undefined ? {} : { republishTopics: entry.republishTopics }),
  };

// ---------------------------------------------------------------- receipt

const record = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};

const eventSummary = (event: MeshEvent): Record<string, unknown> => {
  const data = record(event.data);
  return {
    eventId: event.id, sequence: event.sequence, createdAt: new Date(event.createdAt).toISOString(),
    topic: event.topic, kind: event.kind, from: { id: event.from.id, kind: event.from.kind, name: event.from.name },
    to: event.to ?? null, targetId: data.targetId ?? null, ownerIncarnation: data.ownerIncarnation ?? null,
    ...(event.topic === "fabric.control.ack" ? { accepted: data.accepted ?? null, notRun: data.notRun === true,
      errorCode: data.errorCode ?? null } : { operation: data.operation ?? null }),
    ...(data.cancelCommandId !== undefined ? { cancelCommandId: data.cancelCommandId } : {}),
    bridge: data.bridge ?? null, verification: event.verification ?? null,
  };
};

/** Every event on `topic` after `after`, page by page, that `match` accepts. */
const scan = (store: MeshStore, topic: string, after: number, match: (event: MeshEvent) => boolean): MeshEvent[] => {
  const found: MeshEvent[] = [];
  for (let cursor = after; ;) {
    const page = store.read({ after: cursor, topic, limit: MAX_READ_EVENTS });
    if (page.length === 0) return found;
    for (const event of page) if (match(event)) found.push(event);
    const last = page[page.length - 1]!.sequence;
    if (last <= cursor) return found;
    cursor = last;
  }
};

/**
 * One command's file-side evidence: every command event and ACK on the file mesh with that commandId.
 * `acknowledged` (exit 0) needs exactly one command event and ACKs that agree; anything else is not proof.
 */
export const commandReceipt = (config: NatsLinkConfig, commandId: string, after = 0): Record<string, unknown> => {
  const store = openReadOnly(config);
  try {
    const head = store.latestSequence();
    const commands = scan(store, "fabric.control.command", after, (event) => record(event.data).commandId === commandId);
    const cancels = scan(store, "fabric.control.command", after, (event) => record(event.data).cancelCommandId === commandId);
    const acks = scan(store, "fabric.control.ack", after, (event) => record(event.data).commandId === commandId);
    const outcomes = new Set(acks.map((ack) => record(ack.data).accepted === true));
    const verdict = commands.length === 0 ? acks.length ? "orphan-ack" : "not-found"
      : commands.length > 1 ? "duplicate"
      : acks.length === 0 ? "pending"
      : outcomes.size > 1 ? "conflict"
      : "acknowledged";
    return {
      ok: verdict === "acknowledged",
      verdict,
      commandId,
      link: config.link,
      scanned: { after, through: head },
      commands: commands.map(eventSummary),
      cancellations: cancels.map(eventSummary),
      acks: acks.map(eventSummary),
      accepted: verdict === "acknowledged" ? [...outcomes][0] : null,
    };
  } finally {
    store.closeState();
  }
};

/**
 * One identity's route evidence: its record on the file mesh (native, or this link's mirror), any hub
 * handoff of it, its allow-list entry, and with `nats` its NATS presence. `routable` (exit 0) needs a live
 * record on each side read and, when both are read, the same owner incarnation.
 */
export const sessionReceipt = async (
  config: NatsLinkConfig,
  session: string,
  options: { nats?: boolean; readRoots?: typeof readNatsRoots } = {},
): Promise<Record<string, unknown>> => {
  const match = SESSION.exec(session);
  if (!match) throw new UsageError("--session must be a session id (session:<uuid> or <uuid>)");
  const id = `session:${match[1]}`;
  const store = openReadOnly(config);
  let rows: ParticipantRow[];
  let handoffs: Array<{ handoff: unknown; inForce: boolean }>;
  try {
    rows = fileParticipants(store);
    handoffs = listHandoffs(store).filter((entry) => record(entry.handoff).id === id);
  } finally {
    store.closeState();
  }
  const found = rows.filter((row) => row.id === id);
  const file = found.length === 1 ? found[0]! : undefined;
  const entry = natsLinkAllowListConfig(config).entries.find((candidate) => candidate.kind === "root" && candidate.id === id);
  const side = entry?.side ?? null;
  const viaSpoke = entry ? pr2Fields(entry).viaSpoke : undefined;
  const problems: string[] = [];
  if (!side) problems.push("not on this link's allow-list");
  if (found.length > 1) problems.push("several file-mesh records answer to this id");
  if (!file) problems.push("no file-mesh record");
  else {
    if (file.stale) problems.push("file-mesh record is not live");
    if (side === "nats" && file.remoteHost !== config.link) problems.push(`file-mesh record is not this link's mirror (remoteHost ${file.remoteHost ?? "none"})`);
    if (side === "file" && viaSpoke === undefined && file.remoteHost !== undefined) {
      problems.push(`file-mesh record is a mirror (remoteHost ${file.remoteHost}), not native`);
    }
    // A spoke root is the hub's DIRECT mirror held by its spoke link: remoteHost is the spoke, no relay hop.
    if (viaSpoke !== undefined && (file.remoteHost !== viaSpoke || file.bridgeVia !== undefined)) {
      problems.push(`file-mesh record is not spoke ${viaSpoke}'s direct mirror (remoteHost ${file.remoteHost ?? "none"})`);
    }
  }
  let nats: Record<string, unknown> | null = null;
  if (options.nats) {
    try {
      const roots = await (options.readRoots ?? readNatsRoots)(natsLinkRootsSource(config), { includeStale: true });
      const matches: NatsRootInfo[] = roots.filter((root) => root.id === id);
      const root = matches.length === 1 ? matches[0]! : undefined;
      nats = { read: true, records: matches };
      if (matches.length > 1) problems.push("several NATS presence records answer to this id (ambiguous, unroutable)");
      if (!root) problems.push("no NATS presence record");
      else {
        if (!root.live) problems.push("NATS presence is not live");
        if (side === "file" && root.machine !== config.nats.machine) problems.push(`NATS record is not under the bridge machine (${root.machine})`);
        if (side === "nats" && root.machine === config.nats.machine) problems.push("NATS record is under the bridge machine: a mirror, not the native owner");
        if (file && file.ownerIncarnation !== root.ownerIncarnation) problems.push("owner incarnation differs between the file mesh and NATS");
      }
    } catch (error) {
      nats = { read: false, error: error instanceof Error ? error.message : String(error) };
      problems.push("NATS presence unreadable");
    }
  }
  return {
    ok: problems.length === 0,
    verdict: problems.length === 0 ? "routable" : "not-routable",
    id,
    link: config.link,
    side,
    ...(viaSpoke === undefined ? {} : { viaSpoke }),
    file: file ? participantSummary(file) : null,
    handoffs,
    nats,
    problems,
  };
};

// ---------------------------------------------------------------- main

const write = (out: (text: string) => void, value: unknown): void => out(`${JSON.stringify(value, null, 2)}\n`);

export interface NatsLinkCliIo {
  out?: (text: string) => void;
  err?: (text: string) => void;
  starter?: NatsLinkStarter;
  readRoots?: typeof readNatsRoots;
  env?: NodeJS.ProcessEnv;
  /** start's stop signal; default: SIGTERM/SIGINT (a second signal exits at once, as mesh-bridge run). */
  signal?: AbortSignal;
  /** withdraw's NATS opener, stop bound and signal sender (tests). */
  withdraw?: NatsLinkWithdrawOptions;
}

export const main = async (argv: string[], io: NatsLinkCliIo = {}): Promise<number> => {
  const out = io.out ?? ((text: string) => void process.stdout.write(text));
  const err = io.err ?? ((text: string) => void process.stderr.write(text));
  const log = (line: string): void => err(`nats-link: ${line}\n`);
  const [mode = "", ...rest] = argv;
  if (mode === "help" || mode === "--help" || mode === "-h") {
    out(`${USAGE}\n`);
    return 0;
  }
  if (!Object.hasOwn(VALUED, mode)) {
    err(`${USAGE}\n`);
    return 2;
  }
  try {
    // The link itself runs on Linux and macOS only; read-only modes stay usable anywhere.
    if (mode === "start" || mode === "dry-run") assertNatsLinkPlatform();
    const flags = parseFlags(mode, rest);
    if (mode === "roots") {
      const config = one(flags, "config");
      if (config && ["servers", "machine", "creds", "ca"].some((name) => flags.values.has(name))) {
        throw new UsageError("--config and connection flags are exclusive");
      }
      let source: NatsRootsSource;
      if (config) source = natsLinkRootsSource(loadNatsLinkConfig(config));
      else {
        try {
          source = natsRootsSourceFromEnv(io.env ?? process.env, {
            ...(one(flags, "servers") ? { servers: one(flags, "servers")!.split(",").map((server) => server.trim()) } : {}),
            ...(one(flags, "machine") ? { machine: one(flags, "machine")! } : {}),
            ...(one(flags, "creds") ? { credentials: one(flags, "creds")! } : {}),
            ...(one(flags, "ca") ? { caFile: one(flags, "ca")! } : {}),
          });
        } catch (error) {
          throw new UsageError(error instanceof Error ? error.message : String(error));
        }
      }
      for (const file of [source.credentials, source.caFile]) {
        if (file !== undefined && !path.isAbsolute(file)) throw new UsageError("--creds and --ca must be absolute paths");
      }
      const kinds = (flags.values.get("kind") ?? ["root"]).map((kind) => {
        if (!(KINDS as readonly string[]).includes(kind)) throw new UsageError(`--kind must be one of ${KINDS.join(", ")}`);
        return kind as FabricParticipantKind;
      });
      const timeout = one(flags, "timeout-ms");
      if (timeout !== undefined && (!/^[1-9][0-9]{0,5}$/.test(timeout) || Number(timeout) > 120_000)) {
        throw new UsageError("--timeout-ms must be an integer from 1 through 120000");
      }
      try {
        const roots = await (io.readRoots ?? readNatsRoots)(source, {
          includeStale: flags.booleans.has("include-stale"), kinds,
          ...(timeout === undefined ? {} : { timeoutMs: Number(timeout) }),
        });
        write(out, roots);
        return 0;
      } catch (error) {
        if (!(error instanceof NatsRootsUnavailableError)) throw error;
        log(`${error.code}: ${error.message}`);
        return 1;
      }
    }
    const config = loadNatsLinkConfig(required(flags, "config"));
    if (mode === "dry-run") {
      preflightNatsLinkConfig(config, { createStateDir: false });
      const report = await natsLinkDryRun(config, io.readRoots ? { readRoots: io.readRoots } : {});
      write(out, report);
      return report.refused === 0 ? 0 : 1;
    }
    if (mode === "withdraw") {
      const report = await withdrawNatsLink(config, io.withdraw ?? {});
      write(out, report);
      return report.ok ? 0 : 1;
    }
    if (mode === "start") {
      preflightNatsLinkConfig(config);
      // The hub relay (PR 2) accepts this process as a relay peer only if its config file has no group or other
      // bits (src/mesh/bridge-relay.ts): with spoke links configured, refuse now, by name, never at the first relay.
      if ((config.relay?.spokes.length ?? 0) > 0 && process.platform !== "win32" &&
        (fs.statSync(required(flags, "config")).mode & 0o077) !== 0) {
        throw new NatsLinkConfigError("--config", "must have no group or other permission bits (0600 or 0400) when relay.spokes is set: " +
          "the hub relay authenticates the link by it");
      }
      // Fail fast, before anything opens: an allow entry that contradicts org.json's seats refuses the start.
      identityGate(config);
      const starter = io.starter ?? await loadNatsLinkStarter();
      // One link per state directory: a second start (or a running withdraw) is refused by name.
      const release = acquireNatsLinkLock(config, "start");
      try {
        if (io.signal) return await runNatsLink(config, io.signal, starter, log);
        const controller = new AbortController();
        let signals = 0;
        const stop = (): void => {
          if (++signals === 1) controller.abort();
          else process.exit(0);
        };
        process.on("SIGTERM", stop);
        process.on("SIGINT", stop);
        try {
          return await runNatsLink(config, controller.signal, starter, log);
        } finally {
          // After a requested stop keep the two-signal contract armed until exit (as mesh-bridge run).
          if (!controller.signal.aborted) {
            process.off("SIGTERM", stop);
            process.off("SIGINT", stop);
          }
        }
      } finally {
        release();
      }
    }
    if (mode === "status") {
      const report = await natsLinkStatus(config);
      write(out, report);
      return report.ok === true ? 0 : 1;
    }
    // receipt
    const command = one(flags, "command");
    const session = one(flags, "session");
    if ((command === undefined) === (session === undefined)) throw new UsageError("give exactly one of --command or --session");
    if (command !== undefined && (command.length > 200 || /[\u0000-\u001f\u007f]/.test(command))) {
      throw new UsageError("--command must be a command id");
    }
    if (flags.values.has("after") && command === undefined) throw new UsageError("--after applies to --command only");
    if (flags.booleans.has("nats") && session === undefined) throw new UsageError("--nats applies to --session only");
    const after = one(flags, "after");
    if (after !== undefined && !/^(0|[1-9][0-9]{0,15})$/.test(after)) throw new UsageError("--after must be a sequence number");
    const report = command !== undefined
      ? commandReceipt(config, command, after === undefined ? 0 : Number(after))
      : await sessionReceipt(config, session!, { nats: flags.booleans.has("nats"), ...(io.readRoots ? { readRoots: io.readRoots } : {}) });
    write(out, report);
    return report.ok === true ? 0 : 1;
  } catch (error) {
    if (error instanceof UsageError) {
      err(`nats-link: ${error.message}\n${USAGE}\n`);
      return 2;
    }
    if (error instanceof NatsLinkConfigError || error instanceof BridgeNatsAllowListError || error instanceof BridgeNatsSeatPinsError ||
      error instanceof NatsLinkBusyError || error instanceof NatsLinkPlatformError) {
      log(`${error.code}: ${error.message}`);
      return 2;
    }
    if (error instanceof NatsRootsUnavailableError) {
      log(`${error.code}: ${error.message}`);
      return 1;
    }
    log(error instanceof Error ? error.message : String(error));
    return 1;
  }
};
