// src/mesh/bridge-nats-identity.ts: identity and allow-list gate of A', the NATS <-> file-mesh bridge
// (smarty-dev#15310, lane L3). Contract: src/mesh/bridge-nats-types.ts. Threat model:
// docs/bridge-nats-threat-model.md.
//
// What this module decides, and from what:
//  - Who a message is from: ONLY the presence of the side it came from (and, on NATS, the delivery's
//    credential-bound source machine). An event's `from`, body or text is a lookup key at most.
//  - Who may cross: ONLY the static allow-list, parsed once and frozen. There is no add/remove API.
//  - Which ids are principal seats: ONLY setup/org.json `principals[].orgInstance.fabricSession`, read
//    with smarty-send's strict rules; a seat entry must equal that pin exactly.
//
// Import-light: node builtins and type-only edges, so L5's status/receipt paths can load it.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { FabricPrincipal } from "../fabric-provenance.js";
import {
  BRIDGE_NATS_CLASS_RECEIVE,
  BRIDGE_NATS_DESTINATION_SIDE,
  BRIDGE_NATS_MACHINE,
  BRIDGE_NATS_OPERATIONS,
  BRIDGE_NATS_REFUSAL,
  BRIDGE_NATS_SESSION_ID,
  BRIDGE_NATS_SOURCE_SIDE,
  bridgeNatsDestinationClass,
  bridgeNatsLinkIdentity,
  type BridgeNatsAckCheck,
  type BridgeNatsAckSource,
  type BridgeNatsAllowDecision,
  type BridgeNatsAllowEntry,
  type BridgeNatsAllowEntryInput,
  type BridgeNatsAllowListConfig,
  type BridgeNatsAllowRequest,
  type BridgeNatsCommandRecord,
  type BridgeNatsIdentityGate,
  type BridgeNatsNativePublisher,
  type BridgeNatsOperation,
  type BridgeNatsPresenceRecord,
  type BridgeNatsPresenceView,
  type BridgeNatsRefusalCode,
  type BridgeNatsRepublishDecision,
  type BridgeNatsRepublishRequest,
  type BridgeNatsSide,
  type BridgeNatsVerified,
  type BridgeNatsVerifiedIdentity,
} from "./bridge-nats-types.js";

/** Default freshness of a presence observation and of an issued identity (MIRROR_FRESH_MS, the bridge lease). */
export const BRIDGE_NATS_IDENTITY_MAX_AGE_MS = 15_000;
/** Each allow list holds at most this many entries (L5's NATS_LINK_ALLOW_MAX). */
export const BRIDGE_NATS_ALLOW_MAX = 256;

// Full-match patterns (exec()[0] === value: JavaScript's `$` also matches before a final newline).
const BRIDGE_NAME = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/; // mesh/bridge.ts NAME_PATTERN
const OPERATOR_MACHINE = /^fabric-ops(-[a-z0-9]+)?$/;
const ACTOR_ID = /^[0-9a-f]{32}$/;
const ACTOR_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const PRINCIPAL_NAME = /^[A-Za-z][A-Za-z0-9 -]{0,63}$/;
const TOPIC_PATTERN = /^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*(?:\.\*)?$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/; // smarty-send UUID (lowercase)
const HERDR_NAME = /^[a-z][a-z0-9_-]{0,31}$/; // smarty-send HERDR_NAME
const PRINTABLE = /^[^\u0000-\u001f\u007f]*$/;

const full = (pattern: RegExp, value: unknown): value is string =>
  typeof value === "string" && pattern.exec(value)?.[0] === value;

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const deepFreeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
};

// ---------------------------------------------------------------------------------------------
// Allow-list parsing (pure)
// ---------------------------------------------------------------------------------------------

/** An allow-list the operator must fix. `field` names the JSON path; the message never repeats a value. */
export class BridgeNatsAllowListError extends Error {
  readonly code = "BRIDGE_NATS_ALLOW_LIST_INVALID";
  constructor(readonly field: string, requirement: string) {
    super(`${field} ${requirement}`);
    this.name = "BridgeNatsAllowListError";
  }
}

const strictKeys = (value: Record<string, unknown>, field: string, keys: readonly string[]): void => {
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !keys.includes(key)) {
      throw new BridgeNatsAllowListError(field, `has an unknown key${typeof key === "string" && /^[A-Za-z][A-Za-z0-9]{0,40}$/.test(key) ? ` ${key}` : ""}`);
    }
  }
};

const operationList = (value: unknown, field: string): BridgeNatsOperation[] => {
  if (value === undefined) return ["followUp"];
  if (!Array.isArray(value) || value.length > BRIDGE_NATS_OPERATIONS.length ||
    !value.every(operation => typeof operation === "string" && (BRIDGE_NATS_OPERATIONS as readonly string[]).includes(operation)) ||
    new Set(value).size !== value.length) {
    throw new BridgeNatsAllowListError(field, `must be a list of distinct operations from ${BRIDGE_NATS_OPERATIONS.join(", ")}`);
  }
  return [...value] as BridgeNatsOperation[];
};

const topicList = (value: unknown, field: string): string[] | undefined => {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length > 32 || new Set(value).size !== value.length ||
    !value.every(topic => full(TOPIC_PATTERN, topic) && topic.length <= 128)) {
    throw new BridgeNatsAllowListError(field, "must be at most 32 distinct topics, each exact or ending in .*");
  }
  return [...value] as string[];
};

const principalList = (value: unknown, field: string): string[] | undefined => {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length > 16 || new Set(value).size !== value.length ||
    !value.every(id => typeof id === "string" && id.trim() === id && id.length > 0 && id.length <= 256 && full(PRINTABLE, id))) {
    throw new BridgeNatsAllowListError(field, "must be at most 16 distinct principal ids");
  }
  return [...value] as string[];
};

const noteOf = (value: unknown, field: string): string | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.length > 200 || !full(PRINTABLE, value)) {
    throw new BridgeNatsAllowListError(field, "must be at most 200 printable characters");
  }
  return value;
};

const COMMON_KEYS = ["kind", "send", "receive", "receiveTopics", "principals", "machine", "note"] as const;

/** The receive list within the entry's destination class: default followUp when the class allows it. */
const receiveList = (value: Record<string, unknown>, field: string, side: BridgeNatsSide): BridgeNatsOperation[] => {
  const kind = value.kind === "root" || value.kind === "actor" ? value.kind : "actor";
  const destination = bridgeNatsDestinationClass({ kind, side, ...(typeof value.principal === "string" ? { principal: value.principal } : {}),
    ...(kind === "root" && typeof value.viaSpoke === "string" ? { viaSpoke: value.viaSpoke } : {}) });
  const cap = BRIDGE_NATS_CLASS_RECEIVE[destination];
  if (value.receive === undefined) return cap.includes("followUp") ? ["followUp"] : [];
  const receive = operationList(value.receive, `${field}.receive`);
  if (receive.some(operation => !cap.includes(operation))) {
    throw new BridgeNatsAllowListError(`${field}.receive`, cap.length === 0
      ? `must be empty: a ${destination === "refused" ? "NATS actor" : destination} receives nothing across`
      : `of a ${destination === "seat" ? "principal seat" : destination === "movedLead" ? "moved lead" : "file root or actor"} must be within ${cap.join(", ")}`);
  }
  return receive;
};

const parseEntry = (value: unknown, field: string, side: BridgeNatsSide, natsMachine: string, link: string): BridgeNatsAllowEntry => {
  if (!isPlainObject(value)) throw new BridgeNatsAllowListError(field, "must be a JSON object");
  const send = operationList(value.send, `${field}.send`);
  const receive = receiveList(value, field, side);
  const principals = principalList(value.principals, `${field}.principals`);
  const note = noteOf(value.note, `${field}.note`);
  let machine: string | undefined;
  // Every NATS identity is pinned to its machine (integrator note 8, threat T3): an unpinned seat or moved
  // lead could be registered by another machine while the real one is offline.
  if (side === "nats" && value.machine === undefined) {
    throw new BridgeNatsAllowListError(`${field}.machine`, "is required on every allow.nats entry");
  }
  if (value.machine !== undefined) {
    if (side !== "nats") throw new BridgeNatsAllowListError(`${field}.machine`, "is allowed only in allow.nats");
    if (!full(BRIDGE_NATS_MACHINE, value.machine) || full(OPERATOR_MACHINE, value.machine) || value.machine === natsMachine) {
      throw new BridgeNatsAllowListError(`${field}.machine`, "must be a Path B machine other than the bridge's own");
    }
    machine = value.machine;
  }
  const common = {
    side, send, receive,
    ...(principals !== undefined ? { principals } : {}),
    ...(machine !== undefined ? { machine } : {}),
    ...(note !== undefined ? { note } : {}),
  };
  if (value.kind === "root") {
    strictKeys(value, field, [...COMMON_KEYS, "id", "principal", "viaSpoke", "republishTopics"]);
    if (!full(BRIDGE_NATS_SESSION_ID, value.id)) {
      throw new BridgeNatsAllowListError(`${field}.id`, "must be a canonical root id, session:<lowercase uuid>");
    }
    if (value.principal !== undefined && !full(PRINCIPAL_NAME, value.principal)) {
      throw new BridgeNatsAllowListError(`${field}.principal`, "must be an org.json principal name");
    }
    if (value.viaSpoke !== undefined) {
      // A spoke root is a work-host lane reached through the hub's relay: never a principal seat, never NATS-side.
      if (side !== "file") throw new BridgeNatsAllowListError(`${field}.viaSpoke`, "is allowed only in allow.file");
      if (!full(BRIDGE_NAME, value.viaSpoke) || value.viaSpoke === link) {
        throw new BridgeNatsAllowListError(`${field}.viaSpoke`, "must be another bridge link's name");
      }
      if (value.principal !== undefined) throw new BridgeNatsAllowListError(`${field}.viaSpoke`, "is not allowed on a principal seat");
    }
    if (value.republishTopics !== undefined && side !== "nats") {
      throw new BridgeNatsAllowListError(`${field}.republishTopics`, "is allowed only in allow.nats");
    }
    const receiveTopics = topicList(value.receiveTopics, `${field}.receiveTopics`);
    const republishTopics = topicList(value.republishTopics, `${field}.republishTopics`);
    return {
      kind: "root", id: value.id, ...common,
      ...(value.principal !== undefined ? { principal: value.principal as string } : {}),
      ...(value.viaSpoke !== undefined ? { viaSpoke: value.viaSpoke as string } : {}),
      ...(receiveTopics !== undefined ? { receiveTopics } : {}),
      ...(republishTopics !== undefined ? { republishTopics } : {}),
    };
  }
  if (value.kind === "actor") {
    strictKeys(value, field, [...COMMON_KEYS, "rootId", "name", "id"]);
    if (!full(BRIDGE_NATS_SESSION_ID, value.rootId)) {
      throw new BridgeNatsAllowListError(`${field}.rootId`, "must be a canonical root id, session:<lowercase uuid>");
    }
    if (!full(ACTOR_NAME, value.name)) throw new BridgeNatsAllowListError(`${field}.name`, "must be an actor name");
    if (value.id !== undefined && !full(ACTOR_ID, value.id)) {
      throw new BridgeNatsAllowListError(`${field}.id`, "must be an actor id (32 lowercase hex)");
    }
    if (value.receiveTopics !== undefined) throw new BridgeNatsAllowListError(`${field}.receiveTopics`, "is allowed only on a root");
    return {
      kind: "actor", rootId: value.rootId, name: value.name, ...common,
      ...(value.id !== undefined ? { id: value.id as string } : {}),
    };
  }
  throw new BridgeNatsAllowListError(`${field}.kind`, "must be root or actor");
};

/** The identity an entry names, for duplicate checks across both lists. */
export const bridgeNatsAllowKey = (entry: BridgeNatsAllowEntryInput): string =>
  entry.kind === "root" ? entry.id : `actor:${entry.rootId}/${entry.name}`;

/**
 * Parse the link config's `allow` section into the frozen allow-list the gate uses. Pure (no I/O):
 * L5's parseNatsLinkConfig may call it. Unknown keys are refused at every level. The org.json seat
 * check needs a file read and runs at start (checkBridgeNatsSeatPins, or createBridgeNatsIdentityGate).
 */
export const parseBridgeNatsAllowSection = (
  raw: unknown, options: { readonly link: string; readonly natsMachine: string },
): BridgeNatsAllowListConfig => {
  if (!full(BRIDGE_NAME, options.link)) throw new BridgeNatsAllowListError("link", "must be a bridge name");
  if (!full(BRIDGE_NATS_MACHINE, options.natsMachine) || full(OPERATOR_MACHINE, options.natsMachine)) {
    throw new BridgeNatsAllowListError("nats.machine", "must be a Path B machine name, not an operator machine");
  }
  const section = raw === undefined ? {} : raw;
  if (!isPlainObject(section)) throw new BridgeNatsAllowListError("allow", "must be a JSON object");
  strictKeys(section, "allow", ["file", "nats"]);
  const entries: BridgeNatsAllowEntry[] = [];
  const seen = new Set<string>();
  for (const side of ["file", "nats"] as const) {
    const list = section[side];
    if (list === undefined) continue;
    if (!Array.isArray(list) || list.length > BRIDGE_NATS_ALLOW_MAX) {
      throw new BridgeNatsAllowListError(`allow.${side}`, `must be a list of at most ${BRIDGE_NATS_ALLOW_MAX} entries`);
    }
    list.forEach((value, index) => {
      const field = `allow.${side}[${index}]`;
      const entry = parseEntry(value, field, side, options.natsMachine, options.link);
      // An identity lives on one side: naming it twice (or on both sides) would mirror it both ways and loop.
      for (const key of [bridgeNatsAllowKey(entry), ...(entry.kind === "actor" && entry.id ? [`actor-id:${entry.id}`] : [])]) {
        if (seen.has(key)) throw new BridgeNatsAllowListError(field, "names an identity already listed");
        seen.add(key);
      }
      entries.push(entry);
    });
  }
  return deepFreeze({ version: 1 as const, link: options.link, natsMachine: options.natsMachine, entries });
};

// ---------------------------------------------------------------------------------------------
// org.json seat pins
// ---------------------------------------------------------------------------------------------

export interface BridgeNatsSeatPins {
  /** Principal name -> canonical org seat (`session:` + orgInstance.fabricSession). */
  readonly seats: ReadonlyMap<string, string>;
  /** Every bound session of an org instance (its fabricSession and keeper-pin rotations) -> principal name. */
  readonly bound: ReadonlyMap<string, string>;
}

/** org.json seat pins could not be read; the link fails closed. */
export class BridgeNatsSeatPinsError extends Error {
  readonly code = "BRIDGE_NATS_SEAT_PINS_INVALID";
  constructor(message: string) {
    super(message);
    this.name = "BridgeNatsSeatPinsError";
  }
}

/** Where the factory keeps org.json: the installed release first, then the org workspace (smarty-org-target ORG_JSONS). */
export const bridgeNatsOrgJsonCandidates = (home = os.homedir()): readonly string[] => [
  path.join(home, ".local/share/smarty-dev/factory/current/setup/org.json"),
  path.join(home, "smarty/smarty-pants/setup/org.json"),
];

/**
 * Read the seat pins with smarty-send's strict rules (org_bindings): every principal has a complete,
 * unique orgInstance (herdrAgent, herdrWorkspaceLabel, fabricSession as a lowercase uuid), and the
 * product owner is a bound principal. Keeper pins under a protected Herdr name add bound sessions.
 */
export const readBridgeNatsSeatPins = (doc: unknown): BridgeNatsSeatPins => {
  if (!isPlainObject(doc)) throw new BridgeNatsSeatPinsError("org.json is not one JSON object");
  const owner = isPlainObject(doc.productOwner) ? doc.productOwner.name : undefined;
  if (typeof owner !== "string" || !owner) throw new BridgeNatsSeatPinsError("org.json productOwner.name is not a non-empty string");
  const people = doc.principals;
  if (!Array.isArray(people) || people.length === 0 || !people.every(isPlainObject)) {
    throw new BridgeNatsSeatPinsError("org.json principals is not a list of principals");
  }
  const seats = new Map<string, string>(), bound = new Map<string, string>(), names = new Map<string, string>();
  const seen = new Set<string>();
  for (const person of people as Record<string, unknown>[]) {
    const who = person.name, instance = person.orgInstance;
    const sid = isPlainObject(instance) && typeof instance.fabricSession === "string"
      ? instance.fabricSession.replace(/^session:/, "") : "";
    if (typeof who !== "string" || !who || !isPlainObject(instance) || !full(HERDR_NAME, instance.herdrAgent) ||
        typeof instance.herdrWorkspaceLabel !== "string" || !instance.herdrWorkspaceLabel || !full(UUID, sid)) {
      throw new BridgeNatsSeatPinsError("org.json has an incomplete orgInstance binding");
    }
    for (const key of [`name\0${who}`, `herdrAgent\0${instance.herdrAgent}`, `label\0${instance.herdrWorkspaceLabel}`, `session\0${sid}`]) {
      if (seen.has(key)) throw new BridgeNatsSeatPinsError("org.json has two principals sharing one binding");
      seen.add(key);
    }
    const seat = `session:${sid}`;
    seats.set(who, seat);
    bound.set(seat, who);
    names.set(instance.herdrAgent as string, who);
  }
  if (!seats.has(owner)) throw new BridgeNatsSeatPinsError("org.json product owner is not a bound principal");
  const keeper = isPlainObject(doc.keeper) ? doc.keeper.pins : undefined;
  for (const pin of Array.isArray(keeper) ? keeper : []) {
    if (!isPlainObject(pin) || typeof pin.session !== "string" || typeof pin.name !== "string") continue;
    const sid = pin.session.replace(/^session:/, "");
    const who = names.get(pin.name);
    if (who && full(UUID, sid) && !bound.has(`session:${sid}`)) bound.set(`session:${sid}`, who);
  }
  return Object.freeze({ seats, bound });
};

/** Refuse an org.json another local user could have written: it decides which ids are principal seats. */
const unsafeWriter = (stat: fs.Stats): string | undefined => {
  const uid = process.geteuid?.();
  if (uid === undefined) return undefined;
  if (stat.uid !== uid && stat.uid !== 0) return "is owned by another user";
  if ((stat.mode & 0o022) !== 0) return "is group- or other-writable";
  return undefined;
};

/** Load seat pins from `file`, or the first existing candidate. A missing, unsafe or malformed file fails closed. */
export const loadBridgeNatsSeatPins = (file?: string): BridgeNatsSeatPins & { readonly source: string } => {
  const candidates = file !== undefined ? [file] : bridgeNatsOrgJsonCandidates();
  for (const candidate of candidates) {
    const stat = fs.statSync(candidate, { throwIfNoEntry: false });
    if (!stat) continue;
    if (!stat.isFile()) throw new BridgeNatsSeatPinsError(`org.json ${candidate} is not a regular file`);
    const unsafe = unsafeWriter(stat);
    if (unsafe) throw new BridgeNatsSeatPinsError(`org.json ${candidate} ${unsafe}`);
    let doc: unknown;
    try { doc = JSON.parse(fs.readFileSync(candidate, "utf8")); } catch { throw new BridgeNatsSeatPinsError(`org.json ${candidate} is not JSON`); }
    return Object.freeze({ ...readBridgeNatsSeatPins(doc), source: candidate });
  }
  throw new BridgeNatsSeatPinsError(`org.json not found (${candidates.join(", ")})`);
};

/**
 * Every seat entry equals its principal's org.json fabricSession exactly, and no entry uses a
 * principal's bound session without declaring that principal (and so the seat rules).
 */
export const checkBridgeNatsSeatPins = (config: BridgeNatsAllowListConfig, pins: BridgeNatsSeatPins): void => {
  config.entries.forEach(entry => {
    const field = `allow.${entry.side}[${config.entries.filter(other => other.side === entry.side).indexOf(entry)}]`;
    if (entry.kind !== "root") return;
    if (entry.principal !== undefined) {
      const seat = pins.seats.get(entry.principal);
      if (seat === undefined) throw new BridgeNatsAllowListError(`${field}.principal`, "is not a principal in org.json");
      if (seat !== entry.id) throw new BridgeNatsAllowListError(`${field}.id`, "must equal that principal's org.json orgInstance.fabricSession");
    } else if (pins.bound.has(entry.id)) {
      throw new BridgeNatsAllowListError(`${field}.principal`, "is required: the id is a principal's bound org session");
    }
  });
};

// ---------------------------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------------------------

export interface BridgeNatsIdentityOptions {
  readonly pins: BridgeNatsSeatPins;
  readonly clock?: () => number;
  /** Presence observations and issued identities older than this are refused (default 15 s). */
  readonly maxAgeMs?: number;
}

const refuse = (code: BridgeNatsRefusalCode, reason: string): { ok: false; refuse: BridgeNatsRefusalCode; reason: string } =>
  ({ ok: false, refuse: code, reason });

const topicMatches = (pattern: string, topic: string): boolean =>
  pattern.endsWith(".*") ? topic.length > pattern.length - 1 && topic.startsWith(pattern.slice(0, -1)) : pattern === topic;

const isNativePublisher = (value: unknown): value is BridgeNatsNativePublisher =>
  isPlainObject(value) && value.native === true && value.side === "file" && typeof value.fromId === "string";

const copyPrincipal = (value: unknown): FabricPrincipal | undefined => {
  if (typeof value !== "object" || value === null) return undefined;
  const principal = value as Partial<FabricPrincipal>;
  return typeof principal.id === "string" && principal.id.trim() && principal.id.length <= 256 &&
    (principal.binding === "herdr-client" || principal.binding === "voice-call" || principal.binding === "org-agent")
    ? { id: principal.id, binding: principal.binding } : undefined;
};

/** The allow-list gate. Construct it with createBridgeNatsIdentityGate, which checks the org.json seat pins first. */
export class BridgeNatsIdentity implements BridgeNatsIdentityGate {
  readonly config: BridgeNatsAllowListConfig;
  readonly #clock: () => number;
  readonly #maxAgeMs: number;
  readonly #roots = new Map<string, BridgeNatsAllowEntry>();
  readonly #actors = new Map<string, BridgeNatsAllowEntry>();
  /** Identities this gate issued; allow() refuses any other object, however it is shaped. */
  readonly #issued = new WeakSet<object>();

  constructor(config: BridgeNatsAllowListConfig, options: BridgeNatsIdentityOptions) {
    if (!Object.isFrozen(config) || !Object.isFrozen(config.entries)) {
      throw new TypeError("BridgeNatsIdentity needs the frozen config from parseBridgeNatsAllowSection");
    }
    checkBridgeNatsSeatPins(config, options.pins);
    this.config = config;
    this.#clock = options.clock ?? Date.now;
    const maxAgeMs = options.maxAgeMs ?? BRIDGE_NATS_IDENTITY_MAX_AGE_MS;
    if (!Number.isFinite(maxAgeMs) || maxAgeMs <= 0) throw new TypeError("maxAgeMs must be a positive number");
    this.#maxAgeMs = maxAgeMs;
    for (const entry of config.entries) {
      if (entry.kind === "root") this.#roots.set(entry.id, entry);
      else this.#actors.set(`${entry.rootId}\0${entry.name}`, entry);
    }
  }

  verifySender(side: BridgeNatsSide, claimedId: string, presence: BridgeNatsPresenceView,
    other: BridgeNatsPresenceView, sourceMachine?: string): BridgeNatsVerified {
    if (side === "nats") {
      if (!full(BRIDGE_NATS_MACHINE, sourceMachine)) {
        return refuse(BRIDGE_NATS_REFUSAL.senderUnverified, "a NATS sender needs the delivery's credential-bound source machine");
      }
      if (sourceMachine === this.config.natsMachine) {
        return refuse(BRIDGE_NATS_REFUSAL.sideMismatch, "a delivery from the bridge's own machine is never a NATS sender");
      }
    }
    return this.#verify(side, claimedId, presence, other, "sender", sourceMachine);
  }

  verifyTarget(side: BridgeNatsSide, targetId: string, presence: BridgeNatsPresenceView,
    other: BridgeNatsPresenceView): BridgeNatsVerified {
    return this.#verify(side, targetId, presence, other, "target");
  }

  verifyEventSender(claimedId: string, presence: BridgeNatsPresenceView, other: BridgeNatsPresenceView): BridgeNatsVerified {
    // No source machine exists for a FABRIC_EV event: the id must still resolve to ONE live, fresh KV root
    // on its pinned machine (never the bridge's), allow-listed on NATS. That is all this can prove.
    return this.#verify("nats", claimedId, presence, other, "event");
  }

  #native(side: BridgeNatsSide, record: BridgeNatsPresenceRecord): boolean {
    return side === "nats" ? record.machine !== this.config.natsMachine : record.mirroredBy === undefined;
  }

  #verify(side: BridgeNatsSide, id: string, view: BridgeNatsPresenceView, other: BridgeNatsPresenceView,
    role: "sender" | "target" | "event", sourceMachine?: string): BridgeNatsVerified {
    if (view.side !== side || other.side === side) throw new TypeError("presence views do not match the sides");
    const missing = role === "target" ? BRIDGE_NATS_REFUSAL.targetUnavailable : BRIDGE_NATS_REFUSAL.senderUnverified;
    if (typeof id !== "string" || id.length === 0 || id.length > 256) return refuse(BRIDGE_NATS_REFUSAL.notAllowed, "malformed id");
    const records = [...view.records(id)].filter(record => record.id === id);
    if (side === "nats" && records.some(record => !full(BRIDGE_NATS_MACHINE, record.machine))) {
      return refuse(missing, `${id}: a NATS presence record without a valid machine`);
    }
    // PR 2 relay: a spoke root's home is the hub's DIRECT mirror held by its spoke link, never a native record.
    const listed = side === "file" ? this.#roots.get(id) : undefined;
    const spoke = listed?.kind === "root" ? listed.viaSpoke : undefined;
    const home = (record: BridgeNatsPresenceRecord): boolean => spoke !== undefined
      ? record.mirroredBy === spoke && record.bridgeVia === undefined : this.#native(side, record);
    const native = records.filter(home);
    const mirrored = records.filter(record => !home(record));
    const ours = side === "nats" ? mirrored : mirrored.filter(record => record.mirroredBy === this.config.link);
    if (native.length === 0) {
      return ours.length > 0
        ? refuse(BRIDGE_NATS_REFUSAL.sideMismatch, `${id} is a ${side === "nats" ? "file" : "NATS"} identity this link mirrors`)
        : refuse(missing, `${id} has no ${side} presence`);
    }
    // The NATS directory treats one id on two machines as ambiguous, live or not; so does this gate.
    if (mirrored.length > 0 || new Set(native.map(record => side === "nats" ? record.machine : "file")).size > 1 ||
        (side === "file" && native.length > 1)) {
      return refuse(BRIDGE_NATS_REFUSAL.ambiguous, `${id} answers on more than one ${side === "nats" ? "machine" : "record"}`);
    }
    if ([...other.records(id)].some(record => record.id === id && this.#native(other.side, record))) {
      return refuse(BRIDGE_NATS_REFUSAL.ambiguous, `${id} answers natively on both sides`);
    }
    const record = native[0]!;
    if (!record.live) return refuse(missing, `${id} is not live on ${side}`);
    const now = this.#clock();
    if (!this.#fresh(record.observedAt, now)) return refuse(missing, `${id}: the ${side} presence observation is not fresh`);
    let entry: BridgeNatsAllowEntry | undefined;
    if (record.kind === "main") {
      if (record.rootId !== record.id || !full(BRIDGE_NATS_SESSION_ID, record.id)) {
        return refuse(BRIDGE_NATS_REFUSAL.notAllowed, `${id} is not a canonical root`);
      }
      entry = this.#roots.get(record.id);
    } else if (record.kind === "actor") {
      // An actor's id is always an actor id (32 lowercase hex), pinned or not: a record that resolves to a
      // session id (a root's, a principal seat's, a keeper rotation's) can never speak as an actor entry.
      if (!full(ACTOR_ID, record.id) || this.#roots.has(record.id) || !full(BRIDGE_NATS_SESSION_ID, record.rootId)) {
        return refuse(BRIDGE_NATS_REFUSAL.notAllowed, `${id} is not an actor id`);
      }
      entry = this.#actors.get(`${record.rootId}\0${record.name}`);
      if (entry?.kind === "actor" && entry.id !== undefined && entry.id !== record.id) entry = undefined;
    }
    if (!entry) return refuse(BRIDGE_NATS_REFUSAL.notAllowed, `${id} is not on the allow-list`);
    if (entry.side !== side) return refuse(BRIDGE_NATS_REFUSAL.sideMismatch, `${id} is allow-listed on ${entry.side}, not ${side}`);
    if (side === "nats") {
      if (entry.machine !== undefined && record.machine !== entry.machine) {
        return refuse(missing, `${id} is not live on its pinned machine`);
      }
      if (role === "sender" && record.machine !== sourceMachine) {
        return refuse(BRIDGE_NATS_REFUSAL.senderUnverified, `${id} is not live on the delivery's source machine`);
      }
    }
    if (record.kind === "actor") {
      // An actor runs in its owner root's runtime: the root must be live with it, natively, on the same machine.
      const roots = [...view.records(record.rootId)].filter(root => root.id === record.rootId && this.#native(side, root));
      const root = roots[0];
      if (roots.length !== 1 || !root!.live || root!.kind !== "main" || root!.rootId !== root!.id ||
          (side === "nats" && root!.machine !== record.machine) || !this.#fresh(root!.observedAt, now)) {
        return refuse(missing, `${id}: its owner root is not live with it on ${side}, freshly observed`);
      }
    }
    const identity: BridgeNatsVerifiedIdentity = Object.freeze({
      side, id: record.id, name: record.name, kind: record.kind,
      ...(record.sessionId !== undefined ? { sessionId: record.sessionId } : {}),
      ownerHostId: record.ownerHostId, ownerIdentityId: record.ownerIdentityId,
      ...(record.ownerIncarnation !== undefined ? { ownerIncarnation: record.ownerIncarnation } : {}),
      ...(side === "nats" ? { machine: record.machine! } : {}),
      entry, verifiedAt: now,
      ...(role === "event" ? { presenceOnly: true as const } : {}),
      ...(spoke !== undefined ? { viaSpoke: spoke } : {}),
    });
    this.#issued.add(identity);
    return { ok: true, identity };
  }

  #fresh(observedAt: number, now: number): boolean {
    return Number.isFinite(observedAt) && now - observedAt <= this.#maxAgeMs && observedAt - now <= this.#maxAgeMs;
  }

  #current(identity: unknown): identity is BridgeNatsVerifiedIdentity {
    if (typeof identity !== "object" || identity === null || !this.#issued.has(identity)) return false;
    const age = this.#clock() - (identity as BridgeNatsVerifiedIdentity).verifiedAt;
    return age <= this.#maxAgeMs && age >= -this.#maxAgeMs;
  }

  allow(request: BridgeNatsAllowRequest): BridgeNatsAllowDecision {
    const { direction, sender, target, subject } = request;
    if (direction !== "toNats" && direction !== "toFile") return refuse(BRIDGE_NATS_REFUSAL.notAllowed, "unknown direction");
    if (!this.#current(target)) return refuse(BRIDGE_NATS_REFUSAL.targetUnavailable, "the target was not verified by this gate just now");
    if (target.side !== BRIDGE_NATS_DESTINATION_SIDE[direction]) {
      return refuse(BRIDGE_NATS_REFUSAL.sideMismatch, `${target.id} is not on the ${direction} destination side`);
    }
    const native = isNativePublisher(sender);
    if (!native && !this.#current(sender)) return refuse(BRIDGE_NATS_REFUSAL.senderUnverified, "the sender was not verified by this gate just now");
    // A presence-only identity (an event's claimed sender) is attribution, never control or topic authority.
    if (!native && (sender as BridgeNatsVerifiedIdentity).presenceOnly) {
      return refuse(BRIDGE_NATS_REFUSAL.senderUnverified, "an event's claimed sender is not bound to a credential");
    }
    if (target.presenceOnly) return refuse(BRIDGE_NATS_REFUSAL.targetUnavailable, "an event sender is never a target");
    if (sender.side !== BRIDGE_NATS_SOURCE_SIDE[direction]) {
      return refuse(BRIDGE_NATS_REFUSAL.sideMismatch, `the sender is not on the ${direction} source side`);
    }
    if (subject.type === "control") {
      if (native) return refuse(BRIDGE_NATS_REFUSAL.notAllowed, "a non-participant publisher never sends a control command across");
      const from = sender as BridgeNatsVerifiedIdentity;
      if (!BRIDGE_NATS_OPERATIONS.includes(subject.operation)) return refuse(BRIDGE_NATS_REFUSAL.operationNotAllowed, "unknown operation");
      // The destination class cap comes first and from the entry alone (re-checked here, not only at parse).
      const destination = bridgeNatsDestinationClass(target.entry);
      if (!BRIDGE_NATS_CLASS_RECEIVE[destination].includes(subject.operation)) {
        return refuse(BRIDGE_NATS_REFUSAL.destinationClassRefused, `a ${destination} destination never receives ${subject.operation} from across`);
      }
      if (!from.entry.send.includes(subject.operation)) {
        return refuse(BRIDGE_NATS_REFUSAL.operationNotAllowed, `${from.id} may not send ${subject.operation} across`);
      }
      if (!target.entry.receive.includes(subject.operation)) {
        return refuse(BRIDGE_NATS_REFUSAL.operationNotAllowed, `${target.id} may not receive ${subject.operation} from across`);
      }
      if (request.principal === undefined || request.principal === null) return { ok: true, sender, target, principal: "none" };
      const principal = copyPrincipal(request.principal);
      return { ok: true, sender, target,
        principal: principal && from.entry.principals?.includes(principal.id) ? "carry" : "strip" };
    }
    if (subject.type === "topic") {
      // PR 1 carries topic routes file -> NATS only (N3); NATS -> factory topics are PR 2 (N4).
      if (direction !== "toNats") return refuse(BRIDGE_NATS_REFUSAL.topicNotAllowed, "topic routes cross file -> NATS only");
      // A topic arrives as a followUp (N3), so the target's class must allow followUp.
      if (!BRIDGE_NATS_CLASS_RECEIVE[bridgeNatsDestinationClass(target.entry)].includes("followUp")) {
        return refuse(BRIDGE_NATS_REFUSAL.destinationClassRefused, "this destination class never receives a followUp from across");
      }
      if (typeof subject.topic !== "string" || typeof subject.kind !== "string" || target.entry.kind !== "root" ||
          !(target.entry.receiveTopics ?? []).some(pattern => topicMatches(pattern, subject.topic))) {
        return refuse(BRIDGE_NATS_REFUSAL.topicNotAllowed, `${target.id} may not receive ${String(subject.topic)} from across`);
      }
      return { ok: true, sender, target, principal: request.principal ? "strip" : "none" };
    }
    return refuse(BRIDGE_NATS_REFUSAL.notAllowed, "unknown subject");
  }

  verifyAck(claim: BridgeNatsCommandRecord, ack: BridgeNatsAckSource): BridgeNatsAckCheck {
    const unbound = (reason: string): BridgeNatsAckCheck => refuse(BRIDGE_NATS_REFUSAL.ackUnbound, reason);
    if (ack.side !== BRIDGE_NATS_DESTINATION_SIDE[claim.direction]) return unbound("the ACK came from the command's source side");
    if (ack.commandId !== claim.commandId) return unbound("the ACK names another command");
    if (ack.fromId !== claim.ownerIdentityId) return unbound("the ACK is not from the command's owner");
    if (ack.targetId !== claim.targetId) return unbound("the ACK names another target");
    if (typeof ack.eventId !== "string" || ack.eventId.length === 0) return unbound("the ACK has no event id");
    if (ack.side === "nats") {
      if (claim.targetMachine === undefined || ack.sourceMachine !== claim.targetMachine) return unbound("the ACK is not from the owner's machine");
      if (ack.bodyDigest !== claim.bodyDigest) return unbound("the ACK answers a different body");
      if (claim.envelopeId === undefined || ack.eventId !== `ack:${claim.envelopeId}`) return unbound("the ACK answers another envelope");
    }
    return { ok: true };
  }

  allowRepublish(request: BridgeNatsRepublishRequest): BridgeNatsRepublishDecision {
    const { sender, topic, kind } = request;
    if (!this.#current(sender) || sender.side !== "nats" || sender.presenceOnly !== true) {
      return refuse(BRIDGE_NATS_REFUSAL.senderUnverified, "the event sender was not verified by verifyEventSender just now");
    }
    if (sender.entry.kind !== "root" || typeof topic !== "string" || typeof kind !== "string" ||
        !(sender.entry.republishTopics ?? []).some(pattern => topicMatches(pattern, topic))) {
      return refuse(BRIDGE_NATS_REFUSAL.topicNotAllowed, `${sender.id} may not have ${String(topic)} republished into the file mesh`);
    }
    // Authored as the link, never as the claimed sender: nothing bound the sender to the publisher.
    return { ok: true, author: bridgeNatsLinkIdentity(this.config.link), sender };
  }
}

/** Build the gate. Throws when an allow entry contradicts the org.json seat pins. */
export const createBridgeNatsIdentityGate = (config: BridgeNatsAllowListConfig, options: BridgeNatsIdentityOptions): BridgeNatsIdentity =>
  new BridgeNatsIdentity(config, options);

// ---------------------------------------------------------------------------------------------
// Dry run (read-only): what each allow entry resolves to right now
// ---------------------------------------------------------------------------------------------

export interface BridgeNatsDryRunRow {
  readonly side: BridgeNatsSide;
  readonly key: string;
  readonly kind: "root" | "actor";
  readonly principal?: string;
  readonly note?: string;
  /** "live": verified now and would be mirrored to the other side; otherwise the refusal code. */
  readonly status: "live" | "unresolved" | BridgeNatsRefusalCode;
  readonly reason?: string;
  readonly id?: string;
  readonly machine?: string;
  readonly mirrorTo: BridgeNatsSide;
  readonly send: readonly BridgeNatsOperation[];
  readonly receive: readonly BridgeNatsOperation[];
}

export interface BridgeNatsDryRunReport {
  readonly v: 1;
  readonly link: string;
  readonly natsMachine: string;
  readonly at: string;
  readonly rows: readonly BridgeNatsDryRunRow[];
  readonly live: number;
  readonly refused: number;
}

/**
 * For every allow entry, verify it on its home side as a target would be, and report what the link
 * would mirror. Reads presence views only; writes nothing. Actor entries without an id pin need the
 * view's optional list() to be found by root + name.
 */
export const bridgeNatsDryRun = (gate: BridgeNatsIdentity,
  views: { readonly file: BridgeNatsPresenceView; readonly nats: BridgeNatsPresenceView }, now = Date.now()): BridgeNatsDryRunReport => {
  const rows = gate.config.entries.map((entry): BridgeNatsDryRunRow => {
    const home = views[entry.side], other = views[entry.side === "file" ? "nats" : "file"];
    const base = {
      side: entry.side, key: bridgeNatsAllowKey(entry), kind: entry.kind,
      ...(entry.kind === "root" && entry.principal !== undefined ? { principal: entry.principal } : {}),
      ...(entry.note !== undefined ? { note: entry.note } : {}),
      mirrorTo: entry.side === "file" ? "nats" as const : "file" as const, send: entry.send, receive: entry.receive,
    };
    let id: string | undefined = entry.id;
    if (id === undefined && entry.kind === "actor") {
      const found = (home.list?.() ?? []).filter(record => record.kind === "actor" && record.rootId === entry.rootId &&
        record.name === entry.name && (entry.side === "nats" ? record.machine !== gate.config.natsMachine : record.mirroredBy === undefined));
      if (found.length !== 1) {
        return { ...base, status: found.length === 0 ? "unresolved" : BRIDGE_NATS_REFUSAL.ambiguous,
          reason: found.length === 0 ? "no actor with that root and name in the listing" : "more than one actor with that root and name" };
      }
      id = found[0]!.id;
    }
    const verified = gate.verifyTarget(entry.side, id!, home, other);
    return verified.ok
      ? { ...base, status: "live", id: verified.identity.id, ...(verified.identity.machine ? { machine: verified.identity.machine } : {}) }
      : { ...base, status: verified.refuse, reason: verified.reason, id: id! };
  });
  return Object.freeze({
    v: 1 as const, link: gate.config.link, natsMachine: gate.config.natsMachine, at: new Date(now).toISOString(),
    rows, live: rows.filter(row => row.status === "live").length, refused: rows.filter(row => row.status !== "live").length,
  });
};
