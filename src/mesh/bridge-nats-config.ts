/**
 * The NATS link's config file (smarty-dev#15310, A' lane L5): one JSON object that names everything a
 * file-mesh <-> NATS link needs. Names and paths only: never a credential's bytes, a JWT, a seed or a CA's
 * private part. L5 owns this schema; its `allow` section is L3's `BridgeNatsAllowSection` (bridge-nats-types.ts),
 * checked here with L3's own `parseBridgeNatsAllowSection`; `natsLinkAllowListConfig` gives the frozen allow-list
 * L3's identity gate takes, and `startNatsLink` parses the same section itself. `startNatsLink`
 * (bridge-nats-link.ts) takes the parsed `NatsLinkConfig`; `nats-link start|status|receipt` load it with
 * `loadNatsLinkConfig`.
 *
 * The allow-list is static: it is read once at start and never widened at run time. It only NAMES the
 * identities that may cross. It never proves one: a sender is still mapped only from the verified presence
 * of the side it came from (L1/L2/L3), never from a message body or from this file. The rules that need
 * org.json (a seat's id equals that principal's orgInstance.fabricSession) run at start, from `orgJson`.
 *
 * Import-light on purpose (node builtins and leaf modules): the CLI's status and receipt paths load it
 * without the bridge, the NATS client or the config graph.
 */
import fs from "node:fs";
import { isIP } from "node:net";
import path from "node:path";
import type { FabricMeshNatsConfig } from "../config.js";
import { DEFAULT_INBOX_MAX_AGE_MS } from "../transport/nats/inbox-policy.js";
import { BridgeNatsAllowListError, bridgeNatsAllowKey, parseBridgeNatsAllowSection } from "./bridge-nats-identity.js";
import type { BridgeNatsAllowListConfig, BridgeNatsAllowSection } from "./bridge-nats-types.js";

export const NATS_LINK_CONFIG_VERSION = 1;
/** A config file larger than this is refused unread. */
export const NATS_LINK_CONFIG_MAX_BYTES = 1024 * 1024;

export interface NatsLinkConfig {
  readonly version: 1;
  /** This hub's bridge name on the file side (MeshBridge localName), e.g. `ryzen1`. */
  readonly name: string;
  /** The link name (MeshBridge remoteName): the `remoteHost` of every NATS record mirrored into the file mesh, e.g. `v2`. */
  readonly link: string;
  readonly file: {
    /** Absolute path of the file mesh root (a directory holding state.json or state.db). */
    readonly mesh: string;
    readonly lockProtocol: 1 | 2 | 3;
    /** Explicit keyed-state backend; absent = what the root's moved marker or the environment selects, as mesh-bridge. */
    readonly stateBackend?: "file" | "shadow" | "sqlite";
    readonly maxStateBytes?: number;
  };
  /** Absolute, private (0700) directory for the link's cursor, ledger and stall reason. */
  readonly stateDir: string;
  readonly nats: {
    /** `tls://<DNS name>:<port>` URLs. */
    readonly servers: readonly string[];
    /** The bridge's own Path B machine: every file record mirrored into NATS KV is written under it. */
    readonly machine: string;
    /** Absolute path of the bridge machine's user.creds file (0600, ours). Never its contents. */
    readonly credentials: string;
    /** Absolute path of the broker's CA certificate. */
    readonly caFile: string;
    /** Org slug for event subjects; required when `events` is true. */
    readonly org?: string;
    readonly custodyStream: string;
    readonly livenessStream: string;
    /** The org's Path B machine inventory; must contain `machine`. */
    readonly pathBMachines: readonly string[];
    readonly operatorMachine: string;
    /** Consume and publish the org's event stream (PR 2: v2 -> factory topics). Default false. */
    readonly events: boolean;
    readonly inboxLanes: number;
  };
  /**
   * Who may cross: the config's `allow: {file, nats}` as written (L3's BridgeNatsAllowSection), frozen, after
   * L3's parseBridgeNatsAllowSection accepted it. `file` entries are native on the file mesh and mirrored into
   * NATS KV under nats.machine; `nats` entries are native on the broker and mirrored into the file mesh with
   * remoteHost = link. `natsLinkAllowListConfig` gives the normalized list for L3's gate.
   */
  readonly allow: BridgeNatsAllowSection;
  /**
   * PR 2 relay: the hub's spoke links (the `--remote` names of its `mesh-bridge run` links to work hosts). An
   * `allow.file` root with `viaSpoke` must name one; with none, no spoke root is accepted. The parser always
   * sets it (default empty); a hand-built config may leave it out.
   */
  readonly relay?: { readonly spokes: readonly string[] };
  /** Absolute path of the org.json whose principal seats pin the allow-list; absent = bridgeNatsOrgJsonCandidates(). */
  readonly orgJson?: string;
  /** Deadline of each call to the NATS side, ms (default 30,000). */
  readonly callTimeoutMs: number;
  readonly logLevel: "info" | "debug";
}

export const BRIDGE_NATS_UNSUPPORTED_PLATFORM = "BRIDGE_NATS_UNSUPPORTED_PLATFORM";

/**
 * The link runs on the file-mesh hub: Linux and macOS only. Its ledger, lock and config checks rely on POSIX
 * file modes, fsync/rename and lock semantics, so `nats-link start`, `nats-link dry-run` and startNatsLink
 * refuse on Windows by name rather than run on semantics they were never built for.
 */
export class NatsLinkPlatformError extends Error {
  readonly code = BRIDGE_NATS_UNSUPPORTED_PLATFORM;
  constructor() {
    super("nats-link runs only on Linux and macOS");
    this.name = "NatsLinkPlatformError";
  }
}

/** Refuse a platform the link does not support (read at call time, so tests can set it). */
export const assertNatsLinkPlatform = (platform: NodeJS.Platform = process.platform): void => {
  if (platform === "win32") throw new NatsLinkPlatformError();
};

/** A config the operator must fix. `field` names the JSON path; the message never repeats a value from the file. */
export class NatsLinkConfigError extends Error {
  readonly code = "FABRIC_NATS_LINK_CONFIG";
  constructor(readonly field: string, requirement: string) {
    super(`${field} ${requirement}`);
    this.name = "NatsLinkConfigError";
  }
}

// ponytail: mirrors of validBridgeName (mesh/bridge.ts) and the mesh.nats validators in config.ts, kept here
// so status/receipt never load the bridge or config graphs. tests/bridge-nats-config.test.ts pins them to the
// originals. Full-match comparisons: JavaScript's `$` also matches before a final newline.
const BRIDGE_NAME = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;
const MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;
const OPERATOR_MACHINE = /^fabric-ops(-[a-z0-9]+)?$/;
const ORG = /^[a-z0-9][a-z0-9-]{0,62}$/;
const STREAM = /^[A-Za-z0-9_-]{1,255}$/;
const SERVER = /^tls:\/\/([A-Za-z0-9.-]+):([0-9]{1,5})$/;
const DNS_LABEL = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/;

const full = (pattern: RegExp, value: unknown): value is string => typeof value === "string" && pattern.exec(value)?.[0] === value;
const isObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const object = (value: unknown, field: string, keys: readonly string[], required: readonly string[] = keys): Record<string, unknown> => {
  if (!isObject(value)) throw new NatsLinkConfigError(field, "must be a JSON object");
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !keys.includes(key)) {
      throw new NatsLinkConfigError(field, `has an unknown key${typeof key === "string" && /^[A-Za-z][A-Za-z0-9]{0,40}$/.test(key) ? ` ${key}` : ""}`);
    }
  }
  for (const key of required) if (!Object.hasOwn(value, key)) throw new NatsLinkConfigError(`${field}.${key}`, "is required");
  return value;
};

const absolutePath = (value: unknown, field: string): string => {
  if (typeof value !== "string" || !path.isAbsolute(value) || value.includes("\0") || path.normalize(value) !== value ||
    (value.length > 1 && value.endsWith(path.sep))) {
    throw new NatsLinkConfigError(field, "must be a normalized absolute path");
  }
  return value;
};

const integer = (value: unknown, field: string, fallback: number, min: number, max: number): number => {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new NatsLinkConfigError(field, `must be an integer from ${min} through ${max}`);
  }
  return value;
};

const server = (value: unknown): boolean => {
  if (typeof value !== "string") return false;
  const match = SERVER.exec(value);
  if (!match || match[0] !== value) return false;
  const host = match[1]!;
  // A DNS name only, as mesh.nats.servers. A tls: URL is not WHATWG-special, so mixed-hex IPv4 would survive
  // its parser: normalize with the HTTP hostname rules first, as config.ts does.
  if (host.length > 253) return false;
  try { if (isIP(new URL(`http://${host}`).hostname) !== 0) return false; }
  catch { return false; }
  const labels = (host.endsWith(".") ? host.slice(0, -1) : host).split(".");
  const port = Number(match[2]);
  return labels.every((label) => full(DNS_LABEL, label)) && port >= 1 && port <= 65_535;
};


/**
 * Parse and check one config object. Every rule that needs no filesystem is here; `preflightNatsLinkConfig`
 * checks the files before `start`. Unknown keys are refused at every level, so a misspelt field never
 * silently falls back to a default.
 */
export const parseNatsLinkConfig = (value: unknown): NatsLinkConfig => {
  const root = object(value, "config",
    ["version", "name", "link", "file", "stateDir", "nats", "allow", "relay", "orgJson", "callTimeoutMs", "logLevel"],
    ["version", "name", "link", "file", "stateDir", "nats"]);
  if (root.version !== NATS_LINK_CONFIG_VERSION) throw new NatsLinkConfigError("version", `must be ${NATS_LINK_CONFIG_VERSION}`);
  if (!full(BRIDGE_NAME, root.name)) throw new NatsLinkConfigError("name", "must be a bridge name ([A-Za-z0-9][A-Za-z0-9._-]{0,62})");
  if (!full(BRIDGE_NAME, root.link)) throw new NatsLinkConfigError("link", "must be a bridge name ([A-Za-z0-9][A-Za-z0-9._-]{0,62})");
  if (root.name === root.link) throw new NatsLinkConfigError("link", "must differ from name");

  const file = object(root.file, "file", ["mesh", "lockProtocol", "stateBackend", "maxStateBytes"], ["mesh"]);
  const mesh = absolutePath(file.mesh, "file.mesh");
  const lockProtocol = integer(file.lockProtocol, "file.lockProtocol", 1, 1, 3) as 1 | 2 | 3;
  if (file.stateBackend !== undefined && file.stateBackend !== "file" && file.stateBackend !== "shadow" && file.stateBackend !== "sqlite") {
    throw new NatsLinkConfigError("file.stateBackend", "must be file, shadow or sqlite");
  }
  // mesh-bridge's floor: two maximum events (src/mesh-bridge.ts openBridgeStore).
  const maxStateBytes = file.maxStateBytes === undefined ? undefined
    : integer(file.maxStateBytes, "file.maxStateBytes", 0, 2 * 256 * 1024, Number.MAX_SAFE_INTEGER);
  const stateDir = absolutePath(root.stateDir, "stateDir");
  if (stateDir === mesh || stateDir.startsWith(`${mesh}${path.sep}`) || mesh.startsWith(`${stateDir}${path.sep}`)) {
    throw new NatsLinkConfigError("stateDir", "must not be inside the mesh root, nor contain it");
  }

  const nats = object(root.nats, "nats",
    ["servers", "machine", "credentials", "caFile", "org", "custodyStream", "livenessStream", "pathBMachines", "operatorMachine", "events", "inboxLanes"],
    ["servers", "machine", "credentials", "caFile", "custodyStream", "livenessStream", "pathBMachines", "operatorMachine"]);
  const servers = typeof nats.servers === "string" ? [nats.servers] : nats.servers;
  if (!Array.isArray(servers) || servers.length === 0 || servers.length > 16 || !servers.every(server) || new Set(servers).size !== servers.length) {
    throw new NatsLinkConfigError("nats.servers", "must be one to sixteen distinct tls://<DNS name>:<port> URLs");
  }
  if (!full(MACHINE, nats.machine) || full(OPERATOR_MACHINE, nats.machine)) {
    throw new NatsLinkConfigError("nats.machine", "must be a Path B machine name, not an operator machine");
  }
  const credentials = absolutePath(nats.credentials, "nats.credentials");
  const caFile = absolutePath(nats.caFile, "nats.caFile");
  if (credentials === caFile) throw new NatsLinkConfigError("nats.caFile", "must differ from nats.credentials");
  const events = nats.events === undefined ? false : nats.events;
  if (typeof events !== "boolean") throw new NatsLinkConfigError("nats.events", "must be a boolean");
  if (nats.org === undefined ? events : !full(ORG, nats.org) || nats.org === "sys") {
    throw new NatsLinkConfigError("nats.org", "must be a non-sys org slug; it is required when nats.events is true");
  }
  const streams = (["custodyStream", "livenessStream"] as const).map((key) => {
    const stream = nats[key];
    if (!full(STREAM, stream) || /^(?:FABRIC_MESH_|_INBOX)/.test(stream.toUpperCase())) {
      throw new NatsLinkConfigError(`nats.${key}`, "must be a non-reserved literal stream name");
    }
    return stream;
  });
  if (streams[0]!.toUpperCase() === streams[1]!.toUpperCase()) {
    throw new NatsLinkConfigError("nats.livenessStream", "must differ from nats.custodyStream (case-insensitive)");
  }
  const machines = nats.pathBMachines;
  if (!Array.isArray(machines) || machines.length === 0 || machines.length > 64 ||
    !machines.every((machine) => full(MACHINE, machine) && !full(OPERATOR_MACHINE, machine)) || new Set(machines).size !== machines.length) {
    throw new NatsLinkConfigError("nats.pathBMachines", "must hold 1..64 distinct non-operator machine names");
  }
  if (!machines.includes(nats.machine)) throw new NatsLinkConfigError("nats.pathBMachines", "must contain nats.machine");
  if (!full(OPERATOR_MACHINE, nats.operatorMachine) || (nats.operatorMachine as string).length > 32) {
    throw new NatsLinkConfigError("nats.operatorMachine", "must match fabric-ops(-<lowercase alphanumeric>)");
  }

  // PR 2 relay: the hub's spoke links (their `mesh-bridge run --remote` names). A viaSpoke root must name one.
  const relay = root.relay === undefined ? {} : object(root.relay, "relay", ["spokes"], []);
  const spokes = relay.spokes === undefined ? [] : relay.spokes;
  if (!Array.isArray(spokes) || spokes.length > 64 || new Set(spokes).size !== spokes.length ||
    !spokes.every((spoke) => full(BRIDGE_NAME, spoke) && spoke !== root.link && spoke !== root.name)) {
    throw new NatsLinkConfigError("relay.spokes", "must be at most 64 distinct bridge link names other than name and link");
  }
  let parsed: BridgeNatsAllowListConfig;
  try {
    parsed = parseBridgeNatsAllowSection(root.allow, { link: root.link, natsMachine: nats.machine, spokes: spokes as string[] });
  } catch (error) {
    if (!(error instanceof BridgeNatsAllowListError)) throw error;
    throw new NatsLinkConfigError(error.field, error.message.slice(error.field.length + 1));
  }
  // A machine pin names an org machine: one in the inventory (L3 already refuses the bridge's own).
  parsed.entries.forEach((entry) => {
    if (entry.machine !== undefined && !(machines as string[]).includes(entry.machine)) {
      const index = parsed.entries.filter((other) => other.side === entry.side).indexOf(entry);
      throw new NatsLinkConfigError(`allow.${entry.side}[${index}].machine`, "must be one of nats.pathBMachines");
    }
  });
  // N4 (PR 2): a NATS entry's republishTopics are read from the org's event stream, which needs nats.events.
  if (!events && parsed.entries.some((entry) => entry.side === "nats" && entry.kind === "root" && (entry.republishTopics?.length ?? 0) > 0)) {
    throw new NatsLinkConfigError("nats.events", "must be true when an allow.nats entry lists republishTopics");
  }
  const orgJson = root.orgJson === undefined ? undefined : absolutePath(root.orgJson, "orgJson");
  // The section as written, frozen: L3's parser accepted it, and every reader parses it again the same way.
  const allow = deepFreeze(JSON.parse(JSON.stringify(root.allow ?? {})) as BridgeNatsAllowSection);

  const logLevel = root.logLevel === undefined ? "info" : root.logLevel;
  if (logLevel !== "info" && logLevel !== "debug") throw new NatsLinkConfigError("logLevel", "must be info or debug");
  return {
    version: 1,
    name: root.name,
    link: root.link,
    file: {
      mesh, lockProtocol,
      ...(file.stateBackend === undefined ? {} : { stateBackend: file.stateBackend }),
      ...(maxStateBytes === undefined ? {} : { maxStateBytes }),
    },
    stateDir,
    nats: {
      servers: [...servers] as string[],
      machine: nats.machine,
      credentials, caFile,
      ...(nats.org === undefined ? {} : { org: nats.org as string }),
      custodyStream: streams[0]!, livenessStream: streams[1]!,
      pathBMachines: [...machines] as string[],
      operatorMachine: nats.operatorMachine as string,
      events,
      inboxLanes: integer(nats.inboxLanes, "nats.inboxLanes", 8, 1, 64),
    },
    allow,
    relay: { spokes: Object.freeze([...spokes] as string[]) },
    ...(orgJson === undefined ? {} : { orgJson }),
    callTimeoutMs: integer(root.callTimeoutMs, "callTimeoutMs", 30_000, 1_000, 300_000),
    logLevel,
  };
};

const deepFreeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
};

/** The normalized, frozen allow-list L3's identity gate takes (createBridgeNatsIdentityGate), from the config's section. */
export const natsLinkAllowListConfig = (config: Pick<NatsLinkConfig, "allow" | "link" | "nats" | "relay">): BridgeNatsAllowListConfig =>
  parseBridgeNatsAllowSection(config.allow, { link: config.link, natsMachine: config.nats.machine, spokes: config.relay?.spokes ?? [] });

const euid = (): number | undefined => process.geteuid?.();

/** Refuse a file another local user could have written: the config is the link's authority for who may cross. */
const unsafeWriter = (stat: fs.Stats): string | undefined => {
  const uid = euid();
  if (uid === undefined) return undefined; // No POSIX owners (Windows): nothing to check.
  if (stat.uid !== uid && stat.uid !== 0) return "is owned by another user";
  if ((stat.mode & 0o022) !== 0) return "is group- or other-writable";
  return undefined;
};

/**
 * Read and parse the config file. The file must be a regular file (not a symlink), ours or root's, and not
 * group- or other-writable, because it names who may cross; its directory must not be writable by others.
 */
export const loadNatsLinkConfig = (file: string): NatsLinkConfig => {
  absolutePath(file, "--config");
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) throw new NatsLinkConfigError("--config", "does not exist");
  if (!stat.isFile()) throw new NatsLinkConfigError("--config", "must be a regular file, not a symlink or directory");
  if (stat.size > NATS_LINK_CONFIG_MAX_BYTES) throw new NatsLinkConfigError("--config", "is larger than 1 MiB");
  const unsafe = unsafeWriter(stat);
  if (unsafe) throw new NatsLinkConfigError("--config", unsafe);
  const parent = fs.statSync(path.dirname(file));
  const unsafeParent = unsafeWriter(parent);
  if (unsafeParent && !(parent.uid === 0 && (parent.mode & 0o1000) !== 0)) throw new NatsLinkConfigError("--config directory", unsafeParent);
  let parsed: unknown;
  try {
    parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    throw new NatsLinkConfigError("--config", "is not one JSON object");
  }
  return parseNatsLinkConfig(parsed);
};

export interface NatsLinkStatePaths {
  /** The MeshBridge cursor file. */
  readonly cursor: string;
  /** MeshBridge's per-peer stall reason file (bridgeStallReasonPath of the cursor). */
  readonly stallReason: string;
  /** The ACK/dedupe/correlation ledger's directory (L3 owns its contents). */
  readonly ledger: string;
}

export const natsLinkStatePaths = (config: Pick<NatsLinkConfig, "stateDir">): NatsLinkStatePaths => {
  const cursor = path.join(config.stateDir, "cursor.json");
  return { cursor, stallReason: `${cursor}.last-stall-reason`, ledger: path.join(config.stateDir, "ledger") };
};

/**
 * A wrong mesh path must fail, never become a new empty mesh (MeshStore creates its root): the directory must
 * already hold a mesh's state or event log.
 */
export const assertFabricMeshRoot = (root: string): void => {
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory() ||
    !["state.json", "state.db", "events.jsonl"].some((name) => fs.existsSync(path.join(root, name)))) {
    throw new NatsLinkConfigError("file.mesh", "must be an existing Fabric mesh directory (state.json, state.db or events.jsonl)");
  }
};

/**
 * The checks `start` makes before it opens anything: the credential file is a private regular file of ours
 * (0600 or 0400), the CA is a regular file, the mesh root is an existing mesh (a wrong path must fail, never
 * become a new empty mesh), and the state directory is ours and private (created 0700 when absent).
 * Reads metadata only; never a credential's bytes.
 */
export const preflightNatsLinkConfig = (config: NatsLinkConfig, options: { readonly createStateDir?: boolean } = {}): void => {
  const uid = euid();
  const credentials = fs.lstatSync(config.nats.credentials, { throwIfNoEntry: false });
  if (!credentials?.isFile()) throw new NatsLinkConfigError("nats.credentials", "must be an existing regular file, not a symlink");
  if (uid !== undefined) {
    if (credentials.uid !== uid) throw new NatsLinkConfigError("nats.credentials", "must be owned by the user running the link");
    if ((credentials.mode & 0o077) !== 0) throw new NatsLinkConfigError("nats.credentials", "must not be readable by group or others (0600 or 0400)");
  }
  if (!fs.statSync(config.nats.caFile, { throwIfNoEntry: false })?.isFile()) {
    throw new NatsLinkConfigError("nats.caFile", "must be an existing regular file");
  }
  assertFabricMeshRoot(config.file.mesh);
  const existing = fs.lstatSync(config.stateDir, { throwIfNoEntry: false });
  if (!existing) {
    // A dry run writes nothing: an absent state directory is created only by a real start.
    if (options.createStateDir === false) return;
    fs.mkdirSync(config.stateDir, { recursive: true, mode: 0o700 });
  } else if (!existing.isDirectory()) {
    throw new NatsLinkConfigError("stateDir", "must be a directory, not a symlink or file");
  }
  const state = fs.lstatSync(config.stateDir);
  if (uid !== undefined && (state.uid !== uid || (state.mode & 0o077) !== 0)) {
    throw new NatsLinkConfigError("stateDir", "must be owned by the user running the link and private (0700)");
  }
};

/**
 * The `mesh.nats` settings for the link's own NATS transport, from this config alone. Never merged with
 * PI_FABRIC_MESH_NATS or a fabric.json: the bridge machine's identity comes only from its config file.
 */
export const natsLinkMeshNatsConfig = (config: NatsLinkConfig): FabricMeshNatsConfig => ({
  replicas: 1,
  servers: [...config.nats.servers],
  machine: config.nats.machine,
  credentials: config.nats.credentials,
  tls: { caFile: config.nats.caFile },
  custodyStream: config.nats.custodyStream,
  livenessStream: config.nats.livenessStream,
  pathBMachines: [...config.nats.pathBMachines],
  operatorMachine: config.nats.operatorMachine,
  ...(config.nats.org === undefined ? {} : { org: config.nats.org }),
  events: config.nats.events,
  provision: false,
  inboxLanes: config.nats.inboxLanes,
  ephemeralHost: false,
  ephemeralInactiveSeconds: 3_600,
  inboxMaxAgeMs: DEFAULT_INBOX_MAX_AGE_MS,
});

/**
 * The config as status and receipt print it: every name and path, the allow-list, and nothing read from a
 * credential file.
 */
export const describeNatsLinkConfig = (config: NatsLinkConfig): Record<string, unknown> => ({
  name: config.name,
  link: config.link,
  mesh: config.file.mesh,
  stateDir: config.stateDir,
  nats: {
    servers: config.nats.servers, machine: config.nats.machine, org: config.nats.org ?? null,
    events: config.nats.events, credentials: config.nats.credentials, caFile: config.nats.caFile,
  },
  allow: {
    file: natsLinkAllowListConfig(config).entries.filter((entry) => entry.side === "file").map(bridgeNatsAllowKey),
    nats: natsLinkAllowListConfig(config).entries.filter((entry) => entry.side === "nats").map(bridgeNatsAllowKey),
  },
  relay: { spokes: config.relay?.spokes ?? [] },
});
