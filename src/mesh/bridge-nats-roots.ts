/**
 * Read-only NATS presence reader (smarty-dev#15310, A' lane L5, item N5): which roots (and actors) are live
 * in the org broker's KV presence right now, by the same rule routing uses (KvParticipantDirectory: a
 * presence record and its host lease, same machine, host, identity, root and incarnation, not terminal).
 *
 * It is for readers off the file mesh: `smarty-org-target` and `smarty-send` run from a Main on v2, and
 * `nats-link receipt --session --nats` on the hub. It opens a verified connection and the two KV buckets
 * only: no inbox stream, consumer, host lease or presence of its own, and it writes nothing.
 */
import { randomUUID } from "node:crypto";
import type { FabricTransport, TransportKv } from "../transport/types.js";
import type { FabricParticipantInfo, FabricParticipantKind } from "../topology/types.js";

/** Where to read: a verified Path B connection's settings. Paths only, never a credential's bytes. */
export interface NatsRootsSource {
  readonly servers: readonly string[];
  /** The reader credential's machine (its signed `machine:` tag). */
  readonly machine: string;
  readonly credentials: string;
  readonly caFile?: string;
  /** Inline CA PEM, as mesh.nats.tls.ca; public data. */
  readonly ca?: string;
  readonly custodyStream: string;
  readonly livenessStream: string;
  readonly pathBMachines: readonly string[];
  readonly org?: string;
  readonly events?: boolean;
}

/** One participant as NATS presence shows it; `live` is the routing rule's answer at read time. */
export interface NatsRootInfo {
  readonly id: string;
  readonly kind: string;
  readonly name: string;
  readonly sessionId: string | null;
  readonly rootId: string;
  /** The authenticated writer machine (from the KV key, never the record body). */
  readonly machine: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation: string;
  readonly status: string;
  readonly capabilities: readonly string[];
  readonly live: boolean;
}

/** The two presence buckets of one opened connection. */
export interface NatsPresenceBuckets {
  readonly machine: string;
  readonly presence: TransportKv;
  readonly leases: TransportKv;
  close(): Promise<void>;
}

export type OpenNatsPresence = (source: NatsRootsSource, signal: AbortSignal) => Promise<NatsPresenceBuckets>;

export class NatsRootsUnavailableError extends Error {
  readonly code = "FABRIC_NATS_ROOTS_UNAVAILABLE";
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "NatsRootsUnavailableError";
  }
}

/** The production opener: a verified agent-role connection and the presence/lease buckets, never provisioned. */
export const openNatsPresence: OpenNatsPresence = async (source, signal) => {
  const { openNatsConnection, openNatsKvForMachine } = await import("../transport/nats/index.js");
  const tls = source.caFile !== undefined ? { caFile: source.caFile } : source.ca !== undefined ? { ca: source.ca } : undefined;
  if (!tls) throw new NatsRootsUnavailableError("a NATS CA (caFile or ca) is required");
  const connection = await openNatsConnection({
    servers: [...source.servers],
    credentials: source.credentials,
    machine: source.machine,
    custodyStream: source.custodyStream,
    livenessStream: source.livenessStream,
    pathBMachines: [...source.pathBMachines],
    ...(source.events === undefined ? {} : { events: source.events }),
    ...(source.org === undefined ? {} : { org: source.org }),
    tls,
    name: "nats-link roots (read-only)",
    timeoutMs: 5_000,
    signal,
  });
  try {
    const presence = await openNatsKvForMachine(connection.js, "presence", source.machine, false, connection.onStatus);
    const leases = await openNatsKvForMachine(connection.js, "leases", source.machine, false, connection.onStatus);
    return { machine: source.machine, presence, leases, close: () => connection.close() };
  } catch (error) {
    await connection.close().catch(() => undefined);
    throw error;
  }
};

export interface ReadNatsRootsOptions {
  /** Bound on connecting plus the first complete snapshot of both buckets (default 10 s). */
  readonly timeoutMs?: number;
  readonly includeStale?: boolean;
  /** Participant kinds to list (default roots only). */
  readonly kinds?: readonly FabricParticipantKind[];
  readonly signal?: AbortSignal;
}

const info = (record: FabricParticipantInfo): NatsRootInfo => ({
  id: record.id,
  kind: record.kind,
  name: record.name,
  sessionId: record.sessionId ?? null,
  rootId: record.rootId,
  machine: record.machine ?? "",
  ownerHostId: record.ownerHostId,
  ownerIdentityId: record.ownerIdentityId,
  ownerIncarnation: record.ownerIncarnation ?? "",
  status: record.status,
  capabilities: [...(record.capabilities ?? [])],
  live: record.stale !== true,
});

/**
 * One snapshot of NATS presence. Resolves only after BOTH watches delivered their snapshot (`ready`);
 * otherwise it throws NatsRootsUnavailableError: an unknown answer is never an empty list.
 */
export const readNatsRoots = async (
  source: NatsRootsSource,
  options: ReadNatsRootsOptions = {},
  open: OpenNatsPresence = openNatsPresence,
): Promise<NatsRootInfo[]> => {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const deadline = AbortSignal.timeout(timeoutMs);
  const signal = options.signal ? AbortSignal.any([options.signal, deadline]) : deadline;
  let buckets: NatsPresenceBuckets | undefined;
  let directory: import("../topology/participant-view.js").KvParticipantDirectory | undefined;
  try {
    buckets = await open(source, signal);
    const { KvParticipantDirectory } = await import("../topology/participant-view.js");
    const opened = buckets;
    // A reader is no host: an owner id no record has, and a lease that is never live, so nothing here can
    // be "own" presence and no write path is reachable.
    const transport = {
      kind: "nats",
      self: { machine: opened.machine, hostId: `nats-link-reader:${randomUUID()}` },
      kv: (bucket: "presence" | "leases") => bucket === "presence" ? opened.presence : opened.leases,
    } as unknown as FabricTransport;
    const never = new AbortController();
    directory = new KvParticipantDirectory({
      transport,
      lease: { live: () => false, signal: never.signal },
      identityId: `nats-link-reader:${randomUUID()}`,
      ownerIncarnation: randomUUID(),
      rootId: "nats-link-reader",
    });
    directory.start();
    const view = directory;
    while (!view.ready) {
      // whenChanged settles when the directory becomes ready (or fails); the id is one no record has.
      await view.whenChanged("nats-link-reader:ready", signal);
    }
    const kinds = options.kinds ?? ["root"];
    return view.list({ scope: "project", includeStale: options.includeStale === true, kinds: [...kinds] }).map(info);
  } catch (error) {
    if (options.signal?.aborted) throw error;
    if (deadline.aborted) throw new NatsRootsUnavailableError(`no complete NATS presence snapshot within ${timeoutMs} ms`, { cause: error });
    throw new NatsRootsUnavailableError(`NATS presence unreadable: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  } finally {
    await directory?.close().catch(() => undefined);
    await buckets?.close().catch(() => undefined);
  }
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown): string | undefined => typeof value === "string" && value.trim() ? value.trim() : undefined;

/**
 * The reader's source from a NATS runtime's own environment, as a Main on v2 has it: PI_FABRIC_MESH_NATS
 * (the mesh.nats JSON the pod is launched with), else the PI_FABRIC_NATS_* variables `pi-fabric nats send`
 * reads. Explicit overrides (CLI flags) win field by field. Throws a plain Error naming the missing field.
 */
export const natsRootsSourceFromEnv = (
  env: NodeJS.ProcessEnv = process.env,
  overrides: Partial<Pick<NatsRootsSource, "servers" | "machine" | "credentials" | "caFile">> = {},
): NatsRootsSource => {
  let json: Record<string, unknown> = {};
  if (env.PI_FABRIC_MESH_NATS !== undefined) {
    let parsed: unknown;
    try { parsed = JSON.parse(env.PI_FABRIC_MESH_NATS); } catch { parsed = undefined; }
    if (!isObject(parsed)) throw new Error("PI_FABRIC_MESH_NATS must be a JSON object");
    json = parsed;
  }
  const list = (value: unknown): string[] | undefined =>
    Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string")
      : typeof value === "string" ? value.split(",").map((entry) => entry.trim()).filter(Boolean) : undefined;
  const tls = isObject(json.tls) ? json.tls : {};
  const servers = overrides.servers ?? list(json.servers) ?? list(env.PI_FABRIC_NATS_SERVERS);
  const machine = overrides.machine ?? text(json.machine) ?? text(env.PI_FABRIC_MACHINE);
  const credentials = overrides.credentials ?? text(json.credentials) ?? text(env.PI_FABRIC_NATS_CREDENTIALS);
  const caFile = overrides.caFile ?? text(tls.caFile);
  const ca = overrides.caFile === undefined ? text(tls.ca) : undefined;
  const custodyStream = text(json.custodyStream) ?? text(env.PI_FABRIC_NATS_CUSTODY_STREAM);
  const livenessStream = text(json.livenessStream) ?? text(env.PI_FABRIC_NATS_LIVENESS_STREAM);
  const pathBMachines = list(json.pathBMachines) ?? list(env.PI_FABRIC_NATS_MACHINES);
  const missing = Object.entries({ servers: servers?.length ? servers : undefined, machine, credentials,
    "tls.caFile or tls.ca": caFile ?? ca, custodyStream, livenessStream, pathBMachines: pathBMachines?.length ? pathBMachines : undefined })
    .filter(([, value]) => value === undefined).map(([key]) => key);
  if (missing.length) throw new Error(`NATS reader settings missing: ${missing.join(", ")} (set PI_FABRIC_MESH_NATS or pass flags)`);
  return {
    servers: servers!, machine: machine!, credentials: credentials!,
    ...(caFile !== undefined ? { caFile } : { ca: ca! }),
    custodyStream: custodyStream!, livenessStream: livenessStream!, pathBMachines: pathBMachines!,
    ...(text(json.org) ? { org: text(json.org)! } : {}),
    ...(typeof json.events === "boolean" ? { events: json.events } : {}),
  };
};
