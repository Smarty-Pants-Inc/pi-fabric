import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { openNatsConnection, type NatsConnectionHandle } from "../../transport/nats/connection.js";
import { LEASES_BUCKET, PRESENCE_BUCKET, openNatsKv } from "../../transport/nats/kv.js";
import type { FabricTransport, TransportKv } from "../../transport/types.js";
import { KvParticipantDirectory, type ParticipantOwnerRecord } from "../../topology/participant-view.js";
import { readHostLeases } from "../../topology/host-leases.js";
import { ROOT_ID_PREFIX } from "../../topology/root-inbox.js";
import { BRIDGE_HANDOFF_PREFIX, bridgeHandoffOf, bridgeHandoffScope, type BridgeHandoff } from "../bridge.js";
import { meshStateDocumentProblem, type MeshStateEntry } from "../state-file.js";
import { meshStateSourceOf, readMeshStateNatsMarker } from "../backend-fence.js";
import { pathBMachines } from "./mint.js";
import { policyConfig } from "./send-cli.js";

const USAGE = [
  "Usage: pi-fabric nats owner --session <uuid | session:uuid>",
  "  --creds <absolute-file> --ca <absolute-file> --servers <tls://host:port[,tls://host:port...]>",
  "  --machine <machine> (--mesh <absolute-mesh-root> | --no-mesh) [--org <org>] [--path-b-machines <csv>] [--timeout-ms <ms>]",
  "Policy comes from PI_FABRIC_MESH_NATS or PI_FABRIC_NATS_CUSTODY_STREAM, PI_FABRIC_NATS_LIVENESS_STREAM, PI_FABRIC_NATS_MACHINES.",
  "Read-only JSON receipt; exit 0 only for an owned session and every requested source read; 1 unknown/not owned; 2 usage.",
].join("\n");
const MAX_TIMEOUT_MS = 24 * 60 * 60 * 1_000;
const HOST_PREFIX = "topology/hosts/", PARTICIPANT_PREFIX = "topology/participants/";
const keyFor = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);

class UsageError extends Error {}
interface OwnerOptions {
  rootId: string; creds: string; ca: string; servers: string[]; machine: string;
  org?: string; pathBMachines?: string[]; meshRoot?: string; timeoutMs: number;
}
const absolute = (value: string, flag: string): string => {
  if (!path.isAbsolute(value) || value.includes("\0")) throw new UsageError(`${flag} must be an absolute path`);
  return value;
};
function parseOptions(argv: readonly string[]): OwnerOptions | "help" {
  if (argv.length === 1 && ["--help", "-h", "help"].includes(argv[0]!)) return "help";
  const flags = new Set(["--session", "--creds", "--ca", "--servers", "--machine", "--org", "--path-b-machines", "--mesh", "--timeout-ms"]);
  const values = new Map<string, string>();
  let noMesh = false;
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i]!;
    if (flag === "--no-mesh" && !noMesh) { noMesh = true; continue; }
    if (!flags.has(flag) || values.has(flag) || !argv[i + 1] || argv[i + 1]!.startsWith("--")) throw new UsageError("invalid arguments; see usage");
    values.set(flag, argv[++i]!);
  }
  const required = (flag: string): string => {
    const value = values.get(flag);
    if (!value) throw new UsageError(`${flag} is required`);
    return value;
  };
  // The file-side handoff marker is part of the receipt: skipping it must be an explicit choice.
  if (noMesh === values.has("--mesh")) throw new UsageError("give exactly one of --mesh <dir> or --no-mesh");
  const session = required("--session").replace(/^session:/, "");
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(session)) throw new UsageError("--session must be a lowercase UUID or session:<uuid>");
  const machine = required("--machine");
  if (!/^[a-z0-9][a-z0-9-]{0,31}$/.test(machine)) throw new UsageError("--machine must be a NATS machine name");
  const servers = required("--servers").split(",").map(server => server.trim());
  if (servers.some(server => {
    try { const url = new URL(server); return url.protocol !== "tls:" || !url.hostname || !url.port || !!url.username || !!url.password || !!url.search || !!url.hash || (url.pathname !== "" && url.pathname !== "/"); }
    catch { return true; }
  })) throw new UsageError("--servers must be comma-separated tls://host:port URLs without credentials");
  const timeout = values.get("--timeout-ms") ?? "30000";
  if (!/^[1-9][0-9]*$/.test(timeout) || !Number.isSafeInteger(Number(timeout)) || Number(timeout) > MAX_TIMEOUT_MS) throw new UsageError(`--timeout-ms must be an integer from 1 to ${MAX_TIMEOUT_MS}`);
  const org = values.get("--org");
  if (org !== undefined && (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(org) || org === "sys")) throw new UsageError("--org must be a non-system org name");
  let machines: string[] | undefined;
  if (values.has("--path-b-machines")) {
    try { machines = pathBMachines(values.get("--path-b-machines")!.split(",").map(value => value.trim())); }
    catch { throw new UsageError("--path-b-machines must be 1-64 unique Path B machine names"); }
  }
  return { rootId: ROOT_ID_PREFIX + session, machine, servers, timeoutMs: Number(timeout),
    creds: absolute(required("--creds"), "--creds"), ca: absolute(required("--ca"), "--ca"),
    ...(org === undefined ? {} : { org }), ...(machines === undefined ? {} : { pathBMachines: machines }),
    ...(values.has("--mesh") ? { meshRoot: absolute(values.get("--mesh")!, "--mesh") } : {}) };
}

export interface OwnershipReceipt {
  format: 1;
  checkedAt: string;
  rootId: string;
  sessionId: string;
  reader: { machine: string };
  verdict: "owned" | "absent" | "stale" | "ambiguous";
  owner: { machine: string; hostId: string; identityId: string; incarnation: string; status: string; presenceRevision: number;
    lease: NonNullable<ParticipantOwnerRecord["lease"]> } | null;
  nats: { rows: ParticipantOwnerRecord[] };
  file: { meshRoot: string; handoff: (Pick<BridgeHandoff, "link" | "handedOffAt" | "operator" | "hub"> & { inForce: boolean }) | null;
    mirror: { host: Record<string, unknown> | null; participant: Record<string, unknown> | null } | null } | null;
  /** Unknown sources never mean known absence, even if the other source found a live owner. */
  unknown?: Array<"nats" | "file" | "connection">;
  /** Fixed diagnostic, never an arbitrary broker error, record, path contents, JWT or seed. */
  error?: string;
}
export interface OwnershipReceiptOptions { machine: string; meshRoot?: string; signal?: AbortSignal; now?: () => number }
export type OwnershipView = Pick<KvParticipantDirectory, "ready" | "whenChanged" | "owners" | "resolve">;
export type OwnershipOutcome = { receipt: OwnershipReceipt; exitCode: 0 | 1 };

const baseReceipt = (rootId: string, machine: string, now = Date.now()): OwnershipReceipt => ({
  format: 1, checkedAt: new Date(now).toISOString(), rootId, sessionId: rootId.slice(ROOT_ID_PREFIX.length),
  reader: { machine }, verdict: "stale", owner: null, nats: { rows: [] }, file: null,
});

/** O_RDONLY only: no MeshStore constructor/census, backend opening, custody or lock creation. */
function readJson(file: string, maxBytes: number): unknown {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > maxBytes) throw new Error("invalid file");
    // Read at most the bound even if a non-cooperating writer grows the file after fstat.
    const bytes = Buffer.alloc(Math.min(stat.size + 1, maxBytes + 1));
    let count = 0;
    while (count < bytes.length) {
      const n = fs.readSync(fd, bytes, count, bytes.length - count, null);
      if (n === 0) break;
      count += n;
    }
    if (count > stat.size || count > maxBytes) throw new Error("file changed or too large");
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count)));
  } finally { fs.closeSync(fd); }
}
function directoryNames(dir: string, optional = false): string[] {
  try {
    const stat = fs.lstatSync(dir);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("invalid directory");
    return fs.readdirSync(dir).sort();
  } catch (error) {
    if (optional && (error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}
function topologyValid(entry: MeshStateEntry, prefix: string): void {
  const value = entry.value;
  if (!object(value) || !text(value.id) || entry.key !== keyFor(prefix, value.id) || value.format !== 1 ||
    !text(value.rootId) || (value.remoteHost !== undefined && !text(value.remoteHost))) throw new Error("invalid topology record");
  if (prefix === HOST_PREFIX) {
    if (!object(value.identity) || !text(value.identity.id) || !finite(value.expiresAt)) throw new Error("invalid host record");
  } else if (!text(value.ownerHostId) || !text(value.ownerIdentityId) || !text(value.kind) || !text(value.status)) {
    throw new Error("invalid participant record");
  }
}
function mirrorSummary(entry: MeshStateEntry | undefined, host: boolean): Record<string, unknown> | null {
  if (!entry || !object(entry.value) || !text(entry.value.remoteHost)) return null;
  const value = entry.value;
  // Do not serialize an entire file participant/host: those can carry unrelated metadata/content.
  const fields = host ? ["id", "rootId", "remoteHost", "bridgeVia", "incarnationToken", "updatedAt", "expiresAt"]
    : ["id", "rootId", "remoteHost", "bridgeVia", "ownerHostId", "ownerIdentityId", "ownerIncarnation", "kind", "status", "updatedAt"];
  return { version: entry.version, ...Object.fromEntries(fields.filter(key => value[key] !== undefined).map(key => {
    const item = value[key];
    if (typeof item !== "string" && !finite(item)) throw new Error("invalid mirror field");
    return [key, item];
  })), ...(host && object(value.identity) ? { identityId: value.identity.id } : {}) };
}
function fileStateEntries(meshRoot: string): MeshStateEntry[] {
  const document = readJson(path.join(meshRoot, "state.json"), 32 * 1024 * 1024);
  if (!object(document)) throw new Error("invalid mesh state");
  // Rollback's file envelope carries fence metadata, which is not part of the state payload.
  const { backendEpoch: _epoch, backendDigest, ...payload } = document;
  if (backendDigest !== undefined && (typeof backendDigest !== "string" || !/^sha256:[a-f0-9]{64}$/.test(backendDigest))) throw new Error("invalid backend digest");
  if (meshStateDocumentProblem(payload) !== undefined) throw new Error("invalid mesh state");
  return Object.values((payload as { entries: Record<string, MeshStateEntry> }).entries);
}
function meshStateEntries(meshRoot: string): MeshStateEntry[] {
  directoryNames(meshRoot);
  if (readMeshStateNatsMarker(meshRoot)) throw new Error("NATS state backend is not a local mesh source");
  const file = path.join(meshRoot, "state.db"), before = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!before) {
    // This pure reader rule recognizes moved/NATS markers and epoch mismatches; no backend
    // factory or readFenceMeta/openPrivateStateDb call (those open writable) is involved.
    if (meshStateSourceOf(meshRoot, "none", 0).source !== "file") throw new Error("unknown backend");
    return fileStateEntries(meshRoot);
  }
  if (!before.isFile() || before.isSymbolicLink()) throw new Error("invalid state database");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    const header = Buffer.alloc(100), stat = fs.fstatSync(fd);
    if (stat.dev !== before.dev || stat.ino !== before.ino || fs.readSync(fd, header, 0, 100, 0) !== 100 ||
      header.subarray(0, 16).toString("latin1") !== "SQLite format 3\0") throw new Error("invalid database header");
    if (header[18] === 2 || header[19] === 2) {
      // Even SQLite READONLY can create empty WAL/SHM files when a WAL-mode writer has
      // closed. Refuse that case BEFORE opening; never create the missing sidecars.
      // Live hub writers already hold both. Only existing -shm read marks may change.
      for (const suffix of ["-wal", "-shm"]) {
        const sidecar = fs.lstatSync(file + suffix);
        if (!sidecar.isFile() || sidecar.isSymbolicLink()) throw new Error("invalid database sidecar");
      }
    }
  } finally { fs.closeSync(fd); }
  // First use only. The writer's openNodeSqlite uses the same native builtin, but not
  // its writable opener, initialization, PRAGMAs, schema creation or checkpoint paths.
  const sqlite = process.getBuiltinModule?.("node:sqlite") as typeof import("node:sqlite") | undefined;
  if (!sqlite?.DatabaseSync) throw new Error("node:sqlite unavailable");
  const database = new sqlite.DatabaseSync(file, { readOnly: true, timeout: 0 });
  try {
    const current = fs.lstatSync(file);
    if (!current.isFile() || current.dev !== before.dev || current.ino !== before.ino) throw new Error("state database changed");
    // ONE SELECT statement captures fence metadata and the relevant namespaces in the same
    // SQLite read snapshot. No exec/run, PRAGMA, transaction write or checkpoint is issued.
    const rows = database.prepare(`SELECT 'meta' AS source, name AS key, value, NULL AS version, NULL AS updated_at, NULL AS updated_by
      FROM meta WHERE name IN ('schema', 'backend', 'epoch')
      UNION ALL SELECT 'kv' AS source, key, value, version, updated_at, updated_by FROM kv
      WHERE (key >= ? AND key < ?) OR (key >= ? AND key < ?) OR (key >= ? AND key < ?)`)
      .all(HOST_PREFIX, HOST_PREFIX + "\uffff", PARTICIPANT_PREFIX, PARTICIPANT_PREFIX + "\uffff", BRIDGE_HANDOFF_PREFIX, BRIDGE_HANDOFF_PREFIX + "\uffff");
    const meta = new Map(rows.filter(row => row.source === "meta").map(row => [String(row.key), row.value]));
    if (Number(meta.get("schema")) !== 1 || !text(meta.get("backend")) || meta.get("epoch") === null ||
      meta.get("epoch") === "" || !Number.isSafeInteger(Number(meta.get("epoch")))) throw new Error("invalid database metadata");
    const source = meshStateSourceOf(meshRoot, String(meta.get("backend")), Number(meta.get("epoch")));
    if (source.source === "file") return fileStateEntries(meshRoot);
    const entries = rows.filter(row => row.source === "kv").map(row => {
      if (typeof row.key !== "string" || typeof row.value !== "string" || typeof row.updated_by !== "string") throw new Error("invalid database row");
      return { key: row.key, value: JSON.parse(row.value) as unknown, version: Number(row.version),
        updatedAt: Number(row.updated_at), updatedBy: JSON.parse(row.updated_by) as MeshStateEntry["updatedBy"] };
    });
    if (meshStateDocumentProblem({ format: 1, entries: Object.fromEntries(entries.map(entry => [entry.key, entry])) }) !== undefined) throw new Error("invalid database entries");
    return entries;
  } finally { database.close(); }
}
function readFileReceipt(meshRoot: string, rootId: string, now: number): NonNullable<OwnershipReceipt["file"]> {
  const entries = meshStateEntries(meshRoot);
  const hosts = entries.filter(entry => entry.key.startsWith(HOST_PREFIX));
  const participants = entries.filter(entry => entry.key.startsWith(PARTICIPANT_PREFIX));
  for (const entry of hosts) topologyValid(entry, HOST_PREFIX);
  for (const entry of participants) topologyValid(entry, PARTICIPANT_PREFIX);
  // readParticipantFiles is read-only but tolerant/cached: do not let corrupt files disappear.
  const files: MeshStateEntry[] = [];
  for (const name of directoryNames(path.join(meshRoot, "participants"), true)) {
    if (!/^[0-9a-f]{64}\.json$/.test(name)) continue;
    const parsed = readJson(path.join(meshRoot, "participants", name), 256 * 1024);
    if (!object(parsed) || parsed.format !== 1) throw new Error("invalid participant file");
    const { format: _format, ...entry } = parsed;
    const key = PARTICIPANT_PREFIX + name.slice(0, -5);
    if (entry.key !== key || meshStateDocumentProblem({ format: 1, entries: { [key]: entry } }) !== undefined) throw new Error("invalid participant file");
    topologyValid(entry as unknown as MeshStateEntry, PARTICIPANT_PREFIX);
    files.push(entry as unknown as MeshStateEntry);
  }
  // readHostLeases performs only stat/readdir/read; collect its normally-tolerated read errors.
  const rawLeases: Record<string, unknown>[] = [];
  for (const name of directoryNames(path.join(meshRoot, "host-leases"), true)) {
    if (!name.endsWith(".json")) continue;
    const raw = readJson(path.join(meshRoot, "host-leases", name), 256 * 1024);
    if (!object(raw) || !text(raw.id)) throw new Error("invalid host lease");
    rawLeases.push(raw);
  }
  const problems: string[] = [];
  const leases = readHostLeases(meshRoot, problems);
  if (problems.length) throw new Error("invalid host lease");
  for (const raw of rawLeases) {
    const lease = leases.get(raw.id as string);
    // The shared reader deliberately drops invalid optional metadata. A receipt must not
    // turn malformed native/session evidence into an apparently in-force handoff.
    if (!lease || ["writer", "session", "mirror"].some(field => raw[field] !== undefined &&
      lease[field as "writer" | "session" | "mirror"] === undefined)) throw new Error("incomplete host lease");
  }
  const handoffs = entries.filter(entry => entry.key.startsWith(BRIDGE_HANDOFF_PREFIX));
  if (handoffs.some(entry => bridgeHandoffOf(entry) === undefined)) throw new Error("invalid handoff");
  const handoffEntry = handoffs.find(entry => entry.key === keyFor(BRIDGE_HANDOFF_PREFIX, rootId));
  const handoff = handoffEntry ? bridgeHandoffOf(handoffEntry) : undefined;
  const scope = bridgeHandoffScope(handoffEntry ? [handoffEntry] : [], hosts, [...participants, ...files], () => leases, now, handoff?.link ?? "");
  const host = mirrorSummary(hosts.find(entry => entry.key === keyFor(HOST_PREFIX, rootId)), true);
  const participant = mirrorSummary(participants.find(entry => entry.key === keyFor(PARTICIPANT_PREFIX, rootId)), false);
  return { meshRoot, handoff: handoff ? { link: handoff.link, handedOffAt: handoff.handedOffAt, operator: handoff.operator,
    hub: handoff.hub, inForce: !scope.voided.includes(handoffEntry!) } : null,
    mirror: host || participant ? { host, participant } : null };
}

/** Snapshot readback, not registration or an exclusivity fence. The caller owns start/close. */
export async function ownershipReceipt(view: OwnershipView, rootId: string, options: OwnershipReceiptOptions): Promise<OwnershipOutcome> {
  const receipt = baseReceipt(rootId, options.machine, options.now?.());
  const unknown: NonNullable<OwnershipReceipt["unknown"]> = [];
  try {
    const signal = options.signal ?? AbortSignal.timeout(30_000);
    signal.throwIfAborted();
    while (!view.ready) await view.whenChanged(rootId, signal);
    // No await between the owner list and resolve: both use one ready in-memory watch view.
    const rows = view.owners(rootId), route = view.resolve(rootId);
    receipt.nats.rows = rows;
    receipt.verdict = rows.length === 0 ? "absent" : new Set(rows.map(row => row.machine)).size > 1 ? "ambiguous"
      : route?.live ? "owned" : "stale";
    const row = receipt.verdict === "owned" ? rows[0] : undefined;
    if (row?.lease) receipt.owner = { machine: row.machine, hostId: row.ownerHostId, identityId: row.ownerIdentityId,
      incarnation: row.ownerIncarnation, status: row.status, presenceRevision: row.presenceRevision, lease: { ...row.lease } };
    else if (receipt.verdict === "owned") throw new Error("owner lease unavailable");
  } catch { unknown.push("nats"); receipt.verdict = "stale"; receipt.owner = null; }
  const checkedAt = options.now?.() ?? Date.now();
  receipt.checkedAt = new Date(checkedAt).toISOString();
  if (options.meshRoot !== undefined) {
    try { options.signal?.throwIfAborted(); receipt.file = readFileReceipt(options.meshRoot, rootId, checkedAt); }
    catch { unknown.push("file"); }
  }
  if (unknown.length) { receipt.unknown = unknown; receipt.error = "Ownership sources could not be read completely"; }
  return { receipt, exitCode: receipt.verdict === "owned" && unknown.length === 0 ? 0 : 1 };
}

const noWrites = (): never => { throw new Error("Ownership reader is read-only"); };
function readOnlyKv(kv: TransportKv): TransportKv {
  return { ttlMs: kv.ttlMs, get: kv.get.bind(kv), watch: kv.watch.bind(kv), put: async () => noWrites(), delete: async () => noWrites() };
}
export async function runNatsOwnerCli(argv: string[], io: { out?: (text: string) => void; err?: (text: string) => void } = {}): Promise<number> {
  const out = io.out ?? (value => { process.stdout.write(value); });
  const err = io.err ?? (value => { process.stderr.write(value); });
  let options: OwnerOptions;
  try {
    const parsed = parseOptions(argv);
    if (parsed === "help") { err(`${USAGE}\n`); return 2; }
    options = parsed;
  } catch (error) {
    out(`${JSON.stringify({ format: 1, error: error instanceof UsageError ? error.message : "Invalid arguments", unknown: ["connection"] })}\n`);
    err(`${USAGE}\n`);
    return 2;
  }
  let connection: NatsConnectionHandle | undefined, view: KvParticipantDirectory | undefined;
  let outcome: OwnershipOutcome;
  let stage = "policy";
  try {
    const policy = policyConfig(), signal = AbortSignal.timeout(options.timeoutMs);
    stage = "connection";
    connection = await openNatsConnection({ servers: options.servers, credentials: options.creds, tls: { caFile: options.ca },
      machine: options.machine, role: "agent", custodyStream: policy.custodyStream, livenessStream: policy.livenessStream,
      pathBMachines: options.pathBMachines ?? policy.pathBMachines, ...(options.org === undefined ? {} : { org: options.org }),
      name: "pi-fabric nats owner (read-only)", timeoutMs: options.timeoutMs, signal });
    stage = "presence bucket";
    const presence = readOnlyKv(await openNatsKv(connection.js, { bucket: PRESENCE_BUCKET }, false, connection.onStatus));
    stage = "leases bucket";
    const leases = readOnlyKv(await openNatsKv(connection.js, { bucket: LEASES_BUCKET }, false, connection.onStatus));
    stage = "participant view";
    signal.throwIfAborted();
    // An unrelated reader host avoids the directory's special local-owner monotonic lease gate,
    // including when --machine equals the real owner's machine. This reader never owns a lease.
    const readerId = `owner-reader:${randomUUID()}`;
    const transport: FabricTransport = { kind: "nats", self: { machine: options.machine, hostId: readerId },
      kv: bucket => bucket === "presence" ? presence : leases, send: async () => noWrites(),
      async *inbox() { noWrites(); }, drainState: async () => noWrites(), close: async () => {} };
    view = new KvParticipantDirectory({ transport, lease: { live: () => false, signal }, identityId: readerId,
      ownerIncarnation: readerId, rootId: options.rootId, operationTimeoutMs: Math.min(options.timeoutMs, 30_000) });
    view.start();
    outcome = await ownershipReceipt(view, options.rootId, { machine: options.machine,
      ...(options.meshRoot === undefined ? {} : { meshRoot: options.meshRoot }), signal });
    stage = "connection verification";
    if (!connection.isVerified()) throw new Error("connection not verified");
  } catch {
    const receipt = baseReceipt(options.rootId, options.machine);
    receipt.unknown = ["connection", "nats", ...(options.meshRoot === undefined ? [] : ["file" as const])];
    receipt.error = `Ownership read failed at ${stage}`;
    outcome = { receipt, exitCode: 1 };
  } finally {
    // No publications were ever admitted: close joins the watches; its owned/cleanup maps are empty.
    try { await view?.close(); } catch { /* Connection close below still ends subscriptions. */ }
    try { await connection?.close(); } catch { /* No data writes or custody to release. */ }
  }
  out(`${JSON.stringify(outcome.receipt)}\n`);
  return outcome.exitCode;
}
