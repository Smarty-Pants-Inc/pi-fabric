// Standalone authority-only collector. Never import MeshStore or any product lifecycle module.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

const MAX_SOURCE = 32 * 1024 * 1024;
const MAX_RECORD = 256 * 1024;
const MAX_ROWS = 10_000;
const OMIT = Symbol("omit");
const H = "hash", I = "identity", P = "peer", N = "integer", B = "boolean", SID = "session", A = "alias";
const UPDATED_BY_ID = Symbol("updatedBy.id");
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
// Runtime IDs: fabric-runtime-state.ts; resident IDs: residency/protocol.ts;
// actor/agent IDs: actors/manager.ts and agents/manager.ts (UUID without dashes).
// These two named CI roots alone come from tests/residency.test.ts rootHarness;
// do not generalize its arbitrary `name` argument into an authority allowlist.
const CI_ROOT = "(?:actor-outbox-exact-owner|actor-mailbox-exact-owner)";
// participant: is a UI entityId only; the release owner's shape-only readback found no persisted authority IDs, so reject it.
const ID_SHAPES = [
  ["session:<uuid>", new RegExp(`^session:${UUID}$`, "i")],
  ["<32hex>", /^[0-9a-f]{32}$/],
  ["resident:<24hex>", /^resident:[0-9a-f]{24}$/],
  ["runtime:<uuid>", new RegExp(`^runtime:${UUID}$`, "i")],
  ["session:<exact-owner-ci>:<hex>", new RegExp(`^session:${CI_ROOT}:[0-9a-f]{1,13}$`)],
];
const UPDATED_BY_ID_PATTERNS = [
  /^bridge:[a-z0-9-]{1,32}$/,
  /^spawn-router$/,
  /^maintenance:[a-z0-9-]{1,40}$/,
];
const SESSION_PATTERN = new RegExp(`^(?:${UUID}|${CI_ROOT})$`, "i");
const SAFE_SHAPES = new Set([...ID_SHAPES.map(([shape]) => shape), "bridge", "maintenance", "literal-tool", "session:other", "resident:other", "runtime:other", "participant:other", "actor:other", "agent:other", "length:0", "length:1-32", "length:33-64", "length:65-128", "length:129-256", "length:over-256", "non-string"]);
function idShape(value) {
  if (typeof value !== "string") return "non-string";
  const known = ID_SHAPES.find(([, pattern]) => pattern.test(value)); if (known) return known[0];
  if (value.startsWith("bridge:")) return "bridge";
  if (value === "spawn-router" || value.startsWith("spawn-router")) return "literal-tool";
  if (value.startsWith("maintenance:")) return "maintenance";
  for (const prefix of ["session", "resident", "runtime", "participant", "actor", "agent"]) if (value.startsWith(`${prefix}:`)) return `${prefix}:other`;
  const size = value.length; return size === 0 ? "length:0" : size <= 32 ? "length:1-32" : size <= 64 ? "length:33-64" : size <= 128 ? "length:65-128" : size <= 256 ? "length:129-256" : "length:over-256";
}
function supportedId(value, spec) {
  if (typeof value !== "string") return false;
  if (spec === UPDATED_BY_ID && UPDATED_BY_ID_PATTERNS.some(pattern => pattern.test(value))) return true;
  return ID_SHAPES.some(([, pattern]) => pattern.test(value));
}
const PEER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/;
const enumOf = (...values) => ({ enum: values });
const obj = (fields, required = []) => ({ fields, required });
const array = item => ({ array: item });
const one = enumOf(1), two = enumOf(2);
const identity = obj({ id: I, kind: enumOf("main", "agent", "actor"), sessionId: SID, verified: enumOf("bridge"), name: A }, ["id", "kind", "name"]);
const updatedByIdentity = obj({ id: UPDATED_BY_ID, kind: enumOf("main", "agent", "actor"), sessionId: SID, verified: enumOf("bridge"), name: A }, ["id", "kind", "name"]);
const processRecord = obj({ pid: N, host: P, startTime: H, incarnation: H, link: P, machineId: H, bootId: H });
const mirror = obj({ remoteHost: P, observedAt: N, sourceObservedAt: N, sourceUpdatedAt: N, sourceExpiresAt: N,
  bridgeProcess: processRecord }, ["remoteHost", "observedAt", "sourceObservedAt", "sourceUpdatedAt", "sourceExpiresAt"]);
const session = obj({ id: SID, startedAt: N, updatedAt: N, expiresAt: N }, ["id", "startedAt", "updatedAt", "expiresAt"]);
const writer = obj({ pid: N, host: P, releaseSha: "sha", lockProtocol: enumOf(1, 2, 3),
  stateBackend: enumOf("file", "sqlite", "shadow"), startedAt: N }, ["pid", "host", "releaseSha", "lockProtocol", "stateBackend", "startedAt"]);
const history = { history: true };
const host = obj({ format: one, livenessLeaseFiles: one, id: I, rootId: I, identity,
  incarnationToken: H, bridgeSupersededTokens: array(H), bridgeSupersededTokenSeen: history,
  startedAt: N, updatedAt: N, expiresAt: N, remoteHost: P, bridgeVia: P, mirrorLease: one },
["format", "id", "rootId", "identity", "startedAt", "updatedAt", "expiresAt"]);
const participant = obj({ format: one, id: I, kind: enumOf("root", "agent", "actor", "provider"), rootId: I,
  ownerHostId: I, ownerIdentityId: I, ownerIncarnation: H, parentId: I, sessionId: SID,
  interactive: B, mainBindings: B, livenessLeaseFiles: one,
  status: enumOf("idle", "running", "stopping", "reloading", "queued", "finishing", "completed", "failed", "stopped", "timed_out", "dormant", "preparing", "waiting", "failing-preparation"),
  residency: enumOf("session", "durable"), runner: OMIT,
  transport: enumOf("process", "in-process", "host", "hosted", "rpc", "ssh", "herdr", "tmux", "screen", "localterm", "auto"),
  capabilities: array(enumOf("steer", "followUp", "stop", "ask", "actor-bindings", "attach", "fabric")),
  controlProtocol: enumOf("v1", "legacy"), startedAt: N, updatedAt: N, finishedAt: N, reloadUntil: N,
  remoteHost: P, bridgeVia: P, mainProcess: processRecord,
  name: A, label: OMIT, role: OMIT, herdrPane: OMIT, project: OMIT, projectRoot: OMIT, repository: OMIT,
  cwd: OMIT, model: OMIT, thinking: OMIT, provider: OMIT, pendingMessages: OMIT, currentTool: OMIT,
  turns: OMIT, toolCalls: OMIT, usage: OMIT, finalAnswerReceiptId: OMIT, actorOwnershipToken: OMIT,
  actorQueued: OMIT, actorMessages: OMIT, actorPreparing: OMIT, actorRun: OMIT, actorRemoval: OMIT,
  stopReason: OMIT, ownerRootGoneAt: OMIT, ownerRootGoneBy: OMIT, settledAt: OMIT, retiredAt: OMIT, retiredBy: OMIT,
  task: OMIT, prompt: OMIT, message: OMIT, messages: OMIT, text: OMIT, content: OMIT, data: OMIT },
["format", "id", "kind", "rootId", "ownerHostId", "ownerIdentityId", "status", "startedAt", "updatedAt", "controlProtocol", "capabilities", "transport"]);
const handoff = obj({ format: one, id: I, sessionId: SID, link: P, handedOffAt: N, hub: P, operator: A, name: A },
["format", "id", "sessionId", "link", "handedOffAt", "hub"]);
const policy = obj({ version: one, hostLeases: enumOf("files"), participants: enumOf("files") }, ["version"]);
const lease = obj({ format: one, id: I, rootId: I, identityId: I, startedAt: N, updatedAt: N, expiresAt: N,
  incarnationToken: H, bridgeSupersededTokens: array(H), bridgeSupersededTokenSeen: history,
  reloadUntil: N, session, mirror, mirrorLease: one, writer }, ["format", "id", "rootId", "identityId", "updatedAt", "expiresAt"]);
const grace = obj({ sequence: N, startedAt: N, id: H }, ["sequence", "startedAt"]);
const direction = obj({ after: N, mark: N, offset: N, relayGrace: grace }, ["after", "mark"]);
const cursor = obj({ format: one, local: P, remote: P, toRemote: direction, toLocal: direction }, ["format", "local", "remote", "toRemote", "toLocal"]);
const prefixes = { "topology/hosts/": host, "topology/participants/": participant, "topology/handoffs/": handoff };
const digest = raw => createHash("sha256").update(raw).digest("hex");
export const tokenHash = raw => {
  if (typeof raw !== "string" || !raw || Buffer.byteLength(raw) > 256 || /[\u0000-\u001f\u007f-\u009f\ud800-\udfff]/u.test(raw)) fail("INVALID_SCALAR");
  return `h:${digest(raw).slice(0, 32)}`;
};
// Derive diagnostic paths solely from the fixed projection, never an input key.
const ID_FIELDS = new Map();
for (const [kind, spec, root] of [["host", host, "entry.value"], ["participant", participant, "entry.value"], ["handoff", handoff, "entry.value"], ["policy", policy, "entry.value"], ["lease", lease, "lease"], ["cursor", cursor, "cursor"]]) {
  const fields = new Set();
  const visit = (node, field) => {
    if (node === I || node === SID || node === UPDATED_BY_ID) fields.add(field);
    if (node?.fields) for (const [key, child] of Object.entries(node.fields)) visit(child, `${field}.${key}`);
  };
  visit(spec, root); if (root === "entry.value") visit(updatedByIdentity, "entry.updatedBy"); ID_FIELDS.set(kind, fields);
}
export class SnapshotError extends Error {
  constructor(code, details) {
    super(code); this.name = "SnapshotError"; this.code = code;
    if (code === "UNSUPPORTED_ID" && ID_FIELDS.get(details?.recordKind)?.has(details?.field) && SAFE_SHAPES.has(details?.shape)) {
      this.field = details.field; this.recordKind = details.recordKind; this.shape = details.shape;
    }
  }
}
const fail = (code, details) => { throw new SnapshotError(code, details); };
const safeInteger = value => Number.isSafeInteger(value) && value >= 0;
function context(censusMode = false) {
  const hashes = new Map(), rawIdentities = new Map(), fields = new Map(), aliases = new Map(), aliasRefs = [];
  const unsupported = new Map(), structural = new Set();
  return {
    censusMode, rawIdentities, aliases, aliasRefs,
    resolveAliases() {
      for (const ref of aliasRefs) { const raw = aliases.get(ref.alias); if (rawIdentities.has(raw)) ref.object[ref.key] = raw; }
    },
    hash(raw) {
      const value = tokenHash(raw), prior = hashes.get(value);
      if (prior !== undefined && prior !== raw) fail("HASH_COLLISION");
      hashes.set(value, raw); return value;
    },
    count(field, included) {
      const row = fields.get(field) ?? { field, included: 0, dropped: 0 };
      row[included ? "included" : "dropped"]++; fields.set(field, row);
    },
    recordUnsupported(field, recordKind, shape) {
      const key = `${field}\0${recordKind}\0${shape}`, row = unsupported.get(key) ?? { field, recordKind, shape, count: 0 };
      row.count++; unsupported.set(key, row);
    },
    unsupportedRows() {
      return [...unsupported.values()].sort((a, b) => a.field.localeCompare(b.field) || a.recordKind.localeCompare(b.recordKind) || a.shape.localeCompare(b.shape));
    },
    deferStructural(code) { structural.add(code); },
    firstStructural() { return [...structural].sort()[0]; },
    discardIncluded(field) { const row = fields.get(field); if (!row || row.included < 1) fail("INTERNAL_SCHEMA"); row.included--; row.dropped++; },
    census() {
      const rows = [...fields.values()].sort((a, b) => a.field.localeCompare(b.field));
      return { included: rows.reduce((n, r) => n + r.included, 0), dropped: rows.reduce((n, r) => n + r.dropped, 0), fields: rows };
    },
  };
}

// Structural JSON reader: discarded values are validated lexically but never materialized.
// In particular FILE entries outside the fixed authority key ranges never reach JSON.parse.
class Reader {
  constructor(text, ctx, recordKind) { this.text = text; this.at = 0; this.ctx = ctx; this.recordKind = recordKind; }
  ws() { while (/[\x20\t\r\n]/.test(this.text[this.at] ?? "!")) this.at++; }
  char(c) { this.ws(); if (this.text[this.at++] !== c) fail("INVALID_JSON"); }
  string(materialize = true) {
    this.ws(); const start = this.at; if (this.text[this.at++] !== '"') fail("INVALID_JSON");
    while (this.at < this.text.length) {
      const c = this.text.charCodeAt(this.at++);
      if (c === 34) {
        if (!materialize) return undefined;
        let value; try { value = JSON.parse(this.text.slice(start, this.at)); } catch { fail("INVALID_JSON"); }
        return value;
      }
      if (c < 32) fail("INVALID_JSON");
      if (c === 92) {
        const escape = this.text[this.at++];
        if (escape === "u") { if (!/^[0-9a-fA-F]{4}$/.test(this.text.slice(this.at, this.at + 4))) fail("INVALID_JSON"); this.at += 4; }
        else if (!'"\\/bfnrt'.includes(escape ?? "!")) fail("INVALID_JSON");
      }
    }
    fail("INVALID_JSON");
  }
  object(visit, depth = 0) {
    if (depth > 32) fail("LIMIT_EXCEEDED");
    this.char("{"); this.ws(); const seen = new Set();
    if (this.text[this.at] === "}") { this.at++; return seen; }
    for (;;) {
      const key = this.string();
      if (key.length > 1024 || seen.has(key) || ["__proto__", "constructor", "prototype"].includes(key)) fail("UNSAFE_SCHEMA");
      seen.add(key); this.char(":"); visit(key, depth + 1); this.ws();
      const separator = this.text[this.at++];
      if (separator === "}") return seen;
      if (separator !== ",") fail("INVALID_JSON");
    }
  }
  skip(depth = 0) {
    if (depth > 32) fail("LIMIT_EXCEEDED"); this.ws();
    const c = this.text[this.at];
    if (c === '"') { this.string(false); return; }
    if (c === "{") { this.object((_key, d) => this.skip(d), depth); return; }
    if (c === "[") { this.at++; this.ws(); if (this.text[this.at] === "]") { this.at++; return; }
      for (;;) { this.skip(depth + 1); this.ws(); const end = this.text[this.at++]; if (end === "]") return; if (end !== ",") fail("INVALID_JSON"); }
    }
    this.scalar(false);
  }
  scalar(materialize = true) {
    this.ws(); if (this.text[this.at] === '"') return this.string(materialize);
    const match = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(this.text.slice(this.at));
    if (!match) fail("INVALID_JSON"); this.at += match[0].length;
    if (!materialize) return undefined;
    try { return JSON.parse(match[0]); } catch { fail("INVALID_JSON"); }
  }
  project(spec, field, depth = 0) {
    if (depth > 12) fail("LIMIT_EXCEEDED"); this.ws();
    if (spec === OMIT) { this.skip(depth); this.ctx.count(field, false); return undefined; }
    if (spec?.fields) {
      const output = Object.create(null);
      const seen = this.object((key, d) => {
        if (Object.hasOwn(spec.fields, key)) {
          const value = this.project(spec.fields[key], `${field}.${key}`, d);
          if (value !== undefined) output[key] = value;
          if (spec.fields[key] === A) this.ctx.aliasRefs.push({ object: output, key, alias: value });
        }
        else {
          this.ws(); if (["{", "["].includes(this.text[this.at])) fail("UNSAFE_SCHEMA");
          this.skip(d); this.ctx.count("unknown.scalar", false);
        }
      }, depth);
      for (const key of spec.required) if (!seen.has(key)) fail("MISSING_FIELD");
      this.ctx.count(field, true); return output;
    }
    if (spec?.history) {
      const output = Object.create(null); let count = 0;
      this.object((key, d) => {
        if (++count > 256) fail("LIMIT_EXCEEDED");
        const projectedKey = this.ctx.hash(key), value = this.project(N, `${field}.ordinal`, d);
        output[projectedKey] = value; this.ctx.count(`${field}.token`, true);
      }, depth);
      this.ctx.count(field, true); return output;
    }
    if (spec?.array) {
      const output = []; this.char("["); this.ws();
      if (this.text[this.at] === "]") this.at++;
      else for (;;) {
        if (output.length >= 256) fail("LIMIT_EXCEEDED"); output.push(this.project(spec.array, `${field}.item`, depth + 1));
        this.ws(); const end = this.text[this.at++]; if (end === "]") break; if (end !== ",") fail("INVALID_JSON");
      }
      this.ctx.count(field, true); return output;
    }
    let value = this.scalar();
    if (spec === N && !safeInteger(value) || spec === B && typeof value !== "boolean" || spec?.enum && !spec.enum.includes(value)) fail("INVALID_SCALAR");
    if ([H, I, SID, UPDATED_BY_ID, P, A].includes(spec)) {
      // Diagnose IDs before generic scalar validation, without retaining their raw value.
      const idSlot = spec === I || spec === SID || spec === UPDATED_BY_ID;
      const validId = spec === SID ? typeof value === "string" && SESSION_PATTERN.test(value) : supportedId(value, spec);
      if (idSlot && !validId) {
        const shape = idShape(value);
        if (this.ctx.censusMode) { this.ctx.recordUnsupported(field, this.recordKind, shape); this.ctx.count(field, false); return undefined; }
        fail("UNSUPPORTED_ID", { field, recordKind: this.recordKind, shape });
      }
      // Validate every free string before retaining it or deriving a safe alias.
      const hashed = this.ctx.hash(value).slice(2);
      if (spec === H) value = `h:${hashed}`;
      if (spec === I || spec === UPDATED_BY_ID) this.ctx.rawIdentities.set(value, value);
      if (spec === P && !PEER_PATTERN.test(value)) fail("INVALID_SCALAR");
      if (spec === A) { const raw = value; value = `n:${hashed}`; this.ctx.aliases.set(value, raw); }
    }
    if (spec === "sha" && (typeof value !== "string" || value !== "unknown" && !/^[0-9a-f]{40}$/.test(value))) fail("INVALID_SCALAR");
    this.ctx.count(field, true); return value;
  }
  done() { this.ws(); if (this.at !== this.text.length) fail("INVALID_JSON"); }
}
function classify(key) {
  if (key === "topology/liveness") return { prefix: key, spec: policy, label: "policy", recordKind: "policy" };
  for (const [prefix, spec] of Object.entries(prefixes)) if (key.startsWith(prefix)) {
    if (!/^[0-9a-f]{64}$/.test(key.slice(prefix.length))) fail("INVALID_KEY");
    return { prefix, spec, label: prefix.split("/")[1], recordKind: spec === host ? "host" : spec === participant ? "participant" : "handoff" };
  }
  return undefined;
}
function projectEntry(reader, sourceKey, file = false) {
  const type = classify(sourceKey); if (!type) fail("INVALID_KEY");
  reader.recordKind = type.recordKind;
  let rawKey;
  const schema = obj({ ...(file ? { format: one } : {}), key: "key", value: type.spec, updatedAt: N, version: N, updatedBy: updatedByIdentity },
    ["key", "value", "updatedAt", "version", "updatedBy", ...(file ? ["format"] : [])]);
  // Keys are the one specially validated string scalar, never emitted until their ID join is checked.
  schema.fields.key = enumOf(sourceKey);
  const entry = reader.project(schema, "entry"); rawKey = entry.key;
  if (type.spec !== policy) {
    const rawId = reader.ctx.rawIdentities.get(entry.value.id);
    if (rawId === undefined) {
      if (!reader.ctx.censusMode || entry.value.id !== undefined) {
        if (reader.ctx.censusMode) reader.ctx.deferStructural("KEY_ID_MISMATCH");
        else fail("KEY_ID_MISMATCH");
      }
    } else if (rawKey !== type.prefix + digest(rawId)) {
      if (reader.ctx.censusMode) reader.ctx.deferStructural("KEY_ID_MISMATCH");
      else fail("KEY_ID_MISMATCH");
    }
    entry.key = rawKey;
  }
  if (file) reader.ctx.discardIncluded("entry.format");
  delete entry.format; return entry;
}
function projectText(text, spec, label, ctx, recordKind) {
  const reader = new Reader(text, ctx, recordKind), value = reader.project(spec, label); reader.done(); return value;
}
function fingerprint(stat) { return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`; }
function regular(file, limit) {
  const before = fs.lstatSync(file, { bigint: true });
  if (!before.isFile() || before.nlink !== 1n || before.size > BigInt(limit)) fail("UNSAFE_INPUT");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const opened = fs.fstatSync(fd, { bigint: true }); if (fingerprint(before) !== fingerprint(opened)) fail("SOURCE_CHANGED");
    const text = fs.readFileSync(fd, "utf8");
    if (Buffer.byteLength(text) > limit || fingerprint(opened) !== fingerprint(fs.fstatSync(fd, { bigint: true })) || fingerprint(before) !== fingerprint(fs.lstatSync(file, { bigint: true }))) fail("SOURCE_CHANGED");
    return text;
  } finally { fs.closeSync(fd); }
}
function physical(file, directory = false) {
  const resolved = path.resolve(file); let current = path.parse(resolved).root;
  for (const part of resolved.slice(current.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part); const stat = fs.lstatSync(current);
    if (stat.isSymbolicLink()) fail("UNSAFE_INPUT");
  }
  if (directory && !fs.statSync(resolved).isDirectory()) fail("UNSAFE_INPUT");
  return resolved;
}
function directoryFiles(root, subdir, pattern) {
  const directory = path.join(root, subdir);
  try { physical(directory, true); } catch (error) { if (error.code === "ENOENT") return []; throw error; }
  const names = fs.readdirSync(directory).filter(name => pattern.test(name)).sort();
  if (names.length > MAX_ROWS) fail("LIMIT_EXCEEDED"); return names.map(name => path.join(directory, name));
}
function sourceStamp(files) { return files.map(file => fingerprint(fs.lstatSync(file, { bigint: true }))).join("|"); }
function projectFileState(text, ctx) {
  const reader = new Reader(text, ctx), metadata = Object.create(null), entries = []; let found = false;
  reader.object(key => {
    if (key === "entries") {
      found = true; reader.object(sourceKey => {
        if (classify(sourceKey)) {
          const start = reader.at; entries.push(projectEntry(reader, sourceKey)); if (reader.at - start > MAX_RECORD || entries.length > MAX_ROWS) fail("LIMIT_EXCEEDED");
        } else { reader.skip(); ctx.count("state.excludedEntry", false); }
      });
    } else if (["format", "revisionFormat", "backendEpoch", "highWater"].includes(key)) {
      const spec = key === "format" ? enumOf(1, 2) : key === "revisionFormat" ? two : N;
      metadata[key] = reader.project(spec, `state.${key}`);
    } else if (key === "readGeneration") {
      const value = reader.scalar(); if (typeof value !== "string" || !/^[0-9a-f-]{36}$/.test(value)) fail("INVALID_SCALAR"); ctx.count("state.readGeneration", false);
    } else if (key === "readJournalHash" || key === "backendDigest") {
      const value = reader.scalar(); if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) fail("INVALID_SCALAR"); ctx.count(`state.${key}`, false);
    } else if (["versions", "tombstoneOrder"].includes(key)) { reader.skip(); ctx.count(`state.${key}`, false); }
    else { reader.ws(); if (["{", "["].includes(reader.text[reader.at])) fail("UNSAFE_SCHEMA"); reader.skip(); ctx.count("unknown.scalar", false); }
  });
  reader.done(); if (!found || metadata.format === undefined) fail("UNSUPPORTED_BACKEND"); return { entries, metadata };
}
function walFrames(file) {
  let fd;
  try {
    physical(file); const checked = fs.lstatSync(file);
    if (!checked.isFile() || checked.nlink !== 1) fail("UNSAFE_INPUT");
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
    const header = Buffer.alloc(32), size = fs.fstatSync(fd).size;
    if (size === 0) return 0;
    if (fs.readSync(fd, header, 0, 32, 0) !== 32 || ![0x377f0682, 0x377f0683].includes(header.readUInt32BE(0))) fail("INVALID_WAL");
    const pageSize = header.readUInt32BE(8);
    if (pageSize < 512 || pageSize > 65536 || (pageSize & (pageSize - 1)) !== 0) fail("INVALID_WAL");
    return Math.floor((size - 32) / (24 + pageSize));
  } catch (error) { if (error.code === "ENOENT") return 0; throw error; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
}
function sqliteEntries(root, ctx) {
  const file = physical(path.join(root, "state.db"));
  if (!fs.lstatSync(file).isFile() || fs.lstatSync(file).nlink !== 1) fail("UNSAFE_INPUT");
  for (const suffix of ["-wal", "-shm", "-journal"]) {
    try { physical(file + suffix); if (!fs.lstatSync(file + suffix).isFile() || fs.lstatSync(file + suffix).nlink !== 1) fail("UNSAFE_INPUT"); }
    catch (error) { if (error.code !== "ENOENT") throw error; }
  }
  // Read WAL header only before/after the native connection, never plain-fs-open it
  // while SQLite holds this process's POSIX locks. SHM read-index maintenance is
  // explicitly accepted normal reader behavior by the lead's scoped ruling.
  const observation = { pageCountBefore: 0, pageCountAfter: 0, walFramesBefore: walFrames(file + "-wal"), walFramesAfter: 0 };
  const sqlite = process.getBuiltinModule("node:sqlite"); if (!sqlite?.DatabaseSync) fail("SQLITE_UNAVAILABLE");
  let db;
  try { db = new sqlite.DatabaseSync(file, { readOnly: true, timeout: 250 }); }
  catch { fail("READONLY_UNAVAILABLE"); }
  try {
    db.exec("BEGIN; PRAGMA query_only=ON");
    observation.pageCountBefore = db.prepare("PRAGMA page_count").get().page_count;
    const cols = db.prepare("PRAGMA table_info(kv)").all().map(row => row.name);
    if (JSON.stringify(cols) !== JSON.stringify(["key", "value", "version", "updated_at", "updated_by", "bytes"])) fail("UNSUPPORTED_SCHEMA");
    const metadata = Object.create(null);
    for (const row of db.prepare("SELECT name,value FROM meta WHERE name IN ('schema','backend','epoch','commit_no')").all()) {
      if (row.name === "backend") { if (row.value !== "sqlite") fail("UNSUPPORTED_BACKEND"); ctx.count("state.backend", true); }
      else {
        if (!safeInteger(row.value)) fail("UNSUPPORTED_SCHEMA");
        const name = row.name === "commit_no" ? "commit" : row.name;
        metadata[name] = row.value; ctx.count(`state.${name}`, true);
      }
    }
    if (metadata.schema !== 1 || !safeInteger(metadata.epoch) || !safeInteger(metadata.commit)) fail("UNSUPPORTED_SCHEMA");
    if (db.prepare("SELECT value FROM meta WHERE name='backend'").get()?.value !== "sqlite") fail("UNSUPPORTED_BACKEND");
    const rows = db.prepare(`SELECT key,value,version,updated_at,updated_by FROM kv WHERE
      (key>=? AND key<?) OR (key>=? AND key<?) OR (key>=? AND key<?) OR key=? ORDER BY key LIMIT ?`).all(
      "topology/hosts/", "topology/hosts0", "topology/participants/", "topology/participants0", "topology/handoffs/", "topology/handoffs0", "topology/liveness", MAX_ROWS + 1);
    if (rows.length > MAX_ROWS) fail("LIMIT_EXCEEDED");
    const entries = rows.map(row => {
      if (typeof row.key !== "string" || typeof row.value !== "string" || typeof row.updated_by !== "string" || Buffer.byteLength(row.value) > MAX_RECORD || Buffer.byteLength(row.updated_by) > MAX_RECORD || !safeInteger(row.version) || !safeInteger(row.updated_at)) fail("INVALID_SQLITE_ROW");
      const type = classify(row.key), value = projectText(row.value, type.spec, "entry.value", ctx, type.recordKind), updatedBy = projectText(row.updated_by, updatedByIdentity, "entry.updatedBy", ctx, type.recordKind);
      let key = row.key;
      if (type.spec !== policy) {
        const rawId = ctx.rawIdentities.get(value.id), expected = rawId === undefined ? undefined : type.prefix + digest(rawId);
        if (rawId === undefined) {
          if (!ctx.censusMode || value.id !== undefined) {
            if (ctx.censusMode) ctx.deferStructural("KEY_ID_MISMATCH");
            else fail("KEY_ID_MISMATCH");
          }
        } else if (key !== expected) {
          if (ctx.censusMode) ctx.deferStructural("KEY_ID_MISMATCH");
          else fail("KEY_ID_MISMATCH");
        } else key = expected;
      }
      ctx.count("entry.key", true); ctx.count("entry.version", true); ctx.count("entry.updatedAt", true); ctx.count("entry", true);
      return { key, value, updatedAt: row.updated_at, version: row.version, updatedBy };
    });
    observation.pageCountAfter = db.prepare("PRAGMA page_count").get().page_count;
    if (!safeInteger(observation.pageCountBefore) || !safeInteger(observation.pageCountAfter)) fail("UNSUPPORTED_SCHEMA");
    db.exec("COMMIT"); return { entries, metadata, observation };
  } finally { if (db.isTransaction) db.exec("ROLLBACK"); db.close(); observation.walFramesAfter = walFrames(file + "-wal"); }
}

export async function collectAuthoritySnapshot(options) {
  try {
    if (!options || !["hub", "work"].includes(options.site) || !["dev1", "ryzen2"].includes(options.name) || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/.test(options.peer ?? "") || !Array.isArray(options.cursors ?? []) || (options.cursors ?? []).length > 32) fail("INVALID_OPTIONS");
    const root = physical(options.mesh, true), ctx = context(options.census === true);
    const participantPaths = directoryFiles(root, "participants", /^[0-9a-f]{64}\.json$/), leasePaths = directoryFiles(root, "host-leases", /^[0-9a-f]{32}\.json$/);
    const cursors = (options.cursors ?? []).map(file => physical(file));
    const files = [...participantPaths, ...leasePaths, ...cursors], before = sourceStamp(files);
    const backend = options.site === "hub" ? "sqlite" : "file";
    const statePath = path.join(root, "state.json");
    let stateBefore;
    if (backend === "file") {
      // A DB fence makes an assumed FILE capture unsupported; never read a stale checkpoint.
      if (fs.existsSync(path.join(root, "state.db"))) fail("UNSUPPORTED_BACKEND");
      physical(statePath); stateBefore = fingerprint(fs.lstatSync(statePath, { bigint: true }));
    }
    const state = backend === "sqlite" ? sqliteEntries(root, ctx) : projectFileState(regular(statePath, MAX_SOURCE), ctx);
    const participantFiles = participantPaths.map(file => {
      const key = `topology/participants/${path.basename(file, ".json")}`, reader = new Reader(regular(file, MAX_RECORD), ctx);
      const entry = projectEntry(reader, key, true); reader.done(); return entry;
    });
    const leaseFiles = leasePaths.map(file => {
      const projected = projectText(regular(file, MAX_RECORD), lease, "lease", ctx, "lease"), rawId = ctx.rawIdentities.get(projected.id);
      if (rawId === undefined) {
        if (!ctx.censusMode || projected.id !== undefined) {
          if (ctx.censusMode) ctx.deferStructural("KEY_ID_MISMATCH");
          else fail("KEY_ID_MISMATCH");
        }
      } else if (path.basename(file) !== `${digest(rawId).slice(0, 32)}.json`) {
        if (ctx.censusMode) ctx.deferStructural("KEY_ID_MISMATCH");
        else fail("KEY_ID_MISMATCH");
      }
      return projected;
    });
    const projectedCursors = cursors.map((file, index) => ({ index, ...projectText(regular(file, MAX_RECORD), cursor, "cursor", ctx, "cursor") }));
    const afterPaths = [...directoryFiles(root, "participants", /^[0-9a-f]{64}\.json$/), ...directoryFiles(root, "host-leases", /^[0-9a-f]{32}\.json$/), ...cursors];
    if (files.join("\0") !== afterPaths.join("\0") || before !== sourceStamp(afterPaths) || stateBefore !== undefined && stateBefore !== fingerprint(fs.lstatSync(statePath, { bigint: true }))) fail("SOURCE_CHANGED");
    if (ctx.firstStructural()) fail(ctx.firstStructural());
    ctx.resolveAliases();
    const counts = { ...ctx.census(), ...(state.observation ? { sqlite: state.observation } : {}) };
    if (ctx.censusMode) {
      const output = { schemaVersion: 1, mode: "census", complete: ctx.unsupportedRows().length === 0, backend, unsupported: ctx.unsupportedRows(), counts };
      if (Buffer.byteLength(JSON.stringify(output)) > MAX_SOURCE) fail("LIMIT_EXCEEDED"); return output;
    }
    const output = { schemaVersion: 1, site: options.site, name: options.name, peer: options.peer, backend,
      collectedAt: Date.now(), stateMetadata: state.metadata, stateEntries: state.entries.sort((a, b) => a.key.localeCompare(b.key)), participantFiles, leaseFiles, cursors: projectedCursors, counts };
    if (Buffer.byteLength(JSON.stringify(output)) > MAX_SOURCE) fail("LIMIT_EXCEEDED"); return output;
  } catch (error) { if (error instanceof SnapshotError) throw error; fail("COLLECTION_FAILED"); }
}
