// Private JSONL worker for mixed-release-canary. No source imports or fabricated presence.
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import { pathToFileURL, fileURLToPath } from "node:url";

const hash = value => createHash("sha256").update(value).digest("hex");
const hostKey = id => `topology/hosts/${hash(id)}`;
const participantKey = id => `topology/participants/${hash(id)}`;
const MAX_BYTES = 256 * 1024, MAX_READ = 10_000;

/** Evidence-only redaction: never apply this to forwarded wire or seeded authority. */
export function sanitizeAuthorityEvidence(value) {
  const reject = () => { throw new Error("authority evidence rejected"); };
  const token = value => typeof value !== "string" ? reject() : /^h:[a-f0-9]{32}$/.test(value) ? value : `h:${hash(value).slice(0, 32)}`;
  const visit = value => {
    if (Array.isArray(value)) return value.map(visit);
    if (!value || typeof value !== "object") return value;
    return Object.fromEntries(Object.entries(value).map(([key, item]) => {
      if (["incarnationToken", "ownerIncarnation", "tokenHash", "incarnation", "processIncarnation", "nonce"].includes(key))
        return [key, item == null ? item : token(item)];
      if (["bridgeSupersededTokens", "currentTokens", "knownHistory", "history"].includes(key))
        return [key, Array.isArray(item) ? item.map(token) : reject()];
      if (key === "bridgeSupersededTokenSeen") {
        if (!item || typeof item !== "object" || Array.isArray(item)) return reject();
        return [key, Object.fromEntries(Object.entries(item).map(([k, v]) =>
          [token(k), Number.isSafeInteger(v) && v >= 0 ? v : reject()]))];
      }
      // Explicitly classify the two predicate flags; never print an unknown field
      // or its value in a rejection. Canonical IDs (even bare 32hex) are not tokens.
      if (["tokenless", "supersededHistory"].includes(key)) return [key, typeof item === "boolean" ? item : reject()];
      if (/token|incarnation|nonce|history|secret|credential|password|authorization/i.test(key)) return reject();
      return [key, visit(item)];
    }));
  };
  return visit(value);
}

export function nativeProcessEvidence(pid = process.pid) {
  const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8"), fields = text.slice(text.lastIndexOf(")") + 2).split(" ");
  return { host: os.hostname(), pid, nativeBirth: fields[19], ppid: Number(fields[1]), pgid: Number(fields[2]),
    bootId: fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim() };
}

/** Internal classes are named exports of split chunks, NOT dist/<source-path>.js. */
export async function loadReleaseSymbols(release, wanted = ["MeshStore", "SqliteStateStore", "ParticipantDirectory", "FabricControlPlane"]) {
  const dir = path.join(release, "dist/chunks");
  const found = new Map();
  for (const name of fs.readdirSync(dir).filter(name => name.endsWith(".js")).sort()) {
    const file = path.join(dir, name), text = fs.readFileSync(file, "utf8");
    const exports = [...text.matchAll(/export\s*\{([^}]+)\}/g)].map(match => match[1]).join(",");
    for (const symbol of wanted) {
      if (!new RegExp(`(?:var|let|const)\\s+${symbol}\\s*=\\s*class\\b|class\\s+${symbol}\\b`).test(text)) continue;
      const exported = exports.split(",").map(value => value.trim()).find(value => new RegExp(`^${symbol}(?:\\s+as\\s+\\w+)?$`).test(value));
      if (!exported) continue;
      if (found.has(symbol)) throw new Error(`ambiguous compiled ${symbol} in ${release}`);
      found.set(symbol, { file, key: exported.split(/\s+as\s+/)[1] ?? symbol, sha256: hash(text) });
    }
  }
  const symbols = {}, artifacts = {};
  for (const symbol of wanted) {
    const match = found.get(symbol);
    if (!match) throw new Error(`missing exported compiled class ${symbol} in ${release}`);
    symbols[symbol] = (await import(pathToFileURL(match.file).href))[match.key];
    if (typeof symbols[symbol] !== "function") throw new Error(`invalid compiled ${symbol}`);
    artifacts[symbol] = { path: match.file, sha256: match.sha256 };
  }
  return { ...symbols, artifacts };
}

/** Snapshot-only replay. This never opens a source mesh or imports source modules. */
export function validateAuthoritySnapshot(snapshot, site) {
  if (!snapshot || snapshot.schemaVersion !== 1 || snapshot.site !== site ||
      snapshot.backend !== (site === "hub" ? "sqlite" : "file") ||
      !Number.isFinite(snapshot.collectedAt) || snapshot.collectedAt <= 0 ||
      !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$/.test(snapshot.name ?? "") ||
      !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$/.test(snapshot.peer ?? "") || snapshot.name === snapshot.peer ||
      !snapshot.stateMetadata || !Array.isArray(snapshot.stateEntries) ||
      !Array.isArray(snapshot.participantFiles) || !Array.isArray(snapshot.leaseFiles) || !Array.isArray(snapshot.cursors))
    throw new Error("invalid or missing authority snapshot/backend");
  // Handoff admission observes valid native writer markers, not just TTL. This narrow
  // replay strips imported process observation and therefore refuses that topology rather
  // than silently changing a reservation. A future supported mapping needs a real-path probe.
  if (snapshot.stateEntries.some(entry => entry?.key?.startsWith("topology/handoffs/")))
    throw new Error("unsupported handoff authority: imported writer/process classification needs an owned replay mapping");
  const keys = new Set();
  for (const entry of [...snapshot.stateEntries, ...snapshot.participantFiles]) {
    if (!entry || typeof entry.key !== "string" || !entry.value ||
        !Number.isSafeInteger(entry.version) || entry.version <= 0 || !Number.isFinite(entry.updatedAt) ||
        !entry.updatedBy || typeof entry.updatedBy.id !== "string" || typeof entry.updatedBy.kind !== "string")
      throw new Error("invalid authority entry metadata");
    if (snapshot.stateEntries.includes(entry)) {
      if (keys.has(entry.key)) throw new Error("duplicate authority state key");
      keys.add(entry.key);
    }
    const prefix = ["topology/hosts/", "topology/participants/", "topology/handoffs/"].find(prefix => entry.key.startsWith(prefix));
    if (!prefix && entry.key !== "topology/liveness") throw new Error("unknown authority namespace");
    if (prefix && (typeof entry.value.id !== "string" || entry.key !== prefix + hash(entry.value.id)))
      throw new Error("authority key/id mismatch");
    if (snapshot.participantFiles.includes(entry) && !entry.key.startsWith("topology/participants/"))
      throw new Error("invalid participant file placement");
    if (entry.key.startsWith("topology/hosts/") && (entry.value.format !== 1 || typeof entry.value.rootId !== "string" ||
        !entry.value.identity || typeof entry.value.identity.id !== "string" || !["main", "actor", "agent"].includes(entry.value.identity.kind) ||
        ![entry.value.startedAt, entry.value.updatedAt, entry.value.expiresAt].every(Number.isFinite)))
      throw new Error("invalid authority host record");
    if (entry.key.startsWith("topology/participants/") && (entry.value.format !== 1 ||
        ![entry.value.ownerHostId, entry.value.ownerIdentityId, entry.value.rootId].every(value => typeof value === "string")))
      throw new Error("invalid authority participant record");
  }
  const leaseIds = new Set();
  for (const lease of snapshot.leaseFiles) {
    if (!lease || typeof lease.id !== "string" || typeof lease.rootId !== "string" ||
        typeof lease.identityId !== "string" || !Number.isFinite(lease.updatedAt) || !Number.isFinite(lease.expiresAt))
      throw new Error("invalid authority lease");
    if (leaseIds.has(lease.id)) throw new Error("duplicate authority lease");
    leaseIds.add(lease.id);
  }
  if (new Set(snapshot.participantFiles.map(entry => entry.key)).size !== snapshot.participantFiles.length)
    throw new Error("duplicate authority participant file");
  const tokens = item => {
    if (!item || typeof item !== "object") return;
    for (const [key, value] of Object.entries(item)) {
      if (["incarnationToken", "ownerIncarnation"].includes(key) &&
          (typeof value !== "string" || !/^(?:h:)?[a-f0-9]{32}$/.test(value)))
        throw new Error("authority token is not a stable hash prefix");
      if (key === "bridgeSupersededTokens" && (!Array.isArray(value) || value.some(token => !/^(?:h:)?[a-f0-9]{32}$/.test(token))))
        throw new Error("invalid hashed authority history");
      if (key === "bridgeSupersededTokenSeen" && (!value || typeof value !== "object" || Array.isArray(value) ||
          Object.entries(value).some(([token, ordinal]) => !/^(?:h:)?[a-f0-9]{32}$/.test(token) || !Number.isSafeInteger(ordinal) || ordinal < 0)))
        throw new Error("invalid hashed authority history ordinals");
      tokens(value);
    }
  };
  tokens(snapshot);
  if (!snapshot.stateEntries.some(entry => entry.key.startsWith("topology/hosts/")) && !snapshot.leaseFiles.length)
    throw new Error("empty authority population");
  sanitizeAuthorityEvidence(snapshot); // Reject unknown token-like fields before any seed or child spawn.
  return snapshot;
}

// Only imported process evidence is removed. Ownership/incarnation/history and all expiry
// relationships survive. Missing display names are restored with the already pseudonymous id;
// compiled isIdentity requires a string name, but that name is not an authority predicate.
export function rebaseAuthoritySnapshot(snapshot, replayAt) {
  const delta = replayAt - snapshot.collectedAt;
  const removed = { processFields: 0, displayNames: 0 };
  const timeFields = new Set(["startedAt", "updatedAt", "expiresAt", "observedAt", "sourceObservedAt", "sourceUpdatedAt", "sourceExpiresAt", "issuedAt", "voidedAt", "retiredAt", "reloadUntil", "handedOffAt"]);
  const visit = item => {
    if (Array.isArray(item)) return item.map(visit);
    if (!item || typeof item !== "object") return item;
    const output = {};
    for (const [key, value] of Object.entries(item)) {
      if (["pid", "processIncarnation", "mainProcess", "bridgeProcess"].includes(key)) { removed.processFields++; continue; }
      output[key] = timeFields.has(key) && typeof value === "number" ? value + delta : visit(value);
    }
    if (typeof output.id === "string" && ["main", "actor", "agent"].includes(output.kind) && output.name === undefined) {
      output.name = output.id; removed.displayNames++;
    }
    return output;
  };
  return { snapshot: visit(snapshot), rebase: { capturedAt: snapshot.collectedAt, replayAt, deltaMs: delta,
    policy: "uniform wall-clock translation; no TTL extension; history ordinals unchanged", ...removed } };
}

export async function seedAuthoritySnapshot(snapshot, root, release, replayAt) {
  validateAuthoritySnapshot(snapshot, snapshot.site);
  if (fs.existsSync(root)) throw new Error("authority replay destination already exists");
  fs.mkdirSync(root, { mode: 0o700, recursive: true });
  const { snapshot: seeded, rebase } = rebaseAuthoritySnapshot(snapshot, replayAt);
  const loaded = await loadReleaseSymbols(release, seeded.backend === "sqlite" ? ["MeshStore", "SqliteStateStore"] : ["MeshStore"]);
  const entries = Object.fromEntries(seeded.stateEntries.map(entry => [entry.key, entry]));
  const metadata = seeded.stateMetadata;
  const state = { entries, versions: metadata.versions ?? {}, tombstoneOrder: metadata.tombstoneOrder ?? [],
    highWater: Math.max(metadata.highWater ?? 0, ...seeded.stateEntries.map(entry => entry.version)) };
  if (seeded.backend === "sqlite") {
    const store = loaded.SqliteStateStore.openSync(root, MAX_BYTES, MAX_READ, { initialize: "create" });
    try {
      await store.importState(state);
      const actual = store.exportState();
      const versions = { ...state.versions, ...Object.fromEntries(seeded.stateEntries.map(entry => [entry.key, entry.version])) };
      if (!isDeepStrictEqual(actual.entries, state.entries) || !isDeepStrictEqual(actual.versions, versions) ||
          !isDeepStrictEqual(actual.tombstoneOrder, state.tombstoneOrder) || actual.highWater !== state.highWater)
        throw new Error("seed state versions/tombstones/highWater readback mismatch");
    } finally { store.close(); }
  } else fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: metadata.format ?? 2,
    revisionFormat: metadata.revisionFormat ?? 2, ...state }), { mode: 0o600, flag: "wx" });
  for (const entry of seeded.participantFiles) {
    const dir = path.join(root, "participants"); fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(dir, `${entry.key.slice("topology/participants/".length)}.json`),
      JSON.stringify({ format: 1, ...entry }), { mode: 0o600, flag: "wx" });
  }
  for (const lease of seeded.leaseFiles) {
    const dir = path.join(root, "host-leases"); fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(dir, `${hash(lease.id).slice(0, 32)}.json`), JSON.stringify({ format: 1, ...lease }), { mode: 0o600, flag: "wx" });
  }
  for (const entry of seeded.participantFiles) {
    const actual = JSON.parse(fs.readFileSync(path.join(root, "participants", `${entry.key.slice("topology/participants/".length)}.json`), "utf8"));
    if (!isDeepStrictEqual(actual, { format: 1, ...entry })) throw new Error("seed participant file readback mismatch");
  }
  for (const lease of seeded.leaseFiles) {
    const actual = JSON.parse(fs.readFileSync(path.join(root, "host-leases", `${hash(lease.id).slice(0, 32)}.json`), "utf8"));
    if (!isDeepStrictEqual(actual, { format: 1, ...lease })) throw new Error("seed lease readback mismatch");
  }
  const mesh = new loaded.MeshStore(root, MAX_BYTES, MAX_READ, { stateBackend: seeded.backend, lockProtocol: 3 });
  try {
    if (mesh.stateBackend !== seeded.backend) throw new Error("replay backend mismatch");
    for (const entry of seeded.stateEntries) {
      if (!isDeepStrictEqual(mesh.get(entry.key, { fresh: true }), entry))
        throw new Error("seed authority metadata readback mismatch");
    }
  } finally { mesh.closeState(); }
  return { backend: seeded.backend, stateEntries: seeded.stateEntries.length, participantFiles: seeded.participantFiles.length,
    leaseFiles: seeded.leaseFiles.length, cursors: seeded.cursors, rebase, artifacts: loaded.artifacts };
}

// Independent diagnostic of the FIRST guard candidate, never an admission substitute.
// Bounded-history union/order follows the traced release algorithm, not a handpicked root.
export function authorityPredicates(presence, authority, peer, fromHub, reserved = []) {
  const rows = authority.stateEntries.filter(entry => entry.key.startsWith("topology/hosts/"));
  const own = new Set(reserved);
  if (!reserved.length) for (const entry of [...authority.stateEntries, ...(authority.participantFiles ?? [])]) {
    const value = entry.value, link = value.bridgeVia ?? value.remoteHost;
    if (link === peer) continue;
    const ids = entry.key.startsWith("topology/hosts/") ? [value.id, value.rootId, value.identity?.id] :
      entry.key.startsWith("topology/participants/") ? [value.id, value.rootId, value.ownerIdentityId, value.name, value.sessionId] : [];
    for (const id of ids) if (typeof id === "string") own.add(id);
  }
  return presence.hosts.map(({ record }, index) => {
    const sameRootHosts = rows.filter(entry => entry.value.rootId === record.rootId).map(entry => ({
      hostId: entry.value.id, tokenHash: entry.value.incarnationToken ?? null, remoteHost: entry.value.remoteHost ?? null,
      bridgeVia: entry.value.bridgeVia ?? null, history: entry.value.bridgeSupersededTokens ?? [] }));
    const held = [...rows.filter(entry => entry.value.rootId === record.rootId &&
      (entry.value.bridgeVia ?? entry.value.remoteHost) === peer).map(entry => entry.value),
      ...authority.leaseFiles.filter(lease => lease.rootId === record.rootId)];
    const currentTokens = [...new Set(held.map(value => value.incarnationToken).filter(Boolean))];
    const ordinals = new Map(), origins = new Map();
    held.forEach((value, source) => {
      for (const [token, sequence] of Object.entries(value.bridgeSupersededTokenSeen ?? {}))
        ordinals.set(token, Math.min(ordinals.get(token) ?? Infinity, sequence));
      [...new Set(value.bridgeSupersededTokens ?? [])].slice(-16).forEach((token, index) => {
        if (!origins.has(token)) origins.set(token, { source, index });
      });
    });
    const knownHistory = [...origins.keys()].sort((a, b) => {
      const x = ordinals.get(a), y = ordinals.get(b), aa = origins.get(a), bb = origins.get(b);
      return x === undefined ? y === undefined ? bb.source - aa.source || aa.index - bb.index : -1 :
        y === undefined ? 1 : x - y || bb.source - aa.source || aa.index - bb.index;
    }).slice(-16);
    const admittedByHub = fromHub || record.identity.id === record.id && record.rootId === record.id &&
      record.bridgeVia === undefined && /^session:[A-Za-z0-9-]{8,128}$/.test(record.id);
    const eligible = admittedByHub && !(record.remoteHost !== undefined && (!fromHub || record.bridgeVia !== peer || record.remoteHost === peer)) &&
      ![record.id, record.rootId, record.identity.id].some(id => own.has(id));
    const tokenless = !record.incarnationToken && (currentTokens.length > 0 || knownHistory.length > 0);
    const supersededHistory = Boolean(record.incarnationToken && knownHistory.includes(record.incarnationToken));
    return { index, hostId: record.id, rootId: record.rootId, tokenHash: record.incarnationToken ?? null,
      eligible, admittedByHub, tokenless, supersededHistory, predicateKind: tokenless ? "tokenless" : supersededHistory ? "superseded-history" : null,
      currentTokens, knownHistory, sameRootHosts,
      incomingSameRootHosts: presence.hosts.filter(host => host.record.rootId === record.rootId).map(({ record: source }) => ({
        hostId: source.id, tokenHash: source.incarnationToken ?? null, remoteHost: source.remoteHost ?? null,
        bridgeVia: source.bridgeVia ?? null })),
      leaseOnly: held.filter(value => !rows.some(entry => entry.value.id === value.id)).map(value => ({ hostId: value.id, tokenHash: value.incarnationToken ?? null })) };
  });
}

function replayAuthority(mesh) {
  const stateEntries = ["topology/hosts/", "topology/participants/", "topology/handoffs/"].flatMap(prefix => mesh.listAll(prefix, { fresh: true }));
  const participantDir = path.join(mesh.root, "participants");
  const participantFiles = fs.existsSync(participantDir) ? fs.readdirSync(participantDir).filter(name => /^[a-f0-9]{64}\.json$/.test(name))
    .map(name => JSON.parse(fs.readFileSync(path.join(participantDir, name), "utf8"))) : [];
  const dir = path.join(mesh.root, "host-leases");
  const leaseFiles = fs.existsSync(dir) ? fs.readdirSync(dir).filter(name => /^[a-f0-9]{32}\.json$/.test(name))
    .map(name => JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"))) : [];
  const stripProcess = value => {
    if (Array.isArray(value)) return value.map(stripProcess);
    if (!value || typeof value !== "object") return value;
    return Object.fromEntries(Object.entries(value).filter(([key]) => !["pid", "processIncarnation", "mainProcess", "bridgeProcess"].includes(key))
      .map(([key, value]) => [key, stripProcess(value)]));
  };
  return stripProcess({ stateEntries, participantFiles, leaseFiles });
}

export function matchAuthorityRefusal(error, predicates) {
  const candidates = predicates.filter(row => row.eligible && row.predicateKind);
  const match = /^Bridge presence for (\S+) has an older incarnation than persisted authority$/.exec(error?.error ?? "");
  const staleError = Boolean(match && (error?.code === "FABRIC_BRIDGE_STALE_INCARNATION" || error?.source === "hub-cli-stderr"));
  const actualRoot = match?.[1] ?? null;
  const first = candidates[0] ?? null;
  const sameRoot = candidates.filter(row => row.rootId === actualRoot);
  return { actualRoot, staleError, firstCandidate: first, rootMatchesFirst: Boolean(staleError && actualRoot && first?.rootId === actualRoot),
    attribution: !staleError || !actualRoot || !first || first.rootId !== actualRoot ? "unmatched" : sameRoot.length > 1 ? "ambiguous-same-root" : "unique",
    refused: actualRoot ? sameRoot : [], ambiguity: sameRoot.length > 1 ? "wire error identifies root, not sibling host; first real-order guard candidate is reported separately" : null };
}

/** Byte-transparent transport for the REAL hub CLI. Capture before forwarding; no frame edits. */
async function authorityProxy() {
  const config = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
  const loaded = await loadReleaseSymbols(config.hubRelease, ["MeshStore"]);
  const hub = new loaded.MeshStore(config.hubMesh, MAX_BYTES, MAX_READ, { stateBackend: "sqlite", lockProtocol: 3 });
  process.env.PI_FABRIC_RELEASE_SHA = config.workSha; process.env.PI_FABRIC_BUILD_SHA = config.workSha;
  const workLoaded = await loadReleaseSymbols(config.workRelease, ["MeshStore"]);
  process.env.PI_FABRIC_RELEASE_SHA = config.hubSha; process.env.PI_FABRIC_BUILD_SHA = config.hubSha;
  const work = new workLoaded.MeshStore(config.workMesh, MAX_BYTES, MAX_READ, { stateBackend: "file", lockProtocol: 3 });
  let ordinal = 0, bytes = 0, agentProved = false;
  const pending = new Map();
  const save = record => {
    const text = JSON.stringify(sanitizeAuthorityEvidence(record)) + "\n";
    bytes += Buffer.byteLength(text);
    if (bytes > 32 * 1024 * 1024) throw new Error("authority wire evidence budget exceeded");
    fs.appendFileSync(config.trace, text, { mode: 0o600 });
  };
  const args = [path.join(config.workRelease, "bin/mesh-bridge"), "agent", "--mesh", config.workMesh,
    "--peer", config.hubName, "--lock-protocol", "3"];
  const child = spawn(process.execPath, args, { stdio: ["pipe", "pipe", "inherit"],
    env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "file",
      PI_FABRIC_RELEASE_SHA: config.workSha, PI_FABRIC_BUILD_SHA: config.workSha } });
  save({ type: "transport", role: "harness-transparent-proxy", native: nativeProcessEvidence(),
    pid: process.pid, childPid: child.pid, childNative: nativeProcessEvidence(child.pid), workSha: config.workSha,
    argv: [process.execPath, ...args], artifacts: { hub: loaded.artifacts, work: workLoaded.artifacts } });
  // Line inspection is observational only. Original byte buffers go to the compiled agent/hub.
  const inspect = (stream, onLine) => {
    let buffer = "";
    stream.on("data", chunk => {
      buffer += chunk.toString("utf8");
      if (Buffer.byteLength(buffer) > 4 * 1024 * 1024) throw new Error("authority frame budget exceeded");
      for (;;) {
        const newline = buffer.indexOf("\n"); if (newline < 0) break;
        const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
        onLine(JSON.parse(line));
      }
    });
  };
  inspect(process.stdin, frame => {
    if (frame.op === "presence") { ordinal++; save({ type: "pass-start", pass: ordinal, id: frame.id }); }
    const info = { pass: ordinal, op: frame.op };
    if (frame.op === "mirror") {
      const authority = replayAuthority(work);
      info.predicates = authorityPredicates(frame.args, authority, config.hubName, true);
      save({ type: "request", direction: "hub-to-work", id: frame.id, ...info, presence: frame.args, authority });
    }
    pending.set(frame.id, info);
  });
  inspect(child.stdout, frame => {
    if (!agentProved) {
      const dir = path.join(config.workMesh, ".writer-census");
      const census = fs.existsSync(dir) ? fs.readdirSync(dir).filter(name => name.endsWith(".json")).flatMap(name => {
        try { const value = JSON.parse(fs.readFileSync(path.join(dir, name), "utf8")); return value.pid === child.pid ? [value] : []; } catch { return []; }
      }) : [];
      save({ type: "agent-identity", role: "product-work-agent", process: nativeProcessEvidence(child.pid), census,
        expectedSha: config.workSha, backend: work.stateBackend, lockProtocol: work.lockProtocol,
        bridgeSha256: hash(fs.readFileSync(path.join(config.workRelease, "dist/mesh-bridge.js"))) });
      agentProved = true;
    }
    const info = pending.get(frame.id);
    if (!info) return;
    pending.delete(frame.id);
    if (info.op === "presence" && frame.ok) {
      const authority = replayAuthority(hub);
      save({ type: "presence", direction: "work-to-hub", id: frame.id, ...info, presence: frame.result, authority,
        // Reservations/handoffs are evaluated by the real compiled hub, not this diagnostic.
        predicates: authorityPredicates(frame.result, authority, config.workName, false) });
    }
    if (info.op === "mirror") save({ type: "response", direction: "hub-to-work", ...info, frame,
      authorityAfter: { work: replayAuthority(work), hub: replayAuthority(hub) },
      ...(frame.ok ? {} : { refusal: matchAuthorityRefusal(frame, info.predicates) }) });
  });
  process.stdin.pipe(child.stdin); child.stdout.pipe(process.stdout);
  child.stdin.on("error", () => undefined);
  child.once("error", error => { process.stderr.write(`${error.message}\n`); });
  const closed = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  const absoluteDeadline = Number(process.env.PI_MIXED_CANARY_DEADLINE_AT);
  let hardStop;
  const watchdog = Number.isFinite(absoluteDeadline) && absoluteDeadline > 0 ? setTimeout(() => {
    child.kill("SIGTERM"); hardStop = setTimeout(() => child.kill("SIGKILL"), 2_000);
  }, Math.max(1, absoluteDeadline - Date.now())) : undefined;
  process.stdin.once("end", () => child.stdin.end());
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => child.kill(signal));
  const exit = await closed;
  clearTimeout(watchdog); clearTimeout(hardStop);
  hub.closeState(); work.closeState();
  save({ type: "transport-exit", ...exit });
  process.exitCode = exit.code ?? 1;
}

async function main() {
  let mesh, directory, control, identity, native, artifacts, stopping, producer = false, leaseWatcher;
  let evidencePath, evidenceBytes = 0, lastLeaseAt = -1, evidenceFailure = false;
  const absoluteDeadline = Number(process.env.PI_MIXED_CANARY_DEADLINE_AT);
  let watchdog;
  if (Number.isFinite(absoluteDeadline) && absoluteDeadline > 0) watchdog = setTimeout(() => {
    const force = setTimeout(() => process.exit(2), 2_000);
    void stop().finally(() => { clearTimeout(force); process.exit(2); });
  }, Math.max(1, absoluteDeadline - Date.now()));
  const record = row => {
    if (!evidencePath) return;
    let line;
    try { line = JSON.stringify(sanitizeAuthorityEvidence({ ...row, at: Date.now(), process: nativeProcessEvidence() })) + "\n"; }
    catch { evidenceFailure = true; throw new Error("authority evidence rejected"); }
    evidenceBytes += Buffer.byteLength(line);
    if (evidenceBytes > 16 * 1024 * 1024) throw new Error("producer evidence budget exceeded");
    fs.appendFileSync(evidencePath, line, { mode: 0o600 });
  };
  const expected = new Map(), entries = new Map(), sent = [];
  let invalid = 0, duplicateCalls = 0;
  const events = topic => mesh.read({ topic, after: 0, limit: MAX_READ });
  const presence = ids => ids.map(id => {
    const host = mesh.get(hostKey(id), { fresh: true })?.value ?? null;
    const leaseFile = path.join(mesh.root, "host-leases", `${hash(id).slice(0, 32)}.json`);
    let lease = null;
    try { lease = JSON.parse(fs.readFileSync(leaseFile, "utf8")); } catch (error) { if (error.code !== "ENOENT") throw error; }
    const participant = directory.get(id, Date.now(), { fresh: true }) ?? null;
    const row = mesh.get(participantKey(id), { fresh: true })?.value ?? null;
    const owner = directory.mirroredControlOwner(id, id, id) ?? null;
    return { id, host, lease, leaseFile, participant, row, owner };
  });
  const stop = () => stopping ??= (async () => {
    clearTimeout(watchdog);
    leaseWatcher?.close();
    await control?.close();
    await directory?.close();
    mesh?.closeState();
  })();
  const dispatch = async input => {
    switch (input.op) {
      case "seed": return await seedAuthoritySnapshot(input.snapshot, input.mesh, input.release, input.replayAt);
      case "observe": return { readback: presence(input.ids), authority: replayAuthority(mesh), at: Date.now(), process: nativeProcessEvidence() };
      case "start": {
        if (mesh) throw new Error("worker already started");
        producer = input.producer === true;
        const loaded = await loadReleaseSymbols(input.release, producer ? ["MeshStore", "ParticipantDirectory"] : undefined);
        artifacts = loaded.artifacts;
        const backend = producer ? input.backend : "sqlite";
        if (!["sqlite", "file"].includes(backend)) throw new Error("invalid producer backend");
        if (!producer) loaded.SqliteStateStore.openSync(input.mesh, MAX_BYTES, MAX_READ, { initialize: "create" }).close();
        else if (!fs.existsSync(path.join(input.mesh, backend === "sqlite" ? "state.db" : "state.json")))
          throw new Error("producer requires an existing seeded mesh");
        const storeOptions = { stateBackend: backend, lockProtocol: 3, lockTimeoutMs: 20_000, staleLockMs: 60_000 };
        // The historical 41a API rejects protocol 3 before doing any I/O. Its own
        // bridge also only supports 1|2; do not pretend it spoke protocol 3.
        try { mesh = new loaded.MeshStore(input.mesh, MAX_BYTES, MAX_READ, storeOptions); }
        catch (error) {
          if (producer || !/mesh\.lockProtocol must be 1 or 2/.test(error.message)) throw error;
          mesh = new loaded.MeshStore(input.mesh, MAX_BYTES, MAX_READ, { ...storeOptions, lockProtocol: 2 });
        }
        identity = { id: input.rootId, name: "main", kind: "main", sessionId: input.name };
        if (producer) {
          evidencePath = input.evidencePath;
          if (!evidencePath) throw new Error("missing producer evidence path");
          fs.writeFileSync(evidencePath, "", { mode: 0o600, flag: "wx" });
          // Observe the actual compiled batch, preserving arguments, result, and custody.
          // Applied deletes plus exact before/after rows provide causal compaction evidence.
          const writeBatch = mesh.writeBatch.bind(mesh);
          mesh.writeBatch = async (...args) => {
            const before = replayAuthority(mesh), beganAt = Date.now();
            const result = await writeBatch(...args);
            const after = replayAuthority(mesh);
            record({ type: "compiled-directory-batch", artifacts, before, after, beganAt,
              operations: args[0].ops.map(op => ({ kind: op.kind, key: op.key, ifVersion: op.ifVersion })), results: result });
            return result;
          };
        }
        directory = new loaded.ParticipantDirectory(mesh, {
          enabled: true, hostId: identity.id, rootId: identity.id, identity,
          heartbeatMs: 1_000, leaseMs: 15_000, reapDeadHosts: false,
        });
        // There is no public directory.upsert in these releases. registerSource + start/refresh
        // executes its real fenced host/lease + root participant upsert path.
        const startedAt = Date.now();
        directory.registerSource(() => [directory.root({ id: identity.id, sessionId: input.name,
          status: "idle", startedAt, updatedAt: Date.now(), pendingMessages: 0 }, true, input.name,
          producer ? { role: input.role } : undefined)]);
        await directory.start();
        await directory.refresh();
        const [readback] = presence([identity.id]);
        if (!readback.host || !readback.lease || !readback.participant || !readback.row ||
            readback.host.remoteHost !== undefined || readback.lease.expiresAt <= Date.now() ||
            readback.row.ownerIncarnation !== directory.ownerIncarnation) throw new Error("native registration readback failed");
        native = { id: identity.id, name: input.name, ownerIncarnation: directory.ownerIncarnation };
        if (producer) {
          const observeLease = () => {
            const [row] = presence([identity.id]);
            if (row.lease?.updatedAt > lastLeaseAt) {
              lastLeaseAt = row.lease.updatedAt;
              record({ type: "native-lease-observation", readback: row, artifacts });
            }
          };
          // Native lease rename/write events drive observation; no synthetic refresh/tick.
          leaseWatcher = fs.watch(path.join(mesh.root, "host-leases"), observeLease);
          leaseWatcher.on("error", error => { record({ type: "observer-error", error: error.message }); });
          observeLease();
          return { native, readback, artifacts, process: nativeProcessEvidence(),
            releaseSha: process.env.PI_FABRIC_RELEASE_SHA, buildSha: process.env.PI_FABRIC_BUILD_SHA,
            lockProtocol: mesh.lockProtocol, stateBackend: mesh.stateBackend, role: "product-native-producer" };
        }
        control = new loaded.FabricControlPlane(mesh, identity, {
          enabled: true, hostId: identity.id, ownerIncarnation: directory.ownerIncarnation,
          acknowledgementTimeoutMs: 20_000, bridgeTimeoutMs: 20_000,
          readMirroredOwner: (...args) => directory.mirroredControlOwner(...args),
        });
        control.start((command, from, _signal, verification) => {
          const marker = command.data?.marker, plan = expected.get(marker);
          if (!plan || verification !== "bridge" || from.id !== plan.from || command.targetId !== identity.id ||
              command.operation !== "followUp" || command.message !== marker) {
            invalid++;
            return { accepted: false, error: "unauthenticated or unexpected canary followUp", notRun: true };
          }
          const previous = entries.get(marker);
          if (previous) duplicateCalls++;
          entries.set(marker, { marker, from: from.id, targetId: command.targetId,
            commandId: command.commandId, verification, calls: (previous?.calls ?? 0) + 1, acceptedAt: Date.now() });
          return { accepted: true, messageId: `ack:${marker}`, triggered: true, result: { marker } };
        });
        return { native, readback, artifacts, lockProtocol: mesh.lockProtocol, stateBackend: mesh.stateBackend };
      }
      case "expect":
        for (const plan of input.messages) {
          if (expected.has(plan.marker)) throw new Error("duplicate expectation");
          expected.set(plan.marker, plan);
        }
        return { expected: expected.size };
      case "presence": return presence(input.ids);
      case "waitPresence": {
        // Filesystem notifications carry cross-process progress; a 2s bounded safety net
        // covers missed/coalesced WAL notifications. The timer never grants presence.
        const ready = rows => rows.every((row, index) => {
          const condition = input.conditions[index];
          if (condition.absent) return row.host === null && row.lease === null;
          return row.host && row.lease && row.participant && row.row &&
            (condition.remote === undefined || row.owner?.remoteHost === condition.remote) &&
            (condition.token === undefined || row.host.incarnationToken === condition.token && row.lease.incarnationToken === condition.token) &&
            (!condition.tokenless || row.host.incarnationToken === undefined && row.lease.incarnationToken === undefined);
        });
        return await new Promise((resolve, reject) => {
          let watcher, timer, safety, finished = false, last;
          const finish = (error, rows) => {
            if (finished) return;
            finished = true; watcher?.close(); clearTimeout(timer); clearInterval(safety);
            error ? reject(error) : resolve(rows);
          };
          const check = () => {
            try { last = presence(input.ids); if (ready(last)) finish(null, last); }
            catch (error) { finish(error); }
          };
          watcher = fs.watch(mesh.root, { recursive: true }, check);
          watcher.on("error", error => finish(error));
          timer = setTimeout(() => finish(new Error(`presence deadline: ${JSON.stringify(last)}`)), 25_000);
          safety = setInterval(check, 2_000);
          check();
        });
      }
      case "send": {
        const { target, marker } = input;
        const owner = directory.mirroredControlOwner(target.id, target.id, target.id);
        if (owner?.remoteHost !== target.name || directory.ownerIncarnation !== undefined && owner.ownerIncarnation !== target.ownerIncarnation)
          throw new Error(`no authentic mirrored owner for ${target.name}`);
        const receipt = await control.request(target.id, target.id, "followUp", {
          message: marker, data: { marker }, ownerIncarnation: owner.ownerIncarnation,
        }, target.id, { routedRemoteHost: owner.remoteHost, idempotencyKey: `mixed-release:${marker}`, timeoutMs: 20_000 });
        const commands = events("fabric.control.command").filter(event => event.from.id === identity.id &&
          event.to === target.id && event.data?.data?.marker === marker);
        const commandIds = [...new Set(commands.map(event => event.data.commandId))];
        const acks = events("fabric.control.ack").filter(event => commandIds.includes(event.data?.commandId) &&
          event.to === identity.id && event.from.id === target.id && event.verification === "bridge" &&
          event.data?.targetId === target.id && event.data?.messageId === `ack:${marker}` && event.data?.accepted === true);
        if (!receipt.acknowledged || receipt.routed !== "mesh" || receipt.messageId !== `ack:${marker}` ||
            commandIds.length !== 1 || acks.length !== 1) throw new Error(`uncorrelated or unauthenticated ACK: ${marker}`);
        const result = { marker, target: target.id, commandId: commandIds[0], receipt,
          ack: { id: acks[0].id, from: acks[0].from.id, to: acks[0].to, verification: acks[0].verification,
            commandId: acks[0].data.commandId, bridge: acks[0].data.bridge }, at: Date.now() };
        sent.push(result);
        return result;
      }
      case "historyFollowUp": {
        // No harness admission check: the public compiled API owns route/epoch fencing.
        const { targetId, marker } = input;
        const observed = presence([targetId])[0];
        const actualOwner = directory.mirroredControlOwner(targetId, targetId, targetId) ?? null;
        const beganAt = Date.now(), began = performance.now();
        let receipt, error;
        try {
          receipt = await control.request(targetId, targetId, "followUp", {
            message: marker, data: { marker }, ownerIncarnation: actualOwner?.ownerIncarnation,
          }, targetId, { routedRemoteHost: actualOwner?.remoteHost,
            idempotencyKey: `mixed-release-history:${marker}`, timeoutMs: 20_000 });
        } catch (caught) {
          // Serialize only the actual request rejection, never an RPC/precheck failure.
          error = { source: "FabricControlPlane.request", name: caught.name, code: caught.code, targetId: caught.targetId,
            message: caught.message, stack: caught.stack, notRun: caught.notRun, notDelivered: caught.notDelivered };
        }
        const endedAt = Date.now(), elapsedMs = performance.now() - began;
        const commandEvents = events("fabric.control.command").filter(event => event.from.id === identity.id &&
          event.to === targetId && event.data?.data?.marker === marker);
        const ids = new Set(commandEvents.map(event => event.data.commandId));
        return { marker, source: identity.id, intendedTarget: targetId, actualOwner, observed,
          publicRequestInvoked: true, beganAt, endedAt, elapsedMs, timeoutMs: 20_000, ackGraceMs: 15_000,
          ...(error ? { error } : { receipt }), commandEvents,
          ackEvents: events("fabric.control.ack").filter(event => ids.has(event.data?.commandId)) };
      }
      case "tally": return { native, expected: [...expected.keys()], entries: [...entries.values()], invalid, duplicateCalls, sent,
        commandEvents: events("fabric.control.command"), ackEvents: events("fabric.control.ack") };
      case "stop": await stop(); return { stopped: true };
      default: throw new Error(`unknown worker operation: ${input.op}`);
    }
  };
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  lines.on("line", line => {
    let input;
    try { input = JSON.parse(line); } catch { process.stderr.write("invalid JSONL request\n"); return; }
    void dispatch(input).then(result => {
      if (evidenceFailure && input.op !== "stop") throw new Error("authority evidence rejected");
      process.stdout.write(`${JSON.stringify({ id: input.id, ok: true, result: producer ? sanitizeAuthorityEvidence(result) : result })}\n`);
      if (input.op === "stop") { lines.close(); process.stdin.destroy(); }
    }).catch(error => {
      if (producer && input.op !== "stop") evidenceFailure = true;
      process.stdout.write(`${JSON.stringify({ id: input.id, ok: false, error: producer ? "authority evidence rejected" : error.stack ?? String(error) })}\n`);
    });
  });
  lines.once("close", () => { void stop().catch(error => { process.stderr.write(`${error.stack}\n`); process.exitCode = 1; }); });
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => {
    void stop().finally(() => { lines.close(); process.stdin.destroy(); });
  });
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const entry = process.argv[2] === "authority-proxy" ? authorityProxy : main;
  entry().catch(error => { process.stderr.write(`${error.stack}\n`); process.exitCode = 1; });
}
