import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { writeFileAtomic } from "../core/atomic-write.js";
import { validateMeshTopic, type MeshEvent, type MeshIdentity, type MeshStore } from "./store.js";

/**
 * Scoped external grants: a bearer token that lets an outside process append
 * events to exactly one mesh topic. Only the token's SHA-256 is stored, in a
 * private `grants.json` beside the mesh state. Every mutation runs under the
 * mesh lock through {@link MeshStore.transact}, so use counts decrement
 * atomically with the event append. Authority is local file access to the mesh
 * root; the grant narrows what a token holder without that access can do.
 */

export const MESH_GRANT_TOKEN_ENV = "PI_FABRIC_MESH_TOKEN";
export const MESH_EXTERNAL_DATA_MAX_BYTES = 64 * 1024;
export const MESH_GRANT_MIN_TTL_MS = 60_000;
export const MESH_GRANT_MAX_TTL_MS = 30 * 24 * 60 * 60 * 1_000;
export const MESH_GRANT_MAX_USES = 10_000;
const MAX_ACTIVE_GRANTS = 1_000;
const MAX_GRANT_FILE_BYTES = 4 * 1024 * 1024;
const GRANT_ID_PATTERN = /^[a-zA-Z0-9-]{1,64}$/;
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const KIND_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;

export interface MeshGrantInfo {
  grantId: string;
  topic: string;
  kind?: string;
  createdAt: number;
  expiresAt: number;
  /** Remaining uses. */
  uses: number;
  maxUses: number;
  createdBy: MeshIdentity;
}

interface MeshGrantRecord extends MeshGrantInfo {
  /** Hex SHA-256 of the token; the token itself is never stored. */
  hash: string;
  /** The minting store's event ceiling, so the CLI honours a configured mesh. */
  maxEventBytes: number;
}

interface MeshGrantFile {
  format: 1;
  grants: MeshGrantRecord[];
}

const grantsPath = (store: Pick<MeshStore, "root">): string => path.join(store.root, "grants.json");

const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : undefined;

const hashToken = (token: string): string => createHash("sha256").update(token, "utf8").digest("hex");

const isGrantRecord = (value: unknown): value is MeshGrantRecord => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const grant = value as Partial<MeshGrantRecord>;
  return typeof grant.grantId === "string" && typeof grant.topic === "string" &&
    typeof grant.hash === "string" && /^[0-9a-f]{64}$/.test(grant.hash) &&
    Number.isSafeInteger(grant.expiresAt) && Number.isSafeInteger(grant.uses) &&
    Number.isSafeInteger(grant.maxUses) && Number.isSafeInteger(grant.maxEventBytes) &&
    (grant.kind === undefined || typeof grant.kind === "string");
};

const readGrants = (store: Pick<MeshStore, "root">): MeshGrantFile => {
  let serialized: string;
  try {
    const stat = fs.statSync(grantsPath(store));
    if (stat.size > MAX_GRANT_FILE_BYTES) throw new Error(`exceeds ${MAX_GRANT_FILE_BYTES} bytes`);
    serialized = fs.readFileSync(grantsPath(store), "utf8");
  } catch (error) {
    if (errorCode(error) === "ENOENT") return { format: 1, grants: [] };
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to read Fabric mesh grants: ${message}`);
  }
  try {
    const parsed = JSON.parse(serialized) as Partial<MeshGrantFile>;
    if (parsed?.format === 1 && Array.isArray(parsed.grants) && parsed.grants.every(isGrantRecord)) {
      return { format: 1, grants: parsed.grants };
    }
  } catch { /* reported below */ }
  // Fail closed: damaged grants never authorize and are never silently reset.
  throw new Error("Failed to read Fabric mesh grants: invalid format");
};

const writeGrants = (store: Pick<MeshStore, "root">, grants: MeshGrantRecord[]): void => {
  const serialized = JSON.stringify({ format: 1, grants }, null, 2);
  if (Buffer.byteLength(serialized, "utf8") > MAX_GRANT_FILE_BYTES) {
    throw new Error(`Fabric mesh grants exceed ${MAX_GRANT_FILE_BYTES} bytes`);
  }
  writeFileAtomic(grantsPath(store), serialized, { mode: 0o600 });
};

const info = (grant: MeshGrantRecord): MeshGrantInfo => ({
  grantId: grant.grantId,
  topic: grant.topic,
  ...(grant.kind !== undefined ? { kind: grant.kind } : {}),
  createdAt: grant.createdAt,
  expiresAt: grant.expiresAt,
  uses: grant.uses,
  maxUses: grant.maxUses,
  createdBy: grant.createdBy,
});

const boundedInteger = (value: unknown, name: string, min: number, max: number): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer from ${min} to ${max}`);
  }
  return value;
};

export const validateMeshKind = (kind: string): string => {
  if (!KIND_PATTERN.test(kind)) throw new Error(`Invalid Fabric mesh event kind: ${kind}`);
  return kind;
};

/** Mints a grant; the token is returned once and never persisted. */
export async function createMeshGrant(
  store: Pick<MeshStore, "root" | "maxEventBytes" | "transact">,
  input: { topic: string; kind?: string; ttlMs: number; uses?: number; createdBy: MeshIdentity },
  now = Date.now(),
): Promise<{ grant: MeshGrantInfo; token: string }> {
  const ttlMs = boundedInteger(input.ttlMs, "ttlMs", MESH_GRANT_MIN_TTL_MS, MESH_GRANT_MAX_TTL_MS);
  const uses = boundedInteger(input.uses ?? 1, "uses", 1, MESH_GRANT_MAX_USES);
  validateMeshTopic(input.topic);
  if (input.kind !== undefined) validateMeshKind(input.kind);
  const token = randomBytes(32).toString("base64url");
  const record: MeshGrantRecord = {
    grantId: randomUUID(),
    topic: input.topic,
    ...(input.kind !== undefined ? { kind: input.kind } : {}),
    createdAt: now,
    expiresAt: now + ttlMs,
    uses,
    maxUses: uses,
    createdBy: structuredClone(input.createdBy),
    hash: hashToken(token),
    maxEventBytes: store.maxEventBytes,
  };
  await store.transact(() => {
    const grants = readGrants(store).grants.filter((grant) => grant.expiresAt > now);
    if (grants.length >= MAX_ACTIVE_GRANTS) {
      throw new Error(`At most ${MAX_ACTIVE_GRANTS} mesh grants may be active`);
    }
    writeGrants(store, [...grants, record]);
  });
  return { grant: info(record), token };
}

export async function revokeMeshGrant(
  store: Pick<MeshStore, "root" | "transact">,
  grantId: string,
  now = Date.now(),
): Promise<{ revoked: boolean }> {
  if (!GRANT_ID_PATTERN.test(grantId)) throw new Error(`Invalid Fabric mesh grant id: ${grantId}`);
  return store.transact(() => {
    const grants = readGrants(store).grants;
    const kept = grants.filter((grant) => grant.grantId !== grantId && grant.expiresAt > now);
    const revoked = grants.some((grant) => grant.grantId === grantId);
    if (kept.length !== grants.length) writeGrants(store, kept);
    return { revoked };
  });
}

/** Unexpired grants, never tokens or hashes. */
export function listMeshGrants(store: Pick<MeshStore, "root">, now = Date.now()): MeshGrantInfo[] {
  return readGrants(store).grants
    .filter((grant) => grant.expiresAt > now)
    .sort((left, right) => left.createdAt - right.createdAt || left.grantId.localeCompare(right.grantId))
    .map(info);
}

/**
 * Appends one untrusted external event authorized by `token`. The token hash
 * is compared in constant time against every stored grant, and the use count
 * decrements in the same locked write as the append.
 */
export async function postWithMeshGrant(
  store: Pick<MeshStore, "root" | "transact">,
  input: { token: string; topic?: string; kind?: string; data?: unknown },
  now = Date.now(),
): Promise<{ event: MeshEvent; grant: MeshGrantInfo }> {
  const presented = Buffer.from(hashToken(input.token), "hex");
  if (input.kind !== undefined) validateMeshKind(input.kind);
  let dataBytes = 0;
  if (input.data !== undefined) {
    const serialized = JSON.stringify(input.data);
    if (serialized === undefined) throw new Error("External mesh data must be JSON-serializable");
    dataBytes = Buffer.byteLength(serialized, "utf8");
  }
  if (dataBytes > MESH_EXTERNAL_DATA_MAX_BYTES) {
    throw new Error(`External mesh data exceeds ${MESH_EXTERNAL_DATA_MAX_BYTES} bytes`);
  }
  return store.transact((append) => {
    const grants = readGrants(store).grants;
    let match: MeshGrantRecord | undefined;
    for (const grant of grants) {
      // Compare every candidate so timing does not depend on the match position.
      if (timingSafeEqual(presented, Buffer.from(grant.hash, "hex")) && match === undefined) match = grant;
    }
    if (!match || !TOKEN_PATTERN.test(input.token)) throw new Error("Fabric mesh grant token is not valid or was revoked");
    if (match.expiresAt <= now) throw new Error("Fabric mesh grant has expired");
    if (match.uses <= 0) throw new Error("Fabric mesh grant has no uses left");
    if (input.topic !== undefined && input.topic !== match.topic) {
      throw new Error(`Fabric mesh grant does not cover topic ${input.topic}`);
    }
    if (match.kind !== undefined && input.kind !== undefined && input.kind !== match.kind) {
      throw new Error(`Fabric mesh grant does not cover kind ${input.kind}`);
    }
    const kind = input.kind ?? match.kind ?? "external";
    const event = append({
      topic: match.topic,
      kind,
      from: { id: `external:${match.grantId}`, name: "external", kind: "agent" },
      ...(input.data !== undefined ? { data: input.data } : {}),
      origin: "external",
      untrusted: true,
      grantId: match.grantId,
      maxEventBytes: match.maxEventBytes,
    });
    const updated: MeshGrantRecord = { ...match, uses: match.uses - 1 };
    writeGrants(store, grants.map((grant) => grant === match ? updated : grant));
    return { event, grant: info(updated) };
  });
}

const shellQuote = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;

/**
 * The argv prefix that runs this package's `pi-fabric` CLI. The module may be
 * bundled into a chunk or an entry, so probe the nearby layouts. Source
 * checkouts run the TypeScript entry through Bun.
 */
export const meshCliArgv = (moduleUrl = import.meta.url): string[] => {
  const directory = path.dirname(fileURLToPath(moduleUrl));
  for (const base of [directory, path.dirname(directory), path.dirname(path.dirname(directory))]) {
    for (const extension of [".js", ".ts"]) {
      const candidate = path.join(base, "cli", `index${extension}`);
      if (!fs.existsSync(candidate)) continue;
      if (extension === ".ts") return ["bun", candidate];
      const runtime = path.basename(process.execPath).replace(/\.exe$/i, "");
      return [runtime === "node" || runtime === "bun" ? process.execPath : "node", candidate];
    }
  }
  return ["pi-fabric"];
};

const powershellQuote = (value: string): string => `'${value.replaceAll("'", "''")}'`;

/**
 * A ready-to-run `mesh post` line with the token in the environment, not argv:
 * POSIX shell syntax, or PowerShell on Windows where no POSIX shell is assumed.
 */
export const meshPostCommand = (
  argv: readonly string[],
  input: { root: string; token: string; kind?: string },
  platform: NodeJS.Platform = process.platform,
): string => {
  const quote = platform === "win32" ? powershellQuote : shellQuote;
  const [program = "pi-fabric", ...rest] = argv;
  const invocation = [
    ...(platform === "win32" ? ["&"] : []),
    quote(program),
    ...rest.map(quote),
    "mesh", "post", "--root", quote(input.root),
    ...(input.kind !== undefined ? ["--kind", quote(input.kind)] : []),
    "--data", quote("{}"),
  ].join(" ");
  return platform === "win32"
    ? `$env:${MESH_GRANT_TOKEN_ENV}=${quote(input.token)}; ${invocation}`
    : `${MESH_GRANT_TOKEN_ENV}=${quote(input.token)} ${invocation}`;
};

/**
 * Wraps a durable task script so its exit publishes one notify event through
 * a single-use grant. The original command runs in a subshell, so its own
 * `exit` or traps cannot skip the trailer, and the trailer re-exits with the
 * command's status; a failed post never changes it.
 */
export const wrapDurableNotifyScript = (
  command: string,
  notify: { argv: readonly string[]; root: string; token: string; kind: string; taskId: string; description?: string },
): string => {
  const prefix = JSON.stringify({
    taskId: notify.taskId,
    ...(notify.description !== undefined ? { description: notify.description } : {}),
  }).slice(0, -1);
  return [
    "(",
    command,
    ")",
    "__pi_fabric_status=$?",
    `__pi_fabric_data=${shellQuote(prefix)}`,
    [
      `${MESH_GRANT_TOKEN_ENV}=${shellQuote(notify.token)}`,
      ...notify.argv.map(shellQuote),
      "mesh", "post", "--root", shellQuote(notify.root), "--kind", shellQuote(notify.kind),
      `--data "\${__pi_fabric_data},\\"exitCode\\":\${__pi_fabric_status}}"`,
      "</dev/null >/dev/null 2>&1 || true",
    ].join(" "),
    'exit "$__pi_fabric_status"',
    "",
  ].join("\n");
};
