import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { readHostPolicy } from "../host-policy.js";
import type { MeshStore } from "../mesh/store.js";
// Already in MeshStore's static backend closure; no new optional engine is loaded here.
import { SqliteStateStore, type SqliteFileIdentity } from "../mesh/state-sqlite.js";

export const SLOT_PROTOCOL = "slot-admission-v1" as const;
export const SLOT_GAP = "smarty-dev#7991" as const;
export type SlotRefusalReason = "required" | "invalid" | "stale" | "foreign-root" | "occupied"
  | "frozen" | "authority-unavailable" | "unsupported-authority";

export class ActorSlotClaimRefusedError extends Error {
  readonly code = "FABRIC_ACTOR_SLOT_CLAIM_REFUSED";
  readonly details: { reason: SlotRefusalReason; issue?: string };
  constructor(readonly reason: SlotRefusalReason, message: string, issue?: string) {
    super(message);
    this.name = "ActorSlotClaimRefusedError";
    this.details = { reason, ...(issue ? { issue } : {}) };
  }
}

export interface SlotRoute {
  repository: string;
  /** Exact base pull receiver; its canonical shard suffixes are also protected. */
  topic: string;
  maxSlot: number;
  /** Retained denominator spellings. Omission permits every denominator through maxSlot. */
  denominators?: number[];
}

/** Published ONLY in root-owned host policy, never in fabric.json, an env var or mesh KV. */
export interface SlotAuthorityBinding {
  protocol: typeof SLOT_PROTOCOL;
  meshRoot: string;
  registryRoots: string[];
  /** Trusted authenticated Main roots. Residents must share this enrollment, never infer it from claims. */
  rootIds: string[];
  authorityGeneration: string;
  hubRootId: string;
  physicalHostIdentity: string;
  routesDigest: string;
  /** Absent only in a trusted installer plan, before the one-shot fresh initializer. */
  databaseFileIdentity?: SqliteFileIdentity;
}

export interface SlotStatus {
  protocol: typeof SLOT_PROTOCOL;
  enforcement: "enforced" | "unenforced" | "blocked";
  reason?: "nats-backend" | "existing-authority" | "not-configured" | "unsupported-authority" | "authority-unavailable";
  gap?: typeof SLOT_GAP;
  backend: string;
  authorityGeneration?: string;
  hubRootId?: string;
  routesDigest?: string;
  meshRoot?: string;
  databaseFileIdentity?: SqliteFileIdentity;
  phase?: "enforce";
  issue?: "smarty-dev#13036";
}

const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const positive = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
export const slotPhysicalHostIdentity = (): string => hash(fs.readFileSync("/etc/machine-id", "utf8").trim());
export const slotRoutesDigest = (routes: readonly SlotRoute[]): string => hash(slotBindingSeal(routes));
/** Installer JSON formatting/property order must not change the meaning of a binding. */
export const slotBindingSeal = (binding: unknown): string => {
  const sort = (value: unknown): unknown => Array.isArray(value) ? value.map(sort) : object(value)
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, sort(value[key])])) : value;
  return JSON.stringify(sort(binding));
};
const sameFileIdentity = (left: SqliteFileIdentity | undefined, right: SqliteFileIdentity): boolean =>
  !!left && left.canonicalPath === right.canonicalPath && left.device === right.device && left.inode === right.inode;

function refuse(message: string): never { throw new ActorSlotClaimRefusedError("authority-unavailable", message); }
const existing = (): never => {
  throw new ActorSlotClaimRefusedError("unsupported-authority",
    "Existing SQLite authority cannot be imported or enabled; smarty-dev#13036", "smarty-dev#13036");
};

/** Reject path aliases, including symlink ancestry. Missing final components are allowed for fresh registries. */
export const assertSlotCanonicalPath = (file: string): void => {
  if (!path.isAbsolute(file) || path.resolve(file) !== file) refuse("Slot authority paths must be canonical absolute paths");
  let current = path.parse(file).root;
  for (const part of file.slice(current.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    try {
      if (fs.lstatSync(current).isSymbolicLink()) refuse(`Slot authority path is a symlink: ${current}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
};

/** Resolve aliases only to FIND a trusted expectation, never to authorize the alias itself. */
const expectationPath = (file: string): string => {
  let current = path.resolve(file);
  const missing: string[] = [];
  for (;;) {
    try { return path.join(fs.realpathSync(current), ...missing); }
    catch (error) {
      if (!["ENOENT", "ENOTDIR"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
      const parent = path.dirname(current);
      if (parent === current) return path.resolve(file);
      missing.unshift(path.basename(current)); current = parent;
    }
  }
};

interface Configuration { routes: SlotRoute[]; binding?: SlotAuthorityBinding; configured: boolean }
const configuration = (root: string, validatedRoutes?: (routes: readonly SlotRoute[]) => void): Configuration => {
  const policy = readHostPolicy();
  if (policy.status === "invalid") refuse("Native host policy is untrusted or malformed");
  if (policy.status === "missing" || policy.document.slotAdmission === undefined) return { routes: [], configured: false };
  const raw = policy.document.slotAdmission;
  if (!object(raw) || raw.protocol !== SLOT_PROTOCOL || !Array.isArray(raw.routes) || !Array.isArray(raw.authorities)) {
    return refuse("Malformed native slotAdmission host policy");
  }
  const routes: SlotRoute[] = [];
  for (const route of raw.routes) {
    if (!object(route) || typeof route.repository !== "string" || !/^[a-z0-9_.-]+\/[a-z0-9_.-]+$/.test(route.repository)
      || typeof route.topic !== "string" || !/^[a-zA-Z0-9_.-]+$/.test(route.topic) || /\.shard[0-9]/.test(route.topic)
      || !positive(route.maxSlot) || route.maxSlot > 100_000
      || (route.denominators !== undefined && (!Array.isArray(route.denominators)
        || route.denominators.some(n => !positive(n) || n > (route.maxSlot as number))))) refuse("Malformed native slot route");
    const parsed: SlotRoute = { repository: route.repository, topic: route.topic, maxSlot: route.maxSlot,
      ...(route.denominators ? { denominators: [...route.denominators as number[]] } : {}) };
    if (routes.some(other => other.topic === parsed.topic || other.repository === parsed.repository)) refuse("Duplicate native slot route");
    routes.push(parsed);
  }
  // Route classification survives a later binding failure; it never expands to every topic.
  validatedRoutes?.(routes);
  let canonical: string | undefined;
  try { canonical = fs.realpathSync(root); } catch { /* Fresh initializer may not have created the root yet. */ }
  const candidates = raw.authorities.filter(value => object(value) && (value.meshRoot === path.resolve(root) || value.meshRoot === canonical));
  if (candidates.length > 1) refuse("Ambiguous native slot authority binding");
  const binding = candidates[0];
  if (binding === undefined) return { routes, configured: true };
  if (!object(binding) || binding.protocol !== SLOT_PROTOCOL || typeof binding.meshRoot !== "string"
    || !Array.isArray(binding.registryRoots) || binding.registryRoots.length === 0
    || binding.registryRoots.some(value => typeof value !== "string")
    || !Array.isArray(binding.rootIds) || binding.rootIds.length === 0
    || binding.rootIds.some(value => typeof value !== "string" || !value)
    || typeof binding.authorityGeneration !== "string" || !/^[a-zA-Z0-9_-]{16,128}$/.test(binding.authorityGeneration)
    || typeof binding.hubRootId !== "string" || !binding.hubRootId
    || typeof binding.physicalHostIdentity !== "string" || binding.physicalHostIdentity !== slotPhysicalHostIdentity()
    || binding.routesDigest !== slotRoutesDigest(routes)) refuse("Malformed or foreign-host native slot binding");
  for (const file of [binding.meshRoot, ...binding.registryRoots as string[]]) assertSlotCanonicalPath(file);
  if (new Set(binding.registryRoots).size !== binding.registryRoots.length) refuse("Duplicate native registry binding");
  if (binding.databaseFileIdentity !== undefined) {
    const id = binding.databaseFileIdentity;
    if (!object(id) || id.canonicalPath !== path.join(binding.meshRoot, "state.db")
      || typeof id.device !== "string" || !/^[0-9]+$/.test(id.device)
      || typeof id.inode !== "string" || !/^[1-9][0-9]*$/.test(id.inode)) refuse("Malformed native database identity");
  }
  return { routes, configured: true, binding: JSON.parse(JSON.stringify(binding)) as SlotAuthorityBinding };
};

export interface SlotTopic { route: SlotRoute; slot: number; topic: string; valid: boolean }
export const matchSlotTopic = (routes: readonly SlotRoute[], topic: string): SlotTopic | undefined => {
  for (const route of routes) {
    if (topic === route.topic) return { route, slot: 1, topic, valid: true };
    if (!topic.startsWith(`${route.topic}.shard`)) continue;
    const match = /^([1-9][0-9]*)(?:of([1-9][0-9]*))?$/.exec(topic.slice(route.topic.length + 6));
    const slot = match ? Number(match[1]) : 0;
    const denominator = match?.[2] === undefined ? undefined : Number(match[2]);
    const valid = positive(slot) && slot <= route.maxSlot && (denominator === undefined
      || (positive(denominator) && slot <= denominator && denominator <= route.maxSlot
        && (!route.denominators || route.denominators.includes(denominator))));
    // Malformed/unsupported spellings remain protected; they cannot evade the fence.
    return { route, slot, topic, valid };
  }
  return undefined;
};

/** One immutable native attach; removal/change of policy cannot downgrade an already enforced runtime. */
export class NativeSlotAuthority {
  readonly routes: readonly SlotRoute[];
  readonly binding: SlotAuthorityBinding | undefined;
  readonly #initial: Configuration | undefined;
  readonly #failure: unknown;
  constructor(readonly mesh: MeshStore) {
    let initial: Configuration | undefined, failure: unknown;
    let routes: readonly SlotRoute[] = [];
    try { initial = configuration(mesh.root, validated => { routes = validated; }); } catch (error) { failure = error; }
    this.#initial = initial;
    this.#failure = failure;
    this.routes = initial?.routes ?? routes;
    this.binding = initial?.binding;
  }

  get enforced(): boolean {
    // Lead scope override: NATS remains allowed UNENFORCED, even with retained native
    // binding configuration. forOperation still rejects a DIFFERENT registered authority
    // before taking this fast path; a copied/separate resident cannot downgrade an origin.
    return this.mesh.stateBackend !== "nats" && (!!this.binding || (!!this.#failure && this.routes.length > 0));
  }
  isProtected(topics: readonly string[]): boolean {
    return topics.some(topic => matchSlotTopic(this.routes, topic) !== undefined);
  }

  status(): SlotStatus {
    const base = { protocol: SLOT_PROTOCOL, backend: this.mesh.stateBackend };
    if (this.mesh.stateBackend === "nats") return { ...base, enforcement: "unenforced", reason: "nats-backend", gap: SLOT_GAP };
    if (this.#failure) return { ...base, enforcement: "blocked", reason: "authority-unavailable", gap: SLOT_GAP };
    if (!this.enforced) return { ...base, enforcement: "unenforced",
      reason: this.#initial?.configured ? "existing-authority" : "not-configured", gap: SLOT_GAP,
      ...(this.#initial?.configured ? { issue: "smarty-dev#13036" as const } : {}) };
    try {
      this.assert();
      return { ...base, enforcement: "enforced", phase: "enforce", ...this.facts() };
    } catch {
      return { ...base, enforcement: "blocked", reason: this.mesh.stateBackend === "sqlite" ? "authority-unavailable" : "unsupported-authority",
        gap: SLOT_GAP, ...this.facts() };
    }
  }

  /**
   * The native caller/registry enrollment is consulted BEFORE the unenforced fast path.
   * A separate resident DB must not downgrade a trusted enforcing origin. This table is
   * installer policy on every supported resident, not an origin UUID carried in a request.
   */
  forOperation(registryRoot?: string, callerId?: string): boolean {
    if (this.#failure) {
      // An unrelated invalid global policy supplies neither slot routes nor authority.
      // An already-loaded binding/routes snapshot never takes this ordinary fast path.
      if (!this.binding && this.routes.length === 0) return false;
      throw this.#failure;
    }
    const policy = readHostPolicy();
    if (policy.status === "invalid") {
      if (!this.binding && this.routes.length === 0) return false;
      refuse("Native slot expectation policy is untrusted");
    }
    const raw = policy.status === "valid" ? policy.document.slotAdmission : undefined;
    if (object(raw) && Array.isArray(raw.authorities)) {
      const registryCandidates = registryRoot === undefined ? [] : [registryRoot, expectationPath(registryRoot)];
      const expected = raw.authorities.filter(value => object(value) && (
        (Array.isArray(value.registryRoots) && registryCandidates.some(root => (value.registryRoots as unknown[]).includes(root)))
        || (callerId !== undefined && Array.isArray(value.rootIds) && value.rootIds.includes(callerId))));
      if (expected.length > 1) refuse("Ambiguous native slot origin/registry enrollment");
      if (expected.length === 1 && (!this.binding || slotBindingSeal(expected[0]) !== slotBindingSeal(this.binding))) {
        refuse("Trusted enforcing origin/registry is bound to a different physical mesh authority");
      }
    }
    return this.enforced;
  }

  facts(): Pick<SlotStatus, "authorityGeneration" | "hubRootId" | "routesDigest" | "meshRoot" | "databaseFileIdentity"> {
    const binding = this.binding;
    return binding ? { authorityGeneration: binding.authorityGeneration, hubRootId: binding.hubRootId,
      routesDigest: binding.routesDigest, meshRoot: binding.meshRoot,
      ...(binding.databaseFileIdentity ? { databaseFileIdentity: { ...binding.databaseFileIdentity } } : {}) } : {};
  }

  assert(registryRoot?: string): SlotAuthorityBinding {
    try { return this.#assert(registryRoot); }
    catch (error) {
      if (error instanceof ActorSlotClaimRefusedError) throw error;
      throw new ActorSlotClaimRefusedError("authority-unavailable", `Cannot attest native slot authority: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  #assert(registryRoot?: string): SlotAuthorityBinding {
    if (this.#failure) throw this.#failure;
    const expected = this.binding;
    if (!expected?.databaseFileIdentity) return refuse("Fresh native slot binding has not been durably provisioned; do not infer freshness from an empty database");
    if (this.mesh.stateBackend !== "sqlite") throw new ActorSlotClaimRefusedError("unsupported-authority", "Fresh enforced slots require the bound local SQLite backend (no fallback)");
    const current = configuration(this.mesh.root).binding;
    if (slotBindingSeal(current) !== slotBindingSeal(expected)) refuse("Native slot binding changed since attach");
    if (this.mesh.root !== expected.meshRoot) refuse("Mesh root is not the exact bound canonical path");
    assertSlotCanonicalPath(this.mesh.root);
    assertSlotCanonicalPath(expected.databaseFileIdentity.canonicalPath);
    const rootStat = fs.lstatSync(this.mesh.root), fileStat = fs.lstatSync(expected.databaseFileIdentity.canonicalPath);
    const uid = process.getuid?.();
    if (!rootStat.isDirectory() || !fileStat.isFile() || uid === undefined || rootStat.uid !== uid || fileStat.uid !== uid
      || (rootStat.mode & 0o022) !== 0 || (fileStat.mode & 0o077) !== 0) refuse("Slot authority custody/owner/mode is no longer private");
    if (registryRoot !== undefined) {
      assertSlotCanonicalPath(registryRoot);
      if (!expected.registryRoots.includes(registryRoot)) refuse("Actor registry is not registered with this native slot authority");
    }
    const actual = this.mesh.stateBackendHandle.attestDatabaseFile?.();
    if (!sameFileIdentity(actual, expected.databaseFileIdentity)) refuse("Actual opened SQLite connection does not match native binding");
    if (this.mesh.stateBackendHandle.slotAuthorityBinding?.() !== slotBindingSeal(expected)) refuse("SQLite has no matching native fresh-bootstrap seal; existing import refused (smarty-dev#13036)");
    return expected;
  }
}

/**
 * Installer-only one-shot initialization. Root first publishes a plan in host policy with no
 * databaseFileIdentity. This function returns the COMPLETE binding; the installer must publish
 * that binding in the same root-owned policy before business attach. No guest API calls it.
 *
 * ANY existing DB (even zero bytes/empty) or authority/registry artifact is #13036. A crash
 * after O_EXCL leaves an orphan that cannot be auto-reinitialized; it remains fail-closed.
 * There is deliberately no resume/import/force/frozen path and no inference from an empty ledger.
 * The protocol-capable launcher/root/registry population must be closed from the first attach.
 */
export const initializeFreshSlotAuthority = async (meshRoot: string): Promise<SlotAuthorityBinding> => {
  assertSlotCanonicalPath(meshRoot);
  if (fs.lstatSync(path.join(meshRoot, "state.db"), { throwIfNoEntry: false })) return existing();
  const config = configuration(meshRoot);
  const plan = config.binding;
  if (plan?.databaseFileIdentity) return existing();
  if (!plan) throw new ActorSlotClaimRefusedError("unsupported-authority", "Native fresh slot bootstrap requires a trusted pending host-policy plan");
  if (process.platform !== "linux") throw new ActorSlotClaimRefusedError("unsupported-authority", "Slot bootstrap requires Linux connection-fd attestation");
  assertSlotCanonicalPath(meshRoot);
  if (meshRoot !== plan.meshRoot) refuse("Bootstrap root is not canonical");
  const empty = (directory: string): boolean => {
    try { return fs.lstatSync(directory).isDirectory() && fs.readdirSync(directory).length === 0; }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return true; throw error; }
  };
  if (!empty(meshRoot) || plan.registryRoots.some(root => !empty(root))) return existing();
  fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
  const file = path.join(meshRoot, "state.db");
  let created: fs.BigIntStats;
  try {
    const fd = fs.openSync(file, "wx", 0o600);
    try { created = fs.fstatSync(fd, { bigint: true }); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return existing();
    throw error;
  }
  // Persist the O_EXCL witness and all newly created ancestor names BEFORE opening SQLite or
  // yielding. A first-attach crash cannot turn an initialized database into a fresh empty root.
  for (let directory = meshRoot;; directory = path.dirname(directory)) {
    const fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    if (path.dirname(directory) === directory) break;
  }
  // A failed open/seal/sync is not retried against the now-existing file. It is a blocked orphan.
  const store = await SqliteStateStore.open(meshRoot, 1024 * 1024, 10_000, { initialize: "create" });
  try {
    const identity = store.attestDatabaseFile();
    if (identity.device !== String(created.dev) || identity.inode !== String(created.ino)) refuse("Fresh DB was replaced during bootstrap");
    const binding: SlotAuthorityBinding = { ...plan, databaseFileIdentity: identity };
    await store.initializeSlotAuthority(slotBindingSeal(binding), () => {
      if (!sameFileIdentity(store.attestDatabaseFile(), identity)
        || plan.registryRoots.some(root => !empty(root))) refuse("Fresh bootstrap custody changed before seal");
    });
    await store.sync(5_000);
    return binding;
  } finally { store.close(); }
};
