import { createHash } from "node:crypto";
import type { FabricActorRequest } from "./types.js";
import type { MeshIdentity } from "../mesh/event-log.js";
import type { MeshBatchOperation, MeshBatchView, MeshStateEntry } from "../mesh/state-file.js";
import type { MeshStore } from "../mesh/store.js";
import { ActorSlotClaimRefusedError, NativeSlotAuthority, matchSlotTopic,
  type SlotRefusalReason, type SlotStatus, type SlotTopic } from "./slot-authority.js";

export { ActorSlotClaimRefusedError } from "./slot-authority.js";
export type { SlotStatus, SlotRefusalReason } from "./slot-authority.js";

export const SLOT_CLAIM_PREFIX = "factory/review-slots/v1/";
export const SLOT_ADMISSION_PREFIX = "actors/slot-admissions/v1/";
export type SlotRoleState = "admitted" | "revoking" | "retired";
type RoleName = "review" | "security";
interface Role {
  actorId: string;
  requestKey: string;
  specHash: string;
  normalizedRecoverableSpec: Record<string, unknown>;
  registryRoot: string;
  state: SlotRoleState;
}
interface Pair {
  version: 1;
  kind: "pair";
  authorityGeneration: string;
  claimKey: string;
  claimVersion: number;
  rootId: string;
  topic: string;
  sealed: boolean;
  roles: Partial<Record<RoleName, Role>>;
}
interface SlotAddress { key: string; admissionKey: string; repository: string; slot: number; generation: string }
export interface SlotAdmissionInput {
  actorId: string;
  request: FabricActorRequest;
  spec: Record<string, unknown>;
  registryRoot: string;
  caller: MeshIdentity;
  /** Synchronous native acceptance CAS, at the final SQL boundary, using the ORIGINAL selected ID. */
  beforeCommit?: (actorId: string) => void;
}
export interface SlotRecoverable { actorId: string; spec: Record<string, unknown>; state: SlotRoleState }

export class ActorSlotCreateUncertainError extends Error {
  readonly code = "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN";
  readonly details: { actorId?: string | undefined; claimKey?: string | undefined; claimVersion?: number | undefined; requestKey?: string | undefined };
  readonly actorId: string | undefined;
  readonly claimKey: string | undefined;
  readonly claimVersion: number | undefined;
  readonly requestKey: string | undefined;
  constructor(details: ActorSlotCreateUncertainError["details"], cause: unknown) {
    super("Slot intent committed or outcome uncertain; retain occupancy and reconcile the original actor ID", { cause });
    this.name = "ActorSlotCreateUncertainError";
    this.details = { ...details };
    this.actorId = details.actorId;
    this.claimKey = details.claimKey;
    this.claimVersion = details.claimVersion;
    this.requestKey = details.requestKey;
  }
}

const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const positive = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;
const ADMISSION_PLAN_ATTEMPTS = 3;
const ADMISSION_PLAN_STALE = Symbol("slot admission plan stale");
function refuse(reason: SlotRefusalReason, message: string): never { throw new ActorSlotClaimRefusedError(reason, message); }

/** Strict recoverable JSON, with canonical object order for stable immutable request fingerprints. */
const normalized = (value: unknown, ancestors = new Set<object>()): unknown => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (!value || typeof value !== "object" || ancestors.has(value)) return refuse("invalid", "Slot specification must be finite, acyclic recoverable JSON");
  if (!Array.isArray(value) && ![Object.prototype, null].includes(Object.getPrototypeOf(value))) refuse("invalid", "Slot specification must contain plain JSON objects");
  const next = new Set(ancestors).add(value);
  if (Array.isArray(value)) return value.map(item => normalized(item, next));
  const out: Record<string, unknown> = Object.create(null);
  for (const key of Object.keys(value).sort()) {
    const item = (value as Record<string, unknown>)[key];
    if (item !== undefined) out[key] = normalized(item, next);
  }
  return out;
};
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const fingerprint = (request: FabricActorRequest, spec: Record<string, unknown>, registryRoot: string, callerId: string): string => {
  // These are freshly preallocated/generated each attempt, not immutable actor arguments.
  // smarty-dev#16357: the root lane captured at creation is host-observed, not a caller argument;
  // a retry whose root record was briefly unavailable is still the same request.
  const { id: _id, createdAt: _created, updatedAt: _updated, sessionFile: _file,
    runnerSessionId: _runnerSession, ownershipToken: _ownership, exceptionLane: _lane, ...immutable } = spec;
  return createHash("sha256").update(JSON.stringify(normalized({ request, spec: immutable, registryRoot, callerId }))).digest("hex");
};
const address = (key: string): SlotAddress => {
  const match = /^factory\/review-slots\/v1\/([a-z0-9_.-]+\/[a-z0-9_.-]+)\/([1-9][0-9]*)\/(base|r[1-9][0-9]*)$/.exec(key);
  if (!match || key.length > 256 || key.split("/").some(part => ["__proto__", "constructor", "prototype"].includes(part))
    || !positive(Number(match[2]))) return refuse("invalid", "Slot claim key is not canonical");
  const suffix = key.slice(SLOT_CLAIM_PREFIX.length);
  return { key, admissionKey: SLOT_ADMISSION_PREFIX + suffix, repository: match[1]!, slot: Number(match[2]), generation: match[3]! };
};
const readPair = (entry: MeshStateEntry, generation: string): Pair => {
  const value = entry.value;
  if (!object(value) || value.version !== 1 || value.kind !== "pair" || value.authorityGeneration !== generation
    || !nonempty(value.claimKey) || !positive(value.claimVersion) || !nonempty(value.rootId) || !nonempty(value.topic)
    || typeof value.sealed !== "boolean" || !object(value.roles) || Object.keys(value.roles).some(key => key !== "review" && key !== "security")) {
    return refuse("occupied", "Malformed, foreign or blocked admission is permanent occupied state");
  }
  const addr = address(value.claimKey);
  if (entry.key !== addr.admissionKey) refuse("occupied", "Admission is under the wrong canonical key");
  for (const role of Object.values(value.roles)) {
    if (!object(role) || !nonempty(role.actorId) || !nonempty(role.requestKey) || !nonempty(role.registryRoot)
      || typeof role.specHash !== "string" || !/^[a-f0-9]{64}$/.test(role.specHash) || !object(role.normalizedRecoverableSpec)
      || role.normalizedRecoverableSpec.id !== role.actorId
      || !["admitted", "revoking", "retired"].includes(String(role.state))) refuse("occupied", "Malformed occupied slot role");
    if (role.state !== "admitted" && !value.sealed) refuse("occupied", "Unsealed terminal slot role");
  }
  return copy(value) as unknown as Pair;
};

/** Same-store native claim comparison + durable actor-birth ledger. No registry/projection I/O here. */
export class SlotAdmissionCoordinator {
  readonly #authority: NativeSlotAuthority;
  /** Every materializing observer owes its own sync. A synchronous current check cannot invent one. */
  readonly #syncedIds = new Set<string>();
  constructor(readonly mesh: MeshStore) {
    this.#authority = new NativeSlotAuthority(mesh);
    if (this.#authority.enforced) mesh.stateBackendHandle.installSlotClaimGuard?.(() => { this.#authority.assert(); });
  }

  isProtected(topics: readonly string[]): boolean { return this.#authority.isProtected(topics); }
  status(): SlotStatus { return this.#authority.status(); }

  /**
   * Constructor/restore gate: diagnostics saying "unenforced" do NOT override a native
   * registry/caller expectation bound elsewhere. True on unknown/blocked authority so callers
   * park BEFORE publication; mutating APIs still throw the structured refusal when invoked.
   */
  requiresEnforcement(topics: readonly string[], registryRoot: string, callerId?: string): boolean {
    if (!this.isProtected(topics)) return false;
    try { return this.#authority.forOperation(registryRoot, callerId); }
    catch { return true; }
  }

  /** Before registry enumeration, retain unknown addressed input only for a native enforcing expectation. */
  requiresRecovery(registryRoot: string, callerId?: string): boolean {
    try { return this.#authority.forOperation(registryRoot, callerId); }
    catch { return true; }
  }

  #protected(topics: readonly string[], registryRoot?: string, callerId?: string): boolean {
    if (!this.isProtected(topics)) return false;
    return this.#authority.forOperation(registryRoot, callerId);
  }

  guardClaimMutation(key: string): void {
    if (!key.startsWith(SLOT_CLAIM_PREFIX) || !this.#authority.enforced) return;
    this.#authority.assert();
  }

  async admit(input: SlotAdmissionInput): Promise<{ actorId: string; spec: Record<string, unknown>; reused: boolean } | undefined> {
    const requestedTopics = [...new Set(input.request.topics ?? [])];
    const nativeTopics = Array.isArray(input.spec.topics) ? input.spec.topics.filter((topic): topic is string => typeof topic === "string") : [];
    if (!this.#protected([...new Set([...requestedTopics, ...nativeTopics])], input.registryRoot, input.caller.id)) return undefined;
    const topics = requestedTopics;
    const binding = this.#authority.assert(input.registryRoot);
    // Snapshot native inputs before the first await; later caller mutation cannot change a
    // prepared fingerprint, token or authenticated identity while SQLite acquisition waits.
    input = { ...input, request: normalized(input.request) as FabricActorRequest, caller: copy(input.caller) };
    if (input.caller.kind !== "main" || !binding.rootIds.includes(input.caller.id)) refuse("foreign-root", "Slot caller is not a registered authenticated Main");
    const matches = topics.map(topic => matchSlotTopic(this.#authority.routes, topic)).filter((match): match is SlotTopic => !!match);
    if (matches.length !== 1 || !matches[0]!.valid) refuse("invalid", "One actor may subscribe to exactly one canonical protected receiver");
    const target = matches[0]!;
    // Transitional typing until the wire lane adds the shared request field; never authorize it as native binding.
    const token = (input.request as FabricActorRequest & { slotClaim?: { key: string; ifVersion: number } }).slotClaim;
    if (!token) refuse("required", "Protected slot birth requires slotClaim");
    if (!nonempty(token.key) || !positive(token.ifVersion)) refuse("invalid", "slotClaim requires a canonical key and positive safe entry revision");
    const addr = address(token.key);
    if (addr.repository !== target.route.repository || addr.slot !== target.slot) refuse("invalid", "Claim does not address the exact protected receiver slot");
    if (!nonempty(input.request.idempotencyKey) || input.request.idempotencyKey.length > 256) refuse("required", "Protected slot birth requires a stable bounded idempotencyKey");
    if (!nonempty(input.actorId)) refuse("invalid", "Slot actor ID must be preallocated");
    const spec = normalized(input.spec) as Record<string, unknown>;
    if (spec.id !== input.actorId || spec.name !== input.request.name || typeof spec.instructions !== "string" || !spec.instructions.trim()
      || !Array.isArray(spec.topics) || JSON.stringify([...new Set(spec.topics)].sort()) !== JSON.stringify([...topics].sort())
      || spec.rootId !== input.caller.id) refuse("invalid", "Recoverable slot specification does not match its native identity/request");
    const specHash = fingerprint(input.request, spec, input.registryRoot, input.caller.id);
    // Prevalidate the complete recoverable payload, before taking transaction custody/accepting a request.
    this.#validateBytes(spec);
    let selected: Role | undefined;
    let reused = false;
    let boundaryEntered = false;
    try {
      const prepare = (view: MeshBatchView): MeshBatchOperation[] => {
        selected = undefined;
        reused = false;
        const old = view.get(addr.admissionKey);
        let pair: Pair | undefined = old ? readPair(old, binding.authorityGeneration) : undefined;
        if (pair && (pair.claimKey !== token.key || pair.claimVersion !== token.ifVersion || pair.rootId !== input.caller.id
          || pair.topic !== target.topic || pair.sealed)) refuse("occupied", "Slot generation is sealed or belongs to another claim epoch/root/topic");
        if (pair) {
          // Reconcile an unchanged ORIGINAL request even if the birth claim has since been deleted.
          // It is an occupied identity lookup, not authorization for any new birth.
          const existingRole = Object.values(pair.roles).find(role => role?.normalizedRecoverableSpec.name === input.request.name);
          if (existingRole) {
            if (existingRole.state !== "admitted" || existingRole.requestKey !== input.request.idempotencyKey
              || existingRole.specHash !== specHash || existingRole.registryRoot !== input.registryRoot) refuse("occupied", "Slot role already has a different permanent incarnation/request");
            selected = existingRole;
            reused = true;
            return [];
          }
        }
        const claim = view.get(addr.key);
        if (!claim || claim.version !== token.ifVersion) refuse("stale", "Slot claim is absent or its exact entry revision changed");
        const value = claim.value;
        if (!object(value) || value.version !== 1 || value.repository !== addr.repository || value.slot !== addr.slot
          || value.generation !== addr.generation || !nonempty(value.owner) || value.hubRootId !== binding.hubRootId
          || !Array.isArray(value.names) || value.names.length !== 2 || value.names.some(name => typeof name !== "string" || !name)
          || value.names[0] === value.names[1]) refuse("invalid", "Malformed or mismatched canonical slot claim");
        if (value.claimedBy !== input.caller.id || claim.updatedBy.kind !== "main" || claim.updatedBy.id !== input.caller.id) {
          refuse("foreign-root", "Claim ownership/provenance is not the authenticated requesting Main");
        }
        // #7861 pair convention: the review/security suffixes share one exact generation prefix.
        const reviewName = value.names[0] as string, securityName = value.names[1] as string;
        if (!reviewName.endsWith("-review-astra") || securityName !== `${reviewName.slice(0, -13)}-security-astra`) {
          refuse("invalid", "Slot claim names are not a canonical review/security pair");
        }
        const roleName: RoleName = input.request.name === reviewName ? "review"
          : input.request.name === securityName ? "security" : refuse("invalid", "Actor name is not a claimed review/security role");
        if (pair?.roles[roleName]) refuse("occupied", "Slot role is permanently occupied");
        // Request keys and actor IDs are root-scoped unique throughout this permanent ledger.
        for (const entry of view.listAll(SLOT_ADMISSION_PREFIX)) {
          const incumbent = readPair(entry, binding.authorityGeneration);
          for (const role of Object.values(incumbent.roles)) {
            if (role?.actorId === input.actorId || (incumbent.rootId === input.caller.id && role?.requestKey === input.request.idempotencyKey)) {
              refuse("occupied", "Actor ID or root-scoped idempotencyKey is already admitted");
            }
          }
        }
        pair ??= { version: 1, kind: "pair", authorityGeneration: binding.authorityGeneration, claimKey: addr.key,
          claimVersion: token.ifVersion, rootId: input.caller.id, topic: target.topic, sealed: false, roles: {} };
        selected = { actorId: input.actorId, requestKey: input.request.idempotencyKey!, specHash,
          normalizedRecoverableSpec: spec, registryRoot: input.registryRoot, state: "admitted" };
        pair.roles[roleName] = selected;
        this.#validateBytes(pair);
        return [{ kind: "put", key: addr.admissionKey, value: pair, ifVersion: old?.version ?? view.version(addr.admissionKey) }];
      };
      for (let attempt = 0; ; attempt += 1) {
        // Plan scans/JSON copies before BEGIN IMMEDIATE. Validate the WHOLE ledger prefix,
        // not just the rows seen: a newly inserted pair can steal this root's request key
        // or actor ID. The claim key also fences ownership, revision and deletion/ABA.
        // As in the mesh mirror planner, bounded stale retries end with the original
        // transactional path, preserving progress without trusting an unvalidated plan.
        const early = attempt < ADMISSION_PLAN_ATTEMPTS - 1 ? this.mesh.stateView() : undefined;
        const planned = early ? prepare(early.view) : undefined;
        try {
          await this.mesh.writeBatch({ identity: input.caller, lockTiming: { operation: "slot-admission" }, ops: [], prepare: view => {
            if (!early) return prepare(view);
            if (!this.mesh.stateUnchangedSince(early.revision, [addr.key, SLOT_ADMISSION_PREFIX])) throw ADMISSION_PLAN_STALE;
            // Prefix feeds do not name evicted tombstones. Presence/content is validated,
            // but an absent key's retained CAS revision may now be zero: use the live slot.
            return planned!.map(op => ({ ...op, ifVersion: view.version(op.key) }));
          }, beforeCommit: () => {
            this.#authority.assert(input.registryRoot);
            if (!selected) throw new Error("Slot transaction did not select an actor identity");
            boundaryEntered = true;
            input.beforeCommit?.(selected.actorId);
          } });
          break;
        } catch (error) {
          if (error !== ADMISSION_PLAN_STALE) throw error;
        }
      }
      // No actor success/projection may precede this keyed-state barrier, including exact retry.
      await this.mesh.stateBackendHandle.sync(5_000);
      this.#authority.assert(input.registryRoot);
      if (!selected) throw new Error("Committed slot transaction has no selected role");
      this.#syncedIds.add(selected.actorId);
      return { actorId: selected.actorId, spec: copy(selected.normalizedRecoverableSpec), reused };
    } catch (error) {
      if (boundaryEntered) throw new ActorSlotCreateUncertainError({ actorId: selected?.actorId ?? input.actorId,
        claimKey: token.key, claimVersion: token.ifVersion, requestKey: input.request.idempotencyKey }, error);
      throw error;
    }
  }

  #validateBytes(value: unknown): void {
    if (Buffer.byteLength(JSON.stringify(value), "utf8") > this.mesh.maxEventBytes) refuse("invalid", `Slot admission value exceeds ${this.mesh.maxEventBytes} bytes`);
  }

  #find(actorId: string, registryRoot: string, entries: readonly MeshStateEntry[]): { pair: Pair; role: Role; entry: MeshStateEntry } | undefined {
    const binding = this.#authority.binding;
    if (!binding) return undefined;
    let found: { pair: Pair; role: Role; entry: MeshStateEntry } | undefined;
    for (const entry of entries) {
      const pair = readPair(entry, binding.authorityGeneration);
      for (const role of Object.values(pair.roles)) {
        if (role?.actorId !== actorId || role.registryRoot !== registryRoot) continue;
        if (found) refuse("occupied", "Actor ID appears in multiple native admissions");
        found = { pair, role, entry };
      }
    }
    return found;
  }

  isCurrent(actorId: string, topics: readonly string[], registryRoot: string): boolean {
    try {
      if (!this.#protected(topics, registryRoot)) return true;
      this.withCurrent(actorId, topics, registryRoot, () => undefined);
      return true;
    } catch { return false; }
  }

  withCurrent<T>(actorId: string, topics: readonly string[], registryRoot: string, operation: () => T): T {
    if (!this.#protected(topics, registryRoot)) return operation();
    return this.mesh.withStateWriteFence(() => {
      this.#authority.assert(registryRoot);
      const found = this.#find(actorId, registryRoot, this.mesh.listAll(SLOT_ADMISSION_PREFIX, { fresh: true }));
      if (!found || found.role.state !== "admitted" || !this.#syncedIds.has(actorId)
        || topics.filter(topic => this.isProtected([topic])).length !== 1 || !topics.includes(found.pair.topic)) {
        refuse("occupied", "Protected actor is not the exact current synced admitted ID/topic/registry");
      }
      const result = operation();
      if (object(result) && typeof result.then === "function") throw new Error("Slot current-state operation must be synchronous");
      return result;
    });
  }

  async recoverable(registryRoot: string): Promise<SlotRecoverable[]> {
    if (!this.#authority.forOperation(registryRoot)) return [];
    const binding = this.#authority.assert(registryRoot);
    let entries: MeshStateEntry[] = [];
    await this.mesh.writeBatch({ identity: { id: binding.hubRootId, name: "slot-recovery", kind: "main" },
      lockTiming: { operation: "slot-recovery" }, ops: [], prepare: view => {
      entries = view.listAll(SLOT_ADMISSION_PREFIX);
      return [];
    }, beforeCommit: () => { this.#authority.assert(registryRoot); } });
    // The view already detached these rows. Validate and clone large recoverable specs after
    // releasing custody; no choice or mutation is made from a newer, unfenced state read.
    const recovered: SlotRecoverable[] = [];
    for (const entry of entries) {
      const pair = readPair(entry, binding.authorityGeneration);
      for (const role of Object.values(pair.roles)) if (role?.registryRoot === registryRoot) {
        recovered.push({ actorId: role.actorId, spec: role.normalizedRecoverableSpec, state: role.state });
      }
    }
    await this.mesh.stateBackendHandle.sync(5_000);
    this.#authority.assert(registryRoot);
    for (const role of recovered) if (role.state === "admitted") this.#syncedIds.add(role.actorId);
    return recovered;
  }

  async beginRemoval(actorId: string, topics: readonly string[], registryRoot: string): Promise<void> {
    await this.#transition(actorId, topics, registryRoot, "revoking");
  }

  async retire(actorId: string, topics: readonly string[], registryRoot: string): Promise<void> {
    // Native manager owes checked child settlement + durable registry removal before this call.
    await this.#transition(actorId, topics, registryRoot, "retired");
  }

  async #transition(actorId: string, topics: readonly string[], registryRoot: string, state: "revoking" | "retired"): Promise<void> {
    if (!this.#protected(topics, registryRoot)) return;
    const binding = this.#authority.assert(registryRoot);
    let boundaryEntered = false;
    let found: { pair: Pair; role: Role; entry: MeshStateEntry } | undefined;
    try {
      await this.mesh.writeBatch({ identity: { id: binding.hubRootId, name: "slot-removal", kind: "main" },
        lockTiming: { operation: state === "revoking" ? "slot-revoke" : "slot-retire" }, ops: [], prepare: view => {
        found = this.#find(actorId, registryRoot, view.listAll(SLOT_ADMISSION_PREFIX));
        if (!found || !topics.includes(found.pair.topic)) refuse("occupied", "No exact matching native slot incarnation to retire");
        if (found.role.state === "retired" || found.role.state === state) return [];
        if (state === "retired" && found.role.state !== "revoking") refuse("invalid", "Slot must be durably revoking before retirement");
        found.role.state = state;
        found.pair.sealed = true;
        return [{ kind: "put", key: found.entry.key, value: found.pair, ifVersion: found.entry.version }];
      }, beforeCommit: () => { this.#authority.assert(registryRoot); boundaryEntered = true; } });
      this.#syncedIds.delete(actorId);
      await this.mesh.stateBackendHandle.sync(5_000);
      this.#authority.assert(registryRoot);
    } catch (error) {
      if (boundaryEntered) throw new ActorSlotCreateUncertainError({ actorId, claimKey: found?.pair.claimKey,
        claimVersion: found?.pair.claimVersion, requestKey: found?.role.requestKey }, error);
      throw error;
    }
  }
}

const coordinators = new WeakMap<MeshStore, SlotAdmissionCoordinator>();
export const slotAdmissionFor = (mesh: MeshStore): SlotAdmissionCoordinator => {
  let coordinator = coordinators.get(mesh);
  if (!coordinator) { coordinator = new SlotAdmissionCoordinator(mesh); coordinators.set(mesh, coordinator); }
  return coordinator;
};
