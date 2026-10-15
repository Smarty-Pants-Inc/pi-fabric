import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  prepareWindowsMeshCapability, releaseWindowsMeshLock, windowsLogicalPathUnits, type WindowsMeshCapability, type WindowsMeshReleaseOutcome,
} from "../storage/windows-mesh-release.js";

/**
 * smarty-dev#9569 Commit A: the ONE release-finalization utility for the mesh `.lock`, `custody.lock` and the
 * synchronous/asynchronous fences, plus the Windows release dispatch through the storage module's retained,
 * digest-pinned native capability (design §§4, 5, 7).
 *
 * Boundary (Commit A): this is the Windows RELEASE primitive and its error semantics. Acquisition recovery on Windows
 * (empty-canonical E, complete-owner C, legacy-debris D), the Mac characterization and the mixed-version drain are
 * Commit B / rollout gates; no root-wide Windows guarantee is claimed here.
 */

/**
 * Whether this process runs the Windows release protocol (read at call time). Same predicate as verified-remove.ts's
 * helper lookup: os.type() names the REAL kernel, so Linux suites that inject process.platform = "win32" keep their
 * previous (main's) paths and never need the native helper; real Windows always dispatches here.
 */
export const windowsLockRelease = (): boolean => process.platform === "win32" && os.type() === "Windows_NT";

/** H1-H4 before ANY owner publication on Windows (throws: nothing was published); undefined elsewhere. */
export const prepareMeshLockRelease = (): WindowsMeshCapability | undefined =>
  windowsLockRelease() ? prepareWindowsMeshCapability() : undefined;

export type MeshLockOperationState = "not-started" | "returned" | "threw";

/** A storage outcome that left this holder's ownership unresolved (everything but not-owner/released). */
export type MeshLockReleaseFailureOutcome = Exclude<WindowsMeshReleaseOutcome, { outcome: "not-owner" | "released" }>;

/** Performs RELEASE ONLY (never the application operation) with the original record/capability; undefined: done. */
export type MeshLockReleaser = () => MeshLockReleaseFailureOutcome | undefined;

export interface MeshLockReleaseHandle {
  readonly lock: string;
  readonly release: MeshLockReleaser;
}

const describe = (value: unknown): string => {
  try { return value instanceof Error ? `${value.name}: ${value.message}` : String(value); }
  catch { return "unprintable value"; }
};

/** Runs a releaser; a throw is an unknown outcome (a child may have started), never an inferred pre-commit refusal. */
const runReleaser = (releaser: MeshLockReleaser): MeshLockReleaseFailureOutcome | undefined => {
  try { return releaser(); }
  catch (error) { return { outcome: "release-outcome-unknown", reason: `release invocation threw: ${describe(error)}` }; }
};

const FAILURE_OUTCOMES = new Set(["refused-before-retirement", "helper-unavailable/not-started", "release-outcome-unknown"]);

/**
 * The Windows releaser for one acquisition. `originalRecord` and `capability` are the exact values captured before
 * publication; the closure never re-reads a successor's record or re-resolves a helper. Repeatable: a later call is
 * a new guarded native transaction (design W7) that cannot authenticate a successor.
 */
export const windowsMeshReleaser = (lock: string, originalRecord: string, capability: WindowsMeshCapability): MeshLockReleaser => {
  // E3 before publication: even a short canonical owner can have an over-budget retired owner.
  // Resolve with the actual platform's path rules; only the Win32 transport prefix is excluded from the metric.
  const absoluteLock = path.resolve(lock);
  for (const owner of [path.join(absoluteLock, "owner"), path.join(path.dirname(absoluteLock), `r.${"0".repeat(64)}`, "owner")]) {
    const units = windowsLogicalPathUnits(owner);
    if (units >= 240) throw new RangeError(`Windows mesh lock owner path is too long (${units} logical UTF-16 units): ${owner}`);
  }
  const record = originalRecord;
  const held = capability;
  return () => {
    const result: WindowsMeshReleaseOutcome = releaseWindowsMeshLock(lock, record, held);
    const outcome = (result as { outcome?: unknown } | undefined)?.outcome;
    if (outcome === "not-owner" || outcome === "released") {
      if (result.history?.length) {
        // Resolved ownership is success; retain reconciliation/cleanup history without raising a failure alarm.
        try { console.warn("[pi-fabric] mesh-lock-release-resolved", { lock, outcome, history: result.history }); }
        catch { /* diagnostics never turn a safely resolved release into failure */ }
      }
      return undefined;
    }
    if (typeof outcome === "string" && FAILURE_OUTCOMES.has(outcome)) return result as MeshLockReleaseFailureOutcome;
    return { outcome: "release-outcome-unknown", reason: `unrecognized release outcome: ${describe(outcome)}` };
  };
};

const outcomeText = (outcome: MeshLockReleaseFailureOutcome): string => `${outcome.outcome}: ${outcome.reason}`;

/**
 * A lock release did not resolve this holder's ownership. `retryable` is false: the APPLICATION operation must never be
 * replayed because of this error. `retryRelease()` repeats the release only, with the same original record and
 * captured native capability; repeating it after success is a no-op (`not-owner`).
 */
export class MeshLockReleaseError extends Error {
  override readonly name = "MeshLockReleaseError";
  readonly code = "FABRIC_MESH_LOCK_RELEASE_FAILED" as const;
  readonly kind = "lock-release-failed" as const;
  readonly retryable = false as const;
  /** True only when the operation returned; false does NOT assert that a thrown operation's effects rolled back. */
  readonly operationCompleted: boolean;
  /** A same-record release retry is always safe (it cannot remove a successor); it may refuse until the reported cause is resolved. */
  readonly releaseRetryable = true as const;
  /** Further release failures of the same finalization (nested fence/dual locks), innermost first. */
  readonly related: readonly MeshLockReleaseError[];
  readonly #retry: MeshLockReleaser;

  constructor(readonly lock: string, readonly operationState: MeshLockOperationState,
    readonly releaseOutcome: MeshLockReleaseFailureOutcome, retry: MeshLockReleaser,
    options: { cause?: unknown; related?: readonly MeshLockReleaseError[] } = {}) {
    super(`FABRIC_MESH_LOCK_RELEASE_FAILED: release of ${lock} after operation ${operationState} did not complete ` +
      `(${outcomeText(releaseOutcome)}); retryRelease() repeats the release only, never the operation`,
    "cause" in options ? { cause: options.cause } : undefined);
    this.operationCompleted = operationState === "returned";
    this.related = Object.freeze([...(options.related ?? [])]);
    this.#retry = retry;
  }

  /** Release ONLY, same original record and capability. Throws a new MeshLockReleaseError while unresolved. */
  retryRelease(): void {
    const failure = runReleaser(this.#retry);
    if (failure) throw new MeshLockReleaseError(this.lock, this.operationState, failure, this.#retry);
  }
}

// Secondary release failures of a preserved primary throw. Never written onto the thrown value: objects (including
// non-Error objects) are keys of this WeakMap; every report also raises an alarm line (primitives have only that).
const secondaryReports = new WeakMap<object, MeshLockReleaseError[]>();

/** Release failures reported as secondary to a preserved primary thrown object (innermost/first first). */
export const meshLockSecondaryReleaseErrors = (primary: unknown): readonly MeshLockReleaseError[] =>
  (typeof primary === "object" && primary !== null) || typeof primary === "function"
    ? [...(secondaryReports.get(primary as object) ?? [])] : [];

const reportSecondary = (primary: unknown, error: MeshLockReleaseError): void => {
  if ((typeof primary === "object" && primary !== null) || typeof primary === "function") {
    const list = secondaryReports.get(primary as object);
    if (list) list.push(error); else secondaryReports.set(primary as object, [error]);
  }
  try { console.warn(`[pi-fabric] ALARM ${error.kind} (secondary to ${describe(primary)}): ${error.message}`); }
  catch { /* the report never masks the primary */ }
};

export type MeshLockSettlement<T> =
  | { readonly state: "returned"; readonly value: T }
  | { readonly state: "threw" | "not-started"; readonly error: unknown };

/**
 * The shared finalizer. Runs EVERY release in order (innermost first) whatever earlier releases did, then:
 * returned + no failure: the value; returned + failures: the first MeshLockReleaseError (others in `related`);
 * threw/not-started: the ORIGINAL thrown value (identity preserved, never wrapped or mutated), each release failure
 * reported as secondary. Never a bare `finally` that replaces the primary.
 */
export const finalizeMeshLocks = <T>(settled: MeshLockSettlement<T>, releases: readonly MeshLockReleaseHandle[]): T => {
  const failures: { handle: MeshLockReleaseHandle; outcome: MeshLockReleaseFailureOutcome }[] = [];
  for (const handle of releases) {
    const outcome = runReleaser(handle.release);
    if (outcome) failures.push({ handle, outcome });
  }
  const cause = settled.state === "returned" ? {} : { cause: settled.error };
  const errors = failures.map(({ handle, outcome }) =>
    new MeshLockReleaseError(handle.lock, settled.state, outcome, handle.release, cause));
  if (settled.state === "returned") {
    if (errors.length === 0) return settled.value;
    const first = failures[0]!;
    throw new MeshLockReleaseError(first.handle.lock, "returned", first.outcome, first.handle.release, { related: errors.slice(1) });
  }
  for (const error of errors) reportSecondary(settled.error, error);
  throw settled.error;
};

/** Acquisition failed before the operation: release (all handles), then rethrow `error` with failures as secondary. */
export function failBeforeEntry(error: unknown, releases: readonly MeshLockReleaseHandle[]): never {
  finalizeMeshLocks<never>({ state: "not-started", error }, releases);
  throw error; // unreachable: a non-returned settlement always rethrows
}

/**
 * `started` (optional) tells whether the APPLICATION operation began; a throw before it (an inner lock's acquisition)
 * is settled as "not-started". Without it every throw counts as "threw".
 */
export interface MeshLockSectionOptions { readonly started?: () => boolean }

const thrown = <T>(error: unknown, options: MeshLockSectionOptions): MeshLockSettlement<T> =>
  ({ state: options.started && !options.started() ? "not-started" : "threw", error });

/** Synchronous section under already-held locks, finalized by finalizeMeshLocks. */
export const runUnderMeshLocks = <T>(operation: () => T, releases: readonly MeshLockReleaseHandle[],
  options: MeshLockSectionOptions = {}): T => {
  let settled: MeshLockSettlement<T>;
  try { settled = { state: "returned", value: operation() }; }
  catch (error) { settled = thrown(error, options); }
  return finalizeMeshLocks(settled, releases);
};

/** Asynchronous section under already-held locks, finalized by finalizeMeshLocks. */
export const runUnderMeshLocksAsync = async <T>(operation: () => T | Promise<T>,
  releases: readonly MeshLockReleaseHandle[], options: MeshLockSectionOptions = {}): Promise<T> => {
  let settled: MeshLockSettlement<T>;
  try {
    // A synchronous result is not awaited: its release follows in the same turn, as a bare finally did.
    const result = operation();
    const value = typeof (result as { then?: unknown } | null)?.then === "function" ? await result : result as T;
    settled = { state: "returned", value };
  } catch (error) { settled = thrown(error, options); }
  return finalizeMeshLocks(settled, releases);
};

/** Windows pre-entry refusal: the canonical lock is not exactly `{owner}` holding this acquisition's record. */
export class MeshLockPreEntryRefusedError extends Error {
  override readonly name = "MeshLockPreEntryRefusedError";
  readonly code = "FABRIC_MESH_LOCK_PREENTRY_REFUSED" as const;
  readonly kind = "lock-preentry-refused" as const;
  /** Only an ownership loss before entry is a retryable acquisition; unexpected children need operator action. */
  readonly retryable: boolean;
  constructor(readonly lock: string, readonly reason: "unexpected-children" | "unreadable" | "ownership-lost",
    readonly entries: readonly string[] = []) {
    super(`FABRIC_MESH_LOCK_PREENTRY_REFUSED: ${lock} ${reason === "unexpected-children"
      ? `holds unexpected entries [${entries.join(", ")}] beside owner; the operation was not started (operator action required)`
      : reason === "unreadable" ? "could not be verified before entry; the operation was not started"
        : "no longer holds this acquisition's owner record; the operation was not started"}`);
    this.retryable = reason === "ownership-lost";
  }
}

/**
 * Windows pre-entry validation (design W0/W3 grammar): the canonical child set must be exactly `{owner}` and that
 * owner must be the ORIGINAL record. Known debris is refused before the operation, never entered with.
 */
export const windowsPreEntryRefusal = (lock: string, originalRecord: string): MeshLockPreEntryRefusedError | undefined => {
  let entries: string[];
  try { entries = fs.readdirSync(lock); } catch { return new MeshLockPreEntryRefusedError(lock, "unreadable"); }
  if (entries.length !== 1 || entries[0] !== "owner") return new MeshLockPreEntryRefusedError(lock, "unexpected-children", entries.sort());
  let owner: string;
  try { owner = fs.readFileSync(path.join(lock, "owner"), "utf8"); }
  catch { return new MeshLockPreEntryRefusedError(lock, "unreadable"); }
  return owner === originalRecord ? undefined : new MeshLockPreEntryRefusedError(lock, "ownership-lost");
};

export type { WindowsMeshCapability };
