import fs from "node:fs";
import { diagnostics } from "../core/diagnostics.js";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import type { MeshEvent, MeshStore } from "../mesh/store.js";
import { childProcessEnvironment, writeJsonAtomic } from "../core/atomic-write.js";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../mesh/archive.js";
import { lockFile } from "./file-lock.js";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";
import type { FabricActorInfo } from "../actors/types.js";
import type { FabricParticipantInfo, FabricParticipantRecord } from "../topology/types.js";
import { readWakeJson, residentOwnerLive, residentOwnerSleeping, routesAt, residentDirectories, residentDeliveryMatches,
  acknowledgeResidentDelivery, assertResidentWakeConfig, retainedRoutesAt, residentUnacknowledgedDelivery, residentWakeRetryCandidates, ResidentWakeConfigMismatch, RESIDENT_WAKE_INDEX_MAX_ROOTS, ResidentWakeRecoveryReader, RESIDENT_WAKE_RECOVERY_MAX_BYTES, type ResidentWakeRecoveryBatch, type WakeDelivery } from "./wake-index.js";
export { readWakeJson, residentOwnerLive, residentOwnerSleeping, ResidentWakeConfigMismatch } from "./wake-index.js";

/** Sleep needs archive retention even on a mesh that previously used only a bounded live log. */
export async function ensureResidentWakeArchive(mesh: Pick<MeshStore, "root" | "exclusive">): Promise<void> {
  await mesh.exclusive(() => {
    const existing = MeshArchive.fromRoot(mesh.root);
    if (existing) {
      if (!fs.statSync(existing.dir).isDirectory()) throw new Error("Resident wake archive is unavailable");
      return; // Never replace an operator-selected archive.
    }
    const dir = path.join(mesh.root, "wake-archive");
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    if (!fs.lstatSync(dir).isDirectory()) throw new Error("Resident wake archive is not a directory");
    writeJsonAtomic(path.join(mesh.root, MESH_ARCHIVE_CONFIG), { version: 1, dir }, { durable: true });
  });
}

/** Routing metadata, not a second inbox: deliveries remain in the mesh archive. */
export interface ResidentWakeRoutes {
  format: 1;
  rootId: string;
  hostId: string;
  /** Canonical snapshot of every field of the exact restart configuration. */
  configJson: string;
  actors: Array<{ id: string; name: string; topics: string[]; participant?: FabricParticipantRecord }>;
}
export interface ResidentWakeRequest { format: 1; id: string; sequence?: number; requestedAt: number }
export const residentSleepingPath = (root: string): string => path.join(root, "sleeping.json");
export const residentWakeRequestPath = (root: string): string => path.join(root, "wake-request.json");
export const residentWakeIntentLockPath = (root: string): string => path.join(root, "wake-intent.lock");
const wakeFailurePath = (root: string): string => path.join(root, "wake-failure.json");
const wakeTerminalPath = (root: string): string => path.join(root, "wake-terminal.json");
interface ResidentWakeTerminal {
  format: 1;
  rootId: string;
  delivery: WakeDelivery;
  code: "RESIDENT_WAKE_CONFIG_MISMATCH";
  error: string;
  failedAt: number;
}
/** Failed wakes are retried by existing maintenance, not unrelated publications. */
export const RESIDENT_WAKE_RETRY_MIN_MS = 1_000;
export const RESIDENT_WAKE_RETRY_MAX_MS = 60_000;
/** Bound delivery/config/receipt inspection per sweep, independent of backlog size. */
export const RESIDENT_WAKE_RETRY_MAX_CANDIDATES = 128;
const RESIDENT_WAKE_ATTEMPT_LEASE_MS = 120_000;
interface ResidentWakeFailure {
  format: 1;
  delivery: WakeDelivery;
  error?: string;
  failedAt?: number;
  nextRetryAt?: number;
  failures?: number;
  claimId?: string;
}
type ResidentWakeMesh = Pick<MeshStore, "root" | "listAll" | "exclusive">;
export interface ResidentWakeAdmission { allowed: () => boolean; exclusive: ResidentWakeMesh["exclusive"] }
class ResidentWakeAdmissionClosed extends Error {
  constructor() { super("Resident wake maintenance admission closed"); }
}
const wakeFailureAt = (root: string): ResidentWakeFailure | undefined => readWakeJson<ResidentWakeFailure>(wakeFailurePath(root));
const wakeRetryDelay = (failures: number): number => Math.min(RESIDENT_WAKE_RETRY_MAX_MS,
  RESIDENT_WAKE_RETRY_MIN_MS * 2 ** Math.min(16, Math.max(0, failures - 1)));
const wakeFailures = (failure: ResidentWakeFailure | undefined): number =>
  Number.isSafeInteger(failure?.failures) && failure!.failures! >= 0 ? failure!.failures! : 0;
const residentWakeRetrySweeps = new Map<string, Promise<void>>();

/** Receipt writers share intent custody; only index operations need mesh custody. */
const withWakeReceipt = async <T>(root: string, operation: () => Promise<T>): Promise<T> => {
  if (process.platform === "win32") {
    const { withExclusiveFileLockAsync } = await import("../core/file-lock.js");
    return withExclusiveFileLockAsync({ directory: root, lockName: "wake-receipt.lock",
      timeoutMessage: "Resident wake receipt is busy" }, operation);
  }
  const fd = await lockFile(residentWakeIntentLockPath(root), 90, process.platform === "linux");
  try { return await operation(); } finally { fs.closeSync(fd); }
};
const deliveryCovers = (served: WakeDelivery, pending: WakeDelivery): boolean => served.id === pending.id ||
  (served.sequence !== undefined && pending.sequence !== undefined && served.sequence >= pending.sequence);
const sameWakeDelivery = (left: WakeDelivery, right: WakeDelivery): boolean =>
  left.id === right.id && left.sequence === right.sequence;
const wakeObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** Terminal receipts are untrusted hints, never high-watermark authority. Even
 * a well-typed ID/sequence must name an actual committed, matching archive event.
 * All archive/route/receipt I/O runs outside global mesh custody. */
const wakeTerminalAt = (mesh: ResidentWakeMesh, root: string): ResidentWakeTerminal | undefined => {
  const value = readWakeJson<unknown>(wakeTerminalPath(root));
  if (value === undefined) {
    try { fs.lstatSync(wakeTerminalPath(root)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
    throw new Error("Resident wake terminal receipt is unreadable");
  }
  if (!wakeObject(value) || Object.keys(value).sort().join(",") !== "code,delivery,error,failedAt,format,rootId" ||
      value.format !== 1 || typeof value.rootId !== "string" || !value.rootId.length ||
      value.code !== "RESIDENT_WAKE_CONFIG_MISMATCH" || typeof value.error !== "string" || !value.error.length ||
      !Number.isSafeInteger(value.failedAt) || (value.failedAt as number) < 0 || !wakeObject(value.delivery))
    throw new Error("Resident wake terminal receipt is invalid");
  const delivery = value.delivery;
  if (typeof delivery.id !== "string" || !delivery.id.length || !Number.isSafeInteger(delivery.sequence) ||
      (delivery.sequence as number) < 1 || Object.keys(delivery).some(key => !["id", "sequence", "deferredAt"].includes(key)) ||
      (delivery.deferredAt !== undefined && (!Number.isSafeInteger(delivery.deferredAt) ||
        (delivery.deferredAt as number) < 1 || (delivery.deferredAt as number) > (delivery.sequence as number))))
    throw new Error("Resident wake terminal delivery is invalid");
  const routes = retainedRoutesAt(root);
  const entry = MeshArchive.fromRoot(mesh.root)?.lookupEntry(delivery.sequence as number, RESIDENT_WAKE_RECOVERY_MAX_BYTES);
  if (!routes || routes.rootId !== value.rootId || !entry?.committed || entry.event.id !== delivery.id ||
      entry.event.sequence !== delivery.sequence || !residentDeliveryMatches(routes, entry.event,
        mesh.listAll("topology/subscriptions/", { fresh: true }).map(row => row.value as { from?: string; to?: string; events?: string[] })))
    throw new Error("Resident wake terminal delivery has no matching committed archive event");
  return value as unknown as ResidentWakeTerminal;
};
const clearWakeFailure = (root: string, delivery: WakeDelivery): Promise<void> => withWakeReceipt(root, async () => {
  const prior = wakeFailureAt(root);
  if (prior && deliveryCovers(delivery, prior.delivery)) fs.rmSync(wakeFailurePath(root), { force: true });
});

/** Maintenance may transfer pending to a durable failure receipt; publication keeps pending.
 * A terminal mismatch instead retires only this attempt's watermark, never a newer publication. */
const recordResidentWakeFailure = async (mesh: ResidentWakeMesh, root: string, delivery: WakeDelivery,
  error: unknown, now: number, claimId?: string, acknowledge = true, terminalOnly = false): Promise<boolean> => {
  let terminal = false;
  try {
    const message = String(error);
    const mismatch = error instanceof ResidentWakeConfigMismatch;
    const inspect = mismatch ? (await import("./wake-terminal.js")).withResidentWakeTerminalRoot : undefined;
    return await withWakeReceipt(root, async () => {
      const prior = wakeFailureAt(root);
      if (claimId !== undefined && prior?.claimId !== claimId) return true;
      const existing = wakeTerminalAt(mesh, root); // Invalid bytes never mutate either receipt or pending.
      if (terminalOnly && (!existing || !sameWakeDelivery(existing.delivery, delivery))) return false;
      const disposed = await inspect?.(mesh, root, async rootId => {
        if (existing && existing.rootId !== rootId) return false;
        // The original failure/receipt is not current config authority. Repair
        // must resume normal waking, including interrupted SAME-receipt recovery.
        try { assertResidentWakeConfig(root); return false; }
        catch (error) { if (!(error instanceof ResidentWakeConfigMismatch)) throw error; }
        if (!existing || !sameWakeDelivery(existing.delivery, delivery)) {
          writeJsonAtomic(wakeTerminalPath(root), { format: 1, rootId,
            delivery: { id: delivery.id, ...(delivery.sequence !== undefined ? { sequence: delivery.sequence } : {}) },
            code: "RESIDENT_WAKE_CONFIG_MISMATCH", error: message, failedAt: now } satisfies ResidentWakeTerminal, { durable: true });
          diagnostics.warn("src/residency/wake.ts", `[pi-fabric] resident wake terminal for ${root}: ${message}`);
        }
        terminal = true;
        const acknowledged = await mesh.exclusive(() => {
          const pending = residentUnacknowledgedDelivery(mesh.root, root);
          // Recovery cannot let even a valid later receipt cover a different nudge.
          if (terminalOnly && pending && !sameWakeDelivery(delivery, pending)) return false;
          acknowledgeResidentDelivery(mesh.root, root, delivery);
          return true;
        });
        if (acknowledged && prior && deliveryCovers(delivery, prior.delivery)) fs.rmSync(wakeFailurePath(root), { force: true });
        return true;
      }).catch(() => undefined);
      if (disposed) return true;
      if (terminalOnly) return false;
      const failures = wakeFailures(prior) + 1;
      const retained = prior && deliveryCovers(prior.delivery, delivery) ? prior.delivery : delivery;
      writeJsonAtomic(wakeFailurePath(root), { format: 1, delivery: retained, error: message, failedAt: now,
        failures, nextRetryAt: now + wakeRetryDelay(failures) }, { durable: true });
      // Live/unknown/busy mismatch decisions keep the pending index as well as
      // ordinary backoff. Failure to acquire a native fence never authorizes ACK.
      if (acknowledge && !mismatch) await mesh.exclusive(() => acknowledgeResidentDelivery(mesh.root, root, delivery));
      return false;
    });
  } catch { return terminal; } // Persistence failure retains the pending index or durable claim.
};

/** Finish an interrupted terminal ACK without claiming or launching the same watermark again. */
const acknowledgeWakeTerminal = async (mesh: ResidentWakeMesh, root: string, delivery: WakeDelivery,
  claimId?: string): Promise<boolean> => {
  try {
    const terminal = wakeTerminalAt(mesh, root);
    if (!terminal) return false;
    // A valid DIFFERENT receipt neither covers nor quarantines this nudge.
    if (!sameWakeDelivery(terminal.delivery, delivery)) return false;
    return await recordResidentWakeFailure(mesh, root, delivery, new ResidentWakeConfigMismatch(root), Date.now(), claimId, false, true);
  } catch {
    return true; // Invalid/unavailable evidence is handled by retaining BOTH pending and prior failure unchanged.
  }
};

/** Retained, actually published authority for admission, not a live lease or permission to deliver. */
export function dormantActorRoute(meshRoot: string, actor: FabricActorInfo): FabricParticipantInfo | undefined {
  const candidates = residentDirectories(meshRoot).flatMap(root => {
    const routes = routesAt(root);
    const participant = routes?.actors.find(entry => entry.id === actor.id)?.participant;
    if (!participant || participant.format !== 1 || participant.kind !== "actor" || participant.id !== actor.id ||
        routes!.rootId !== actor.rootId || participant.rootId !== actor.rootId ||
        typeof actor.ownershipToken !== "string" || participant.actorOwnershipToken !== actor.ownershipToken || participant.ownerHostId !== routes!.hostId ||
        !participant.ownerIdentityId || participant.remoteHost || participant.residency !== "durable" ||
        !Array.isArray(participant.capabilities) || participant.controlProtocol !== "v1" ||
        ["stopping", "reloading", "stopped"].includes(participant.status)) return [];
    return [{ ...participant, local: false, stale: true }];
  });
  return candidates.length === 1 ? candidates[0] : undefined;
}

/** The committed wake intent still owns work; lack of notification is not cancellation. */
export class ResidentWakePending extends Error {
  readonly code = "RESIDENT_WAKE_PENDING";
  constructor(readonly root: string, message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "ResidentWakePending";
  }
}

/** A native watcher failed after being created; callers may make one bounded re-probe. */
export class ResidentWakeWatchError extends Error {
  readonly code = "RESIDENT_WAKE_WATCH_ERROR";
  constructor(readonly root: string, message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "ResidentWakeWatchError";
  }
}

const wakeWatchProofs = new Map<string, Promise<void>>();
const wakeWatchClosers = new Map<string, () => Promise<void>>();
const wakeWatchClosing = new Map<string, Set<Promise<void>>>();
const wakeWatchErrors = new Map<string, Set<() => void>>();

/** Subscribe without opening a watcher or arming a timer. */
export function subscribeResidentWakeWatchErrors(root: string, listener: () => void): () => Promise<void> {
  root = path.resolve(root);
  let listeners = wakeWatchErrors.get(root);
  if (!listeners) wakeWatchErrors.set(root, listeners = new Set());
  listeners.add(listener);
  return async () => {
    listeners.delete(listener);
    if (!listeners.size) {
      wakeWatchErrors.delete(root);
      // The execution owner, not the process-global proof cache, owns this handle.
      // In particular Windows cannot remove the residency root while it is watched.
      const close = wakeWatchClosers.get(root);
      wakeWatchClosers.delete(root);
      wakeWatchProofs.delete(root);
      if (close) await close();
      await Promise.all(wakeWatchClosing.get(root) ?? []);
    }
  };
}

/** One proof per root/process. Only a native watcher error invalidates a cached result. */
export function assertResidentWakeWatch(root: string): Promise<void> {
  root = path.resolve(root);
  const cached = wakeWatchProofs.get(root);
  if (cached) return cached;
  const name = `.wake-watch-${randomUUID()}`;
  const probe = path.join(root, name);
  const proof = new Promise<void>((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    let proven = false;
    let closed: Promise<void> | undefined;
    const closeWatcher = (): Promise<void> => {
      if (closed) return closed;
      if (!watcher) return Promise.resolve();
      const closingWatcher = watcher;
      closed = new Promise<void>(resolve => closingWatcher.once("close", resolve));
      let pending = wakeWatchClosing.get(root);
      if (!pending) wakeWatchClosing.set(root, pending = new Set());
      pending.add(closed);
      void closed.then(() => {
        pending.delete(closed!);
        if (!pending.size) wakeWatchClosing.delete(root);
      });
      closingWatcher.close();
      return closed;
    };
    const finish = (error?: unknown): void => {
      if (settled) return;
      settled = true;
      if (deadline) clearTimeout(deadline);
      fs.rmSync(probe, { force: true });
      if (error) { void closeWatcher(); wakeWatchClosers.delete(root); reject(error); }
      else { proven = true; watcher?.unref?.(); resolve(); }
    };
    wakeWatchClosers.set(root, () => {
      if (!settled) finish(new Error("Resident wake watcher closed with its host"));
      return closeWatcher();
    });
    try {
      watcher = fs.watch(root, (_event, filename) => {
        if (!settled && (filename === null || String(filename) === name) && fs.existsSync(probe)) finish();
      });
      watcher.once("error", error => {
        wakeWatchProofs.delete(root);
        if (proven) { void closeWatcher(); wakeWatchClosers.delete(root); }
        else finish(new ResidentWakeWatchError(root, `Resident wake watcher failed: ${String(error)}`, error));
        for (const listener of wakeWatchErrors.get(root) ?? []) listener();
      });
      deadline = setTimeout(() => finish(new Error("Resident wake watcher did not notify")), 1_000);
      fs.writeFileSync(probe, "", { flag: "wx", mode: 0o600 });
    } catch (error) { finish(error); }
  });
  wakeWatchProofs.set(root, proof);
  return proof;
}

export class ResidentWakeStartupFailed extends Error {
  readonly code = "RESIDENT_WAKE_STARTUP_FAILED";
  constructor(readonly root: string, message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "ResidentWakeStartupFailed";
  }
}

export const residentStartupReady = (root: string): boolean => {
  const owner = readWakeJson<ResidentHostOwner>(path.join(root, "owner.json"));
  const receipt = readWakeJson<{ token?: string }>(path.join(root, "maintenance-ready.json"));
  return !!owner && residentOwnerLive(root) && !residentOwnerSleeping(root) &&
    (owner.maintenanceReady !== 1 || receipt?.token === owner.token);
};

/** Subscribe before reading. Owned child IPC/exit survives unavailable or silent fs.watch. */
export function waitResidentChange(root: string, ready: () => boolean, timeoutMs: number, failure: string,
  child?: ChildProcess): Promise<void> {
  return new Promise((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const finish = (error?: unknown): void => {
      if (settled) return;
      settled = true;
      watcher?.close();
      child?.removeListener("message", message);
      child?.removeListener("exit", exited);
      child?.removeListener("error", failed);
      if (deadline) clearTimeout(deadline);
      if (error) reject(error); else resolve();
    };
    const check = (): void => {
      // An owned startup is proven by the child, never an intermediate file write.
      // Files get exactly one fallback read at the deadline, even with healthy watches.
      if (settled || child) return;
      try { if (ready()) finish(); } catch (error) { finish(error); }
    };
    const reread = (cause: unknown): void => {
      if (settled) return;
      // One direct state read covers lost events and watcher failure. Never poll.
      try { finish(ready() ? undefined : new ResidentWakePending(root, failure, cause)); }
      catch (error) { finish(new ResidentWakePending(root, failure, error)); }
    };
    const message = (value: unknown): void => {
      const receipt = value as { event?: string; root?: string; token?: string; reason?: string } | null;
      if (!receipt || receipt.root !== root) return;
      if (receipt.event === "resident-ready" && typeof receipt.token === "string" && receipt.token.length) finish();
      else if (receipt.event === "resident-wake-config-mismatch") finish(new ResidentWakeConfigMismatch(root));
      else if (receipt.event === "resident-startup-failed") failed(new Error(receipt.reason ?? failure));
      else if (receipt.event === "resident-wake-pending") finish(new ResidentWakePending(root, receipt.reason ?? failure));
    };
    const failed = (error: Error): void => finish(new ResidentWakeStartupFailed(root, error.message, error));
    const exited = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (settled) return;
      failed(new Error(`Resident startup child exited (${signal ?? code ?? "unknown"}) before ready`));
    };
    child?.on("message", message);
    child?.once("exit", exited);
    child?.once("error", failed);
    // A failed watch is only a failed notification channel. Keep native child outcomes armed.
    const watchFailed = (error: unknown): void => {
      if (child) { watcher?.close(); watcher = undefined; }
      else reread(error);
    };
    deadline = setTimeout(() => reread(new Error(failure)), timeoutMs);
    try {
      watcher = fs.watch(root, check);
      watcher.once("error", watchFailed);
      check();
    } catch (error) { watchFailed(error); }
    if (child && (child.exitCode !== null || child.signalCode !== null)) exited(child.exitCode, child.signalCode);
  });
}

/** Reuse the installed Node launcher, never keep a polling hostd warm. */
export async function requestResidentWake(root: string, delivery: { id: string; sequence?: number },
  launch: ((configPath: string, config: ResidentHostConfig) => Promise<void>) | undefined,
  onIntentWritten: ((request: ResidentWakeRequest) => Promise<void>) | undefined, admission: ResidentWakeAdmission): Promise<void> {
  const assertAdmission = (): void => { if (!admission.allowed()) throw new ResidentWakeAdmissionClosed(); };
  assertAdmission();
  const configPath = path.join(root, "config.json");
  const config = assertResidentWakeConfig(root);
  // Short lock shared with the launcher's final snapshot AND release of lifetime wake.lock.
  // A publisher after that snapshot cannot write intent until the launcher has released custody.
  // Windows has no POSIX wake launcher/custody holder (durable native wake remains
  // unsupported). Preserve pure routing/injected-launch tests without weakening POSIX fences.
  const fd = process.platform === "win32" ? undefined :
    await lockFile(residentWakeIntentLockPath(root), 90, process.platform === "linux");
  let written: ResidentWakeRequest | undefined;
  let warm = false;
  try {
    assertAdmission();
    assertResidentWakeConfig(root, config);
    warm = residentOwnerLive(root) && !residentOwnerSleeping(root);
    const previous = readWakeJson<ResidentWakeRequest>(residentWakeRequestPath(root));
    const request = previous?.sequence !== undefined && delivery.sequence !== undefined && previous.sequence > delivery.sequence
      ? previous : { format: 1 as const, ...delivery, requestedAt: Date.now() };
    const writeIntent = (): void => {
      assertAdmission(); // Live owner admission, inside mesh custody after the intent-lock wait.
      // Persist AFTER mesh commit and BEFORE spawn. The existing cursor owns delivery.
      writeJsonAtomic(residentWakeRequestPath(root), request, { durable: true });
      written = request;
    };
    await admission.exclusive(writeIntent);
  } finally { if (fd !== undefined) fs.closeSync(fd); }
  if (written) await onIntentWritten?.(written);
  assertAdmission();
  if (warm && residentOwnerLive(root) && !residentOwnerSleeping(root)) return; // Recheck after the mesh wait.
  // Spawn is outside the short transaction. A busy contender is covered by the holder's final check.
  assertResidentWakeConfig(root, config);
  if (launch) return launch(configPath, config);
  const { scriptSpawnArgs } = await import("../agents/transports/process-utils.js");
  const [runtime, ...args] = await scriptSpawnArgs(path.join(path.dirname(config.fabricExtensionPath), "residency", "launcher.js"),
    ["--config", configPath, "--wake"]);
  assertAdmission(); // Runtime preparation may have yielded to staging, drain, handover or close.
  assertResidentWakeConfig(root, config); // the async runtime lookup may have yielded to a config rewrite
  const child = spawn(runtime!, args, { cwd: config.cwd, detached: process.platform !== "win32",
    env: childProcessEnvironment(), stdio: ["ignore", "ignore", "ignore", "ipc"] });
  try {
    await waitResidentChange(root, () => residentStartupReady(root), 90_000,
      "Resident wake startup pending", child);
  } finally {
    // The lifetime launcher retains host custody. Only this bounded startup channel ends.
    if (child.connected) child.disconnect();
    child.unref();
  }
}

export interface ResidentWakeRecovery {
  config: Pick<ResidentHostConfig, "rootId" | "residencyRoot">;
  /** Fresh registry-owned actors, not retained routing metadata. */
  ownedActors: readonly FabricActorInfo[];
  batch: ResidentWakeRecoveryBatch;
  ignored: number;
}

/** Startup records are hints, never authority to launch another resident. Validate
 * the exact archived event, current registry lineage, route and replay cursor. */
const recoveryDeliveryPending = (meshRoot: string, root: string, routes: ResidentWakeRoutes,
  delivery: WakeDelivery | undefined, recovery: ResidentWakeRecovery, lifecycle: readonly { from?: string; to?: string; events?: string[] }[]): boolean => {
  if (root !== recovery.config.residencyRoot || routes.rootId !== recovery.config.rootId || !delivery ||
      !Number.isSafeInteger(delivery.sequence) || delivery.sequence! < 1) return false;
  const owned = recovery.ownedActors.filter(actor => actor.rootId === recovery.config.rootId && actor.residency === "durable");
  const actors = routes.actors.filter(route => owned.some(actor => actor.id === route.id));
  if (!actors.length) return false;
  try {
    const entry = MeshArchive.fromRoot(meshRoot)?.lookupEntry(delivery.sequence!, RESIDENT_WAKE_RECOVERY_MAX_BYTES - recovery.batch.bytes);
    if (!entry?.committed || entry.event.id !== delivery.id || entry.event.sequence !== delivery.sequence) return false;
    recovery.batch.bytes += Buffer.byteLength(entry.line);
    return actors.some(actor => {
      if (!residentDeliveryMatches({ ...routes, actors: [actor] }, entry.event, lifecycle)) return false;
      const scope = owned.find(row => row.id === actor.id)!.scope;
      const cursorFile = path.join(root, `actor-mesh-cursor.json.${scope}`);
      const cursor = readWakeJson<{ format?: number; cursor?: number; last?: { sequence?: number } }>(
        fs.existsSync(cursorFile) ? cursorFile : path.join(root, "actor-mesh-cursor.json"));
      // No saved cursor means a new monitor starts at the tail, not behind this
      // event. Only an actual checkpoint (legacy no-anchor checkpoints replay
      // conservatively from sequence zero) can prove a pending delivery.
      return !!cursor && cursor.format === 1 && typeof cursor.cursor === "number" && Number.isFinite(cursor.cursor) &&
        cursor.cursor >= 0 && (cursor.last?.sequence === undefined ||
          (Number.isSafeInteger(cursor.last.sequence) && cursor.last.sequence >= 0 && cursor.last.sequence < delivery.sequence!));
    });
  } catch { return false; } // Unknown proof stays durable, but cannot authorize a wake.
};

/** Drain durable pending wakes after publication (including batches/bridges), or with no
 * events on resident startup. Recovery does not depend on the publishing process surviving. */
export async function wakeResidentActors(mesh: Pick<MeshStore, "root" | "listAll" | "exclusive">, events: readonly MeshEvent[],
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>, recovery?: ResidentWakeRecovery,
  canWake: () => boolean = () => true): Promise<void> {
  if (!canWake()) return;
  const admission: ResidentWakeAdmission = { allowed: canWake, exclusive: mesh.exclusive.bind(mesh) };
  const lifecycle = events.some(event => event.topic === "fabric.participant.lifecycle")
    ? mesh.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as {
      from?: string; to?: string; events?: string[];
    }) : [];
  // Unrelated publications own no pending/backlog custody. Recovery keeps its
  // explicit batch path, including startup calls with no events.
  const publicationRoots = recovery ? [] : residentDirectories(mesh.root).filter(root => {
    try {
      const routes = retainedRoutesAt(root);
      return routes && events.some(event => residentDeliveryMatches(routes, event, lifecycle));
    } catch (error) {
      // Preserve per-root isolation when retained actor metadata is malformed.
      diagnostics.warn("src/residency/wake.ts", `[pi-fabric] resident wake deferred for ${root}: ${String(error)}`);
      return false;
    }
  });
  if (!recovery && !publicationRoots.length) return;
  const pending = recovery ? recovery.batch.pending : await mesh.exclusive(() => {
    const reader = new ResidentWakeRecoveryReader(mesh.root);
    try { return reader.next().pending; } finally { reader.close(); }
  });
  // A successful wake served this request. Later admission closure must not
  // manufacture another wake; only an ACK failure/crash leaves at-least-once work.
  const acknowledge = (root: string, delivery: WakeDelivery): Promise<void> =>
    mesh.exclusive(() => acknowledgeResidentDelivery(mesh.root, root, delivery));
  let attempted = 0;
  // A subsequent MATCHING delivery must not let a newly re-filled journal starve old overflow.
  const deferred = [...pending].filter(([, delivery]) => delivery.deferredAt !== undefined &&
    (!events.length || events.some(event => event.sequence > delivery.deferredAt!))).map(([root]) => root);
  for (const root of new Set([...deferred, ...pending.keys(), ...publicationRoots])) {
    if (!canWake()) break;
    if (!recovery && attempted >= RESIDENT_WAKE_INDEX_MAX_ROOTS) break; // Overflow stays pending for the next drain.
    let delivery: { id: string; sequence?: number } | undefined;
    let attemptedDelivery: WakeDelivery | undefined;
    let served = false;
    const onIntentWritten = async (written: ResidentWakeRequest): Promise<void> => {
      // The intent may adopt a newer watermark than this publication captured.
      // Capture only; pending remains durable through launch and its outcome.
      delivery = written;
    };
    try {
      // Reject foreign paths before opening their retained config or route files.
      if (recovery && root !== recovery.config.residencyRoot) { recovery.ignored++; continue; }
      const routes = retainedRoutesAt(root);
      if (recovery && (!routes || !recoveryDeliveryPending(mesh.root, root, routes, pending.get(root), recovery,
          mesh.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as { from?: string; to?: string; events?: string[] })))) {
        recovery.ignored++; continue;
      }
      if (!routes) continue;
      const matched = events.filter(event => residentDeliveryMatches(routes, event, lifecycle));
      // Publication owns only matching wake work. Recovery/maintenance own older nudges.
      if (!recovery && !matched.length) continue;
      if (residentOwnerLive(root) && !residentOwnerSleeping(root)) {
        const covered = pending.get(root);
        if (covered) {
          delivery = covered;
          if (await acknowledgeWakeTerminal(mesh, root, covered)) continue;
          attempted++;
          attemptedDelivery = covered;
          await requestResidentWake(root, covered, launch, onIntentWritten, admission);
          served = true;
          await acknowledge(root, delivery);
        }
        continue;
      }
      const last = matched.at(-1);
      const request = readWakeJson<ResidentWakeRequest>(residentWakeRequestPath(root));
      const sleeping = readWakeJson<{ request?: unknown }>(residentSleepingPath(root));
      const failed = readWakeJson<{ delivery?: { id: string; sequence?: number } }>(wakeFailurePath(root));
      // A new matching delivery keeps the existing awaited wake behavior, even during backoff.
      delivery = last ? { id: last.id, sequence: last.sequence } : pending.get(root) ?? failed?.delivery ??
        (request && JSON.stringify(sleeping?.request) !== JSON.stringify(request) ? request : undefined);
      if (delivery) {
        if (await acknowledgeWakeTerminal(mesh, root, delivery)) continue;
        attempted++;
        attemptedDelivery = delivery;
        await requestResidentWake(root, delivery, launch, onIntentWritten, admission);
        served = true;
        await acknowledge(root, delivery);
        await clearWakeFailure(root, delivery);
      }
    } catch (error) {
      if (error instanceof ResidentWakeAdmissionClosed) {
        // Refusal is not a publisher attempt failure. All prelaunch gates stay
        // in requestResidentWake; the original pending watermark owns the retry.
        return;
      }
      // One failed root must never abort the others or turn a committed publish into a retry.
      // Failed publication wakes keep pending even when a backoff receipt is
      // durable. In particular, a post-success ACK error must NEVER run the
      // failure recorder (which could otherwise turn that error into an ACK).
      if (delivery && !served && await recordResidentWakeFailure(mesh, root, attemptedDelivery ?? delivery,
          error, Date.now(), undefined, false)) continue;
      diagnostics.warn("src/residency/wake.ts", `[pi-fabric] resident wake deferred for ${root}: ${String(error)}`);
    }
  }
}

/** Retry durable pending/failure nudges on the existing retention cadence. No timer is armed here. */
export async function retryResidentWakes(mesh: ResidentWakeMesh,
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>, now = Date.now(),
  canWake: () => boolean = () => true): Promise<void> {
  if (!canWake()) return;
  const key = path.resolve(mesh.root);
  const running = residentWakeRetrySweeps.get(key);
  if (running) return running;
  const sweep = (async () => {
    const candidates = await mesh.exclusive(() => {
      if (!canWake()) return [];
      return residentWakeRetryCandidates(mesh.root); // Bounded pages; next sweep resumes the cursor.
    });
    if (!canWake()) return;
    const attempts: Array<{ root: string; delivery: WakeDelivery; claimId: string }> = [];
    for (const root of candidates.slice(0, RESIDENT_WAKE_RETRY_MAX_CANDIDATES)) {
      if (!canWake()) break;
      const claimId = randomUUID();
      try {
        // Most receipt-directory candidates are idle or still backed off. Observe
        // those without taking a native per-root writer lock on every sweep.
        const observedPrior = wakeFailureAt(root);
        const observedRoutes = retainedRoutesAt(root);
        const observedCurrent = await mesh.exclusive(() => canWake() ? residentUnacknowledgedDelivery(mesh.root, root) : undefined);
        if (!canWake()) break;
        const observedDelivery = observedCurrent && (!observedPrior?.delivery || deliveryCovers(observedCurrent, observedPrior.delivery))
          ? observedCurrent : observedPrior?.delivery;
        if (observedRoutes && observedDelivery && await acknowledgeWakeTerminal(mesh, root, observedDelivery)) continue;
        if (observedRoutes && ((observedPrior?.nextRetryAt !== undefined && observedPrior.nextRetryAt > now) ||
            (!observedCurrent && !observedPrior?.delivery))) continue;
        if (!observedRoutes) {
          try { fs.lstatSync(path.join(root, "wake-routes.json")); continue; }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
          try { fs.lstatSync(root); }
          catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
            if (observedCurrent) await mesh.exclusive(() => { if (canWake()) acknowledgeResidentDelivery(mesh.root, root, observedCurrent); });
            continue; // An absent residency directory has no receipt requiring root custody.
          }
        }
        const delivery = await withWakeReceipt(root, async () => {
          if (!canWake()) return;
          const prior = wakeFailureAt(root);
          const routes = retainedRoutesAt(root);
          let retired = false;
          if (!routes) {
            // Only ENOENT retires routes; malformed/unreadable metadata retains work.
            try { fs.lstatSync(path.join(root, "wake-routes.json")); }
            catch (error) {
              if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
              retired = true;
            }
          }
          // Revalidate only this keyed watermark under mesh custody. Config,
          // receipt parsing and durable receipt writes do not hold the mesh lock.
          const current = await mesh.exclusive(() => canWake() ? residentUnacknowledgedDelivery(mesh.root, root) : undefined);
          if (!canWake()) return;
          if (retired) {
            if (current) await mesh.exclusive(() => { if (canWake()) acknowledgeResidentDelivery(mesh.root, root, current); });
            if (canWake() && prior) fs.rmSync(wakeFailurePath(root), { force: true });
            return;
          }
          if (!routes || (prior?.nextRetryAt !== undefined && prior.nextRetryAt > now)) return;
          let delivery = prior?.delivery;
          if (current && (!delivery || (current.sequence !== undefined &&
              (delivery.sequence === undefined || current.sequence >= delivery.sequence)))) delivery = current;
          if (!delivery) return;
          writeJsonAtomic(wakeFailurePath(root), { ...prior, format: 1, delivery, claimId,
            nextRetryAt: now + RESIDENT_WAKE_ATTEMPT_LEASE_MS }, { durable: true });
          return delivery;
        });
        if (delivery) attempts.push({ root, delivery, claimId });
      } catch (error) {
        // Without durable claim custody, leave the index/receipt for a later sweep.
        diagnostics.warn("src/residency/wake.ts", `[pi-fabric] resident wake retry deferred for ${root}: ${String(error)}`);
      }
    }
    await Promise.all(attempts.map(async ({ root, delivery, claimId }) => {
      let served: WakeDelivery = delivery;
      try {
        if (await acknowledgeWakeTerminal(mesh, root, delivery, claimId)) return;
        await requestResidentWake(root, delivery, launch, written => withWakeReceipt(root, async () => {
          served = written;
          // The durable claim owns only the original watermark until launch succeeds.
          if (wakeFailureAt(root)?.claimId === claimId)
            await mesh.exclusive(() => acknowledgeResidentDelivery(mesh.root, root, delivery));
        }), { allowed: canWake, exclusive: mesh.exclusive.bind(mesh) });
        await withWakeReceipt(root, async () => {
          if (wakeFailureAt(root)?.claimId !== claimId) return;
          await mesh.exclusive(() => acknowledgeResidentDelivery(mesh.root, root, served));
          fs.rmSync(wakeFailurePath(root), { force: true });
        });
      } catch (error) {
        if (error instanceof ResidentWakeAdmissionClosed) {
          // Revocation is not a root failure. Retain the consolidated nudge, release
          // only our claim, and make it eligible when the owner's gate opens again.
          await withWakeReceipt(root, async () => {
            const current = wakeFailureAt(root);
            if (current?.claimId !== claimId) return;
            const { claimId: _claimId, ...retained } = current;
            writeJsonAtomic(wakeFailurePath(root), { ...retained, nextRetryAt: now }, { durable: true });
          });
          return;
        }
        if (await recordResidentWakeFailure(mesh, root, delivery, error, Math.max(now, Date.now()), claimId)) return;
        diagnostics.warn("src/residency/wake.ts", `[pi-fabric] resident wake deferred for ${root}: ${String(error)}`);
      }
    }));
  })();
  residentWakeRetrySweeps.set(key, sweep);
  try { await sweep; }
  finally { if (residentWakeRetrySweeps.get(key) === sweep) residentWakeRetrySweeps.delete(key); }
}

/** Explicit wake helper; message routing never calls it before command admission/commit. */
export async function wakeDormantActor(meshRoot: string, id: string, admission?: ResidentWakeAdmission): Promise<boolean> {
  const candidates = residentDirectories(meshRoot).flatMap(root => {
    const routes = retainedRoutesAt(root);
    return routes?.actors.filter(actor => actor.id === id || actor.name === id || actor.id.startsWith(id))
      .map(actor => ({ root, actor })) ?? [];
  });
  if (candidates.length !== 1) return false;
  const { root } = candidates[0]!;
  if (residentOwnerLive(root) && !residentOwnerSleeping(root)) return false;
  // A root path alone carries no live owner authority. Legacy no-match probes
  // remain harmless, but an actual direct wake requires the caller's mesh gate.
  if (!admission) throw new ResidentWakeAdmissionClosed();
  await requestResidentWake(root, { id: `direct:${id}` }, undefined, undefined, admission);
  await waitResidentChange(root, () => {
    const owner = readWakeJson<ResidentHostOwner>(path.join(root, "owner.json"));
    const ready = readWakeJson<{ token?: string }>(path.join(root, "maintenance-ready.json"));
    return !!owner && residentOwnerLive(root) && !residentOwnerSleeping(root) &&
      (owner.maintenanceReady !== 1 || ready?.token === owner.token);
  }, 90_000, `Timed out waking dormant Fabric actor ${id}; delivery was not published`);
  return true;
}
