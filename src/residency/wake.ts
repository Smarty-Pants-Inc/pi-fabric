import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import type { MeshEvent, MeshStore } from "../mesh/store.js";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../mesh/archive.js";
import { lockFile } from "./file-lock.js";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";
import type { FabricActorInfo } from "../actors/types.js";
import type { FabricParticipantInfo, FabricParticipantRecord } from "../topology/types.js";
import { readWakeJson, residentOwnerLive, residentOwnerSleeping, routesAt, residentDirectories, residentDeliveryMatches,
  acknowledgeResidentDelivery, assertResidentWakeConfig, retainedRoutesAt, ResidentWakeConfigMismatch, RESIDENT_WAKE_INDEX_MAX_ROOTS, ResidentWakeRecoveryReader, RESIDENT_WAKE_RECOVERY_MAX_BYTES, type ResidentWakeRecoveryBatch, type WakeDelivery } from "./wake-index.js";
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
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>,
  onIntentWritten?: (request: ResidentWakeRequest) => Promise<void>): Promise<void> {
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
    warm = residentOwnerLive(root) && !residentOwnerSleeping(root);
    const previous = readWakeJson<ResidentWakeRequest>(residentWakeRequestPath(root));
    const request = previous?.sequence !== undefined && delivery.sequence !== undefined && previous.sequence > delivery.sequence
      ? previous : { format: 1 as const, ...delivery, requestedAt: Date.now() };
    // Persist AFTER mesh commit and BEFORE spawn. The existing cursor owns delivery.
    writeJsonAtomic(residentWakeRequestPath(root), request, { durable: true });
    written = request;
  } finally { if (fd !== undefined) fs.closeSync(fd); }
  if (written) await onIntentWritten?.(written);
  if (warm) return; // A racing warm owner covers this durable request without a new child.
  // Spawn is outside the short transaction. A busy contender is covered by the holder's final check.
  assertResidentWakeConfig(root, config);
  if (launch) return launch(configPath, config);
  const { scriptSpawnArgs } = await import("../agents/transports/process-utils.js");
  const [runtime, ...args] = await scriptSpawnArgs(path.join(path.dirname(config.fabricExtensionPath), "residency", "launcher.js"),
    ["--config", configPath, "--wake"]);
  assertResidentWakeConfig(root, config); // the async runtime lookup may have yielded to a config rewrite
  const child = spawn(runtime!, args, { cwd: config.cwd, detached: process.platform !== "win32",
    stdio: ["ignore", "ignore", "ignore", "ipc"] });
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
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>, recovery?: ResidentWakeRecovery): Promise<void> {
  const lifecycle = events.some(event => event.topic === "fabric.participant.lifecycle")
    ? mesh.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as {
      from?: string; to?: string; events?: string[];
    }) : [];
  const pending = recovery ? recovery.batch.pending : await mesh.exclusive(() => {
    const reader = new ResidentWakeRecoveryReader(mesh.root);
    try { return reader.next().pending; } finally { reader.close(); }
  });
  const acknowledge = (root: string, delivery: WakeDelivery): Promise<void> =>
    mesh.exclusive(() => acknowledgeResidentDelivery(mesh.root, root, delivery));
  let attempted = 0;
  // A subsequent MATCHING delivery must not let a newly re-filled journal starve old overflow.
  const deferred = [...pending].filter(([, delivery]) => delivery.deferredAt !== undefined &&
    (!events.length || events.some(event => event.sequence > delivery.deferredAt!))).map(([root]) => root);
  for (const root of new Set([...deferred, ...pending.keys(), ...(recovery ? [] : residentDirectories(mesh.root))])) {
    if (!recovery && attempted >= RESIDENT_WAKE_INDEX_MAX_ROOTS) break; // Overflow stays pending for the next drain.
    let delivery: { id: string; sequence?: number } | undefined;
    try {
      // Reject foreign paths before opening their retained config or route files.
      if (recovery && root !== recovery.config.residencyRoot) { recovery.ignored++; continue; }
      const routes = retainedRoutesAt(root);
      if (recovery && (!routes || !recoveryDeliveryPending(mesh.root, root, routes, pending.get(root), recovery,
          mesh.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as { from?: string; to?: string; events?: string[] })))) {
        recovery.ignored++; continue;
      }
      if (!routes) continue;
      if (residentOwnerLive(root) && !residentOwnerSleeping(root)) {
        const covered = pending.get(root);
        if (covered) {
          delivery = covered;
          attempted++;
          await requestResidentWake(root, covered, launch, written => acknowledge(root, written));
        }
        continue;
      }
      const matched = events.filter(event => {
        return residentDeliveryMatches(routes, event, lifecycle);
      });
      const last = matched.at(-1);
      const request = readWakeJson<ResidentWakeRequest>(residentWakeRequestPath(root));
      const sleeping = readWakeJson<{ request?: unknown }>(residentSleepingPath(root));
      const failed = readWakeJson<{ delivery?: { id: string; sequence?: number } }>(wakeFailurePath(root));
      // Next delivery is an event-driven retry, even when its own topic does not match this root.
      delivery = last ? { id: last.id, sequence: last.sequence } : pending.get(root) ?? failed?.delivery ??
        (request && JSON.stringify(sleeping?.request) !== JSON.stringify(request) ? request : undefined);
      if (delivery) {
        attempted++;
        await requestResidentWake(root, delivery, launch, written => acknowledge(root, written));
        fs.rmSync(wakeFailurePath(root), { force: true });
      }
    } catch (error) {
      // One failed root must never abort the others or turn a committed publish into a retry.
      // The durable nudge survives spawn failure; a separate receipt retains write failures too.
      if (delivery) {
        try {
          writeJsonAtomic(wakeFailurePath(root), { format: 1, delivery, error: String(error), failedAt: Date.now() }, { durable: true });
          await acknowledge(root, delivery);
        } catch { /* Double failure: write nothing else. The archive-coupled index remains pending. */ }
      }
      console.warn(`[pi-fabric] resident wake deferred for ${root}: ${String(error)}`);
    }
  }
}

/** Explicit wake helper; message routing never calls it before command admission/commit. */
export async function wakeDormantActor(meshRoot: string, id: string): Promise<boolean> {
  const candidates = residentDirectories(meshRoot).flatMap(root => {
    const routes = retainedRoutesAt(root);
    return routes?.actors.filter(actor => actor.id === id || actor.name === id || actor.id.startsWith(id))
      .map(actor => ({ root, actor })) ?? [];
  });
  if (candidates.length !== 1) return false;
  const { root } = candidates[0]!;
  if (residentOwnerLive(root) && !residentOwnerSleeping(root)) return false;
  await requestResidentWake(root, { id: `direct:${id}` });
  await waitResidentChange(root, () => {
    const owner = readWakeJson<ResidentHostOwner>(path.join(root, "owner.json"));
    const ready = readWakeJson<{ token?: string }>(path.join(root, "maintenance-ready.json"));
    return !!owner && residentOwnerLive(root) && !residentOwnerSleeping(root) &&
      (owner.maintenanceReady !== 1 || ready?.token === owner.token);
  }, 90_000, `Timed out waking dormant Fabric actor ${id}; delivery was not published`);
  return true;
}
