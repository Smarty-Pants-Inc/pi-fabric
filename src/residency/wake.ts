import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import type { MeshEvent, MeshStore } from "../mesh/store.js";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../mesh/archive.js";
import { residentProcessAlive } from "./process-identity.js";
import { lockFile } from "./file-lock.js";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";
import type { FabricActorInfo } from "../actors/types.js";
import type { FabricParticipantInfo, FabricParticipantRecord } from "../topology/types.js";

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
  actors: Array<{ id: string; name: string; topics: string[]; participant?: FabricParticipantRecord }>;
}
export interface ResidentWakeRequest { format: 1; id: string; sequence?: number; requestedAt: number }
export const residentSleepingPath = (root: string): string => path.join(root, "sleeping.json");
export const residentWakeRequestPath = (root: string): string => path.join(root, "wake-request.json");
export const residentWakeIntentLockPath = (root: string): string => path.join(root, "wake-intent.lock");
const wakeFailurePath = (root: string): string => path.join(root, "wake-failure.json");
export const readWakeJson = <T>(file: string): T | undefined => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || (process.getuid && stat.uid !== process.getuid())) return undefined;
    return JSON.parse(fs.readFileSync(fd, "utf8")) as T;
  } catch { return undefined; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
};
export const residentOwnerLive = (root: string): boolean => {
  const owner = readWakeJson<ResidentHostOwner>(path.join(root, "owner.json"));
  return !!owner && residentProcessAlive(owner.pid, owner.processStartTime);
};
export const residentOwnerSleeping = (root: string): boolean => {
  const owner = readWakeJson<ResidentHostOwner>(path.join(root, "owner.json"));
  const sleeping = readWakeJson<{ token?: string }>(residentSleepingPath(root));
  return !!owner && owner.token === sleeping?.token;
};

const routesAt = (root: string): ResidentWakeRoutes | undefined => {
  const routes = readWakeJson<ResidentWakeRoutes>(path.join(root, "wake-routes.json"));
  const config = readWakeJson<ResidentHostConfig>(path.join(root, "config.json"));
  // Never execute a path supplied by an event. Only this host's retained config is authority.
  if (routes?.format !== 1 || !Array.isArray(routes.actors) || routes.rootId !== config?.rootId ||
      typeof config.residencyRoot !== "string" || path.resolve(config.residencyRoot) !== path.resolve(root)) return undefined;
  return routes;
};
const residentDirectories = (meshRoot: string): string[] => {
  const directory = path.join(meshRoot, "residency");
  try { return fs.readdirSync(directory, { withFileTypes: true }).filter(entry => entry.isDirectory())
    .map(entry => path.join(directory, entry.name)); } catch { return []; }
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

/** Prove this root can notify before releasing any actor's warm runtime. */
export async function assertResidentWakeWatch(root: string): Promise<void> {
  const name = `.wake-watch-${randomUUID()}`;
  const probe = path.join(root, name);
  let watcher: fs.FSWatcher | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      watcher = fs.watch(root, (_event, filename) => {
        if ((filename === null || String(filename) === name) && fs.existsSync(probe)) resolve();
      });
      watcher.once("error", reject);
      deadline = setTimeout(() => reject(new Error("Resident wake watcher did not notify")), 1_000);
      fs.writeFileSync(probe, "", { flag: "wx", mode: 0o600 });
    });
  } finally {
    watcher?.close();
    if (deadline) clearTimeout(deadline);
    fs.rmSync(probe, { force: true });
  }
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
      if (settled) return;
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
      else if (receipt.event === "resident-startup-failed") failed(new Error(receipt.reason ?? failure));
      else if (receipt.event === "resident-wake-pending") finish(new ResidentWakePending(root, receipt.reason ?? failure));
    };
    const failed = (error: Error): void => finish(new ResidentWakeStartupFailed(root, error.message, error));
    const exited = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (settled) return;
      try {
        if (ready()) finish();
        else failed(new Error(`Resident startup child exited (${signal ?? code ?? "unknown"}) before ready`));
      } catch (error) { failed(error instanceof Error ? error : new Error(String(error))); }
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
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>): Promise<void> {
  const configPath = path.join(root, "config.json");
  const config = readWakeJson<ResidentHostConfig>(configPath);
  if (!config || typeof config.residencyRoot !== "string" || path.resolve(config.residencyRoot) !== path.resolve(root)) throw new Error("Invalid resident wake config");
  // Short lock shared with the launcher's final snapshot AND release of lifetime wake.lock.
  // A publisher after that snapshot cannot write intent until the launcher has released custody.
  // Windows has no POSIX wake launcher/custody holder (durable native wake remains
  // unsupported). Preserve pure routing/injected-launch tests without weakening POSIX fences.
  const fd = process.platform === "win32" ? undefined :
    await lockFile(residentWakeIntentLockPath(root), 90, process.platform === "linux");
  try {
    if (residentOwnerLive(root) && !residentOwnerSleeping(root)) return;
    const previous = readWakeJson<ResidentWakeRequest>(residentWakeRequestPath(root));
    const request = previous?.sequence !== undefined && delivery.sequence !== undefined && previous.sequence > delivery.sequence
      ? previous : { format: 1 as const, ...delivery, requestedAt: Date.now() };
    // Persist AFTER mesh commit and BEFORE spawn. The existing cursor owns delivery.
    writeJsonAtomic(residentWakeRequestPath(root), request, { durable: true });
  } finally { if (fd !== undefined) fs.closeSync(fd); }
  // Spawn is outside the short transaction. A busy contender is covered by the holder's final check.
  if (launch) return launch(configPath, config);
  const { scriptSpawnArgs } = await import("../agents/transports/process-utils.js");
  const [runtime, ...args] = await scriptSpawnArgs(path.join(path.dirname(config.fabricExtensionPath), "residency", "launcher.js"),
    ["--config", configPath, "--wake"]);
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

/** Called only after the event log's durability barrier, including batch/bridge publishers. */
export async function wakeResidentActors(mesh: Pick<MeshStore, "root" | "listAll">, events: readonly MeshEvent[],
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>): Promise<void> {
  const lifecycle = events.some(event => event.topic === "fabric.participant.lifecycle")
    ? mesh.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as {
      from?: string; to?: string; events?: string[];
    }) : [];
  for (const root of residentDirectories(mesh.root)) {
    let delivery: { id: string; sequence?: number } | undefined;
    try {
      const routes = routesAt(root);
      if (!routes || (residentOwnerLive(root) && !residentOwnerSleeping(root))) continue;
      const matched = events.filter(event => {
        const command = event.topic === "fabric.control.command" ? event.data as { targetId?: string } | undefined : undefined;
        const source = event.topic === "fabric.participant.lifecycle" ? event.data as { source?: { id?: string }; event?: string } | undefined : undefined;
        return routes.actors.some(actor =>
          (command ? command.targetId === actor.id && event.to === routes.hostId :
            (event.to === actor.id || event.to === actor.name || actor.topics.includes(event.topic)) &&
            (event.from.id !== actor.id || event.to === actor.id || event.to === actor.name)) ||
          (source && lifecycle.some(subscription => subscription.to === actor.id && subscription.from === source.source?.id &&
            typeof source.event === "string" && subscription.events?.includes(source.event))));
      });
      const last = matched.at(-1);
      const request = readWakeJson<ResidentWakeRequest>(residentWakeRequestPath(root));
      const sleeping = readWakeJson<{ request?: unknown }>(residentSleepingPath(root));
      const failed = readWakeJson<{ delivery?: { id: string; sequence?: number } }>(wakeFailurePath(root));
      // Next delivery is an event-driven retry, even when its own topic does not match this root.
      delivery = last ? { id: last.id, sequence: last.sequence } : failed?.delivery ??
        (request && JSON.stringify(sleeping?.request) !== JSON.stringify(request) ? request : undefined);
      if (delivery) {
        await requestResidentWake(root, delivery, launch);
        fs.rmSync(wakeFailurePath(root), { force: true });
      }
    } catch (error) {
      // One failed root must never abort the others or turn a committed publish into a retry.
      // The durable nudge survives spawn failure; a separate receipt retains write failures too.
      if (delivery) {
        try { writeJsonAtomic(wakeFailurePath(root), { format: 1, delivery, error: String(error), failedAt: Date.now() }, { durable: true }); }
        catch { /* Storage unavailable: the committed archived delivery remains authoritative. */ }
      }
      console.warn(`[pi-fabric] resident wake deferred for ${root}: ${String(error)}`);
    }
  }
}

/** Explicit wake helper; message routing never calls it before command admission/commit. */
export async function wakeDormantActor(meshRoot: string, id: string): Promise<boolean> {
  const candidates = residentDirectories(meshRoot).flatMap(root => {
    const routes = routesAt(root);
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
