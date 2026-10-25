/**
 * Operator actions on one NATS link (smarty-dev#15310, A' lane L5): the link's state-directory lock and
 * `nats-link withdraw`, the tested rollback.
 *
 * The lock (`<stateDir>/link.lock`, 0600) names the one process that owns the link's cursor, ledger and
 * mirrors: `start` holds it while the link runs, `withdraw` while it withdraws. A second start on the same
 * state directory is refused; a lock whose process is gone (pid dead, or reused with another start time) is
 * stale and replaced.
 *
 * Withdraw stops the running link (SIGTERM to the lock's process, verified by its start time), then deletes
 * everything the link mirrored: on NATS every lease and presence key under the link's own machine (the
 * standard grant lets it write nowhere else, so that is all it ever wrote), leases first so routing sees the
 * roots gone at once; on the file mesh every record mirrored under the link's name (StoreBridgeSide.withdraw,
 * as `mesh-bridge` decommissions a link). The link must not be restarted until then: use `Restart=on-failure`
 * (a requested stop exits 0) or stop the unit first.
 */
import fs from "node:fs";
import path from "node:path";
import { processStartTime, residentProcessAlive } from "../residency/process-identity.js";
import type { TransportKv } from "../transport/types.js";
import { StoreBridgeSide } from "./bridge.js";
import { readMeshStateMovedMarker } from "./backend-fence.js";
import { assertFabricMeshRoot, type NatsLinkConfig } from "./bridge-nats-config.js";
import { openNatsPresence, type NatsRootsSource, type OpenNatsPresence } from "./bridge-nats-roots.js";
import { MeshStore } from "./store.js";

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts).
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;

export interface NatsLinkLockRecord {
  readonly v: 1;
  readonly link: string;
  readonly purpose: "start" | "withdraw";
  readonly pid: number;
  /** Linux /proc start time of `pid`, so a reused pid is never taken for the holder. */
  readonly startTime?: string;
  readonly at: string;
}

/** Another live process owns the link's state directory. */
export class NatsLinkBusyError extends Error {
  readonly code = "FABRIC_NATS_LINK_BUSY";
  constructor(readonly holder: NatsLinkLockRecord) {
    super(`link ${holder.link} is held by ${holder.purpose} pid ${holder.pid} since ${holder.at}`);
    this.name = "NatsLinkBusyError";
  }
}

export const natsLinkLockPath = (config: Pick<NatsLinkConfig, "stateDir">): string => path.join(config.stateDir, "link.lock");

const parseLock = (text: string): NatsLinkLockRecord | undefined => {
  try {
    const value = JSON.parse(text) as Partial<NatsLinkLockRecord>;
    if (value.v === 1 && typeof value.link === "string" && (value.purpose === "start" || value.purpose === "withdraw") &&
      Number.isSafeInteger(value.pid) && (value.pid as number) > 0 && typeof value.at === "string" &&
      (value.startTime === undefined || typeof value.startTime === "string")) return value as NatsLinkLockRecord;
  } catch { /* A torn or foreign file is no holder. */ }
  return undefined;
};

const readLock = (file: string): NatsLinkLockRecord | undefined => {
  try { return parseLock(fs.readFileSync(file, "utf8")); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
};

/** The live process that holds the link's lock, if any. A dead or reused pid is no holder. */
export const natsLinkLockHolder = (config: Pick<NatsLinkConfig, "stateDir">): NatsLinkLockRecord | undefined => {
  const record = readLock(natsLinkLockPath(config));
  return record && residentProcessAlive(record.pid, record.startTime) ? record : undefined;
};

/**
 * Take the link's lock for this process, or throw NatsLinkBusyError naming the live holder. Exclusive
 * create decides between racing processes. A stale lock is removed only while the same file (device and
 * inode) is still there, then the exclusive create is retried once. Returns the release function, which
 * removes the lock only while it still holds this process's record.
 */
export const acquireNatsLinkLock = (config: Pick<NatsLinkConfig, "stateDir" | "link">, purpose: NatsLinkLockRecord["purpose"]): () => void => {
  const file = natsLinkLockPath(config);
  const startTime = processStartTime(process.pid);
  const record: NatsLinkLockRecord = { v: 1, link: config.link, purpose, pid: process.pid,
    ...(startTime === undefined ? {} : { startTime }), at: new Date().toISOString() };
  const text = `${JSON.stringify(record)}\n`;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    let fd: number;
    try {
      fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const seen = fs.lstatSync(file, { throwIfNoEntry: false });
      const held = readLock(file);
      if (held && residentProcessAlive(held.pid, held.startTime)) throw new NatsLinkBusyError(held);
      const now = fs.lstatSync(file, { throwIfNoEntry: false });
      if (seen && now && seen.dev === now.dev && seen.ino === now.ino) fs.unlinkSync(file);
      continue;
    }
    try {
      fs.writeSync(fd, text);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    return () => {
      try { if (fs.readFileSync(file, "utf8") === text) fs.unlinkSync(file); } catch { /* Already gone. */ }
    };
  }
  const held = readLock(file);
  if (held) throw new NatsLinkBusyError(held);
  throw new Error(`could not take the link lock ${file}`);
};

/** Every key under `machine` in one KV bucket, with its revision, from one complete snapshot. */
const ownKeys = async (kv: TransportKv, machine: string, signal: AbortSignal): Promise<Map<string, number>> => {
  const keys = new Map<string, number>();
  const watch = new AbortController();
  const stop = (): void => watch.abort(signal.reason);
  signal.addEventListener("abort", stop, { once: true });
  try {
    for await (const event of kv.watch<unknown>(">", watch.signal)) {
      if (event.type === "ready") return keys;
      if (event.type === "reset") { keys.clear(); continue; }
      const key = event.type === "put" ? event.entry.key : event.key;
      if (!key.startsWith(`${machine}.`)) continue;
      if (event.type === "put") keys.set(key, event.entry.revision);
      else keys.delete(key);
    }
    throw new Error("KV watch ended before its snapshot was complete");
  } finally {
    signal.removeEventListener("abort", stop);
    watch.abort();
  }
};

export interface NatsLinkWithdrawReport {
  readonly ok: boolean;
  readonly link: string;
  /** The running link this withdraw stopped, if any. */
  readonly stopped: { readonly pid: number } | null;
  readonly nats: { readonly machine: string; readonly deleted: readonly string[]; readonly moved: readonly string[] } | { readonly error: string };
  readonly file: { readonly removed: readonly string[]; readonly remaining: readonly string[] } | { readonly error: string };
  readonly problems: readonly string[];
}

export interface NatsLinkWithdrawOptions {
  readonly open?: OpenNatsPresence;
  /** Bound on the running link's stop (default 10 s, the unit's TimeoutStopSec). */
  readonly stopTimeoutMs?: number;
  /** Bound on the NATS connection and snapshot (default 10 s). */
  readonly natsTimeoutMs?: number;
  readonly kill?: (pid: number, signal: NodeJS.Signals) => void;
  readonly sleep?: (ms: number) => Promise<void>;
}

/** The records the file mesh holds under this link's name (`remoteHost`, or `bridgeVia` for a relayed one). */
const fileMirrors = (store: MeshStore, link: string): string[] =>
  store.listAll("topology/participants/", { fresh: true }).flatMap((entry) => {
    const value = entry.value as { id?: unknown; remoteHost?: unknown; bridgeVia?: unknown } | null;
    const via = typeof value?.bridgeVia === "string" && typeof value.remoteHost === "string" ? value.bridgeVia : value?.remoteHost;
    return via === link && typeof value?.id === "string" ? [value.id] : [];
  }).sort();

/**
 * The link's own NATS connection for presence and lease KV only (roots, receipt --nats, dry-run, withdraw): its
 * machine and credential under the unscoped standard agent policy, as L1's port verifies it. Never `events` or
 * `org`: those would make the connection demand the full events template (fabric.ev publish) and org grants,
 * and refuse the consume-only bridge credential an N4 config runs with, blocking the rollback on exactly the
 * N4 configs. These reads need neither.
 */
export const natsLinkRootsSource = (config: NatsLinkConfig): NatsRootsSource => ({
  servers: config.nats.servers, machine: config.nats.machine, credentials: config.nats.credentials, caFile: config.nats.caFile,
  custodyStream: config.nats.custodyStream, livenessStream: config.nats.livenessStream, pathBMachines: config.nats.pathBMachines,
});

const message = (error: unknown): string => (error instanceof Error ? error.message : String(error)).replace(/[\r\n]+/g, " ").slice(0, 500);

/**
 * The rollback: stop the running link, then withdraw its mirrors on both sides. Never withdraws while a
 * link it could not stop still runs (it would mirror them again). `ok` only when the link is stopped and both
 * sides are clean.
 */
export const withdrawNatsLink = async (config: NatsLinkConfig, options: NatsLinkWithdrawOptions = {}): Promise<NatsLinkWithdrawReport> => {
  const problems: string[] = [];
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  let stopped: { pid: number } | null = null;
  const holder = natsLinkLockHolder(config);
  if (holder?.purpose === "withdraw") throw new NatsLinkBusyError(holder);
  if (holder) {
    (options.kill ?? ((pid, signal) => process.kill(pid, signal)))(holder.pid, "SIGTERM");
    const deadline = Date.now() + (options.stopTimeoutMs ?? 10_000);
    while (natsLinkLockHolder(config) && Date.now() < deadline) await sleep(100);
    if (natsLinkLockHolder(config)) {
      return { ok: false, link: config.link, stopped: null, nats: { error: "not attempted" }, file: { error: "not attempted" },
        problems: [`the running link (pid ${holder.pid}) did not stop; nothing withdrawn`] };
    }
    stopped = { pid: holder.pid };
  }
  const release = acquireNatsLinkLock(config, "withdraw");
  try {
    let nats: NatsLinkWithdrawReport["nats"];
    try {
      const signal = AbortSignal.timeout(options.natsTimeoutMs ?? 10_000);
      const buckets = await (options.open ?? openNatsPresence)(natsLinkRootsSource(config), signal);
      try {
        const deleted: string[] = [], moved: string[] = [];
        // Leases first: without its lease no presence record routes, so receivers see the roots gone at once.
        for (const kv of [buckets.leases, buckets.presence]) {
          for (const [key, revision] of await ownKeys(kv, config.nats.machine, signal)) {
            if (await kv.delete(key, revision, signal)) deleted.push(key);
            else moved.push(key);
          }
        }
        if (moved.length) problems.push(`NATS keys changed during the withdraw (a writer is still active): ${moved.length}`);
        nats = { machine: config.nats.machine, deleted, moved };
      } finally {
        await buckets.close().catch(() => undefined);
      }
    } catch (error) {
      nats = { error: message(error) };
      problems.push("NATS withdraw failed");
    }
    let file: NatsLinkWithdrawReport["file"];
    try {
      assertFabricMeshRoot(config.file.mesh);
      const stateBackend = config.file.stateBackend ?? (readMeshStateMovedMarker(config.file.mesh) !== undefined ? "sqlite" : undefined);
      const store = new MeshStore(config.file.mesh, MAX_EVENT_BYTES, MAX_READ_EVENTS, {
        lockProtocol: config.file.lockProtocol,
        ...(stateBackend === undefined ? {} : { stateBackend }),
        ...(config.file.maxStateBytes === undefined ? {} : { maxStateBytes: config.file.maxStateBytes }),
      });
      try {
        const before = fileMirrors(store, config.link);
        await new StoreBridgeSide(store, config.link).withdraw();
        const remaining = fileMirrors(store, config.link);
        file = { removed: before.filter((id) => !remaining.includes(id)), remaining };
        if (remaining.length) problems.push(`file-mesh mirrors remain under ${config.link}: ${remaining.length}`);
      } finally {
        store.closeState();
      }
    } catch (error) {
      file = { error: message(error) };
      problems.push("file-mesh withdraw failed");
    }
    return { ok: problems.length === 0, link: config.link, stopped, nats, file, problems };
  } finally {
    release();
  }
};
