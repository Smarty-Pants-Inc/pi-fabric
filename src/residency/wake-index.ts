import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic, syncPathNamespace } from "../core/atomic-write.js";
import type { MeshEvent } from "../mesh/event-log.js";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";
import { residentProcessAlive } from "./process-identity.js";
import type { ResidentWakeRoutes } from "./wake.js";

export interface WakeDelivery { id: string; sequence?: number }
export interface WakeSubscription { from?: string; to?: string; events?: string[] }
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
  const sleeping = readWakeJson<{ token?: string }>(path.join(root, "sleeping.json"));
  return !!owner && owner.token === sleeping?.token;
};
export const residentDirectories = (meshRoot: string): string[] => {
  const directory = path.join(meshRoot, "residency");
  try { return fs.readdirSync(directory, { withFileTypes: true }).filter(entry => entry.isDirectory())
    .map(entry => path.join(directory, entry.name)); } catch { return []; }
};
/** JSON semantics (including omitted undefined fields), with recursively sorted object keys. */
export const canonicalResidentWakeConfig = (value: unknown): string => {
  const sort = (json: unknown): unknown => Array.isArray(json) ? json.map(sort) :
    json !== null && typeof json === "object" ? Object.fromEntries(Object.entries(json)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, field]) => [key, sort(field)])) : json;
  return JSON.stringify(sort(JSON.parse(JSON.stringify(value))));
};
export const residentWakeConfigMatches = (saved: unknown, expected: unknown): boolean => {
  if (!saved || typeof saved !== "object" || Array.isArray(saved) ||
      !expected || typeof expected !== "object" || Array.isArray(expected)) return false;
  try { return canonicalResidentWakeConfig(saved) === canonicalResidentWakeConfig(expected); }
  catch { return false; }
};

/** A restart config is never repaired or partially accepted by a delivery-owned wake. */
export class ResidentWakeConfigMismatch extends Error {
  readonly code = "RESIDENT_WAKE_CONFIG_MISMATCH";
  constructor(readonly root: string) {
    super(`Resident wake config differs from the retained restart snapshot or is unreadable: ${root}`);
    this.name = "ResidentWakeConfigMismatch";
  }
}

/** Retain delivery indexing even on config refusal; this snapshot cannot start a process. */
export const retainedRoutesAt = (root: string): ResidentWakeRoutes | undefined => {
  const routes = readWakeJson<ResidentWakeRoutes>(path.join(root, "wake-routes.json"));
  if (routes?.format !== 1 || !Array.isArray(routes.actors) || typeof routes.configJson !== "string") return undefined;
  try {
    const config = JSON.parse(routes.configJson) as ResidentHostConfig;
    if (routes.rootId !== config?.rootId || typeof config.residencyRoot !== "string" ||
        path.resolve(config.residencyRoot) !== path.resolve(root) ||
        canonicalResidentWakeConfig(config) !== routes.configJson) return undefined;
    return routes;
  } catch { return undefined; }
};
export const routesAt = (root: string): ResidentWakeRoutes | undefined => {
  const routes = retainedRoutesAt(root);
  const config = readWakeJson<ResidentHostConfig>(path.join(root, "config.json"));
  // Every field, including future authority-bearing fields, is part of admission.
  if (!routes || !residentWakeConfigMatches(config, JSON.parse(routes.configJson))) return undefined;
  return routes;
};

/** Re-read persisted config and the host's exact snapshot at each actual wake start. */
export const assertResidentWakeConfig = (root: string, launchConfig?: ResidentHostConfig): ResidentHostConfig => {
  const routes = retainedRoutesAt(root);
  const config = readWakeJson<ResidentHostConfig>(path.join(root, "config.json"));
  if (!routes || !residentWakeConfigMatches(config, JSON.parse(routes.configJson)) ||
      (launchConfig !== undefined && !residentWakeConfigMatches(config, launchConfig)))
    throw new ResidentWakeConfigMismatch(root);
  return config!;
};
export const residentDeliveryMatches = (routes: ResidentWakeRoutes, event: MeshEvent, lifecycle: readonly WakeSubscription[]): boolean => {
  const command = event.topic === "fabric.control.command" ? event.data as { targetId?: string } | undefined : undefined;
  const source = event.topic === "fabric.participant.lifecycle" ? event.data as { source?: { id?: string }; event?: string } | undefined : undefined;
  return routes.actors.some(actor =>
    (command ? command.targetId === actor.id && event.to === routes.hostId :
      (event.to === actor.id || event.to === actor.name || actor.topics.includes(event.topic)) &&
      (event.from.id !== actor.id || event.to === actor.id || event.to === actor.name)) ||
    (source && lifecycle.some(subscription => subscription.to === actor.id && subscription.from === source.source?.id &&
      typeof source.event === "string" && subscription.events?.includes(source.event))));
};

// This is a nudge index, not a second inbox: one high-watermark per resident root.
// Actor cursors still drain the original archived deliveries FIFO/exactly once.
// Appends and acknowledgements take the SAME mesh lock as archival publication.
// Bounded checkpoint compaction preserves every outstanding watermark, never scans history.
export const RESIDENT_WAKE_INDEX_MAX_BYTES = 128 * 1024;
export const RESIDENT_WAKE_INDEX_MAX_ROOTS = 128;
export const residentWakeIndexPath = (meshRoot: string): string => path.join(meshRoot, "wake-unacknowledged.jsonl");
type Record = { format: 1; root: string; delivery: WakeDelivery; op: "pending" | "ack" };
const covers = (ack: WakeDelivery, pending: WakeDelivery): boolean => ack.id === pending.id ||
  (ack.sequence !== undefined && pending.sequence !== undefined && ack.sequence >= pending.sequence);

class WakeIndex {
  readonly pending = new Map<string, WakeDelivery>();
  #stamp = "";
  constructor(readonly file: string, readonly meshRoot: string) {}
  refresh(): void {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(this.file); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      this.pending.clear(); this.#stamp = ""; return;
    }
    if (!stat.isFile() || (process.getuid && stat.uid !== process.getuid()) || stat.size > RESIDENT_WAKE_INDEX_MAX_BYTES)
      throw new Error("Resident wake index is unsafe or exceeds its bounded read");
    const stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    if (stamp === this.#stamp) return;
    const fd = fs.openSync(this.file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    let text: string;
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.ino !== stat.ino || opened.dev !== stat.dev ||
          opened.size > RESIDENT_WAKE_INDEX_MAX_BYTES || (process.getuid && opened.uid !== process.getuid()))
        throw new Error("Resident wake index changed during bounded read");
      text = fs.readFileSync(fd, "utf8");
    } finally { fs.closeSync(fd); }
    this.pending.clear();
    for (const line of text.split("\n")) {
      if (!line) continue;
      const record = JSON.parse(line) as Record;
      if (record.format !== 1 || !["pending", "ack"].includes(record.op) || typeof record.root !== "string" ||
          path.dirname(record.root) !== path.join(this.meshRoot, "residency") ||
          typeof record.delivery?.id !== "string" ||
          (record.delivery.sequence !== undefined && !Number.isSafeInteger(record.delivery.sequence)))
        throw new Error("Invalid resident wake index record");
      if (record.op === "pending") this.pending.set(record.root, record.delivery);
      else {
        const pending = this.pending.get(record.root);
        if (pending && covers(record.delivery, pending)) this.pending.delete(record.root);
      }
      if (this.pending.size > RESIDENT_WAKE_INDEX_MAX_ROOTS) throw new Error("Resident wake index root bound exceeded");
    }
    this.#stamp = stamp;
  }
  append(record: Record): void {
    this.refresh();
    if (record.op === "pending" && !this.pending.has(record.root) && this.pending.size >= RESIDENT_WAKE_INDEX_MAX_ROOTS)
      throw new Error("Resident wake index root bound exceeded");
    const line = JSON.stringify(record) + "\n";
    if (Buffer.byteLength(line) > 1024) throw new Error("Resident wake index record bound exceeded");
    const size = this.#stamp ? fs.statSync(this.file).size : 0;
    if (size + Buffer.byteLength(line) > RESIDENT_WAKE_INDEX_MAX_BYTES) {
      const checkpoint = [...this.pending].map(([root, delivery]) => JSON.stringify({ format: 1, op: "pending", root, delivery }) + "\n").join("");
      if (Buffer.byteLength(checkpoint + line) > RESIDENT_WAKE_INDEX_MAX_BYTES) throw new Error("Resident wake index checkpoint bound exceeded");
      writeFileAtomic(this.file, checkpoint, { durable: true });
    }
    const fd = fs.openSync(this.file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND | fs.constants.O_NOFOLLOW, 0o600);
    try { fs.writeFileSync(fd, line); fs.fsyncSync(fd); syncPathNamespace(this.file, fs.fstatSync(fd)); }
    finally { fs.closeSync(fd); }
    // Keep the same cache path for writers and readers, including other publishers.
    this.#stamp = ""; this.refresh();
  }
}
const indexes = new Map<string, WakeIndex>();
const indexAt = (meshRoot: string): WakeIndex => {
  let index = indexes.get(meshRoot);
  if (!index) {
    if (indexes.size >= 32) indexes.delete(indexes.keys().next().value!);
    index = new WakeIndex(residentWakeIndexPath(meshRoot), meshRoot); indexes.set(meshRoot, index);
  }
  index.refresh(); return index;
};

/** Synchronous, under the archive/live publication lock, BEFORE any wake attempt. */
export function indexResidentDeliveries(meshRoot: string, event: MeshEvent, lifecycle: readonly WakeSubscription[]): void {
  for (const root of residentDirectories(meshRoot)) {
    const routes = retainedRoutesAt(root);
    if (!routes || (residentOwnerLive(root) && !residentOwnerSleeping(root)) ||
        !residentDeliveryMatches(routes, event, lifecycle)) continue;
    const index = indexAt(meshRoot);
    const prior = index.pending.get(root);
    if (prior && covers(prior, event)) continue;
    index.append({ format: 1, op: "pending", root, delivery: { id: event.id, sequence: event.sequence } });
  }
}

export function residentUnacknowledgedDeliveries(meshRoot: string): Map<string, WakeDelivery> {
  return new Map(indexAt(meshRoot).pending);
}
/** Caller holds the mesh publication lock. Only durable request/receipt success may acknowledge. */
export function acknowledgeResidentDelivery(meshRoot: string, root: string, delivery: WakeDelivery): void {
  const index = indexAt(meshRoot);
  const pending = index.pending.get(root);
  if (pending && covers(delivery, pending)) index.append({ format: 1, op: "ack", root, delivery });
}
