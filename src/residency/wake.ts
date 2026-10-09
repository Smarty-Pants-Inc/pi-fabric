import fs from "node:fs";
import path from "node:path";
import type { MeshEvent, MeshStore } from "../mesh/store.js";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../mesh/archive.js";
import { residentProcessAlive } from "./process-identity.js";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";

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
  actors: Array<{ id: string; name: string; topics: string[] }>;
}
export const residentSleepingPath = (root: string): string => path.join(root, "sleeping.json");
export const residentWakeRequestPath = (root: string): string => path.join(root, "wake-request.json");
export const readWakeJson = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; } catch { return undefined; }
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
      path.resolve(config.residencyRoot) !== path.resolve(root)) return undefined;
  return routes;
};
const residentDirectories = (meshRoot: string): string[] => {
  const directory = path.join(meshRoot, "residency");
  try { return fs.readdirSync(directory, { withFileTypes: true }).filter(entry => entry.isDirectory())
    .map(entry => path.join(directory, entry.name)); } catch { return []; }
};

/** Reuse the installed Node launcher, never keep a polling hostd warm. */
export async function requestResidentWake(root: string, delivery: { id: string; sequence?: number },
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>): Promise<void> {
  if (residentOwnerLive(root) && !residentOwnerSleeping(root)) return;
  const configPath = path.join(root, "config.json");
  const config = readWakeJson<ResidentHostConfig>(configPath);
  if (!config || path.resolve(config.residencyRoot) !== path.resolve(root)) throw new Error("Invalid resident wake config");
  // Persist AFTER mesh commit and BEFORE spawn. This is a nudge; the existing cursor owns delivery.
  writeJsonAtomic(residentWakeRequestPath(root), { format: 1, ...delivery, requestedAt: Date.now() }, { durable: true });
  if (launch) return launch(configPath, config);
  const { spawnDetached } = await import("../agents/transports/process-utils.js");
  await spawnDetached(path.join(path.dirname(config.fabricExtensionPath), "residency", "launcher.js"),
    ["--config", configPath, "--wake"], config.cwd);
}

/** Called only after the event log's durability barrier, including batch/bridge publishers. */
export async function wakeResidentActors(mesh: Pick<MeshStore, "root" | "listAll">, events: readonly MeshEvent[],
  launch?: (configPath: string, config: ResidentHostConfig) => Promise<void>): Promise<void> {
  const lifecycle = events.some(event => event.topic === "fabric.participant.lifecycle")
    ? mesh.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as {
      from?: string; to?: string; events?: string[];
    }) : [];
  for (const root of residentDirectories(mesh.root)) {
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
    if (last) await requestResidentWake(root, { id: last.id, sequence: last.sequence }, launch);
  }
}

/** Pre-publication direct-route discovery. Ordinary directory/control admission still follows. */
export async function wakeDormantActor(meshRoot: string, id: string): Promise<boolean> {
  const candidates = residentDirectories(meshRoot).flatMap(root => {
    const routes = routesAt(root);
    return routes?.actors.filter(actor => actor.id === id || actor.name === id || actor.id.startsWith(id))
      .map(actor => ({ root, actor })) ?? [];
  });
  if (candidates.length !== 1) return false; // Preserve selector ambiguity; never guess an owner.
  const { root } = candidates[0]!;
  if (residentOwnerLive(root) && !residentOwnerSleeping(root)) return false;
  await requestResidentWake(root, { id: `direct:${id}` });
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    const owner = readWakeJson<ResidentHostOwner>(path.join(root, "owner.json"));
    const ready = readWakeJson<{ token?: string }>(path.join(root, "maintenance-ready.json"));
    if (owner && residentOwnerLive(root) && !residentOwnerSleeping(root) &&
      (owner.maintenanceReady !== 1 || ready?.token === owner.token)) return true;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waking dormant Fabric actor ${id}; delivery was not published`);
}
