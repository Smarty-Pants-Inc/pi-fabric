import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const USAGE = `Usage:
  pi-fabric session pending <session-uuid | session:uuid> [--mesh <meshRoot>] [--json]

--mesh defaults to $PI_FABRIC_MESH_ROOT; no implicit mesh discovery.
Output is always JSON. This inventories recorded state, not live process memory.`;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ACTOR_ID = /^[a-f0-9]{32}$/;
const STATUSES = new Set(["dormant", "idle", "queued", "preparing", "waiting", "running", "stopped", "failed", "failing-preparation"]);
const MAX_BYTES = 64 * 1024 * 1024;
type Row = Record<string, unknown>;
interface CliIo {
  stdout: Pick<NodeJS.WritableStream, "write">;
  stderr: Pick<NodeJS.WritableStream, "write">;
  env?: NodeJS.ProcessEnv;
}
class UsageError extends Error {}
const row = (value: unknown): Row | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Row : undefined;
const time = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0;
const absent = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === "ENOENT";
const signature = (stat: fs.BigIntStats): string => `${stat.dev}:${stat.ino}:${stat.mode}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;

/** No store constructors, locks, recovery, or receipt acknowledgement: filesystem reads only. */
class Observation {
  readonly unknown: string[] = [];
  readonly seen = new Map<string, string | undefined>();
  problem(file: string, reason: string): void { this.unknown.push(`${file}: ${reason}`); }
  /** Stop change-tracking a path that turned out not to hold this root's state (another session's). */
  forget(file: string): void { this.seen.delete(file); }
  /**
   * List a shared namespace (mesh root, follow-up, route and actor roots) WITHOUT change tracking:
   * every session writes there, so its metadata changing is not a change to this root's state.
   */
  quietNames(directory: string, optional = false): string[] | undefined {
    try {
      const stat = fs.lstatSync(directory, { throwIfNoEntry: false });
      if (!stat) {
        if (optional) return [];
        throw new Error("missing directory");
      }
      if (!stat.isDirectory()) throw new Error("unsafe directory");
      return fs.readdirSync(directory).sort();
    } catch {
      this.problem(directory, "missing, unreadable or non-directory namespace");
      return undefined;
    }
  }
  stat(file: string): fs.BigIntStats | undefined {
    try {
      const stat = fs.lstatSync(file, { bigint: true });
      this.seen.set(file, signature(stat));
      return stat;
    } catch (error) {
      if (!absent(error)) throw error;
      this.seen.set(file, undefined);
      return undefined;
    }
  }
  json(file: string): { present: boolean | null; value?: unknown } {
    let fd: number | undefined;
    let present: boolean | null = null;
    try {
      const stat = this.stat(file);
      if (!stat) return { present: false };
      present = true;
      if (!stat.isFile() || stat.size > BigInt(MAX_BYTES)) throw new Error("unsafe file");
      fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
      if (signature(fs.fstatSync(fd, { bigint: true })) !== signature(stat)) throw new Error("changed file");
      const value: unknown = JSON.parse(fs.readFileSync(fd, "utf8"));
      if (signature(fs.fstatSync(fd, { bigint: true })) !== signature(stat)) throw new Error("changed file");
      return { present: true, value };
    } catch {
      // Do not echo parser/OS errors: they can contain message text or file contents.
      this.problem(file, "unreadable, changed, oversized, or invalid JSON file");
      return { present };
    } finally { if (fd !== undefined) fs.closeSync(fd); }
  }
  names(directory: string): string[] | undefined {
    try {
      const stat = this.stat(directory);
      if (!stat) return [];
      if (!stat.isDirectory()) throw new Error("unsafe directory");
      return fs.readdirSync(directory).sort();
    } catch {
      this.problem(directory, "unreadable or non-directory namespace");
      return undefined;
    }
  }
  verify(): void {
    for (const [file, before] of this.seen) {
      try {
        let after: string | undefined;
        try { after = signature(fs.lstatSync(file, { bigint: true })); }
        catch (error) { if (!absent(error)) throw error; }
        if (before !== after) this.problem(file, "changed during inventory; retry with writers quiesced");
      } catch { this.problem(file, "cannot verify observation"); }
    }
  }
}

interface ActorInventory {
  id: string;
  name: string;
  scope: "project" | "session";
  rootId: string;
  registry: string;
  status: string;
  queued: number | null;
  inFlight: number | null;
  queueFiles: string[];
  settledWindows: number | null;
  pendingDeliveryOutcomes: number | null;
  childCompletionEntries: number | null;
}

const actorInventory = (observation: Observation, source: Row, registry: string, scope: ActorInventory["scope"]): ActorInventory => {
  const directory = path.join(path.dirname(registry), source.id as string);
  const names = observation.names(directory);
  const result: ActorInventory = {
    id: source.id as string, name: source.name as string, rootId: source.rootId as string, scope, registry,
    status: source.status as string, queued: 0, inFlight: 0, queueFiles: [], settledWindows: 0,
    pendingDeliveryOutcomes: 0, childCompletionEntries: null,
  };
  const items = new Map<string, boolean>();
  let valid = names !== undefined;
  for (const name of names ?? []) {
    if (!name.startsWith("queue-")) continue;
    const file = path.join(directory, name);
    result.queueFiles.push(file);
    const parsed = observation.json(file);
    const saved = row(parsed.value);
    if (!/^queue-[a-f0-9]{16}\.json$/.test(name) || !saved || saved.format !== 1 || !Array.isArray(saved.items) ||
        (saved.settledWindows !== undefined && !Array.isArray(saved.settledWindows)) ||
        (saved.pendingDeliveryOutcomes !== undefined && !Array.isArray(saved.pendingDeliveryOutcomes))) {
      observation.problem(file, "unknown queue checkpoint shape"); valid = false; continue;
    }
    for (const item of saved.items) {
      const entry = row(item);
      if (!entry || typeof entry.id !== "string" || !entry.id || typeof entry.source !== "string" ||
          !time(entry.createdAt) || (entry.resumed !== undefined && typeof entry.resumed !== "boolean")) {
        observation.problem(file, "unknown queue item shape"); valid = false; continue;
      }
      // A predecessor and its committed successor may both retain the same item.
      items.set(entry.id, items.get(entry.id) === true || entry.resumed === true);
    }
    if ((saved.settledWindows as unknown[] | undefined)?.some(value => !row(value)) ||
        (saved.pendingDeliveryOutcomes as unknown[] | undefined)?.some(value => !row(value))) {
      observation.problem(file, "unknown pending queue bookkeeping shape"); valid = false;
    }
    result.settledWindows! += (saved.settledWindows as unknown[] | undefined)?.length ?? 0;
    result.pendingDeliveryOutcomes! += (saved.pendingDeliveryOutcomes as unknown[] | undefined)?.length ?? 0;
  }
  result.queued = valid ? [...items.values()].filter(inFlight => !inFlight).length : null;
  result.inFlight = valid ? [...items.values()].filter(Boolean).length : null;
  if (!valid) result.settledWindows = result.pendingDeliveryOutcomes = null;

  // pending() is NOT a reader: it recovers archives, takes a claim lock, and removes/acks files.
  // Nonempty child custody therefore remains explicitly unreconciled, never a guessed zero.
  const childDirectory = path.join(directory, "child-completions");
  const children = observation.names(childDirectory);
  if (children) {
    const entries = children.filter(name => /^[a-f0-9]{32}\.(json|source|abandon)$/.test(name));
    result.childCompletionEntries = entries.length;
    if (children.length) {
      result.childCompletionEntries = entries.length || null;
      observation.problem(childDirectory, "nonempty child completion custody requires owner reconciliation; pending count unknown");
    }
  }
  return result;
};

export const runSessionPendingCli = async (
  argv: string[], io: CliIo = { stdout: process.stdout, stderr: process.stderr },
): Promise<number> => {
  try {
    const positionals: string[] = [];
    const flags = new Set<string>();
    let mesh: string | undefined;
    for (let index = 0; index < argv.length; index++) {
      const arg = argv[index]!;
      if (!arg.startsWith("-")) { positionals.push(arg); continue; }
      if (!["--mesh", "--json", "--help"].includes(arg) || flags.has(arg)) throw new UsageError("Unknown or repeated option");
      flags.add(arg);
      if (arg === "--mesh") {
        mesh = argv[++index];
        if (!mesh?.trim() || mesh.startsWith("-")) throw new UsageError("--mesh requires a path");
      }
    }
    if (flags.has("--help")) { io.stdout.write(`${USAGE}\n`); return 0; }
    const sessionId = positionals[0]?.replace(/^session:/, "");
    if (positionals.length !== 1 || !sessionId || !UUID.test(sessionId)) throw new UsageError("Expected exactly one session UUID or session:UUID");
    mesh ??= (io.env ?? process.env).PI_FABRIC_MESH_ROOT?.trim();
    if (!mesh) {
      io.stdout.write(`${JSON.stringify({ format: 1, error: "mesh-required", unknown: ["Specify --mesh or PI_FABRIC_MESH_ROOT; implicit discovery imports the runtime store graph"] })}\n`);
      return 1;
    }
    const meshRoot = path.resolve(mesh);
    const rootId = `session:${sessionId}`;
    const observation = new Observation();
    const mailbox = path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.json`);
    const followUps: { file: string; present: boolean | null; count: number | null; ids?: string[]; oldestAt?: number; owner?: Row | null } =
      { file: mailbox, present: null, count: null };
    const actors: ActorInventory[] = [];
    const routing: { successorRootId: string | null; routes: number | null; unfinishedRoutes: number | null } =
      { successorRootId: null, routes: null, unfinishedRoutes: null };
    let actorCoverage = true;
    const root = observation.quietNames(meshRoot);
    if (!root) {
      observation.problem(meshRoot, "missing or unreadable mesh root"); actorCoverage = false;
    } else {
      observation.quietNames(path.join(meshRoot, "main-followups"), true);
      const journal = observation.json(mailbox);
      followUps.present = journal.present;
      if (journal.present === false) followUps.count = 0;
      else {
        const saved = row(journal.value);
        if (!saved || saved.version !== 1 || !Array.isArray(saved.items) || saved.items.some(item => {
          const entry = row(item); const from = row(entry?.from);
          return !entry || typeof entry.id !== "string" || !entry.id || !time(entry.sentAt) ||
            typeof entry.message !== "string" || !from || typeof from.id !== "string" || !from.id.trim() ||
            typeof from.kind !== "string" || !from.kind.trim();
        })) observation.problem(mailbox, "unknown follow-up journal shape");
        else {
          const items = saved.items as Array<{ id: string; sentAt: number }>;
          followUps.count = items.length; followUps.ids = items.map(item => item.id);
          if (items.length) followUps.oldestAt = items.reduce((oldest, item) => Math.min(oldest, item.sentAt), Infinity);
        }
      }
      const ownerFile = mailbox.replace(/\.json$/, ".owner.json");
      const owner = observation.json(ownerFile);
      if (owner.present === false) followUps.owner = null;
      else {
        const value = row(owner.value);
        if (!value || value.rootId !== rootId || value.sessionId !== sessionId || value.ownerIdentityId !== rootId ||
            !Number.isSafeInteger(value.pid) || (value.pid as number) <= 0 ||
            ["host", "activationId", "processStartedAt"].some(key => value[key] !== undefined && typeof value[key] !== "string") ||
            (value.retired !== undefined && value.retired !== true)) {
          followUps.owner = null; observation.problem(ownerFile, "unknown or mismatched inbox owner shape");
        } else followUps.owner = Object.fromEntries(["rootId", "sessionId", "ownerIdentityId", "pid", "host", "activationId", "processStartedAt", "retired"]
          .filter(key => value[key] !== undefined).map(key => [key, value[key]]));
      }
      const successorFile = path.join(meshRoot, "main-followups", "successors", `${createHash("sha256").update(rootId).digest("hex")}.json`);
      const successor = observation.json(successorFile);
      if (successor.present !== false) {
        const value = row(successor.value);
        if (!value || value.oldRoot !== rootId || (value.newRoot !== undefined && typeof value.newRoot !== "string") ||
            (value.activationId !== undefined && typeof value.activationId !== "string")) {
          observation.problem(successorFile, "unknown successor shape");
        } else routing.successorRootId = value.newRoot as string | undefined ?? null;
      }
      const routeDirectory = path.join(meshRoot, "main-followups", "routes");
      const routes = observation.quietNames(routeDirectory, true);
      routing.routes = routing.unfinishedRoutes = 0;
      let routesValid = routes !== undefined;
      for (const name of routes ?? []) {
        const file = path.join(routeDirectory, name);
        const read = observation.json(file);
        if (read.present === false) { observation.forget(file); continue; } // removed after the listing
        const route = row(read.value);
        if (route && route.oldRoot !== rootId && route.newRoot !== rootId) observation.forget(file); // another root's route
        if (!/^[a-f0-9]{64}\.json$/.test(name) || !route || typeof route.oldRoot !== "string" ||
            typeof route.messageId !== "string" || (route.newRoot !== undefined && typeof route.newRoot !== "string") ||
            (route.done !== undefined && route.done !== true)) {
          observation.problem(file, "unknown follow-up route shape"); routesValid = false; continue;
        }
        if (route.oldRoot === rootId || route.newRoot === rootId) {
          routing.routes!++; if (!route.done) routing.unfinishedRoutes!++;
        }
      }
      if (!routesValid) routing.routes = routing.unfinishedRoutes = null;
      const actorRoot = path.join(meshRoot, "actors");
      const names = observation.quietNames(actorRoot, true);
      if (!names) actorCoverage = false;
      const registries: Array<{ file: string; scope: ActorInventory["scope"] }> = [{ file: path.join(actorRoot, "actors.json"), scope: "project" }];
      for (const name of names ?? []) {
        const directory = path.join(actorRoot, name);
        if (ACTOR_ID.test(name)) {
          // A project-scope actor directory: its actor keeps writing here, so do not track its metadata.
          // Only an actors.json inside would make it a session registry root (an unusual session id).
          try {
            if (fs.lstatSync(path.join(directory, "actors.json"), { throwIfNoEntry: false })) {
              registries.push({ file: path.join(directory, "actors.json"), scope: "session" });
            }
          } catch (error) {
            if ((error as NodeJS.ErrnoException)?.code !== "ENOTDIR") { observation.problem(directory, "unreadable actor namespace"); actorCoverage = false; }
          }
          continue;
        }
        try {
          // Untracked: another session creating an actor changes its registry directory's metadata.
          const stat = fs.lstatSync(directory, { throwIfNoEntry: false });
          if (stat?.isSymbolicLink()) { observation.problem(directory, "symlink in actor namespace"); actorCoverage = false; }
          else if (stat?.isDirectory()) registries.push({ file: path.join(directory, "actors.json"), scope: "session" });
        } catch { observation.problem(directory, "unreadable actor namespace"); actorCoverage = false; }
      }
      // This session's own registry path is always read and tracked, even if absent when listed.
      const ownRegistry = path.join(actorRoot, sessionId, "actors.json");
      if (!registries.some(entry => entry.file === ownRegistry)) registries.push({ file: ownRegistry, scope: "session" });
      for (const { file, scope } of registries) {
        // This session's own registry stays tracked (its appearance mid-scan is a change). Another
        // registry is tracked only if it holds a row of this root.
        const own = path.basename(path.dirname(file)) === sessionId;
        const registry = observation.json(file);
        if (registry.present === false) { if (!own) observation.forget(file); continue; }
        const saved = row(registry.value);
        if (!saved || saved.format !== 1 || !Array.isArray(saved.actors)) {
          observation.problem(file, "unknown actor registry shape"); actorCoverage = false; continue;
        }
        if (!own && !saved.actors.some(candidate => row(candidate)?.rootId === rootId)) observation.forget(file);
        for (const candidate of saved.actors) {
          const value = row(candidate);
          if (!value || typeof value.rootId !== "string") {
            observation.problem(file, "actor ownership unknown"); actorCoverage = false; continue;
          }
          if (value.rootId !== rootId) continue;
          if (typeof value.id !== "string" || !ACTOR_ID.test(value.id) || typeof value.name !== "string" ||
              typeof value.status !== "string" || !STATUSES.has(value.status)) {
            observation.problem(file, "unknown owned actor shape"); actorCoverage = false; continue;
          }
          actors.push(actorInventory(observation, value, file, scope));
        }
      }
      // A queue checkpoint for this root's lineage (ActorManager.#queueFile: queue-<sha256(rootId\0residency)[:16]>)
      // under an actor directory with no owned registry row is recorded work of unknown custody, not zero.
      const lineages = new Set(["session", "durable"].map(residency =>
        `queue-${createHash("sha256").update([rootId, residency].join("\0")).digest("hex").slice(0, 16)}.json`));
      const inventoried = new Set(actors.map(entry => path.join(path.dirname(entry.registry), entry.id)));
      // Other sessions' actors keep writing in a shared mesh: list their directories without change
      // tracking, but track this root's two expected checkpoint paths in each (present or absent), so
      // one written during the scan is a change, not a silent zero.
      // null: not an actor directory (absent or not a directory); undefined: unreadable (unknown).
      const list = (directory: string): string[] | null | undefined => {
        try { return fs.readdirSync(directory); }
        catch (error) {
          if (absent(error) || (error as NodeJS.ErrnoException)?.code === "ENOTDIR") return null;
          observation.problem(directory, "unreadable actor directory"); return undefined;
        }
      };
      for (const { file } of registries) {
        const registryRoot = path.dirname(file);
        const entries = registryRoot === actorRoot ? names : observation.quietNames(registryRoot, true);
        if (!entries) { actorCoverage = false; continue; }
        for (const name of entries) {
          const directory = path.join(registryRoot, name);
          if (!ACTOR_ID.test(name) || inventoried.has(directory)) continue;
          const children = list(directory);
          if (children === null) continue;
          if (!children) { actorCoverage = false; continue; }
          const found = [...lineages].filter(lineage => children.includes(lineage) ||
            observation.stat(path.join(directory, lineage)) !== undefined);
          if (found.length) {
            observation.problem(directory, "queue checkpoint for this root without an owned registry row; custody unknown");
            actorCoverage = false;
          }
        }
      }
    }
    observation.verify();
    const complete = observation.unknown.length === 0;
    const sum = (key: "queued" | "inFlight" | "settledWindows" | "pendingDeliveryOutcomes" | "childCompletionEntries"): number | null =>
      complete && actorCoverage && actors.every(actor => actor[key] !== null) ? actors.reduce((total, actor) => total + actor[key]!, 0) : null;
    io.stdout.write(`${JSON.stringify({ format: 1, checkedAt: new Date().toISOString(), meshRoot, sessionId, rootId,
      basis: "recorded-files-not-live-state", complete, followUps, routing, actors,
      totals: { actors: actorCoverage && complete ? actors.length : null, followUps: complete ? followUps.count : null,
        queued: sum("queued"), inFlight: sum("inFlight"), settledWindows: sum("settledWindows"),
        pendingDeliveryOutcomes: sum("pendingDeliveryOutcomes"), childCompletionEntries: sum("childCompletionEntries") },
      unknown: observation.unknown })}\n`);
    return complete ? 0 : 1;
  } catch (error) {
    const usage = error instanceof UsageError;
    io[usage ? "stderr" : "stdout"].write(`${JSON.stringify({ format: 1, error: usage ? "usage" : "unreadable-state",
      ...(usage ? { message: error.message, usage: USAGE } : { unknown: ["Inventory could not be completed"] }) })}\n`);
    return usage ? 2 : 1;
  }
};
