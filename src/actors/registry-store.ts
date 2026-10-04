import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  AtomicFileWriter, writeFileAtomic, writeJsonAtomic, encodeOwnerIdentityLine,
  decodeOwnerIdentityLine, lockOwnerLiveness,
} from "../core/atomic-write.js";

const ACTOR_REGISTRY_LOCK_TIMEOUT_MS = 5_000;
const ACTOR_REGISTRY_STALE_LOCK_MS = 30_000;

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error
    ? String((error as NodeJS.ErrnoException).code)
    : undefined;

// Match mesh lock identity semantics: age is not evidence that a live holder died.
const processAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return errorCode(error) !== "ESRCH"; }
};

const processStartTime = (pid: number): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const start = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
    return start && /^\d+$/.test(start) ? start : undefined;
  } catch { return undefined; }
};

const hasRemovalDecision = (actors: readonly unknown[]): boolean => actors.some((actor) =>
  typeof actor === "object" && actor !== null && "removal" in actor && actor.removal !== undefined,
);

/** Disk protocol shared by registry merges and fenced lineage adoption. */
export class ActorRegistryStore {
  readonly #registryPath: string;
  readonly #actorRoot: string;
  readonly #writer: AtomicFileWriter;

  constructor(actorRoot: string) {
    this.#actorRoot = actorRoot;
    this.#registryPath = path.join(actorRoot, "actors.json");
    this.#writer = new AtomicFileWriter(this.#registryPath);
  }

  records(): Array<Record<string, unknown> & { id: string }> {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.#registryPath, "utf8")) as {
        actors?: unknown;
      };
      if (!Array.isArray(parsed.actors)) return [];
      return parsed.actors.flatMap((record) =>
        typeof record === "object" &&
        record !== null &&
        !Array.isArray(record) &&
        typeof (record as { id?: unknown }).id === "string"
          ? [record as Record<string, unknown> & { id: string }]
          : [],
      );
    } catch {
      return [];
    }
  }

  /** Retains registry custody while an adoption also acquires the mesh resume fence. */
  async withLock<T>(operation: () => T | Promise<T>): Promise<T> {
    const lockPath = `${this.#registryPath}.lock`;
    const ownerPath = path.join(lockPath, "owner");
    const deadline = Date.now() + ACTOR_REGISTRY_LOCK_TIMEOUT_MS;
    const token = randomUUID();
    const ownerRecord = `${token}\n${process.pid}\n${Date.now()}\n${encodeOwnerIdentityLine()}\n`;
    fs.mkdirSync(this.#actorRoot, { recursive: true, mode: 0o700 });
    while (true) {
      try {
        fs.mkdirSync(lockPath, { mode: 0o700 });
        fs.writeFileSync(ownerPath, ownerRecord, {
          encoding: "utf8",
          mode: 0o600,
        });
        break;
      } catch (error) {
        if (errorCode(error) !== "EEXIST") throw error;
        try {
          const stat = fs.lstatSync(lockPath);
          if (!stat.isDirectory()) throw new Error("Invalid actor registry lock");
          const firstOwner = fs.readFileSync(ownerPath, "utf8");
          const [holder, pidText, createdText, startText] = firstOwner.trim().split("\n");
          const pid = Number(pidText);
          const validPid = Number.isSafeInteger(pid) && pid > 0;
          const validOwner = !!holder && validPid && createdText !== undefined &&
            createdText.trim() !== "" && Number.isFinite(Number(createdText));
          // A torn fourth line cannot prove PID reuse. Unknown/denied identities stay live.
          const recordedStart = firstOwner.endsWith("\n") && startText && /^\d+$/.test(startText) ? startText : undefined;
          const alive = validPid && processAlive(pid);
          const actualStart = alive && recordedStart ? processStartTime(pid) : undefined;
          // New owners carry boot/namespace identity; numeric fourth lines retain the
          // fork's legacy PID-reuse evidence. Torn/malformed structured identities stay
          // unknown, never falling back to an unrelated PID in this namespace.
          const structuredIdentity = startText?.startsWith("{") === true;
          const identityLine = firstOwner.endsWith("\n") && structuredIdentity ? startText : undefined;
          const dead = structuredIdentity
            ? validOwner && decodeOwnerIdentityLine(identityLine) !== undefined &&
              // This lock can span asynchronous adoption: creation age is NOT a
              // heartbeat or an exit receipt. Only positive identity/death evidence
              // authorizes reaping; an unobservable foreign holder remains in custody.
              lockOwnerLiveness(pid, Number.NaN, identityLine, { legacyAlive: processAlive }) === "dead"
            : alive ? validOwner && actualStart !== undefined && actualStart !== recordedStart :
              validOwner || Date.now() - stat.mtimeMs > ACTOR_REGISTRY_STALE_LOCK_MS;
          if (dead) {
            const current = fs.lstatSync(lockPath);
            if (current.isDirectory() && current.dev === stat.dev && current.ino === stat.ino &&
              fs.readFileSync(ownerPath, "utf8") === firstOwner) {
              // Retain a nonempty, identity-bound fence just like the mesh lock. A paused
              // reaper cannot rename a successor over it; never recursively delete the
              // canonical path after checking its owner (that would reopen the race).
              const fence = `${lockPath}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${firstOwner}`).digest("hex")}`;
              fs.renameSync(lockPath, fence);
              continue;
            }
          }
        } catch {
          // Lock creation or stale recovery raced; retry until the deadline.
        }
        if (Date.now() >= deadline) {
          throw new Error("Timed out waiting for the Fabric actor registry lock");
        }
        await delay(10);
      }
    }
    try {
      return await operation();
    } finally {
      try {
        const owner = fs.readFileSync(ownerPath, "utf8");
        if (owner.startsWith(`${token}\n`)) {
          fs.rmSync(lockPath, { recursive: true, force: true });
        }
      } catch {
        // A recovering process already removed this lock.
      }
    }
  }

  fingerprint(): string | undefined {
    try {
      const stat = fs.statSync(this.#registryPath);
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    } catch {
      return undefined;
    }
  }

  read(): unknown {
    return JSON.parse(fs.readFileSync(this.#registryPath, "utf8"));
  }

  /** Call within withLock for read-modify-write operations. Pending decisions and custody are durable. */
  write(actors: readonly Record<string, unknown>[], options?: { durable?: boolean }): void {
    let previous: string | undefined;
    try { previous = fs.readFileSync(this.#registryPath, "utf8"); }
    catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
    let previousActors: readonly Record<string, unknown>[] = [];
    try {
      const parsed = JSON.parse(previous ?? "null") as { actors?: unknown } | null;
      if (Array.isArray(parsed?.actors)) previousActors = parsed.actors;
    } catch { /* Malformed bytes carry no accepted, recoverable decision. */ }
    const custody = (rows: readonly Record<string, unknown>[]): string => JSON.stringify(rows.map((row) =>
      [row?.id, row?.rootId, row?.residency, row?.adoptedAt, row?.adoptedFrom]).sort((a, b) =>
      String(a[0]).localeCompare(String(b[0]))));
    const durable = options?.durable === true || hasRemovalDecision(actors) ||
      actors.some((actor) => actor.adoptedAt !== undefined || actor.adoptedFrom !== undefined) ||
      custody(previousActors) !== custody(actors);
    const serialized = JSON.stringify({ format: 1, actors }, null, 2);
    if (!durable) {
      // Status/time/history without a custody change are rebuildable soft metadata.
      this.#writer.write(serialized);
      return;
    }
    try {
      this.#writer.write(serialized, { durable: true });
    } catch (error) {
      // The post-rename barrier may fail after replacement. Restore an accepted
      // earlier decision with its barriers; never acknowledge the failed commit.
      if (previous === undefined) fs.rmSync(this.#registryPath, { force: true });
      else writeFileAtomic(this.#registryPath, previous, { durable: true });
      throw error;
    }
  }
}
