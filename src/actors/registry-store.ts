import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic, writeJsonAtomic } from "../core/atomic-write.js";

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

  constructor(actorRoot: string) {
    this.#actorRoot = actorRoot;
    this.#registryPath = path.join(actorRoot, "actors.json");
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

  /** The callback must be synchronous: release precedes promise assimilation. */
  async withLock<T>(operation: () => T): Promise<T> {
    const lockPath = `${this.#registryPath}.lock`;
    const ownerPath = path.join(lockPath, "owner");
    const deadline = Date.now() + ACTOR_REGISTRY_LOCK_TIMEOUT_MS;
    const token = randomUUID();
    const started = processStartTime(process.pid);
    const ownerRecord = `${token}\n${process.pid}\n${Date.now()}\n${started ? `${started}\n` : ""}`;
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
          const dead = alive ? validOwner && actualStart !== undefined && actualStart !== recordedStart :
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
      return operation();
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
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`;
    } catch {
      return undefined;
    }
  }

  read(): unknown {
    return JSON.parse(fs.readFileSync(this.#registryPath, "utf8"));
  }

  /** Call within withLock. Definitions, lineage claims and pending decisions are durable. */
  write(actors: readonly Record<string, unknown>[], options?: { durable?: boolean }): void {
    // A barrier belongs to an inode, not its contents. Every replacement carrying an
    // accepted removal must establish its own barriers, including foreign/preserved rows.
    if (!options?.durable && !hasRemovalDecision(actors)) {
      writeJsonAtomic(this.#registryPath, { format: 1, actors }, { space: 2, durable: true });
      return;
    }
    const previous = fs.readFileSync(this.#registryPath, "utf8");
    const rollbackDurable = true; // The restored definitions/lineage are authoritative too.
    try {
      writeJsonAtomic(this.#registryPath, { format: 1, actors }, { space: 2, durable: true });
    } catch (error) {
      // A directory barrier can fail after rename installed the new registry. Restore the
      // live decision under the lock. The replacement restores authoritative definitions
      // and lineage as well as removal decisions, so it needs barriers too.
      // Never report the failed commit as accepted.
      writeFileAtomic(this.#registryPath, previous, { durable: rollbackDurable });
      throw error;
    }
  }
}
