import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { AtomicFileWriter, writeFileAtomic } from "../core/atomic-write.js";
import { ActorRegistryPayloads } from "./registry-payloads.js";

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
  readonly #payloads: ActorRegistryPayloads;
  readonly #ownProcessStart: string | undefined;

  constructor(actorRoot: string) {
    // Capture immutable self identity before multi-registry acquisition, never
    // reread it while holding the earlier fence. No work at module import.
    this.#ownProcessStart = processStartTime(process.pid);
    this.#actorRoot = actorRoot;
    this.#registryPath = path.join(actorRoot, "actors.json");
    this.#writer = new AtomicFileWriter(this.#registryPath);
    this.#payloads = new ActorRegistryPayloads(actorRoot);
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

  /** Instructions remain inline; prior PR sidecars are accepted for migration. */
  instructions(record: Record<string, unknown>): unknown {
    return this.#payloads.instructions(record);
  }

  messages(record: Record<string, unknown>, limit?: number): unknown[] {
    return this.#payloads.messages(record, limit);
  }

  messageCount(record: Record<string, unknown>): number {
    return this.#payloads.count(record);
  }

  /** Restore the full inline downgrade view. Checkpoint-only rows must be hydrated too;
   * old releases otherwise see an empty inline ring. Stop writers first. */
  async restoreInlineForDowngrade(): Promise<number> {
    return this.withLock(() => {
      const previous = fs.readFileSync(this.#registryPath, "utf8");
      const parsed = JSON.parse(previous) as { format?: number; actors?: unknown };
      if (!parsed || !Array.isArray(parsed.actors)) throw new Error("Invalid actor registry for downgrade");
      const actors = parsed.actors.map((value: unknown) => {
        if (typeof value !== "object" || value === null || Array.isArray(value) ||
            typeof (value as { id?: unknown }).id !== "string") return value;
        const record = value as Record<string, unknown>;
        // Always pass rows through the common readers: an old owner may have
        // removed both selecting fields while the accepted head survives only
        // in messages-head.json. Those readers also fail closed on bad payloads.
        const restored = { ...record, instructions: this.instructions(record), messages: this.messages(record) };
        delete (restored as Record<string, unknown>).instructionsFile;
        delete (restored as Record<string, unknown>).messageHistory;
        return restored;
      });
      try { writeFileAtomic(this.#registryPath, JSON.stringify({ ...parsed, format: 1, actors }, null, 2), { durable: true }); }
      catch (error) {
        writeFileAtomic(this.#registryPath, previous, { durable: true });
        throw error;
      }
      return actors.length;
    });
  }

  /** Acquire all registry fences in one global order before any mesh lock.
   * Deduplicate normalized paths: project/session roots may name the same store.
   * The callback retains every fence through source selection and publication;
   * it must never be invoked while the caller already holds mesh custody. */
  static withLocks<T>(registries: readonly ActorRegistryStore[], operation: () => T | Promise<T>): Promise<T> {
    const stores = new Map(registries.map((store) => [path.resolve(store.#registryPath), store]));
    const ordered = [...stores.keys()].sort().map((key) => stores.get(key)!);
    const acquire = (index: number): Promise<T> => index === ordered.length
      ? Promise.resolve().then(operation)
      : ordered[index]!.withLock(() => acquire(index + 1));
    return acquire(0);
  }

  /** Global order: actor registries (sorted path), then mesh; never the reverse.
   * Retains registry custody while adoption/publication acquires the mesh fence. */
  async withLock<T>(operation: () => T | Promise<T>): Promise<T> {
    const lockPath = `${this.#registryPath}.lock`;
    const ownerPath = path.join(lockPath, "owner");
    const deadline = Date.now() + ACTOR_REGISTRY_LOCK_TIMEOUT_MS;
    const token = randomUUID();
    const started = this.#ownProcessStart;
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
    let durable = options?.durable === true || hasRemovalDecision(actors) ||
      actors.some((actor) => actor.adoptedAt !== undefined || actor.adoptedFrom !== undefined) ||
      custody(previousActors) !== custody(actors);
    const prior = new Map(previousActors.map((actor) => [actor?.id, actor]));
    const metadata = actors.map((actor) => this.#payloads.compact(actor, prior.get(actor.id)));
    // Publishing a new sidecar reference is a commit, not soft status: the payload
    // and its selecting registry head must both survive a crash/migration.
    durable ||= metadata.some((actor) => {
      const before = prior.get(actor.id);
      return actor.instructionsFile !== before?.instructionsFile ||
        JSON.stringify(actor.messageHistory) !== JSON.stringify(before?.messageHistory);
    });
    const serialized = JSON.stringify({ format: 1, actors: metadata }, null, 2);
    try {
      this.#writer.write(serialized, { durable });
      this.#payloads.publishHeads(metadata);
    } catch (error) {
      // The post-rename barrier may fail after replacement. Restore an accepted
      // earlier decision with its barriers; never acknowledge the failed commit.
      if (previous === undefined) fs.rmSync(this.#registryPath, { force: true });
      else writeFileAtomic(this.#registryPath, previous, { durable: true });
      throw error;
    }
  }
}
