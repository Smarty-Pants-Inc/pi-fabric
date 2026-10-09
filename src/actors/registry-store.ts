import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { AtomicFileWriter, renameAtomic, syncDirectoryChain, writeFileAtomic } from "../core/atomic-write.js";
import { ActorRegistryPayloads } from "./registry-payloads.js";

const ACTOR_REGISTRY_LOCK_TIMEOUT_MS = 5_000;
const ACTOR_REGISTRY_STALE_LOCK_MS = 30_000;
// A veto that persists under custody is retried with fresh state up to the
// pre-#554 bound. Backoff happens outside the fence (smarty-dev#816).
const ACTOR_REGISTRY_VETO_RETRY_MS = 5_000;
const ACTOR_REGISTRY_VETO_BACKOFF_MIN_MS = 10;
const ACTOR_REGISTRY_VETO_BACKOFF_MAX_MS = 200;
const VETOED: unique symbol = Symbol("vetoed");

/** A caller validate() veto that persisted under custody: nothing was committed; retry. */
export class ActorRegistryUpdateVetoedError extends Error {
  readonly code = "FABRIC_ACTOR_REGISTRY_UPDATE_VETOED";
  readonly retryable = true;
  constructor() {
    super("Actor registry update was vetoed by its validation and not committed; retry");
    this.name = "ActorRegistryUpdateVetoedError";
  }
}

/** The fence wait expired before custody was acquired. */
class ActorRegistryLockTimeoutError extends Error {
  constructor() { super("Timed out waiting for the Fabric actor registry lock"); }
}

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

const freezeRegistryValue = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) freezeRegistryValue(child);
  }
  return value;
};

// Stores naming the same normalized path share one immutable decoded generation.
// Read identity is taken from the open descriptor, so an atomic rename between
// lookup and decoding cannot cache new bytes under the old inode (or vice versa).
// Timestamps alone cannot prove identity: Windows fstat may report zero file IDs.
// Such descriptors always miss rather than aliasing unrelated file generations.
// Keep at most 64 normalized paths, least recently read first. Managers also
// release their path on close; ad-hoc store readers remain bounded by this LRU.
const REGISTRY_READ_CACHE_LIMIT = 64;
const registryReadCache = new Map<string, { generation: string; value: unknown }>();

const hasRemovalDecision = (actors: readonly unknown[]): boolean => actors.some((actor) =>
  typeof actor === "object" && actor !== null && "removal" in actor && actor.removal !== undefined,
);

export interface ActorRegistrySnapshot {
  readonly generation: string | undefined;
  readonly bytes: string | undefined;
  readonly actors: Array<Record<string, unknown> & { id: string }>;
}

export interface ActorRegistryMutation<T> {
  actors: readonly Record<string, unknown>[];
  durable?: boolean;
  /** Cheap synchronous validation of caller-local state after acquisition. */
  validate?: () => boolean;
  value: T;
}

/** Disk protocol shared by registry merges and fenced lineage adoption. */
export class ActorRegistryStore {
  readonly #registryPath: string;
  readonly #actorRoot: string;
  readonly #writer: AtomicFileWriter;
  readonly #payloads: ActorRegistryPayloads;
  readonly #ownProcessStart: string | undefined;
  #snapshot: ActorRegistrySnapshot | undefined;
  readonly #encoded = new WeakMap<Record<string, unknown>, string>();
  readonly #now: () => number;

  /** `now` is a monotonic millisecond clock (injectable for tests); it bounds vetoed-save retries. */
  constructor(actorRoot: string, options?: { now?: () => number }) {
    this.#now = options?.now ?? (() => performance.now());
    // Capture immutable self identity before multi-registry acquisition, never
    // reread it while holding the earlier fence. No work at module import.
    this.#ownProcessStart = processStartTime(process.pid);
    this.#actorRoot = actorRoot;
    this.#registryPath = path.resolve(actorRoot, "actors.json");
    this.#writer = new AtomicFileWriter(this.#registryPath);
    this.#payloads = new ActorRegistryPayloads(actorRoot);
  }

  /** A generation is the atomic file identity, not a process-local counter. Old
   * format-1 writers participate without a migration or an auxiliary head file. */
  snapshot(): ActorRegistrySnapshot {
    for (;;) {
      const generation = this.fingerprint();
      if (this.#snapshot && this.#snapshot.generation === generation) return this.#snapshot;
      let bytes: string | undefined;
      try { bytes = fs.readFileSync(this.#registryPath, "utf8"); }
      catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
      let actors: ActorRegistrySnapshot["actors"] = [];
      try {
        const parsed = JSON.parse(bytes ?? "null") as { actors?: unknown } | null;
        if (Array.isArray(parsed?.actors)) actors = parsed.actors.filter((row): row is Record<string, unknown> & { id: string } =>
          typeof row === "object" && row !== null && !Array.isArray(row) && typeof row.id === "string");
      } catch { /* Preserve the historical malformed-registry recovery contract. */ }
      if (this.fingerprint() !== generation) continue;
      return this.#snapshot = { generation, bytes, actors: freezeRegistryValue(actors) };
    }
  }

  /** Read/merge/compact/encode and stage outside the optimistic fence. After a
   * conflict, update also uses this under custody to guarantee forward progress.
   * Stale preparations never publish heads, and fallback re-runs source selection. */
  prepare(actors: readonly Record<string, unknown>[], options: { durable?: boolean } = {}, snapshot = this.snapshot()) {
    const prior = new Map(snapshot.actors.map(row => [row.id, row]));
    const smallState = (row: Record<string, unknown>): string =>
      JSON.stringify(Object.keys(row).sort().map(key => [key, row[key]]));
    const reusable = actors.map(row => {
      const before = prior.get(String(row.id));
      if (!before || row === before || row.registryMessageReset === true ||
        (Array.isArray(row.registryMessageAppend) && row.registryMessageAppend.length > 0)) return row;
      const { instructions, messages, registryMessageAppend: _append, registryMessageReset: _reset, ...small } = row;
      const { instructions: oldInstructions, messages: oldMessages, ...oldSmall } = before;
      // Persona strings dominate registry size. Compare them directly; serialize
      // only small metadata and (when needed) the bounded message ring.
      const selectedStub = messages === undefined && row.messageHistory !== undefined &&
        Array.isArray(oldMessages) && oldMessages.length === 0;
      return instructions === oldInstructions && smallState(small) === smallState(oldSmall) &&
        (selectedStub || messages === oldMessages || JSON.stringify(messages) === JSON.stringify(oldMessages)) ? before : row;
    });
    const payload = this.#payloads.prepare(reusable, prior);
    const metadata = payload.metadata;
    const custody = (rows: readonly Record<string, unknown>[]): string => JSON.stringify(rows.map(row =>
      [row.id, row.rootId, row.residency, row.adoptedAt, row.adoptedFrom]).sort((a, b) => String(a[0]).localeCompare(String(b[0]))));
    const changed = metadata.filter(row => row !== prior.get(String(row.id)));
    const durable = options.durable === true || hasRemovalDecision(actors) ||
      actors.some(row => row.adoptedAt !== undefined || row.adoptedFrom !== undefined) ||
      custody(snapshot.actors) !== custody(actors) || changed.some(row =>
        row.instructionsFile !== prior.get(String(row.id))?.instructionsFile ||
        JSON.stringify(row.messageHistory) !== JSON.stringify(prior.get(String(row.id))?.messageHistory));
    const encode = (row: Record<string, unknown>): string => {
      let bytes = this.#encoded.get(row);
      if (bytes === undefined) { bytes = JSON.stringify(row); this.#encoded.set(row, bytes); }
      return bytes;
    };
    const encoded = metadata.map(encode);
    const serialized = `{"format":1,"actors":[${encoded.join(",")}]}`;
    // Detach only changed rows before caching; unchanged actors require neither
    // another parse nor another serialization. All decoding stays outside custody.
    const changedHeads: Record<string, unknown>[] = [];
    const accepted = freezeRegistryValue(metadata.map((source, index) => {
      if (source === prior.get(String(source.id))) return source as Record<string, unknown> & { id: string };
      const row = JSON.parse(encoded[index]!) as Record<string, unknown> & { id: string };
      this.#encoded.set(row, encoded[index]!);
      changedHeads.push(row);
      return row;
    }));
    const temporary = `${this.#registryPath}.${process.pid}.${randomUUID()}.prepared`;
    // File data and pre-rename namespace barriers happen outside custody.
    try { writeFileAtomic(temporary, serialized, { durable }); }
    catch (error) {
      try { fs.rmSync(temporary, { force: true }); } catch { /* Preserve the failed preparation barrier. */ }
      throw error;
    }
    let renamed = false;
    return {
      valid: () => this.fingerprint() === snapshot.generation && payload.valid(),
      commit: (): void => {
        try {
          payload.commit();
          renameAtomic(temporary, this.#registryPath);
          renamed = true;
          if (durable) syncDirectoryChain(this.#actorRoot);
          this.#payloads.publishHeads(changedHeads);
          this.#snapshot = { generation: this.fingerprint(), bytes: serialized, actors: accepted };
        } catch (error) {
          if (renamed) {
            if (snapshot.bytes === undefined) fs.rmSync(this.#registryPath, { force: true });
            else writeFileAtomic(this.#registryPath, snapshot.bytes, { durable: true });
          }
          this.#snapshot = undefined;
          throw error;
        } finally {
          registryReadCache.delete(this.#registryPath);
        }
      },
      dispose: () => fs.rmSync(temporary, { force: true }),
    };
  }

  async update<T>(select: (current: ActorRegistrySnapshot["actors"]) => ActorRegistryMutation<T> | undefined): Promise<T | undefined> {
    // ONE monotonic deadline for the whole update, computed once: no attempt starts,
    // and nothing validates or commits, after it (smarty-dev#816, pi-fabric#577).
    const retryUntil = this.#now() + ACTOR_REGISTRY_VETO_RETRY_MS;
    let vetoed = false;
    const expire = (): never => { throw new ActorRegistryUpdateVetoedError(); };
    const live = (): void => { if (this.#now() >= retryUntil) expire(); };
    // Every fence wait gets only the remaining budget, never a fresh 5 s, and the
    // deadline is rechecked right after acquisition; a throw releases the fence.
    const locked = async <R>(operation: () => R): Promise<R> => {
      const remaining = retryUntil - this.#now();
      if (remaining <= 0) expire();
      try {
        return await this.withLock(() => { live(); return operation(); }, remaining);
      } catch (error) {
        if (vetoed && error instanceof ActorRegistryLockTimeoutError) expire();
        throw error;
      }
    };
    const snapshot = this.snapshot();
    const mutation = select(snapshot.actors);
    if (!mutation) return undefined;
    const prepared = this.prepare(mutation.actors, { durable: mutation.durable === true }, snapshot);
    try {
      const committed = await locked(() => {
        if (!prepared.valid() || mutation.validate?.() === false) return false;
        live();
        prepared.commit();
        return true;
      });
      if (committed) return mutation.value;
    } finally { prepared.dispose(); }
    vetoed = true;
    // One optimistic attempt keeps the uncontended fence cheap. After any race,
    // select and prepare under custody: a slow codec must not starve behind even
    // infrequent writers on a CPU-starved host (smarty-dev#816).
    // Caller-local cancellation/ownership validation still vetoes publication. A veto
    // re-selects from a fresh snapshot; one that persists is never reported as
    // success: undefined means only that select() declined to write.
    const attempt = (): T | undefined | typeof VETOED => {
      live();
      const current = this.snapshot();
      const selected = select(current.actors);
      if (!selected) return undefined;
      const prepared = this.prepare(selected.actors, { durable: selected.durable === true }, current);
      try {
        live();
        if (prepared.valid() && selected.validate?.() !== false) {
          live();
          prepared.commit();
          return selected.value;
        }
      } finally { prepared.dispose(); }
      return VETOED;
    };
    let result = await locked(() => {
      const first = attempt();
      return first === VETOED ? attempt() : first;
    });
    // Under contention (load 50+) the veto can persist across back-to-back tries.
    // Keep retrying with fresh state until the old 5 s bound, holding the fence
    // only per attempt and never across the jittered backoff (smarty-dev#816).
    while (result === VETOED) {
      const remaining = retryUntil - this.#now();
      if (remaining <= 0) expire();
      const backoff = ACTOR_REGISTRY_VETO_BACKOFF_MIN_MS +
        Math.random() * (ACTOR_REGISTRY_VETO_BACKOFF_MAX_MS - ACTOR_REGISTRY_VETO_BACKOFF_MIN_MS);
      await delay(Math.min(backoff, remaining));
      // locked() rechecks the deadline after the backoff, before trying the fence.
      result = await locked(attempt);
    }
    return result;
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
      } finally {
        registryReadCache.delete(this.#registryPath);
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
  async withLock<T>(operation: () => T | Promise<T>, timeoutMs = ACTOR_REGISTRY_LOCK_TIMEOUT_MS): Promise<T> {
    const lockPath = `${this.#registryPath}.lock`;
    const ownerPath = path.join(lockPath, "owner");
    const deadline = Date.now() + Math.min(timeoutMs, ACTOR_REGISTRY_LOCK_TIMEOUT_MS);
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
          throw new ActorRegistryLockTimeoutError();
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
      const stat = fs.statSync(this.#registryPath, { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
    } catch {
      return undefined;
    }
  }

  /** Release shared decoded state when its manager closes or its root is removed. */
  releaseReadCache(): void {
    registryReadCache.delete(this.#registryPath);
    this.#snapshot = undefined;
  }

  /** One descriptor-bound identity check per call; callers receive a deeply frozen view. */
  read(): unknown {
    let fd: number;
    try { fd = fs.openSync(this.#registryPath, "r"); }
    catch (error) { this.releaseReadCache(); throw error; }
    try {
      const stat = fs.fstatSync(fd, { bigint: true });
      const generation = stat.ino > 0n && stat.mtimeNs > 0n && stat.ctimeNs > 0n
        ? `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`
        : undefined;
      const cached = registryReadCache.get(this.#registryPath);
      // Delete/reinsert promotes both unchanged and replaced generations.
      registryReadCache.delete(this.#registryPath);
      if (generation !== undefined && cached?.generation === generation) {
        registryReadCache.set(this.#registryPath, cached);
        return cached.value;
      }
      const value: unknown = freezeRegistryValue(JSON.parse(fs.readFileSync(fd, "utf8")));
      if (generation !== undefined) {
        registryReadCache.set(this.#registryPath, { generation, value });
        if (registryReadCache.size > REGISTRY_READ_CACHE_LIMIT) {
          registryReadCache.delete(registryReadCache.keys().next().value!);
        }
      }
      return value;
    } finally {
      fs.closeSync(fd);
    }
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
    } finally {
      registryReadCache.delete(this.#registryPath);
    }
  }
}
