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

// Diagnostic only: keep owner field5 bounded and unable to inject identity slots.
const assertLockLabel = (label: string): void => {
  if (typeof label !== "string" || label.length === 0 || label.length > 64 || /[^\x20-\x7e]/.test(label)) {
    throw new TypeError("Actor registry lock label must be 1-64 printable ASCII characters");
  }
};

const VETO_REASONS = ["cancelled", "revision", "ownership", "actor-state", "caller-validation", "generation", "payload-generation", "lock", "deadline"] as const;
export type ActorRegistryVetoReason = typeof VETO_REASONS[number];
const safeVetoReason = (reason: unknown): ActorRegistryVetoReason =>
  VETO_REASONS.includes(reason as ActorRegistryVetoReason) ? reason as ActorRegistryVetoReason : "caller-validation";

/** A caller validate() veto that persisted under custody: nothing was committed; retry. */
export class ActorRegistryUpdateVetoedError extends Error {
  readonly code = "FABRIC_ACTOR_REGISTRY_UPDATE_VETOED";
  readonly retryable = true;
  readonly reason: ActorRegistryVetoReason;
  constructor(reason: ActorRegistryVetoReason = "caller-validation") {
    super("Actor registry update was vetoed by its validation and not committed; retry");
    this.name = "ActorRegistryUpdateVetoedError";
    this.reason = safeVetoReason(reason);
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
const REGISTRY_READ_RACY_WINDOW_MS = 2_000;
const registryReadCache = new Map<string, { generation: string; readTimeMs: number; value: unknown }>();
// The racy-file age proof compares filesystem mtime with the client's clock.
// Linux local filesystems share that clock; remote/unknown filesystems do not.
// Probe the opened file: a remote file bind mount can sit under a local directory.
// Node has no fstatfs; /proc/self/fd resolves the descriptor's backing filesystem.
// Overlayfs is not proof that its backing layers share the host clock.
// Key verdicts by descriptor dev/ino so remounts and replacements are rechecked.
// Keep verdicts bounded too, and drop a path's retained identities on release.
const REGISTRY_LOCAL_FILESYSTEM_TYPES = new Set([
  0xEF53, // ext2/3/4
  0x58465342, // XFS
  0x9123683E, // btrfs
  0x01021994, // tmpfs
  0xF2F52010, // f2fs
  0x2FC12FC1, // ZFS
]);
const registryLocalClockCache = new Map<string, { filePath: string; local: boolean }>();
const registryHasLocalClock = (filePath: string, fd: number, stat: fs.BigIntStats): boolean => {
  // Other platforms' statfs type values are not comparable to Linux magic values.
  if (process.platform !== "linux" || stat.ino <= 0n) return false;
  const identity = `${stat.dev}:${stat.ino}`;
  const known = registryLocalClockCache.get(identity);
  if (known !== undefined) return known.local;
  let local = false;
  try { local = REGISTRY_LOCAL_FILESYSTEM_TYPES.has(fs.statfsSync(`/proc/self/fd/${fd}`).type); }
  catch { /* An unavailable filesystem identity cannot prove a host-local clock. */ }
  registryLocalClockCache.set(identity, { filePath, local });
  if (registryLocalClockCache.size > REGISTRY_READ_CACHE_LIMIT) {
    registryLocalClockCache.delete(registryLocalClockCache.keys().next().value!);
  }
  return local;
};

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
  /** Cheap synchronous validation after acquisition. A fixed reason is a veto, like false. */
  validate?: () => boolean | ActorRegistryVetoReason;
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
    const reusable = actors.map(input => {
      const before = prior.get(String(input.id));
      // smarty-dev#6829: a loaded actor whose ring is exactly the one this process committed at
      // the current head selects that head instead of re-reading and re-encoding its history
      // (the dominant registry lock hold). Any append, reset or in-place change misses the digest.
      // ponytail: one stringify+sha256 of the ring per save stays; tracking ring identity in the
      // manager would remove it but must prove every in-place message mutation site.
      const row: Record<string, unknown> = before && before.messageHistory !== undefined && input.messageHistory === undefined &&
        input.registryMessageReset !== true && Array.isArray(input.messages) &&
        !(Array.isArray(input.registryMessageAppend) && input.registryMessageAppend.length > 0) &&
        this.#payloads.committedRing(String(input.id), before.messageHistory, input.messages)
        ? (({ messages: _messages, ...rest }) => ({ ...rest, messageHistory: before.messageHistory }))(input)
        : input;
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
    let barrierDue = false;
    let committedGeneration: string | undefined;
    const rollback = (): void => {
      if (snapshot.bytes === undefined) fs.rmSync(this.#registryPath, { force: true });
      else writeFileAtomic(this.#registryPath, snapshot.bytes, { durable: true });
    };
    return {
      valid: () => this.fingerprint() === snapshot.generation && payload.valid(),
      // Diagnostic only, evaluated after valid() refused; no actor text or paths are logged.
      vetoReason: (): ActorRegistryVetoReason => this.fingerprint() !== snapshot.generation ? "generation" : "payload-generation",
      /**
       * Under custody: the payload commit, the rename and (unless `deferBarrier`) the directory barrier.
       * smarty-dev#6100: `deferBarrier` leaves the post-rename directory fsync chain to `barrier()`, which the
       * caller runs AFTER releasing the fence and before acknowledging the write (update() does).
       */
      commit: (options: { deferBarrier?: boolean } = {}): void => {
        try {
          payload.commit();
          renameAtomic(temporary, this.#registryPath);
          renamed = true;
          if (durable && options.deferBarrier !== true) syncDirectoryChain(this.#actorRoot);
          this.#payloads.publishHeads(changedHeads);
          payload.accept(changedHeads, accepted);
          committedGeneration = this.fingerprint();
          barrierDue = durable && options.deferBarrier === true;
          this.#snapshot = { generation: committedGeneration, bytes: serialized, actors: accepted };
        } catch (error) {
          if (renamed) rollback();
          this.#snapshot = undefined;
          throw error;
        } finally {
          registryReadCache.delete(this.#registryPath);
        }
      },
      /**
       * smarty-dev#6100: the deferred post-rename directory barrier, outside custody. The file's bytes and the
       * pre-rename barriers were synced before the fence; only the directory entry remains. A writer that took the
       * fence after this rename built its generation on ours under custody and runs its own barrier before it
       * acknowledges, so syncing the entry late never exposes an acknowledged write to a power loss. On a failure
       * the barrier is retried under custody with a generation re-check: still ours, the old contract holds (one
       * more try, else roll back and throw); superseded, the error is thrown without touching the newer generation.
       */
      barrier: async (): Promise<void> => {
        if (!barrierDue) return;
        barrierDue = false;
        try { syncDirectoryChain(this.#actorRoot); return; } catch (error) {
          await this.withLock(() => {
            if (this.fingerprint() !== committedGeneration) throw error;
            try { syncDirectoryChain(this.#actorRoot); } catch (again) {
              try { rollback(); } finally { this.#snapshot = undefined; registryReadCache.delete(this.#registryPath); }
              throw again;
            }
          }, undefined, "barrier");
        }
      },
      dispose: () => fs.rmSync(temporary, { force: true }),
    };
  }

  async update<T>(select: (current: ActorRegistrySnapshot["actors"]) => ActorRegistryMutation<T> | undefined, label = "update"): Promise<T | undefined> {
    assertLockLabel(label);
    // ONE monotonic deadline for the whole update, computed once: no attempt starts,
    // and nothing validates or commits, after it (smarty-dev#816, pi-fabric#577).
    const retryUntil = this.#now() + ACTOR_REGISTRY_VETO_RETRY_MS;
    let vetoed = false;
    const refusals = new Set<ActorRegistryVetoReason>();
    const callerVeto = (validate: ActorRegistryMutation<T>["validate"]): boolean => {
      let validation;
      try { validation = validate?.(); }
      catch (error) {
        if (!(error instanceof ActorRegistryUpdateVetoedError)) throw error;
        validation = error.reason;
      }
      if (validation !== false && typeof validation !== "string") return false;
      refusals.add(safeVetoReason(validation));
      return true;
    };
    // Once per failed update episode, at the throw site, not once per retry. Fixed labels
    // explain the validation without logging registry contents, credentials or actor prose.
    const expire = (): never => {
      console.warn(`[pi-fabric] actor registry update vetoed: operation=${label}; validations=${[...refusals].join(",") || "deadline"}; budgetMs=${ACTOR_REGISTRY_VETO_RETRY_MS}`);
      throw new ActorRegistryUpdateVetoedError([...refusals].at(-1) ?? "deadline");
    };
    const live = (): void => { if (this.#now() >= retryUntil) expire(); };
    // Every fence wait gets only the remaining budget, never a fresh 5 s, and the
    // deadline is rechecked right after acquisition; a throw releases the fence.
    const locked = async <R>(operation: () => R): Promise<R> => {
      const remaining = retryUntil - this.#now();
      if (remaining <= 0) expire();
      try {
        return await this.withLock(() => { live(); return operation(); }, remaining, label);
      } catch (error) {
        if (vetoed && error instanceof ActorRegistryLockTimeoutError) { refusals.add("lock"); expire(); }
        throw error;
      }
    };
    const snapshot = this.snapshot();
    const mutation = select(snapshot.actors);
    if (!mutation) return undefined;
    const prepared = this.prepare(mutation.actors, { durable: mutation.durable === true }, snapshot);
    try {
      // smarty-dev#6100: custody covers only the generation check, the rename and the payload commit; the directory
      // barrier runs after release, before the write is acknowledged.
      const committed = await locked(() => {
        if (!prepared.valid()) { refusals.add(prepared.vetoReason()); return false; }
        if (callerVeto(mutation.validate)) return false;
        live();
        prepared.commit({ deferBarrier: true });
        return true;
      });
      if (committed) {
        await prepared.barrier();
        return mutation.value;
      }
    } finally { prepared.dispose(); }
    vetoed = true;
    // One optimistic attempt keeps the uncontended fence cheap. After any race,
    // select and prepare under custody: a slow codec must not starve behind even
    // infrequent writers on a CPU-starved host (smarty-dev#816).
    // Caller-local cancellation/ownership validation still vetoes publication. A veto
    // re-selects from a fresh snapshot; one that persists is never reported as
    // success: undefined means only that select() declined to write.
    let barrier: (() => Promise<void>) | undefined;
    const attempt = (): T | undefined | typeof VETOED => {
      live();
      const current = this.snapshot();
      const selected = select(current.actors);
      if (!selected) return undefined;
      const prepared = this.prepare(selected.actors, { durable: selected.durable === true }, current);
      try {
        live();
        if (!prepared.valid()) refusals.add(prepared.vetoReason());
        else if (!callerVeto(selected.validate)) {
          live();
          prepared.commit({ deferBarrier: true });
          barrier = prepared.barrier;
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
    await barrier?.(); // smarty-dev#6100: the committed attempt's directory barrier, after release
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

  /** Actor ids whose committed ring digest this store remembers (tests). */
  committedRingIds(): string[] { return this.#payloads.committedRingIds(); }

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
    }, undefined, "downgrade");
  }

  /** Acquire all registry fences in one global order before any mesh lock.
   * Deduplicate normalized paths: project/session roots may name the same store.
   * The callback retains every fence through source selection and publication;
   * it must never be invoked while the caller already holds mesh custody. */
  static withLocks<T>(registries: readonly ActorRegistryStore[], operation: () => T | Promise<T>, label = "external"): Promise<T> {
    assertLockLabel(label);
    const stores = new Map(registries.map((store) => [path.resolve(store.#registryPath), store]));
    const ordered = [...stores.keys()].sort().map((key) => stores.get(key)!);
    const acquire = (index: number): Promise<T> => index === ordered.length
      ? Promise.resolve().then(operation)
      : ordered[index]!.withLock(() => acquire(index + 1), undefined, label);
    return acquire(0);
  }

  /** Global order: actor registries (sorted path), then mesh; never the reverse.
   * Retains registry custody while adoption/publication acquires the mesh fence. */
  async withLock<T>(operation: () => T | Promise<T>, timeoutMs = ACTOR_REGISTRY_LOCK_TIMEOUT_MS, label = "external"): Promise<T> {
    assertLockLabel(label);
    const lockPath = `${this.#registryPath}.lock`;
    const ownerPath = path.join(lockPath, "owner");
    const deadline = Date.now() + Math.min(timeoutMs, ACTOR_REGISTRY_LOCK_TIMEOUT_MS);
    const token = randomUUID();
    const started = this.#ownProcessStart;
    // Preserve the historical first four slots, including an empty incarnation.
    const ownerRecord = `${token}\n${process.pid}\n${Date.now()}\n${started ?? ""}\n${label}\n`;
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
    for (const [identity, verdict] of registryLocalClockCache) {
      if (verdict.filePath === this.#registryPath) registryLocalClockCache.delete(identity);
    }
    this.#snapshot = undefined;
  }

  /** One descriptor-bound identity check per call; callers receive a deeply frozen view. */
  read(): unknown {
    // smarty-dev#7791: an unchanged registry is answered from one stat of the path, with no open. stat follows the
    // path exactly as open does, so it names the inode the open would read; the hit rule is the open path's own
    // (same generation, local clock already proven for that dev:ino, bytes older than the racy window). Anything
    // else (changed, missing, unproven) takes the descriptor-bound path below.
    const cached = registryReadCache.get(this.#registryPath);
    if (cached) {
      let stat: fs.BigIntStats | undefined;
      try { stat = fs.statSync(this.#registryPath, { bigint: true }); } catch { stat = undefined; }
      if (stat && cached.generation === `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}` &&
        stat.mtimeNs < BigInt(cached.readTimeMs - REGISTRY_READ_RACY_WINDOW_MS) * 1_000_000n) {
        registryReadCache.delete(this.#registryPath);
        registryReadCache.set(this.#registryPath, cached);
        return cached.value;
      }
    }
    let fd: number;
    try { fd = fs.openSync(this.#registryPath, "r"); }
    catch (error) { this.releaseReadCache(); throw error; }
    try {
      const readTimeMs = Date.now();
      const stat = fs.fstatSync(fd, { bigint: true });
      const generation = registryHasLocalClock(this.#registryPath, fd, stat) &&
        stat.ino > 0n && stat.mtimeNs > 0n && stat.ctimeNs > 0n
        ? `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`
        : undefined;
      const cached = registryReadCache.get(this.#registryPath);
      // Delete/reinsert promotes both unchanged and replaced generations.
      registryReadCache.delete(this.#registryPath);
      // On host-local filesystems, coarse timestamps can hide same-size in-place
      // writes within a 2 s quantum. Remote/unknown clocks never reach this hit path.
      // Prove the bytes against their original read-start time, not the current
      // clock: a racy entry must be re-read once settled, never merely age into a hit.
      if (generation !== undefined && cached?.generation === generation &&
        stat.mtimeNs < BigInt(cached.readTimeMs - REGISTRY_READ_RACY_WINDOW_MS) * 1_000_000n) {
        registryReadCache.set(this.#registryPath, cached);
        return cached.value;
      }
      const value: unknown = freezeRegistryValue(JSON.parse(fs.readFileSync(fd, "utf8")));
      if (generation !== undefined) {
        registryReadCache.set(this.#registryPath, { generation, readTimeMs, value });
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
