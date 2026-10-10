import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncPathNamespace, writeFileAtomic } from "../core/atomic-write.js";

const HISTORY_LIMIT = 100;

export interface ActorMessageHistory {
  version: 1;
  offset: number;
  bytes: number;
  count: number;
}

type Row = Record<string, unknown>;
type Transaction = { previous?: ActorMessageHistory; reset?: boolean; messages: unknown[] };
type PreparedTail = {
  size: number;
  generation: string | undefined;
  appends: Array<{ contents: Buffer; transaction: Transaction; ref: ActorMessageHistory }>;
};

const tailSnapshot = (file: string): { size: number; generation: string } | undefined => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    const size = Number(stat.size);
    if (!Number.isSafeInteger(size)) throw new Error("Actor message log is too large");
    return { size, generation: `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}` };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
};

const ringDigest = (encoded: string): string => createHash("sha256").update(encoded).digest("hex");

const history = (value: unknown): ActorMessageHistory | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("Invalid actor message history reference");
  const ref = value as ActorMessageHistory;
  if (ref.version !== 1 || !Number.isSafeInteger(ref.offset) || ref.offset < 0 ||
      !Number.isSafeInteger(ref.bytes) || ref.bytes <= 0 ||
      !Number.isSafeInteger(ref.count) || ref.count < 0 || ref.count > HISTORY_LIMIT) {
    throw new Error("Invalid actor message history reference");
  }
  return ref;
};

/** Append-only history transactions. The atomic registry selects a committed head;
 * failed/unpublished appends are harmless archives, never a predecessor of a later
 * commit. Instructions stay inline for mixed-release readers. A durable per-actor
 * checkpoint preserves accepted heads when an old owned-row serializer drops
 * unknown fields. Payload barriers precede the registry rename. */
export class ActorRegistryPayloads {
  readonly #rings = new Map<string, { head: string; messages: unknown[] }>();
  #preparing: Map<string, PreparedTail> | undefined;
  // smarty-dev#6829: digest of the last-100 ring this process COMMITTED at each actor's head.
  // A committed head selects an immutable byte range, so a matching in-memory ring needs no
  // history re-read. Speculative digests are promoted only by accept() after the commit.
  readonly #committedRings = new Map<string, { head: string; digest: string }>();
  /** Actor ids with a remembered committed ring (tests). */
  committedRingIds(): string[] { return [...this.#committedRings.keys()]; }
  #speculativeRings: Map<string, { head: string; digest: string }> | undefined;

  /** True when `messages` is exactly the ring this process committed at `head` for `id`. */
  committedRing(id: string, head: unknown, messages: readonly unknown[]): boolean {
    const committed = this.#committedRings.get(id);
    return committed !== undefined && committed.head === JSON.stringify(head) &&
      committed.digest === ringDigest(JSON.stringify(messages.slice(-HISTORY_LIMIT)));
  }

  /** Encode and read histories before registry acquisition. Offsets are speculative;
   * commit validates every append tail under custody before writing any bytes. */
  prepare(rows: readonly Row[], prior: ReadonlyMap<unknown, Row>) {
    const tails = new Map<string, PreparedTail>();
    const rings = new Map<string, { head: string; digest: string }>();
    this.#preparing = tails;
    this.#speculativeRings = rings;
    let metadata: Row[];
    try { metadata = rows.map(row => row === prior.get(row.id) ? row : this.compact(row, prior.get(row.id))); }
    finally { this.#preparing = undefined; this.#speculativeRings = undefined; this.#rings.clear(); }
    return {
      metadata,
      /** After the registry commit: remember the rings whose heads it published. */
      accept: (heads: readonly Row[], committed: readonly Row[]): void => {
        // smarty-dev#8533: forget actors the committed registry no longer holds (removed).
        // A ceded row stays bounded by the registry; its next head change misses the digest.
        const ids = new Set(committed.map(row => row.id));
        for (const id of this.#committedRings.keys()) if (!ids.has(id)) this.#committedRings.delete(id);
        for (const row of heads) {
          const ring = typeof row.id === "string" ? rings.get(row.id) : undefined;
          if (ring && ring.head === JSON.stringify(row.messageHistory)) this.#committedRings.set(row.id as string, ring);
        }
      },
      valid: (): boolean => [...tails].every(([file, tail]) => tailSnapshot(file)?.generation === tail.generation),
      commit: (): void => {
        for (const [file, tail] of tails) {
          const fd = fs.openSync(file, "a", 0o600);
          try {
            for (const append of tail.appends) {
              let written = 0;
              while (written < append.contents.length) {
                const bytes = fs.writeSync(fd, append.contents, written, append.contents.length - written);
                if (!bytes) throw new Error("Incomplete actor message append");
                written += bytes;
              }
            }
            fs.fsyncSync(fd);
          } finally { fs.closeSync(fd); }
          syncPathNamespace(file);
        }
      },
    };
  }

  savedHead(id: string): ActorMessageHistory | undefined {
    try { return history(JSON.parse(fs.readFileSync(path.join(this.directory(id), "messages-head.json"), "utf8"))); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
  }

  /** Missing registry references never select an arbitrary (possibly orphan) tail. */
  count(row: Row): number {
    const ref = history(row.messageHistory);
    if (ref) return ref.count;
    if (typeof row.id === "string" && this.savedHead(row.id)) return this.messages(row).length;
    return Math.min(HISTORY_LIMIT, Array.isArray(row.messages) ? row.messages.length : 0);
  }

  mergeLegacy(before: unknown[], inline: unknown): unknown[] {
    if (!Array.isArray(inline) || inline.length === 0) return before;
    const identity = (message: unknown): string => {
      if (typeof message === "object" && message !== null && "id" in message) {
        const row = message as Row;
        return JSON.stringify([row.id, row.direction]);
      }
      return JSON.stringify(message);
    };
    const known = new Set(before.map(identity));
    return [...before, ...inline.filter(message => {
      const key = identity(message);
      if (known.has(key)) return false;
      known.add(key);
      return true;
    })].slice(-HISTORY_LIMIT);
  }

  /** Complete before acknowledging the registry commit, under its shared lock.
   * Explicit registry heads still win, including during an interrupted publish.
   * Failed publishes restore checkpoints as well as the registry. */
  publishHeads(rows: readonly Row[]): void {
    const updates: Array<{ file: string; previous: string | undefined; next: string }> = [];
    for (const row of rows) {
      const ref = history(row.messageHistory);
      if (!ref || typeof row.id !== "string") continue;
      const file = path.join(this.directory(row.id), "messages-head.json");
      let previous: string | undefined;
      try { previous = fs.readFileSync(file, "utf8"); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      const next = JSON.stringify(ref);
      if (previous !== next) updates.push({ file, previous, next });
    }
    const attempted: typeof updates = [];
    try {
      for (const update of updates) {
        attempted.push(update);
        writeFileAtomic(update.file, update.next, { durable: true });
      }
    } catch (error) {
      for (const update of attempted.reverse()) {
        if (update.previous !== undefined) writeFileAtomic(update.file, update.previous, { durable: true });
        else { fs.rmSync(update.file, { force: true }); syncPathNamespace(path.dirname(update.file)); }
      }
      throw error;
    }
  }

  constructor(readonly actorRoot: string) {}

  directory(id: string): string {
    const safeId = /^[a-f0-9]{32}$/.test(id) ? id : createHash("sha256").update(id).digest("hex");
    return path.join(this.actorRoot, safeId, "registry");
  }

  instructions(row: Row): unknown {
    if (row.instructionsFile === undefined) return row.instructions;
    if (typeof row.id !== "string" || typeof row.instructionsFile !== "string" ||
        !/^[a-f0-9]{64}$/.test(row.instructionsFile)) throw new Error("Invalid actor instructions reference");
    const text = fs.readFileSync(path.join(this.directory(row.id), `instructions-${row.instructionsFile}.txt`), "utf8");
    if (createHash("sha256").update(text).digest("hex") !== row.instructionsFile) {
      throw new Error("Corrupt actor instructions payload");
    }
    return text;
  }

  messages(row: Row, limit = HISTORY_LIMIT): unknown[] {
    const explicit = history(row.messageHistory);
    const ref = explicit ?? (typeof row.id === "string" ? this.savedHead(row.id) : undefined);
    if (!ref) return Array.isArray(row.messages) ? row.messages.slice(-limit) : [];
    if (!explicit) return this.mergeLegacy(this.messages({ id: row.id, messageHistory: ref }), row.messages).slice(-limit);
    if (typeof row.id !== "string") throw new Error("Invalid actor message identity");
    const key = JSON.stringify(ref);
    const cached = this.#rings.get(row.id);
    if (limit === HISTORY_LIMIT && cached?.head === key) return structuredClone(cached.messages);
    const chunks: unknown[][] = [];
    let count = 0, current: ActorMessageHistory | undefined = ref;
    const file = path.join(this.directory(row.id), "messages.jsonl");
    let fd: number | undefined;
    try {
      const tail = this.#preparing?.get(file);
      while (current && count < limit) {
        const pending = tail?.appends.find(append => append.ref.offset === current!.offset && append.ref.bytes === current!.bytes);
        if (pending) {
          chunks.push(pending.transaction.messages);
          count += pending.transaction.messages.length;
          current = pending.transaction.reset ? undefined : pending.transaction.previous;
          continue;
        }
        fd ??= fs.openSync(file, "r");
        if (current.offset + current.bytes > fs.fstatSync(fd).size) throw new Error("Truncated actor message history");
        const buffer = Buffer.alloc(current.bytes);
        let read = 0;
        while (read < buffer.length) {
          const bytes = fs.readSync(fd, buffer, read, buffer.length - read, current.offset + read);
          if (!bytes) throw new Error("Truncated actor message transaction");
          read += bytes;
        }
        const transaction = JSON.parse(buffer.toString("utf8")) as Transaction;
        if (!Array.isArray(transaction.messages)) throw new Error("Invalid actor message transaction");
        chunks.push(transaction.messages);
        count += transaction.messages.length;
        const previous = transaction.reset ? undefined : history(transaction.previous);
        if (previous && previous.offset + previous.bytes > current.offset) throw new Error("Invalid actor history predecessor");
        current = previous;
      }
    } finally { if (fd !== undefined) fs.closeSync(fd); }
    const messages = chunks.reverse().flat().slice(-limit);
    if (limit === HISTORY_LIMIT) this.#rings.set(row.id, { head: key, messages: structuredClone(messages) });
    return messages;
  }

  #append(id: string, messages: unknown[], previous?: ActorMessageHistory, reset = false): ActorMessageHistory {
    const file = path.join(this.directory(id), "messages.jsonl");
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    // A leading newline isolates a prior torn append. Offsets select only a complete
    // transaction, so neither abandoned tails nor later appends affect old readers.
    const transaction: Transaction = { ...(previous ? { previous } : {}), ...(reset ? { reset: true } : {}), messages };
    const contents = Buffer.from(`\n${JSON.stringify(transaction)}\n`);
    if (this.#preparing) {
      let tail = this.#preparing.get(file);
      if (!tail) {
        const snapshot = tailSnapshot(file);
        tail = { size: snapshot?.size ?? 0, generation: snapshot?.generation, appends: [] };
        this.#preparing.set(file, tail);
      }
      const ref: ActorMessageHistory = { version: 1,
        offset: tail.size + tail.appends.reduce((bytes, append) => bytes + append.contents.length, 0),
        bytes: contents.length,
        count: Math.min(HISTORY_LIMIT, (reset ? 0 : previous?.count ?? 0) + messages.length) };
      tail.appends.push({ contents, transaction, ref });
      return ref;
    }
    const fd = fs.openSync(file, "a", 0o600);
    let offset: number;
    try {
      offset = fs.fstatSync(fd).size;
      let written = 0;
      while (written < contents.length) {
        const bytes = fs.writeSync(fd, contents, written, contents.length - written);
        if (!bytes) throw new Error("Incomplete actor message append");
        written += bytes;
      }
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    syncPathNamespace(file);
    return { version: 1, offset, bytes: contents.length,
      count: Math.min(HISTORY_LIMIT, (reset ? 0 : previous?.count ?? 0) + messages.length) };
  }

  compact(row: Row, previous?: Row): Row {
    if (typeof row.id !== "string") return row;
    const id = row.id;
    const compact = { ...row };
    delete compact.registryMessageAppend;
    delete compact.registryMessageReset;
    // Read the previous PR layout once, but never publish an instructions stub.
    // Hydrated manager rows are committed inline by the durable atomic registry
    // writer: never re-fsync an existing instruction file through a read-only
    // handle (FlushFileBuffers on Windows requires write access).
    if (row.instructionsFile !== undefined) compact.instructions = this.instructions(row);
    delete compact.instructionsFile;
    // Filter skips are bounded, rebuildable telemetry, not accepted activations.
    // Preserve their existing soft inline journal until substantive history exists;
    // externalizing this case would introduce fsyncs on every filter poll.
    const filteredOnly = (value: unknown): boolean => Array.isArray(value) && value.every(message =>
      typeof message === "object" && message !== null &&
      typeof (message as Row).reason === "string" && String((message as Row).reason).startsWith("filtered: "));
    if (row.messageHistory === undefined && previous?.messageHistory === undefined &&
        this.savedHead(id) === undefined && filteredOnly(row.messages) &&
        filteredOnly(row.registryMessageAppend ?? []) && filteredOnly(previous?.messages ?? []) &&
        (Array.isArray(previous?.messages) ? previous.messages.length : 0) +
          (Array.isArray(row.registryMessageAppend) ? row.registryMessageAppend.length : 0) <= HISTORY_LIMIT &&
        (row.messages as unknown[]).length <= HISTORY_LIMIT) {
      compact.messages = (row.messages as unknown[]).slice(-HISTORY_LIMIT);
      return compact;
    }
    let ref = history(row.messageHistory);
    if (!ref) {
      const saved = this.savedHead(id);
      const legacyRewrite = row.registryMessageReset !== true && previous?.messageHistory === undefined && saved !== undefined;
      ref = history(previous?.messageHistory) ?? saved;
      // Archive ALL legacy messages, not only the manager's in-memory last 100.
      const pending = Array.isArray(row.registryMessageAppend) ? row.registryMessageAppend : [];
      const legacy = Array.isArray(previous?.messages) ? previous.messages :
        pending.length === 0 && Array.isArray(row.messages) ? row.messages : [];
      if (!ref && legacy.length) ref = this.#append(id, legacy);
      // The manager retains unsaved additions separately from its bounded ring:
      // even a synchronous burst larger than 100 must archive every addition.
      if (pending.length) ref = this.#append(id, pending, ref);
      if (row.registryMessageReset === true) ref = this.#append(id, [], ref, true);
      if (Array.isArray(row.messages)) {
        const before = ref ? this.messages({ id, messageHistory: ref }) : [];
        // Missing references are a legacy save, NOT a request to clear history.
        const messages = legacyRewrite ? this.mergeLegacy(before, row.messages) : row.messages.slice(-HISTORY_LIMIT);
        const encoded = messages.map((message) => JSON.stringify(message));
        const old = before.map((message) => JSON.stringify(message));
        if (JSON.stringify(encoded) !== JSON.stringify(old)) {
          let overlap = Math.min(old.length, encoded.length);
          while (overlap && !old.slice(-overlap).every((value, index) => value === encoded[index])) overlap--;
          ref = this.#append(id, messages.slice(overlap), ref, overlap === 0);
        }
        if (ref) {
          this.#rings.set(id, { head: JSON.stringify(ref), messages: structuredClone(messages) });
          this.#speculativeRings?.set(id, { head: JSON.stringify(ref), digest: ringDigest(`[${encoded.join(",")}]`) });
        }
      }
    }
    if (ref) {
      compact.messageHistory = ref;
      compact.messages = []; // Format-1 downgrade stub: old loaders accept an array.
    }
    return compact;
  }
}
