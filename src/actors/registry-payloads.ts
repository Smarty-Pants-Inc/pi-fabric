import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncPathNamespace, writeFileAtomic } from "../core/atomic-write.js";

const HISTORY_LIMIT = 100;
const INLINE_INSTRUCTIONS_BYTES = 1_024;

export interface ActorMessageHistory {
  version: 1;
  offset: number;
  bytes: number;
  count: number;
}

type Row = Record<string, unknown>;
type Transaction = { previous?: ActorMessageHistory; reset?: boolean; messages: unknown[] };

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
 * commit. Instructions are immutable, content-addressed files. Payload barriers
 * precede the registry rename, including the first migration from inline records. */
export class ActorRegistryPayloads {
  readonly #rings = new Map<string, { head: string; messages: unknown[] }>();
  readonly #instructions = new Map<string, { file: string; text: string }>();

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
    const ref = history(row.messageHistory);
    if (!ref) return Array.isArray(row.messages) ? row.messages.slice(-limit) : [];
    if (typeof row.id !== "string") throw new Error("Invalid actor message identity");
    const key = JSON.stringify(ref);
    const cached = this.#rings.get(row.id);
    if (limit === HISTORY_LIMIT && cached?.head === key) return structuredClone(cached.messages);
    const chunks: unknown[][] = [];
    let count = 0, current: ActorMessageHistory | undefined = ref;
    const fd = fs.openSync(path.join(this.directory(row.id), "messages.jsonl"), "r");
    try {
      const size = fs.fstatSync(fd).size;
      while (current && count < limit) {
        if (current.offset + current.bytes > size) throw new Error("Truncated actor message history");
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
    } finally { fs.closeSync(fd); }
    const messages = chunks.reverse().flat().slice(-limit);
    if (limit === HISTORY_LIMIT) this.#rings.set(row.id, { head: key, messages: structuredClone(messages) });
    return messages;
  }

  #append(id: string, messages: unknown[], previous?: ActorMessageHistory, reset = false): ActorMessageHistory {
    const file = path.join(this.directory(id), "messages.jsonl");
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    // A leading newline isolates a prior torn append. Offsets select only a complete
    // transaction, so neither abandoned tails nor later appends affect old readers.
    const contents = Buffer.from(`\n${JSON.stringify({ ...(previous ? { previous } : {}), ...(reset ? { reset: true } : {}), messages })}\n`);
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
    if (typeof row.instructions === "string" && row.instructionsFile === undefined &&
        Buffer.byteLength(row.instructions, "utf8") > INLINE_INSTRUCTIONS_BYTES) {
      const hash = createHash("sha256").update(row.instructions).digest("hex");
      const file = path.join(this.directory(id), `instructions-${hash}.txt`);
      const cached = this.#instructions.get(id);
      if (cached?.file !== file || cached.text !== row.instructions) {
        if (!fs.existsSync(file)) writeFileAtomic(file, row.instructions, { durable: true });
        else {
          if (fs.readFileSync(file, "utf8") !== row.instructions) throw new Error("Corrupt actor instructions payload");
          // A previous barrier may have failed after rename. Existence is not a
          // durability receipt; retry the file and namespace before publishing it.
          const fd = fs.openSync(file, "r");
          try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
          syncPathNamespace(file);
        }
        this.#instructions.set(id, { file, text: row.instructions });
      }
      compact.instructionsFile = hash;
      compact.instructions = "[External actor instructions: upgrade pi-fabric or run scripts/actor-registry-downgrade.ts before executing this actor.]";
    }
    let ref = history(row.messageHistory);
    if (!ref) {
      ref = history(previous?.messageHistory);
      // Archive ALL legacy messages, not only the manager's in-memory last 100.
      const pending = Array.isArray(row.registryMessageAppend) ? row.registryMessageAppend : [];
      const legacy = Array.isArray(previous?.messages) ? previous.messages :
        pending.length === 0 && Array.isArray(row.messages) ? row.messages : [];
      if (!ref && legacy.length) ref = this.#append(id, legacy);
      // The manager retains unsaved additions separately from its bounded ring:
      // even a synchronous burst larger than 100 must archive every addition.
      if (pending.length) ref = this.#append(id, pending, ref);
      if (Array.isArray(row.messages)) {
        const messages = row.messages.slice(-HISTORY_LIMIT);
        const before = ref ? this.messages({ id, messageHistory: ref }) : [];
        const encoded = messages.map((message) => JSON.stringify(message));
        const old = before.map((message) => JSON.stringify(message));
        if (JSON.stringify(encoded) !== JSON.stringify(old)) {
          let overlap = Math.min(old.length, encoded.length);
          while (overlap && !old.slice(-overlap).every((value, index) => value === encoded[index])) overlap--;
          ref = this.#append(id, messages.slice(overlap), ref, overlap === 0);
        }
        if (ref) this.#rings.set(id, { head: JSON.stringify(ref), messages: structuredClone(messages) });
      }
    }
    if (ref) {
      compact.messageHistory = ref;
      compact.messages = []; // Format-1 downgrade stub: old loaders accept an array.
    }
    return compact;
  }
}
