import fs from "node:fs";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { syncDirectoryChain } from "../core/atomic-write.js";

/** Host-owned send identity, retained with the receiver's durable queue, never user data. */
export interface DeliverySend {
  eventId: string;
  to: string;
  from: string;
  mode: "steer" | "followUp" | "publish";
}
export type DeliveryOutcome = "delivered" | "superseded" | "failed" | "unknown";
export interface DeliveryOutcomeRecord extends DeliverySend {
  outcome: DeliveryOutcome;
  reason: string;
  at: number;
}
const day = (at: number): string => new Date(at).toISOString().slice(0, 10).replaceAll("-", "");

const outcomeKey = (record: DeliverySend & { outcome: DeliveryOutcome }): string =>
  JSON.stringify([record.eventId, record.to, record.from, record.mode, record.outcome]);
interface OutcomeIndex { dev: number; ino: number; offset: number; tail: string; decoder: StringDecoder; keys: Set<string> }
const indexes = new Map<string, OutcomeIndex>();

/** Incremental read-only dedup evidence, rebuilt from retained JSONL after restart.
 * Writer ownership is still the native receiver/control fence, not this cache.
 */
const indexedOutcomes = (file: string): ReadonlySet<string> => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY);
  try {
    const stat = fs.fstatSync(fd);
    let index = indexes.get(file);
    if (!index || index.dev !== stat.dev || index.ino !== stat.ino || stat.size < index.offset) {
      index = { dev: stat.dev, ino: stat.ino, offset: 0, tail: "", decoder: new StringDecoder("utf8"), keys: new Set() };
      indexes.delete(file); indexes.set(file, index);
      while (indexes.size > 16) indexes.delete(indexes.keys().next().value!);
    }
    let buffer: Buffer | undefined;
    while (index.offset < stat.size) {
      buffer ??= Buffer.alloc(64 * 1024);
      const count = fs.readSync(fd, buffer, 0, Math.min(buffer.length, stat.size - index.offset), index.offset);
      if (!count) break;
      index.offset += count;
      const lines = (index.tail + index.decoder.write(buffer.subarray(0, count))).split("\n");
      index.tail = lines.pop()!;
      for (const line of lines) {
        try {
          const value = JSON.parse(line) as DeliveryOutcomeRecord;
          const send = deliverySend(value);
          if (send && ["delivered", "superseded", "failed", "unknown"].includes(value.outcome)) index.keys.add(outcomeKey({ ...send, outcome: value.outcome }));
        } catch { /* A malformed old line is not delivery evidence. */ }
      }
    }
    return index.keys;
  } finally { fs.closeSync(fd); }
};
/** No mesh state/lock: one O_APPEND write and durability barrier per final observation.
 * Unknown is an observation, not a receipt; a later receiver-delivered line is valid.
 * Keep today's UTC partition and the preceding six (seven daily partitions).
 */
export const appendDeliveryOutcome = (
  meshRoot: string,
  send: DeliverySend,
  outcome: DeliveryOutcome,
  reason: string,
  at = Date.now(),
): void => {
  const directory = path.join(meshRoot, "delivery-outcomes");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const cutoff = day(at - 6 * 24 * 60 * 60_000);
  const key = outcomeKey({ ...send, outcome });
  const retained: string[] = [];
  for (const name of fs.readdirSync(directory)) {
    if (!/^\d{8}\.jsonl$/.test(name)) continue;
    const file = path.join(directory, name);
    if (name.slice(0, 8) < cutoff) { fs.rmSync(file, { force: true }); indexes.delete(file); }
    else retained.push(file);
  }
  for (const file of retained.sort().reverse()) {
    if (indexedOutcomes(file).has(key)) {
      // A restart can repeat a final observation after append but before queue cleanup.
      // Windows FlushFileBuffers needs a writable handle (a read-only fsync is EPERM);
      // O_APPEND without a write keeps the receipt barrier unable to alter the journal.
      const receipt = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_APPEND);
      try { fs.fsyncSync(receipt); } finally { fs.closeSync(receipt); }
      syncDirectoryChain(directory);
      return;
    }
  }
  const record: DeliveryOutcomeRecord = { ...send, outcome, reason, at };
  const line = Buffer.from(JSON.stringify(record) + "\n");
  const fd = fs.openSync(path.join(directory, day(at) + ".jsonl"), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND, 0o600);
  try {
    if (fs.writeSync(fd, line) !== line.length) throw new Error("Short delivery outcome append");
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  syncDirectoryChain(directory);
};

/** Validate retained host metadata; never infer receipt identity from arbitrary message data. */
export const deliverySend = (value: unknown): DeliverySend | undefined => {
  if (!value || typeof value !== "object") return undefined;
  const send = value as DeliverySend;
  return typeof send.eventId === "string" && typeof send.to === "string" && typeof send.from === "string" &&
    (send.mode === "steer" || send.mode === "followUp" || send.mode === "publish")
    ? { eventId: send.eventId, to: send.to, from: send.from, mode: send.mode } : undefined;
};
