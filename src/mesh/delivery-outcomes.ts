import fs from "node:fs";
import path from "node:path";
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
  const record: DeliveryOutcomeRecord = { ...send, outcome, reason, at };
  const line = Buffer.from(JSON.stringify(record) + "\n");
  const fd = fs.openSync(path.join(directory, day(at) + ".jsonl"), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND, 0o600);
  try {
    if (fs.writeSync(fd, line) !== line.length) throw new Error("Short delivery outcome append");
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  syncDirectoryChain(directory);
  const cutoff = day(at - 6 * 24 * 60 * 60_000);
  for (const name of fs.readdirSync(directory)) {
    if (/^\d{8}\.jsonl$/.test(name) && name.slice(0, 8) < cutoff) fs.rmSync(path.join(directory, name), { force: true });
  }
};

/** Validate retained host metadata; never infer receipt identity from arbitrary message data. */
export const deliverySend = (value: unknown): DeliverySend | undefined => {
  if (!value || typeof value !== "object") return undefined;
  const send = value as DeliverySend;
  return typeof send.eventId === "string" && typeof send.to === "string" && typeof send.from === "string" &&
    (send.mode === "steer" || send.mode === "followUp" || send.mode === "publish")
    ? { eventId: send.eventId, to: send.to, from: send.from, mode: send.mode } : undefined;
};
