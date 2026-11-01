import { createHash } from "node:crypto";
import type { MeshEvent, MeshIdentity, MeshStore } from "../mesh/store.js";

/** DLQ/max-deliveries, not a timeout: only failed singleton render/admission attempts count. */
export const ROOT_INBOX_MAX_FAILED_OFFERS = 3;
interface Payload { chunks: number; sha256: string }
export interface RootInboxDeadLetter {
  version: 1;
  rootId: string;
  id: string;
  sequence: number;
  failures: number;
  firstFailedAt: number;
  lastFailedAt: number;
  reason: string;
  status: "failed" | "parked" | "replaying" | "consumed";
  original?: Payload;
  replay?: Payload;
  parkedAt?: number;
  reported?: boolean;
}
export type RootInboxReplayRepair = Partial<Pick<MeshEvent, "text" | "kind" | "createdAt">>;
const digest = (text: string): string => createHash("sha256").update(text).digest("hex");

/**
 * Reuse mesh state and its durability barrier, not another broker, file journal, timer or queue.
 * Like RabbitMQ DLX / NATS JetStream maxDeliver, park before advancing the consumer cursor.
 * Actor dead letters use the same save-before-forget ordering, but their queue runtime is not
 * Main's inbox. Chunking here preserves the FULL event, even at the mesh value-size ceiling.
 */
export class RootInboxDeadLetters {
  readonly #consumed = new Map<string, RootInboxDeadLetter>();
  readonly #confirmed = new Set<string>();
  constructor(readonly mesh: MeshStore, readonly identity: MeshIdentity, readonly inboxKey: string) {}
  get prefix(): string { return `${this.inboxKey}/dead-letter/`; }
  key(id: string): string { return this.prefix + digest(id); }
  get(id: string): RootInboxDeadLetter | undefined {
    return this.#consumed.get(id) ?? this.mesh.get(this.key(id), { fresh: true })?.value as RootInboxDeadLetter | undefined;
  }
  list(): RootInboxDeadLetter[] {
    return this.mesh.listAll(this.prefix, { fresh: true }).map(row => this.get((row.value as RootInboxDeadLetter).id)!);
  }
  excludes(id: string): boolean {
    const status = this.get(id)?.status;
    return status === "parked" || status === "replaying" || status === "consumed";
  }
  /** Park/replay are expressly NOT consumption, even below the ordinary cursor/high-water. */
  acked(id: string): boolean | undefined {
    const status = this.get(id)?.status;
    return status === "consumed" ? true : status === "parked" || status === "replaying" ? false : undefined;
  }
  async failed(event: MeshEvent, reason: string, now: number): Promise<void> {
    const entry = this.mesh.get(this.key(event.id), { fresh: true });
    const prior = entry?.value as RootInboxDeadLetter | undefined;
    if (prior?.status === "parked" || prior?.status === "consumed") return;
    const failures = (prior?.failures ?? 0) + 1;
    const parking = failures >= ROOT_INBOX_MAX_FAILED_OFFERS;
    const original = prior?.original ?? (parking ? await this.#store(event) : undefined);
    const value: RootInboxDeadLetter = {
      ...prior, version: 1, rootId: this.identity.id, id: event.id, sequence: event.sequence,
      failures, firstFailedAt: prior?.firstFailedAt ?? now, lastFailedAt: now, reason: reason.slice(0, 512),
      status: parking ? "parked" : "failed", ...(original ? { original } : {}),
      ...(parking ? { parkedAt: prior?.parkedAt ?? now } : {}),
    };
    await this.mesh.put({ key: this.key(event.id), value, identity: this.identity, ifVersion: entry?.version ?? 0 });
    // Do not permit the cursor to forget a parked event until its bytes and metadata are durable.
    // A failed barrier aborts this reconcile; the next reconcile confirms it before skipping.
    await this.mesh.sync();
  }
  async confirmParks(): Promise<void> {
    const keys = this.list().filter(row => row.original).map(row => JSON.stringify([row.id, row.status, row.original, row.replay]));
    if (keys.some(key => !this.#confirmed.has(key))) await this.mesh.sync();
    for (const key of keys) this.#confirmed.add(key);
  }
  async report(row: RootInboxDeadLetter, topic: string): Promise<void> {
    if (!row.original || row.reported) return;
    await this.mesh.publish({ topic, kind: "alarm", from: this.identity, to: this.identity.id,
      dedupeKey: `root-inbox-dead-letter:${this.identity.id}:${row.id}`,
      text: `Root inbox parked event ${row.id} after ${ROOT_INBOX_MAX_FAILED_OFFERS} failed singleton offers; replayable at ${this.key(row.id)}.`,
      data: { rootId: this.identity.id, reason: "dead_letter", eventId: row.id, sequence: row.sequence,
        failures: ROOT_INBOX_MAX_FAILED_OFFERS, deadLetterKey: this.key(row.id) },
    });
    // Publication has its own durable dedupe receipt: crash/save failure here cannot double-report.
    const entry = this.mesh.get(this.key(row.id), { fresh: true });
    if (entry) await this.mesh.put({ key: entry.key, value: { ...(entry.value as RootInboxDeadLetter), reported: true },
      identity: this.identity, ifVersion: entry.version });
  }
  read(id: string, original = false): MeshEvent {
    const row = this.get(id), payload = original ? row?.original : row?.replay ?? row?.original;
    if (!payload) throw new Error("Root inbox dead letter has no retained event");
    let text = "";
    for (let index = 0; index < payload.chunks; index++) {
      const chunk = this.mesh.get(this.#chunkKey(id, payload.sha256, index), { fresh: true })?.value;
      if (typeof chunk !== "string") throw new Error("Root inbox dead letter is missing event bytes");
      text += chunk;
    }
    if (digest(text) !== payload.sha256) throw new Error("Root inbox dead letter event digest mismatch");
    return JSON.parse(text) as MeshEvent;
  }
  async replay(id: string, repair: RootInboxReplayRepair, validate: (event: MeshEvent) => void): Promise<MeshEvent> {
    const entry = this.mesh.get(this.key(id), { fresh: true });
    const row = entry?.value as RootInboxDeadLetter | undefined;
    if (!row?.original || row.status === "consumed") throw new Error("Root inbox dead letter is not replayable");
    // Only rendering fields may be repaired. Identity, sender, provenance and work receipts stay
    // original; the original full bytes remain separately readable after every replay/ack.
    const original = this.read(id, true);
    const event = { ...original,
      ...(repair.text !== undefined ? { text: repair.text } : {}),
      ...(repair.kind !== undefined ? { kind: repair.kind } : {}),
      ...(repair.createdAt !== undefined ? { createdAt: repair.createdAt } : {}),
    };
    validate(event);
    const replay = await this.#store(event);
    await this.mesh.put({ key: entry!.key, value: { ...row, status: "replaying", replay }, identity: this.identity, ifVersion: entry!.version });
    await this.mesh.sync();
    return event;
  }
  consume(id: string): void {
    const row = this.get(id);
    if (row?.status === "replaying") this.#consumed.set(id, { ...row, status: "consumed" });
  }
  async flush(): Promise<void> {
    for (const [id, row] of this.#consumed) {
      const entry = this.mesh.get(this.key(id), { fresh: true });
      await this.mesh.put({ key: this.key(id), value: { ...row, reported: (entry?.value as RootInboxDeadLetter | undefined)?.reported ?? row.reported },
        identity: this.identity, ifVersion: entry?.version ?? 0 });
      await this.mesh.sync();
      this.#consumed.delete(id);
    }
  }
  #chunkKey(id: string, sha256: string, index: number): string {
    return `${this.inboxKey}/dead-letter-bytes/${digest(id)}/${sha256}/${index}`;
  }
  async #store(event: MeshEvent): Promise<Payload> {
    const text = JSON.stringify(event), sha256 = digest(text);
    // JSON escaping expands a UTF-16 code unit by at most six bytes; leave envelope headroom.
    const size = Math.max(1, Math.floor((this.mesh.maxEventBytes - 512) / 8));
    const chunks = Math.ceil(text.length / size);
    for (let index = 0; index < chunks; index++) await this.mesh.put({
      key: this.#chunkKey(event.id, sha256, index), value: text.slice(index * size, (index + 1) * size), identity: this.identity,
    });
    return { chunks, sha256 };
  }
}
