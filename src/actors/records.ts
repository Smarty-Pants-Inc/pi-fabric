import type { MeshEvent, MeshStore } from "../mesh/store.js";

export interface FabricActorRecordsOptions {
  topic: string;
  /** Default 512; larger requests are capped at 4096. */
  maxEntries?: number;
  /** Positive safe integer; default six hours. Age >= maxAgeMs is unknown. */
  maxAgeMs?: number;
}

export interface FabricActorRecord {
  readonly state: "held" | "waited" | "answered" | "open";
  readonly at: string;
  readonly fresh: boolean;
}

export interface FabricActorRecordsView {
  /** Unknown, expired, or unverifiable records return undefined. */
  readonly get: (key: string) => Readonly<FabricActorRecord> | undefined;
}

export type FabricActorRecordsSnapshot = ReadonlyArray<readonly [string, FabricActorRecord]>;

interface ProjectedRecord {
  readonly state: FabricActorRecord["state"];
  readonly at: string;
  readonly eventTime: number;
  readonly sequence: number;
}

const DEFAULT_MAX_AGE_MS = 6 * 60 * 60 * 1_000;
const TOPIC_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
export const normalizeActorRecords = (value: unknown): FabricActorRecordsOptions | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid actor records option");
  }
  const { topic, maxEntries, maxAgeMs } = value as { topic?: unknown; maxEntries?: unknown; maxAgeMs?: unknown };
  if (typeof topic !== "string" || !TOPIC_PATTERN.test(topic)) {
    throw new Error("Invalid actor records topic");
  }
  if (maxEntries !== undefined && (typeof maxEntries !== "number" || !Number.isSafeInteger(maxEntries) || maxEntries < 1)) {
    throw new Error("Actor records maxEntries must be a positive safe integer");
  }
  if (maxAgeMs !== undefined && (typeof maxAgeMs !== "number" || !Number.isSafeInteger(maxAgeMs) || maxAgeMs < 1)) {
    throw new Error("Actor records maxAgeMs must be a positive safe integer");
  }
  return {
    topic,
    maxEntries: Math.min((maxEntries as number | undefined) ?? 512, 4096),
    maxAgeMs: (maxAgeMs as number | undefined) ?? DEFAULT_MAX_AGE_MS,
  };
};

/** Host-only projection. Recency is a valid mesh write, never a predicate read. */
export class ActorRecords {
  readonly #entries = new Map<string, ProjectedRecord>();
  #sequence = 0;
  #replayFloor = 1;

  readonly options: FabricActorRecordsOptions;
  constructor(options: FabricActorRecordsOptions) {
    this.options = normalizeActorRecords(options)!;
  }

  /** One-time retained-window replay on creation/restoration, not on validity checks. */
  replay(mesh: Pick<MeshStore, "latestSequence" | "maxReadEvents"> & Partial<Pick<MeshStore, "read">>): void {
    const ceiling = mesh.latestSequence();
    // Replaying an empty/missing window must not preserve a previous terminal value.
    this.#entries.clear();
    this.#sequence = 0;
    this.#replayFloor = ceiling + 1;
    let after = 0;
    replay: while (mesh.read && after < ceiling) {
      const events = mesh.read({ topic: this.options.topic, after, limit: mesh.maxReadEvents });
      if (!events.length) break;
      for (const event of events) {
        if (event.sequence > ceiling) break replay;
        if (Number.isSafeInteger(event.sequence) && event.sequence > 0) {
          this.#replayFloor = Math.min(this.#replayFloor, event.sequence);
        }
        this.accept(event);
      }
      const next = events.at(-1)!.sequence;
      if (!Number.isSafeInteger(next) || next <= after) break;
      after = next;
    }
    // An event missing from this retained window cannot arrive later as "current".
    this.#sequence = Math.max(this.#sequence, ceiling);
  }

  accept(event: MeshEvent): void {
    if (event.topic !== this.options.topic) return;
    const data = event.data;
    if (typeof data !== "object" || data === null || Array.isArray(data)) return;
    const { key, state } = data as { key?: unknown; state?: unknown };
    if (typeof key !== "string" || (state !== "held" && state !== "waited" && state !== "answered" && state !== "open")) return;
    if (!Number.isSafeInteger(event.sequence) || event.sequence < 1) {
      // No ordering proof: fail open instead of retaining an older terminal value.
      this.#entries.delete(key);
      return;
    }
    if (event.sequence < this.#replayFloor || event.sequence <= this.#sequence) return;
    this.#sequence = event.sequence;
    this.#entries.delete(key);
    if (typeof event.createdAt !== "number" || !Number.isSafeInteger(event.createdAt) || event.createdAt < 0) return;
    const date = new Date(event.createdAt);
    if (!Number.isFinite(date.getTime())) return;
    this.#entries.set(key, Object.freeze({ state, at: date.toISOString(), eventTime: event.createdAt, sequence: event.sequence }));
    while (this.#entries.size > (this.options.maxEntries ?? 512)) {
      this.#entries.delete(this.#entries.keys().next().value!);
    }
  }

  snapshot(now = Date.now()): FabricActorRecordsSnapshot {
    const values: Array<readonly [string, FabricActorRecord]> = [];
    for (const [key, record] of this.#entries) {
      const age = now - record.eventTime;
      if (!Number.isFinite(age) || age < 0 || age >= (this.options.maxAgeMs ?? DEFAULT_MAX_AGE_MS) || record.sequence < this.#replayFloor) {
        this.#entries.delete(key);
        continue;
      }
      values.push([key, Object.freeze({ state: record.state, at: record.at, fresh: true })]);
    }
    return values;
  }
}
