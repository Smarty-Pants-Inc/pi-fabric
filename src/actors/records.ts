import type { MeshEvent, MeshStore } from "../mesh/store.js";

export interface FabricActorRecordsOptions {
  topic: string;
  /** Default 512; larger requests are capped at 4096. */
  maxEntries?: number;
}

export interface FabricActorRecord {
  readonly state: "held" | "waited" | "answered" | "open";
  readonly at: string;
}

export interface FabricActorRecordsView {
  readonly get: (key: string) => Readonly<FabricActorRecord> | undefined;
}

export type FabricActorRecordsSnapshot = ReadonlyArray<readonly [string, FabricActorRecord]>;

const TOPIC_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
export const normalizeActorRecords = (value: unknown): FabricActorRecordsOptions | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid actor records option");
  }
  const { topic, maxEntries } = value as { topic?: unknown; maxEntries?: unknown };
  if (typeof topic !== "string" || !TOPIC_PATTERN.test(topic)) {
    throw new Error("Invalid actor records topic");
  }
  if (maxEntries !== undefined && (typeof maxEntries !== "number" || !Number.isSafeInteger(maxEntries) || maxEntries < 1)) {
    throw new Error("Actor records maxEntries must be a positive safe integer");
  }
  return { topic, maxEntries: Math.min((maxEntries as number | undefined) ?? 512, 4096) };
};

/** Host-only projection. Recency is a valid mesh write, never a predicate read. */
export class ActorRecords {
  readonly #entries = new Map<string, FabricActorRecord>();
  #sequence = 0;

  readonly options: FabricActorRecordsOptions;
  constructor(options: FabricActorRecordsOptions) {
    this.options = normalizeActorRecords(options)!;
  }

  /** One-time retained-window replay on creation/restoration, not on validity checks. */
  replay(mesh: Pick<MeshStore, "latestSequence" | "maxReadEvents"> & Partial<Pick<MeshStore, "read">>): void {
    if (!mesh.read) return;
    const ceiling = mesh.latestSequence();
    let after = 0;
    while (after < ceiling) {
      const events = mesh.read({ topic: this.options.topic, after, limit: mesh.maxReadEvents });
      if (!events.length) break;
      for (const event of events) {
        if (event.sequence > ceiling) return;
        this.accept(event);
      }
      const next = events.at(-1)!.sequence;
      if (next <= after) break;
      after = next;
    }
  }

  accept(event: MeshEvent): void {
    if (event.topic !== this.options.topic || !Number.isSafeInteger(event.sequence) || event.sequence <= this.#sequence) return;
    const data = event.data;
    if (typeof data !== "object" || data === null || Array.isArray(data)) return;
    const { key, state } = data as { key?: unknown; state?: unknown };
    if (typeof key !== "string" || (state !== "held" && state !== "waited" && state !== "answered" && state !== "open")) return;
    const date = new Date(event.createdAt);
    if (!Number.isFinite(date.getTime())) return;
    this.#sequence = event.sequence;
    this.#entries.delete(key);
    this.#entries.set(key, Object.freeze({ state, at: date.toISOString() }));
    while (this.#entries.size > (this.options.maxEntries ?? 512)) {
      this.#entries.delete(this.#entries.keys().next().value!);
    }
  }

  snapshot(): FabricActorRecordsSnapshot {
    return [...this.#entries];
  }
}
