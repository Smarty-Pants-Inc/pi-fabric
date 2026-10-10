import type { MeshEvent, MeshPublishInput, MeshTailResult } from "./event-log.js";
import { MeshStore, type MeshStoreOptions } from "./store.js";
import type { JetStreamEventLog, JetStreamEventLogOptions } from "./jetstream-event-log.js";

export interface EventLogReadOptions {
  after?: number;
  topic?: string;
  to?: string;
  limit?: number;
}

/** Async experiment seam; MeshStore keeps its synchronous file API and default. */
export interface MeshEventLogBackend {
  publish(input: MeshPublishInput): Promise<MeshEvent>;
  publishBatch(inputs: MeshPublishInput[]): Promise<MeshEvent[]>;
  read(input?: EventLogReadOptions): Promise<MeshEvent[]>;
  tail(cursor: number, limit?: number): Promise<MeshTailResult>;
  nextEventAfter(after: number): Promise<MeshEvent | undefined>;
  latestSequence(): Promise<number>;
  oldestSequence(): Promise<number | undefined>;
  latestCursor(): Promise<{ cursor: number; last?: { sequence: number; id: string } }>;
  close(): Promise<void>;
}

/** Missing ACK is not proof of non-publication. Persist/retry exactly these bytes and id. */
export class JetStreamPublishUncertainError extends Error {
  constructor(readonly pending: import("./jetstream-event-log.js").PendingMeshEvent, cause: unknown) {
    super(`JetStream publication outcome uncertain for ${pending.id}; retry publishPrepared(pending) within the duplicate window`, { cause });
    this.name = "JetStreamPublishUncertainError";
  }
}

export class JetStreamCursorExpiredError extends Error {
  constructor(readonly cursor: number, readonly firstSequence: number) {
    super(`JetStream cursor ${cursor} precedes retained sequence ${firstSequence}`);
    this.name = "JetStreamCursorExpiredError";
  }
}

/** An ordered batch is not an atomic transaction. Checkpoint its ACKed prefix; retry exact suffix ids. */
export class JetStreamBatchPublishError extends Error {
  constructor(readonly committed: MeshEvent[], readonly remaining: import("./jetstream-event-log.js").PendingMeshEvent[], cause: unknown) {
    super("JetStream batch stopped; checkpoint committed prefix and retry remaining prepared events", { cause });
    this.name = "JetStreamBatchPublishError";
  }
}

/** Adapt the existing file implementation, rather than reproducing its semantics. */
export async function openFileEventLog(
  root: string, maxEventBytes = 64 * 1024, maxReadEvents = 1000, options: MeshStoreOptions = {},
): Promise<MeshEventLogBackend> {
  const store = new MeshStore(root, maxEventBytes, maxReadEvents, options);
  return {
    publish: input => store.publish(input),
    publishBatch: inputs => store.publishBatch(inputs),
    read: async input => store.read(input),
    tail: async (cursor, limit) => store.tail(cursor, limit),
    nextEventAfter: async after => store.nextEventAfter(after),
    latestSequence: async () => store.latestSequence(),
    oldestSequence: async () => store.oldestSequence(),
    latestCursor: async () => store.latestCursor(),
    close: async () => { store.closeState(); },
  };
}

/** NATS is loaded only on explicit use. No environment selector or runtime cutover. */
export async function openJetStreamEventLog(options: JetStreamEventLogOptions): Promise<JetStreamEventLog> {
  const { JetStreamEventLog } = await import("./jetstream-event-log.js");
  return JetStreamEventLog.open(options);
}

export type { JetStreamEventLogOptions, JetStreamReaderOptions, JetStreamReader, PendingMeshEvent } from "./jetstream-event-log.js";
