// An in-process fake of the JetStream operations the NATS mesh state backend uses (smarty-dev#7504), with the
// server's semantics: one stream, expected-last-sequence CAS (err 10071 as NatsWrongLastSequenceError), msgID dedupe
// (a duplicate returns the original sequence), ordered delivery after a sequence, asynchronous like the network.
import { MeshStateNatsRefusedError, NatsWrongLastSequenceError, type JetStreamPort, type NatsStreamMessage } from "../../src/mesh/state-backend-nats.js";

export interface FakeJetStreamServer {
  /** Every message ever stored, by sequence (index seq - 1), purged ones included; `firstSeq` is the purge floor. */
  readonly messages: (NatsStreamMessage & { subject: string })[];
  readonly firstSeq: number;
  /** Runs before each purge is applied (inject a racing writer). */
  beforePurge?: () => void;
  /** Runs (awaited) before a consumer starts: the window between a snapshot load and the consumer start. */
  beforeConsume?: (afterSeq: number) => Promise<void> | void;
  /** Hides stored messages from getMessage (a dropped chunk). */
  readonly hidden: Set<number>;
  /** Stream names that exist on this server (ensureStream adds one; tests add an old-named stream). */
  readonly streams: Set<string>;
  /** Ports opened on this server (one per backend, like separate connections). */
  port(): FakePort;
  version: string;
  syncAlways: boolean | undefined;
  /** The server INFO fields the backend checks. */
  maxPayload: number | undefined;
  authRequired: boolean;
  /**
   * The stream's max_msg_size: set by the first ensureStream or createStream (like a created stream); preset it for an
   * existing one (-1: none). Unset, `streamInfo` reports the backend's cap (an existing stream a migration made).
   */
  streamMaxMsgSize: number | undefined;
  /** The target identity `server()` reports (smarty-dev#9690). */
  identity: { name?: string; cluster?: string; domain?: string; account?: string };
  /** Whether the one stream exists (default true), and its creation time. */
  streamExists: boolean;
  streamCreated: string;
  /** Runs before each publish is stored (inject a concurrent writer); return "lose-ack" to store it but fail the ack once. */
  beforePublish?: (data: Uint8Array) => void | "lose-ack";
  /** Runs after a publish is stored, before its ack (inject writers that commit after it). */
  afterStore?: (seq: number) => void;
  /** Appends a raw record (a foreign or malformed writer). */
  append(data: Uint8Array, subject?: string): number;
  /** Holds deliveries to consumers (a slow projector) until `resume`. */
  pause(): void;
  resume(): void;
  /** Breaks every consumer's delivery (its onError). */
  breakDelivery(error: Error): void;
}

export interface FakePort extends JetStreamPort {
  readonly calls: string[];
  /** Holds this port's deliveries only (one lagging replica) until `resume`. */
  pause(): void;
  resume(): void;
}

export const fakeJetStream = (): FakeJetStreamServer => {
  const messages: (NatsStreamMessage & { subject: string })[] = [];
  let firstSeq = 0;
  const ids = new Map<string, number>();
  const listeners = new Set<(message: NatsStreamMessage) => void>();
  const breakers = new Set<(error: Error) => void>();
  let paused: (() => void)[] | undefined;
  const later = (run: () => void) => { if (paused) paused.push(run); else setImmediate(run); };
  const live = (seq: number) => seq >= Math.max(1, firstSeq) ? messages[seq - 1] : undefined;
  const store = (data: Uint8Array, subject = "state", msgID?: string): number => {
    const message = { seq: messages.length + 1, data: new Uint8Array(data), subject, ...(msgID ? { msgID } : {}) };
    if (firstSeq === 0) firstSeq = message.seq;
    messages.push(message);
    for (const listener of listeners) later(() => listener(message));
    return message.seq;
  };
  const server: FakeJetStreamServer = {
    messages, version: "2.14.7", syncAlways: true, hidden: new Set<number>(), maxPayload: 1024 * 1024, authRequired: false,
    streamMaxMsgSize: undefined, streams: new Set<string>(), identity: { name: "fake-server", account: "FLEET" },
    streamExists: true, streamCreated: "2026-10-11T00:00:00.000000000Z",
    get firstSeq() { return firstSeq; },
    append: (data, subject) => store(data, subject),
    pause: () => { paused ??= []; },
    resume: () => { const queued = paused ?? []; paused = undefined; for (const run of queued) setImmediate(run); },
    breakDelivery: (error) => { for (const broken of breakers) broken(error); },
    port: () => {
      const calls: string[] = [];
      let open = true;
      const own = new Set<(message: NatsStreamMessage) => void>();
      let held: (() => void)[] | undefined;
      const port: FakePort = {
        calls,
        pause: () => { held ??= []; },
        resume: () => { const queued = held ?? []; held = undefined; for (const run of queued) setImmediate(run); },
        server: async () => {
          calls.push("server");
          return { version: server.version, syncAlways: server.syncAlways, authRequired: server.authRequired,
            ...(server.maxPayload === undefined ? {} : { maxPayload: server.maxPayload }), ...server.identity };
        },
        ensureStream: async (stream, _subjects, { maxMsgSize, create }) => {
          calls.push("ensureStream");
          // `create: false` (a root's open, a proof, a migration's existing stream) never creates one.
          if (create === false && !server.streamExists) {
            throw new MeshStateNatsRefusedError(`the stream ${stream} does not exist (a NATS root opens only a migrated stream)`);
          }
          server.streams.add(stream);
          server.streamExists = true;
          server.streamMaxMsgSize ??= maxMsgSize;
          return { lastSeq: messages.length, maxMsgSize: server.streamMaxMsgSize };
        },
        streamExists: async (stream) => { calls.push("streamExists"); return server.streams.has(stream); },
        streamInfo: async () => {
          calls.push("streamInfo");
          return server.streamExists ? { created: server.streamCreated, lastSeq: messages.length, maxMsgSize: server.streamMaxMsgSize ?? 1024 * 1024 } : undefined;
        },
        createStream: async (stream, _subjects, { maxMsgSize }) => {
          calls.push("createStream");
          if (server.streamExists) throw new Error(`NATS stream ${stream} already exists`);
          server.streams.add(stream);
          server.streamExists = true;
          server.streamMaxMsgSize = maxMsgSize;
          return { created: server.streamCreated };
        },
        deleteStream: async () => {
          calls.push("deleteStream");
          const existed = server.streamExists;
          server.streamExists = false;
          messages.splice(0);
          firstSeq = 0;
          ids.clear();
          return existed;
        },
        lastSequence: async () => { calls.push("lastSequence"); return messages.length; },
        firstSequence: async () => { calls.push("firstSequence"); return firstSeq; },
        getMessage: async (_stream, seq) => {
          calls.push("getMessage");
          await new Promise<void>(resolve => setImmediate(resolve));
          const message = server.hidden.has(seq) ? undefined : live(seq);
          return message && { seq: message.seq, data: message.data, ...(message.msgID ? { msgID: message.msgID } : {}) };
        },
        lastBySubject: async (_stream, subject) => {
          calls.push("lastBySubject");
          for (let seq = messages.length; seq >= Math.max(1, firstSeq); seq--) {
            const message = messages[seq - 1]!;
            // One stream (stream names are ignored), so subjects compare by their last segment (state, snapshot): a
            // backend on a root's hashed stream and a test replica opened as "S" read the same stream (smarty-dev#9663).
            if (message.subject.split(".").at(-1) === subject.split(".").at(-1)) return { seq, data: message.data, ...(message.msgID ? { msgID: message.msgID } : {}) };
          }
          return undefined;
        },
        purge: async (_stream, beforeSeq) => {
          calls.push("purge");
          await new Promise<void>(resolve => setImmediate(resolve));
          server.beforePurge?.();
          firstSeq = Math.max(firstSeq, Math.min(beforeSeq, messages.length + 1));
        },
        publish: async (subject, data, { expectLastSequence, msgID }) => {
          calls.push("publish");
          if (!open) throw new Error("connection closed");
          if (server.streamMaxMsgSize !== undefined && server.streamMaxMsgSize > 0 && data.byteLength > server.streamMaxMsgSize) {
            throw new Error("message size exceeds maximum allowed"); // the server's own refusal: final, not transient
          }
          await new Promise<void>(resolve => setImmediate(resolve));
          // nats-server 2.14 checks the expected last sequence BEFORE the duplicate window (measured on the real server).
          if (expectLastSequence !== messages.length) throw new NatsWrongLastSequenceError(expectLastSequence, `wrong last sequence: ${messages.length}`);
          const seen = ids.get(msgID);
          if (seen !== undefined) return { seq: seen, duplicate: true };
          const verdict = server.beforePublish?.(data);
          if (expectLastSequence !== messages.length) throw new NatsWrongLastSequenceError(expectLastSequence, `wrong last sequence: ${messages.length}`);
          const seq = store(data, subject, msgID);
          ids.set(msgID, seq);
          server.afterStore?.(seq);
          if (verdict === "lose-ack") throw Object.assign(new Error("timeout: no ack"), { name: "TimeoutError", transient: true });
          return { seq, duplicate: false };
        },
        consume: async (_stream, afterSeq, onMessage, onError) => {
          calls.push("consume");
          await server.beforeConsume?.(afterSeq);
          if (onError) breakers.add(onError);
          // Like an ordered consumer: in order, from the first message still stored after `afterSeq` (a purge skips).
          let delivered = afterSeq;
          const deliver = (message: NatsStreamMessage) => {
            if (held) { held.push(() => deliver(message)); return; }
            if (message.seq > delivered && live(message.seq)) { delivered = message.seq; onMessage({ seq: message.seq, data: message.data }); }
          };
          const listener = deliver;
          for (const message of messages.slice(afterSeq)) later(() => listener(message));
          listeners.add(listener); own.add(listener);
          return { stop: async () => { listeners.delete(listener); own.delete(listener); } };
        },
        close: async () => { open = false; for (const listener of own) listeners.delete(listener); },
      };
      return port;
    },
  };
  return server;
};
