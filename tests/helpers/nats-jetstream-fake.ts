// An in-process fake of the five JetStream operations the NATS mesh state backend uses (smarty-dev#7504), with the
// server's semantics: one stream, expected-last-sequence CAS (err 10071 as NatsWrongLastSequenceError), msgID dedupe
// (a duplicate returns the original sequence), ordered delivery after a sequence, asynchronous like the network.
import { NatsWrongLastSequenceError, type JetStreamPort, type NatsStreamMessage } from "../../src/mesh/state-backend-nats.js";

export interface FakeJetStreamServer {
  readonly messages: NatsStreamMessage[];
  /** Ports opened on this server (one per backend, like separate connections). */
  port(): FakePort;
  version: string;
  syncAlways: boolean | undefined;
  /** Runs before each publish is stored (inject a concurrent writer); return "lose-ack" to store it but fail the ack once. */
  beforePublish?: (data: Uint8Array) => void | "lose-ack";
  /** Appends a raw record (a foreign or malformed writer). */
  append(data: Uint8Array): number;
  /** Holds deliveries to consumers (a slow projector) until `resume`. */
  pause(): void;
  resume(): void;
}

export interface FakePort extends JetStreamPort { readonly calls: string[] }

export const fakeJetStream = (): FakeJetStreamServer => {
  const messages: NatsStreamMessage[] = [];
  const ids = new Map<string, number>();
  const listeners = new Set<(message: NatsStreamMessage) => void>();
  let paused: (() => void)[] | undefined;
  const later = (run: () => void) => { if (paused) paused.push(run); else setImmediate(run); };
  const store = (data: Uint8Array): number => {
    const message = { seq: messages.length + 1, data: new Uint8Array(data) };
    messages.push(message);
    for (const listener of listeners) later(() => listener(message));
    return message.seq;
  };
  const server: FakeJetStreamServer = {
    messages, version: "2.14.7", syncAlways: true,
    append: store,
    pause: () => { paused ??= []; },
    resume: () => { const queued = paused ?? []; paused = undefined; for (const run of queued) setImmediate(run); },
    port: () => {
      const calls: string[] = [];
      let open = true;
      const own = new Set<(message: NatsStreamMessage) => void>();
      const port: FakePort = {
        calls,
        server: async () => { calls.push("server"); return { version: server.version, syncAlways: server.syncAlways }; },
        ensureStream: async () => { calls.push("ensureStream"); return { lastSeq: messages.length }; },
        lastSequence: async () => { calls.push("lastSequence"); return messages.length; },
        publish: async (_subject, data, { expectLastSequence, msgID }) => {
          calls.push("publish");
          if (!open) throw new Error("connection closed");
          await new Promise<void>(resolve => setImmediate(resolve));
          const seen = ids.get(msgID);
          if (seen !== undefined) return { seq: seen, duplicate: true };
          if (expectLastSequence !== messages.length) throw new NatsWrongLastSequenceError(expectLastSequence, `wrong last sequence: ${messages.length}`);
          const verdict = server.beforePublish?.(data);
          if (expectLastSequence !== messages.length) throw new NatsWrongLastSequenceError(expectLastSequence, `wrong last sequence: ${messages.length}`);
          const seq = store(data);
          ids.set(msgID, seq);
          if (verdict === "lose-ack") throw new Error("timeout: no ack");
          return { seq, duplicate: false };
        },
        consume: async (_stream, afterSeq, onMessage) => {
          calls.push("consume");
          let delivered = afterSeq;
          const listener = (message: NatsStreamMessage) => { if (message.seq === delivered + 1) { delivered = message.seq; onMessage(message); } };
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
