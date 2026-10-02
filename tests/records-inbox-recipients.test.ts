import { describe, expect, it } from "vitest";
import { RecordsInbox } from "../src/records/inbox.js";
import type { RecordEnvelope, RecordsOps } from "../src/records/store.js";

// S1: a root's alias that equals another root's canonical ID must not widen its records mailbox.
const store = (records: RecordEnvelope[]) => {
  const cursors = new Map<string, number>();
  const opened = new Map<string, readonly string[]>();
  const ops = {
    org: "org", origin: "origin",
    async openConsumer(consumer: string, names: readonly string[]) {
      opened.set(consumer, names);
      return { after: cursors.get(consumer) ?? 0, pending: null };
    },
    async saveConsumer(consumer: string, after: number) { cursors.set(consumer, after); },
    async byIds(ids: readonly string[]) { return records.filter(record => ids.includes(record.id)); },
    async page(args: { after: number; limit: number; to?: readonly string[]; exceptAuthor?: string }) {
      const matched = records.filter(record => record.sequence > args.after && record.from !== args.exceptAuthor &&
        (!args.to || args.to.includes(String(record.data.to))));
      const next = Math.max(args.after, ...records.map(record => record.sequence));
      return { records: matched.slice(0, args.limit), next, frontier: next, origin: "origin" };
    },
  } as unknown as RecordsOps;
  return { ops, opened };
};
const ask = (sequence: number, to: string): RecordEnvelope => ({
  id: `r${sequence}`, org: "org", origin: "origin", sequence, ref: "ref", topic: "t", kind: "ask" as RecordEnvelope["kind"],
  from: "session:sender", createdAt: 0, data: { to }, key: `k${sequence}`,
} as RecordEnvelope);
const session = { holdsBatch: () => true };

describe("records inbox recipient set", () => {
  it("delivers a record addressed to a root ID only to that root, not to a root aliased by that ID", async () => {
    const { ops, opened } = store([ask(1, "session:incumbent"), ask(2, "rival-name")]);
    const incumbent = new RecordsInbox(ops, "session:incumbent", () => ["session:incumbent", "incumbent-name"]);
    const rival = new RecordsInbox(ops, "session:rival", () => ["session:rival", "session:incumbent"]);
    expect((await incumbent.next(session)).records.map(record => record.id)).toEqual(["r1"]);
    expect((await rival.next(session)).records.map(record => record.id)).toEqual([]);
    expect(opened.get("session:rival")).toEqual(["session:rival"]);
  });

  it("keeps the root's own canonical ID and ordinary names", async () => {
    const { ops } = store([ask(1, "session:own"), ask(2, "own-name")]);
    const inbox = new RecordsInbox(ops, "session:principal", () => ["session:own", "own-name"]);
    expect((await inbox.next(session)).records.map(record => record.id)).toEqual(["r1", "r2"]);
  });
});
