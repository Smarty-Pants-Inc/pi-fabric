import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { openFileEventLog, openJetStreamEventLog, type MeshEventLogBackend } from "../src/mesh/event-backend.js";
import type { MeshPublishInput } from "../src/mesh/event-log.js";
import { natsAvailable, startNatsCluster, type LocalNatsCluster } from "./helpers/nats-cluster.js";

const from = { id: "test:conformance", name: "conformance", kind: "main" as const };
let cluster: LocalNatsCluster;
const roots: string[] = [];
const logs: MeshEventLogBackend[] = [];
beforeAll(async () => { if (natsAvailable) cluster = await startNatsCluster(1, "conformance"); }, 40_000);
afterEach(async () => {
  for (const log of logs.splice(0)) await log.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
afterAll(async () => { await cluster?.stop(); });

for (const kind of ["file", "jetstream"] as const) {
  describe.skipIf(kind === "jetstream" && !natsAvailable)(`${kind} event-log conformance`, () => {
    const create = async (root = fs.mkdtempSync(path.join(os.tmpdir(), "event-conformance-"))) => {
      if (!roots.includes(root)) roots.push(root);
      const log = kind === "file" ? await openFileEventLog(root, 64 * 1024, 2000)
        : await openJetStreamEventLog({ root, servers: cluster.servers, maxReadEvents: 2000 });
      logs.push(log);
      return { log, root };
    };

    it("publishes ordered global events, reads after a sequence and tails individual cursors", async () => {
      const { log } = await create();
      const start = (await log.latestCursor()).cursor;
      const a = await log.publish({ topic: "team.auth", from, text: "one" });
      const b = await log.publish({ topic: "ops/alarm:v1", from, to: "reviewer", data: { task: 2 } });
      expect([a.sequence, b.sequence]).toEqual([1, 2]);
      expect(await log.read({ after: a.sequence })).toEqual([b]);
      const first = await log.tail(start, 1);
      expect(first.events).toEqual([a]);
      const second = await log.tail(first.nextOffset, 10);
      expect(second.events).toEqual([b]);
      expect(second.nextOffset).toBe((await log.latestCursor()).cursor);
      expect(await log.latestSequence()).toBe(b.sequence);
      expect(await log.oldestSequence()).toBe(a.sequence);
      expect(await log.nextEventAfter(a.sequence)).toEqual(b);
    });

    it("resumes a saved tail and sequence cursor after closing and reopening the backend", async () => {
      const { log, root } = await create();
      await log.publishBatch(Array.from({ length: 8 }, (_, n) => ({ topic: "team.resume", from, data: n })));
      const page = await log.tail(0, 3);
      await log.close();
      const restarted = (await create(root)).log;
      expect((await restarted.tail(page.nextOffset, 10)).events.map(event => event.data)).toEqual([3, 4, 5, 6, 7]);
      expect((await restarted.read({ after: page.events.at(-1)!.sequence })).map(event => event.data)).toEqual([3, 4, 5, 6, 7]);
    });

    it("dedupes a retry, including after backend restart, and returns original bytes", async () => {
      const { log, root } = await create();
      const packet = { topic: "team.once", from, text: "original", dedupeKey: "durable-once" };
      const first = await log.publish(packet);
      expect(await log.publish({ ...packet, text: "changed retry" })).toEqual(first);
      await log.close();
      const restarted = (await create(root)).log;
      expect(await restarted.publish(packet)).toEqual(first);
      expect(await restarted.read({ after: 0 })).toEqual([first]);
      expect(await restarted.latestSequence()).toBe(1);
    });

    it("publishes a large multi-prefix batch in order, with no lost or repeated event", async () => {
      const { log } = await create();
      const input: MeshPublishInput[] = Array.from({ length: 1200 }, (_, n) => ({ topic: "team.batch", from, data: n }));
      const published = [];
      for (let offset = 0; offset < input.length;) {
        const prefix = await log.publishBatch(input.slice(offset, offset + 256));
        expect(prefix.length).toBeGreaterThan(0);
        published.push(...prefix);
        offset += prefix.length;
      }
      expect(published.map(event => event.sequence)).toEqual(Array.from({ length: 1200 }, (_, i) => i + 1));
      expect((await log.read({ after: 0, limit: 2000 })).map(event => event.data)).toEqual(input.map(event => event.data));
      expect(new Set(published.map(event => event.id)).size).toBe(1200);
      await expect(log.publishBatch([])).rejects.toThrow("1..256");
      await expect(log.publishBatch(input.slice(0, 257))).rejects.toThrow("1..256");
    }, 60_000);

    it("filters exact topic and recipient, and reads the recent matching suffix by default", async () => {
      const { log } = await create();
      const topics = ["a.b", "a/b", "a:b", "a_b", "a-b"];
      for (let n = 0; n < 25; n++) await log.publish({ topic: topics[n % 5]!, from, to: n % 2 ? "odd" : "even", data: n });
      expect((await log.read({ after: 0, topic: "a.b" })).map(event => event.data)).toEqual([0, 5, 10, 15, 20]);
      expect((await log.read({ topic: "a.b", limit: 2 })).map(event => event.data)).toEqual([15, 20]);
      expect((await log.read({ after: 0, topic: "a.b", to: "odd" })).map(event => event.data)).toEqual([5, 15]);
      expect(await log.read({ after: 0, topic: "empty.topic" })).toEqual([]);
      await expect(log.publish({ topic: "bad topic", from })).rejects.toThrow("Invalid Fabric mesh topic");
    });
  });
}
