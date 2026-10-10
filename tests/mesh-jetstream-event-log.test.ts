import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { connect } from "nats";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { openJetStreamEventLog } from "../src/mesh/event-backend.js";
import { JetStreamCursorExpiredError, JetStreamBatchPublishError, JetStreamPublishUncertainError } from "../src/mesh/event-backend.js";
import { natsAvailable, startNatsCluster, type LocalNatsCluster } from "./helpers/nats-cluster.js";

const from = { id: "test:jetstream", name: "jetstream", kind: "main" as const };
let cluster: LocalNatsCluster;
let serial = 0;
const options = () => ({ root: path.join(cluster.root, `mesh-${serial++}`), servers: cluster.servers });
beforeAll(async () => { if (natsAvailable) cluster = await startNatsCluster(1, "specific"); }, 40_000);
afterAll(async () => { await cluster?.stop(); });

describe.skipIf(!natsAvailable)("opt-in JetStream event log", () => {
  it("uses one stream/root and injective topic subjects with exact Nats-Msg-Id", async () => {
    const one = await openJetStreamEventLog(options());
    const two = await openJetStreamEventLog(options());
    const nc = await connect({ servers: cluster.servers });
    try {
      expect(one.streamName).not.toBe(two.streamName);
      const topics = ["team.a", "team/a", "team:a", "team_a", "team-a"];
      expect(new Set(topics.map(topic => one.subject(topic))).size).toBe(5);
      for (const topic of topics) expect(one.subject(topic).split(".")).toHaveLength(3);
      const event = await one.publish({ topic: topics[0]!, from, eventId: "fabric-known-id" });
      const manager = await nc.jetstreamManager();
      const info = await manager.streams.info(one.streamName);
      expect(info.config.subjects).toEqual([`${one.subjectPrefix}.*`]);
      expect(info.config.retention).toBe("limits");
      expect(info.config.max_bytes).toBe(64 * 1024 * 1024);
      expect(info.config.max_age).toBe(0);
      const stored = await manager.streams.getMessage(one.streamName, { seq: event.sequence });
      expect(stored.header.get("Nats-Msg-Id")).toBe(event.id);
      expect(JSON.parse(Buffer.from(stored.data).toString()).sequence).toBeUndefined();
      expect(await two.read({ after: 0 })).toEqual([]);
      expect(fs.existsSync(path.join(cluster.root, "sequence"))).toBe(false);
      expect(fs.existsSync(path.join(cluster.root, "events.jsonl"))).toBe(false);
    } finally { await nc.close(); await one.close(); await two.close(); }
  });

  it("shares a canonical root id across host mount paths and refuses policy drift", async () => {
    const config = { ...options(), rootId: "shared-mesh-identity", retention: "archive" as const };
    const first = await openJetStreamEventLog(config);
    const second = await openJetStreamEventLog({ ...config, root: "/different/mount" });
    try {
      const event = await first.publish({ topic: "shared.root", from });
      expect(await second.read({ after: 0 })).toEqual([event]);
      await expect(openJetStreamEventLog({ ...config, duplicateWindowMs: 60_000 })).rejects.toThrow("configuration mismatch");
      await expect(first.publish({ topic: "fenced", from, fence: fn => fn() })).rejects.toThrow("synchronous publication fences");
    } finally { await first.close(); await second.close(); }
  });

  it("blocks next() until publication, checkpoints with confirmed ACK, and resumes a durable cursor", async () => {
    const config = options();
    let log = await openJetStreamEventLog(config);
    try {
      let reader = await log.openReader({ cursorId: "saved-reader", topic: "reader.work" });
      let settled = false;
      const waiting = reader.next(2000).then(event => { settled = true; return event; });
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(settled).toBe(false);
      const first = await log.publish({ topic: "reader.work", from, data: 1 });
      expect(await waiting).toEqual(first);
      await expect(reader.next(1000)).rejects.toThrow("checkpoint");
      await reader.ack(first);
      expect(await reader.checkpoint()).toBe(1);
      await reader.close();
      await log.close();
      log = await openJetStreamEventLog(config);
      reader = await log.openReader({ cursorId: "saved-reader", topic: "reader.work", after: 0 });
      await log.publish({ topic: "unrelated", from });
      const second = await log.publish({ topic: "reader.work", from, data: 2 });
      expect(await reader.next(1000)).toEqual(second);
      await reader.close(); // Unhandled delivery is NOT acknowledged.
      reader = await log.openReader({ cursorId: "saved-reader", topic: "reader.work" });
      expect(await reader.next(2000)).toEqual(second);
      await reader.ack(second);
      expect(await reader.checkpoint()).toBe(second.sequence);
      expect(await reader.next(1000)).toBeUndefined();
      await reader.close();
      await expect(log.openReader({ cursorId: "saved-reader", topic: "different" })).rejects.toThrow("configuration mismatch");
      expect(await log.deleteReader("saved-reader")).toBe(true);
    } finally { await log.close(); }
  }, 15_000);

  it("keeps durable stream and cursor checkpoints across a graceful server restart", async () => {
    const config = options();
    let log = await openJetStreamEventLog(config);
    const first = await log.publish({ topic: "restart", from, dedupeKey: "server-restart-key" });
    const second = await log.publish({ topic: "restart", from });
    const reader = await log.openReader({ cursorId: "server-restart-cursor" });
    expect(await reader.next(1000)).toEqual(first);
    await reader.ack(first);
    await reader.close();
    await log.close();
    await cluster.stopServer(cluster.names[0]!);
    await cluster.restart(cluster.names[0]!);
    log = await openJetStreamEventLog(config);
    try {
      expect(await log.read({ after: 0 })).toEqual([first, second]);
      expect(await log.publish({ topic: "restart", from, dedupeKey: "server-restart-key" })).toEqual(first);
      const resumed = await log.openReader({ cursorId: "server-restart-cursor" });
      expect(await resumed.next(2000)).toEqual(second);
      await resumed.ack(second);
      await resumed.close();
    } finally { await log.close(); }
  }, 40_000);

  it("retains original pending bytes/id on retry and enforces payload size before admission", async () => {
    const log = await openJetStreamEventLog(options());
    try {
      const data = { payload: "original" };
      const pending = log.prepare({ topic: "retry.prepared", from, data });
      data.payload = "mutated";
      const first = await log.publishPrepared(pending);
      expect(await log.publishPrepared(pending)).toEqual(first);
      expect(first.data).toEqual({ payload: "original" });
      await expect(log.publish({ topic: "oversize", from, text: "x".repeat(64 * 1024) })).rejects.toThrow("exceeds");
      await expect(log.publish({ topic: "bad.id", from, eventId: " padded-id " })).rejects.toThrow("Invalid Fabric event id");
      await expect(log.publish({ topic: "bad.id", from, eventId: "id\tcontrol" })).rejects.toThrow("Invalid Fabric event id");
      expect(await log.latestSequence()).toBe(1);
      await expect(log.publishBatch([{ topic: "valid", from }, { topic: "bad topic", from }])).rejects.toThrow("Invalid");
      expect(await log.latestSequence()).toBe(1);
    } finally { await log.close(); }
  });

  it("returns an acknowledged batch prefix and exact retry suffix after an ambiguous ACK loss", async () => {
    const log = await openJetStreamEventLog(options());
    const publish = log.publishPrepared.bind(log);
    const spy = vi.spyOn(log, "publishPrepared");
    let calls = 0;
    spy.mockImplementation(async pending => {
      const event = await publish(pending);
      if (++calls === 2) throw new JetStreamPublishUncertainError(pending, new Error("injected lost ACK after commit"));
      return event;
    });
    try {
      let error: unknown;
      try { await log.publishBatch([0, 1, 2].map(n => ({ topic: "batch.uncertain", from, data: n }))); }
      catch (cause) { error = cause; }
      expect(error).toBeInstanceOf(JetStreamBatchPublishError);
      const stopped = error as JetStreamBatchPublishError;
      expect(stopped.committed.map(event => event.data)).toEqual([0]);
      expect(stopped.remaining.map(event => event.data)).toEqual([1, 2]);
      expect(stopped.cause).toBeInstanceOf(JetStreamPublishUncertainError);
      spy.mockRestore();
      for (const pending of stopped.remaining) await log.publishPrepared(pending);
      const events = await log.read({ after: 0 });
      expect(events.map(event => event.data)).toEqual([0, 1, 2]);
      expect(events[1]!.id).toBe(stopped.remaining[0]!.id);
    } finally { spy.mockRestore(); await log.close(); }
  });

  it("uses limits retention without resetting sequence and reports an expired saved cursor", async () => {
    const log = await openJetStreamEventLog({ ...options(), maxBytes: 8192 });
    try {
      for (let i = 0; i < 20; i++) await log.publish({ topic: "retained", from, text: "x".repeat(1500) });
      expect(await log.latestSequence()).toBe(20);
      expect(await log.oldestSequence()).toBeGreaterThan(1);
      await expect(log.read({ after: 1 })).rejects.toBeInstanceOf(JetStreamCursorExpiredError);
      const retained = await log.read({ after: 0 });
      expect(retained.length).toBeLessThan(20);
      expect(retained.at(-1)!.sequence).toBe(20);
      await expect(log.openReader({ cursorId: "old-cursor", after: 1 })).rejects.toBeInstanceOf(JetStreamCursorExpiredError);
    } finally { await log.close(); }
  });

  it("supports explicit age retention without inventing an archive TTL default", async () => {
    const log = await openJetStreamEventLog({ ...options(), retention: "archive", maxAgeMs: 1500, duplicateWindowMs: 1000 });
    const nc = await connect({ servers: cluster.servers });
    try {
      const info = await (await nc.jetstreamManager()).streams.info(log.streamName);
      expect(info.config.max_age).toBe(1_500_000_000);
      expect(info.config.max_bytes).toBe(-1);
      await log.publish({ topic: "expires", from });
      await new Promise(resolve => setTimeout(resolve, 1800));
      expect(await log.read({ after: 0 })).toEqual([]);
      expect(await log.latestSequence()).toBe(1);
    } finally { await nc.close(); await log.close(); }
  });
});
