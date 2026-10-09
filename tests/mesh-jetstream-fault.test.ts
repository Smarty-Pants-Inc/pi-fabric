import fs from "node:fs";
import path from "node:path";
import { connect } from "nats";
import { afterAll, describe, expect, it } from "vitest";
import { openJetStreamEventLog, type PendingMeshEvent } from "../src/mesh/event-backend.js";
import { natsAvailable, startNatsCluster, type LocalNatsCluster } from "./helpers/nats-cluster.js";

const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const from = { id: "test:fault", name: "fault", kind: "main" as const };
let ownedCluster: LocalNatsCluster | undefined;
// Vitest deadlines do not unwind an async test body: still reap servers on a timeout/setup error.
afterAll(async () => { await ownedCluster?.stop(); });

describe.skipIf(!natsAvailable)("JetStream R3 event-log fault", () => {
  it("kills the stream leader mid-publish, preserves every ACK and dedupes all retries", async () => {
    const cluster = ownedCluster = await startNatsCluster(3, "fault");
    const nc = await connect({ servers: cluster.servers });
    const manager = await nc.jetstreamManager();
    const log = await openJetStreamEventLog({ root: path.join(cluster.root, "mesh"), servers: cluster.servers,
      replicas: 3, maxReadEvents: 2000, duplicateWindowMs: 600_000, requestTimeoutMs: 5000 });
    const report: Record<string, unknown> = { replicas: 3, serverNames: cluster.names, syncInterval: "always" };
    try {
      const leaderBefore = (await manager.streams.info(log.streamName)).cluster!.leader!;
      console.log("FAULT_PHASE leader", leaderBefore);
      expect(cluster.names).toContain(leaderBefore);
      // This acknowledged prefix is known before the injection; retain exact retry envelopes.
      const baselinePending = Array.from({ length: 20 }, (_, n) => log.prepare({ topic: "fault.baseline", from, data: n }));
      const baseline = [];
      for (const event of baselinePending) baseline.push(await log.publishPrepared(event));
      const reader = await log.openReader({ cursorId: "fault-reader" });
      expect(await reader.next(5000)).toEqual(baseline[0]);
      await reader.ack(baseline[0]!);
      const pending = Array.from({ length: 512 }, (_, n) => log.prepare({ topic: "fault.inflight", from,
        data: { n, payload: "x".repeat(24 * 1024) } }));
      const acknowledged = [...baseline];
      let settled = 0, inflightAtKill = 0, injection = 0;
      let killed: Promise<void> | undefined;
      const attempts = pending.map(event => log.publishPrepared(event).then(result => {
        acknowledged.push(result);
        // Inject synchronously on the FIRST in-flight ACK, so at least one burst event is
        // acknowledged while other publications are still outstanding. kill() sends SIGKILL
        // before its first await; this does not wait for the entire ACK batch to settle.
        if (!killed) {
          injection = Date.now();
          inflightAtKill = pending.length - settled - 1;
          console.log("FAULT_PHASE killing after first burst ACK", inflightAtKill);
          killed = cluster.kill(leaderBefore);
        }
        return { ok: true, event: result };
      }, error => ({ ok: false, error: String(error) })).finally(() => { settled++; }));
      const outcomes = await Promise.all(attempts);
      expect(killed).toBeDefined();
      expect(inflightAtKill).toBeGreaterThan(0);
      await killed;
      console.log("FAULT_PHASE killed");
      console.log("FAULT_PHASE attempts settled", acknowledged.length);
      report.leaderBefore = leaderBefore;
      report.inflightAtKill = inflightAtKill;
      report.acknowledgedBeforeRetry = acknowledged.length;
      report.uncertainAttempts = outcomes.filter(outcome => !outcome.ok).length;
      // Retry the acknowledged baseline and every in-flight id inside the same window.
      const retry = async (event: PendingMeshEvent) => {
        for (;;) {
          try { return await log.publishPrepared(event); }
          catch (error) {
            if (Date.now() - injection > 30_000) throw error;
            await pause(100);
          }
        }
      };
      for (const [i, event] of baselinePending.entries()) expect(await retry(event)).toEqual(baseline[i]);
      expect(await reader.checkpoint()).toBe(baseline[0]!.sequence);
      expect(await reader.next(5000)).toEqual(baseline[1]);
      await reader.ack(baseline[1]!);
      await reader.close();
      report.durableCursorAfterLeaderLoss = baseline[1]!.sequence;
      const retryStarted = Date.now();
      const retries = [];
      for (const event of pending) {
        retries.push(await retry(event));
      }
      console.log("FAULT_PHASE retries done", retries.length);
      const recovered = await log.read({ after: 0, limit: 2000 });
      console.log("FAULT_PHASE read done", recovered.length);
      const ids = new Set(recovered.map(event => event.id));
      for (const event of acknowledged) expect(ids.has(event.id), `lost acknowledged id ${event.id}`).toBe(true);
      expect(ids.size).toBe(baseline.length + pending.length);
      expect(recovered).toHaveLength(ids.size);
      expect(recovered.map(event => event.sequence)).toEqual(Array.from({ length: recovered.length }, (_, i) => i + 1));
      expect(new Set(retries.map(event => event.sequence)).size).toBe(pending.length);
      const leaderAfter = (await manager.streams.info(log.streamName)).cluster!.leader;
      expect(leaderAfter).not.toBe(leaderBefore);
      report.leaderAfter = leaderAfter;
      report.retryDurationMs = Date.now() - retryStarted;
      report.recoveredEvents = recovered.length;
      report.uniqueIds = ids.size;
      report.lostAcknowledged = 0;
      report.duplicateEvents = 0;
      // Restart the killed replica and verify that all R3 members rejoin/catch up.
      console.log("FAULT_PHASE restarting");
      await cluster.restart(leaderBefore);
      console.log("FAULT_PHASE restarted");
      const deadline = Date.now() + 30_000;
      let finalInfo = await manager.streams.info(log.streamName);
      while (!finalInfo.cluster?.replicas?.every(replica => replica.current) && Date.now() < deadline) {
        await pause(100);
        finalInfo = await manager.streams.info(log.streamName);
      }
      expect(finalInfo.cluster!.replicas).toHaveLength(2);
      expect(finalInfo.cluster!.replicas!.every(replica => replica.current)).toBe(true);
      report.cluster = finalInfo.cluster;
      report.result = "PASS";
      console.log(`R3_FAULT_RESULT ${JSON.stringify(report)}`);
    } catch (error) { report.result = "FAIL"; report.error = String(error); throw error; }
    finally {
      if (process.env.TASK_OUT) fs.writeFileSync(path.join(process.env.TASK_OUT, "fault-result.json"), JSON.stringify(report, null, 2) + "\n");
      await log.close(); await nc.close(); await cluster.stop();
    }
  }, 100_000);
});
