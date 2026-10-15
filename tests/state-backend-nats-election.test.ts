import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { importMeshStateToNats } from "../src/mesh/backend-migration-nats.js";
import { closeNatsStateBackend, MeshStateNatsUnavailableError, natsStreamName, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { startNatsElectionCluster, stopNatsElectionCluster, type NatsElectionCluster } from "./helpers/nats-election-cluster.js";

const binary = process.env.NATS_BIN?.trim();
const identity: MeshIdentity = { id: "election-test", name: "election-test", kind: "agent" };
const clusters: NatsElectionCluster[] = [];
afterEach(async () => {
  for (const cluster of clusters.splice(0)) {
    await stopNatsElectionCluster(cluster);
    console.log(`NATS_ELECTION_CLEANUP ${JSON.stringify(cluster.cleanupReceipt())}`);
  }
});
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const streamInfo = async (seed: string, stream: string) => {
  const [{ connect }, { jetstreamManager }] = await Promise.all([import("@nats-io/transport-node"), import("@nats-io/jetstream")]);
  const nc = await connect({ servers: [seed], ignoreClusterUpdates: true });
  try { return await (await jetstreamManager(nc)).streams.info(stream); }
  finally { await nc.drain(); }
};
const streamReplicas = async (seed: string, stream: string): Promise<number> => (await streamInfo(seed, stream)).config.num_replicas;
const getMonitorJson = async (url: string): Promise<Record<string, any>> => {
  const response = await fetch(url, { signal: AbortSignal.timeout(500) });
  if (!response.ok) throw new Error(`${url} returned HTTP ${response.status}`);
  return await response.json() as Record<string, any>;
};
const observePeerElection = async (cluster: NatsElectionCluster, paused: string, deadline: number) => {
  let last: unknown = "no peer status yet";
  while (Date.now() < deadline) {
    try {
      const states = await Promise.all(cluster.nodes.filter(node => node.name !== paused).map(async node => {
        const [health, routeInfo, server, jsz] = await Promise.all([
          fetch(`${node.monitorUrl}/healthz?js-enabled-only=true`, { signal: AbortSignal.timeout(500) }),
          getMonitorJson(`${node.monitorUrl}/routez`), getMonitorJson(`${node.monitorUrl}/varz`),
          getMonitorJson(`${node.monitorUrl}/jsz?raft=true`),
        ]);
        return { node: node.name, id: server.server_id, leader: jsz.meta_cluster?.leader, health: health.status,
          routes: routeInfo.num_routes ?? 0, syncAlways: server.jetstream?.config?.sync_always === true };
      }));
      const leaders = new Set(states.map(state => state.leader));
      const leader = states.length === 2 && leaders.size === 1 ? states[0]!.leader : undefined;
      const leaderNode = states.find(state => state.node === leader || state.id === leader);
      if (typeof leader === "string" && leaderNode && leaderNode.node !== paused
        && states.every(state => state.health === 200 && state.routes >= 1 && state.syncAlways)) return states;
      last = states;
    } catch (error) { last = error instanceof Error ? error.message : String(error); }
    await sleep(100);
  }
  throw new Error(`healthy peers did not agree on a new metadata leader before SIGCONT: ${JSON.stringify(last)}`);
};
describe("real three-node NATS metadata-leader election", () => {
  it.skipIf(!binary)("bounds unavailable R3 placement and recovers migrations and new-root opens when the peer returns", async () => {
    const cluster = await startNatsElectionCluster(binary!);
    clusters.push(cluster);
    const root = path.join(cluster.root, "mesh-state");
    fs.mkdirSync(root);
    const options = { servers: [cluster.seedUrl], monitoringUrl: cluster.seedMonitorUrl, replicas: 3 };
    const seedBackend = await openNatsStateBackend({ root, ...options, maxEventBytes: 64 * 1024 });
    try { await seedBackend.put({ key: "election/seed", value: { durable: true }, identity }); }
    finally { closeNatsStateBackend(root); }
    const initial = { stream: natsStreamName(root), entries: 1 };
    const readyLeader = cluster.nodes.find(node => node.name === cluster.metaLeader || node.metaId === cluster.metaLeader);
    console.log(`NATS_ELECTION_READY ${JSON.stringify({ nodeNames: cluster.nodes.map(node => node.name), routes: 2, agreedMetaLeader: cluster.metaLeader, leaderNode: readyLeader?.name, seed: cluster.seedUrl, stream: initial.stream, preFaultSeedTransaction: "passed" })}`);

    const { connectJetStream } = await import("../src/mesh/state-backend-nats-client.js");
    const faultRoots = Array.from({ length: 5 }, (_, index) => path.join(cluster.root, `fault-open-${index + 1}`));
    const migrationRoot = path.join(cluster.root, "fault-migration");
    const unavailableRoot = path.join(cluster.root, "placement-unavailable");
    const unavailableStore = new MeshStore(unavailableRoot, 64 * 1024, 1_000, { stateBackend: "file" });
    try { await unavailableStore.put({ key: "election/unavailable", value: { durable: true }, identity }); }
    finally { unavailableStore.closeState(); }
    const migrationStore = new MeshStore(migrationRoot, 64 * 1024, 1_000, { stateBackend: "file" });
    try { await migrationStore.put({ key: "election/fault-migration", value: { durable: true }, identity }); }
    finally { migrationStore.closeState(); }
    type RawClient = Awaited<ReturnType<typeof connectJetStream>>;
    let raw: RawClient | undefined;
    let sequence = 0;
    let migrationOptions: { servers: string[]; monitoringUrl: string; replicas: number } | undefined;
    type MigrationResult = Awaited<ReturnType<typeof importMeshStateToNats>>;
    let directReads: Promise<PromiseSettledResult<unknown>[]> | undefined;
    let migrationsPromise: Promise<PromiseSettledResult<MigrationResult>[]> | undefined;
    let opensPromise: Promise<PromiseSettledResult<unknown>[]> | undefined;
    let resumePromise: Promise<boolean> | undefined;
    let resumeNow: (() => void) | undefined;
    let pausedAt = 0;
    let resumedAt = 0;
    let leader: NatsElectionCluster["nodes"][number] | undefined;
    try {
      for (const rootPath of faultRoots) fs.mkdirSync(rootPath);
      leader = await cluster.pauseMetaLeader(paused => {
        leader = paused;
        pausedAt = Date.now();
        resumePromise = new Promise<boolean>(resolve => {
          let resumed = false;
          const resume = () => {
            if (resumed) return;
            resumed = true;
            clearTimeout(safetyTimer);
            resumedAt = Date.now();
            resolve(paused.child.kill("SIGCONT"));
          };
          const safetyTimer = setTimeout(resume, 70_000);
          resumeNow = resume;
        });
        migrationOptions = { servers: [cluster.seedUrl], monitoringUrl: cluster.seedMonitorUrl, replicas: 3 };
        directReads = Promise.allSettled([
          raw!.streamExists(initial.stream), raw!.lastSequence(initial.stream), raw!.getMessage(initial.stream, sequence),
        ]);
      }, async survivingSeed => {
        raw = await connectJetStream([survivingSeed], { monitoringUrl: cluster.seedMonitorUrl, replicas: 3 });
        sequence = await raw.lastSequence(initial.stream);
      });
      expect(cluster.seedUrl).not.toBe(leader.clientUrl);
      let peerStates: Awaited<ReturnType<typeof observePeerElection>> | undefined;
      let peerObservationError: string | undefined;
      try { peerStates = await observePeerElection(cluster, leader.name, pausedAt + 14_500); }
      catch (error) { peerObservationError = error instanceof Error ? error.message : String(error); }
      expect(peerStates, peerObservationError ?? "healthy peers did not elect a different metadata leader").toBeDefined();
      // NATS 2.14.7 selectPeerGroup intentionally allows R3 on two live peers when the third's config/stats
      // are known. Missing peer metadata instead returns 10005. Both are valid; neither may degrade to R1.
      // See upstream jetstream_cluster.go:8193 and :8313-8325 and TestJetStreamClusterCreateR3StreamWithOfflineNodes.
      const placementTimeoutMs = 30_000;
      const startedAt = Date.now();
      const placement = await importMeshStateToNats(unavailableRoot, { ...migrationOptions!, placementTimeoutMs }).then(
        value => ({ kind: "success-r3" as const, value }), error => ({ kind: "unavailable" as const, error }));
      const elapsedMs = Date.now() - startedAt;
      console.log(`NATS_PLACEMENT_OUTCOME ${JSON.stringify({ branch: placement.kind, placementTimeoutMs, elapsedMs })}`);
      expect(elapsedMs).toBeLessThan(placementTimeoutMs + 2_000);
      expect(resumedAt, "placement must settle before the safety SIGCONT").toBe(0);
      if (placement.kind === "unavailable") {
        expect(placement.error).toBeInstanceOf(MeshStateNatsUnavailableError);
        expect(placement.error).toMatchObject({ code: "FABRIC_MESH_STATE_NATS_UNAVAILABLE", reason: "placement-unavailable", natsErrCode: 10005 });
        expect(elapsedMs).toBeGreaterThanOrEqual(placementTimeoutMs);
        expect(await raw!.streamExists(natsStreamName(unavailableRoot))).toBe(false);
      } else {
        expect(await streamReplicas(cluster.seedUrl, placement.value.stream)).toBe(3);
        const duringPause = await openNatsStateBackend({ root: unavailableRoot, ...migrationOptions! });
        try { await duringPause.put({ key: "election/during-pause", value: { durable: true }, identity }); }
        finally { closeNatsStateBackend(unavailableRoot); }
        expect(resumedAt, "the R3 write must commit before SIGCONT").toBe(0);
      }
      // Counterexample: a normal peer restart within the default 30s budget must recover at R3.
      migrationsPromise = Promise.allSettled([importMeshStateToNats(migrationRoot, migrationOptions!)]);
      opensPromise = Promise.allSettled(faultRoots.map(rootPath => openNatsStateBackend({ root: rootPath, ...migrationOptions })));
      await sleep(250);
      resumeNow?.();
      const resumed = await resumePromise!;
      const [readResults, migrations, openResults] = await Promise.all([directReads!, migrationsPromise!, opensPromise!]);
      if (placement.kind === "success-r3") {
        // Observe the actual resumed stream replica becoming current, then reopen through that node and read
        // the write committed during its pause. Merely seeing num_replicas:3 on the healthy seed is not enough.
        await expect.poll(async () => {
          const info = await streamInfo(cluster.seedUrl, placement.value.stream);
          return info.cluster?.leader === leader!.name || info.cluster?.replicas?.some(peer => peer.name === leader!.name && peer.current) === true;
        }, { timeout: 10_000, interval: 100 }).toBe(true);
        const caughtUp = await openNatsStateBackend({ root: unavailableRoot, servers: [leader.clientUrl], monitoringUrl: leader.monitorUrl, replicas: 3 });
        try { expect(caughtUp.get("election/during-pause")?.value).toEqual({ durable: true }); }
        finally { closeNatsStateBackend(unavailableRoot); }
      }
      const details = (results: PromiseSettledResult<unknown>[]) => results.map(result => result.status === "rejected"
        ? (result.reason instanceof Error ? result.reason.stack ?? result.reason.message : String(result.reason)) : "fulfilled");
      expect(resumed, `timer failed to SIGCONT paused leader ${leader.name}`).toBe(true);
      expect(peerStates, peerObservationError ?? "healthy peers did not elect a different metadata leader").toBeDefined();
      expect(migrationOptions!.servers).toEqual([cluster.seedUrl]);
      expect(readResults.map(result => result.status), `all direct reads must work during the bounded election: ${JSON.stringify(details(readResults))}`).toEqual(["fulfilled", "fulfilled", "fulfilled"]);
      expect(migrations.map(result => result.status), `new-root R3 migration must recover when placement returns within budget: ${JSON.stringify(details(migrations))}`).toEqual(["fulfilled"]);
      const fresh = migrations[0]!.status === "fulfilled" ? migrations[0]!.value : undefined;
      expect(fresh, `new-root migration failed: ${JSON.stringify(migrations[0])}`).toMatchObject({ stream: natsStreamName(migrationRoot), entries: 1 });
      const replicas = await streamReplicas(cluster.seedUrl, fresh!.stream);
      expect(replicas).toBe(3);
      expect(await Promise.all(faultRoots.map(rootPath => streamReplicas(cluster.seedUrl, natsStreamName(rootPath))))).toEqual([3, 3, 3, 3, 3]);
      expect(openResults.map(result => result.status), `all five new-root backend opens must work during the bounded election: ${JSON.stringify(details(openResults))}`).toEqual(["fulfilled", "fulfilled", "fulfilled", "fulfilled", "fulfilled"]);
      const freshConverged = await importMeshStateToNats(migrationRoot, migrationOptions!);
      expect(freshConverged).toMatchObject({ stream: natsStreamName(migrationRoot), entries: 1, converged: true });
      const recoveryReads = await Promise.all([
        raw!.streamExists(initial.stream), raw!.lastSequence(initial.stream), raw!.getMessage(initial.stream, sequence),
      ]);
      expect(recoveryReads[0]).toBe(true);
      expect(recoveryReads[1]).toBeGreaterThanOrEqual(sequence);
      expect(recoveryReads[2]).toBeDefined();
      expect(await importMeshStateToNats(migrationRoot, migrationOptions!)).toMatchObject({ stream: natsStreamName(migrationRoot), entries: 1, converged: true });
      console.log(`NATS_ELECTION_FAULT ${JSON.stringify({ pausedLeader: leader.name, seedSurvived: cluster.seedUrl !== leader.clientUrl, stoppedLeaderMs: resumedAt - pausedAt, differentLeaderObserved: peerStates, directReadsDuringPause: readResults.map(result => result.status), placementOutcome: { branch: placement.kind, placementTimeoutMs, elapsedMs, preResumeWriteCaughtUp: placement.kind === "success-r3" ? true : "not-applicable" }, recoveredMigration: { stream: fresh!.stream, entries: fresh!.entries, replicas }, newRootConvergence: freshConverged.converged, newRootOpensDuringPause: openResults.map(result => result.status), recoveryReads: "passed", recoveryMigrations: "passed" })}`);
    } finally {
      resumeNow?.();
      if (resumePromise) await resumePromise;
      await raw?.close();
      for (const rootPath of faultRoots) closeNatsStateBackend(rootPath);
      closeNatsStateBackend(root);
      closeNatsStateBackend(migrationRoot);
      closeNatsStateBackend(unavailableRoot);
    }

    // Both competing real JetStream publishes use the same expected last sequence. Exactly one may commit.
    // The final stream is no longer consumed below; this deliberately tests the CAS gate at the server.
    const casRaw = await connectJetStream([cluster.seedUrl], { monitoringUrl: cluster.seedMonitorUrl, replicas: 3 });
    try {
      const stream = initial.stream;
      const seq = await casRaw.lastSequence(stream);
      const subject = `${stream}.state`;
      const payload = Buffer.from(JSON.stringify({ cas: "election-counterexample" }));
      const results = await Promise.allSettled([
        casRaw.publish(subject, payload, { expectLastSequence: seq, msgID: "election-cas-a" }),
        casRaw.publish(subject, payload, { expectLastSequence: seq, msgID: "election-cas-b" }),
      ]);
      expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
      expect(results.filter(result => result.status === "rejected")).toHaveLength(1);
      expect(await casRaw.lastSequence(stream)).toBe(seq + 1);
      console.log(`NATS_ELECTION_CAS ${JSON.stringify({ startSequence: seq, fulfilled: 1, rejected: 1, finalSequence: seq + 1 })}`);
    } finally { await casRaw.close(); }
  }, 90_000);
});
