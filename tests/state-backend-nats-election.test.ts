import fs from "node:fs";
import path from "node:path";
import type { Consumer, ConsumerAPI, ConsumerInfo, Consumers } from "@nats-io/jetstream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { importMeshStateToNats } from "../src/mesh/backend-migration-nats.js";
import { closeNatsStateBackend, MeshStateNatsUnavailableError, natsStreamName, openNatsStateBackend, type JetStreamPort } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { selectNatsSyncAlways } from "../src/mesh/nats-monitor-config.js";
import { startNatsElectionCluster, stopNatsElectionCluster, type NatsElectionCluster } from "./helpers/nats-election-cluster.js";
import { captureNatsConsumer, waitForNatsConsumerReady, waitForNatsStreamReady } from "./helpers/nats-consumer-ready.js";

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
const waitForStopped = async (child: { pid?: number | undefined }, deadline: number): Promise<void> => {
  if (process.platform !== "linux" || child.pid === undefined) {
    throw new Error("NATS_STOP_PROOF_UNAVAILABLE: SIGSTOP stopped-state proof needs Linux /proc and an owned child PID");
  }
  while (Date.now() < deadline) {
    try {
      const stat = fs.readFileSync(`/proc/${child.pid}/stat`, "utf8");
      const state = stat.slice(stat.lastIndexOf(")") + 2, stat.lastIndexOf(")") + 3);
      if (state === "T" || state === "t") return;
    } catch { /* the owned child is still transitioning; the bounded deadline is the safety net */ }
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  throw new Error(`owned NATS child ${child.pid} did not report stopped before first NEXT`);
};
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
          routes: routeInfo.num_routes ?? 0, syncAlways: selectNatsSyncAlways(server, jsz) };
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
  // Keep: #11950/#13112's real R3-history / silent R1-receiver split, without changing CREATE or pull budgets.
  // Skip reason off Linux: SIGSTOP stopped-state proof needs Linux /proc.
  it.skipIf(process.platform !== "linux" || !binary)("bootstraps exact R3 history while the actual distinct R1 consumer host is paused before first NEXT", async () => {
    const cluster = await startNatsElectionCluster(binary!);
    clusters.push(cluster); // Existing afterEach owns native child exit/scratch receipts on every outcome.
    const [{ connect }, { jetstream, jetstreamManager }, { connectJetStream }] = await Promise.all([
      import("@nats-io/transport-node"), import("@nats-io/jetstream"), import("../src/mesh/state-backend-nats-client.js"),
    ]);
    const observer = await connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true, reconnect: false, timeout: 1_000 });
    const manager = await jetstreamManager(observer, { timeout: 1_000 });
    const nodeFor = (name: string | undefined) => cluster.nodes.find(node => node.name === name || node.metaId === name);
    const seedNode = cluster.nodes.find(node => node.clientUrl === cluster.seedUrl)!;
    const ownStreams = new Set<string>();
    const roots: string[] = [];
    const ports: JetStreamPort[] = [];
    const creates: ConsumerInfo[] = [];
    const consumed = new Map<string, Consumer>();
    const placements: unknown[] = [];
    const metrics: { records: number; gets: number; elapsedMs: number }[] = [];
    // Setup and placement have separate cumulative budgets. In particular, a healthy seed's initial NEXT may
    // recover on the SDK's 30s pull expiry without spending the 30s allowed to find the actual fault placement.
    let setupRemainingMs = 60_000;
    let placementRemainingMs = 30_000;
    const inFlight = new Set<Promise<unknown>>();
    const track = <T,>(operation: Promise<T>): Promise<T> => {
      inFlight.add(operation);
      void operation.then(() => inFlight.delete(operation), () => inFlight.delete(operation));
      return operation;
    };
    const deferred = <T,>() => {
      let resolve!: (value: T) => void;
      const promise = new Promise<T>(yes => { resolve = yes; });
      return { promise, resolve };
    };
    const bounded = async <T,>(promise: Promise<T>, ms: number, label: string): Promise<T> => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try { return await Promise.race([promise, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms}ms; underlying operation not retried`)), ms);
      })]); } finally { clearTimeout(timer); }
    };
    let paused: NatsElectionCluster["nodes"][number] | undefined;
    let pausedAt = 0;
    let resumedAt = 0;
    let resumeTimer: ReturnType<typeof setTimeout> | undefined;
    let faultStream: string | undefined;
    let faultConsumer: Consumer | undefined;
    let faultInfo: ConsumerInfo | undefined;
    let faults = 0;
    let selecting = false;
    let selectionDeadline = 0;
    const faultArmed = deferred<void>();
    const resumed = deferred<void>();
    const lateHistory = deferred<void>();
    const received: number[] = [];
    const resume = () => {
      if (!paused || resumedAt) return;
      clearTimeout(resumeTimer);
      const ok = paused.child.kill("SIGCONT"); // Only the fixture's owned direct ChildProcess, never PID/name discovery.
      resumedAt = performance.now();
      resumed.resolve();
      expect(ok, "owned R1 receiver SIGCONT").toBe(true);
    };
    const consumerPrototype = Object.getPrototypeOf(manager.consumers) as ConsumerAPI;
    const originalAdd = consumerPrototype.add;
    const addSpy = vi.spyOn(consumerPrototype, "add").mockImplementation(async function (this: ConsumerAPI, ...args: Parameters<ConsumerAPI["add"]>) {
      const ack = await originalAdd.apply(this, args);
      if (ownStreams.has(args[0])) creates.push(ack); // Actual successful CREATE ACK, no separately sampled consumer.
      return ack;
    });
    const consumersPrototype = Object.getPrototypeOf(jetstream(observer).consumers) as Consumers;
    const originalGet = consumersPrototype.get;
    const getSpy = vi.spyOn(consumersPrototype, "get").mockImplementation(async function (this: Consumers, ...args: Parameters<Consumers["get"]>) {
      const consumer = await originalGet.apply(this, args);
      if (!ownStreams.has(args[0])) return consumer;
      // Install only AFTER the real get returns: no added await inside the adapter's existing 1s bootstrap get.
      const consume = consumer.consume.bind(consumer);
      consumer.consume = async (...consumeArgs: Parameters<Consumer["consume"]>) => {
        consumed.set(args[0], consumer); // The selected get, never a late ACK from an abandoned bootstrap attempt.
        if (selecting) {
          const info = await consumer.info(false); // Uncached INFO, before original consume can issue its first NEXT.
          const stream = await manager.streams.info(args[0]);
          const ack = [...creates].reverse().find(item => item.stream_name === args[0] && item.name === info.name);
          expect(ack, "faulting open must have its own real CREATE ACK").toBeDefined();
          expect(info).toMatchObject({ stream_name: args[0], config: { num_replicas: 1, mem_storage: true, ack_policy: "none" } });
          expect(ack!.cluster?.leader).toBe(info.cluster?.leader);
          expect(stream.config.num_replicas).toBe(3);
          expect(stream.state).toMatchObject({ first_seq: 1, last_seq: 3 });
          const host = nodeFor(info.cluster?.leader);
          const streamHost = nodeFor(stream.cluster?.leader);
          expect(host, "map actual R1 consumer host to an owned child").toBeDefined();
          expect(streamHost, "map R3 stream leader").toBeDefined();
          const qualifies = host !== seedNode && host !== streamHost;
          const placement = { setup: placements.length + 1, stream: args[0], consumer: info.name, createLeader: ack!.cluster?.leader,
            consumerHost: host!.name, streamLeader: streamHost!.name, connectedSeed: seedNode.name, qualifies };
          placements.push(placement);
          console.log(`NATS_BOOTSTRAP_PLACEMENT ${JSON.stringify(placement)}`);
          if (!selecting || Date.now() >= selectionDeadline) throw new Error("placement precondition expired before first NEXT; no fault issued");
          if (qualifies) {
            expect(faults).toBe(0);
            expect(host!.child.exitCode).toBeNull();
            expect(host!.child.signalCode).toBeNull();
            paused = host;
            faultStream = args[0];
            faultConsumer = consumer;
            faultInfo = info;
            faults++;
            pausedAt = performance.now();
            resumeTimer = setTimeout(resume, 6_000); // One finite fault duration; finally also resumes on failed assertions.
            expect(host!.child.kill("SIGSTOP"), "owned R1 receiver SIGSTOP").toBe(true);
            // SIGSTOP's kill() return only queues the signal. Wait for the owned child to report state T before
            // allowing the original consume() to issue its first NEXT; this is the event under test, not a sleep.
            await waitForStopped(host!.child, Date.now() + 1_000);
            faultArmed.resolve();
          }
        }
        return consume(...consumeArgs); // Original SDK consumer/options/request; no reset, delete, or forced error.
      };
      return consumer;
    });
    const makePort = async () => {
      const port = await connectJetStream([cluster.seedUrl], { monitoringUrl: cluster.seedMonitorUrl, replicas: 3 });
      ports.push(port);
      const get = port.getMessage.bind(port);
      let gets = 0;
      port.getMessage = (...args: Parameters<JetStreamPort["getMessage"]>) => { gets++; return get(...args); };
      const consume = port.consume.bind(port);
      port.consume = async (stream, after, onMessage, onError) => {
        // Gate stream materialization before setup or fault selection, while all nodes are still running.
        // This is NOT receiver readiness: the faulting consume below must issue its first NEXT while paused.
        await waitForNatsStreamReady(cluster.seedUrl, stream);
        const subscription = await consume(stream, after, message => {
          onMessage(message);
          if (stream === faultStream && message.seq <= 3) {
            received.push(message.seq);
            if (received.length === 3) lateHistory.resolve();
          }
        }, onError);
        // Empty seed opens have no bootstrap history to wait for. Observe their real pending NEXT before the first
        // setup publish; otherwise a not-yet-routed first pull can consume the entire write budget under load.
        // The faulting reopen deliberately bypasses this readiness gate: its original receiver is stopped.
        if (stream !== faultStream) {
          const selected = await consumed.get(stream)!.info(true); // Cached real successful get identity, no extra request.
          expect(creates.some(info => info.stream_name === stream && info.name === selected.name)).toBe(true);
          await waitForNatsConsumerReady(cluster.seedUrl, stream, selected.name);
        }
        return subscription;
      };
      const close = port.close.bind(port);
      let closing: Promise<void> | undefined;
      port.close = () => closing ??= close();
      return { port, gets: () => gets };
    };
    const makeSeed = async (name: string, records: number) => {
      const root = path.join(cluster.root, name);
      fs.mkdirSync(root);
      roots.push(root);
      ownStreams.add(natsStreamName(root));
      const started = performance.now();
      if (setupRemainingMs <= 0) throw new Error(`SETUP ${name}: cumulative seed preparation budget exhausted`);
      const deadline = started + setupRemainingMs;
      let active = true;
      let port: JetStreamPort | undefined;
      const assertActive = () => {
        if (!active || performance.now() >= deadline) throw new Error(`SETUP ${name}: seed preparation budget expired`);
      };
      const preparing = (async () => {
        ({ port } = await makePort());
        assertActive();
        const backend = await openNatsStateBackend({ root, port, replicas: 3, snapshotEvery: 0 });
        assertActive();
        for (let index = 0; index < records; index++) {
          assertActive();
          // Exactly one write per seed key. An unknown outcome or a projector BUSY ends SETUP; never retry it.
          await backend.put({ key: `bootstrap/${index}`, value: { index }, identity });
        }
        assertActive();
        const head = (await manager.streams.info(natsStreamName(root))).state.last_seq;
        assertActive();
        expect(head).toBe(records);
        // put resolves after native projection. Confirm the actual server head, not just an ACK or a fixed sleep.
        expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: head, knownSeq: head });
        const expected = backend.listAll();
        console.log(`NATS_BOOTSTRAP_SEED_READY ${JSON.stringify({ name, records, head, elapsedMs: performance.now() - started })}`);
        return { root, expected };
      })();
      try { return await bounded(preparing, setupRemainingMs, `SETUP ${name}`); }
      catch (error) { throw new Error(`SETUP ${name}: ${error instanceof Error ? error.message : String(error)}`, { cause: error }); }
      finally {
        active = false;
        closeNatsStateBackend(root);
        await port?.close();
        // A deadline observes, but does not cancel, I/O. Join before final cleanup so no late open can register a
        // prepared backend or begin another seed write after this setup has returned or failed.
        await preparing.catch(() => undefined);
        closeNatsStateBackend(root);
        await port?.close();
        setupRemainingMs -= performance.now() - started;
      }
    };
    try {
      // Observations, not an invented one-GET or hard-millisecond performance SLA. Parent records BASE/candidate.
      for (const records of [3, 1_000]) {
        const { root, expected } = await makeSeed(`healthy-bootstrap-${records}`, records);
        const measured = await makePort();
        const started = performance.now();
        const backend = await openNatsStateBackend({ root, port: measured.port, replicas: 3, snapshotEvery: 0 });
        metrics.push({ records, gets: measured.gets(), elapsedMs: performance.now() - started });
        expect(backend.listAll()).toEqual(expected);
        expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: records, knownSeq: records });
        console.log(`NATS_BOOTSTRAP_HEALTHY_COST ${JSON.stringify(metrics.at(-1))}`);
        closeNatsStateBackend(root);
        await measured.port.close();
      }
      // Seed preparation is readiness-confirmed BEFORE opening this placement window. Neither its time nor a
      // failed/ambiguous seed write may be mistaken for a failed fault-placement attempt.
      for (let setup = 1; setup <= 32 && placementRemainingMs > 0; setup++) {
        const { root, expected } = await makeSeed(`fault-bootstrap-${setup}`, 3);
        const placementStarted = performance.now();
        selectionDeadline = Date.now() + placementRemainingMs;
        const remaining = () => Math.max(1, selectionDeadline - Date.now());
        const measured = await bounded(track(makePort()), remaining(), "connect placement precondition");
        selecting = true;
        const started = performance.now();
        const opening = track(openNatsStateBackend({ root, port: measured.port, replicas: 3, snapshotEvery: 0 }));
        // Selection ends at the actual pre-NEXT placement event, not at a later faulted open's completion.
        const selected = await bounded(Promise.race([
          opening.then(backend => ({ backend })), faultArmed.promise.then(() => ({ fault: true as const })),
        ]), remaining(), "reopen placement precondition");
        const backend = "backend" in selected ? selected.backend : await bounded(opening, 5_500, "faulted initial open");
        selecting = false;
        placementRemainingMs -= performance.now() - placementStarted;
        expect(backend.listAll()).toEqual(expected);
        expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 3, knownSeq: 3 });
        if (!faults) {
          closeNatsStateBackend(root);
          await measured.port.close();
          continue; // Not a failed-open retry; exact healthy state was proved before ordinary close.
        }
        expect(resumedAt, "initial open must apply exact H WHILE the original receiver host is paused").toBe(0);
        expect(performance.now() - pausedAt).toBeLessThan(5_000 + 500);
        expect(performance.now() - started).toBeLessThan(5_000 + 1_000); // CREATE/INFO overhead is not a replacement waiter budget.
        expect(received).toEqual([]);
        expect(creates.filter(info => info.stream_name === faultStream), "exactly seed + faulting reopen CREATEs").toHaveLength(2);
        await bounded(resumed.promise, 7_000, "six-second receiver resume");
        // Prove the pause outlived the original 5s waiter with 500ms margin, not an exact 6s timer duration.
        expect(resumedAt - pausedAt).toBeGreaterThan(5_000 + 500);
        // Original native onMessage callbacks drive this event; no polling framework or transport reconnect.
        await bounded(lateHistory.promise, 3_000, "original consumer late sequences 1-3");
        expect(received).toEqual([1, 2, 3]);
        expect(backend.listAll()).toEqual(expected);
        expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 3, knownSeq: 3 });
        expect(backend.diagnostics().nats?.failure).toBeUndefined();
        expect((await faultConsumer!.info(false)).name).toBe(faultInfo!.name);
        expect((await manager.consumers.info(faultStream!, faultInfo!.name)).created).toBe(faultInfo!.created);
        // SIGCONT, a leader flip and late delivery do not prove every R3 replica is current or that the
        // original consumer has registered its next pull. Observe both, under the existing recovery bound.
        await waitForNatsConsumerReady(cluster.seedUrl, faultStream!, faultInfo!.name, { timeoutMs: 15_000 });
        const entry = expected[0]!;
        // Exactly one test-level PUT. Only the product's native same-msgID retries may recover a lost ACK.
        await backend.put({ key: entry.key, ifVersion: entry.version, value: { resumed: true }, identity });
        expect(backend.get(entry.key)).toMatchObject({ value: { resumed: true }, version: entry.version + 1 });
        expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 4, knownSeq: 4 });
        expect(creates.filter(info => info.stream_name === faultStream)).toHaveLength(2);
        expect((await manager.streams.info(faultStream!)).config.num_replicas).toBe(3);
        console.log(`NATS_BOOTSTRAP_FAULT ${JSON.stringify({ setups: placements.length, faults, paused: paused!.name,
          consumer: faultInfo!.name, pausedAt, resumedAt, received, gets: measured.gets(), metrics, exactHistoryAndPostResumeCAS: true })}`);
        closeNatsStateBackend(root);
        await measured.port.close();
        break;
      }
      expect(faults, `no distinct consumer placement within 32 successful setups/30s: ${JSON.stringify(placements)}`).toBe(1);
    } finally {
      resume(); // Must precede observer/port close and the existing fixture afterEach cleanup.
      clearTimeout(resumeTimer);
      selecting = false;
      // Join timed-out connect/open operations before restoring instrumentation and taking the cleanup snapshot.
      // Every product request keeps its existing finite SDK/write budget; no open is retried by this harness.
      await Promise.allSettled([...inFlight]);
      getSpy.mockRestore();
      addSpy.mockRestore();
      for (const root of roots) closeNatsStateBackend(root);
      try { await bounded(Promise.all(ports.map(port => port.close())), 3_000, "bootstrap ports close"); }
      finally { await observer.close(); }
    }
  }, 180_000); // Covers separate 60s SETUP + 30s placement + recovery; the faulted-open budget remains 5s.

  it.skipIf(!binary)("bounds unavailable R3 placement and recovers migrations and new-root opens when the peer returns", async () => {
    const cluster = await startNatsElectionCluster(binary!);
    clusters.push(cluster);
    const root = path.join(cluster.root, "mesh-state");
    fs.mkdirSync(root);
    const options = { servers: [cluster.seedUrl], monitoringUrl: cluster.seedMonitorUrl, replicas: 3 };
    const initial = { stream: natsStreamName(root), entries: 1 };
    const faultRoots = Array.from({ length: 5 }, (_, index) => path.join(cluster.root, `fault-open-${index + 1}`));
    const migrationRoot = path.join(cluster.root, "fault-migration");
    const unavailableRoot = path.join(cluster.root, "placement-unavailable");
    const ownStreams = new Set([root, ...faultRoots, migrationRoot, unavailableRoot].map(natsStreamName));
    const [{ connect }, { jetstreamManager }] = await Promise.all([import("@nats-io/transport-node"), import("@nats-io/jetstream")]);
    const successfulCreates: { stream: string; ms: number; healthy: boolean }[] = [];
    const qualifiedOpens: string[] = [];
    const correlationErrors: string[] = [];
    let pausedNode: string | undefined;
    let observer: Awaited<ReturnType<typeof connect>> | undefined;
    let restoreCreateSpy: (() => void) | undefined;
    try {
    observer = await connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true, reconnect: false, timeout: 1_000 });
    const manager = await jetstreamManager(observer, { timeout: 1_000 });
    // The public manager exposes the same SDK prototype used by real product ordered-consumer opens.
    const consumerPrototype = Object.getPrototypeOf(manager.consumers) as ConsumerAPI;
    const originalAdd = consumerPrototype.add;
    const createSpy = vi.spyOn(consumerPrototype, "add").mockImplementation(async function (this: ConsumerAPI, ...args: Parameters<ConsumerAPI["add"]>) {
      const [stream, config] = args;
      const measured = ownStreams.has(stream) && config.ack_policy === "none" && config.num_replicas === 1
        && config.mem_storage === true && !config.durable_name && !config.deliver_subject;
      const name = config.name;
      const requestPausedNode = pausedNode;
      const started = performance.now();
      // Recording only: always execute the real API with its original receiver, arguments, errors and ACK.
      const ack = await originalAdd.apply(this, args);
      const ms = performance.now() - started;
      if (measured) {
        const leader = cluster.nodes.find(node => node.name === ack.cluster?.leader || node.metaId === ack.cluster?.leader);
        if (!name || ack.stream_name !== stream || ack.name !== name || ack.config?.name !== name
          || ack.config.num_replicas !== 1 || ack.config.mem_storage !== true || !leader || !Number.isFinite(ms) || ms < 0) {
          correlationErrors.push("real CREATE request/ACK identity, placement or timestamp mismatch");
        } else {
          // A request assigned to the paused node stays excluded even if its ACK arrives after SIGCONT.
          successfulCreates.push({ stream, ms, healthy: requestPausedNode !== leader.name });
        }
      }
      return ack;
    });
    restoreCreateSpy = () => createSpy.mockRestore();
    const { value: seedBackend, name: seedConsumer } = await captureNatsConsumer(cluster.seedUrl, initial.stream, () =>
      openNatsStateBackend({ root, ...options, maxEventBytes: 64 * 1024 }));
    qualifiedOpens.push(initial.stream);
    try {
      await waitForNatsConsumerReady(cluster.seedUrl, initial.stream, seedConsumer);
      await seedBackend.put({ key: "election/seed", value: { durable: true }, identity });
      const consumers = await manager.consumers.list(initial.stream).next();
      expect(consumers).toHaveLength(1);
      const original = consumers[0]!;
      expect(original.config.inactive_threshold).toBe(10_000_000_000);
      // consume() drives NEXT requests; INFO polling is only a bounded safety net for pull registration.
      await expect.poll(async () => (await manager.consumers.info(initial.stream, original.name)).num_waiting,
        { timeout: 2_000, interval: 100 }).toBeGreaterThan(0);
      const before = await manager.consumers.info(initial.stream, original.name);
      expect(before.num_waiting).toBeGreaterThan(0);
      const lastSequence = (await manager.streams.info(initial.stream)).state.last_seq;
      const idleStarted = performance.now();
      await sleep(15_000); // No publishes: exceed the initial consumer's 10s inactive threshold with active pulls.
      const idleMs = performance.now() - idleStarted;
      const afterIdle = await manager.consumers.info(initial.stream, original.name);
      expect(idleMs).toBeGreaterThan(10_000);
      expect(afterIdle.name).toBe(original.name);
      expect(afterIdle.created).toBe(original.created);
      expect(afterIdle.config.inactive_threshold).toBe(10_000_000_000);
      expect(afterIdle.num_waiting).toBeGreaterThan(0);
      expect(afterIdle.delivered.consumer_seq).toBe(before.delivered.consumer_seq);
      expect(afterIdle.delivered.stream_seq).toBe(before.delivered.stream_seq);
      expect((await manager.streams.info(initial.stream)).state.last_seq).toBe(lastSequence);
      // put resolves only after the real projector applies the publish; overwrite keeps entries at one.
      await seedBackend.put({ key: "election/seed", value: { durable: true, idle: true }, identity });
      expect(seedBackend.get("election/seed")?.value).toEqual({ durable: true, idle: true });
      const afterDelivery = await manager.consumers.info(initial.stream, original.name);
      const receivedAfterIdle = afterDelivery.delivered.stream_seq > afterIdle.delivered.stream_seq;
      const sameConsumer = afterDelivery.name === original.name && afterDelivery.created === original.created;
      expect(receivedAfterIdle).toBe(true);
      expect(sameConsumer).toBe(true);
      expect(afterDelivery.config.inactive_threshold).toBe(10_000_000_000);
      expect((await manager.consumers.list(initial.stream).next()).map(info => info.name)).toEqual([original.name]);
      console.log(`NATS_CONSUMER_IDLE ${JSON.stringify({ name: original.name, configuredMs: afterIdle.config.inactive_threshold! / 1_000_000,
        idleMs, receivedAfterIdle, sameConsumer, waitingBefore: before.num_waiting, waitingAfter: afterIdle.num_waiting })}`);
    } finally { closeNatsStateBackend(root); }
    const readyLeader = cluster.nodes.find(node => node.name === cluster.metaLeader || node.metaId === cluster.metaLeader);
    console.log(`NATS_ELECTION_READY ${JSON.stringify({ nodeNames: cluster.nodes.map(node => node.name), routes: 2, agreedMetaLeader: cluster.metaLeader, leaderNode: readyLeader?.name, seed: cluster.seedUrl, stream: initial.stream, preFaultSeedTransaction: "passed" })}`);

    const { connectJetStream } = await import("../src/mesh/state-backend-nats-client.js");
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
        pausedNode = paused.name;
        resumePromise = new Promise<boolean>(resolve => {
          let resumed = false;
          const resume = () => {
            if (resumed) return;
            resumed = true;
            clearTimeout(safetyTimer);
            resumedAt = Date.now();
            pausedNode = undefined;
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
        // A late CREATE or post-create leader-wait expiry may leave a stream; it must still be R3.
        if (await raw!.streamExists(natsStreamName(unavailableRoot))) {
          expect(await streamReplicas(cluster.seedUrl, natsStreamName(unavailableRoot))).toBe(3);
        }
      } else {
        expect(await streamReplicas(cluster.seedUrl, placement.value.stream)).toBe(3);
        const duringPause = await openNatsStateBackend({ root: unavailableRoot, ...migrationOptions! });
        qualifiedOpens.push(natsStreamName(unavailableRoot));
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
        await waitForNatsStreamReady(cluster.seedUrl, placement.value.stream, { timeoutMs: 10_000 });
        const caughtUp = await openNatsStateBackend({ root: unavailableRoot, servers: [leader.clientUrl], monitoringUrl: leader.monitorUrl, replicas: 3 });
        qualifiedOpens.push(natsStreamName(unavailableRoot));
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
      qualifiedOpens.push(...faultRoots.map(natsStreamName));
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
      await waitForNatsStreamReady(cluster.seedUrl, stream);
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
    expect(correlationErrors, "real CREATE call/ACK measurement must be valid").toEqual([]);
    expect(successfulCreates.filter(sample => sample.healthy).length).toBeGreaterThan(0);
    for (const stream of new Set(qualifiedOpens)) {
      expect(successfulCreates.filter(sample => sample.stream === stream).length,
        `missing real CREATE call/ACK samples for qualified opens of ${stream}`).toBeGreaterThanOrEqual(qualifiedOpens.filter(name => name === stream).length);
    }
    } finally {
      restoreCreateSpy?.();
      // close() tears down the local socket without a protocol drain. Bound even an unexpected SDK stall.
      let closeTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([observer?.close(), new Promise<never>((_, reject) => {
          closeTimer = setTimeout(() => reject(new Error("consumer observer close exceeded 1s")), 1_000);
        })]);
      } finally {
        clearTimeout(closeTimer);
        closeNatsStateBackend(root);
        const samplesMs = successfulCreates.filter(sample => sample.healthy).map(sample => sample.ms);
        const sorted = [...samplesMs].sort((a, b) => a - b);
        const percentile = (p: number) => sorted.length ? sorted[Math.ceil(sorted.length * p) - 1]! : null;
        console.log(`NATS_CONSUMER_CREATE_LATENCY ${JSON.stringify({ unit: "ms", samplesMs, count: samplesMs.length,
          p50Ms: percentile(0.5), p99Ms: percentile(0.99), maxMs: sorted.at(-1) ?? null,
          excludedPaused: successfulCreates.length - samplesMs.length, correlationErrors, qualifiedOpens: qualifiedOpens.length })}`);
      }
    }
  }, 90_000);
});
