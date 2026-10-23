import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { expect, it } from "vitest";
import type { DatabaseSync } from "node:sqlite";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Keep reason: #13645, real SQLite writer contention across OS processes at the FILE/SQLite
// bridge/control-plane boundary. No fabricated busy errors, held transactions, or CPU spins.
// Explicit opt-in performance probe: ordinary CI skips its 44 OS children. A skip is NOT
// measurement proof; run with PI_TEST_FLEET_CONTENTION=1 on the comparison host.
// PI_TEST_DISK_IO=1 uses an actual SQLite agent store, 40 ACKs and a fixed 60 s disk
// window. The ordinary 8-ACK fixture deliberately keeps its original FILE agent store.
// PI_TEST_ survives the existing test isolation's intentional PI_FABRIC_ selector scrub.
// Disk instrumentation wraps actual native BEGIN/COMMIT/ROLLBACK, not callback lockStats.
const DISK_IO = process.env.PI_TEST_DISK_IO === "1";
const WRITERS = 40, OTHER_LINKS = 4, SAMPLES = DISK_IO ? 40 : 8, SAMPLE_INTERVAL_MS = 1_500;
const HEARTBEAT_MS = 5_000, LEASE_MS = 15_000, MEASURE_MIN_MS = DISK_IO ? 60_000 : 12_000;
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, Math.max(0, ms)));
const bounded = async <T>(work: Promise<T>, ms: number, what: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${what} exceeded ${ms} ms`)), ms);
    })]);
  } finally { if (timer) clearTimeout(timer); }
};
const round = (value: number) => +value.toFixed(2);
const summary = (values: number[]) => {
  const sorted = values.slice().sort((a, b) => a - b);
  const percentile = (q: number) => round(sorted[Math.ceil((sorted.length - 1) * q)] ?? 0);
  return { n: sorted.length, p50Ms: percentile(.5), p95Ms: percentile(.95), maxMs: percentile(1) };
};
interface SqliteHoldStart {
  database: string; connectionId: number; beginAtWall: number; beginAtMonotonicMs: number;
}
interface SqliteHoldSample extends SqliteHoldStart {
  endAtWall: number; endAtMonotonicMs: number; holdMs: number; outcome: "COMMIT" | "ROLLBACK";
}
const sqliteHoldRecorder = (agentRoot: string) => {
  const sqlite = process.getBuiltinModule("node:sqlite");
  if (typeof sqlite?.DatabaseSync?.prototype.location !== "function") {
    throw new Error("disk mode requires native DatabaseSync.location for exact agent DB attribution");
  }
  const prototype = sqlite.DatabaseSync.prototype, original = prototype.exec;
  const database = path.join(agentRoot, "state.db"), rows: SqliteHoldSample[] = [];
  const active = new Map<DatabaseSync, SqliteHoldStart>(), connections = new WeakMap<DatabaseSync, number>();
  const excludedDatabases = new Set<string>(), errors: string[] = [];
  let nextConnection = 0;
  const installedAtWall = Date.now(), installedAtMonotonicMs = performance.now();
  prototype.exec = function(sql) {
    const statement = sql.trim().replace(/;$/, "").trim().toUpperCase();
    if (statement === "BEGIN IMMEDIATE") {
      // Native main-file identity BEFORE BEGIN: never time the acquisition attempt or
      // confuse this worker's SQLite hub connection with its actual far/agent DB.
      const location = this.location("main");
      if (location !== database) {
        excludedDatabases.add(location ?? "<memory>");
        return original.call(this, sql);
      }
      const result = original.call(this, sql); // A failed/busy acquisition produces NO hold.
      const beginAtMonotonicMs = performance.now(), beginAtWall = Date.now();
      if (active.has(this)) errors.push("nested agent BEGIN IMMEDIATE");
      let connectionId = connections.get(this);
      if (connectionId === undefined) { connectionId = ++nextConnection; connections.set(this, connectionId); }
      active.set(this, { database: location, connectionId, beginAtWall, beginAtMonotonicMs });
      return result;
    }
    const began = active.get(this);
    if (!began || (statement !== "COMMIT" && statement !== "ROLLBACK")) return original.call(this, sql);
    // Failed COMMIT can leave a writer transaction alive: keep its original BEGIN
    // until actual ROLLBACK, and fail the measurement rather than truncate its hold.
    let succeeded = false;
    try { const result = original.call(this, sql); succeeded = true; return result; }
    finally {
      const endAtMonotonicMs = performance.now(), endAtWall = Date.now();
      if (!succeeded) errors.push(`agent ${statement} threw`);
      if (!this.isTransaction) {
        active.delete(this);
        rows.push({ ...began, endAtWall, endAtMonotonicMs,
          holdMs: endAtMonotonicMs - began.beginAtMonotonicMs, outcome: statement });
      } else if (succeeded) errors.push(`agent ${statement} returned with transaction still active`);
    }
  };
  return {
    restore: () => { prototype.exec = original; },
    assertOutsideTransaction: () => {
      if (active.size) throw new Error("disk window must start outside agent SQLite transactions");
    },
    snapshot: (windowStartWall: number, windowStartMonotonicMs: number) => {
      const collectedThroughWall = Date.now(), collectedThroughMonotonicMs = performance.now();
      const windowEndMonotonicMs = windowStartMonotonicMs + MEASURE_MIN_MS;
      if (errors.length || active.size) throw new Error(`invalid/censored actual agent SQLite holds: ${errors}; open=${active.size}`);
      if (rows.some(row => ![row.beginAtWall, row.beginAtMonotonicMs, row.endAtWall,
        row.endAtMonotonicMs, row.holdMs].every(Number.isFinite) || row.holdMs < 0)) {
        throw new Error("nonfinite/invalid actual agent SQLite hold");
      }
      // Exact monotonic overlap, FULL durations including end-crossing transactions.
      // No guessed FILE endpoints, callback timestamps, acquisition wait, or clipping.
      const measured = rows.filter(row => row.beginAtMonotonicMs < windowEndMonotonicMs
        && row.endAtMonotonicMs > windowStartMonotonicMs);
      if (!measured.length) throw new Error("no actual agent SQLite writer holds in disk window");
      return { schema: 1, root: agentRoot, database, backend: "sqlite", pid: process.pid,
        installedAtWall, installedAtMonotonicMs, collectedThroughWall, collectedThroughMonotonicMs,
        installedBeforeStoreConstruction: true, attribution: "native DatabaseSync.location('main') exact path",
        hook: "node:sqlite DatabaseSync.prototype.exec; original receiver/SQL exactly once",
        holdDefinition: "successful BEGIN IMMEDIATE return through completed COMMIT/ROLLBACK return; excludes acquisition wait; includes terminal exec; traced user-space boundaries, not kernel lock-release instants",
        acquisitionWaitIncluded: false, perClass: null,
        perClassLimitation: "native exec exposes transaction boundaries, not MeshLockClass; no class inferred",
        windowStartWall, windowEndWall: windowStartWall + MEASURE_MIN_MS,
        windowStartMonotonicMs, windowEndMonotonicMs, windowMs: MEASURE_MIN_MS,
        selection: "transaction monotonic interval overlaps [windowStartMonotonicMs,windowEndMonotonicMs)",
        boundaryPolicy: "full actual durations for every overlapping transaction, never clipped",
        observedTransactionCount: rows.length,
        beforeWindowTransactions: rows.filter(row => row.endAtMonotonicMs <= windowStartMonotonicMs).length,
        afterWindowTransactions: rows.filter(row => row.beginAtMonotonicMs >= windowEndMonotonicMs).length,
        boundaryCrossingCount: measured.filter(row => row.beginAtMonotonicMs < windowStartMonotonicMs
          || row.endAtMonotonicMs > windowEndMonotonicMs).length,
        openTransactionCount: active.size, instrumentationErrors: errors, excludedDatabases: [...excludedDatabases].sort(),
        sampleCount: measured.length, maxHoldMs: Math.max(...measured.map(row => row.holdMs)),
        totalHoldMs: measured.reduce((sum, row) => sum + row.holdMs, 0), samples: measured };
    },
  };
};
const cliSide = (store: MeshStore, peer: string): StoreBridgeSide => {
  // Same test on pinned main: its four-argument constructor ignores candidate CLI's fifth arg.
  const Side = StoreBridgeSide as new (store: MeshStore, peer: string, now: () => number,
    monitorLease: boolean, deferLeaseInstalls?: boolean) => StoreBridgeSide;
  return new Side(store, peer, Date.now, true, true);
};
const native = async (store: MeshStore, name: string) => {
  const now = Date.now(), id = `session:${name}-00000000`, ownerIncarnation = `fixture:${id}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: { format: 1, id, rootId: id,
    identity, incarnationToken: name, startedAt: now, updatedAt: now, expiresAt: now + 180_000 } });
  writeParticipantFile(store.root, { key: `topology/participants/${hash(id)}`, version: 1,
    updatedAt: now, updatedBy: identity, value: { format: 1, id, kind: "root", rootId: id,
      ownerHostId: id, ownerIdentityId: id, ownerIncarnation, name, label: name, status: "idle",
      runner: "pi", transport: "host", capabilities: ["followUp"], sessionId: name,
      startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return { id, identity, ownerIncarnation };
};

// One real ParticipantDirectory per OS writer. Four additional processes run actual bridge
// links with their own native FILE-spoke directory, stdio protocol, mirror leases and inbound
// work events. Every directory uses the production 5 s heartbeat / 15 s lease. Alternating
// busy/idle presence models an active Pi; it is not an unrelated synthetic SQLite UPDATE.
const CHILD = String.raw`
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";
import { PassThrough } from "node:stream";
import path from "node:path";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const { ParticipantDirectory } = await jiti.import("./src/topology/participant-directory.ts");
const { readHostLease } = await jiti.import("./src/topology/host-leases.ts");
const [mode, root, index, targetId] = process.argv.slice(1);
const peer = "spoke-" + index, id = "session:contention-" + mode + "-" + index;
const identity = { id, name: "main", kind: "main", sessionId: id.slice(8) };
const emit = value => process.stdout.write(JSON.stringify({ pid: process.pid, mode, index, ...value }) + "\n");
const mesh = new MeshStore(mode === "writer" ? root : path.join(root, peer), 65536, 1000,
  { stateBackend: mode === "writer" ? "sqlite" : "file", lockTimeoutMs: 5000 });
let measuring = false, activeMirror = false, hubMirrorCommits = 0, mirrorCalls = 0;
let lastMirroredAt = 0, mirrorRenewals = 0, refreshWork = Promise.resolve();
const holds = [], opened = new WeakMap();
const sqlite = process.getBuiltinModule("node:sqlite");
const originalExec = sqlite.DatabaseSync.prototype.exec;
// Also restore on startup failure/watchdog/explicit exit; this is private to this child.
process.once("exit", () => { sqlite.DatabaseSync.prototype.exec = originalExec; });
sqlite.DatabaseSync.prototype.exec = function(sql) {
  const statement = sql.trim().toUpperCase();
  if (statement === "BEGIN IMMEDIATE") {
    const result = originalExec.call(this, sql);
    opened.set(this, { at: performance.now(), measured: measuring, mirrored: activeMirror });
    return result;
  }
  const active = opened.get(this);
  try { return originalExec.call(this, sql); }
  finally {
    if (active && (statement === "COMMIT" || statement === "ROLLBACK")) {
      opened.delete(this);
      if (active.measured) holds.push(performance.now() - active.at);
      if (active.measured && active.mirrored && statement === "COMMIT") hubMirrorCommits++;
    }
  }
};
const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity,
  heartbeatMs: 5000, leaseMs: 15000, reapDeadHosts: false });
const startedAt = Date.now();
directory.registerSource(() => [{ format: 1, id, kind: "root", rootId: id,
  ownerHostId: id, ownerIdentityId: id, ownerIncarnation: directory.ownerIncarnation,
  name: "main", label: "contention " + mode + " " + index,
  status: (Math.floor((Date.now() - startedAt) / 5000) % 2) ? "busy" : "idle", runner: "pi", transport: "host",
  capabilities: ["followUp"], sessionId: identity.sessionId, startedAt,
  updatedAt: Date.now(), controlProtocol: "v1" }]);
const refresh = directory.refresh.bind(directory);
directory.refresh = () => {
  const observed = measuring, began = Date.now();
  const work = (async () => {
    try {
      await refresh();
      const lease = readHostLease(mesh.root, id);
      if (observed) emit({ type: "heartbeat", ok: true, began, ended: Date.now(),
        updatedAt: lease?.updatedAt ?? 0, expiresAt: lease?.expiresAt ?? 0 });
      if (mode === "link" && observed) {
        const event = await mesh.publish({ topic: "fleet.work.contention", kind: "ask",
          from: identity, to: targetId, text: peer + "-" + began });
        emit({ type: "traffic", id: event.id });
      }
    } catch (error) {
      if (observed) emit({ type: "heartbeat", ok: false, began, ended: Date.now(), error: String(error) });
      throw error;
    }
  })();
  refreshWork = work.catch(() => undefined);
  return work;
};
let hub, bridge, local, agent, remote, input, output, serving, running, runFailure;
await directory.start();
if (mode === "link") {
  const { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } = await jiti.import("./src/mesh/bridge.ts");
  hub = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite", lockTimeoutMs: 5000 });
  local = new StoreBridgeSide(hub, peer, Date.now, true, true);
  agent = new StoreBridgeSide(mesh, "hub", Date.now, true, true);
  const mirror = local.mirror.bind(local);
  local.mirror = async (...args) => {
    activeMirror = true;
    try {
      await mirror(...args);
      if (measuring) {
        mirrorCalls++;
        const lease = readHostLease(root, id);
        if (lease?.mirror?.remoteHost === peer && lease.updatedAt > lastMirroredAt) {
          lastMirroredAt = lease.updatedAt;
          mirrorRenewals++;
        }
      }
    } finally { activeMirror = false; }
  };
  input = new PassThrough(); output = new PassThrough();
  serving = serveBridgeAgent(agent, input, output);
  remote = new RemoteBridgeSide(output, input, 15000);
  bridge = new MeshBridge({ localName: "hub", remoteName: peer, local, remote,
    cursorPath: path.join(root, peer + "-cursor.json"), stopMs: 500 });
  await bridge.start();
  running = bridge.run().catch(error => { runFailure = String(error); emit({ type: "failure", error: runFailure }); });
}
const lease = readHostLease(mesh.root, id);
emit({ type: "ready", id, updatedAt: lease?.updatedAt ?? 0, expiresAt: lease?.expiresAt ?? 0 });
const inputLines = createInterface({ input: process.stdin });
const watchdog = setTimeout(() => { emit({ type: "watchdog" }); process.exit(2); },
  process.env.PI_TEST_DISK_IO === "1" ? 300000 : 150000);
await new Promise(resolve => {
  inputLines.once("close", resolve);
  inputLines.on("line", line => {
    if (line === "measure") {
      lastMirroredAt = mode === "link" ? readHostLease(root, id)?.updatedAt ?? 0 : 0;
      measuring = true;
      emit({ type: "measuring", at: Date.now() });
    }
    if (line === "snapshot") {
      directory.suspend();
      void refreshWork.then(() => {
        const native = readHostLease(mesh.root, id);
        const mirrored = mode === "link" ? readHostLease(root, id) : undefined;
        emit({ type: "snapshot", at: Date.now(), native, mirrored,
          mirrorCalls, mirrorRenewals, hubMirrorCommits, holds, runFailure });
      });
    }
    if (line === "stop") resolve();
  });
});
measuring = false;
inputLines.close();
await refreshWork;
await directory.close();
if (bridge) {
  await bridge.stop(); await running; remote.close(); input.end(); await serving; output.end();
  await agent.fence();
  await Promise.all([local.flushLeaseEffects?.(), agent.flushLeaseEffects?.()]);
  hub.closeState();
}
mesh.closeState();
clearTimeout(watchdog);
emit({ type: "closed", runFailure });
process.exit(runFailure ? 1 : 0);
`;

interface Message {
  type: string; pid: number; id?: string; ok?: boolean; began?: number; ended?: number;
  updatedAt?: number; expiresAt?: number; at?: number; error?: string; runFailure?: string;
  native?: { updatedAt: number; expiresAt: number };
  mirrored?: { updatedAt: number; expiresAt: number; mirror?: unknown };
  mirrorCalls?: number; mirrorRenewals?: number; hubMirrorCommits?: number; holds?: number[];
}
interface Worker {
  process: ChildProcessWithoutNullStreams; mode: string; index: number; messages: Message[];
  stderr: string; ready: Promise<Message>; done: Promise<{ pid: number; code: number | null; signal: NodeJS.Signals | null }>;
}
const worker = (mode: string, root: string, index: number, targetId: string): Worker => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, mode, root, String(index), targetId],
    { cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"] });
  let resolveReady!: (message: Message) => void, rejectReady!: (error: Error) => void;
  const ready = new Promise<Message>((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  // A startup rejection is still awaited by Promise.all, including a later worker's early exit.
  void ready.catch(() => undefined);
  const result: Worker = { process: child, mode, index, messages: [], stderr: "", ready,
    done: new Promise(resolve => child.once("close", (code, signal) => {
      rejectReady(new Error(`${mode} ${index} exited ${code ?? signal}: ${result.stderr}`));
      resolve({ pid: child.pid!, code, signal });
    })) };
  let buffered = "";
  child.stdout.setEncoding("utf8"); child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    buffered += chunk;
    for (let newline; (newline = buffered.indexOf("\n")) >= 0;) {
      const line = buffered.slice(0, newline); buffered = buffered.slice(newline + 1);
      if (!line.trim()) continue;
      try {
        const message = JSON.parse(line) as Message;
        result.messages.push(message);
        if (message.type === "ready") resolveReady(message);
      } catch (error) { rejectReady(new Error(`invalid child output: ${line}; ${error}`)); }
    }
  });
  child.stderr.on("data", (chunk: string) => { result.stderr += chunk; });
  child.once("error", rejectReady);
  child.stdin.on("error", error => rejectReady(error));
  return result;
};
const alive = (pid: number): boolean => { try { process.kill(pid, 0); return true; } catch { return false; } };
interface Sample {
  index: number; requestAt: number; commandId?: string; acceptedAt?: number; agentAckAt?: number;
  hubAckAt?: number; observedAt?: number; agentAck?: MeshEvent; hubAck?: MeshEvent;
  receipt: Promise<void>; received: () => void;
}

it.runIf(process.env.PI_TEST_FLEET_CONTENTION === "1" || DISK_IO)("measures followUp ACK returns with 40 native SQLite writers and four inbound FILE-spoke bridges (explicit opt-in)", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fleet-contention-ack-"));
  // Install before either store: classify the ACTUAL native database, never a configured hub proxy.
  const sqliteHolds = DISK_IO ? sqliteHoldRecorder(path.join(root, "far")) : undefined;
  const restore: (() => void)[] = [];
  try {
  const deadline = performance.now() + (DISK_IO ? 300_000 : 150_000);
  const remaining = () => Math.max(1, deadline - performance.now());
  const hub = new MeshStore(path.join(root, "hub"), 65536, 1000, { stateBackend: "sqlite", lockTimeoutMs: 5000 });
  const agentBackend = DISK_IO ? "sqlite" : "file";
  const far = new MeshStore(path.join(root, "far"), 65536, 1000, { stateBackend: agentBackend });
  if (DISK_IO) console.log("FLEET_CONTENTION_STORES " + JSON.stringify({ root,
    agentRoot: far.root, hubRoot: hub.root, agentDatabase: path.join(far.root, "state.db"),
    pid: process.pid, agentBackend, hubBackend: "sqlite",
    transport: "production serveBridgeAgent/RemoteBridgeSide JSON protocol over in-process PassThrough streams; not SSH or network" }));
  const local = cliSide(hub, "far"), agent = cliSide(far, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, 15_000);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(root, "ack-cursor.json"), stopMs: 500 });
  const children: Worker[] = [], samples: Sample[] = [], byCommand = new Map<string, Sample>();
  const accepted: string[] = [];
  const originalPublishHub = hub.publish, originalPublishFar = far.publish;
  const publishHub = originalPublishHub.bind(hub), publishFar = originalPublishFar.bind(far);
  restore.push(() => { hub.publish = originalPublishHub; far.publish = originalPublishFar; });
  let sender: FabricControlPlane | undefined, receiver: FabricControlPlane | undefined;
  let running: Promise<void> | undefined, runFailure: unknown;
  let measurementStart = 0, measurementStartWall = 0, measurementEnd = 0;
  let evidence: Record<string, unknown> | undefined;
  try {
    const lead = await native(hub, "contention-hub"), lane = await native(far, "contention-agent");
    for (let index = 0; index < WRITERS; index++) children.push(worker("writer", hub.root, index, lead.id));
    // These four links have native spoke roots and real inbound presence/event traffic. They
    // are separate OS processes, not four outbound views of the same hub-only roots.
    for (let index = 0; index < OTHER_LINKS; index++) children.push(worker("link", hub.root, index, lead.id));
    await bounded(Promise.all(children.map(child => child.ready)), remaining(), "44 processes ready");
    const directory = new ParticipantDirectory(hub, { enabled: true, hostId: lead.id, rootId: lead.id, identity: lead.identity });
    sender = new FabricControlPlane(hub, lead.identity, { enabled: true, hostId: lead.id, pollMs: 20,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    receiver = new FabricControlPlane(far, lane.identity, { enabled: true, hostId: lane.id,
      ownerIncarnation: lane.ownerIncarnation, pollMs: 20 });
    sender.start(() => ({ accepted: false }));
    receiver.start((command, from, _signal, verification) => {
      const sample = samples.find(row => command.message === `contention-${row.index}`)!;
      expect(sample).toBeDefined();
      expect(command).toMatchObject({ targetId: lane.id, operation: "followUp", ownerIncarnation: lane.ownerIncarnation });
      expect(from.id).toBe(lead.id); expect(verification).toBe("bridge");
      sample.commandId = command.commandId; sample.acceptedAt = performance.now();
      byCommand.set(command.commandId, sample); accepted.push(command.commandId);
      return { accepted: true, messageId: `accepted-${sample.index}` };
    });
    far.publish = async data => {
      const event = await publishFar(data);
      if (event.topic === "fabric.control.ack") {
        const sample = byCommand.get((event.data as { commandId: string }).commandId)!;
        sample.agentAckAt = performance.now(); sample.agentAck = event;
      }
      return event;
    };
    hub.publish = async data => {
      const event = await publishHub(data);
      if (event.topic === "fabric.control.ack") {
        const sample = byCommand.get((event.data as { commandId: string }).commandId)!;
        sample.hubAckAt = performance.now(); sample.hubAck = event; sample.received();
      }
      return event;
    };
    await bounded(bridge.start(), remaining(), "measured bridge startup");
    running = bridge.run().catch(error => { runFailure = error; });
    // Only the disk opt-in warms the already-running links; ordinary 8-sample behavior is unchanged.
    if (DISK_IO) await sleep(HEARTBEAT_MS);
    sqliteHolds?.assertOutsideTransaction();
    measurementStart = performance.now(); measurementStartWall = Date.now();
    if (DISK_IO) console.log("FLEET_CONTENTION_WINDOW " + JSON.stringify({
      agentRoot: far.root, hubRoot: hub.root, measurementStartWall,
      measurementWindowEndWall: measurementStartWall + MEASURE_MIN_MS, windowMs: MEASURE_MIN_MS }));
    for (const child of children) child.process.stdin.write("measure\n");
    const liveAtStart = children.filter(child => alive(child.process.pid!)).map(child => child.process.pid!);
    expect(liveAtStart).toHaveLength(WRITERS + OTHER_LINKS);
    await bounded(Promise.all(Array.from({ length: SAMPLES }, async (_, index) => {
      await sleep(index * SAMPLE_INTERVAL_MS);
      let received!: () => void;
      const receipt = new Promise<void>(resolve => { received = resolve; });
      const sample: Sample = { index, requestAt: performance.now(), receipt, received };
      samples.push(sample);
      const result = await sender!.request(lane.id, lane.id, "followUp", {
        message: `contention-${index}`, ownerIncarnation: lane.ownerIncarnation,
      }, lane.id, { routedRemoteHost: "far", idempotencyKey: `contention-${index}`, timeoutMs: 60_000 });
      sample.observedAt = performance.now();
      await bounded(receipt, remaining(), `ACK ${index} hub publish return`);
      expect(result).toMatchObject({ acknowledged: true, messageId: `accepted-${index}` });
      expect(sample.hubAck).toMatchObject({ topic: "fabric.control.ack", kind: "accepted", to: lead.id,
        from: lane.identity, verification: "bridge", data: { commandId: sample.commandId,
          targetId: lane.id, ownerIncarnation: lane.ownerIncarnation, accepted: true,
          messageId: `accepted-${index}`, bridge: { from: "far", id: sample.agentAck!.id } } });
    })), remaining(), `${SAMPLES} correlated ACKs under load`);
    await sleep(MEASURE_MIN_MS - (performance.now() - measurementStart));
    measurementEnd = performance.now();
    const liveAtEnd = children.filter(child => alive(child.process.pid!)).map(child => child.process.pid!);
    expect(liveAtEnd).toEqual(liveAtStart);
    for (const child of children) child.process.stdin.write("snapshot\n");
    // fs.watch/change frames drive the real bridge. This bounded 100 ms observer only waits
    // for child snapshots and already-published inbound traffic to drain before no-drop checks.
    await bounded((async () => {
      while (children.some(child => !child.messages.some(message => message.type === "snapshot"))) await sleep(100);
      while (children.some(child => child.messages.filter(message => message.type === "traffic").some(message =>
        !hub.read({ limit: 1000 }).some(event => (event.data as { bridge?: { id?: string } })?.bridge?.id === message.id)))) await sleep(100);
    })(), remaining(), "heartbeat snapshots and inbound bridge traffic drain");
    const childRows = children.map(child => {
      const ready = child.messages.find(message => message.type === "ready")!;
      const snapshot = child.messages.find(message => message.type === "snapshot")!;
      const heartbeats = child.messages.filter(message => message.type === "heartbeat");
      expect(child.messages.filter(message => message.type === "failure")).toHaveLength(0);
      expect(heartbeats.filter(message => message.ok).length).toBeGreaterThanOrEqual(2);
      expect(Math.max(...heartbeats.map(message => message.began!)) -
        Math.min(...heartbeats.map(message => message.began!))).toBeGreaterThanOrEqual(4_500);
      expect(heartbeats.filter(message => !message.ok)).toHaveLength(0);
      expect(snapshot.native!.updatedAt).toBeGreaterThan(ready.updatedAt!);
      expect(snapshot.native!.expiresAt - snapshot.native!.updatedAt).toBe(LEASE_MS);
      expect(snapshot.native!.expiresAt).toBeGreaterThan(Date.now());
      if (child.mode === "link") {
        expect(snapshot.mirrorCalls).toBeGreaterThan(0);
        expect(snapshot.mirrorRenewals).toBeGreaterThan(0);
        expect(snapshot.hubMirrorCommits).toBeGreaterThan(0);
        expect(snapshot.mirrored?.mirror).toBeDefined();
        expect(snapshot.mirrored!.updatedAt).toBeGreaterThan(ready.updatedAt!);
        expect(snapshot.mirrored!.updatedAt).toBe(snapshot.native!.updatedAt);
      }
      const trafficIds = child.messages.filter(message => message.type === "traffic").map(message => message.id!);
      for (const id of trafficIds) expect(hub.read({ limit: 1000 }).filter(event =>
        (event.data as { bridge?: { id?: string } })?.bridge?.id === id)).toHaveLength(1);
      return { pid: child.process.pid, mode: child.mode, index: child.index, heartbeats,
        readyLease: { updatedAt: ready.updatedAt, expiresAt: ready.expiresAt }, nativeLease: snapshot.native,
        mirroredLease: snapshot.mirrored, mirrorCalls: snapshot.mirrorCalls, mirrorRenewals: snapshot.mirrorRenewals,
        hubMirrorCommits: snapshot.hubMirrorCommits, trafficIds, holds: summary(snapshot.holds ?? []) };
    });
    const events = (store: MeshStore, topic: string) => store.read({ limit: 1000 }).filter(event => event.topic === topic);
    expect(events(hub, "fabric.control.command")).toHaveLength(SAMPLES);
    expect(events(far, "fabric.control.command")).toHaveLength(SAMPLES);
    expect(events(hub, "fabric.control.ack")).toHaveLength(SAMPLES);
    expect(events(far, "fabric.control.ack")).toHaveLength(SAMPLES);
    expect(accepted).toHaveLength(SAMPLES); expect(new Set(accepted).size).toBe(SAMPLES);
    expect(runFailure).toBeUndefined();
    for (const sample of samples) {
      const sent = events(hub, "fabric.control.command").find(event =>
        (event.data as { commandId: string }).commandId === sample.commandId)!;
      expect(events(far, "fabric.control.command").find(event =>
        (event.data as { commandId: string }).commandId === sample.commandId))
        .toMatchObject({ verification: "bridge", data: { bridge: { from: "hub", id: sent.id } } });
    }
    const rows = samples.sort((a, b) => a.index - b.index).map(sample => ({ index: sample.index,
      commandId: sample.commandId, sourceAckId: sample.agentAck!.id, destinationAckId: sample.hubAck!.id,
      ackReturnMs: round(sample.hubAckAt! - sample.agentAckAt!),
      requesterObservedMs: round(sample.observedAt! - sample.agentAckAt!),
      requestRoundTripMs: round(sample.observedAt! - sample.requestAt),
      offsetMs: round(sample.requestAt - measurementStart) }));
    evidence = { scenario: "40 native SQLite writer processes + four inbound FILE-spoke bridge processes",
      label: process.env.ACK_MEASUREMENT_LABEL ?? "unspecified", host: os.hostname(), root,
      agentRoot: far.root, hubRoot: hub.root, agentPid: process.pid, agentBackend, hubBackend: "sqlite",
      agentDatabase: DISK_IO ? path.join(far.root, "state.db") : undefined,
      transport: "production serveBridgeAgent/RemoteBridgeSide JSON protocol over in-process PassThrough streams; not SSH or network",
      diskIo: DISK_IO, warmupMs: DISK_IO ? HEARTBEAT_MS : 0,
      measurementWindowEndWall: measurementStartWall + MEASURE_MIN_MS,
      measurementEndWall: Date.now(), diskWindowMs: DISK_IO ? MEASURE_MIN_MS : undefined,
      heartbeatMs: HEARTBEAT_MS, leaseMs: LEASE_MS, samples: SAMPLES, sampleIntervalMs: SAMPLE_INTERVAL_MS,
      measurementStartWall, measuredLoadMs: round(measurementEnd - measurementStart),
      liveAtStart, liveAtEnd, clock: "same-process performance.now; agent durable ACK publish return -> hub durable ACK publish return",
      ackReturn: summary(rows.map(row => row.ackReturnMs)), requesterObserved: summary(rows.map(row => row.requesterObservedMs)),
      rows, children: childRows, tensOfSecondsReproduced: rows.some(row => row.ackReturnMs >= 10_000) };
  } finally {
    for (const child of children) if (child.process.exitCode === null && child.process.signalCode === null) child.process.stdin.end("stop\n");
    let exits: Awaited<Worker["done"]>[] = [];
    try { exits = await bounded(Promise.all(children.map(child => child.done)), 8_000, "normal child cleanup"); }
    catch {
      for (const child of children) if (child.process.exitCode === null && child.process.signalCode === null) child.process.kill("SIGTERM");
      try { exits = await bounded(Promise.all(children.map(child => child.done)), 2_000, "TERM child cleanup"); }
      catch {
        for (const child of children) if (child.process.exitCode === null && child.process.signalCode === null) child.process.kill("SIGKILL");
        exits = await bounded(Promise.all(children.map(child => child.done)), 3_000, "KILL child cleanup");
      }
    }
    await sender?.close(); await receiver?.close();
    await bridge.stop(); await running;
    remote.close(); input.end(); await serving; output.end(); await agent.fence();
    const flush = (side: StoreBridgeSide) => (side as StoreBridgeSide & { flushLeaseEffects?: () => Promise<void> }).flushLeaseEffects?.();
    await Promise.all([flush(local), flush(agent)]);
    hub.publish = originalPublishHub; far.publish = originalPublishFar;
    hub.closeState(); far.closeState();
    const remainingPids = children.filter(child => alive(child.process.pid!)).map(child => child.process.pid!);
    fs.rmSync(root, { recursive: true, force: true });
    console.log("FLEET_CONTENTION_CLEANUP " + JSON.stringify({ exits, remainingPids, storesClosed: true, scratchRemoved: !fs.existsSync(root) }));
    if (evidence) {
      if (sqliteHolds) evidence.agentSqliteHolds = sqliteHolds.snapshot(measurementStartWall, measurementStart);
      console.log("FLEET_CONTENTION_RESULT " + JSON.stringify(evidence));
    }
    else console.log("FLEET_CONTENTION_CHILD_DIAGNOSTICS " + JSON.stringify(children.map(child => ({
      pid: child.process.pid, mode: child.mode, index: child.index, messages: child.messages, stderr: child.stderr }))));
    expect(remainingPids).toHaveLength(0);
    expect(exits.every(exit => exit.code === 0 && exit.signal === null)).toBe(true);
  }
  } finally {
    // Even an assertion, failed startup, snapshot, or teardown must not leak monkeypatches.
    try { for (const reset of restore.reverse()) reset(); }
    finally { sqliteHolds?.restore(); }
  }
}, DISK_IO ? 330_000 : 180_000);
