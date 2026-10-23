import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough, type Readable } from "node:stream";
import { expect, it } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { isMeshRetryableBusy } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const SAMPLES = 6, RENEWED_ROOTS = 150, WRITER_HOLD_MS = 120;
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const native = async (store: MeshStore, name: string) => {
  const now = Date.now(), id = `session:${name}-00000000`, ownerIncarnation = `fixture:${id}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id, rootId: id, identity, incarnationToken: name,
    startedAt: now, updatedAt: now, expiresAt: now + 60_000 };
  await store.put({ key: `topology/hosts/${hash(id)}`, value: host, identity });
  writeParticipantFile(store.root, { key: `topology/participants/${hash(id)}`, version: 1,
    updatedAt: now, updatedBy: identity, value: { format: 1, id, kind: "root", rootId: id,
      ownerHostId: id, ownerIdentityId: id, ownerIncarnation, name, label: name, status: "idle",
      runner: "pi", transport: "host", capabilities: ["followUp"], sessionId: name,
      startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return { id, identity, host, ownerIncarnation };
};

// The fifth argument is the candidate CLI's deferred-effects opt-in. Pinned main's four-argument
// constructor ignores it at runtime. This SAME test runs in two real source trees, not a false/true
// flag A/B on candidate sources; no new candidate-only method is required by the baseline.
const cliSide = (store: MeshStore, peer: string): StoreBridgeSide => {
  const Side = StoreBridgeSide as new (store: MeshStore, peer: string, now: () => number,
    monitorLease: boolean, deferLeaseInstalls?: boolean) => StoreBridgeSide;
  return new Side(store, peer, Date.now, true, true);
};

type Frame = { id?: number; op?: string; args?: unknown; ok?: boolean;
  result?: { events?: MeshEvent[] }; notification?: string };
const observeFrames = (stream: Readable, frames: Frame[]) => {
  let buffered = "";
  stream.on("data", chunk => {
    buffered += String(chunk);
    for (let newline; (newline = buffered.indexOf("\n")) >= 0;) {
      const line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      if (line.trim()) frames.push(JSON.parse(line) as Frame);
    }
  });
};
const bounded = async <T>(work: Promise<T>, what: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`ACK measurement: ${what} exceeded 3 s`)), 3_000);
    })]);
  } finally { if (timer) clearTimeout(timer); }
};
interface Sample {
  index: number; requestAt: number; acceptedAt?: number; agentAckAt?: number; hubAckAt?: number;
  observedAt?: number; lockAt?: number; releasedAt?: number; commandId?: string;
  agentAck?: MeshEvent; hubAck?: MeshEvent; attempts: number; busyWhileHeld: number;
  hubReceipt: Promise<void>; receiptReturned: () => void;
}
const round = (ms: number) => +ms.toFixed(2);
const summary = (values: number[]) => {
  const sorted = values.slice().sort((a, b) => a - b);
  const p = (q: number) => round(sorted[Math.ceil((sorted.length - 1) * q)]!);
  return { n: sorted.length, p50Ms: p(0.5), p95Ms: p(0.95), maxMs: p(1) };
};

// Keep reason: #13645's SQLite-hub / FILE-agent boundary, real stdio RPC and control-plane ACK
// correlation. Unlike event.createdAt age, these clocks span actual agent ACK commit -> hub append
// receipt -> requesting control-plane resolution, including real SQLite busy refusals and retries.
it("measures true toLocal followUp ACK return during a contended SQLite writer and native lease renewals", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-ack-latency-"));
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 500,
    { stateBackend: "sqlite", lockTimeoutMs: 80 });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 500, { stateBackend: "file" });
  const local = cliSide(hub, "far"), agent = cliSide(far, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const requests: Frame[] = [], responses: Frame[] = [], logs: string[] = [];
  observeFrames(input, requests); observeFrames(output, responses);
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, 3_000);
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath, stopMs: 500, log: line => logs.push(line) });
  const samples: Sample[] = [];
  const byCommand = new Map<string, Sample>();
  let sender: FabricControlPlane | undefined, receiver: FabricControlPlane | undefined;
  let running: Promise<void> | undefined, runFailure: unknown;
  let raw: InstanceType<typeof import("node:sqlite")["DatabaseSync"]> | undefined;
  let held = false, heldSample: Sample | undefined, releaseTimer: ReturnType<typeof setTimeout> | undefined;
  const release = () => {
    if (releaseTimer) clearTimeout(releaseTimer);
    releaseTimer = undefined;
    if (held) { raw!.exec("COMMIT"); held = false; heldSample!.releasedAt = performance.now(); }
  };
  const publishHub = hub.publish.bind(hub), publishFar = far.publish.bind(far);
  try {
    const lead = await native(hub, "ack-hub"), lane = await native(far, "ack-agent");
    // Real native lease-file replacements drive the normal fs.watch -> change-frame path. There
    // is no injected filesystem delay or fake timer. 150 roots match the lease-install age probe.
    const renewed: Array<Awaited<ReturnType<typeof native>>> = [];
    for (let index = 0; index < RENEWED_ROOTS; index++) renewed.push(await native(far, `ack-background-${index}`));
    const directory = new ParticipantDirectory(hub, { enabled: true, hostId: lead.id, rootId: lead.id, identity: lead.identity });
    sender = new FabricControlPlane(hub, lead.identity, { enabled: true, hostId: lead.id, pollMs: 20,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    receiver = new FabricControlPlane(far, lane.identity, { enabled: true, hostId: lane.id,
      ownerIncarnation: lane.ownerIncarnation, pollMs: 20 });
    const accepted: string[] = [];
    sender.start(() => ({ accepted: false }));
    receiver.start((command, from, _signal, verification) => {
      expect(command).toMatchObject({ targetId: lane.id, operation: "followUp", ownerIncarnation: lane.ownerIncarnation });
      expect(from.id).toBe(lead.id); expect(verification).toBe("bridge");
      const sample = samples.at(-1)!;
      expect(command.message).toBe(`latency-${sample.index}`);
      sample.commandId = command.commandId; sample.acceptedAt = performance.now();
      byCommand.set(command.commandId, sample); accepted.push(command.commandId);
      if (sample.index >= 0) {
        const now = Date.now();
        for (const owner of renewed) writeHostLease(far.root, { id: owner.id, rootId: owner.id,
          identityId: owner.id, incarnationToken: owner.host.incarnationToken, startedAt: owner.host.startedAt,
          updatedAt: now, expiresAt: now + 60_000 });
      }
      return { accepted: true, messageId: `accepted-${sample.index}` };
    });
    await bounded(bridge.start(), "initial presence");
    const { DatabaseSync } = await import("node:sqlite");
    raw = new DatabaseSync(path.join(hub.root, "state.db"));
    raw.exec("PRAGMA busy_timeout = 0; CREATE TABLE ack_latency_probe (value INTEGER NOT NULL); INSERT INTO ack_latency_probe VALUES (0)");
    // Observe actual publications without changing their input/result/error. The raw connection
    // holds a real UPDATE in state.db; hub.publish's real ownership fence must encounter SQLITE_BUSY.
    far.publish = async input => {
      const event = await publishFar(input);
      if (event.topic === "fabric.control.ack") {
        const sample = byCommand.get((event.data as { commandId: string }).commandId)!;
        sample.agentAckAt = performance.now(); sample.agentAck = event;
        if (sample.index >= 0) {
          expect(held).toBe(false);
          raw!.exec("BEGIN IMMEDIATE; UPDATE ack_latency_probe SET value = value + 1");
          held = true; heldSample = sample; sample.lockAt = performance.now();
          // Test stimulus only: the event source for bridge retries remains SQLite's busy refusal;
          // this one-shot timer ends the competing transaction, it never drives bridge progress.
          releaseTimer = setTimeout(release, WRITER_HOLD_MS);
        }
      }
      return event;
    };
    hub.publish = async input => {
      const isAck = input.topic === "fabric.control.ack";
      const data = input.data as { commandId?: string } | undefined;
      const sample = isAck && data?.commandId ? byCommand.get(data.commandId) : undefined;
      if (sample) sample.attempts++;
      try {
        const event = await publishHub(input);
        if (sample) { sample.hubAckAt = performance.now(); sample.hubAck = event; sample.receiptReturned(); }
        return event;
      } catch (error) {
        if (sample && held && isMeshRetryableBusy(error)) sample.busyWhileHeld++;
        throw error;
      }
    };
    running = bridge.run().catch(error => { runFailure = error; });
    for (let index = -1; index < SAMPLES; index++) {
      let receiptReturned!: () => void;
      const hubReceipt = new Promise<void>(resolve => { receiptReturned = resolve; });
      const sample: Sample = { index, requestAt: performance.now(), attempts: 0, busyWhileHeld: 0,
        hubReceipt, receiptReturned };
      samples.push(sample);
      const result = await bounded(sender.request(lane.id, lane.id, "followUp", {
        message: `latency-${index}`, ownerIncarnation: lane.ownerIncarnation,
      }, lane.id, { routedRemoteHost: "far", idempotencyKey: `ack-latency-${index}` }), `sample ${index}`);
      sample.observedAt = performance.now();
      // The requesting reader may see the committed append before publish's awaited cleanup
      // returns. Keep both clocks, and separately await the actual destination publish receipt.
      await bounded(sample.hubReceipt, `sample ${index} hub publish receipt`);
      expect(runFailure).toBeUndefined();
      expect(result).toMatchObject({ acknowledged: true, messageId: `accepted-${index}` });
      expect(sample.agentAck).toBeDefined(); expect(sample.hubAck).toBeDefined();
      expect(sample.hubAck).toMatchObject({ topic: "fabric.control.ack", kind: "accepted", to: lead.id,
        from: { id: lane.id, name: "main", kind: "main", sessionId: "ack-agent" }, verification: "bridge",
        data: { commandId: sample.commandId, targetId: lane.id, ownerIncarnation: lane.ownerIncarnation,
          accepted: true, messageId: `accepted-${index}`, bridge: { from: "far", id: sample.agentAck!.id } } });
      if (index >= 0) {
        expect(sample.busyWhileHeld, "a real inbound ACK publication hit the competing SQLite writer").toBeGreaterThan(0);
        expect(sample.attempts).toBeGreaterThan(1);
        expect(sample.releasedAt).toBeDefined();
        expect(sample.hubAckAt!).toBeGreaterThanOrEqual(sample.releasedAt!);
      }
    }
    // Let the successful receipt checkpoint finish; no polling or event.createdAt latency proxy.
    await new Promise<void>(resolve => setImmediate(resolve));
    const all = (store: MeshStore, topic: string) => store.read({ limit: 500 }).filter(event => event.topic === topic);
    const hubCommands = all(hub, "fabric.control.command"), agentCommands = all(far, "fabric.control.command");
    expect(hubCommands).toHaveLength(SAMPLES + 1); expect(agentCommands).toHaveLength(SAMPLES + 1);
    expect(all(hub, "fabric.control.ack")).toHaveLength(SAMPLES + 1);
    expect(all(far, "fabric.control.ack")).toHaveLength(SAMPLES + 1);
    expect(new Set(accepted).size).toBe(SAMPLES + 1); expect(accepted).toHaveLength(SAMPLES + 1);
    for (const sample of samples) {
      const sent = hubCommands.find(event => (event.data as { commandId: string }).commandId === sample.commandId)!;
      expect(agentCommands.find(event => (event.data as { commandId: string }).commandId === sample.commandId))
        .toMatchObject({ verification: "bridge", data: { bridge: { from: "hub", id: sent.id } } });
      expect(requests.some(frame => frame.op?.startsWith("publish") && JSON.stringify(frame.args).includes(sent.id))).toBe(true);
      expect(responses.some(frame => frame.ok && frame.result?.events?.some(event => event.id === sample.agentAck!.id))).toBe(true);
    }
    const cursor = JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toLocal: { after: number; mark: number } };
    expect(cursor.toLocal.after).toBeGreaterThanOrEqual(samples.at(-1)!.agentAck!.sequence);
    expect(cursor.toLocal.mark).toBeGreaterThanOrEqual(samples.at(-1)!.hubAck!.sequence);
    const measured = samples.filter(sample => sample.index >= 0);
    const rows = measured.map(sample => ({ sample: sample.index, commandId: sample.commandId,
      sourceAckId: sample.agentAck!.id, destinationAckSequence: sample.hubAck!.sequence,
      ackReturnMs: round(sample.hubAckAt! - sample.agentAckAt!),
      requesterObservedMs: round(sample.observedAt! - sample.agentAckAt!),
      requestRoundTripMs: round(sample.observedAt! - sample.requestAt),
      writerHeldMs: round(sample.releasedAt! - sample.lockAt!),
      afterWriterReleaseMs: round(sample.hubAckAt! - sample.releasedAt!),
      attempts: sample.attempts, busyWhileHeld: sample.busyWhileHeld }));
    console.log(JSON.stringify({ scenario: "true toLocal ACK return, SQLite hub / FILE agent / in-process stdio",
      label: process.env.ACK_MEASUREMENT_LABEL ?? "unspecified", host: os.hostname(), samples: SAMPLES,
      renewedRootsPerSample: RENEWED_ROOTS, requestedWriterHoldMs: WRITER_HOLD_MS,
      clock: "same-process performance.now; agent durable ACK publish return -> hub durable ACK publish return",
      ackReturn: summary(measured.map(sample => sample.hubAckAt! - sample.agentAckAt!)),
      requesterObserved: summary(measured.map(sample => sample.observedAt! - sample.agentAckAt!)),
      warmupAckReturnMs: round(samples[0]!.hubAckAt! - samples[0]!.agentAckAt!), rows, logs }));
  } finally {
    release();
    await sender?.close(); await receiver?.close();
    await bridge.stop(); await running;
    remote.close(); input.end(); await serving; output.end();
    await agent.fence();
    // Candidate CLI drains these on exit; old main has only synchronous effects.
    const flush = (side: StoreBridgeSide) =>
      (side as StoreBridgeSide & { flushLeaseEffects?: () => Promise<void> }).flushLeaseEffects?.();
    await Promise.all([flush(local), flush(agent)]);
    hub.publish = publishHub; far.publish = publishFar;
    raw?.close(); hub.closeState(); far.closeState();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);
