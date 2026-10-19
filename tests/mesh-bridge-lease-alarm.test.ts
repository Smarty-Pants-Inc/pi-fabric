// Linux process/boot trust boundary; existing hosted-only guards remain untouched.
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { BRIDGE_STALL_ALARM_TOPIC, bridgeStallReasonPath, isBridgedTopic, StoreBridgeSide } from "../src/mesh/bridge.js";
import { BRIDGE_LEASE_ALARM_AFTER_MS } from "../src/topology/bridge-alarms.js";
import { rootPresenceAlarms } from "../src/topology/stall-alarms.js";
import { MIRROR_FRESH_MS, readHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { processIncarnation, readPhysicalHostIdentity } from "../src/core/atomic-write.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
const owner: MeshIdentity = { id: "session:11111111-1111-4111-8111-111111111111", name: "owner", kind: "main" };
const remoteId = "session:22222222-2222-4222-8222-222222222222";
const hostKey = `topology/hosts/${createHash("sha256").update(remoteId).digest("hex")}`;
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); vi.unstubAllEnvs(); });
async function waitFor(check: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!check() && performance.now() < deadline) await sleep(5);
  expect(check()).toBe(true);
}
function changed<T>(root: string, check: () => T | undefined, timeoutMs = 20_000): Promise<T> {
  return new Promise((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    const timer = setTimeout(() => finish(new Error(`observation timed out: ${root}`)), timeoutMs);
    function finish(error?: Error, value?: T) { clearTimeout(timer); watcher?.close(); error ? reject(error) : resolve(value!); }
    function inspect() { try { const value = check(); if (value !== undefined) finish(undefined, value); } catch (error) { finish(error as Error); } }
    watcher = fs.watch(root, { recursive: true }, inspect); inspect();
  });
}
function messages(child: ChildProcess) {
  const receipts: Array<Record<string, any>> = [];
  child.on("message", message => receipts.push(message as Record<string, any>));
  return { receipts, wait(kind: string, after = 0): Promise<Record<string, any>> {
    const previous = receipts.slice(after).find(item => item.kind === kind);
    if (previous) return Promise.resolve(previous);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => done(new Error(`missing ${kind}: ${JSON.stringify(receipts)}`)), 20_000);
      const receive = (item: any) => {
        if (item.kind === `${kind}-failed` || item.kind === "failure") done(new Error(`${item.kind}: ${String(item.error)}`));
        else if (item.kind === kind) done(undefined, item);
      };
      const exit = (code: number | null) => done(new Error(`child exited ${code} awaiting ${kind}`));
      function done(error?: Error, item?: any) { clearTimeout(timer); child.off("message", receive); child.off("exit", exit); error ? reject(error) : resolve(item); }
      child.on("message", receive); child.on("exit", exit);
    });
  }};
}
const childAlive = (child: ChildProcess): boolean => child.exitCode === null && child.signalCode === null;
async function sendChildMessage(child: ChildProcess, message: Record<string, unknown>): Promise<void> {
  if (!childAlive(child) || !child.connected) return;
  await new Promise<void>((resolve, reject) => {
    child.send(message, error => {
      if (!error || ((error as NodeJS.ErrnoException).code === "EPIPE" && !childAlive(child))) resolve();
      else reject(error);
    });
  });
}
function waitForExit(child: ChildProcess): Promise<void> {
  if (!childAlive(child)) return Promise.resolve();
  return new Promise(resolve => {
    const onExit = () => { child.off("exit", onExit); resolve(); };
    child.once("exit", onExit);
    if (!childAlive(child)) onExit();
  });
}
async function terminate(child: ChildProcess) {
  if (!childAlive(child)) return;
  if (process.platform === "linux") child.kill("SIGCONT");
  const exited = waitForExit(child);
  let sendError: unknown;
  try { await sendChildMessage(child, { op: "shutdown" }); } catch (error) { sendError = error; }
  const timer = setTimeout(() => child.kill("SIGKILL"), 2_000);
  try { await exited; } finally { clearTimeout(timer); }
  if (sendError) throw sendError;
}
async function fixture(populated: boolean, watchdog: Record<string, unknown> = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lease-process-"));
  const hubRoot = path.join(root, "hub"), farRoot = path.join(root, "far");
  fs.mkdirSync(hubRoot); fs.mkdirSync(farRoot);
  const program = path.join(root, "native.mjs");
  const source = String.raw`
    import { Transform } from "node:stream";
    import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
    import { BRIDGE_STALL_ALARM_TOPIC, MeshBridge, StoreBridgeSide, RemoteBridgeSide, serveBridgeAgent } from ${JSON.stringify(path.resolve("src/mesh/bridge.ts"))};
    import { writeHostLease } from ${JSON.stringify(path.resolve("src/topology/host-leases.ts"))};
    const [role, root, cursor, populated, watchdogWire = "{}"] = process.argv.slice(2);
    const watchdog = JSON.parse(watchdogWire);
    let wallClockOffsetMs = 0;
    if (role === "bridge") {
      const realDateNow = Date.now.bind(Date);
      Date.now = () => realDateNow() + wallClockOffsetMs;
    }
    const store = new MeshStore(root, 64 * 1024, 100);
    const send = value => process.send?.(value);
    process.on("uncaughtException", error => { send({ kind: "failure", error: String(error.stack) }); process.exit(1); });
    process.on("unhandledRejection", error => { send({ kind: "failure", error: String(error?.stack || error) }); process.exit(1); });
    let gates = [], sourceReadPaused = false, releaseSourceRead, bridgeForStop;
    process.on("message", message => {
      if (message.op === "wall-clock-offset") wallClockOffsetMs = message.offsetMs;
      if (message.op === "pause-source-read") { sourceReadPaused = true; send({ kind: "source-read-paused" }); }
      if (message.op === "resume-source-read") {
        sourceReadPaused = false; releaseSourceRead?.(); releaseSourceRead = undefined;
        send({ kind: "source-read-resumed" });
      }
      if (message.op === "pause") { gates.forEach(gate => gate.paused = true); send({ kind: "paused" }); }
      if (message.op === "resume" || message.op === "shutdown") {
        gates.forEach(gate => { gate.paused = false; for (const bytes of gate.queued.splice(0)) gate.push(bytes); gate.bytes = 0; });
        send({ kind: "resumed" });
      }
      // Never invoke populated bridge.stop/withdraw locally.
      if (message.op === "publish") void store.publish(message.event)
        .then(result => send({ kind: "published", sequence: result.sequence }))
        .catch(error => send({ kind: "publish-failed", error: String(error) }));
      if (message.op === "shutdown") {
        const stopping = watchdog.stopBridgeOnShutdown && bridgeForStop ? bridgeForStop.stop() : undefined;
        if (stopping) void stopping.then(() => { store.closeState(); process.exit(0); });
        else { store.closeState(); process.exit(0); }
      }
    });
    if (role === "agent") {
      if (populated === "yes") {
        const now = Date.now(), id = ${JSON.stringify(remoteId)}, identity = { id, name: "remote", kind: "main" };
        await store.put({ key: ${JSON.stringify(hostKey)}, identity, value: {
          format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 600000,
          incarnationToken: "remote-native-generation", pid: process.pid,
          bridgeProcess: { pid: process.pid, link: "forged", incarnation: "forged" }
        }});
        writeHostLease(root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now,
          expiresAt: now + 600000, incarnationToken: "remote-native-generation" });
      }
      class Gate extends Transform {
        paused = false; queued = []; bytes = 0;
        _transform(chunk, encoding, callback) {
          if (this.paused) {
            this.bytes += chunk.length;
            if (this.bytes > 4 * 1024 * 1024) return callback(new Error("transport gate overflow"));
            this.queued.push(Buffer.from(chunk)); send({ kind: "buffered", bytes: this.bytes });
          } else this.push(chunk);
          callback();
        }
      }
      const input = new Gate(), output = new Gate(); gates = [input, output];
      process.stdin.pipe(input); output.pipe(process.stdout);
      const side = new StoreBridgeSide(store, "hub", Date.now, true);
      if (watchdog.inboundTailStall) side.tail = async (after, offset) => ({ events: [], through: after, ...(offset !== undefined ? { offset } : {}) });
      send({ kind: "ready", pid: process.pid });
      await serveBridgeAgent(side, input, output);
      store.closeState();
      process.exit(0);
    } else {
      const local = new StoreBridgeSide(store, "far", Date.now, true);
      const localTail = local.tail.bind(local);
      local.tail = async (after, offset) => {
        if (sourceReadPaused) await new Promise(resolve => { releaseSourceRead = resolve; send({ kind: "source-read-blocked" }); });
        return localTail(after, offset);
      };
      const remote = new RemoteBridgeSide(process.stdin, process.stdout, 120000);
      const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath: cursor,
        log: line => send({ kind: "bridge-log", line }),
        watchdogIntervalMs: watchdog.intervalMs, watchdogProbeTimeoutMs: watchdog.probeTimeoutMs,
        watchdogStallAfterMs: watchdog.stallAfterMs, directionStallAfterMs: watchdog.directionStallAfterMs,
        ...(watchdog.callbacks === false ? {} : {
          notifyWatchdog: () => send({ kind: "watchdog" }),
          publishStallAlarm: async alarm => {
            const event = await store.publish({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall",
              from: { id: "bridge:hub", name: "mesh-bridge", kind: "main" }, to: ${JSON.stringify(owner.id)}, text: alarm.reason, data: alarm });
            send({ kind: "stall-alarm", alarm, sequence: event.sequence });
          },
          publishDeliveryAgeAlarm: async alarm => {
            const event = await store.publish({ topic: "ops.owner",
              kind: alarm.state === "outage" ? "bridge.delivery-age.alarm" : "bridge.delivery-age.recovered",
              from: { id: "bridge:hub", name: "mesh-bridge", kind: "main" }, to: ${JSON.stringify(owner.id)},
              text: alarm.direction + " " + alarm.reason + " delivery-age episode " + alarm.state, data: alarm });
            send({ kind: "delivery-age-alarm", alarm, sequence: event.sequence });
          },
        }),
      });
      bridgeForStop = bridge;
      await remote.hello();
      send({ kind: "ready", pid: process.pid });
      await bridge.run(); send({ kind: "ended" });
    }
  `;
  buildSync({ stdin: { contents: source, resolveDir: process.cwd() }, outfile: program,
    bundle: true, platform: "node", format: "esm", packages: "external" });
  const launch = (role: string, meshRoot: string) => spawn(process.execPath,
    [program, role, meshRoot, path.join(root, "cursor.json"), populated ? "yes" : "no", JSON.stringify(watchdog)],
    { stdio: ["pipe", "pipe", "pipe", "ipc"] });
  const agent = launch("agent", farRoot), agentMessages = messages(agent);
  const bridge = launch("bridge", hubRoot), bridgeMessages = messages(bridge);
  let stderr = "";
  const transportErrors: string[] = [];
  for (const [name, child] of [["bridge", bridge], ["agent", agent]] as const) {
    child.stderr!.on("data", bytes => {
      // The local deletion guard announces its hosted-only report mode in every child; it is not product stderr.
      stderr += String(bytes).replace(/^hosted-only \(outside-temp deletion reporting\): push and let CI run it\r?\n/gm, "");
    });
    child.stdin!.on("error", error => { transportErrors.push(`${name}.stdin: ${error.message}`); });
    child.stdout!.on("error", error => { transportErrors.push(`${name}.stdout: ${error.message}`); });
  }
  bridge.stdout!.pipe(agent.stdin!); agent.stdout!.pipe(bridge.stdin!);
  const mesh = new MeshStore(hubRoot, 64 * 1024, 100);
  const farMesh = new MeshStore(farRoot, 64 * 1024, 100);
  cleanup.push(async () => {
    const failures: unknown[] = [];
    try {
      if (childAlive(agent)) {
        if (process.platform === "linux") agent.kill("SIGCONT");
        await sendChildMessage(agent, { op: "resume" });
      }
    } catch (error) { failures.push(error); }
    try { await terminate(bridge); } catch (error) { failures.push(error); }
    try { await terminate(agent); } catch (error) { failures.push(error); }
    if (transportErrors.length) failures.push(new Error(`bridge fixture transport errors: ${transportErrors.join("; ")}`));
    mesh.closeState(); farMesh.closeState();
    fs.rmSync(root, { recursive: true, force: true }); // only this fixture's temp, after both child exits
    if (failures.length) throw new AggregateError(failures, "bridge fixture cleanup failed");
  });
  await Promise.all([agentMessages.wait("ready"), bridgeMessages.wait("ready")]);
  const lease = () => readHostLease(hubRoot, remoteId);
  const sample = () => lease()?.mirror?.observedAt;
  const sweep = (now = Date.now(), store = mesh) => rootPresenceAlarms(store, owner, owner.id, [], 900_000, now);
  const alarms = () => mesh.read({ topic: "ops.owner", after: 0, limit: 100 }).filter(event => event.kind.startsWith("bridge.lease."));
  const stallAlarms = () => mesh.read({ topic: BRIDGE_STALL_ALARM_TOPIC, after: 0, limit: 100 });
  const healthy = () => {
    expect(stderr).toBe(""); expect(transportErrors).toEqual([]); expect(bridge.exitCode).toBeNull(); expect(agent.exitCode).toBeNull();
    expect([...bridgeMessages.receipts, ...agentMessages.receipts].filter(item => item.kind === "failure" || item.kind === "ended")).toEqual([]);
  };
  return { root, hubRoot, farRoot, mesh, farMesh, bridge, agent, bridgeMessages, agentMessages, lease, sample, sweep,
    alarms, stallAlarms, diagnostics: () => stderr, healthy };
}
// Keep reason: real process liveness/cross-host provenance; unit mocks cannot establish idle renewal.
describe.skipIf(process.platform !== "linux")("mirrored lease bridge alarms via the committed owner sweep", () => {
  it("renews 80s idle, alarms once on buffered transport, recovers in place, rejects foreign/dead evidence", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", owner.id);
    expect(BRIDGE_LEASE_ALARM_AFTER_MS).toBe(45_000); expect(BRIDGE_LEASE_ALARM_AFTER_MS).toBe(3 * MIRROR_FRESH_MS);
    const f = await fixture(true);
    const initial = await changed(f.hubRoot, () => f.sample());
    const stamp = f.lease()!.mirror!.bridgeProcess!;
    expect(stamp).toMatchObject({ link: "far", pid: f.bridge.pid, ...readPhysicalHostIdentity() });
    expect(stamp.pid).not.toBe(process.pid); expect(stamp.pid).not.toBe(f.agent.pid);
    expect(stamp.incarnation).toBe(await processIncarnation(f.bridge.pid!));
    // No writes, manual sync or fixture heartbeat: existing product lease deadline only.
    const started = Date.now();
    for (let sweep = 0; sweep < 4; sweep++) {
      await sleep(20_000); await f.sweep();
      expect(f.sample()).toBeGreaterThan(initial); expect(Date.now() - f.sample()!).toBeLessThan(MIRROR_FRESH_MS);
      expect(f.alarms()).toEqual([]); f.healthy();
    }
    expect(Date.now() - started).toBeGreaterThanOrEqual(80_000);
    const pausedAfter = f.agentMessages.receipts.length;
    f.agent.send({ op: "pause" }); await f.agentMessages.wait("paused", pausedAfter);
    const last = f.sample()!;
    await f.agentMessages.wait("buffered", pausedAfter);
    expect(f.sample()).toBe(last);
    await f.sweep(last + 44_999); await f.sweep(last + 45_000); expect(f.alarms()).toEqual([]);
    const authenticBeforeAlarm = JSON.stringify(f.lease());
    // The process stays alive and genuinely unresponsive for three freshness intervals.
    // Explicit now above/below the boundary checks equality without a timing race; this
    // wait also proves the stalled sequence on real elapsed time, not just clock injection.
    await sleep(Math.max(0, last + 45_001 - Date.now()));
    expect(Date.now() - last).toBeGreaterThanOrEqual(45_001);
    await f.sweep(last + 45_001);
    expect(f.alarms()).toHaveLength(1);
    expect(f.alarms()[0]).toMatchObject({ topic: "ops.owner", kind: "bridge.lease.alarm", to: owner.id,
      data: { link: "far", pid: f.bridge.pid, lastObservedAt: last, sampleAgeMs: 45_001 } });
    expect(f.alarms()[0]!.text).toContain("far"); expect(f.alarms()[0]!.text).toContain("45001");
    await f.sweep(last + 50_000); await f.sweep(last + 75_000);
    const replacement = new MeshStore(f.hubRoot, 64 * 1024, 100);
    try { await f.sweep(last + 45_001, replacement); } finally { replacement.closeState(); }
    expect(f.alarms()).toHaveLength(1); expect(f.sample()).toBe(last);
    expect(JSON.stringify(f.lease())).toBe(authenticBeforeAlarm);
    const resumedAfter = f.agentMessages.receipts.length;
    f.agent.send({ op: "resume" }); await f.agentMessages.wait("resumed", resumedAfter);
    await changed(f.hubRoot, () => {
      const lease = f.lease(), next = lease?.mirror?.observedAt;
      // The first resumed round may be an old queued snapshot: #1250 correctly keeps
      // that authority stale. Recovery requires a genuinely fresh subsequent round.
      return next !== undefined && next > last && lease!.expiresAt > Date.now() ? next : undefined;
    });
    await f.sweep(); await f.sweep(); await f.sweep();
    expect(f.alarms().map(event => event.kind)).toEqual(["bridge.lease.alarm", "bridge.lease.recovered"]);
    expect(f.alarms()[1]).toMatchObject({ to: owner.id, data: { link: "far", pid: f.bridge.pid } }); f.healthy();
    const entry = f.mesh.get(hostKey, { fresh: true })!, authentic = f.lease()!;
    const rejectEvidence = async (label: string, mutate: (lease: FabricHostLease) => void) => {
      const store = new MeshStore(path.join(f.root, label), 64 * 1024, 100);
      try {
        await store.put({ key: hostKey, identity: entry.updatedBy, value: entry.value });
        const lease = structuredClone(authentic); mutate(lease); writeHostLease(store.root, lease);
        await f.sweep(lease.mirror!.observedAt + 45_001, store);
        expect(store.read({ topic: "ops.owner", after: 0 }), label).toEqual([]);
      } finally { store.closeState(); }
    };
    await rejectEvidence("wrong-boot", lease => { lease.mirror!.bridgeProcess!.bootId = "foreign-boot"; });
    await rejectEvidence("foreign-machine", lease => { lease.mirror!.bridgeProcess!.machineId = "foreign-machine"; });
    await rejectEvidence("wrong-process-incarnation", lease => { lease.mirror!.bridgeProcess!.incarnation = "1"; });
    await rejectEvidence("wrong-link", lease => { lease.mirror!.bridgeProcess!.link = "other"; });
    await rejectEvidence("wrong-host-incarnation", lease => { lease.incarnationToken = "other-native-generation"; });
    await rejectEvidence("foreign-owner", lease => { lease.identityId = owner.id; });
    await terminate(f.bridge); await rejectEvidence("dead-process", () => {});
  }, 180_000);
  it("groups current roots by physical link, so a fresh root suppresses only its own link's alarm", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", owner.id);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lease-groups-"));
    const mesh = new MeshStore(root, 64 * 1024, 100);
    cleanup.push(async () => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
    const now = Date.now();
    const presence = (suffix: string) => {
      const id = `session:33333333-3333-4333-8333-33333333333${suffix}`;
      const identity: MeshIdentity = { id, name: "remote", kind: "main" };
      return { record: { format: 1 as const, id, rootId: id, identity, startedAt: now, updatedAt: now,
        expiresAt: now + 600_000, incarnationToken: `native-${suffix}` }, observedAt: now, expiresAt: now + 600_000 };
    };
    const a = presence("1"), b = presence("2"), c = presence("3");
    await new StoreBridgeSide(mesh, "healthy", Date.now, true).mirror({ hosts: [a, b], participants: [] });
    await new StoreBridgeSide(mesh, "stalled", Date.now, true).mirror({ hosts: [c], participants: [] });
    for (const host of [a, c]) {
      const lease = readHostLease(root, host.record.id)!;
      lease.mirror!.observedAt = now - 45_001;
      lease.expiresAt = now - 30_001;
      writeHostLease(root, lease);
    }
    await rootPresenceAlarms(mesh, owner, owner.id, [], 900_000, Date.now());
    const alarms = mesh.read({ topic: "ops.owner", after: 0, limit: 100 });
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ kind: "bridge.lease.alarm", data: { link: "stalled" } });
  });
  it("keeps an idle healthy link alive with repeated notifications and no alarm", async () => {
    const f = await fixture(true, { intervalMs: 20, probeTimeoutMs: 60, stallAfterMs: 150, directionStallAfterMs: 150 });
    await changed(f.hubRoot, () => f.sample());
    await waitFor(() => f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length >= 4);
    expect(f.stallAlarms()).toEqual([]);
    expect(f.bridge.exitCode).toBeNull();
    expect(f.agent.exitCode).toBeNull();
  }, 10_000);

  it("stops watchdog pings and publishes one v1 alarm/reason when the real remote process is frozen", async () => {
    const f = await fixture(true, { intervalMs: 25, probeTimeoutMs: 30, stallAfterMs: 120, directionStallAfterMs: 900 });
    await changed(f.hubRoot, () => f.sample());
    await waitFor(() => f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length >= 3);
    expect(f.stallAlarms()).toEqual([]);
    expect(f.bridge.exitCode).toBeNull();
    f.agent.kill("SIGSTOP");
    const message = await f.bridgeMessages.wait("stall-alarm");
    const notificationsAtAlarm = f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length;
    await sleep(120);
    const notificationsAfterAlarm = f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length;
    expect(notificationsAtAlarm).toBeGreaterThanOrEqual(3);
    expect(notificationsAfterAlarm).toBe(notificationsAtAlarm);
    expect(f.bridge.exitCode).toBeNull();
    expect(f.stallAlarms()).toHaveLength(1);
    const alarm = f.stallAlarms()[0]!;
    expect(alarm).toMatchObject({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall", to: owner.id,
      data: { v: 1, host: "hub", peer: "far", direction: "both" } });
    expect((alarm.data as Record<string, unknown>).reason).toEqual(expect.stringMatching(/^no successful round trip for \d+ ms$/));
    const data = alarm.data as Record<string, unknown>;
    expect(Object.keys(data).sort()).toEqual(["at", "direction", "host", "lastOkToLocal", "lastOkToRemote", "peer", "reason", "stallId", "v"]);
    expect(data.at).toEqual(expect.stringMatching(/^\d{4}-\d{2}-\d{2}T.*Z$/));
    expect(data.lastOkToLocal).toEqual(expect.stringMatching(/^\d{4}-\d{2}-\d{2}T.*Z$/));
    expect(data.lastOkToRemote).toEqual(expect.stringMatching(/^\d{4}-\d{2}-\d{2}T.*Z$/));
    expect(data.stallId).toEqual(expect.stringMatching(/^\d+$/));
    expect(message.alarm).toMatchObject(data);
    const reasonPath = bridgeStallReasonPath(path.join(f.root, "cursor.json"));
    const reason = fs.readFileSync(reasonPath, "utf8");
    expect(reason).toMatch(/^mesh-bridge: stall v1 host=.* peer=far direction=both at=.* lastOkToRemote=.* lastOkToLocal=.* stallId=\d+ reason=no successful round trip for \d+ ms restartCount=unknown\n$/);
    expect(f.stallAlarms()).toHaveLength(1);
  }, 10_000);

  it("exits non-zero with one directional alarm when inbound events stall but outbound delivery still works", async () => {
    const f = await fixture(true, { intervalMs: 20, probeTimeoutMs: 100, stallAfterMs: 1_500,
      directionStallAfterMs: 150, inboundTailStall: true });
    await changed(f.hubRoot, () => f.sample());
    const publishedAfter = f.agentMessages.receipts.length;
    f.agent.send({ op: "publish", event: { topic: "ops.owner", kind: "pr.wake",
      from: { id: remoteId, name: "remote", kind: "main" }, to: owner.id, data: { path: "inbound" } } });
    await f.agentMessages.wait("published", publishedAfter);
    await f.mesh.publish({ topic: "ops.owner", kind: "pr.wake", from: owner, to: remoteId, data: { path: "outbound" } });
    await waitFor(() => f.farMesh.read({ topic: "ops.owner", after: 0 }).some(event => (event.data as { path?: string } | undefined)?.path === "outbound"));
    await f.bridgeMessages.wait("stall-alarm");
    await waitFor(() => f.bridge.exitCode !== null, 3_000);
    expect(f.bridge.exitCode).toBe(1);
    expect(f.bridge.signalCode).toBeNull();
    await waitFor(() => f.agent.exitCode !== null, 3_000);
    expect(f.agent.exitCode).toBe(0);
    expect(f.diagnostics()).not.toMatch(/EPIPE/);
    expect(f.stallAlarms()).toHaveLength(1);
    const alarm = f.stallAlarms()[0]!;
    expect(alarm).toMatchObject({ kind: "bridge-stall", to: owner.id,
      data: { v: 1, peer: "far", direction: "toLocal" } });
    expect((alarm.data as Record<string, unknown>).reason).toEqual(expect.stringMatching(/no toLocal event receipt/));
    const reason = fs.readFileSync(bridgeStallReasonPath(path.join(f.root, "cursor.json")), "utf8");
    expect(reason).toMatch(/^mesh-bridge: stall v1 host=.* peer=far direction=toLocal at=.* lastOkToRemote=.* lastOkToLocal=.* stallId=\d+ reason=no toLocal event receipt .* while toRemote remains current restartCount=unknown\n$/);
    expect(f.stallAlarms()).toHaveLength(1);
  }, 30_000);

  it("detects a directional stall without reporting callbacks and ignores wall-clock jumps", async () => {
    const f = await fixture(true, { callbacks: false, intervalMs: 20, probeTimeoutMs: 100,
      stallAfterMs: 1_500, directionStallAfterMs: 500, inboundTailStall: true });
    await changed(f.hubRoot, () => f.sample());
    const publishedAfter = f.agentMessages.receipts.length;
    f.agent.send({ op: "publish", event: { topic: "ops.owner", kind: "pr.wake",
      from: { id: remoteId, name: "remote", kind: "main" }, to: owner.id, data: { path: "inbound-no-callbacks" } } });
    await f.agentMessages.wait("published", publishedAfter);
    await f.mesh.publish({ topic: "ops.owner", kind: "pr.wake", from: owner, to: remoteId, data: { path: "outbound-no-callbacks" } });
    await waitFor(() => f.farMesh.read({ topic: "ops.owner", after: 0 }).some(event =>
      (event.data as { path?: string } | undefined)?.path === "outbound-no-callbacks"));

    // A forward wall-clock jump cannot make the direction appear stalled early.
    f.bridge.send({ op: "wall-clock-offset", offsetMs: 600_000 });
    await sleep(120);
    expect(f.bridge.exitCode).toBeNull();
    // A backward jump cannot mask the elapsed monotonic stall deadline.
    f.bridge.send({ op: "wall-clock-offset", offsetMs: -600_000 });
    await waitFor(() => f.bridge.exitCode !== null, 3_000);
    expect(f.bridge.exitCode).toBe(1);
    expect(f.bridge.signalCode).toBeNull();
    expect(f.bridgeMessages.receipts.filter(item => item.kind === "watchdog" || item.kind === "stall-alarm")).toEqual([]);
    await waitFor(() => f.agent.exitCode !== null, 3_000);
    expect(f.agent.exitCode).toBe(0);
    expect(f.diagnostics()).not.toMatch(/EPIPE/);
  }, 10_000);

  it("does not treat a quiet but healthy direction as a stall while outbound events are acknowledged", async () => {
    const f = await fixture(true, { intervalMs: 20, probeTimeoutMs: 60, stallAfterMs: 150, directionStallAfterMs: 150 });
    await changed(f.hubRoot, () => f.sample());
    await f.mesh.publish({ topic: "ops.owner", kind: "pr.wake", from: owner, to: remoteId, data: { path: "outbound" } });
    await waitFor(() => f.farMesh.read({ topic: "ops.owner", after: 0 }).some(event => (event.data as { path?: string } | undefined)?.path === "outbound"));
    await sleep(250);
    expect(f.stallAlarms()).toEqual([]);
    expect(f.bridge.exitCode).toBeNull();
    expect(f.agent.exitCode).toBeNull();
    expect(f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length).toBeGreaterThanOrEqual(3);
  }, 10_000);

  it("alarms on a 65-second source-cursor backlog without stalling or withholding the watchdog", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", owner.id);
    const f = await fixture(true, { intervalMs: 1_000, probeTimeoutMs: 500, stallAfterMs: 1_500,
      directionStallAfterMs: 180_000, stopBridgeOnShutdown: true });
    await changed(f.hubRoot, () => f.sample()); // ready precedes the async cursor initialization in this fixture
    const now = Date.now(), ownerIncarnation = `fixture:${owner.id}`;
    const ownerHostKey = `topology/hosts/${createHash("sha256").update(owner.id).digest("hex")}`;
    const ownerParticipantKey = `topology/participants/${createHash("sha256").update(owner.id).digest("hex")}`;
    await f.mesh.put({ key: ownerHostKey, identity: owner, value: { format: 1, id: owner.id, rootId: owner.id,
      identity: owner, startedAt: now, updatedAt: now, expiresAt: now + 600_000, incarnationToken: ownerIncarnation } });
    writeHostLease(f.hubRoot, { id: owner.id, rootId: owner.id, identityId: owner.id, startedAt: now,
      updatedAt: now, expiresAt: now + 600_000, incarnationToken: ownerIncarnation });
    writeParticipantFile(f.hubRoot, { key: ownerParticipantKey, version: 1, updatedAt: now, updatedBy: owner,
      value: { format: 1, id: owner.id, kind: "root", rootId: owner.id, ownerHostId: owner.id,
        ownerIdentityId: owner.id, ownerIncarnation, name: "owner", label: "OWNER", status: "idle",
        runner: "pi", transport: "host", capabilities: ["followUp", "steer"], sessionId: "owner",
        startedAt: now, updatedAt: now, controlProtocol: "v1" } });

    const blockedAfter = f.bridgeMessages.receipts.length;
    f.bridge.send({ op: "pause-source-read" });
    await f.bridgeMessages.wait("source-read-paused", blockedAfter);
    const beforeWatchdogs = f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length;
    const command = (commandId: string) => ({ topic: "fabric.control.command", kind: "followUp", from: owner,
      to: remoteId, data: { version: 1, commandId, targetId: remoteId, operation: "followUp", replyTo: owner.id,
        ownerIncarnation: "remote-native-generation", requestedAt: Date.now(), deadlineAt: Date.now() + 120_000,
        message: commandId } });
    const old = await f.mesh.publish(command("cursor-backlog-old"));
    await f.bridgeMessages.wait("source-read-blocked", blockedAfter);
    expect(f.farMesh.read({ topic: "fabric.control.command", after: 0, limit: 10 })).toEqual([]);
    const heldAt = Date.now();
    await sleep(65_000);
    expect(Date.now() - heldAt).toBeGreaterThanOrEqual(65_000);
    expect(f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length).toBeGreaterThan(beforeWatchdogs);
    expect(f.stallAlarms()).toEqual([]);
    expect(f.bridge.exitCode).toBeNull(); expect(f.agent.exitCode).toBeNull();

    f.bridge.send({ op: "resume-source-read" });
    await f.bridgeMessages.wait("source-read-resumed");
    await waitFor(() => f.farMesh.read({ topic: "fabric.control.command", after: 0, limit: 10 })
      .some(event => (event.data as { commandId?: string }).commandId === "cursor-backlog-old"));
    const oldAtDestination = f.farMesh.read({ topic: "fabric.control.command", after: 0, limit: 10 })
      .find(event => (event.data as { commandId?: string }).commandId === "cursor-backlog-old")!;
    expect(oldAtDestination.createdAt - old.createdAt).toBeGreaterThanOrEqual(65_000);
    expect(oldAtDestination.createdAt - old.createdAt).toBeLessThan(120_000);
    await waitFor(() => f.mesh.read({ topic: "ops.owner", after: 0, limit: 100 })
      .some(event => event.kind === "bridge.delivery-age.alarm"));
    const alarm = f.mesh.read({ topic: "ops.owner", after: 0, limit: 100 })
      .find(event => event.kind === "bridge.delivery-age.alarm")!;
    expect(alarm).toMatchObject({ to: owner.id, data: { direction: "toRemote", reason: "delivery-age" } });

    await f.mesh.publish(command("cursor-backlog-fresh"));
    await waitFor(() => f.farMesh.read({ topic: "fabric.control.command", after: 0, limit: 10 })
      .some(event => (event.data as { commandId?: string }).commandId === "cursor-backlog-fresh"));
    await waitFor(() => f.mesh.read({ topic: "ops.owner", after: 0, limit: 100 })
      .some(event => event.kind === "bridge.delivery-age.recovered"));
    const ageEvents = f.mesh.read({ topic: "ops.owner", after: 0, limit: 100 })
      .filter(event => event.kind === "bridge.delivery-age.alarm" || event.kind === "bridge.delivery-age.recovered");
    expect(ageEvents.map(event => event.kind)).toEqual(["bridge.delivery-age.alarm", "bridge.delivery-age.recovered"]);
    expect(f.stallAlarms()).toEqual([]);
    expect(f.bridge.exitCode).toBeNull(); expect(f.agent.exitCode).toBeNull();
    expect(f.bridgeMessages.receipts.filter(item => item.kind === "watchdog").length).toBeGreaterThan(beforeWatchdogs);

    await terminate(f.bridge);
    const telemetry = f.bridgeMessages.receipts.filter(item => item.kind === "bridge-log").map(item => String(item.line));
    expect(telemetry.some(line => /^bridge delivery-age v1 peer=far minute=.*toRemote count=.*deliveryMaxMs=\d+.*cursorLagSequences=\d+ cursorLagSeconds=\d+/.test(line))).toBe(true);
    const staleLine = telemetry.find(line => line.includes("toRemote") && /cursorLagSeconds=(6[0-9]|[7-9][0-9]|1[01][0-9])/.test(line));
    expect(staleLine).toBeDefined();
    expect(staleLine).toMatch(/cursorLagSequences=1 cursorLagSeconds=(6[0-9]|[7-9][0-9]|1[01][0-9])/);
    console.log(`REAL_BRIDGE_TELEMETRY_SAMPLE ${staleLine}`);
    expect(old.sequence).toBe(1);
  }, 100_000);

  it("carries bridge alarms only to a canonical session, never a factory/broadcast address", () => {
    expect(isBridgedTopic({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall", to: owner.id })).toBe(true);
    expect(isBridgedTopic({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "other", to: owner.id })).toBe(false);
    for (const to of [undefined, "factory", "org", "topic:fabric.alarm.bridge-stall", "session:not-a-uuid"]) {
      expect(isBridgedTopic({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall", to })).toBe(false);
    }
    for (const kind of ["bridge.lease.alarm", "bridge.lease.recovered", "bridge.delivery-age.alarm", "bridge.delivery-age.recovered"]) {
      expect(isBridgedTopic({ topic: "ops.owner", kind, to: owner.id })).toBe(true);
      for (const to of [undefined, "factory", "org", "topic:ops.owner", "session:not-a-uuid"]) {
        expect(isBridgedTopic({ topic: "ops.owner", kind, to })).toBe(false);
      }
    }
  });
  it("keeps an empty running link lease-free and exempt across repeated future sweeps", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", owner.id);
    const f = await fixture(false);
    await changed(f.root, () => fs.existsSync(path.join(f.root, "cursor.json")) ? true : undefined);
    for (const offset of [0, 45_001, 90_002, 135_003]) await f.sweep(Date.now() + offset);
    expect(f.lease()).toBeUndefined(); expect(f.mesh.listAll("topology/hosts/", { fresh: true })).toEqual([]);
    expect(f.alarms()).toEqual([]); f.healthy();
    // Synthetic time proves only empty exemption; populated idle above uses real time.
  }, 20_000);
});
