// KEEP: #16378 recorded failure + data safety. Actual source/receiver stores and
// stdio RPC; async mirror wait is deliberately worker-free. No native COMMIT claim.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeDispatchDeferredError, BridgeStartDeadlineError, MeshBridge, RemoteBridgeSide, serveBridgeAgent,
  StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const removals = vi.hoisted(() => [] as string[]);
vi.mock("../src/topology/host-leases.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/topology/host-leases.js")>();
  return { ...actual, removeHostLeaseIf: (root: string, id: string, owns: Parameters<typeof actual.removeHostLeaseIf>[2]) => {
    const lease = actual.readHostLease(root, id);
    if (!lease || !owns(lease)) return false;
    removals.push(`${root}:${id}`); return true;
  } };
});
const roots: string[] = [];
const closers: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks(); vi.useRealTimers();
  expect(removals).toEqual([]); removals.length = 0;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(yes => { resolve = yes; }); return { promise, resolve }; };
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const native = async (store: MeshStore, name: string) => {
  const now = Date.now(), id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name, kind: "main", sessionId: name };
  await store.put({ key: key("hosts", id), identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000 } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: `fixture:${id}`, kind: "root", name,
    label: name, status: "idle", runner: "pi", transport: "host", capabilities: [], sessionId: name,
    startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return identity;
};
const fixture = async (pageSize = 100) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-dispatch-progress-")); roots.push(root);
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, pageSize, { stateBackend: "file" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 100, { stateBackend: "file" });
  closers.push(() => { hub.closeState(); far.closeState(); });
  const lead = await native(hub, "dispatch-hub"), target = await native(far, "dispatch-far");
  const local = new StoreBridgeSide(hub, "far"), agent = new StoreBridgeSide(far, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const requests: Array<{ id: number; op: string }> = [];
  input.on("data", chunk => { for (const line of String(chunk).trim().split("\n")) requests.push(JSON.parse(line)); });
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  await remote.hello();
  const logs: string[] = [];
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
    stopMs: 1_000, log: line => logs.push(line) });
  await bridge.start();
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toRemote: { after: number }; toLocal: { after: number } };
  const publish = (text: string) => hub.publish({ topic: "fleet.work.test", kind: "probe", from: lead, to: target.id, text });
  return { hub, far, lead, target, local, agent, remote, bridge, requests, logs, cursor, publish };
};

describe("dispatch refusal source retention and progress", () => {
  it("retains only the uncommitted batch suffix across an async healthy mirror, then delivers exact once without notifications", async () => {
    const f = await fixture();
    const source = [await f.publish("private-prefix"), await f.publish("private-suffix-1"), await f.publish("private-suffix-2")];
    // Suppress notifications: the existing pending-work retry, NOT a new wake engine, must recover.
    f.local.subscribeChanges = () => () => undefined;
    f.remote.subscribeChanges = async () => () => undefined;
    const entered = deferred(), release = deferred(), deferredLog = deferred();
    const sample = await f.local.presence();
    const mirror = f.agent.mirror.bind(f.agent);
    let holding = false;
    vi.spyOn(f.agent, "mirror").mockImplementation(async (...args) => {
      if (holding) { holding = false; entered.resolve(); await release.promise; }
      return mirror(...args);
    });
    const batch = f.remote.publishBatchWithReceipt!;
    let first = true, pendingMirror: Promise<void> | undefined;
    vi.spyOn(f.remote, "publishBatchWithReceipt").mockImplementation(async items => {
      if (!first) return batch(items);
      first = false;
      const prefix = await batch(items.slice(0, 1));
      holding = true;
      pendingMirror = f.remote.mirror(sample); // composite is settled; real async mirror now wins the next seam
      return prefix;
    });
    const serviceEntered = new Map<string, number>();
    const receiverBatch = f.agent.publishBatch.bind(f.agent);
    vi.spyOn(f.agent, "publishBatch").mockImplementation(async (...args) => {
      for (const { event } of args[0]) serviceEntered.set(event.text!, performance.now());
      await new Promise(resolve => setTimeout(resolve, 120));
      return receiverBatch(...args);
    });
    const single = f.agent.publishWithReceipt.bind(f.agent);
    vi.spyOn(f.agent, "publishWithReceipt").mockImplementation(async (...args) => {
      serviceEntered.set(args[0].text!, performance.now());
      await new Promise(resolve => setTimeout(resolve, 120)); // SENT receiver service, not scheduling deferral
      return single(...args);
    });
    let refusedAt = 0;
    const log = f.bridge.options.log!;
    f.bridge.options.log = line => {
      log(line);
      if (line.startsWith("bridge dispatch deferred (toRemote)")) { refusedAt ||= performance.now(); deferredLog.resolve(); }
    };
    const running = f.bridge.run();
    closers.push(async () => { release.resolve(); await f.bridge.stop(); await running; });
    try {
      await entered.promise; await deferredLog.promise;
      expect(f.cursor().toRemote.after).toBe(source[0]!.sequence);
      expect(f.far.read({ topic: "fleet.work.test" }).map(event => event.text)).toEqual(["private-prefix"]);
      // Worker is free during this wait, but protected traffic still pays the ACCEPTED C1 latency.
      const since = performance.now();
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(f.cursor().toRemote.after).toBe(source[0]!.sequence);
      release.resolve(); await pendingMirror;
      await vi.waitFor(() => expect(f.cursor().toRemote.after).toBe(source[2]!.sequence), { timeout: 4_000 });
      expect(performance.now() - since).toBeGreaterThanOrEqual(150);
      const delivered = f.far.read({ topic: "fleet.work.test" });
      expect(delivered.map(event => event.text)).toEqual(["private-prefix", "private-suffix-1", "private-suffix-2"]);
      expect(delivered.map(event => (event.data as BridgePublish["data"]).bridge.id)).toEqual(source.map(event => event.id));
      expect(f.logs.some(line => line.includes("bridge mirror degraded"))).toBe(false);
      const samples = f.logs.filter(line => line.startsWith("bridge dispatch v1 dir=toRemote"));
      expect(samples).toHaveLength(3);
      expect(samples[0]).toMatch(/schedulingDelayMs=0$/);
      for (const [index, line] of samples.slice(1).entries()) {
        const delayMs = Number(/schedulingDelayMs=(\d+)/.exec(line)?.[1]);
        expect(delayMs).toBeGreaterThanOrEqual(150);
        // The receiver deliberately waits 120ms AFTER dispatch. It must not inflate
        // this message's scheduling sample (waiting behind earlier messages is real).
        expect(delayMs).toBeLessThanOrEqual(serviceEntered.get(`private-suffix-${index + 1}`)! - refusedAt + 30);
      }
      expect(f.logs.join("\n")).not.toContain("private-");
      console.info(`[dispatch-async-healthy] ${samples.join("; ")}`);
    } finally { release.resolve(); }
  });

  it("RUN loop renews a 15 s projection during >=30 s sustained outbound drain and makes inbound progress despite refresh refusals (VIRTUAL TIME)", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(1);
    const messages: MeshEvent[] = [];
    for (let index = 0; index < 36; index++) messages.push(await f.publish(`virtual-drain-${index}`));
    const inbound = await f.far.publish({ topic: "fleet.work.test", kind: "probe", from: f.target, to: f.lead.id, text: "inbound-eventual" });
    const started = now;
    const renewals = [readHostLease(f.far.root, f.lead.id)!.expiresAt];
    const publish = f.agent.publishWithReceipt.bind(f.agent);
    let refusals = 0;
    vi.spyOn(f.agent, "publishWithReceipt").mockImplementation(async (...args) => {
      // Every append is a real stdio protected call. Let toLocal's overlapping presence
      // attempt lose the reciprocal gate while it is in flight; it must NOT freshen anything.
      const before = f.remote.lastPresenceRenewalAt;
      await expect(f.bridge.syncPresence()).rejects.toBeInstanceOf(BridgeDispatchDeferredError);
      expect(f.remote.lastPresenceRenewalAt).toBe(before);
      refusals++;
      now += 1_000;
      const expiry = readHostLease(f.far.root, f.lead.id)!.expiresAt;
      expect(expiry, "projection renewed BEFORE every 15 s expiry").toBeGreaterThan(now);
      if (renewals.at(-1) !== expiry) renewals.push(expiry);
      return publish(...args);
    });
    // Real bidirectional run-loop arbitration, with notifications suppressed so the
    // retained drain/inline renewal must suffice. Only wall time is virtual, not V8/SQLite.
    f.local.subscribeChanges = () => () => undefined;
    f.remote.subscribeChanges = async () => () => undefined;
    const running = f.bridge.run();
    closers.push(async () => { await f.bridge.stop(); await running; });
    await vi.waitFor(() => {
      expect(f.cursor().toRemote.after).toBeGreaterThanOrEqual(messages.at(-1)!.sequence);
      expect(f.cursor().toLocal.after).toBeGreaterThanOrEqual(inbound.sequence);
    }, { timeout: 5_000 });
    await f.bridge.stop(); await running;
    expect(now - started).toBe(36_000);
    expect(refusals).toBe(36);
    expect(renewals.length).toBeGreaterThanOrEqual(3);
    expect(f.far.read({ topic: "fleet.work.test", limit: 100 }).filter(event => event.verification === "bridge")).toHaveLength(36);
    expect(f.hub.read({ topic: "fleet.work.test", limit: 100 }).filter(event => event.verification === "bridge")).toMatchObject([
      { text: "inbound-eventual", data: { bridge: { id: inbound.id } } },
    ]);
    expect(f.cursor().toRemote.after).toBeGreaterThanOrEqual(messages.at(-1)!.sequence);
    expect(f.cursor().toLocal.after).toBeGreaterThanOrEqual(inbound.sequence);
    expect(f.logs.some(line => line.startsWith("stall "))).toBe(false);
    console.info(`[dispatch-drain-VIRTUAL] durationMs=${now - started} refusals=${refusals} projectionExpiries=${JSON.stringify(renewals)}`);
  });

  it("start diagnostics classify a local scheduling refusal without mirror-degraded/WAL backoff", async () => {
    const f = await fixture();
    let attempts = 0;
    const started = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => started + Math.max(0, attempts - 1));
    vi.spyOn(f.bridge, "start").mockImplementation(async () => { attempts++; throw new BridgeDispatchDeferredError("traffic-in-flight"); });
    await expect(f.bridge.startWithBackoff(1)).rejects.toBeInstanceOf(BridgeStartDeadlineError);
    expect(f.logs.some(line => line.startsWith("start: bridge dispatch deferred; retrying in 1 ms"))).toBe(true);
    expect(f.logs.join("\n")).not.toContain("bridge mirror degraded");
  });
});
