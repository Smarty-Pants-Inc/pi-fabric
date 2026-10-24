import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const settle = async () => { await sleep(40); await new Promise<void>(resolve => setImmediate(resolve)); };
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const native = async (store: MeshStore, name: string) => {
  const now = Date.now();
  const id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id, rootId: id, identity, incarnationToken: name, startedAt: now, updatedAt: now, expiresAt: now + 15_000 };
  await store.put({ key: key("hosts", id), value: host, identity });
  const entry: MeshStateEntry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name, label: name, status: "idle", runner: "pi", transport: "host", capabilities: [],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } };
  writeParticipantFile(store.root, entry);
  return { id, identity, host, entry };
};

// Keep reason: recorded #13645 failure across SQLite hub / file agent trust boundary.
it.each(["none", "mirror-once", "subscription-once", "subscription-held", "subscription-starved"] as const)("continues mirror ACKs after link-up (%s)", async busy => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-renewal-regress-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 500, { stateBackend: "sqlite", lockTimeoutMs: 100 });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 500, { stateBackend: "file" });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const local = new StoreBridgeSide(hub, "far"), agent = new StoreBridgeSide(far, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const requests: Array<{ id: number; op: string }> = [];
  const responses: Array<{ id?: number; ok?: boolean; notification?: string; kind?: string; error?: string }> = [];
  input.on("data", chunk => { for (const line of String(chunk).trim().split("\n")) requests.push(JSON.parse(line)); });
  output.on("data", chunk => { for (const line of String(chunk).trim().split("\n")) responses.push(JSON.parse(line)); });
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, 30_000);
  cleanups.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  const logs: string[] = [];
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(hub.root, "bridge-cursor.json"), stopMs: 100, log: line => logs.push(line) });
  const lead = await native(hub, "hub-native"), lane = await native(far, "far-native");
  await bridge.start(); // CLI path: first mirror/ACK precedes run()'s subscription setup.
  if (busy === "mirror-once") {
    vi.spyOn(local, "mirror").mockRejectedValueOnce(Object.assign(new Error("seeded mesh busy"), { code: "FABRIC_MESH_LOCK_TIMEOUT" }));
  }
  const { DatabaseSync } = await import("node:sqlite");
  const raw = new DatabaseSync(path.join(hub.root, "state.db"));
  raw.exec("PRAGMA busy_timeout = 0");
  let held = busy === "subscription-once" || busy === "subscription-held";
  const release = () => { if (held) { raw.exec("ROLLBACK"); held = false; } };
  cleanups.push(() => { release(); raw.close(); });
  if (held) raw.exec("BEGIN IMMEDIATE");
  if (busy === "subscription-once") setTimeout(release, 50);
  if (busy === "subscription-starved") {
    // Adversarial but real SQLite contention at each zero-wait observation fence.
    // Leave the writer available between probes: ordinary mirror commits CAN succeed.
    const fence = hub.withStateWriteFence.bind(hub);
    vi.spyOn(hub, "withStateWriteFence").mockImplementation(<T>(operation: () => T): T => {
      raw.exec("BEGIN IMMEDIATE");
      try { return fence(operation); } finally { raw.exec("ROLLBACK"); }
    });
  }
  let failure: unknown;
  const running = bridge.run().catch(error => { failure = error; });
  cleanups.push(async () => { await bridge.stop(); await running; });
  expect(Number.isFinite(remote.lastPresenceRenewalAt), "start() received the initial mirror ACK").toBe(true);
  await settle();
  const samples = [];
  try {
    // Normal event source: real native lease-file replacements -> fs.watch -> stdio
    // change frames. Fake only time; settle() is a bounded window for real I/O delivery.
    for (let beat = 1; beat <= 12; beat++) {
      await vi.advanceTimersByTimeAsync(5_000);
      for (const [store, owner] of [[hub, lead], [far, lane]] as const) {
        writeHostLease(store.root, { id: owner.id, rootId: owner.id, identityId: owner.id,
          startedAt: owner.host.startedAt, incarnationToken: owner.host.incarnationToken,
          updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
      }
      await settle();
      samples.push({ beat, age: Date.now() - remote.lastPresenceRenewalAt,
        mirrorRequests: requests.filter(request => request.op === "mirror").length,
        notifications: responses.filter(response => response.notification === "change").length });
    }
    console.log(JSON.stringify({ busy, samples, logs, failure: String(failure),
      requests: requests.map(({ id, op }) => ({ id, op })),
      replies: responses.filter(response => response.id !== undefined).map(({ id, ok, error }) => ({ id, ok, error })) }));
    expect(failure).toBeUndefined();
    expect(samples.at(-1)!.age).toBeLessThan(15_000);
    expect(samples.at(-1)!.mirrorRequests).toBeGreaterThan(1);
    expect(samples.at(-1)!.notifications).toBeGreaterThan(0);
    if (busy !== "subscription-held") {
      expect(readHostLeases(hub.root).get(lane.id)!.expiresAt).toBeGreaterThan(Date.now());
      expect(readHostLeases(far.root).get(lead.id)!.expiresAt).toBeGreaterThan(Date.now());
    }
  } finally {
    release();
    await vi.advanceTimersByTimeAsync(10_000);
    await settle();
    if (busy === "subscription-held") {
      expect(local.holds(lane.id)).toBe(true);
      expect(remote.lastPresenceRenewalAt).toBeGreaterThan(Date.now() - 15_000);
    }
  }
}, 45_000);
