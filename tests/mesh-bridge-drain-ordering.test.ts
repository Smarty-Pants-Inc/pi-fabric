import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, type BridgePresence, type BridgeSide } from "../src/mesh/bridge.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); vi.useRealTimers(); }
});

const identity = (name: string): MeshIdentity => ({ id: `session:${name}-00000000`, name: "main", kind: "main" });
const presence = (who: MeshIdentity): BridgePresence => {
  const now = Date.now();
  return { hosts: [{ record: { format: 1, id: who.id, rootId: who.id, identity: who,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 }, observedAt: now, expiresAt: now + 120_000 }],
    participants: [], reserved: [who.id] };
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-drain-order-"));
  const hub = identity("hub"), far = identity("far");
  const empty = { hosts: [], participants: [], reserved: [] };
  const side = (): BridgeSide => ({ latestSequence: () => Promise.resolve(0),
    read: after => Promise.resolve({ events: [], through: after }), presence: () => Promise.resolve(empty),
    mirror: () => Promise.resolve(), publish: () => Promise.resolve({ sequence: 1 }),
    bridgedIds: after => Promise.resolve({ ids: [], through: after, done: true }) });
  const local: BridgeSide = { ...side(), presence: () => Promise.resolve(presence(hub)),
    owned: () => Promise.resolve(presence(far)), holds: () => true };
  const remote: BridgeSide = { ...side(), presence: () => Promise.resolve(presence(far)) };
  const cursorPath = path.join(root, "cursor.json"), lines: string[] = [];
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
    log: line => lines.push(line) });
  cleanups.push(async () => { await bridge.stop(); fs.rmSync(root, { recursive: true, force: true }); });
  const events: MeshEvent[] = ["fabric.control.command", "fabric.control.ack"].map((topic, index) => ({
    id: `control-${index}`, sequence: index + 1, topic, kind: index ? "ack" : "followUp",
    createdAt: Date.now(), from: far, to: hub.id, verification: "mesh", data: {
      version: 1, commandId: "drain-order", targetId: far.id, operation: "followUp", replyTo: far.id,
      destinationRemoteHost: "hub", accepted: true,
    },
  }));
  return { bridge, local, remote, cursorPath, events, lines };
};

describe("bridge drain instrumentation ordering", () => {
  // Keep: PR #1648 changed command/ACK scheduling at the durable publication boundary.
  it("keeps the baseline microtask order through membership refresh, command/ACK checkpoints and completion", async () => {
    const f = fixture();
    await f.bridge.start();
    const trace: string[] = [], delivered: string[] = [];
    let tick = 0, generation = 0;
    const pulse = () => {
      tick = 0;
      const current = ++generation;
      const next = () => { if (current === generation && tick < 64) { tick++; queueMicrotask(next); } };
      queueMicrotask(next);
    };
    const mark = (label: string) => trace.push(`${label}:${tick}`);
    const immediate = globalThis.setImmediate;
    vi.spyOn(globalThis, "setImmediate").mockImplementation((callback, ...args) => immediate(() => {
      pulse(); // Count from the existing yield's callback, not from the preceding event-loop turn.
      callback(...args);
    }));
    f.remote.read = after => {
      pulse(); mark(`read-${after}`);
      return Promise.resolve({ events: f.events.filter(event => event.sequence > after), through: 2 });
    };
    f.remote.latestSequence = () => { mark("head"); return Promise.resolve(2); };
    const nativePresence = f.local.presence;
    f.local.presence = () => { mark("presence"); return nativePresence(); };
    const owned = f.local.owned!;
    let fresh = false;
    f.local.owned = () => {
      mark("owned");
      return fresh ? owned() : Promise.resolve({ hosts: [], participants: [] });
    };
    f.local.mirrored = () => true;
    vi.spyOn(f.bridge, "syncPresence").mockImplementation(() => {
      mark("membership"); fresh = true;
      return Promise.resolve();
    });
    f.local.publish = event => {
      mark(event.topic); delivered.push(event.topic);
      pulse();
      return Promise.resolve({ sequence: delivered.length, createdAt: Date.now() });
    };
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (to === f.cursorPath) mark(`checkpoint-${JSON.parse(fs.readFileSync(to, "utf8")).toLocal.after}`);
    });
    expect(await f.bridge.step()).toEqual({ toRemote: 0, toLocal: 2, dropped: 0 });
    mark("done"); generation++;
    expect(delivered).toEqual(["fabric.control.command", "fabric.control.ack"]);
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toLocal.after).toBe(2);
    // Captured on main with the same BridgeSide promises; no wall-clock deadline is involved.
    expect(trace).toEqual([
      "read-0:0", "head:1", "presence:2", "owned:2", "membership:7", "presence:9", "owned:9",
      "fabric.control.command:15", "checkpoint-1:4", "fabric.control.ack:1", "checkpoint-2:4",
      "checkpoint-2:1", "read-2:0", "done:3",
    ]);
  });

  // Keep: a fatal notification while a destination receipt is pending is not cheap to reach live.
  it("closes the drain window and pending delivery alarms when run fails, without stop", async () => {
    vi.useFakeTimers({ now: 1_800_000_000_000 });
    const f = fixture();
    await f.bridge.start(); // Start with empty logs; later controls must not become the initial cursor.
    let fail!: (error: Error) => void;
    let release!: (receipt: { sequence: number; createdAt: number }) => void;
    let published = false;
    const unsubscribed: string[] = [];
    f.local.subscribeChanges = (_changed, failed) => {
      fail = failed!;
      return () => { unsubscribed.push("local"); };
    };
    f.remote.subscribeChanges = () => () => { unsubscribed.push("remote"); };
    f.remote.read = after => Promise.resolve({ events: f.events.filter(event => event.sequence > after), through: 2 });
    f.remote.latestSequence = () => Promise.resolve(2);
    f.local.publish = () => {
      if (published) return Promise.resolve({ sequence: 2, createdAt: Date.now() });
      published = true;
      return new Promise(resolve => { release = resolve; });
    };
    const fatal = new Error("notification stream failed");
    const outcome = f.bridge.run().then(() => undefined, error => error);
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(published).toBe(true);
      expect(vi.getTimerCount()).toBeGreaterThanOrEqual(2); // Receipt alarm and drain window, plus source expiry.
      expect(f.lines.some(line => line.startsWith("bridge drain-time"))).toBe(false);
      fail(fatal);
      expect(await outcome).toBe(fatal);
      expect(unsubscribed).toEqual(["local", "remote"]);
      expect(vi.getTimerCount()).toBe(0);
      expect(f.lines.filter(line => line.startsWith("bridge drain-time"))).toHaveLength(2);
      const finalLines = [...f.lines];
      await vi.advanceTimersByTimeAsync(120_000);
      expect(f.lines).toEqual(finalLines); // No post-failure flush or ACK-age alarm.
    } finally {
      fail?.(fatal);
      release?.({ sequence: 1, createdAt: Date.now() });
      await vi.runAllTimersAsync();
      await outcome;
    }
    expect(vi.getTimerCount()).toBe(0); // An in-flight continuation cannot reopen the monitor.
  });
});
