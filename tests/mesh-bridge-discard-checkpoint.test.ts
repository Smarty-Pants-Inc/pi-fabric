import { createHash } from "node:crypto";
import { performance as monotonicClock } from "node:perf_hooks";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BRIDGE_DIRECTION_STALL_AFTER_MS,
  BRIDGE_PAGE_BYTES,
  MeshBridge,
  StoreBridgeSide,
  type BridgeStallAlarm,
  type MeshBridgeOptions,
} from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [];
const cleanups: Array<() => Promise<void>> = [];
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const sourceIdentity: MeshIdentity = { id: "session:hub-00000000", name: "hub", kind: "main" };
const key = (family: "hosts" | "participants", id: string) =>
  `topology/${family}/${createHash("sha256").update(id).digest("hex")}`;

function fixture(readLimit = 500) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-discard-checkpoint-"));
  roots.push(root);
  const source = new MeshStore(path.join(root, "hub"), 64 * 1024, readLimit);
  const destination = new MeshStore(path.join(root, "far"), 64 * 1024, readLimit);
  const local = new StoreBridgeSide(source, "far");
  const remote = new StoreBridgeSide(destination, "hub");
  const cursorPath = path.join(root, "cursor.json");
  cleanups.push(async () => { source.closeState(); destination.closeState(); });
  return { root, source, destination, local, remote, cursorPath };
}

type Fixture = ReturnType<typeof fixture>;
type SavedCursor = { toRemote: { after: number; offset?: number }; toLocal: { after: number; offset?: number } };

function seedCursor(f: Fixture): void {
  fs.writeFileSync(f.cursorPath, JSON.stringify({ format: 1, local: "hub", remote: "far",
    toRemote: { after: 0, mark: 0 }, toLocal: { after: 0, mark: 0 } }));
}

function readCursor(f: Fixture): SavedCursor {
  return JSON.parse(fs.readFileSync(f.cursorPath, "utf8")) as SavedCursor;
}

async function seedEvents(f: Fixture, count: number, addressedTo?: string | ((index: number) => string)) {
  const inputs = Array.from({ length: count }, (_, index) => ({ topic: "ops.owner", kind: "pr.wake",
    from: sourceIdentity,
    to: typeof addressedTo === "function" ? addressedTo(index) : addressedTo ?? `session:unknown-${String(index).padStart(8, "0")}`,
    data: { path: "discard-checkpoint", index } }));
  for (let index = 0; index < inputs.length;) {
    const committed = await f.source.publishBatch(inputs.slice(index, index + 256));
    expect(committed.length).toBeGreaterThan(0);
    index += committed.length;
  }
  const events = f.source.read({ after: 0, limit: count });
  expect(events).toHaveLength(count);
  return events;
}

function launch(f: Fixture, overrides: Partial<MeshBridgeOptions> = {}) {
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: f.local, remote: f.remote,
    cursorPath: f.cursorPath, stopMs: 50, watchdogIntervalMs: 1, watchdogProbeTimeoutMs: 100,
    directionStallAfterMs: BRIDGE_DIRECTION_STALL_AFTER_MS, ...overrides });
  let failure: unknown;
  const running = bridge.run().then(() => undefined, error => { failure = error; });
  cleanups.push(async () => { await bridge.stop(); await running; });
  return { bridge, running, failure: () => failure };
}

async function until(check: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 2));
  expect(check(), "condition did not become true before the test deadline").toBe(true);
}

async function waitForGate(entered: Promise<void>): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("controlled slow await was not reached")), 5_000);
    void entered.then(() => { clearTimeout(timer); resolve(); }, error => { clearTimeout(timer); reject(error); });
  });
}

// #1651 shares one post-miss sample across a page, so 400 presence calls are no
// longer a reachable mid-page hook. Drive explicit membership notifications at a
// retained event instead; discarded events advance the watchdog's logical clock.
function membershipNotifications(f: Fixture): (line: string) => void {
  let changed: (() => void) | undefined;
  f.local.subscribeChanges = () => () => undefined;
  f.remote.subscribeChanges = notify => {
    changed = () => notify("presence");
    return () => { changed = undefined; };
  };
  return line => {
    if (line.includes("bridge membership pending")) setImmediate(() => changed?.());
  };
}

function slowMembershipRefreshes(f: Fixture) {
  const notify = membershipNotifications(f);
  let logicalMs = 0;
  let discarded = 0;
  let refreshes = 0;
  let startup = true;
  let entered!: () => void;
  let release!: () => void;
  let reject!: (error: Error) => void;
  const enteredGate = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>((resolve, fail) => { release = resolve; reject = fail; });
  void gate.catch(() => undefined);
  const original = f.remote.presence.bind(f.remote);
  vi.spyOn(monotonicClock, "now").mockImplementation(() => logicalMs);
  f.remote.presence = async () => {
    const presence = await original();
    if (startup) { startup = false; return presence; }
    refreshes += 1;
    entered();
    await gate;
    await turn(); // the independent watchdog runs while the retained sample is gated
    return presence;
  };
  return {
    entered: enteredGate,
    debug: (line: string) => {
      if (skippedIds([line]).length && ++discarded % 64 === 0) logicalMs += 40_000;
      notify(line);
    },
    refreshes: () => refreshes,
    logicalMs: () => logicalMs,
    release: () => release(),
    reject: (error: Error) => reject(error),
    restore: () => { f.remote.presence = original; },
  };
}

async function addRoot(store: MeshStore, name: string, id = `session:${name}-00000000`): Promise<string> {
  const identity: MeshIdentity = { id, name: `${name}-root`, kind: "main", sessionId: `${name}-root` };
  const now = Date.now();
  await store.put({ key: key("hosts", id), identity,
    value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 600_000 } });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: `${name}-root`,
      label: `${name.toUpperCase()}_ROOT`, status: "idle", runner: "pi", transport: "host", capabilities: [], sessionId: `${name}-root`,
      startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return id;
}

async function seedHeldPage(f: Fixture) {
  const knownOther = await addRoot(f.source, "known-other");
  expect((await f.local.presence()).reserved).toContain(knownOther);
  return seedEvents(f, 500, index => index < 399 ? knownOther : `session:unknown-${String(index).padStart(8, "0")}`);
}

const skippedIds = (lines: string[]): string[] => lines.flatMap(line => {
  const match = /toRemote: not addressed across: \S+ (\S+) from /.exec(line);
  return match ? [match[1]!] : [];
});

describe("durable discard checkpoints (smarty-dev#16879)", () => {
  // Keep reason: recorded #16879 incident; a slow discard page must durably checkpoint
  // its consumed prefix before the coalesced membership refresh holds the remaining suffix.
  it("checkpoints a 500-event unaddressed page while it is still running and keeps the watchdog healthy", async () => {
    const f = fixture(500);
    const expected = await seedHeldPage(f);
    seedCursor(f);
    const page = await f.local.tail(0);
    expect(page.events.map(event => event.id)).toEqual(expected.map(event => event.id));
    expect(page.through).toBe(expected.at(-1)!.sequence);
    expect(page.events.length).toBe(500); // one real source page, below BRIDGE_PAGE_BYTES
    expect(page.events.reduce((sum, event) => sum + Buffer.byteLength(JSON.stringify(event)), 0)).toBeLessThan(BRIDGE_PAGE_BYTES);

    const slow = slowMembershipRefreshes(f);
    const alarms: BridgeStallAlarm[] = [];
    const watchdogAt: number[] = [];
    const live = launch(f, {
      debug: slow.debug,
      notifyWatchdog: () => watchdogAt.push(slow.logicalMs()),
      publishStallAlarm: async alarm => { alarms.push(alarm); },
    });
    try {
      await waitForGate(slow.entered);
      const durable = readCursor(f).toRemote.after;
      expect(slow.refreshes()).toBe(1);
      expect(slow.logicalMs()).toBeGreaterThan(BRIDGE_DIRECTION_STALL_AFTER_MS);
      expect(durable).toBeGreaterThan(0);
      expect(durable).toBe(expected[398]!.sequence); // Includes the prefix since the last periodic checkpoint.
      expect(durable).toBeLessThan(expected[399]!.sequence);
      expect(readCursor(f).toRemote.offset).toBeUndefined(); // Never save the unread page-end offset.
      await until(() => alarms.length > 0 || watchdogAt.some(at => at > BRIDGE_DIRECTION_STALL_AFTER_MS));
      expect(watchdogAt.some(at => at > BRIDGE_DIRECTION_STALL_AFTER_MS)).toBe(true);
      expect(alarms).toEqual([]);
      slow.release();
      await until(() => readCursor(f).toRemote.after === expected.at(-1)!.sequence);
      expect(alarms).toEqual([]);
      expect(live.failure()).toBeUndefined();
    } finally {
      slow.release();
      await live.bridge.stop();
      await live.running;
    }
  }, 20_000);

  // Keep reason: #16879 data safety; a restart must honor only the durable prefix and resume the real page suffix.
  it("restarts at the durable sequence checkpoint without replaying it or losing the suffix/byte offset", async () => {
    const f = fixture(500);
    const expected = await seedHeldPage(f);
    seedCursor(f);
    const page = await f.local.tail(0);
    expect(page.events).toHaveLength(500);
    expect(page.through).toBe(expected.at(-1)!.sequence);

    const firstPass: string[] = [];
    const slow = slowMembershipRefreshes(f);
    const first = launch(f, { debug: line => { slow.debug(line); firstPass.push(line); } });
    try {
      await waitForGate(slow.entered);
      const checkpoint = readCursor(f).toRemote.after;
      expect(checkpoint).toBeGreaterThan(0);
      expect(checkpoint).toBe(expected[398]!.sequence);
      expect(checkpoint).toBeLessThan(expected[399]!.sequence);
      expect(readCursor(f).toRemote.offset).toBeUndefined();
      expect(skippedIds(firstPass)).toEqual(expected.slice(0, 399).map(event => event.id));
      await first.bridge.stop();
      slow.reject(new Error("stop at durable mid-page checkpoint"));
      await first.running;
      expect(first.failure()).toBeUndefined();

      slow.restore();
      const resumedPass: string[] = [];
      const resumedReads: Array<{ after: number; events: number[]; through: number }> = [];
      const restarted = { ...f, local: new StoreBridgeSide(f.source, "far"), remote: new StoreBridgeSide(f.destination, "hub") };
      const tail = restarted.local.tail.bind(restarted.local);
      restarted.local.tail = async (after, offset, pageBytes) => {
        const resumed = await tail(after, offset, pageBytes);
        resumedReads.push({ after, events: resumed.events.map(event => event.sequence), through: resumed.through });
        return resumed;
      };
      const notify = membershipNotifications(restarted);
      const resumed = launch(restarted, { debug: line => { notify(line); resumedPass.push(line); } });
      await until(() => readCursor(f).toRemote.after === expected.at(-1)!.sequence);
      const suffix = expected.filter(event => event.sequence > checkpoint);
      expect(resumedReads[0]).toEqual({ after: checkpoint,
        events: suffix.map(event => event.sequence), through: expected.at(-1)!.sequence });
      expect(skippedIds(resumedPass)).toEqual(suffix.map(event => event.id));
      expect(skippedIds(firstPass).slice(0, checkpoint)).toEqual(expected.slice(0, checkpoint).map(event => event.id));
      expect(readCursor(f).toRemote.offset).toBe((await f.local.latestCursor()).offset);
      expect(resumed.failure()).toBeUndefined();
      await resumed.bridge.stop();
      await resumed.running;
    } finally {
      slow.reject(new Error("test cleanup"));
      await first.bridge.stop();
      await first.running;
    }
  }, 20_000);

  // Keep reason: #16879 data safety; a held event remains unread across restart,
  // and a newly admitted recipient receives it exactly once, not zero or twice.
  it("re-evaluates a held event after restart and forwards it exactly once when its recipient joins", async () => {
    const f = fixture(500);
    const expected = await seedHeldPage(f);
    const held = expected[399]!;
    seedCursor(f);
    const slow = slowMembershipRefreshes(f);
    const first = launch(f, { debug: slow.debug });
    try {
      await waitForGate(slow.entered);
      const saved = readCursor(f).toRemote;
      expect(saved.after).toBe(expected[398]!.sequence);
      expect(saved.after).toBeLessThan(held.sequence);
      expect(saved.offset).toBeUndefined();
      expect(f.destination.read()).toEqual([]);
      await first.bridge.stop();
      slow.reject(new Error("restart during the held membership sample"));
      await first.running;
      expect(first.failure()).toBeUndefined();
      slow.restore();

      await addRoot(f.destination, "joined", held.to!);
      const restarted = { ...f, local: new StoreBridgeSide(f.source, "far"), remote: new StoreBridgeSide(f.destination, "hub") };
      const reads: Array<{ after: number; offset?: number | undefined; ids: string[] }> = [];
      const tail = restarted.local.tail.bind(restarted.local);
      restarted.local.tail = async (after, offset, pageBytes) => {
        const page = await tail(after, offset, pageBytes);
        reads.push({ after, offset, ids: page.events.map(event => event.id) });
        return page;
      };
      const resumedPass: string[] = [];
      const notify = membershipNotifications(restarted);
      const resumed = launch(restarted, { debug: line => { notify(line); resumedPass.push(line); } });
      await until(() => readCursor(f).toRemote.after === expected.at(-1)!.sequence);
      expect(reads[0]).toEqual({ after: saved.after, offset: saved.offset,
        ids: expected.slice(399).map(event => event.id) });
      expect(skippedIds(resumedPass)).toEqual(expected.slice(400).map(event => event.id));
      expect(f.destination.read().map(event => (event.data as { bridge?: { id?: string } }).bridge?.id)).toEqual([held.id]);
      expect(readCursor(f).toRemote.offset).toBe((await f.local.latestCursor()).offset);
      expect(resumed.failure()).toBeUndefined();
      await resumed.bridge.stop();
      await resumed.running;

      const final = new MeshBridge({ localName: "hub", remoteName: "far", local: new StoreBridgeSide(f.source, "far"),
        remote: new StoreBridgeSide(f.destination, "hub"), cursorPath: f.cursorPath, stopMs: 50 });
      try { await final.start(); await final.step(); } finally { await final.stop(); }
      expect(f.destination.read().map(event => (event.data as { bridge?: { id?: string } }).bridge?.id)).toEqual([held.id]);
    } finally {
      slow.reject(new Error("test cleanup"));
      await first.bridge.stop();
      await first.running;
    }
  }, 20_000);

  // Keep reason: #16879 safety boundary; discard checkpoints may not cross a still-unconfirmed addressed publication.
  it("does not checkpoint past an unconfirmed publication and keeps the existing 180 s watchdog effective", async () => {
    const f = fixture(500);
    const remoteId = await addRoot(f.destination, "far");
    await f.source.publish({ topic: "ops.owner", kind: "pr.wake", from: sourceIdentity,
      to: "session:unknown-prefix-0000", data: { path: "discard-prefix" } });
    await seedEvents(f, 2, remoteId);
    seedCursor(f);
    const events = f.source.read({ after: 0, limit: 10 });
    expect(events).toHaveLength(3);
    expect(events[0]!.to).toBe("session:unknown-prefix-0000");
    expect(events[1]!.to).toBe(remoteId);

    let rejectGate!: (error: Error) => void;
    const pending = new Promise<never>((_resolve, reject) => { rejectGate = reject; });
    void pending.catch(() => undefined);
    let entered!: () => void;
    const enteredBatch = new Promise<void>(resolve => { entered = resolve; });
    f.remote.publishBatchWithReceipt = async () => { entered(); return pending; };
    let logicalMs = 0;
    vi.spyOn(monotonicClock, "now").mockImplementation(() => logicalMs);
    const alarms: BridgeStallAlarm[] = [];
    let watchdogProbes = 0;
    const live = launch(f, { notifyWatchdog: () => { watchdogProbes += 1; },
      publishStallAlarm: async alarm => { alarms.push(alarm); } });
    try {
      await waitForGate(enteredBatch);
      expect(BRIDGE_DIRECTION_STALL_AFTER_MS).toBe(180_000);
      expect(readCursor(f).toRemote.after).toBeLessThan(events[1]!.sequence);
      await until(() => watchdogProbes > 0);
      // Let a normal responsive probe observe the genuinely unconfirmed destination boundary.
      logicalMs = BRIDGE_DIRECTION_STALL_AFTER_MS + 1;
      await until(() => alarms.length > 0 || live.failure() !== undefined, 3_000);
      expect(alarms).toHaveLength(1);
      expect(alarms[0]).toMatchObject({ direction: "toRemote", reason: expect.stringContaining("no toRemote acknowledgement") });
      expect(readCursor(f).toRemote.after).toBeLessThan(events[1]!.sequence);
      expect(f.destination.read()).toEqual([]);
      expect(live.failure()).toBeUndefined();
    } finally {
      await live.bridge.stop();
      rejectGate(new Error("release blocked destination publication"));
      await live.running;
    }
  }, 20_000);

  // Keep reason: #16879 data safety; a failed membership refresh is an unread boundary, not discard evidence.
  it("does not checkpoint past a failed membership refresh for an unaddressed event", async () => {
    const f = fixture(500);
    const expected = await seedHeldPage(f);
    seedCursor(f);
    const failing = expected[399]!;
    const slow = slowMembershipRefreshes(f);
    const live = launch(f, { debug: slow.debug });
    try {
      await waitForGate(slow.entered);
      expect(readCursor(f).toRemote.after).toBe(expected[398]!.sequence);
      slow.reject(new Error("membership refresh failed at the retained boundary"));
      await until(() => live.failure() !== undefined);
      expect(slow.refreshes()).toBe(1);
      expect(String(live.failure())).toContain("membership refresh failed");
      expect(readCursor(f).toRemote.after).toBe(expected[398]!.sequence);
      expect(readCursor(f).toRemote.after).toBeLessThan(failing.sequence);
      expect(readCursor(f).toRemote.offset).toBeUndefined();
      expect(f.destination.read()).toEqual([]);
    } finally {
      slow.reject(new Error("test cleanup"));
      await live.bridge.stop();
      await live.running;
    }
  }, 20_000);
});
