import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshSnapshotRetriesExhaustedError, MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { watchBridgeStore } from "../src/mesh/change-notifier.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [];
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

const deferred = <T = void,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve: (value?: T) => resolve(value as T) };
};
const within = async <T,>(work: Promise<T>, label: string, ms = 4_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const stamp = (event: MeshEvent) => (event.data as { bridge?: { from?: unknown; id?: unknown } } | undefined)?.bridge;
const addNative = async (store: MeshStore, name: string, now = Date.now()) => {
  const id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000 };
  const participant = { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: `fixture:${id}`, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1" };
  await store.put({ key: key("hosts", id), identity, value: host });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: host.expiresAt });
  const entry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: participant };
  writeParticipantFile(store.root, entry);
  return { identity, entry };
};
const changeNative = (store: MeshStore, root: Awaited<ReturnType<typeof addNative>>, now = Date.now()) => {
  writeParticipantFile(store.root, { ...root.entry, updatedAt: now, value: { ...root.entry.value, status: "running", updatedAt: now } });
};

const observeEvents = (store: MeshStore) => {
  const entries: MeshEvent[] = [];
  const waiters = new Set<{ check: (event: MeshEvent) => boolean; resolve: (event: MeshEvent) => void }>();
  let after = 0;
  const scan = () => {
    for (;;) {
      const page = store.read({ after, limit: 256 });
      if (!page.length) break;
      for (const event of page) {
        after = event.sequence;
        entries.push(event);
        for (const waiter of [...waiters]) if (waiter.check(event)) {
          waiters.delete(waiter);
          waiter.resolve(event);
        }
      }
    }
  };
  const close = watchBridgeStore(store.root, kind => { if (kind === "events") scan(); }, error => { throw error; });
  scan();
  return {
    entries, close,
    wait(check: (event: MeshEvent) => boolean, label: string) {
      const existing = entries.find(check);
      if (existing) return Promise.resolve(existing);
      const result = deferred<MeshEvent>();
      const waiter = { check, resolve: result.resolve };
      waiters.add(waiter);
      scan();
      return within(result.promise, label).finally(() => waiters.delete(waiter));
    },
  };
};

type Snapshot = Parameters<StoreBridgeSide["mirror"]>[0];
const contains = (snapshot: Snapshot, id: string) => snapshot.hosts.some(host => host.record.id === id);

const fixture = async (clock?: () => number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-snapshot-isolation-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100, { stateBackend: "sqlite" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 100, { stateBackend: "sqlite" });
  const local = new StoreBridgeSide(hub, "far", clock), agent = new StoreBridgeSide(far, "hub", clock);
  const input = new PassThrough(), output = new PassThrough();
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, 15_000);
  const logs: string[] = [];
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
    presenceMs: 60_000, stopMs: 200, ...(clock ? { clock } : {}), log: line => logs.push(line) });
  let runFailure: unknown;
  let running: Promise<void> | undefined;
  let closed = false;
  const hubEvents = observeEvents(hub), farEvents = observeEvents(far);
  const readStarts = { local: deferred<void>(), remote: deferred<void>() };
  const watchedPages: Array<{ check: (event: MeshEvent) => boolean; side: "local" | "remote"; signal: ReturnType<typeof deferred<void>> }> = [];
  const watchedTails: Array<{ after: number; side: "local" | "remote"; signal: ReturnType<typeof deferred<void>> }> = [];
  const localTail = local.tail!.bind(local), remoteTail = remote.tail!.bind(remote);
  local.tail = async (...args) => {
    const page = await localTail(...args);
    readStarts.local.resolve();
    for (const waiter of watchedPages) if (waiter.side === "local" && page.events.some(waiter.check)) waiter.signal.resolve();
    for (const waiter of watchedTails) if (waiter.side === "local" && Number(args[0]) >= waiter.after) waiter.signal.resolve();
    return page;
  };
  remote.tail = async (...args) => {
    const page = await remoteTail(...args);
    readStarts.remote.resolve();
    for (const waiter of watchedPages) if (waiter.side === "remote" && page.events.some(waiter.check)) waiter.signal.resolve();
    for (const waiter of watchedTails) if (waiter.side === "remote" && Number(args[0]) >= waiter.after) waiter.signal.resolve();
    return page;
  };

  let rejectRemoteSnapshots = false;
  let remoteFailures = 0;
  const remoteFailureWaiters: Array<{ count: number; resolve: () => void }> = [];
  let remoteGate: { entered: ReturnType<typeof deferred<void>>; release: ReturnType<typeof deferred<void>> } | undefined;
  const mirrorRemote = agent.mirror.bind(agent);
  agent.mirror = async (...args) => {
    const gate = remoteGate;
    if (gate) {
      remoteGate = undefined;
      gate.entered.resolve();
      await gate.release.promise;
    }
    if (rejectRemoteSnapshots) {
      remoteFailures++;
      for (const waiter of [...remoteFailureWaiters]) if (remoteFailures >= waiter.count) {
        remoteFailureWaiters.splice(remoteFailureWaiters.indexOf(waiter), 1);
        waiter.resolve();
      }
      throw new MeshSnapshotRetriesExhaustedError(3);
    }
    return mirrorRemote(...args);
  };

  const localCommitWaiters: Array<{ check: (snapshot: Snapshot) => boolean; resolve: () => void }> = [];
  let localFailure: { check: (snapshot: Snapshot) => boolean; signal: ReturnType<typeof deferred<void>> } | undefined;
  let localGate: { check: (snapshot: Snapshot) => boolean; entered: ReturnType<typeof deferred<void>>;
    release: ReturnType<typeof deferred<void>> } | undefined;
  const mirrorLocal = local.mirror.bind(local);
  local.mirror = async (...args) => {
    const snapshot = args[0];
    const gate = localGate;
    if (gate && gate.check(snapshot)) {
      localGate = undefined;
      gate.entered.resolve();
      await gate.release.promise;
    }
    if (localFailure?.check(snapshot)) {
      localFailure.signal.resolve();
      throw new MeshSnapshotRetriesExhaustedError(3);
    }
    const result = await mirrorLocal(...args);
    for (const waiter of [...localCommitWaiters]) if (waiter.check(snapshot)) {
      localCommitWaiters.splice(localCommitWaiters.indexOf(waiter), 1);
      waiter.resolve();
    }
    return result;
  };

  const lead = await addNative(hub, "bridge-hub", clock?.());
  const lane = await addNative(far, "bridge-lane", clock?.());
  await remote.hello();
  await bridge.start();
  running = bridge.run().catch(error => { runFailure = error; });
  await within(Promise.all([readStarts.local.promise, readStarts.remote.promise]).then(() => undefined), "both traffic directions to start");

  const readCursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as {
    toLocal: { after: number }; toRemote: { after: number };
  };
  const close = async () => {
    if (closed) return;
    closed = true;
    rejectRemoteSnapshots = false;
    localFailure = undefined;
    remoteGate?.release.resolve();
    localGate?.release.resolve();
    hubEvents.close(); farEvents.close();
    await bridge.stop();
    if (running) await running;
    remote.close(); input.end(); await serving; output.end();
    hub.closeState(); far.closeState();
  };
  cleanups.push(close);
  return {
    hub, far, local, agent, remote, bridge, lead, lane, logs, cursorPath, hubEvents, farEvents,
    get runFailure() { return runFailure; },
    setRemoteFailure(value: boolean) { rejectRemoteSnapshots = value; },
    holdNextRemoteMirror() {
      const gate = { entered: deferred<void>(), release: deferred<void>() };
      remoteGate = gate;
      return gate;
    },
    waitRemoteFailures(count: number) {
      if (remoteFailures >= count) return Promise.resolve();
      const waiter = { count, resolve: () => undefined } as { count: number; resolve: () => void };
      const result = deferred<void>();
      waiter.resolve = result.resolve;
      remoteFailureWaiters.push(waiter);
      return within(result.promise, `remote snapshot exhaustion ${count}`);
    },
    watchLocalCommit(check: (snapshot: Snapshot) => boolean) {
      const result = deferred<void>();
      localCommitWaiters.push({ check, resolve: result.resolve });
      return result.promise;
    },
    holdNextLocalMirror(check: (snapshot: Snapshot) => boolean) {
      const gate = { check, entered: deferred<void>(), release: deferred<void>() };
      localGate = gate;
      return gate;
    },
    failLocalMirrors(check: (snapshot: Snapshot) => boolean) {
      const signal = deferred<void>();
      localFailure = { check, signal };
      return { failed: signal.promise, clear: () => { localFailure = undefined; } };
    },
    watchSourcePage(side: "local" | "remote", check: (event: MeshEvent) => boolean) {
      const signal = deferred<void>();
      watchedPages.push({ side, check, signal });
      return signal.promise;
    },
    localPCommit() {
      const result = deferred<void>();
      localCommitWaiters.push({ check: () => true, resolve: result.resolve });
      return result.promise;
    },
    readCursor,
    watchTailAfter(side: "local" | "remote", after: number) {
      const current = side === "local" ? readCursor().toRemote.after : readCursor().toLocal.after;
      if (current >= after) return Promise.resolve();
      const signal = deferred<void>();
      watchedTails.push({ side, after, signal });
      return within(signal.promise, "source cursor to pass the delivered event");
    },
    async beginOldPass() {
      const gate = this.holdNextRemoteMirror();
      const committed = this.localPCommit();
      this.setRemoteFailure(true);
      changeNative(hub, lead, clock?.());
      await within(gate.entered.promise, "old remote projection to be sampled");
      await within(committed, "old hub-local sample to commit");
      return gate;
    },
    async waitForwarded(store: MeshStore, originalId: string) {
      const observer = store === hub ? hubEvents : farEvents;
      return observer.wait(event => stamp(event)?.id === originalId, "forwarded event");
    },
  };
};

// Incident reason: a remote-projection snapshot refusal must not poison both traffic loops.
// Only committed hub-local ownership authorizes routing; stores, holds, publication, cursors
// and bridge stamps below remain on their real paths despite the injected typed RPC refusal.
describe("snapshot installation isolation", () => {
  it("keeps existing traffic in both directions live during one projection exhaustion and bounds its retry timers", async () => {
    const f = await fixture();
    const timers = vi.spyOn(globalThis, "setTimeout"), timerStart = timers.mock.calls.length;
    try {
      const gate = await f.beginOldPass();
      gate.release.resolve();
      await f.waitRemoteFailures(1);
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(true);
      expect((await f.local.owned()).hosts.some(host => host.record.id === f.lane.identity.id)).toBe(true);

      const outbound = await f.hub.publish({ topic: "fleet.work.snapshot-isolation", kind: "ask",
        from: f.lead.identity, to: f.lane.identity.id, text: "existing outbound" });
      const inbound = await f.far.publish({ topic: "fleet.work.snapshot-isolation", kind: "ask",
        from: f.lane.identity, to: f.lead.identity.id, text: "existing inbound" });
      const [toFar, toHub] = await Promise.all([
        f.waitForwarded(f.far, outbound.id), f.waitForwarded(f.hub, inbound.id),
      ]);
      expect(toFar).toMatchObject({ verification: "bridge", to: f.lane.identity.id,
        data: { bridge: { from: "hub", id: outbound.id } } });
      expect(toHub).toMatchObject({ verification: "bridge", to: f.lead.identity.id,
        data: { bridge: { from: "far", id: inbound.id } } });
      await Promise.all([f.watchTailAfter("local", outbound.sequence), f.watchTailAfter("remote", inbound.sequence)]);
      expect(f.readCursor().toRemote.after).toBeGreaterThanOrEqual(outbound.sequence);
      expect(f.readCursor().toLocal.after).toBeGreaterThanOrEqual(inbound.sequence);
      await f.waitRemoteFailures(3);
      const shortTimers = timers.mock.calls.slice(timerStart).map(([, delay]) => Number(delay))
        .filter(delay => Number.isFinite(delay) && delay > 0 && delay < 1_000);
      expect(shortTimers.length).toBeGreaterThan(0);
      expect(shortTimers.every(delay => delay <= 250)).toBe(true);
      expect(f.runFailure).toBeUndefined();
    } finally { f.setRemoteFailure(false); }
  }, 12_000);

  it.each(["new", "expired"] as const)("requires a fresh hub-local commit for a %s recipient even when the other projection exhausts", async kind => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    let gate: ReturnType<typeof f.holdNextRemoteMirror> | undefined;
    let localGate: ReturnType<typeof f.holdNextLocalMirror> | undefined;
    try {
      gate = await f.beginOldPass();
      let recipient = f.lane;
      if (kind === "new") recipient = await addNative(f.far, "bridge-new-recipient", now);
      else {
        now += BRIDGE_LEASE_MS + 1;
        expect(f.local.holds(recipient.identity.id, recipient.identity)).toBe(false);
      }
      const matchesRecipient = (snapshot: Snapshot) => contains(snapshot, recipient.identity.id);
      localGate = f.holdNextLocalMirror(matchesRecipient);
      const qCommit = f.watchLocalCommit(matchesRecipient);
      const sourceRead = f.watchSourcePage("local", event => event.topic === "fleet.work.snapshot-isolation" &&
        event.text === `${kind} recipient`);
      const origin = await f.hub.publish({ topic: "fleet.work.snapshot-isolation", kind: "ask",
        from: f.lead.identity, to: recipient.identity.id, text: `${kind} recipient` });
      await within(sourceRead, "recipient event to reach the live source reader");
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(f.farEvents.entries.some(event => stamp(event)?.id === origin.id)).toBe(false);
      expect(f.readCursor().toRemote.after).toBeLessThan(origin.sequence);

      gate.release.resolve();
      await f.waitRemoteFailures(1);
      await within(localGate.entered.promise, "fresh recipient snapshot to reach the hub-local mirror");
      expect(f.local.holds(recipient.identity.id, recipient.identity)).toBe(false);
      expect(f.farEvents.entries.some(event => stamp(event)?.id === origin.id)).toBe(false);
      expect(f.readCursor().toRemote.after).toBeLessThan(origin.sequence);
      localGate.release.resolve();
      await within(qCommit, "hub-local mirror completion");
      expect(f.local.holds(recipient.identity.id, recipient.identity)).toBe(true);
      expect((await f.local.owned()).hosts.some(host => host.record.id === recipient.identity.id)).toBe(true);
      const forwarded = await f.waitForwarded(f.far, origin.id);
      expect(forwarded).toMatchObject({ verification: "bridge", to: recipient.identity.id,
        data: { bridge: { from: "hub", id: origin.id } } });
      await f.watchTailAfter("local", origin.sequence);
      expect(f.readCursor().toRemote.after).toBeGreaterThanOrEqual(origin.sequence);
      expect(f.runFailure).toBeUndefined();
    } finally {
      gate?.release.resolve(); localGate?.release.resolve(); f.setRemoteFailure(false);
    }
  }, 12_000);

  it("retains a newcomer boundary without sending or dropping when its fresh local installation exhausts", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    let gate: ReturnType<typeof f.holdNextRemoteMirror> | undefined;
    try {
      gate = await f.beginOldPass();
      const recipient = await addNative(f.far, "bridge-uncommitted-recipient", now);
      const matchesRecipient = (snapshot: Snapshot) => contains(snapshot, recipient.identity.id);
      const localFailure = f.failLocalMirrors(matchesRecipient);
      const sourceRead = f.watchSourcePage("local", event => event.topic === "fleet.work.snapshot-isolation" &&
        event.text === "retained boundary");
      const origin = await f.hub.publish({ topic: "fleet.work.snapshot-isolation", kind: "ask",
        from: f.lead.identity, to: recipient.identity.id, text: "retained boundary" });
      await within(sourceRead, "new-recipient event to reach the live source reader");
      await new Promise<void>(resolve => setImmediate(resolve));
      gate.release.resolve();
      await localFailure.failed;
      await f.waitRemoteFailures(1);
      expect(f.local.holds(recipient.identity.id, recipient.identity)).toBe(false);
      expect(f.farEvents.entries.some(event => stamp(event)?.id === origin.id)).toBe(false);
      expect(f.readCursor().toRemote.after).toBeLessThan(origin.sequence);
      expect(f.logs.some(line => line.includes(origin.id) && /dropped|refused/.test(line))).toBe(false);

      const committed = f.watchLocalCommit(matchesRecipient);
      localFailure.clear(); f.setRemoteFailure(false);
      await within(committed, "fresh hub-local retry to commit");
      expect(f.local.holds(recipient.identity.id, recipient.identity)).toBe(true);
      expect((await f.local.owned()).hosts.some(host => host.record.id === recipient.identity.id)).toBe(true);
      const forwarded = await f.waitForwarded(f.far, origin.id);
      expect(forwarded).toMatchObject({ verification: "bridge", to: recipient.identity.id,
        data: { bridge: { from: "hub", id: origin.id } } });
      await f.watchTailAfter("local", origin.sequence);
      expect(f.readCursor().toRemote.after).toBeGreaterThanOrEqual(origin.sequence);
      expect(f.runFailure).toBeUndefined();
    } finally { gate?.release.resolve(); f.setRemoteFailure(false); }
  }, 12_000);
});
