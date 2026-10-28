// Keep reason: aged lease evidence treated as complete omission can delete a live root during stalled membership recovery.
// Production notification run + real stores/installers; only mirror completion is gated.
// No scratch/session deletion: retain fixture evidence, close only owned subscriptions/stores.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BridgeMirrorDeferredError, BridgeStaleIncarnationError, MeshBridge, StoreBridgeSide,
  type BridgeSide,
} from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of closes.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const gate = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const until = async (check: () => boolean, label: string) => {
  const end = performance.now() + 5_000;
  while (!check() && performance.now() < end) await sleep(5);
  expect(check(), label).toBe(true);
};
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const identity = (name: string): MeshIdentity => ({ id: `session:stalled-${name}`, name: "main", kind: "main", sessionId: name });
const native = async (store: MeshStore, who: MeshIdentity) => {
  const now = Date.now(), id = who.id;
  await store.put({ key: key("hosts", id), identity: who, value: {
    format: 1, id, rootId: id, identity: who, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: who, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: `fixture:${id}`,
    name: who.sessionId!, label: who.sessionId!, status: "idle", runner: "pi", transport: "host", capabilities: ["followUp"],
    sessionId: who.sessionId!, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
};

async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-stalled-membership-"));
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100, { stateBackend: "file" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 100, { stateBackend: "file" });
  const local = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
  const lead = identity("lead-root"), lane = identity("lane-root");
  await native(hub, lead);
  // Explicit notices supplement the real filesystem subscription; neither grants authority.
  type Notice = Parameters<NonNullable<BridgeSide["subscribeChanges"]>>[0];
  let notice!: Notice;
  const subscribe = remote.subscribeChanges.bind(remote);
  vi.spyOn(remote, "subscribeChanges").mockImplementation((changed, failed) => {
    notice = changed;
    return subscribe(changed, failed);
  });
  const logs: string[] = [];
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
    membershipRetryMs: 25, stopMs: 200, log: line => logs.push(line), debug: line => logs.push(line) });
  let failure: unknown;
  const read = vi.spyOn(remote, "tail");
  const running = bridge.run().catch(error => { failure = error; });
  const releases: Array<() => void> = [];
  const passes: Promise<unknown>[] = [];
  closes.push(async () => {
    for (const release of releases) release();
    await Promise.all(passes);
    await bridge.stop();
    await running;
    hub.closeState(); far.closeState();
  });
  await until(() => read.mock.calls.length > 0 || failure !== undefined, "notification drains started");
  expect(failure).toBeUndefined();
  await sleep(60);
  const events = () => hub.read({ after: 0, limit: 100 });
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toLocal: { after: number }; toRemote: { after: number } };
  const mirrorLocal = local.mirror.bind(local);
  const recover = async () => {
    await mirrorLocal(await remote.presence(), Date.now()); // real admission, leases, custody and COMMIT
    expect(local.holds(lane.id)).toBe(true);
    notice("lease");
  };
  const send = async (ack = false, sender = lane) => {
    const event = await far.publish(ack ? {
      topic: "fabric.control.ack", kind: "ack", from: sender, to: lead.id,
      data: { version: 1, commandId: "stalled-ack", targetId: sender.id, accepted: true },
    } : {
      topic: "fabric.control.command", kind: "followUp", from: sender, to: lead.id,
      data: { version: 1, commandId: "stalled-command", targetId: lead.id, replyTo: sender.id,
        operation: "followUp", destinationRemoteHost: "hub", ownerIncarnation: `fixture:${lead.id}`, message: "retained body" },
    });
    notice("events");
    return event;
  };
  const stall = async (inboundFailure?: Error) => {
    const entered = gate(), outbound = gate(), inbound = gate();
    releases.push(outbound.resolve, inbound.resolve);
    const mirror = remote.mirror.bind(remote);
    let returned = false, calls = 0;
    vi.spyOn(remote, "mirror").mockImplementation(async (...args) => {
      calls++; entered.resolve();
      await outbound.promise;
      try { return await mirror(...args); } finally { returned = true; }
    });
    if (inboundFailure) vi.spyOn(local, "mirror").mockImplementation(async () => {
      await inbound.promise;
      throw inboundFailure;
    });
    const pass = bridge.syncPresence().catch(error => error);
    passes.push(pass);
    await entered.promise;
    const syncPresence = bridge.syncPresence.bind(bridge);
    let forwardJoined = false;
    const sync = vi.spyOn(bridge, "syncPresence").mockImplementation(() => {
      // Distinguish the real event/retained forward waiter from a background loop join.
      if (new Error().stack?.includes("#refreshForwardMembership")) forwardJoined = true;
      return syncPresence();
    });
    const joined = () => forwardJoined;
    return { pass, joined, inbound, outbound, sync, assertHeld: () => {
      expect(calls).toBe(1); expect(returned).toBe(false);
    } };
  };
  return { bridge, hub, far, local, remote, lead, lane, logs, events, cursor, recover, send, stall,
    notice: (kind: Parameters<Notice>[0]) => notice(kind), failure: () => failure };
}

describe("notification forwarding through an unresolved shared membership pass", () => {
  it.each([
    ["toLocal", false], ["toLocal", true], ["toRemote", false], ["toRemote", true],
  ] as const)("releases absent %s membership and later work with every mirror taking 1.2 s (ACK=%s)", async (direction, ack) => {
    const f = await fixture();
    await native(f.far, f.lane);
    await f.bridge.syncPresence();
    const mirror = f.remote.mirror.bind(f.remote);
    vi.spyOn(f.remote, "mirror").mockImplementation(async (...args) => {
      await sleep(1_200);
      return mirror(...args);
    });
    const missing = identity("never-installed");
    const outbound = async (to: string) => f.hub.publish(ack ? {
      topic: "fabric.control.ack", kind: "ack", from: f.lead, to,
      data: { version: 1, commandId: `outbound-${to}`, targetId: f.lead.id, accepted: true },
    } : {
      topic: "fabric.control.command", kind: "followUp", from: f.lead, to,
      data: { version: 1, commandId: `outbound-${to}`, targetId: to, replyTo: f.lead.id,
        operation: "followUp", destinationRemoteHost: "far", message: "later work" },
    });
    const first = direction === "toLocal" ? await f.send(ack, missing) : await outbound(missing.id);
    const later = direction === "toLocal" ? await f.send(ack) : await outbound(f.lane.id);
    f.notice("events");
    const destination = direction === "toLocal" ? f.hub : f.far;
    await until(() => f.cursor()[direction].after >= later.sequence, "absent event refused and later cursor unblocked");
    const delivered = destination.read({ after: 0, limit: 100 }).filter(event =>
      (event.data as { bridge?: { id?: string } })?.bridge?.id === later.id);
    expect(delivered).toHaveLength(1);
    expect(destination.read({ after: 0, limit: 100 }).some(event =>
      (event.data as { bridge?: { id?: string } })?.bridge?.id === first.id)).toBe(false);
    expect(f.cursor()[direction].after).toBeGreaterThanOrEqual(first.sequence);
    expect(f.failure()).toBeUndefined();
  }, 15_000);

  it("does not use a pre-miss absence to drop a sender that appears before the next sample", async () => {
    const f = await fixture(), held = await f.stall();
    const event = await f.send();
    await until(() => f.logs.some(line => line.includes("membership pending")), "old sample timed out at the retained event");
    await native(f.far, f.lane);
    expect(f.local.holds(f.lane.id)).toBe(false);
    held.outbound.resolve();
    await held.pass;
    await until(() => f.events().length === 1, "a later sample admits the newly appeared sender");
    expect((f.events()[0]!.data as { bridge: { id: string } }).bridge.id).toBe(event.id);
    expect(f.logs.some(line => line.includes(event.id) && line.includes(": dropped "))).toBe(false);
  }, 12_000);

  it("keeps a never-installed sender pending when every delayed aggregate fails", async () => {
    const f = await fixture();
    let failures = 0;
    vi.spyOn(f.remote, "mirror").mockImplementation(async () => {
      await sleep(1_200);
      failures++;
      throw new BridgeMirrorDeferredError(false, 25);
    });
    const event = await f.send();
    await until(() => failures >= 2, "multiple delayed passes failed rather than supplying absence evidence");
    expect(f.cursor().toLocal.after).toBeLessThan(event.sequence);
    expect(f.events()).toEqual([]);
    expect(f.failure()).toBeUndefined();
  }, 12_000);

  it("does not reuse a completed absence for a different event boundary", async () => {
    const f = await fixture();
    const mirrorRemote = f.remote.mirror.bind(f.remote);
    vi.spyOn(f.remote, "mirror").mockImplementation(async (...args) => {
      await sleep(1_200);
      return mirrorRemote(...args);
    });
    const first = await f.send();
    await until(() => f.cursor().toLocal.after >= first.sequence, "first absent boundary consumed");
    const entered = gate(), release = gate();
    const mirrorLocal = f.local.mirror.bind(f.local);
    vi.spyOn(f.local, "mirror").mockImplementation(async (...args) => {
      entered.resolve();
      await release.promise;
      return mirrorLocal(...args);
    });
    const next = identity("later-sender");
    await native(f.far, next);
    const later = await f.send(false, next);
    try {
      await entered.promise;
      await sleep(1_250);
      expect(f.cursor().toLocal.after).toBeLessThan(later.sequence);
      expect(f.events()).toEqual([]);
    } finally { release.resolve(); }
    await until(() => f.events().length === 1, "new boundary waits for its own current authority");
    expect((f.events()[0]!.data as { bridge: { id: string } }).bridge.id).toBe(later.id);
  }, 15_000);

  it.each([false, true])("uses real canonical recovery %s after entering the wait, without completing public sync", async late => {
    const f = await fixture(), held = await f.stall();
    await native(f.far, f.lane);
    const command = await f.send();
    const ack = await f.send(true);
    await until(held.joined, "event entered membership refresh before recovery");
    if (late) {
      await until(() => f.logs.some(line => line.includes("membership pending")), "first cap retained unknown sender without a sampled deadline");
      expect(f.events()).toEqual([]);
      expect(f.cursor().toLocal.after).toBeLessThan(command.sequence);
      expect(f.local.holds(f.lane.id)).toBe(false);
    }
    await f.recover();
    await until(() => f.events().length === 2, "command and ACK delivered before outbound release");
    expect(f.events().map(event => (event.data as { bridge: { id: string } }).bridge.id)).toEqual([command.id, ack.id]);
    held.assertHeld();
    let settled = false;
    void held.pass.then(() => { settled = true; });
    await sleep(30);
    expect(settled, "public sync remains strict").toBe(false);
    expect(f.cursor().toLocal.after).toBeGreaterThanOrEqual(ack.sequence);
    expect(f.failure()).toBeUndefined();
    expect(f.events()).toHaveLength(2);
  }, 12_000);

  it("keeps an explicit public step strict even on an already-running notification bridge", async () => {
    const f = await fixture(), held = await f.stall();
    const command = await f.send();
    await until(held.joined, "notification forward is already waiting on unchanged source membership");
    let settled = false;
    const strict = f.bridge.step().then(result => { settled = true; return result; });
    void strict.catch(() => undefined);
    await sleep(1_200);
    held.assertHeld();
    expect(settled, "step cannot borrow the notification forward's timeout").toBe(false);
    expect(f.events()).toEqual([]);
    expect(f.cursor().toLocal.after).toBeLessThan(command.sequence);
    held.outbound.resolve();
    await strict;
    expect(f.events()).toEqual([]);
    expect(f.failure()).toBeUndefined();
  }, 12_000);

  it("retains absent membership across capped retries, then consumes only after an old plus genuinely fresh negative pass", async () => {
    const f = await fixture(), held = await f.stall();
    const command = await f.send(); // canonical id, but never installed or sampled as live
    await until(() => f.logs.filter(line => line.includes("joined in-flight pass")).length >= 3, "retained-boundary retry also escapes the stalled pass");
    held.assertHeld();
    expect(f.events()).toEqual([]);
    expect(f.cursor().toLocal.after).toBeLessThan(command.sequence);
    expect(f.far.read({ after: 0, limit: 100 }).find(event => event.id === command.id)?.data).toMatchObject({ message: "retained body" });
    expect(held.sync.mock.calls.length).toBeLessThanOrEqual(6);
    held.outbound.resolve();
    await held.pass;
    await until(() => f.cursor().toLocal.after >= command.sequence, "completed post-miss absence releases refusal");
    expect(f.events()).toEqual([]);
    expect(held.sync.mock.calls.length).toBeGreaterThan(1);
    expect(f.logs.some(line => line.includes(command.id) && line.includes("not a live participant"))).toBe(true);
    expect(f.failure()).toBeUndefined();
  }, 12_000);

  it.each(["permanent", "stale"] as const)("fences late %s half-failure after a cap even while the other mirror never settles", async kind => {
    const f = await fixture();
    const error = kind === "stale" ? new BridgeStaleIncarnationError(f.lane.id) : new Error("permanent mirror refusal");
    const held = await f.stall(error);
    await native(f.far, f.lane);
    const command = await f.send();
    await until(() => f.logs.some(line => line.includes("membership pending")), "forward cap elapsed before rejection");
    held.inbound.resolve();
    await sleep(10); // individual mirror rejection, not aggregate settlement
    await f.recover();
    await sleep(1_150); // allow the escaped retained waiter to resample and hit the fatal fence
    held.assertHeld();
    expect(f.events()).toEqual([]);
    expect(f.cursor().toLocal.after).toBeLessThan(command.sequence);
    if (kind === "stale") await expect(f.bridge.syncPresence()).rejects.toBe(error);
    held.outbound.resolve();
    expect(await held.pass).toBe(error);
    await until(() => f.failure() === error, "existing notification policy receives original failure");
  }, 12_000);

  it.each(["sender takeover", "ACK target takeover", "lease expiry"] as const)(
    "still applies the real held COMMIT fence after recovery and %s at the append seam", async loss => {
      const f = await fixture(), held = await f.stall();
      await native(f.far, f.lane);
      const other = identity("ack-target");
      if (loss === "ACK target takeover") await native(f.far, other);
      const append = f.local.publishWithReceipt.bind(f.local);
      let attempts = 0;
      vi.spyOn(f.local, "publishWithReceipt").mockImplementation(async (event, ids) => {
        attempts++;
        const lost = loss === "ACK target takeover" ? other : f.lane;
        expect(f.local.holds(lost.id)).toBe(true);
        if (loss === "lease expiry") {
          writeHostLease(f.hub.root, { ...readHostLease(f.hub.root, lost.id)!, expiresAt: Date.now() - 1 });
        } else await native(f.hub, lost); // positive native reservation, not a fabricated refusal
        expect(f.local.holds(lost.id)).toBe(false);
        return append(event, ids); // actual StoreBridgeSide held fence refuses before COMMIT
      });
      const event = loss === "ACK target takeover" ? await f.far.publish({
        topic: "fabric.control.ack", kind: "ack", from: f.lane, to: f.lead.id,
        data: { version: 1, commandId: "lost-ack-target", targetId: other.id, accepted: true },
      }) : await f.send();
      f.notice("events");
      await until(held.joined, "entered the old shared pass before watch-style recovery");
      await f.recover();
      await until(() => attempts > 0, "positive rules reached the actual append seam");
      await sleep(50);
      held.assertHeld();
      expect(attempts).toBe(1); // bounded attempted refresh cannot retry publication from old rules
      expect(f.events()).toEqual([]);
      if (loss === "lease expiry") expect(f.cursor().toLocal.after).toBeLessThan(event.sequence);
      else await until(() => f.cursor().toLocal.after >= event.sequence, "proven native takeover is refused, not indefinite uncertainty");
      expect(f.failure()).toBeUndefined();
    }, 12_000,
  );

  it("does not turn a retryable half-failure into a fatal latch or consume the retained event", async () => {
    const f = await fixture(), error = new BridgeMirrorDeferredError(false, 100);
    const held = await f.stall(error);
    await native(f.far, f.lane);
    const command = await f.send();
    await until(() => f.logs.some(line => line.includes("membership pending")), "retryable probe reached bounded wait");
    held.inbound.resolve();
    await sleep(20);
    expect(f.events()).toEqual([]);
    expect(f.cursor().toLocal.after).toBeLessThan(command.sequence);
    await f.recover();
    await until(() => f.events().length === 1, "positive canonical authority remains usable after retryable failure");
    held.assertHeld();
    expect(f.failure()).toBeUndefined();
    held.outbound.resolve();
    expect(await held.pass).toBe(error); // strict callers still receive the retryable original
  }, 12_000);
});
