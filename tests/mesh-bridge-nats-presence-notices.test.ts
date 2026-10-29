// Keep reason: a remote lease renewal must not indefinitely block control traffic behind an absent peer.
// Real KV reader, raw NATS port, carrier, MeshBridge.run and file store; the KV/inbox are in-memory,
// not a live broker. Only outbound mirror completion is delayed or gated.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeMirrorDeferredError } from "../src/mesh/bridge.js";
import { createNatsFileLink } from "../src/mesh/bridge-nats-file.js";
import { NatsBridgePort, natsBridgeAdmit, type NatsBridgeInboxMessage, type NatsBridgeInboxSource } from "../src/mesh/bridge-nats-port.js";
import { bridgeNatsEnvelopeId, bridgeNatsLinkIdentity } from "../src/mesh/bridge-nats-types.js";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import type { NatsCarrierEnvelope } from "../src/mesh/bridge-nats-carrier.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { hKey, kvWorld, natsLease, pKey, seedNative } from "./helpers/bridge-nats-kv.js";
import { MemoryLedger } from "./helpers/bridge-nats-l2-fakes.js";

const BRIDGE_MACHINE = "test-bridge";
const NATS_MACHINE = "test-peer";
const RENEWAL_MACHINE = "test-renewer";
const LINK = "test-link";
const FILE_INCARNATION = "file-inc-1";
const NATS_INCARNATION = "nats-inc-1";
const identity = (digit: string, name: string): MeshIdentity => ({
  id: `session:${digit.repeat(8)}-${digit.repeat(4)}-4${digit.repeat(3)}-8${digit.repeat(3)}-${digit.repeat(12)}`,
  name, kind: "main",
});
const FILE = identity("1", "file-root"), PEER = identity("2", "remote-root");
const RENEWER = identity("3", "renewing-root"), ABSENT = identity("4", "absent-root");
const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of closes.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const until = async (check: () => boolean, label: string) => {
  const end = performance.now() + 5_000;
  while (!check() && performance.now() < end) await sleep(5);
  expect(check(), label).toBe(true);
};
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const participant = (who: MeshIdentity, at = Date.now(), incarnation = NATS_INCARNATION): FabricParticipantRecord => ({
  format: 1, id: who.id, kind: "root", rootId: who.id, ownerHostId: who.id, ownerIdentityId: who.id,
  ownerIncarnation: incarnation, name: who.name, label: who.name, status: "idle", runner: "pi", transport: "host",
  capabilities: ["followUp"], sessionId: who.id.slice(8), startedAt: at - 1_000, updatedAt: at, controlProtocol: "v1",
});
async function nativeFile(store: MeshStore): Promise<void> {
  const now = Date.now(), id = FILE.id;
  await store.put({ key: key("hosts", id), identity: FILE, value: {
    format: 1, id, rootId: id, identity: FILE, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: FILE,
    value: participant(FILE, now, FILE_INCARNATION) });
}

class Inbox implements NatsBridgeInboxSource {
  readonly queue: NatsBridgeInboxMessage[] = [];
  #wake: (() => void) | undefined;
  isVerified(): boolean { return true; }
  push(message: NatsBridgeInboxMessage): void { this.queue.push(message); this.#wake?.(); }
  async *consume(signal: AbortSignal): AsyncIterable<NatsBridgeInboxMessage> {
    while (!signal.aborted) {
      const message = this.queue.shift() ?? await new Promise<NatsBridgeInboxMessage | undefined>(resolve => {
        const abort = (): void => { this.#wake = undefined; resolve(undefined); };
        this.#wake = () => { this.#wake = undefined; signal.removeEventListener("abort", abort); resolve(this.queue.shift()); };
        signal.addEventListener("abort", abort, { once: true });
      });
      if (!message) return;
      yield message;
    }
  }
}
interface MirrorCall { startedAt: number; settledAt?: number; result: "pending" | "fulfilled" | "rejected" }

async function fixture(options: { endedPeer?: boolean; mirrorDelayMs?: number } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-notices-"));
  const store = new MeshStore(path.join(root, "hub"), 256 * 1024, 100, { stateBackend: "file" });
  const world = kvWorld(), inbox = new Inbox(), ledger = new MemoryLedger();
  const sent: NatsCarrierEnvelope[] = [];
  const logs: string[] = [], notices: Array<{ at: number; kind: BridgeChange }> = [];
  const revisions: Array<{ at: number; revision: number }> = [];
  const mirrorCalls: MirrorCall[] = [], writes: Promise<unknown>[] = [], releases: Array<() => void> = [];
  const gates: Array<{ release: ReturnType<typeof deferred<void>>; entered: ReturnType<typeof deferred<MirrorCall>> }> = [];
  const linkIdentity = bridgeNatsLinkIdentity(LINK);
  const port = new NatsBridgePort({
    transport: world.transport(BRIDGE_MACHINE, linkIdentity.id, async envelope => {
      sent.push(JSON.parse(JSON.stringify(envelope)) as NatsCarrierEnvelope);
      return { id: envelope.id, sequence: sent.length, duplicate: false, envelope: { ...envelope, createdAt: Date.now() } } as never;
    }),
    inbox, link: LINK,
    admit: natsBridgeAdmit({ file: [{ kind: "root", id: FILE.id }],
      nats: [PEER, RENEWER, ABSENT].map(who => ({ kind: "root" as const, id: who.id })) }),
    processProof: { pid: 4242, pidStartTime: "777", bootId: "12345678-1234-4234-8234-123456789abc" },
  });
  const cursorPath = path.join(root, "cursor.json");
  const link = createNatsFileLink({
    store, localName: "test-hub", link: LINK, cursorPath, port, ledger,
    authorize: request => ((request.senderId === FILE.id && [PEER.id, ABSENT.id].includes(request.targetId)) ||
      ([PEER.id, ABSENT.id].includes(request.senderId) && request.targetId === FILE.id))
      ? { ok: true, principal: "none" } : { ok: false, refuse: "not-allowed", reason: "not in the fixture allow-list" },
    authorizeTopic: () => ({ ok: false, refuse: "not-allowed", reason: "no topic routes" }),
    mirrorAllowed: record => record.kind === "root" && record.id === FILE.id,
    log: line => logs.push(line),
    bridge: { presenceMs: 60_000, membershipRetryMs: 25, stopMs: 200, debug: line => logs.push(line) },
  });
  let failure: unknown, mirrorError: Error | undefined;
  let running = Promise.resolve();
  const renewalTimers: Array<ReturnType<typeof setInterval>> = [];
  closes.push(async () => {
    for (const timer of renewalTimers) clearInterval(timer);
    for (const release of releases) release();
    await link.bridge.stop();
    await running;
    await Promise.all(writes);
    await link.remote.fence();
    await port.close();
    store.closeState();
    // Delete only the directory allocated by this fixture, never pre-existing evidence.
    fs.rmSync(root, { recursive: true, force: true });
  });
  const mirror = link.remote.mirror.bind(link.remote);
  vi.spyOn(link.remote, "mirror").mockImplementation((...args) => {
    const call: MirrorCall = { startedAt: performance.now(), result: "pending" };
    mirrorCalls.push(call);
    const held = gates.shift(), error = mirrorError;
    held?.entered.resolve(call);
    const write = (async () => {
      try {
        await sleep(options.mirrorDelayMs ?? 1_200); // Every outbound call, including startup and retries.
        if (held) await held.release.promise;
        if (error) throw error;
        await mirror(...args);
        call.result = "fulfilled";
      } catch (error) { call.result = "rejected"; throw error; }
      finally { call.settledAt = performance.now(); }
    })();
    writes.push(write.catch(error => error));
    return write;
  });
  const subscribe = link.remote.subscribeChanges.bind(link.remote);
  vi.spyOn(link.remote, "subscribeChanges").mockImplementation((changed, failed) => subscribe(kind => {
    notices.push({ at: performance.now(), kind });
    changed(kind); // Observe the production carrier output without changing its classification.
  }, failed));
  const holdNextMirror = () => {
    const release = deferred<void>(), entered = deferred<MirrorCall>();
    gates.push({ release, entered });
    const finish = () => release.resolve(undefined);
    releases.push(finish);
    return { entered: entered.promise, release: finish };
  };
  const holdTargetInstall = () => {
    const release = deferred<void>();
    let entered = false;
    const install = link.local.mirror.bind(link.local);
    const finish = () => release.resolve(undefined);
    releases.push(finish);
    vi.spyOn(link.local, "mirror").mockImplementation((...args) => {
      const write = (async () => {
        if (args[0].hosts.some(host => host.record.id === ABSENT.id)) {
          entered = true;
          await release.promise;
        }
        return install(...args); // The real admission, custody, lease and COMMIT fences remain in use.
      })();
      writes.push(write.catch(error => error));
      return write;
    });
    return { release: finish, entered: () => entered };
  };
  const events = () => store.read({ after: 0, limit: 100 });
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toRemote: { after: number }; toLocal: { after: number } };
  const seed = (who: MeshIdentity, machine: string) => seedNative(world, machine, participant(who), Date.now());
  await nativeFile(store);
  seed(PEER, NATS_MACHINE);
  seed(RENEWER, RENEWAL_MACHINE);
  if (options.endedPeer) seed(ABSENT, NATS_MACHINE);
  await port.start();
  await link.bridge.start();
  await vi.waitFor(() => expect(port.resolve(FILE.id)?.route).toMatchObject({ machine: BRIDGE_MACHINE, live: true }));

  // Establish ACK claims through the real decoder and inbound carrier, not by inserting ledger records.
  let streamSequence = 0;
  const inboundRequest = async (who: MeshIdentity, id: string): Promise<MeshEvent> => {
    const now = Date.now(), calls: string[] = [];
    const envelope = { id, kind: "command", from: who, to: { machine: BRIDGE_MACHINE, hostId: FILE.id },
      targetId: FILE.id, operation: "followUp", createdAt: now,
      body: { version: 1, commandId: `${id}-command`, targetId: FILE.id, operation: "followUp", replyTo: who.id,
        ownerIncarnation: FILE_INCARNATION, message: "inbound request", requestedAt: now, requestCreatedAt: now, deadlineAt: now + 60_000 } };
    const sequence = ++streamSequence;
    inbox.push({ subject: `fabric.in.${BRIDGE_MACHINE}.${hashToken(FILE.id)}.cmd.${hashToken(FILE.id)}.${NATS_MACHINE}`,
      data: Buffer.from(JSON.stringify({ format: 1, sourceMachine: NATS_MACHINE, envelope })),
      info: { streamSequence: sequence, deliveryCount: 1, timestampNanos: now * 1_000_000 },
      async ackAck() { calls.push("ack"); return true; }, nak() { calls.push("nak"); },
      term() { calls.push("term"); }, working() {},
    });
    await vi.waitFor(async () => expect(await link.remote.latestSequence()).toBeGreaterThanOrEqual(sequence));
    await link.bridge.step();
    await link.bridge.step();
    expect(calls).toEqual(["ack"]);
    const received = events().filter(event => (event.data as { bridge?: { id?: string } })?.bridge?.id === id);
    expect(received).toHaveLength(1);
    expect(received[0]!.from.id).toBe(who.id);
    return received[0]!;
  };
  const liveRequest = await inboundRequest(PEER, "seed-live");
  const endedRequest = options.endedPeer ? await inboundRequest(ABSENT, "seed-ended") : undefined;
  if (options.endedPeer) {
    // A genuinely ended requester: its ACK is correlated, but it no longer has a canonical live owner.
    const presenceKey = pKey(NATS_MACHINE, ABSENT.id), leaseKey = hKey(NATS_MACHINE, ABSENT.id);
    expect(world.presence.delete(presenceKey, world.presence.values.get(presenceKey)!.revision)).toBe(true);
    expect(world.leases.delete(leaseKey, world.leases.values.get(leaseKey)!.revision)).toBe(true);
    await vi.waitFor(() => expect(port.resolve(ABSENT.id)).toBeUndefined());
    await link.bridge.syncPresence();
  }
  const tail = vi.spyOn(link.local, "tail");
  running = link.bridge.run().catch(error => { failure = error; });
  await until(() => tail.mock.calls.length > 0 || failure !== undefined, "notification direction started");
  expect(failure).toBeUndefined();
  await sleep(60); // Let the real file watch settle its initial mirrored projection.
  await until(() => mirrorCalls.every(call => call.result !== "pending"), "startup mirror work settled before the retained-boundary scenario");
  expect(link.local.holds(PEER.id)).toBe(true);
  expect(link.local.holds(ABSENT.id)).toBe(false);

  const publish = async (ack: boolean, to = ABSENT.id): Promise<MeshEvent> => {
    const request = to === ABSENT.id ? endedRequest : liveRequest;
    if (ack && !request) throw new Error("ACK fixture needs a previously admitted request");
    const now = Date.now();
    return store.publish(ack ? {
      topic: "fabric.control.ack", kind: "accepted", from: FILE, to,
      data: { version: 1, commandId: (request!.data as { commandId: string }).commandId,
        targetId: FILE.id, ownerIncarnation: FILE_INCARNATION, accepted: true, messageId: `answer-${to}` },
    } : {
      topic: "fabric.control.command", kind: "followUp", from: FILE, to,
      data: { version: 1, commandId: `outbound-${to}`, targetId: to, operation: "followUp", replyTo: FILE.id,
        ownerIncarnation: NATS_INCARNATION, destinationRemoteHost: LINK, message: "retained body",
        requestedAt: now, requestCreatedAt: now, deadlineAt: now + 60_000 },
    });
  };
  const renewOnce = () => {
    const leaseKey = hKey(RENEWAL_MACHINE, RENEWER.id), current = world.leases.values.get(leaseKey)!;
    const revision = world.leases.put(leaseKey, current.owner, natsLease(RENEWER.id, NATS_INCARNATION, Date.now()), current.revision);
    revisions.push({ at: performance.now(), revision });
    return revision;
  };
  const renew = () => {
    // Raw lease revisions are the stimulus. KV watch delivery, not polling, drives the bridge.
    const timer = setInterval(renewOnce, 300);
    renewalTimers.push(timer);
    return () => clearInterval(timer);
  };
  return { world, port, link, ledger, store, sent, notices, logs, mirrorCalls, revisions, events, cursor, publish,
    holdNextMirror, holdTargetInstall, renewOnce, renew, appear: () => seed(ABSENT, NATS_MACHINE),
    rejectMirrors: (error: Error) => { mirrorError = error; }, failure: () => failure };
}

describe("NATS KV notice kinds through the production file link", () => {
  it("delivers actual remote lease and presence revisions to carrier subscribers with their bucket kinds", async () => {
    const f = await fixture({ mirrorDelayMs: 0 });
    const beforeLease = f.notices.length;
    const revision = f.renewOnce();
    await until(() => f.notices.length > beforeLease, "lease revision reached the real carrier subscription");
    expect(f.port.kv.reader.lease(RENEWAL_MACHINE, RENEWER.id)?.entry.revision).toBe(revision);
    expect(f.notices.slice(beforeLease).map(notice => notice.kind)).toEqual(["lease"]);
    const beforePresence = f.notices.length, presenceKey = pKey(NATS_MACHINE, PEER.id);
    const current = f.world.presence.values.get(presenceKey)!;
    f.world.presence.put(presenceKey, current.owner, { ...participant(PEER), status: "running" }, current.revision);
    await until(() => f.notices.length > beforePresence, "presence revision reached the real carrier subscription");
    expect(f.notices.slice(beforePresence).map(notice => notice.kind)).toEqual(["presence"]);
    expect(f.failure()).toBeUndefined();
  });

  it("conservatively treats a legacy port's kindless callback as presence", async () => {
    const f = await fixture({ mirrorDelayMs: 0 });
    const watch = f.port.watchPresence.bind(f.port), observed: BridgeChange[] = [], failures: Error[] = [];
    // Model only the older callback ABI; a real remote KV revision still triggers it.
    vi.spyOn(f.port, "watchPresence").mockImplementation((changed, failed) => watch(() => changed(), failed));
    const unwatch = f.link.remote.subscribeChanges(kind => observed.push(kind), error => failures.push(error));
    try {
      f.renewOnce();
      await until(() => observed.length > 0, "legacy callback reached the production carrier");
      expect(observed).toEqual(["presence"]);
      expect(failures).toEqual([]);
      expect(f.failure()).toBeUndefined();
    } finally { unwatch(); }
  });

  it.each([false, true])("refuses an absent canonical recipient and drains valid command/ACK work during actual 300 ms lease renewals (ACK=%s)", async ack => {
    const f = await fixture({ endedPeer: ack });
    expect(f.port.resolve(ABSENT.id)).toBeUndefined();
    expect(f.link.local.mirrored(ABSENT.id)).toBe(false);
    expect((await f.link.local.presence()).reserved).not.toContain(ABSENT.id);
    expect((await f.link.remote.presence()).hosts.some(host => host.record.id === ABSENT.id)).toBe(false);
    if (ack) expect([...f.ledger.records.values()].find(record => record.requesterId === ABSENT.id)).toMatchObject({
      direction: "toFile", sourceEventId: "seed-ended", targetId: FILE.id, replyTo: ABSENT.id,
    });
    const old = f.holdNextMirror();
    const preMiss = f.link.bridge.syncPresence().catch(error => error);
    const preMissMirror = await old.entered;
    const stopRenewing = f.renew();
    try {
      const missedAt = performance.now();
      const refused = await f.publish(ack), command = await f.publish(false, PEER.id), validAck = await f.publish(true, PEER.id);
      await until(() => f.logs.some(line => line.includes(refused.id) && line.includes("membership pending")), "first cap retains the absent recipient");
      expect(f.cursor().toRemote.after).toBeLessThan(refused.sequence);
      expect(f.sent).toEqual([]);
      const source = f.events().find(event => event.id === refused.id);
      expect(source).toMatchObject({ from: FILE, to: ABSENT.id, data: ack ? { accepted: true, targetId: FILE.id } : { message: "retained body" } });

      const fresh = f.holdNextMirror();
      old.release();
      expect(await preMiss).toBeUndefined();
      const postMissMirror = await fresh.entered;
      expect(preMissMirror.startedAt).toBeLessThan(missedAt);
      expect(postMissMirror.startedAt).toBeGreaterThanOrEqual(missedAt);
      await sleep(1_250); // Cross the post-miss cap while that sample is still unfinished.
      expect(postMissMirror.result).toBe("pending");
      expect(f.cursor().toRemote.after, "a completed pre-miss absence alone cannot consume the event").toBeLessThan(refused.sequence);
      expect(f.sent).toEqual([]);
      fresh.release();

      await until(() => f.cursor().toRemote.after >= validAck.sequence, "completed post-miss absence releases the cursor and valid suffix despite remote renewals");
      expect(postMissMirror.result).toBe("fulfilled");
      const during = f.notices.filter(notice => notice.at >= postMissMirror.startedAt && notice.at <= postMissMirror.settledAt!);
      expect(during.length, "at least three actual carrier notices occurred inside the completed post-miss mirror").toBeGreaterThanOrEqual(3);
      expect(during.map(notice => notice.kind)).toEqual(Array(during.length).fill("lease"));
      const revisionsDuring = f.revisions.filter(write => write.at >= postMissMirror.startedAt && write.at <= postMissMirror.settledAt!);
      expect(revisionsDuring.length, "the notice sequence was backed by repeated real KV lease revisions").toBeGreaterThanOrEqual(3);
      expect(revisionsDuring.every((write, index) => index === 0 || write.revision > revisionsDuring[index - 1]!.revision)).toBe(true);
      expect(f.mirrorCalls.every(call => call.result !== "rejected")).toBe(true);
      expect(f.mirrorCalls.filter(call => call.result === "fulfilled").every(call => call.settledAt! - call.startedAt >= 1_100), "no fast retry supplies absence").toBe(true);
      expect(f.sent.map(envelope => envelope.id)).toEqual([bridgeNatsEnvelopeId(LINK, command.id), "ack:seed-live"]);
      expect(f.sent[0]).toMatchObject({ kind: "command", from: FILE, targetId: PEER.id, body: { message: "retained body" } });
      expect(f.sent[1]).toMatchObject({ kind: "ack", from: FILE, to: { machine: NATS_MACHINE, hostId: PEER.id },
        body: { commandId: "seed-live-command", accepted: true, targetId: FILE.id } });
      expect(f.logs.some(line => line.includes(refused.id) && line.includes("membership pending"))).toBe(true);
      expect(f.logs.some(line => [command.id, validAck.id].some(id => line.includes(id)) && /: dropped |: refused /.test(line))).toBe(false);
      expect(f.events().find(event => event.id === refused.id)).toEqual(source);
      expect(f.link.local.holds(PEER.id)).toBe(true);
      expect(f.link.local.holds(ABSENT.id)).toBe(false);
      expect(f.failure()).toBeUndefined();
    } finally { stopRenewing(); }
  }, 20_000);

  it.each(["pre-miss", "post-miss"] as const)("does not drop a newly present target using a %s absence while its canonical installer is held", async timing => {
    const f = await fixture(), old = f.holdNextMirror();
    const preMiss = f.link.bridge.syncPresence().catch(error => error);
    const preMissMirror = await old.entered;
    const event = await f.publish(false);
    await until(() => f.logs.some(line => line.includes(event.id) && line.includes("membership pending")), "new target missed while the old sample was pending");
    let sampledAbsence: MirrorCall;
    let releaseAbsence: () => void;
    if (timing === "post-miss") {
      const fresh = f.holdNextMirror();
      old.release();
      expect(await preMiss).toBeUndefined();
      sampledAbsence = await fresh.entered;
      releaseAbsence = fresh.release;
    } else {
      sampledAbsence = preMissMirror;
      releaseAbsence = old.release;
    }
    const install = f.holdTargetInstall(), before = f.notices.length;
    f.appear(); // Actual lease + presence bucket puts, after the held sample read absence.
    await until(() => install.entered(), "the watch reaches the real new-target installer");
    expect(f.notices.slice(before).map(notice => notice.kind)).toContain("presence");
    expect(f.port.resolve(ABSENT.id)?.route.live).toBe(true);
    expect(f.link.local.holds(ABSENT.id)).toBe(false);
    releaseAbsence();
    expect(await preMiss).toBeUndefined();
    await until(() => sampledAbsence.result === "fulfilled", "the stale absence aggregate completed successfully");
    await sleep(1_250); // A fresh aggregate cannot commit its new authority while the installer is held.
    expect(f.cursor().toRemote.after).toBeLessThan(event.sequence);
    expect(f.sent).toEqual([]);
    expect(f.link.local.holds(ABSENT.id)).toBe(false);
    expect(f.mirrorCalls.some(call => call.startedAt > sampledAbsence.startedAt)).toBe(true);
    install.release();
    await until(() => f.cursor().toRemote.after >= event.sequence, "a fresh canonical install delivers the retained event instead of dropping it");
    expect(f.link.local.holds(ABSENT.id)).toBe(true);
    expect(f.sent.map(envelope => envelope.id)).toEqual([bridgeNatsEnvelopeId(LINK, event.id)]);
    expect(f.sent[0]).toMatchObject({ targetId: ABSENT.id, body: { message: "retained body" } });
    expect(f.logs.some(line => line.includes(event.id) && /: dropped |: refused /.test(line))).toBe(false);
    expect(f.failure()).toBeUndefined();
  }, 20_000);

  it("does not consume absent membership or its valid suffix when the slow outbound aggregates keep failing", async () => {
    const f = await fixture();
    f.rejectMirrors(new BridgeMirrorDeferredError(false, 25));
    const stopRenewing = f.renew();
    try {
      const event = await f.publish(false), later = await f.publish(false, PEER.id);
      await until(() => f.mirrorCalls.filter(call => call.result === "rejected").length >= 2, "multiple completed slow aggregates rejected instead of proving absence");
      expect(f.cursor().toRemote.after).toBeLessThan(event.sequence);
      expect(f.cursor().toRemote.after).toBeLessThan(later.sequence);
      expect(f.sent).toEqual([]);
      expect(f.link.local.holds(ABSENT.id)).toBe(false);
      expect(f.logs.some(line => line.includes(event.id) && /: dropped |: refused /.test(line))).toBe(false);
      expect(f.failure()).toBeUndefined();
    } finally { stopRenewing(); }
  }, 20_000);

  it.each([10, 30])("refuses an absent recipient and drains a valid suffix at the aggregate 180 s / K real KV presence cadence (K=%i)", async k => {
    const presenceEveryMs = 180_000 / k;
    // This is the aggregate cadence of K staggered 180 s roots, not a fixture containing K roots.
    // The extra 50 ms is timer-quantization headroom for the >=1,200 ms elapsed assertion below.
    const f = await fixture({ mirrorDelayMs: 1_250 });
    const held: Array<ReturnType<typeof f.holdNextMirror>> = [];
    const aggregates: Array<Promise<unknown>> = [];
    const presenceWrites: Array<{ at: number; revision: number; noticeAfter: number }> = [];
    const firstPresenceWrite = deferred<{ at: number; revision: number; noticeAfter: number }>();
    let presenceTimer: ReturnType<typeof setInterval> | undefined;
    let peerLeaseTimer: ReturnType<typeof setInterval> | undefined;
    let stopRenewing: (() => void) | undefined;
    // Native NATS leases last only 15 s: the literal 18 s presence interval must not expire PEER.
    // These are real KV lease puts; their watch events, not a fabricated callback, renew its mirror.
    const renewPeer = () => {
      const leaseKey = hKey(NATS_MACHINE, PEER.id), current = f.world.leases.values.get(leaseKey)!;
      f.world.leases.put(leaseKey, current.owner, natsLease(PEER.id, NATS_INCARNATION, Date.now()), current.revision);
    };
    try {
      expect(f.world.leases.ttlMs).toBe(15_000);
      expect(f.port.resolve(ABSENT.id)).toBeUndefined();
      expect(f.link.local.holds(ABSENT.id)).toBe(false);
      const old = f.holdNextMirror();
      held.push(old);
      const preMiss = f.link.bridge.syncPresence().catch(error => error);
      aggregates.push(preMiss);
      const preMissMirror = await old.entered;
      // Match the original slow cases: do not race watch renewal against the pre-miss bootstrap.
      stopRenewing = f.renew();
      peerLeaseTimer = setInterval(renewPeer, 5_000);
      const missedAt = performance.now();
      const refused = await f.publish(false), command = await f.publish(false, PEER.id), validAck = await f.publish(true, PEER.id);
      const source = f.events().find(event => event.id === refused.id);
      expect(source).toMatchObject({ from: FILE, to: ABSENT.id, data: { message: "retained body" } });
      await until(() => f.logs.some(line => line.includes(refused.id) && line.includes("membership pending")), "the absent recipient is retained before a completed post-miss sample");
      expect(f.cursor().toRemote.after).toBeLessThan(refused.sequence);
      expect(f.sent).toEqual([]);

      const invalidated = f.holdNextMirror();
      held.push(invalidated);
      old.release();
      expect(await preMiss).toBeUndefined();
      const postMissMirror = await invalidated.entered;
      const invalidatedAggregate = f.link.bridge.syncPresence().catch(error => error);
      aggregates.push(invalidatedAggregate);
      expect(preMissMirror.startedAt).toBeLessThan(missedAt);
      expect(postMissMirror.startedAt).toBeGreaterThanOrEqual(missedAt);
      const presenceKey = pKey(RENEWAL_MACHINE, RENEWER.id);
      // A literal 18 s / 6 s interval drives real remote presence KV revisions through port/carrier.
      // Keep it recurring through the final drain; no direct presence notice or fast retry is injected.
      presenceTimer = setInterval(() => {
        const current = f.world.presence.values.get(presenceKey)!;
        const at = performance.now(), noticeAfter = f.notices.length;
        const revision = f.world.presence.put(presenceKey, current.owner, {
          ...participant(RENEWER), status: presenceWrites.length % 2 === 0 ? "running" : "idle",
        }, current.revision);
        const write = { at, revision, noticeAfter };
        presenceWrites.push(write);
        firstPresenceWrite.resolve(write);
      }, presenceEveryMs);
      await sleep(1_300); // Cross the 1 s foreground cap, still without a completed post-miss aggregate.
      expect(postMissMirror.result).toBe("pending");
      expect(f.cursor().toRemote.after).toBeLessThan(refused.sequence);
      expect(f.sent).toEqual([]);

      const first = await firstPresenceWrite.promise;
      await until(() => f.notices.slice(first.noticeAfter).some(notice => notice.kind === "presence" && notice.at >= first.at), "an actual cadence presence put reaches the carrier while the post-miss aggregate is unfinished");
      const presenceNotice = f.notices.slice(first.noticeAfter).find(notice => notice.kind === "presence" && notice.at >= first.at)!;
      expect(first.at).toBeGreaterThan(postMissMirror.startedAt);
      expect(f.world.presence.values.get(presenceKey)!.revision).toBeGreaterThanOrEqual(first.revision);
      expect(postMissMirror.result).toBe("pending");
      expect(f.cursor().toRemote.after).toBeLessThan(refused.sequence);
      expect(f.sent).toEqual([]);
      expect(f.port.resolve(PEER.id)?.route.live).toBe(true);
      expect(f.link.local.holds(PEER.id)).toBe(true);

      const fresh = f.holdNextMirror();
      held.push(fresh);
      invalidated.release();
      expect(await invalidatedAggregate).toBeUndefined();
      expect(postMissMirror.result).toBe("fulfilled");
      const freshMirror = await fresh.entered;
      const freshAggregate = f.link.bridge.syncPresence().catch(error => error);
      aggregates.push(freshAggregate);
      expect(presenceNotice.at).toBeLessThanOrEqual(postMissMirror.settledAt!);
      expect(freshMirror.startedAt).toBeGreaterThan(presenceNotice.at);
      await sleep(1_300);
      expect(freshMirror.result).toBe("pending");
      expect(f.cursor().toRemote.after, "a completed but presence-invalidated absence must not consume the event").toBeLessThan(refused.sequence);
      expect(f.sent).toEqual([]);
      expect(f.logs.some(line => line.includes(refused.id) && line.includes("not addressed across:"))).toBe(false);
      fresh.release();
      expect(await freshAggregate).toBeUndefined();
      await until(() => f.cursor().toRemote.after >= validAck.sequence, "fresh successful evidence refuses the absent recipient and drains the valid suffix at the realistic presence cadence");
      expect(freshMirror.result).toBe("fulfilled");
      // Outbound missing-recipient refusal is reported as "not addressed across", not a send.
      expect(f.logs.filter(line => line.includes(refused.id) && line.includes("toRemote: not addressed across:"))).toHaveLength(1);
      expect(f.sent.map(envelope => envelope.id)).toEqual([bridgeNatsEnvelopeId(LINK, command.id), "ack:seed-live"]);
      expect(f.sent[0]).toMatchObject({ kind: "command", from: FILE, targetId: PEER.id, body: { message: "retained body" } });
      expect(f.sent[1]).toMatchObject({ kind: "ack", from: FILE, to: { machine: NATS_MACHINE, hostId: PEER.id },
        body: { commandId: "seed-live-command", accepted: true, targetId: FILE.id } });
      expect(f.logs.some(line => [command.id, validAck.id].some(id => line.includes(id)) && /: dropped |: refused /.test(line))).toBe(false);
      expect(f.revisions.filter(write => write.at >= postMissMirror.startedAt && write.at <= postMissMirror.settledAt!).length).toBeGreaterThanOrEqual(3);
      expect(f.mirrorCalls.every(call => call.result !== "rejected")).toBe(true);
      expect(f.mirrorCalls.filter(call => call.result === "fulfilled").every(call => call.settledAt! - call.startedAt >= 1_200), "every completed mirror, including retries, remained slow").toBe(true);
      expect(f.events().find(event => event.id === refused.id)).toEqual(source);
      expect(f.port.resolve(ABSENT.id)).toBeUndefined();
      expect(f.port.resolve(PEER.id)?.route.live).toBe(true);
      expect(f.link.local.holds(ABSENT.id)).toBe(false);
      expect(f.link.local.holds(PEER.id)).toBe(true);
      expect(f.failure()).toBeUndefined();
    } finally {
      clearInterval(presenceTimer);
      clearInterval(peerLeaseTimer);
      stopRenewing?.();
      for (const gate of held) gate.release();
      await Promise.all(aggregates); // Await the real aggregates owning the entered mirror gates.
    }
  }, 60_000);
});
