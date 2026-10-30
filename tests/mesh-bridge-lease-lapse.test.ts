import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Keep: recorded command loss at the ownership trust boundary. Only time/presence sampling is
// injected; real stores, append fences, durable receipts, cursors and control handlers decide delivery.
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const native = async (store: MeshStore, name: string) => {
  const now = Date.now(), id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  await store.put({ key: key("hosts", id), identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 600_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 600_000 });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: `fixture:${id}`, name, label: name, status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return identity;
};
const all = (store: MeshStore) => store.read({ after: 0, limit: 100 });
const commands = (store: MeshStore) => all(store).filter(event => event.topic === "fabric.control.command" && event.kind === "followUp");
const stamp = (event: MeshEvent) => (event.data as { bridge?: { id: string } } | undefined)?.bridge?.id;
const fixture = async (stateBackend: "file" | "sqlite" = "file") => {
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lease-lapse-"));
  const hub = new MeshStore(path.join(root, "hub"), 65_536, 100, { stateBackend });
  const far = new MeshStore(path.join(root, "far"), 65_536, 100, { stateBackend });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const recipient = await native(hub, "recipient"), sender = await native(far, "sender");
  const local = new StoreBridgeSide(hub, "far", () => now), remote = new StoreBridgeSide(far, "hub", () => now);
  const presence = remote.presence.bind(remote);
  let sampled = false;
  // Legacy observations have no sampled deadline. An expired local mirror still fails the
  // same commit fence, and must not require the new-peer sampled-deadline heuristic to survive.
  vi.spyOn(remote, "presence").mockImplementation(async () => {
    const snapshot = await presence();
    return sampled ? snapshot : { ...snapshot, hosts: snapshot.hosts.map(({ observedAt: _, ...host }) => host) };
  });
  const logs: string[] = [], cursorPath = path.join(root, "cursor.json");
  const options = { localName: "hub", remoteName: "far", local, remote, cursorPath,
    log: (line: string) => logs.push(line) };
  let bridge = new MeshBridge(options);
  await bridge.start();
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as {
    toLocal: { after: number; offset?: number; leaseLapse?: { sequence: number; id: string; startedAt: number } };
  };
  const advance = (elapsed: number) => {
    now += elapsed;
    for (const [store, who] of [[far, sender], [hub, recipient]] as const) {
      writeHostLease(store.root, { ...readHostLease(store.root, who.id)!, updatedAt: now, expiresAt: now + 600_000 });
    }
  };
  const publish = local.publish.bind(local);
  let fault: "lapse" | "takeover" | "fence" | undefined = "lapse", attempts = 0;
  vi.spyOn(local, "publish").mockImplementation(async (...args) => {
    if (args[0].topic === "fabric.control.command" && args[0].kind === "followUp") {
      attempts++;
      if (fault) {
        advance(BRIDGE_LEASE_MS + 1);
        if (fault === "takeover" && attempts === 2) await native(hub, "sender");
        if (fault === "fence") await local.fence();
      }
    }
    return publish(...args);
  });
  const send = (tag = "retained", extra: Record<string, unknown> = {}) => far.publish({
    topic: "fabric.control.command", kind: "followUp", from: sender, to: recipient.id,
    dedupeKey: `source:${tag}`, data: {
      version: 1, commandId: tag, targetId: recipient.id, replyTo: sender.id, operation: "followUp",
      ownerIncarnation: `fixture:${recipient.id}`, requestedAt: now, deadlineAt: now + 300_000,
      destinationRemoteHost: "hub", message: tag, ...extra,
    },
  });
  return { hub, far, local, remote, sender, recipient, logs, cursor, send, advance,
    get bridge() { return bridge; }, get now() { return now; }, get attempts() { return attempts; },
    fault: (value: typeof fault) => { fault = value; }, sampled: () => { sampled = true; },
    reconnect: async () => { bridge = new MeshBridge(options); await bridge.start(); },
  };
};
const refusalLines = (logs: string[], id: string) => logs.filter(line => line.includes(id) && /refused|dropped/.test(line));

describe("bounded own-root lease lapse", () => {
  it.each(["file", "sqlite"] as const)("retains a mid-drain lapse and its same-key retry, then delivers once (%s)", async backend => {
    const f = await fixture(backend), sent = await f.send();
    const before = f.cursor().toLocal;
    // Linux namespace preparation creates this incidentally; Windows need not prepare it
    // before a refused append. Seed an empty directory so the assertion tests no receipts,
    // not the platform's lazy namespace preparation policy.
    fs.mkdirSync(path.join(f.hub.root, "event-receipts"), { recursive: true });
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(f.attempts).toBe(2); // one refresh and retry, not an inline retry loop
    expect(f.cursor().toLocal.after).toBe(before.after);
    expect(f.cursor().toLocal.offset).toBe(before.offset);
    expect(commands(f.hub)).toEqual([]);
    expect(fs.readdirSync(path.join(f.hub.root, "event-receipts"))).toEqual([]);
    expect(refusalLines(f.logs, sent.id)).toEqual([]);
    const retried = await f.send();
    expect(retried.id).toBe(sent.id); // real source receipt, not a freshly keyed happy path
    expect(commands(f.far)).toHaveLength(1);
    f.fault(undefined);
    await f.bridge.syncPresence();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(commands(f.hub).map(stamp)).toEqual([sent.id]);
    expect(commands(f.hub)[0]).toMatchObject({ data: { commandId: "retained", message: "retained" } });
    expect(f.cursor().toLocal.after).toBe(sent.sequence);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(commands(f.hub)).toHaveLength(1);
  });

  it.each(["takeover", "fence"] as const)("still refuses a proven %s with its event id", async fault => {
    const f = await fixture(), sent = await f.send();
    f.fault(fault);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(f.cursor().toLocal.after).toBe(sent.sequence);
    expect(commands(f.hub)).toEqual([]);
    expect(refusalLines(f.logs, sent.id)).toHaveLength(1);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
  });

  it.each([false, true])("refuses a continued lapse once at the total bound (reconnect=%s), without losing later events", async reconnect => {
    const f = await fixture(), first = await f.send("blocked");
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    const retained = f.cursor().toLocal.leaseLapse!;
    expect(retained).toMatchObject({ sequence: first.sequence, id: first.id, startedAt: f.now });
    // Samples/renewals and a reconnect must not restart this event's total window.
    f.advance(BRIDGE_LEASE_MS * 2 + 1);
    if (reconnect) await f.reconnect();
    else await f.bridge.syncPresence();
    expect(f.cursor().toLocal.leaseLapse).toEqual(retained);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(f.cursor().toLocal.after).toBe(first.sequence);
    expect(refusalLines(f.logs, first.id)).toHaveLength(1);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
    f.fault(undefined);
    const next = await f.send("after-bound");
    await f.bridge.syncPresence();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(commands(f.hub).map(stamp)).toEqual([next.id]);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(refusalLines(f.logs, first.id)).toHaveLength(1);
  });

  it("does not restart the lapse window when new samples change the refusal to sampled-stale", async () => {
    const f = await fixture(), sent = await f.send();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    const retained = f.cursor().toLocal.leaseLapse!;
    f.sampled(); f.advance(5_000);
    await f.bridge.syncPresence();
    expect(f.cursor().toLocal.leaseLapse).toEqual(retained);
    // Both new commit waits expire their samples too. Their elapsed time crosses the
    // original window: it must refuse now, not grant another 30 s for the new sample.
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(f.now).toBeGreaterThanOrEqual(retained.startedAt + 2 * BRIDGE_LEASE_MS);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
    expect(refusalLines(f.logs, sent.id)).toHaveLength(1);
    expect(commands(f.hub)).toEqual([]);
  });

  it("a still-refused prefilter boundary expires without starting another lapse window", async () => {
    const f = await fixture(), sent = await f.send();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    f.advance(2 * BRIDGE_LEASE_MS);
    f.fault(undefined);
    const mirror = f.local.mirror.bind(f.local);
    vi.spyOn(f.local, "mirror").mockImplementation(async (...args) => {
      await mirror(...args);
      writeHostLease(f.hub.root, { ...readHostLease(f.hub.root, f.sender.id)!, expiresAt: f.now - 1 });
    });
    await f.bridge.syncPresence();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(f.attempts).toBe(2); // no additional publish past the refused membership prefilter
    expect(refusalLines(f.logs, sent.id)).toHaveLength(1);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
  });

  it("a lapsed own mirror is not uncertainty when another link currently reserves its root", async () => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    expect(f.local.leaseLapsed(f.sender.id, f.sender)).toBe(true);
    const other: MeshIdentity = { id: "session:reservation-00000000", name: "main", kind: "main" };
    await f.hub.put({ key: key("hosts", other.id), identity: other, value: {
      format: 1, id: other.id, rootId: f.sender.id, identity: other, remoteHost: "other-link",
      startedAt: f.now, updatedAt: f.now, expiresAt: f.now + 600_000,
    } });
    expect(f.local.leaseLapsed(f.sender.id, f.sender)).toBe(false);
    const sent = await f.send();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(commands(f.hub)).toEqual([]);
    expect(refusalLines(f.logs, sent.id)).toHaveLength(1);
  });

  it("never defers a foreign ACK target just because its sender's own mirror lapsed", async () => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    expect(f.local.leaseLapsed(f.sender.id, f.sender)).toBe(true);
    const sent = await f.far.publish({ topic: "fabric.control.ack", kind: "ack", from: f.sender, to: f.recipient.id,
      data: { version: 1, commandId: "foreign-target", targetId: f.recipient.id, accepted: true } });
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(all(f.hub)).toEqual([]);
    expect(refusalLines(f.logs, sent.id)).toHaveLength(1);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
  });

  it("retains the consumed prefix and queues following events behind the lapse without losing either", async () => {
    const f = await fixture();
    f.fault(undefined);
    const first = await f.send("prefix");
    expect(await f.bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    f.fault("lapse");
    const held = await f.send("held"), next = await f.send("following");
    // Force the single-event fence so this test controls which event experiences the lapse.
    vi.spyOn(f.local, "publishBatchWithReceipt").mockImplementation(async entries => {
      const result = [];
      for (const entry of entries) result.push(await f.local.publishWithReceipt(entry.event, entry.held));
      return result;
    });
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(f.cursor().toLocal.after).toBe(first.sequence);
    expect(commands(f.hub).map(stamp)).toEqual([first.id]);
    expect(refusalLines(f.logs, held.id)).toEqual([]);
    f.fault(undefined);
    await f.bridge.syncPresence();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 2, dropped: 0 });
    expect(commands(f.hub).map(stamp)).toEqual([first.id, held.id, next.id]);
  });

  it("keeps structural destination refusal hard even when the sender mirror has expired", async () => {
    const f = await fixture(), sent = await f.send("wrong-link", { destinationRemoteHost: "elsewhere" });
    f.advance(BRIDGE_LEASE_MS + 1);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(commands(f.hub)).toEqual([]);
    expect(refusalLines(f.logs, sent.id)).toHaveLength(1);
    expect(f.cursor().toLocal.leaseLapse).toBeUndefined();
  });

  it("lands a real sender's same logical key after disconnect/retry without recording a failed admission as done", async () => {
    const f = await fixture();
    const receive = vi.fn(() => ({ accepted: true, messageId: "delivered-once" }));
    const receiver = new FabricControlPlane(f.hub, f.recipient, { enabled: true, hostId: f.recipient.id,
      ownerIncarnation: `fixture:${f.recipient.id}`, pollMs: 10 });
    const plane = () => new FabricControlPlane(f.far, f.sender, { enabled: true, hostId: f.sender.id, pollMs: 10 });
    let sender = plane();
    cleanups.push(async () => { await sender.close(); await receiver.close(); });
    receiver.start(receive); sender.start(() => ({ accepted: false }));
    const request = () => sender.request(f.recipient.id, f.recipient.id, "followUp", {
      ownerIncarnation: `fixture:${f.recipient.id}`, message: "same-key-body",
    }, f.recipient.id, { idempotencyKey: "same-logical-key", timeoutMs: 300_000 });
    const first = request().catch(error => error);
    await vi.waitFor(() => expect(commands(f.far)).toHaveLength(1));
    const original = commands(f.far)[0]!;
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(receive).not.toHaveBeenCalled();
    expect(commands(f.hub)).toEqual([]);
    await sender.close();
    expect(await first).toBeInstanceOf(Error);
    sender = plane(); sender.start(() => ({ accepted: false }));
    const retry = request(); void retry.catch(() => undefined);
    await vi.waitFor(() => expect(commands(f.far)).toHaveLength(2));
    const repeated = commands(f.far)[1]!;
    expect(repeated.id).not.toBe(original.id); // file control retries append a new event
    expect(repeated.data).toMatchObject({ commandId: (original.data as { commandId: string }).commandId });
    f.fault(undefined);
    await f.bridge.syncPresence();
    await f.bridge.step();
    await vi.waitFor(() => expect(all(f.hub).filter(event => event.topic === "fabric.control.ack")).toHaveLength(1));
    await f.bridge.step();
    await expect(retry).resolves.toMatchObject({ acknowledged: true, messageId: "delivered-once" });
    expect(receive).toHaveBeenCalledOnce();
    expect(commands(f.hub).map(stamp)).toEqual([original.id]);
    // Successful destination receipts still dedupe the same logical command, and fingerprints
    // still reject changed input. Do not erase receipts to make a refused retry deliver.
    const bridged = commands(f.hub)[0]!;
    const publication = { topic: bridged.topic, kind: bridged.kind, from: f.sender, to: f.recipient.id,
      data: bridged.data as Record<string, unknown> & { bridge: { from: string; id: string } } };
    const receipt = await f.local.publish(publication, [f.sender.id]);
    expect(receipt.sequence).toBe(bridged.sequence);
    await expect(f.local.publish({ ...publication, data: { ...publication.data, message: "changed" } }, [f.sender.id]))
      .rejects.toMatchObject({ code: "FABRIC_IDEMPOTENCY_KEY_REUSED" });
    expect(receive).toHaveBeenCalledOnce();
    expect(commands(f.hub)).toHaveLength(1);
  });
});
