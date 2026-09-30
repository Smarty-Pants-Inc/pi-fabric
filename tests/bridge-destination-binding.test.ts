import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { MeshBridge, StoreBridgeSide, bridgeStampOf, type BridgePresence, type BridgeRead } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const hub: MeshIdentity = { id: "session:hub00000", name: "hub", kind: "main" };
const owner: MeshIdentity = { id: "session:remote00", name: "remote", kind: "main" };
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
};

// Hold the actual source-log read, not target.publish: the dispatcher must decide before
// exposing a payload to a transport. All other operations use the real store/side/cursors.
class HeldReadSide extends StoreBridgeSide {
  hold: { entered: ReturnType<typeof deferred>; release: ReturnType<typeof deferred> } | undefined;
  async read(after: number): Promise<BridgeRead> {
    const hold = this.hold;
    if (hold) {
      this.hold = undefined;
      hold.entered.resolve();
      await hold.release.promise;
    }
    return super.read(after);
  }
}

async function native(store: MeshStore, identity: MeshIdentity): Promise<void> {
  const now = Date.now();
  const host = {
    format: 1, id: identity.id, rootId: identity.id, identity,
    startedAt: now, updatedAt: now, expiresAt: now + 600_000,
  } as BridgePresence["hosts"][number]["record"];
  const participant = {
    format: 1, id: identity.id, kind: "root", name: identity.name,
    ownerHostId: identity.id, ownerIdentityId: identity.id, rootId: identity.id,
    sessionId: identity.id.slice("session:".length), label: identity.name,
    startedAt: now, updatedAt: now,
  } as BridgePresence["participants"][number];
  await store.put({ key: key("topology/hosts/", identity.id), value: host, identity });
  await store.put({ key: key("topology/participants/", identity.id), value: participant, identity });
}

async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-binding-"));
  roots.push(root);
  const local = new MeshStore(path.join(root, "hub"), 64 * 1024, 1_000);
  const originalRemote = new MeshStore(path.join(root, "forge"), 64 * 1024, 1_000);
  const replacementRemote = new MeshStore(path.join(root, "ryzen2"), 64 * 1024, 1_000);
  await native(local, hub);
  // Two different native meshes deliberately advertise the same canonical root X.
  await native(originalRemote, owner);
  await native(replacementRemote, owner);
  const originalLocal = new HeldReadSide(local, "Forge");
  const replacementLocal = new HeldReadSide(local, "ryzen2");
  const originalSide = new StoreBridgeSide(originalRemote, "Dev1");
  const replacementSide = new StoreBridgeSide(replacementRemote, "Dev1");
  const make = (remoteName: string, side: HeldReadSide, remote: StoreBridgeSide) => new MeshBridge({
    localName: "Dev1", remoteName, local: side, remote,
    cursorPath: path.join(root, `${remoteName}.cursor.json`), presenceMs: 60_000,
  });
  const original = make("Forge", originalLocal, originalSide);
  const replacement = make("ryzen2", replacementLocal, replacementSide);
  // Both dispatchers establish real cursors before publication. The second is already
  // started while Forge holds X; only after withdrawal can its presence be admitted.
  await original.start();
  await original.syncPresence();
  await replacement.start();
  await replacement.syncPresence();
  expect(originalLocal.holds(owner.id)).toBe(true);
  expect(replacementLocal.holds(owner.id)).toBe(false);
  const takeover = async () => {
    await originalLocal.withdraw();
    await replacement.syncPresence();
    expect(originalLocal.holds(owner.id)).toBe(false);
    expect(replacementLocal.holds(owner.id)).toBe(true);
  };
  const command = (kind: string, data: Record<string, unknown>, from = hub, to = owner.id, store = local) =>
    store.publish({ topic: "fabric.control.command", kind, from, to, text: "private payload", data });
  const events = (store: MeshStore) => store.read({ after: 0, limit: store.maxReadEvents });
  const remoteCommands = (store: MeshStore) => events(store).filter((event) => event.topic === "fabric.control.command");
  return { root, local, originalRemote, replacementRemote, originalLocal, replacementLocal,
    original, replacement, takeover, command, events, remoteCommands };
}

const bound = (operation: string) => ({ version: 1, commandId: operation === "cancel" ? "cancel-fixed-origin" : "request-fixed-origin",
  operation: operation === "message" ? "steer" : operation, targetId: owner.id, replyTo: hub.id,
  requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
  ...(operation === "cancel" ? { cancelCommandId: "request-fixed-origin" } : { message: "private payload" }),
  destinationRemoteHost: "Forge" });

describe("bridge immutable command destination binding", () => {
  it("two actual bridges: takeover while publication is held discloses neither payload nor cancel/replay", async () => {
    const f = await fixture();
    const publication = deferred();
    const pending = publication.promise.then(() => f.command("request", bound("message")));
    await f.takeover();
    publication.resolve();
    await pending;
    await f.command("cancel", bound("cancel"));
    await f.command("request", { ...bound("message"), commandId: "bounded-retry" });
    await f.original.step();
    expect((await f.replacement.step()).dropped).toBe(3);
    expect(f.remoteCommands(f.replacementRemote)).toEqual([]);
    expect(f.remoteCommands(f.originalRemote)).toEqual([]);
    // A saved cursor/restart must not turn the refusal into a replay.
    const restarted = new MeshBridge({ ...f.replacement.options });
    await restarted.start();
    await restarted.step();
    expect(f.remoteCommands(f.replacementRemote)).toEqual([]);
    // Positive control: the replacement really can dispatch X, but only its own binding.
    const fresh = await f.command("request", { requestId: "fresh-replacement", destinationRemoteHost: "ryzen2" });
    expect((await restarted.step()).toRemote).toBe(1);
    expect(f.remoteCommands(f.replacementRemote).map((event) => bridgeStampOf(event)?.id)).toEqual([fresh.id]);
  });

  it("two actual bridges: takeover while forwarding is held discloses neither payload nor cancel/replay", async () => {
    const f = await fixture();
    await f.command("request", bound("message"));
    const originalHold = { entered: deferred(), release: deferred() };
    const replacementHold = { entered: deferred(), release: deferred() };
    f.originalLocal.hold = originalHold;
    f.replacementLocal.hold = replacementHold;
    const passes = [f.original.step(), f.replacement.step()];
    try {
      await Promise.all([originalHold.entered.promise, replacementHold.entered.promise]);
      await f.takeover();
      await f.command("cancel", bound("cancel"));
      await f.command("request", { ...bound("message"), commandId: "bounded-retry" });
    } finally {
      originalHold.release.resolve();
      replacementHold.release.resolve();
      await Promise.all(passes);
    }
    expect(f.remoteCommands(f.replacementRemote)).toEqual([]);
    expect(f.remoteCommands(f.originalRemote)).toEqual([]);
    await f.replacement.step();
    expect(f.remoteCommands(f.replacementRemote)).toEqual([]);
    const fresh = await f.command("request", { requestId: "fresh-replacement", destinationRemoteHost: "ryzen2" });
    expect((await f.replacement.step()).toRemote).toBe(1);
    expect(f.remoteCommands(f.replacementRemote).map((event) => bridgeStampOf(event)?.id)).toEqual([fresh.id]);
  });

  it("keeps original-link commands/cancels and legacy commands, with unchanged binding on the wire", async () => {
    const f = await fixture();
    const payload = await f.command("request", bound("message"));
    const cancel = await f.command("cancel", bound("cancel"));
    const legacy = await f.command("request", { requestId: "legacy" });
    expect((await f.original.step()).toRemote).toBe(3);
    const received = f.remoteCommands(f.originalRemote);
    expect(received.map((event) => bridgeStampOf(event)?.id)).toEqual([payload.id, cancel.id, legacy.id]);
    expect(received.slice(0, 2).map((event) => (event.data as Record<string, unknown>).destinationRemoteHost)).toEqual(["Forge", "Forge"]);
    await f.original.step();
    expect(f.remoteCommands(f.originalRemote)).toHaveLength(3);
  });

  it("refuses native, malformed and other-link commands in both directions; absent stays compatible", async () => {
    const f = await fixture();
    const invalid = [null, false, 7, {}, [], "", "ryzen2", "Dev1"];
    for (const destinationRemoteHost of invalid) await f.command("request", { destinationRemoteHost });
    // A native command stays in its local log, never gaining a bridge stamp.
    const nativeEvent = await f.command("request", { destinationRemoteHost: null }, hub, hub.id);
    expect((await f.original.step()).toRemote).toBe(0);
    expect(f.remoteCommands(f.originalRemote)).toEqual([]);
    expect(bridgeStampOf(f.events(f.local).find((event) => event.id === nativeEvent.id)!)).toBeUndefined();
    for (const destinationRemoteHost of [null, false, 7, {}, [], "", "Forge", "ryzen2"])
      await f.command("cancel", { destinationRemoteHost }, owner, hub.id, f.originalRemote);
    const inbound = await f.command("request", { destinationRemoteHost: "Dev1" }, owner, hub.id, f.originalRemote);
    const legacy = await f.command("request", {}, owner, hub.id, f.originalRemote);
    expect((await f.original.step()).toLocal).toBe(2);
    const imported = f.events(f.local).filter((event) => bridgeStampOf(event));
    expect(imported.map((event) => bridgeStampOf(event)?.id)).toEqual([inbound.id, legacy.id]);
  });

  it("does not interpret work/ACK business data as routing; original committed ACK survives withdrawal", async () => {
    const f = await fixture();
    const work = await f.local.publish({ topic: "fleet.work.test", kind: "work", from: hub, to: owner.id,
      data: { destinationRemoteHost: null } });
    expect((await f.original.step()).toRemote).toBe(1);
    expect(f.events(f.originalRemote).some((event) => bridgeStampOf(event)?.id === work.id)).toBe(true);
    const ack = await f.originalRemote.publish({ topic: "fabric.control.ack", kind: "ack", from: owner, to: hub.id,
      data: { requestId: "request-fixed-origin", targetId: owner.id, destinationRemoteHost: "business-only", accepted: true } });
    expect((await f.original.step()).toLocal).toBe(1);
    await f.takeover();
    await f.replacement.step();
    const committed = f.events(f.local).filter((event) => bridgeStampOf(event)?.id === ack.id);
    expect(committed).toHaveLength(1);
    expect(bridgeStampOf(committed[0]!)).toEqual({ from: "Forge", id: ack.id });
    expect(f.events(f.replacementRemote).some((event) => bridgeStampOf(event)?.id === ack.id)).toBe(false);
  });
});
