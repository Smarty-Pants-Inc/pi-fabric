import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide, type BridgeSide } from "../src/mesh/bridge.js";
import { handOffNativeIdentity } from "../src/mesh/bridge-handoff.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// #15305: the shared outbound log must not turn proven native/other-link traffic into
// two-sided presence writes. Real SQLite stores and bridge.run() admission; only change
// notifications are withheld. step() drains while run() is idle, so queued fs.watch
// delivery cannot accidentally rescue a missing same-link recipient or mask refresh cost.
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); }
  finally { vi.restoreAllMocks(); vi.useRealTimers(); }
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const until = async (check: () => boolean) => {
  const deadline = performance.now() + 5_000;
  while (!check() && performance.now() < deadline) await turn();
  expect(check(), "bounded bridge observation").toBe(true);
};
const native = async (store: MeshStore, name: string, ttl = 120_000) => {
  const now = Date.now(), id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: `${name}-00000000` };
  await store.put({ key: key("hosts", id), identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + ttl,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + ttl });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: `fixture:${id}`,
    name, label: name, status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: identity.sessionId, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return identity;
};
const events = (store: MeshStore): MeshEvent[] => {
  const result: MeshEvent[] = [];
  for (;;) {
    const page = store.read({ after: result.at(-1)?.sequence ?? 0, limit: 1000 });
    if (!page.length) return result;
    result.push(...page);
  }
};
const fixture = async (beforeLaunch?: (hub: MeshStore, far: MeshStore) => Promise<void>) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-refresh-scope-"));
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 1000, { stateBackend: "sqlite" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 1000, { stateBackend: "sqlite" });
  const other = new MeshStore(path.join(root, "other"), 64 * 1024, 1000, { stateBackend: "sqlite" });
  const lead = await native(hub, "lead");
  const foreign = await native(other, "foreign");
  await new StoreBridgeSide(hub, "other").mirror(await new StoreBridgeSide(other, "hub").presence());
  await beforeLaunch?.(hub, far);
  const local = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
  type Change = Parameters<NonNullable<BridgeSide["subscribeChanges"]>>[0];
  const notices: Partial<Record<"local" | "remote", Change>> = {};
  vi.spyOn(local, "subscribeChanges").mockImplementation(changed => {
    notices.local = changed;
    return () => { delete notices.local; };
  });
  vi.spyOn(remote, "subscribeChanges").mockImplementation(changed => {
    notices.remote = changed;
    return () => { delete notices.remote; };
  });
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath });
  const localReads = vi.spyOn(local, "tail"), remoteReads = vi.spyOn(remote, "tail");
  let failure: unknown;
  const running = bridge.run().catch(error => { failure = error; });
  cleanups.push(async () => {
    await bridge.stop(); await running;
    hub.closeState(); far.closeState(); other.closeState();
    fs.rmSync(root, { recursive: true, force: true });
  });
  await until(() => (localReads.mock.calls.length > 0 && remoteReads.mock.calls.length > 0) || failure !== undefined);
  expect(failure).toBeUndefined();
  for (let n = 0; n < 10; n++) await turn();
  const sync = vi.spyOn(bridge, "syncPresence");
  const send = (to: string, data: Record<string, unknown> = {}, topic = "fabric.control.command") =>
    hub.publish({ topic, kind: topic === "fabric.control.ack" ? "ack" : "followUp", from: lead, to,
      data: { version: 1, commandId: `scope-${events(hub).length}`, targetId: to, replyTo: lead.id, operation: "followUp", ...data } });
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toRemote: { after: number } };
  return { hub, far, other, lead, foreign, local, remote, bridge, notices, sync, send, cursor, failure: () => failure };
};
const stampId = (event: MeshEvent) => (event.data as { bridge?: { id: string } }).bridge?.id;

describe("outbound missing-recipient refresh scope (#15305)", () => {
  it.each(["native", "other-link"] as const)("does not refresh or forward a known %s recipient", async recipient => {
    const f = await fixture();
    const to = recipient === "native" ? f.lead.id : f.foreign.id;
    expect((await f.local.presence()).reserved).toContain(to);
    const sent = await f.send(to);
    expect(await f.bridge.step()).toMatchObject({ toRemote: 0, dropped: 0 });
    expect(f.sync).not.toHaveBeenCalled();
    expect(events(f.far)).toEqual([]);
    expect(f.cursor().toRemote.after).toBe(sent.sequence);
    expect(f.failure()).toBeUndefined();
  });

  it.each([null, "other", "hub", "valid-but-unseen-link"])("does not refresh an unknown recipient explicitly destined to %s", async destinationRemoteHost => {
    const f = await fixture();
    const sent = await f.send("session:unknown-00000000", { destinationRemoteHost });
    await f.bridge.step();
    expect(f.sync).not.toHaveBeenCalled();
    expect(events(f.far)).toEqual([]);
    expect(f.cursor().toRemote.after).toBe(sent.sequence);
  });

  it.each([{}, { destinationRemoteHost: "far" }])("refreshes and forwards a new same-link recipient (%j)", async data => {
    const f = await fixture();
    const newcomer = await native(f.far, "newcomer"); // no presence notification
    expect(f.local.holds(newcomer.id)).toBe(false);
    const sent = await f.send(newcomer.id, data);
    expect(await f.bridge.step()).toMatchObject({ toRemote: 1, dropped: 0 });
    expect(f.sync).toHaveBeenCalledTimes(1);
    expect(f.local.holds(newcomer.id)).toBe(true);
    expect(events(f.far).map(stampId)).toEqual([sent.id]);
    expect(events(f.far)[0]!.data).toMatchObject(data);
  });

  it("refreshes an unknown recipient with no destination, without granting authority", async () => {
    const f = await fixture();
    const sent = await f.send("session:unknown-00000000");
    await f.bridge.step();
    expect(f.sync).toHaveBeenCalledTimes(1);
    expect(events(f.far)).toEqual([]);
    expect(f.cursor().toRemote.after).toBe(sent.sequence);
  });

  it.each([42, {}, [], "", "bad/link", "with space", "a".repeat(64)])(
    "keeps malformed destination %j on the existing refresh/refusal path", async destinationRemoteHost => {
      const f = await fixture();
      const newcomer = await native(f.far, "malformed");
      const sent = await f.send(newcomer.id, { destinationRemoteHost });
      await f.bridge.step();
      expect(f.sync).toHaveBeenCalledTimes(1);
      expect(events(f.far)).toEqual([]);
      expect(f.cursor().toRemote.after).toBe(sent.sequence);
    });

  it.each(["fleet.work.scope", "fabric.control.ack"])("does not interpret destination fields on %s", async topic => {
    const f = await fixture();
    const newcomer = await native(f.far, "business-field");
    const data = { destinationRemoteHost: null, accepted: true };
    const sent = await f.send(newcomer.id, data, topic);
    expect(await f.bridge.step()).toMatchObject({ toRemote: 1 });
    expect(f.sync).toHaveBeenCalledTimes(1);
    expect(events(f.far).map(stampId)).toEqual([sent.id]);
    expect(events(f.far)[0]!.data).toMatchObject(data);
  });

  it("uses handoff-aware reservations: a new recipient handed to this link still refreshes and forwards", async () => {
    let mover!: MeshIdentity;
    const f = await fixture(async (hub, far) => {
      mover = await native(hub, "mover", -1);
      await handOffNativeIdentity(hub, { session: mover.id, link: "far", operator: "test" });
      // Its root appears remotely only AFTER run() has become idle.
      expect(events(far)).toEqual([]);
    });
    expect((await f.local.presence()).reserved).not.toContain(mover.id);
    await native(f.far, "mover");
    const sent = await f.send(mover.id, { destinationRemoteHost: "far" });
    expect(await f.bridge.step()).toMatchObject({ toRemote: 1 });
    expect(f.sync).toHaveBeenCalledTimes(1);
    expect(events(f.far).map(stampId)).toEqual([sent.id]);
  });

  it("keeps another link's handoff reserved even before it has a mirror", async () => {
    let mover!: MeshIdentity;
    const f = await fixture(async hub => {
      mover = await native(hub, "mover", -1);
      await handOffNativeIdentity(hub, { session: mover.id, link: "other", operator: "test" });
    });
    await native(f.far, "mover"); // untrusted same-id claim cannot undo the handoff
    expect((await f.local.presence()).reserved).toContain(mover.id);
    await f.send(mover.id);
    await f.bridge.step();
    expect(f.sync).not.toHaveBeenCalled();
    expect(events(f.far)).toEqual([]);
    expect(f.local.holds(mover.id)).toBe(false);
  });

  it("refuses a native takeover during the missing-recipient refresh", async () => {
    const f = await fixture();
    const newcomer = await native(f.far, "taken-over");
    f.sync.mockImplementation(async () => {
      await MeshBridge.prototype.syncPresence.call(f.bridge);
      await native(f.hub, "taken-over");
    });
    const sent = await f.send(newcomer.id);
    await f.bridge.step();
    expect(f.sync).toHaveBeenCalledTimes(1);
    expect(events(f.far)).toEqual([]);
    expect(f.local.holds(newcomer.id)).toBe(false);
    expect(f.cursor().toRemote.after).toBe(sent.sequence);
  });

  it("measures 1000 not-addressed events at the SQLite driver", async () => {
    const f = await fixture();
    const inputs = Array.from({ length: 1000 }, (_, n) => ({ topic: "fabric.control.command", kind: "followUp",
      from: f.lead, to: n % 2 ? f.lead.id : f.foreign.id,
      data: { version: 1, commandId: `cost-${n}`, destinationRemoteHost: n % 2 ? null : "other" } }));
    let at = 0;
    while (at < inputs.length) at += (await f.hub.publishBatch(inputs.slice(at, at + 32))).length;
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    const exec = sqlite.DatabaseSync.prototype.exec;
    let begins = 0;
    const driver = vi.spyOn(sqlite.DatabaseSync.prototype, "exec").mockImplementation(function (this: InstanceType<typeof sqlite.DatabaseSync>, sql: string) {
      const result = exec.call(this, sql);
      if (sql.trim().toUpperCase() === "BEGIN IMMEDIATE") begins++;
      return result;
    });
    console.log("refresh-scope: starting 1000-event drain");
    const result = await f.bridge.step();
    driver.mockRestore();
    console.log(`refresh-scope: events=1000 syncPresence=${f.sync.mock.calls.length} BEGIN_IMMEDIATE=${begins} forwarded=${result.toRemote}`);
    expect(events(f.far)).toEqual([]);
    expect(f.cursor().toRemote.after).toBe(events(f.hub).at(-1)!.sequence);
    expect(f.sync).not.toHaveBeenCalled();
    expect(begins).toBe(0);
    const before = f.sync.mock.calls.length;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.sync.mock.calls.length).toBe(before); // no deferred refresh/poll introduced
    expect(f.failure()).toBeUndefined();
  }, 300_000);
});
