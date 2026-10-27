import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough, Transform } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeOwnershipError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: recorded cross-link ancestry incident, trust-boundary ownership and data safety.
// Use real SQLite state, stdio RPC, mirror planning and the commit-time held ownership gate.
const NOW = Date.UTC(2100, 0, 1);
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.useRealTimers();
});
const key = (prefix: string, id: string) => `topology/${prefix}/${createHash("sha256").update(id).digest("hex")}`;
const hostKey = (id: string) => key("hosts", id);
const participantKey = (id: string) => key("participants", id);
const root = (name: string) => {
  const id = `session:${name}-00000001`;
  const identity: MeshIdentity = { id, name, kind: "main", sessionId: `${name}-00000001` };
  const host: FabricHostRecord = { format: 1, id, rootId: id, identity, incarnationToken: `token-${name}`,
    startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 120_000 };
  const participant: FabricParticipantRecord = { format: 1, id, rootId: id, kind: "root", ownerHostId: id,
    ownerIdentityId: id, name, status: "idle", runner: "pi", transport: "host", capabilities: ["followUp"],
    sessionId: identity.sessionId!, startedAt: host.startedAt, updatedAt: NOW, controlProtocol: "v1" };
  return { identity, host, participant };
};
const installRoot = async (store: MeshStore, value: ReturnType<typeof root>) => {
  await store.put({ key: hostKey(value.host.id), identity: value.identity, value: value.host });
  await store.put({ key: participantKey(value.participant.id), identity: value.identity, value: value.participant });
  writeHostLease(store.root, { id: value.host.id, rootId: value.host.rootId, identityId: value.identity.id,
    startedAt: value.host.startedAt, updatedAt: NOW, expiresAt: value.host.expiresAt, incarnationToken: value.host.incarnationToken! });
};

type Shape = "host" | "participant" | "both";
const setup = async (receiver: "agent" | "hub", olderCapabilities: boolean) => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-child-root-"));
  cleanups.push(() => fs.rmSync(dir, { recursive: true, force: true }));
  const hub = new MeshStore(path.join(dir, "hub"), 64 * 1024, 100, { stateBackend: "sqlite" });
  const spoke = new MeshStore(path.join(dir, "spoke"), 64 * 1024, 100, { stateBackend: "sqlite" });
  cleanups.push(() => { hub.closeState(); spoke.closeState(); });
  // Reverse the transport placement too: the same ancestry must work in both receiving paths.
  const localName = receiver === "agent" ? "hub" : "spoke";
  const remoteName = receiver === "agent" ? "spoke" : "hub";
  const local = new StoreBridgeSide(receiver === "agent" ? hub : spoke, remoteName, () => Date.now(), false);
  const agent = new StoreBridgeSide(receiver === "agent" ? spoke : hub, localName, () => Date.now(), false);
  const input = new PassThrough(), output = new PassThrough();
  // Capability-wire emulation only, NOT a claim to execute an old release's implementation.
  let buffered = "";
  const replies = new Transform({ transform(chunk, _encoding, done) {
    buffered += String(chunk);
    for (;;) {
      const end = buffered.indexOf("\n");
      if (end < 0) break;
      const reply = JSON.parse(buffered.slice(0, end));
      buffered = buffered.slice(end + 1);
      if (olderCapabilities && reply.result?.partialPresence === true) {
        delete reply.result.partialPresence;
        delete reply.result.nonRootHosts;
      }
      this.push(`${JSON.stringify(reply)}\n`);
    }
    done();
  } });
  replies.pipe(output);
  const serving = serveBridgeAgent(agent, input, replies);
  const remote = new RemoteBridgeSide(output, input);
  const bridge = new MeshBridge({ localName, remoteName, local, remote, presenceMs: 0,
    cursorPath: path.join(dir, "bridge.cursor.json") });
  cleanups.push(async () => { await bridge.stop(); remote.close(); input.end(); await serving; replies.destroy(); output.end(); });
  const parent = root("parent-main"), sender = root("spoke-main");
  await installRoot(hub, parent);
  await installRoot(spoke, sender);
  return { hub, spoke, local, agent, remote, bridge, parent, sender,
    receiver: receiver === "agent" ? agent : local };
};

const installChild = async (f: Awaited<ReturnType<typeof setup>>, shape: Shape, resident = false) => {
  const id = resident ? `resident:${"b".repeat(24)}` : "runtime:remote-child";
  const identity: MeshIdentity = { id: "agent:remote-child-identity", name: "remote-child", kind: "agent" };
  const host: FabricHostRecord = { format: 1, id, rootId: f.parent.host.id, identity,
    startedAt: NOW - 500, updatedAt: NOW, expiresAt: NOW + 1_000 };
  const participant: FabricParticipantRecord = { format: 1, id: identity.id, kind: "agent", rootId: f.parent.host.id,
    ownerHostId: id, ownerIdentityId: identity.id, name: "remote-child", status: "running", runner: "pi", transport: "process",
    capabilities: [], sessionId: "remote-child-session", startedAt: host.startedAt, updatedAt: NOW, controlProtocol: "v1" };
  if (shape !== "participant") {
    await f.spoke.put({ key: hostKey(id), identity, value: host });
    writeHostLease(f.spoke.root, { id, rootId: host.rootId, identityId: identity.id,
      startedAt: host.startedAt, updatedAt: NOW, expiresAt: host.expiresAt });
  }
  if (shape !== "host") {
    const entry = await f.spoke.put({ key: participantKey(participant.id), identity, value: participant });
    // Both storage copies participate in reservation and must survive all passes unchanged.
    writeParticipantFile(f.spoke.root, entry);
  }
  const snapshot = () => ({ host: f.spoke.get(hostKey(id), { fresh: true }),
    participant: f.spoke.get(participantKey(participant.id), { fresh: true }),
    lease: shape === "participant" ? undefined : fs.readFileSync(hostLeasePath(f.spoke.root, id)),
    file: shape === "host" ? undefined : fs.readFileSync(path.join(f.spoke.root, "participants",
      `${participantKey(participant.id).split("/").at(-1)}.json`), "utf8") });
  return { id, identity, snapshot, before: snapshot() };
};
const expectParent = (f: Awaited<ReturnType<typeof setup>>) => {
  for (const rowKey of [hostKey(f.parent.host.id), participantKey(f.parent.host.id)]) {
    expect(f.spoke.get(rowKey, { fresh: true })?.value).toMatchObject({ id: f.parent.host.id, remoteHost: "hub" });
  }
  expect(f.receiver.holds(f.parent.host.id, f.parent.identity)).toBe(true);
};
const control = (f: Awaited<ReturnType<typeof setup>>, commandId: string) => ({
  topic: "fabric.control.command", kind: "followUp", from: f.parent.identity, to: f.sender.host.id,
  data: { version: 1, commandId, targetId: f.sender.host.id, replyTo: f.parent.host.id, action: "followUp", text: "hello" },
});
const crossesOnce = async (f: Awaited<ReturnType<typeof setup>>, commandId: string) => {
  const sent = await f.hub.publish(control(f, commandId));
  await f.bridge.step();
  await f.bridge.step();
  const delivered = () => f.spoke.read({ after: 0, limit: 100 }).filter(e => e.topic === "fabric.control.command" && (e.data as { commandId?: unknown } | undefined)?.commandId === commandId);
  expect(delivered()).toMatchObject([{ to: f.sender.host.id, data: { bridge: { id: sent.id } } }]);
  // Agent RPC deliberately accepts no held ids. Exercise its real commit-time gate as well,
  // using the same durable command identity: retrying after admission must not append twice.
  const event = { ...control(f, commandId), data: { ...control(f, commandId).data,
    bridge: { from: "hub", id: sent.id } } } as BridgePublish;
  await expect(f.receiver.publish(event, [f.parent.host.id])).resolves.toMatchObject({ sequence: delivered()[0]!.sequence });
  expect(delivered()).toHaveLength(1);

  const replyId = `${commandId}-to-parent`;
  const reply = await f.spoke.publish({ ...control(f, replyId), from: f.sender.identity, to: f.parent.host.id,
    data: { ...control(f, replyId).data, targetId: f.parent.host.id, replyTo: f.sender.host.id } });
  await f.bridge.step();
  await f.bridge.step();
  expect(f.hub.read({ after: 0, limit: 100 }).filter(e => e.topic === "fabric.control.command" &&
    (e.data as { commandId?: unknown } | undefined)?.commandId === replyId))
    .toMatchObject([{ to: f.parent.host.id, data: { bridge: { id: reply.id } } }]);
};

const placements = (["agent", "hub"] as const).flatMap(receiver => [false, true].map(olderCapabilities => ({ receiver, olderCapabilities })));
describe.each(placements)("cross-link child ancestry ($receiver receiver, olderCapabilities=$olderCapabilities)", ({ receiver, olderCapabilities }) => {
  it.each(["host", "participant", "both"] as const)("retains and restores Main mirrors with a live then expired-but-present native %s", async shape => {
    const f = await setup(receiver, olderCapabilities);
    await f.bridge.start();
    expectParent(f);
    const child = await installChild(f, shape);
    const origin = f.hub.get(participantKey(f.parent.host.id), { fresh: true });
    const presence = f.local.presence.bind(f.local);
    let partial = false;
    vi.spyOn(f.local, "presence").mockImplementation(async () => ({ ...(await presence()), ...(partial ? { partial: true } : {}) }));
    for (const expired of [false, true]) {
      vi.setSystemTime(NOW + (expired ? 2_000 : 0));
      for (const incomplete of [false, true]) {
        partial = incomplete;
        await f.bridge.syncPresence();
        expectParent(f);
        const reserved = (await f.receiver.presence()).reserved;
        expect(reserved).toEqual(expect.arrayContaining([child.id, child.identity.id]));
        expect(reserved).not.toContain(f.parent.host.id);
        await crossesOnce(f, `command-${shape}-${expired}-${incomplete}`);
        expect(child.snapshot()).toEqual(child.before);
        // A destination tombstone, including participant-only loss, is destination CAS state,
        // not an origin-version watermark. No source heartbeat/change is needed to recover.
        for (const rowKey of incomplete ? [participantKey(f.parent.host.id)] : [hostKey(f.parent.host.id), participantKey(f.parent.host.id)]) {
          await f.spoke.delete({ key: rowKey });
          expect(f.spoke.get(rowKey, { fresh: true })).toBeUndefined();
        }
        await f.bridge.syncPresence();
        expectParent(f);
        expect(f.hub.get(participantKey(f.parent.host.id), { fresh: true })).toEqual(origin);
        expect(child.snapshot()).toEqual(child.before);
      }
    }
  }, 60_000); // Four real-store/RPC cycles plus fenced retries; not a latency benchmark.

  it.each([false, true])("inserts absent Main mirrors beside native runtime/resident ancestry (resident=%s)", async resident => {
    const f = await setup(receiver, olderCapabilities);
    const child = await installChild(f, "both", resident);
    const origin = f.hub.get(participantKey(f.parent.host.id), { fresh: true });
    expect(f.spoke.get(hostKey(f.parent.host.id), { fresh: true })).toBeUndefined();
    expect(f.spoke.get(participantKey(f.parent.host.id), { fresh: true })).toBeUndefined();
    await f.bridge.start();
    expectParent(f);
    await crossesOnce(f, "initial-command");
    expect(child.snapshot()).toEqual(child.before);
    expect(f.hub.get(participantKey(f.parent.host.id), { fresh: true })).toEqual(origin);
  });
});

it.each(["canonical Main", "malformed host", "malformed participant", "writer mismatch", "other-link Main", "other-link child", "noncanonical child", "participant writer mismatch"] as const)(
  "still refuses parent authority reserved by a native/foreign %s", async claim => {
    const f = await setup("agent", false);
    await f.bridge.start();
    expectParent(f);
    if (claim === "canonical Main") await installRoot(f.spoke, f.parent);
    else if (claim === "other-link Main") {
      await f.spoke.put({ key: hostKey(f.parent.host.id), identity: f.parent.identity,
        value: { ...f.parent.host, remoteHost: "other-link" } });
    } else {
      const identity: MeshIdentity = { id: "agent:invalid-child", name: "invalid-child", kind: "agent" };
      if (claim === "malformed participant" || claim === "participant writer mismatch") {
        await f.spoke.put({ key: participantKey(identity.id), identity: claim === "participant writer mismatch" ? f.sender.identity : identity,
          value: { ...f.parent.participant, id: identity.id, kind: claim === "malformed participant" ? "root" : "agent",
            ownerHostId: "runtime:invalid-child", ownerIdentityId: identity.id } });
      } else {
        const id = claim === "noncanonical child" ? "task:invalid-child" : "runtime:invalid-child";
        await f.spoke.put({ key: hostKey(id), identity: claim === "writer mismatch" ? f.sender.identity : identity,
          value: { format: 1, id, rootId: f.parent.host.id, identity,
            startedAt: claim === "malformed host" ? null : NOW, updatedAt: NOW, expiresAt: NOW + 1_000,
            ...(claim === "other-link child" ? { remoteHost: "other-link" } : {}) } });
      }
    }
    const before = f.spoke.listAll("topology/", { fresh: true });
    expect((await f.receiver.presence()).reserved).toContain(f.parent.host.id);
    expect(f.receiver.holds(f.parent.host.id, f.parent.identity)).toBe(false);
    const event = { ...control(f, "refused-command"), data: { ...control(f, "refused-command").data,
      bridge: { from: "hub", id: "refused-event" } } } as BridgePublish;
    await expect(f.receiver.publish(event, [f.parent.host.id])).rejects.toBeInstanceOf(BridgeOwnershipError);
    await f.remote.mirror({ hosts: [{ record: f.parent.host, expiresAt: f.parent.host.expiresAt }], participants: [f.parent.participant], partial: true });
    expect(f.spoke.listAll("topology/", { fresh: true })).toEqual(before);
    expect(f.spoke.read({ after: 0 }).filter(e => e.topic === "fabric.control.command")).toHaveLength(0);
  });
