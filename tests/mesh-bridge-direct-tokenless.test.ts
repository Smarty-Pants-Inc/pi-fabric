import { createHash } from "node:crypto";
import { PassThrough } from "node:stream";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, BridgeStaleIncarnationError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { ownProcessIncarnation, readPhysicalHostIdentity } from "../src/core/atomic-write.js";

const roots: string[] = [];
const stores: MeshStore[] = [];
const closers: Array<() => Promise<void>> = [];
const NOW = Date.UTC(2100, 0, 1);

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const store of stores.splice(0).reverse()) store.closeState();
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const key = (prefix: string, id: string) => `${prefix}${createHash("sha256").update(id).digest("hex")}`;
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);

const fixture = (name: string, token: string | undefined, startedAt: number, expiresAt = startedAt + 120_000) => {
  const identity: MeshIdentity = { id: `session:${name}-00000001`, name, kind: "main", sessionId: name };
  const host: FabricHostRecord = { format: 1, id: identity.id, rootId: identity.id, identity,
    startedAt, updatedAt: startedAt, expiresAt, ...(token === undefined ? {} : { incarnationToken: token }) };
  const participant: FabricParticipantRecord = { format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, ownerIncarnation: `fixture:${token ?? "legacy"}`,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt, updatedAt: startedAt, controlProtocol: "v1" };
  return { identity, host, participant,
    presence: { hosts: [{ record: host, expiresAt }], participants: [participant] } };
};

const leaseFor = ({ host }: ReturnType<typeof fixture>) => ({ id: host.id, rootId: host.rootId,
  identityId: host.identity.id, startedAt: host.startedAt, updatedAt: host.updatedAt, expiresAt: host.expiresAt,
  ...(host.incarnationToken === undefined ? {} : { incarnationToken: host.incarnationToken }) });

const openStore = (stateBackend: "file" | "sqlite" = "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-direct-tokenless-"));
  roots.push(root);
  const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend });
  stores.push(store);
  return store;
};

const installNative = async (store: MeshStore, value: ReturnType<typeof fixture>) => {
  await store.put({ key: hostKey(value.host.id), identity: value.identity, value: value.host });
  await store.put({ key: participantKey(value.participant.id), identity: value.identity, value: value.participant });
  writeHostLease(store.root, leaseFor(value));
};

// Keep reason: own-root reservation and incarnation admission cross the real trusted-hub RPC
// boundary, including the work-host receiver's fromHub path.
const ownRootLink = (monitorLease = true, backend: "file" | "sqlite" = "sqlite") => {
  const hub = openStore(backend);
  const work = openStore(backend);
  const input = new PassThrough();
  const output = new PassThrough();
  const local = new StoreBridgeSide(hub, "intel1", () => Date.now());
  const agent = new StoreBridgeSide(work, "hub", () => Date.now(), monitorLease);
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "intel1", local, remote,
    cursorPath: path.join(hub.root, "intel1.cursor.json"), presenceMs: 0 });
  closers.push(async () => {
    await bridge.stop();
    remote.close();
    input.end();
    await serving;
    output.end();
  });
  return { hub, work, local, agent, remote, bridge };
};

const installParticipantFile = (store: MeshStore, value: ReturnType<typeof fixture>) => {
  writeParticipantFile(store.root, { key: participantKey(value.participant.id), version: 1,
    updatedAt: value.participant.updatedAt, updatedBy: value.identity, value: value.participant });
};

const ownRootEcho = async () => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW);
  const link = ownRootLink();
  const echo = fixture("own-root", undefined, NOW - 1_000, NOW + 120_000);
  const native = fixture("own-root", "preexisting-native-token", NOW - 2_000, NOW + 120_000);
  // Model the incident's native-looking hub row explicitly. It is NOT synthesized from a
  // hub lease/participant, and is NOT a structurally marked cross-spoke relay.
  await installNative(link.hub, echo);
  const computed = await link.local.presence();
  expect(computed.hosts).toHaveLength(1);
  expect(computed.hosts[0]!.record).toMatchObject({ id: echo.host.id });
  expect(computed.hosts[0]!.record.incarnationToken).toBeUndefined();
  expect(computed.hosts[0]!.record.remoteHost).toBeUndefined();
  expect(computed.hosts[0]!.record.bridgeVia).toBeUndefined();
  return { ...link, echo, native };
};

describe("own-root diagnostic", () => {
  it("exports only canonical roots across repeated sibling-token passes without poisoning genuine root authority", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const f = ownRootLink();
    const rootId = "session:shared-root-00000001";
    const h1 = fixture("hub-sibling-1", "T1", NOW - 2_000, NOW + 120_000);
    const h2 = fixture("hub-sibling-2", "T2", NOW - 1_000, NOW + 120_000);
    for (const h of [h1, h2]) {
      h.host.rootId = rootId;
      h.participant.rootId = rootId;
      await installNative(f.hub, h);
    }
    const canonical = fixture("shared-root", "ROOT", NOW - 3_000, NOW + 120_000);
    const unrelated = fixture("unrelated-canonical", "U", NOW - 1_000, NOW + 120_000);
    await installNative(f.hub, canonical);
    await installNative(f.hub, unrelated);
    const mirrored = vi.spyOn(f.agent, "mirror");
    for (let pass = 0; pass < 4; pass++) {
      vi.setSystemTime(NOW + pass * 6_000);
      const computed = await f.local.presence();
      expect(computed.hosts.map(h => h.record.id).sort()).toEqual([rootId, unrelated.host.id].sort());
      // Filtering host export must not waive native sibling/task identities or roots.
      for (const id of [h1.host.id, h2.host.id, rootId]) expect(computed.reserved).toContain(id);
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      const call = mirrored.mock.calls[pass]!;
      expect(call[2]).toBe(true);
      expect(call[0].hosts.map(h => h.record.id).sort()).toEqual([rootId, unrelated.host.id].sort());
      for (const h of [h1, h2]) {
        expect(f.work.get(hostKey(h.host.id), { fresh: true })).toBeUndefined();
        expect(readHostLease(f.work.root, h.host.id)).toBeUndefined();
      }
      for (const h of [canonical, unrelated]) {
        const held = f.work.get(hostKey(h.host.id), { fresh: true })!.value as FabricHostRecord;
        expect(held.incarnationToken).toBe(h.host.incarnationToken);
        expect(held.bridgeSupersededTokens).toBeUndefined();
        expect(readHostLease(f.work.root, h.host.id)).toMatchObject({ incarnationToken: h.host.incarnationToken });
        expect(readHostLease(f.work.root, h.host.id)!.expiresAt).toBeGreaterThan(Date.now());
      }
    }
    expect(mirrored).toHaveBeenCalledTimes(4);
  });

  it.each([false, true])("omits conflicting duplicate canonical hosts at the outbound boundary rather than choosing by array order (reverse=%s)", async reverse => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const f = ownRootLink(false);
    const canonical = fixture("duplicate-root", "ROOT", NOW - 1_000);
    const unrelated = fixture("duplicate-control", "U", NOW - 1_000);
    await installNative(f.hub, canonical);
    await installNative(f.hub, unrelated);
    const presence = f.local.presence.bind(f.local);
    vi.spyOn(f.local, "presence").mockImplementation(async () => {
      const computed = await presence();
      const conflict = { record: { ...canonical.host, incarnationToken: "CONFLICT" }, expiresAt: canonical.host.expiresAt };
      return { ...computed, hosts: reverse ? [conflict, ...computed.hosts] : [...computed.hosts, conflict] };
    });
    const mirrored = vi.spyOn(f.agent, "mirror");
    for (let pass = 0; pass < 3; pass++) {
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      expect(mirrored.mock.calls[pass]![0].hosts.map(h => h.record.id)).toEqual([unrelated.host.id]);
      expect(f.work.get(hostKey(canonical.host.id), { fresh: true })).toBeUndefined();
      expect(readHostLease(f.work.root, canonical.host.id)).toBeUndefined();
      expect(readHostLease(f.work.root, unrelated.host.id)).toMatchObject({ incarnationToken: "U" });
    }
  });

  it.each(["native host row", "native participant-file-only"] as const)(
    "%s reserves the own root before tokenless fromHub admission even with a token lease", async (shape) => {
      const f = await ownRootEcho();
      writeHostLease(f.work.root, leaseFor(f.native));
      if (shape === "native host row") {
        // A tokenless old host row is enough to reserve; the token-bearing lease is separate.
        await f.work.put({ key: hostKey(f.echo.host.id), identity: f.echo.identity, value: f.echo.host });
      } else installParticipantFile(f.work, f.native);
      const before = {
        host: f.work.get(hostKey(f.echo.host.id), { fresh: true }),
        participant: f.work.get(participantKey(f.echo.participant.id), { fresh: true }),
        lease: fs.readFileSync(hostLeasePath(f.work.root, f.echo.host.id)),
      };
      const computed = await f.agent.presence();
      expect(computed.reserved).toContain(f.echo.host.rootId);
      if (shape === "native participant-file-only") {
        expect(before.host).toBeUndefined();
        expect(before.participant).toBeUndefined();
        expect(computed.hosts).toEqual([]);
        expect(computed.participants).toEqual([]); // no participant-derived host synthesizer
      }
      const mirrored = vi.spyOn(f.agent, "mirror");
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      expect(mirrored).toHaveBeenCalledTimes(1);
      expect(mirrored.mock.calls[0]![2]).toBe(true); // serveBridgeAgent supplied fromHub
      expect(mirrored.mock.calls[0]![0].hosts[0]!.record.id).toBe(f.echo.host.id);
      expect(f.work.get(hostKey(f.echo.host.id), { fresh: true })).toEqual(before.host);
      expect(f.work.get(participantKey(f.echo.participant.id), { fresh: true })).toEqual(before.participant);
      expect(fs.readFileSync(hostLeasePath(f.work.root, f.echo.host.id))).toEqual(before.lease);
    });

  it("native token lease ONLY does not reserve and skips tokenless fromHub authority without stopping the link", async () => {
    const f = await ownRootEcho();
    writeHostLease(f.work.root, leaseFor(f.native));
    const before = fs.readFileSync(hostLeasePath(f.work.root, f.native.host.id));
    expect(await f.agent.presence()).toEqual({ hosts: [], participants: [], reserved: [] });
    const mirrored = vi.spyOn(f.agent, "mirror");
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(mirrored).toHaveBeenCalledTimes(1);
    expect(mirrored.mock.calls[0]![2]).toBe(true);
    await expect(mirrored.mock.results[0]!.value).resolves.toBeUndefined();
    expect(f.work.get(hostKey(f.native.host.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(participantKey(f.native.participant.id), { fresh: true })).toBeUndefined();
    expect(fs.readFileSync(hostLeasePath(f.work.root, f.native.host.id))).toEqual(before);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(mirrored).toHaveBeenCalledTimes(2); // no permanent link fence
  });

  it("injected hub lock-timeout before any agent mirror leaves the next first pass able to skip lease-only authority", async () => {
    const f = await ownRootEcho();
    writeHostLease(f.work.root, leaseFor(f.native));
    const before = fs.readFileSync(hostLeasePath(f.work.root, f.native.host.id));
    const mirrored = vi.spyOn(f.agent, "mirror");
    // Honest injection, following existing bridge tests: no real contending lock or timeout
    // duration is claimed. This models failure during the hub presence read, before mirrors.
    vi.spyOn(f.local, "presence").mockRejectedValueOnce(Object.assign(new Error("injected hub lock timeout"),
      { code: "FABRIC_MESH_LOCK_TIMEOUT" }));
    await expect(f.bridge.syncPresence()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(mirrored).not.toHaveBeenCalled();
    expect(fs.readFileSync(hostLeasePath(f.work.root, f.native.host.id))).toEqual(before);
    vi.setSystemTime(NOW + 1_000); // modeled incident timing, not a wall-clock measurement
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(mirrored).toHaveBeenCalledTimes(1);
    expect(mirrored.mock.calls[0]![2]).toBe(true);
    expect(f.work.get(hostKey(f.native.host.id), { fresh: true })).toBeUndefined();
    expect(fs.readFileSync(hostLeasePath(f.work.root, f.native.host.id))).toEqual(before);
  });

  it("monitorLease ownProcessIncarnation is nested writer evidence, not newly minted root token authority", async () => {
    const f = await ownRootEcho();
    writeHostLease(f.work.root, leaseFor(f.echo)); // lease only, but no root token authority
    expect(await f.agent.presence()).toEqual({ hosts: [], participants: [], reserved: [] });
    const processIncarnation = await ownProcessIncarnation();
    const physicalHost = readPhysicalHostIdentity();
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const held = f.work.get(hostKey(f.echo.host.id), { fresh: true })!.value as FabricHostRecord;
    const lease = readHostLease(f.work.root, f.echo.host.id)!;
    expect(held).toMatchObject({ remoteHost: "hub", mirrorLease: 1 });
    expect(held.incarnationToken).toBeUndefined();
    expect(lease.incarnationToken).toBeUndefined();
    expect(lease.bridgeSupersededTokens).toBeUndefined();
    if (physicalHost && processIncarnation) {
      expect(lease.mirror?.bridgeProcess).toEqual({ ...physicalHost, link: "hub", pid: process.pid,
        incarnation: processIncarnation });
    } else expect(lease.mirror?.bridgeProcess).toBeUndefined();
    vi.setSystemTime(NOW + 1_000);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined(); // monitoring cannot fence next pass
    expect(readHostLease(f.work.root, f.echo.host.id)!.incarnationToken).toBeUndefined();
  });

  it.each(["lease ONLY", "lease plus participant file", "lease plus participant row"] as const)(
    "hub %s with no host row cannot compute or RPC-export a presence host", async (shape) => {
      vi.useFakeTimers();
      vi.setSystemTime(NOW);
      const f = ownRootLink();
      const legacy = fixture("hub-lease-only", undefined, NOW - 1_000, NOW + 120_000);
      writeHostLease(f.hub.root, leaseFor(legacy));
      if (shape === "lease plus participant file") installParticipantFile(f.hub, legacy);
      if (shape === "lease plus participant row") await f.hub.put({ key: participantKey(legacy.participant.id),
        identity: legacy.identity, value: legacy.participant });
      expect(f.hub.get(hostKey(legacy.host.id), { fresh: true })).toBeUndefined();
      const computed = await f.local.presence(); // real source scan, never a fixture RPC stub
      expect(computed.hosts).toEqual([]);
      expect(computed.participants).toEqual([]);
      if (shape !== "lease ONLY") expect(computed.reserved).toContain(legacy.host.id);
      else expect(computed.reserved).toEqual([]);
      const mirrored = vi.spyOn(f.agent, "mirror");
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      expect(mirrored).toHaveBeenCalledTimes(1);
      expect(mirrored.mock.calls[0]![0]).toEqual({ hosts: [], participants: [] });
      expect(f.work.get(hostKey(legacy.host.id), { fresh: true })).toBeUndefined();
      expect(readHostLease(f.work.root, legacy.host.id)).toBeUndefined();
    });
});

describe("direct tokenless presence through MeshBridge RPC", () => {
  // CODE-F1's independent probe sequence, through JSON framing and presenceArg, not
  // merely StoreBridgeSide.mirror: malformed presence must not erase the A -> B fence.
  it.each(["file", "sqlite"] as const)("CODE-F1 preserves B and its A fence when tokenless updatedAt NaN becomes null over RPC (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const a = fixture("rpc-tokenless-root", "A", NOW - 2_000, NOW + 120_000);
    const b = fixture("rpc-tokenless-root", "B", NOW - 1_000, NOW + 120_000);
    await f.agent.mirror(a.presence);
    await f.agent.mirror(b.presence);
    const before = { host: f.work.get(hostKey(b.host.id), { fresh: true }),
      participant: f.work.get(participantKey(b.host.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.work.root, b.host.id)) };
    expect(before.host?.value).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"] });
    const legacy = fixture("rpc-tokenless-root", undefined, NOW - 1_000, NOW + 120_000);
    expect(JSON.stringify(Number.NaN)).toBe("null");
    await f.remote.mirror({ hosts: [{ record: { ...legacy.host, updatedAt: Number.NaN },
      expiresAt: NOW + 120_000, observedAt: NOW }], participants: [{ ...legacy.participant, name: "untrusted-tokenless" }] });
    expect(f.work.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, b.host.id))).toEqual(before.lease);
    await expect(f.remote.mirror(a.presence)).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
    expect(f.work.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, b.host.id))).toEqual(before.lease);
  });

  it.each(["file", "sqlite"] as const)("WORKHOST fromHub renews existing B beyond expiry and keeps command/ACK usable (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const a = fixture("hub-direct-root", "A", NOW - 2_000, NOW + 5_000);
    const b = fixture("hub-direct-root", "B", NOW - 1_000, NOW + 5_000);
    b.participant.name = "held-B";
    await f.agent.mirror(a.presence);
    await f.agent.mirror(b.presence);
    const participantBefore = f.work.get(participantKey(b.host.id), { fresh: true });
    const legacy = fixture("hub-direct-root", undefined, NOW, NOW + 120_000);
    legacy.participant.name = "untrusted-tokenless";
    const target = fixture("work-native", "W", NOW, NOW + 120_000);
    await installNative(f.hub, legacy);
    await installNative(f.work, target);
    const mirrored = vi.spyOn(f.agent, "mirror");
    await f.bridge.start();
    for (const now of [NOW + 20_000, NOW + 40_000]) {
      vi.setSystemTime(now); // prior receiver sample has expired; the source remains live
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(participantBefore);
      expect(f.work.get(hostKey(b.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "B",
        startedAt: b.host.startedAt, identity: b.identity, bridgeSupersededTokens: ["A"] });
      expect(readHostLease(f.work.root, b.host.id)).toMatchObject({ incarnationToken: "B", expiresAt: now + BRIDGE_LEASE_MS });
      expect(f.agent.holds(b.host.id, b.identity)).toBe(true);
    }
    expect(mirrored.mock.calls.every(call => call[2] === true)).toBe(true);
    const sent = await f.hub.publish({ topic: "fabric.control.command", kind: "followUp", from: legacy.identity,
      to: target.host.id, data: { version: 1, commandId: "tokenless-command", targetId: target.host.id,
        replyTo: legacy.host.id, action: "followUp", text: "still usable" } });
    expect((await f.bridge.step()).toRemote).toBe(1);
    expect(f.work.read({ after: 0 }).filter(e => e.topic === "fabric.control.command")).toMatchObject([
      { data: { bridge: { id: sent.id } } },
    ]);
    await f.work.publish({ topic: "fabric.control.ack", kind: "ack", from: target.identity, to: legacy.host.id,
      data: { commandId: "tokenless-command", targetId: target.host.id, accepted: true } });
    expect((await f.bridge.step()).toLocal).toBe(1);
    expect(f.hub.read({ after: 0 }).filter(e => e.topic === "fabric.control.ack")).toHaveLength(1);
    expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(participantBefore);
  });

  it.each(["expiresAt", "observedAt"] as const)("keeps a coherent root and lease unchanged when the real wire carries an unusable %s sample", async field => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const f = ownRootLink(false);
    const b = fixture("bad-wire-sample", "B", NOW - 1_000, NOW + 5_000);
    await f.agent.mirror(b.presence);
    const before = { host: f.work.get(hostKey(b.host.id), { fresh: true }),
      participant: f.work.get(participantKey(b.host.id), { fresh: true }), lease: fs.readFileSync(hostLeasePath(f.work.root, b.host.id)) };
    const legacy = fixture("bad-wire-sample", undefined, NOW, NOW + 120_000);
    const sample = { record: legacy.host, observedAt: NOW, expiresAt: NOW + 120_000, [field]: NaN };
    // JSON serializes NaN as null: retain identifiable invalid source liveness rather than
    // dropping the host before root preservation can prevent omission deletion.
    await expect(f.remote.mirror({ hosts: [sample], participants: [legacy.participant] })).resolves.toBeUndefined();
    expect(f.work.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, b.host.id))).toEqual(before.lease);
  });

  it("renews live tokenless R without replacing B authority, then keeps unrelated and valid calls usable", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const hub = openStore();
    const spoke = openStore();
    const input = new PassThrough();
    const output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(spoke, "hub"), input, output);
    const remote = new RemoteBridgeSide(output, input);
    const bridge = new MeshBridge({ localName: "hub", remoteName: "spoke",
      local: new StoreBridgeSide(hub, "spoke"), remote,
      cursorPath: path.join(hub.root, "spoke.cursor.json"), presenceMs: 0 });
    closers.push(async () => {
      await bridge.stop();
      remote.close();
      input.end();
      await serving;
      output.end();
    });

    const rootA = fixture("direct-root", "A", NOW - 2_000, NOW + 10_000);
    const rootB = fixture("direct-root", "B", NOW, NOW + 10_000);
    rootB.participant.name = "participant-B";
    const legacy = fixture("direct-root", undefined, NOW + 1_000, NOW + 120_000);
    legacy.participant.name = "old-release-tokenless";
    const unrelated = fixture("unrelated", "U", NOW + 1_000);

    // The receiver already holds the token-B authority and persisted A->B history.
    const held = new StoreBridgeSide(hub, "spoke", () => Date.now());
    await held.mirror(rootA.presence);
    await held.mirror(rootB.presence);
    const before = {
      host: hub.get(hostKey(rootB.host.id), { fresh: true }),
      participant: hub.get(participantKey(rootB.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(hub.root, rootB.host.id)),
    };
    expect(before.host?.value).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"] });
    expect(before.participant?.value).toMatchObject({ name: "participant-B" });
    expect(readHostLease(hub.root, rootB.host.id)).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"] });

    // This is the old writer's real native state/lease emission: no RPC response is faked.
    await installNative(spoke, legacy);
    await installNative(spoke, unrelated);

    await expect(bridge.syncPresence()).resolves.toBeUndefined();
    expect(hub.get(hostKey(rootB.host.id), { fresh: true })?.value).toMatchObject({
      incarnationToken: "B", bridgeSupersededTokens: ["A"],
    });
    expect(hub.get(participantKey(rootB.participant.id), { fresh: true })).toEqual(before.participant);
    expect(readHostLease(hub.root, rootB.host.id)).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"] });
    expect(hub.get(hostKey(unrelated.host.id), { fresh: true })?.value).toMatchObject({
      id: unrelated.host.id, remoteHost: "spoke", incarnationToken: "U",
    });
    expect(hub.get(participantKey(unrelated.participant.id), { fresh: true })?.value).toMatchObject({
      id: unrelated.participant.id, remoteHost: "spoke",
    });

    // The tokenless old writer remains live beyond the receiver's original B expiry; a valid
    // direct presence refresh must extend liveness without replacing B or participant-B.
    const originalExpiry = readHostLease(hub.root, rootB.host.id)!.expiresAt;
    vi.setSystemTime(NOW + 20_000);
    await expect(bridge.syncPresence()).resolves.toBeUndefined();
    expect(readHostLease(hub.root, rootB.host.id)!.expiresAt).toBeGreaterThan(originalExpiry);
    expect(hub.get(hostKey(rootB.host.id), { fresh: true })?.value).toMatchObject({
      incarnationToken: "B", bridgeSupersededTokens: ["A"],
    });
    expect(hub.get(participantKey(rootB.participant.id), { fresh: true })).toEqual(before.participant);
    expect(held.holds(rootB.host.id, rootB.identity)).toBe(true);
    expect(hub.get(hostKey(unrelated.host.id), { fresh: true })?.value).toMatchObject({ remoteHost: "spoke" });

    // A same-token writer is still a valid renewal on the same direct link.
    const sameToken = fixture("direct-root", "B", NOW + 21_000, NOW + 140_000);
    sameToken.participant.name = "participant-B-renewed";
    await installNative(spoke, sameToken);
    await expect(bridge.syncPresence()).resolves.toBeUndefined();
    expect(hub.get(hostKey(rootB.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "B" });
    expect(hub.get(participantKey(rootB.participant.id), { fresh: true })?.value).toMatchObject({ name: "participant-B-renewed" });

    // The explicitly superseded token A remains fenced over the same direct RPC path.
    const replayA = fixture("direct-root", "A", NOW + 22_000, NOW + 140_000);
    replayA.participant.name = "replayed-A";
    await installNative(spoke, replayA);
    await expect(bridge.syncPresence()).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
    expect(hub.get(hostKey(rootB.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "B" });
    expect(hub.get(participantKey(rootB.participant.id), { fresh: true })?.value).toMatchObject({ name: "participant-B-renewed" });
    expect(hub.get(hostKey(unrelated.host.id), { fresh: true })?.value).toMatchObject({ remoteHost: "spoke" });
    expect(fs.readFileSync(hostLeasePath(hub.root, rootB.host.id))).not.toEqual(before.lease);
  });
});

// Keep reason: CODE-F1 is a recorded trust-boundary/data-safety failure. Exercise JSON in
// both directions, including native state with a valid lease that must NOT repair bad fields.
const invalidTimestampCases = ["record.startedAt", "record.updatedAt", "record.expiresAt", "expiresAt", "observedAt"].flatMap(field =>
  [{ label: "NaN", value: NaN }, { label: "null", value: null }, { label: "missing", value: undefined },
    { label: "string", value: String(NOW) }, { label: "Infinity", value: Infinity }, { label: "-Infinity", value: -Infinity },
    { label: "object", value: {} }].filter(({ label }) => field !== "observedAt" || label !== "missing")
    .map(({ label, value }) => ({ field, label, value }))).concat([
      { field: "expiresAt", label: "expired", value: NOW - 1 },
      { field: "expiresAt", label: "at-observation", value: NOW + 20_000 },
      { field: "observedAt", label: "after-expiry", value: NOW + 120_001 },
    ]);

for (const backend of ["file", "sqlite"] as const) {
  for (const direction of ["mirror RPC", "native presence RPC"] as const) {
    describe(`malformed direct timestamps (${backend}, ${direction})`, () => {
      it.each(invalidTimestampCases.flatMap(sample => [undefined, "B"].map(token => ({ ...sample, token,
        writer: token ?? "tokenless" }))))("preserves B, participants, lease and A fence for $field=$label ($writer)", async ({ field, value, token }) => {
        vi.useFakeTimers({ toFake: ["Date"] });
        vi.setSystemTime(NOW);
        const f = ownRootLink(false, backend);
        const receiver = direction === "mirror RPC" ? f.agent : f.local;
        const store = direction === "mirror RPC" ? f.work : f.hub;
        const a = fixture("matrix-root", "A", NOW - 2_000, NOW + 5_000);
        const b = fixture("matrix-root", "B", NOW - 1_000, NOW + 5_000);
        await receiver.mirror(a.presence);
        await receiver.mirror(b.presence);
        const before = { host: store.get(hostKey(b.host.id), { fresh: true }),
          participant: store.get(participantKey(b.host.id), { fresh: true }),
          lease: fs.readFileSync(hostLeasePath(store.root, b.host.id)) };
        vi.setSystemTime(NOW + 20_000); // preservation is not a renewal, even after B expired
        const legacy = fixture("matrix-root", token, NOW, NOW + 120_000);
        legacy.participant.name = "untrusted-tokenless";
        const unrelated = fixture("matrix-unrelated", "U", NOW, NOW + 120_000);
        const unknown = fixture("matrix-unknown", token, NOW, NOW + 120_000);
        const mutate = (sample: { record: FabricHostRecord; expiresAt: number; observedAt?: number }) => {
          const target = (field.startsWith("record.") ? sample.record : sample) as unknown as Record<string, unknown>;
          const name = field.replace("record.", "");
          if (value === undefined) delete target[name];
          else target[name] = value;
        };
        if (direction === "mirror RPC") {
          const samples = [legacy, unknown].map(v => ({ record: { ...v.host }, expiresAt: v.host.expiresAt, observedAt: NOW + 20_000 }));
          for (const sample of samples) mutate(sample);
          await f.remote.mirror({ hosts: [...samples, ...unrelated.presence.hosts],
            participants: [legacy.participant, unknown.participant, unrelated.participant] });
        } else {
          for (const v of [legacy, unknown, unrelated]) await installNative(f.work, v);
          if (field.startsWith("record.")) {
            for (const v of [legacy, unknown]) {
              const sample = { record: { ...v.host }, expiresAt: v.host.expiresAt };
              mutate(sample);
              await f.work.put({ key: hostKey(v.host.id), identity: v.identity, value: sample.record });
              // The valid file lease remains: effective liveness must not launder the bad host.
              expect(readHostLease(f.work.root, v.host.id)!.expiresAt).toBe(NOW + 120_000);
            }
          } else {
            const computed = await f.agent.presence();
            for (const sample of computed.hosts) if (sample.record.id !== unrelated.host.id) mutate(sample);
            vi.spyOn(f.agent, "presence").mockResolvedValueOnce(computed); // bad outer sample, real JSON response
          }
          await f.bridge.syncPresence();
        }
        expect(store.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
        expect(store.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
        expect(fs.readFileSync(hostLeasePath(store.root, b.host.id))).toEqual(before.lease);
        expect(store.get(hostKey(unknown.host.id), { fresh: true })).toBeUndefined();
        expect(store.get(participantKey(unknown.host.id), { fresh: true })).toBeUndefined();
        expect(readHostLease(store.root, unknown.host.id)).toBeUndefined();
        expect(store.get(hostKey(unrelated.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
        expect(store.get(participantKey(unrelated.host.id), { fresh: true })?.value).toMatchObject({ name: unrelated.participant.name });
        const replay = fixture("matrix-root", "A", NOW + 20_000, NOW + 120_000);
        if (direction === "mirror RPC") await expect(f.remote.mirror(replay.presence)).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
        else {
          await installNative(f.work, replay);
          await expect(f.bridge.syncPresence()).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
        }
        expect(store.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
        expect(store.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
        expect(fs.readFileSync(hostLeasePath(store.root, b.host.id))).toEqual(before.lease);
      });

      it("still admits coherent legacy tokenless renewal and same B, but withdraws genuinely omitted roots", async () => {
        vi.useFakeTimers({ toFake: ["Date"] });
        vi.setSystemTime(NOW);
        const f = ownRootLink(false, backend);
        const receiver = direction === "mirror RPC" ? f.agent : f.local;
        const store = direction === "mirror RPC" ? f.work : f.hub;
        const a = fixture("allowed-root", "A", NOW - 2_000, NOW + 5_000);
        const b = fixture("allowed-root", "B", NOW - 1_000, NOW + 5_000);
        await receiver.mirror(a.presence);
        await receiver.mirror(b.presence);
        const participant = store.get(participantKey(b.host.id), { fresh: true });
        vi.setSystemTime(NOW + 20_000);
        const legacy = fixture("allowed-root", undefined, NOW, NOW + 120_000);
        legacy.participant.name = "untrusted-tokenless";
        if (direction === "mirror RPC") await f.remote.mirror(legacy.presence); // absent observedAt is valid legacy
        else {
          await installNative(f.work, legacy);
          const computed = await f.agent.presence();
          for (const host of computed.hosts) delete host.observedAt;
          vi.spyOn(f.agent, "presence").mockResolvedValueOnce(computed);
          await f.bridge.syncPresence();
        }
        expect(store.get(participantKey(b.host.id), { fresh: true })).toEqual(participant);
        expect(readHostLease(store.root, b.host.id)).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"],
          expiresAt: NOW + 20_000 + BRIDGE_LEASE_MS });
        const same = fixture("allowed-root", "B", NOW + 20_000, NOW + 140_000);
        same.participant.name = "same-B-admitted";
        if (direction === "mirror RPC") await f.remote.mirror(same.presence);
        else { await installNative(f.work, same); await f.bridge.syncPresence(); }
        expect(store.get(participantKey(b.host.id), { fresh: true })?.value).toMatchObject({ name: "same-B-admitted" });
        if (direction === "mirror RPC") await f.remote.mirror({ hosts: [], participants: [] });
        else {
          // Real RPC empty snapshot; do not delete another fixture's files to model omission.
          vi.spyOn(f.agent, "presence").mockResolvedValueOnce({ hosts: [], participants: [], reserved: [] });
          await f.bridge.syncPresence();
        }
        expect(store.get(hostKey(b.host.id), { fresh: true })).toBeUndefined();
        expect(store.get(participantKey(b.host.id), { fresh: true })).toBeUndefined();
        expect(readHostLease(store.root, b.host.id)).toBeUndefined();
      });
    });
  }
}

describe("preservation-only native samples", () => {
  it.each(["file", "sqlite"] as const)("do not create native routing authority while unrelated commands continue (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const bad = fixture("invalid-native-target", undefined, NOW - 1_000, NOW + 120_000);
    const good = fixture("valid-native-target", "G", NOW - 1_000, NOW + 120_000);
    const sender = fixture("valid-remote-sender", "S", NOW - 1_000, NOW + 120_000);
    await installNative(f.hub, bad);
    await f.hub.put({ key: hostKey(bad.host.id), identity: bad.identity, value: { ...bad.host, startedAt: null } });
    await installNative(f.hub, good);
    await installNative(f.work, sender);
    await f.bridge.start();
    for (const target of [bad.host.id, bad.participant.label!, bad.participant.sessionId!, good.host.id]) {
      await f.work.publish({ topic: "fabric.control.command", kind: "followUp", from: sender.identity, to: target,
        data: { version: 1, commandId: `routing-${target}`, targetId: target, replyTo: sender.host.id,
          action: "followUp", text: "routing probe" } });
    }
    expect((await f.bridge.step()).toLocal).toBe(1);
    expect(f.hub.read({ after: 0 }).filter(event => event.topic === "fabric.control.command")).toMatchObject([{ to: good.host.id }]);
    expect(f.work.get(hostKey(bad.host.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.work.root, bad.host.id)).toBeUndefined();
  });
});

describe("malformed direct RPC recovery custody", () => {
  it.each(["file", "sqlite"] as const)("defers lease/unlease effects through recovery, repeated skips and plan retry (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const a = fixture("recovery-root", "A", NOW - 2_000, NOW + 5_000);
    const b = fixture("recovery-root", "B", NOW - 1_000, NOW + 5_000);
    await f.agent.mirror(a.presence);
    await f.agent.mirror(b.presence);
    const identity: MeshIdentity = { id: "bridge:hub", name: "hub", kind: "main" };
    const pending = new CommitOutbox(f.work, "bridge/hub", identity, {});
    const otherId = `${b.host.id}-other`;
    const lease = readHostLease(f.work.root, b.host.id)!;
    writeHostLease(f.work.root, { ...lease, id: otherId });
    await f.work.writeBatch({ identity, ops: [], prepare: () => pending.plan().stage([], [
      { kind: "lease", key: `lease:${b.host.id}`, payload: { ...lease, expiresAt: NOW + 999_000 } },
      { kind: "unlease", key: `lease:${otherId}`, payload: { ...lease, id: otherId, key: hostKey(otherId) } },
    ], { durable: true }) });
    const before = { host: f.work.get(hostKey(b.host.id), { fresh: true }),
      participant: f.work.get(participantKey(b.host.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.work.root, b.host.id)),
      otherLease: fs.readFileSync(hostLeasePath(f.work.root, otherId)), rows: f.work.listAll(pending.prefix, { fresh: true }) };
    const input = new PassThrough(), output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(f.work, "hub", () => Date.now()), input, output);
    const remote = new RemoteBridgeSide(output, input);
    closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    vi.setSystemTime(NOW + 20_000);
    const legacy = fixture("recovery-root", undefined, NOW, NOW + 120_000);
    const unrelated = fixture("recovery-unrelated", "U", NOW, NOW + 120_000);
    for (let pass = 0; pass < 3; pass++) {
      const write = f.work.writeBatch.bind(f.work);
      const retry = pass === 1 && backend === "sqlite" ? vi.spyOn(f.work, "writeBatch").mockImplementationOnce(async batch => {
        await write({ identity, ops: [{ kind: "put", key: "topology/hosts/retry-probe", value: {} }] });
        return write(batch);
      }) : undefined;
      await remote.mirror({ hosts: [{ record: { ...legacy.host, updatedAt: NaN }, expiresAt: NOW + 120_000,
        observedAt: NOW + 20_000 }, ...unrelated.presence.hosts], participants: [legacy.participant, unrelated.participant] });
      if (retry) { expect(retry).toHaveBeenCalledTimes(2); retry.mockRestore(); }
      expect(f.work.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
      expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
      expect(fs.readFileSync(hostLeasePath(f.work.root, b.host.id))).toEqual(before.lease);
      expect(fs.readFileSync(hostLeasePath(f.work.root, otherId))).toEqual(before.otherLease);
      for (const row of before.rows) expect(f.work.get(row.key, { fresh: true })).toEqual(row);
      expect(f.work.get(hostKey(unrelated.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
    }
    await remote.mirror({ hosts: [{ record: legacy.host, expiresAt: NOW + 120_000, observedAt: NOW + 20_000 }],
      participants: [legacy.participant] });
    expect(readHostLease(f.work.root, b.host.id)).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"],
      expiresAt: NOW + 20_000 + BRIDGE_LEASE_MS });
    expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
    const replay = fixture("recovery-root", "A", NOW + 20_000, NOW + 120_000);
    await expect(remote.mirror(replay.presence)).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
  });
});

// Keep reason: error metadata is an untrusted protocol boundary, not arbitrary remote text.
describe("stale incarnation diagnostics over the real RPC wire", () => {
  it.each([false, true])("retains root and failing host in explicit stale errors (sibling host=%s)", async sibling => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const f = ownRootLink(false);
    const a = fixture("diagnostic-root", "A", NOW - 2_000);
    const b = fixture("diagnostic-root", "B", NOW - 1_000);
    await f.agent.mirror(a.presence);
    await f.agent.mirror(b.presence);
    const before = { host: f.work.get(hostKey(b.host.id), { fresh: true }),
      participant: f.work.get(participantKey(b.host.id), { fresh: true }), lease: fs.readFileSync(hostLeasePath(f.work.root, b.host.id)) };
    if (sibling) a.host.id = "task:sibling-host-00000001"; // host IDs are not necessarily canonical roots
    const replies: Record<string, unknown>[] = [];
    f.remote.input.on("data", chunk => {
      for (const line of String(chunk).trim().split("\n")) replies.push(JSON.parse(line));
    });
    if (!sibling) await installNative(f.hub, a);
    const failed = sibling ? f.remote.mirror(a.presence) : f.bridge.syncPresence();
    const error = await failed.catch((value: unknown) => value);
    expect(error).toBeInstanceOf(BridgeStaleIncarnationError);
    expect(error).toMatchObject({ rootId: b.host.rootId, hostId: a.host.id,
      code: "FABRIC_BRIDGE_STALE_INCARNATION", retryable: false });
    expect((error as Error).message).toContain(b.host.rootId);
    expect((error as Error).message).toContain(a.host.id);
    expect(replies.find(reply => reply.code === "FABRIC_BRIDGE_STALE_INCARNATION"))
      .toMatchObject({ rootId: b.host.rootId, hostId: a.host.id });
    expect(f.work.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, b.host.id))).toEqual(before.lease);
    // Constructor compatibility: the historical single root argument remains argument 1.
    expect(new BridgeStaleIncarnationError(b.host.rootId).rootId).toBe(b.host.rootId);
    expect(new BridgeStaleIncarnationError(b.host.rootId).hostId).toBeUndefined();
  });

  it("also sanitizes optional metadata in a late stale mirror reply", async () => {
    vi.useFakeTimers();
    const input = new PassThrough(), output = new PassThrough();
    const remote = new RemoteBridgeSide(input, output, 10);
    closers.push(async () => { remote.close(); input.end(); output.end(); });
    let id: number | undefined;
    output.on("data", chunk => { id = (JSON.parse(String(chunk)) as { id: number }).id; });
    const pending = remote.mirror({ hosts: [], participants: [] }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(11);
    expect(await pending).toMatchObject({ code: "FABRIC_BRIDGE_STALE" });
    input.write(JSON.stringify({ id, ok: false, code: "FABRIC_BRIDGE_STALE_INCARNATION", error: "UNTRUSTED\u001b[31m",
      rootId: "session:late-root-00000001", hostId: "unsafe\nmetadata" }) + "\n");
    const closed = await remote.closed;
    expect(closed).toBeInstanceOf(BridgeStaleIncarnationError);
    expect(closed).toMatchObject({ rootId: "session:late-root-00000001", hostId: undefined });
    expect(closed.message).not.toMatch(/UNTRUSTED|unsafe|\u001b/);
  });

  it.each([
    { label: "legacy missing", fields: {}, rootId: undefined, hostId: undefined },
    { label: "valid independent host", fields: { rootId: "session:diagnostic-00000001", hostId: "task:sibling-1" }, rootId: "session:diagnostic-00000001", hostId: "task:sibling-1" },
    { label: "invalid root", fields: { rootId: "not-a-root", hostId: "task:valid" }, rootId: undefined, hostId: "task:valid" },
    { label: "invalid host", fields: { rootId: "session:diagnostic-00000001", hostId: { id: "unsafe" } }, rootId: "session:diagnostic-00000001", hostId: undefined },
    { label: "control characters", fields: { rootId: "session:diagnostic-00000001\n", hostId: "unsafe\u001b[31m" }, rootId: undefined, hostId: undefined },
    { label: "oversized", fields: { rootId: "session:" + "a".repeat(129), hostId: "a".repeat(257) }, rootId: undefined, hostId: undefined },
    { label: "empty and numeric", fields: { rootId: "", hostId: 42 }, rootId: undefined, hostId: undefined },
  ])("validates optional fields independently and ignores unsafe error text: $label", async ({ fields, rootId, hostId }) => {
    const input = new PassThrough(), output = new PassThrough();
    const remote = new RemoteBridgeSide(input, output);
    closers.push(async () => { remote.close(); input.end(); output.end(); });
    output.on("data", chunk => {
      const request = JSON.parse(String(chunk));
      input.write(JSON.stringify({ id: request.id, ok: false, code: "FABRIC_BRIDGE_STALE_INCARNATION",
        error: "UNTRUSTED\u001b[31m peer text", ...fields }) + "\n");
    });
    const error = await remote.mirror({ hosts: [], participants: [] }).catch((value: unknown) => value);
    expect(error).toBeInstanceOf(BridgeStaleIncarnationError);
    expect(error).toMatchObject({ rootId, hostId });
    expect((error as Error).message).not.toMatch(/UNTRUSTED|\u001b/);
    expect((error as Error).message).toBe(new BridgeStaleIncarnationError(rootId, hostId).message);
  });
});
