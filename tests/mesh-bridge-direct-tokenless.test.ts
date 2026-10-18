import { createHash } from "node:crypto";
import { PassThrough, Transform } from "node:stream";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, BRIDGE_PROTOCOL_VERSION, BridgeStaleIncarnationError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { runBridge } from "../src/mesh-bridge.js";
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
const ownRootLink = (monitorLease = true, backend: "file" | "sqlite" = "sqlite", legacy = false) => {
  const hub = openStore(backend);
  const work = openStore(backend);
  const input = new PassThrough();
  const output = new PassThrough();
  const local = new StoreBridgeSide(hub, "intel1", () => Date.now());
  const agent = new StoreBridgeSide(work, "hub", () => Date.now(), monitorLease);
  // Emulate the legacy hello on the actual RPC wire, preserving every other agent operation.
  let buffered = "";
  const legacyReplies = legacy ? new Transform({ transform(chunk, _encoding, done) {
    buffered += String(chunk);
    for (;;) {
      const newline = buffered.indexOf("\n");
      if (newline < 0) break;
      const response = JSON.parse(buffered.slice(0, newline));
      buffered = buffered.slice(newline + 1);
      if (response.result?.partialPresence === true) delete response.result.partialPresence;
      this.push(`${JSON.stringify(response)}\n`);
    }
    done();
  } }) : undefined;
  legacyReplies?.pipe(output);
  const serving = serveBridgeAgent(agent, input, legacyReplies ?? output);
  const remote = new RemoteBridgeSide(output, input);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "intel1", local, remote,
    cursorPath: path.join(hub.root, "intel1.cursor.json"), presenceMs: 0 });
  closers.push(async () => {
    await bridge.stop();
    remote.close();
    input.end();
    await serving;
    legacyReplies?.destroy();
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
  it("CLI keeps a legacy link alive after a partial presence cycle", async () => {
    const store = openStore("file");
    const flags = new Map([["mesh", store.root], ["name", "hub"], ["remote", "intel1"],
      ["cursor", path.join(store.root, "cursor.json")], ["call-timeout-ms", "1000"]]);
    // A real transport peer advertises notifications but predates partialPresence.
    const peer = `
      require("node:readline").createInterface({ input: process.stdin }).on("line", line => {
        const request = JSON.parse(line);
        const result = request.op === "hello" ? { version: ${BRIDGE_PROTOCOL_VERSION}, changes: true }
          : request.op === "presence" ? { hosts: [null], participants: [] }
          : request.op === "latestSequence" ? 0
          : request.op === "bridgedIds" ? { ids: [], through: 0, done: true }
          : request.op === "read" ? { events: [], through: 0, done: true } : {};
        process.stdout.write(JSON.stringify({ id: request.id, ok: true, result }) + "\\n");
      });
    `;
    const controller = new AbortController();
    const logs: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation(chunk => {
      logs.push(String(chunk));
      if (String(chunk).includes("presence INFO:")) controller.abort();
      return true;
    });
    const deadline = setTimeout(() => controller.abort(), 3_000);
    try {
      const code = await runBridge(flags, [process.execPath, "-e", peer], controller.signal);
      expect(code).toBe(0);
      expect(logs.join("")).toContain("linked hub <-> intel1");
      expect(logs.join("")).not.toContain("stopped:");
      expect(logs.filter(line => line.includes("presence INFO:")).length).toBe(1);
      expect(logs.join("")).toContain("peer=intel1 renewalAgeMs=");
      expect(logs.join("")).toMatch(/legacyCompletedPartials=[1-9]\d*/);
    } finally {
      clearTimeout(deadline);
      controller.abort();
    }
  });

  it("completes legacy partial mirrors and leaves partialPresence-capable payloads unchanged", async () => {
    const toPeer = new PassThrough();
    const fromPeer = new PassThrough();
    const remote = new RemoteBridgeSide(fromPeer, toPeer);
    const mirrorPayloads: unknown[] = [];
    let supportsPartial = false;
    toPeer.on("data", chunk => {
      const request = JSON.parse(String(chunk)) as { id: number; op: string; args?: unknown };
      if (request.op === "mirror") mirrorPayloads.push(request.args);
      fromPeer.write(`${JSON.stringify({ id: request.id, ok: true,
        result: request.op === "hello" ? { version: BRIDGE_PROTOCOL_VERSION, changes: true,
          ...(supportsPartial ? { partialPresence: true } : {}) } : request.op === "latestSequence" ? 7 : {} })}\n`);
    });
    const partial = { hosts: [], participants: [], partial: true, droppedCount: 1 };
    try {
      await remote.mirror({ hosts: [], participants: [] });
      await expect(remote.mirror(partial)).resolves.toBeUndefined();
      await remote.hello();
      await remote.mirror(partial);
      expect(mirrorPayloads).toEqual([
        { hosts: [], participants: [] }, { hosts: [], participants: [] }, { hosts: [], participants: [] },
      ]);
      expect(remote.legacyCompletedPartials).toBe(2);
      expect(remote.lastPresenceRenewalAt).toBeTypeOf("number");
      await expect(remote.latestSequence()).resolves.toBe(7);
      supportsPartial = true;
      await remote.hello();
      await remote.mirror(partial);
      expect(mirrorPayloads[3]).toEqual(partial);
      expect(remote.legacyCompletedPartials).toBe(2);
      supportsPartial = false;
      await remote.hello();
      await remote.mirror(partial);
      expect(mirrorPayloads[4]).toEqual({ hosts: [], participants: [] });
      expect(remote.legacyCompletedPartials).toBe(3);
    } finally {
      remote.close();
    }
    await expect(remote.mirror(partial)).rejects.toThrow("Bridge transport closed");
    expect(remote.legacyCompletedPartials).toBe(3);
  });

  it.each(["file", "sqlite"] as const)("does not trust peer protected:false metadata across real presence RPC (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("peer-false-protected-victim", "B", NOW - 1_000, NOW + 120_000);
    const progress = fixture("peer-false-protected-progress", "U", NOW, NOW + 120_000);
    await installNative(f.work, victim);
    await f.bridge.syncPresence();
    const before = { host: f.hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id)) };
    const hostile: BridgePresence = { hosts: progress.presence.hosts, participants: progress.presence.participants,
      dropped: [{ type: "host", protected: false, id: victim.host.id, rootId: victim.host.rootId,
        identityId: victim.identity.id }], partial: false, droppedCount: 0, reserved: [] };
    const response = vi.spyOn(f.agent, "presence").mockResolvedValue(hostile);

    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(response).toHaveBeenCalledTimes(1);
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id))).toEqual(before.lease);
    expect(f.hub.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });

    response.mockRestore();
    await f.work.delete({ key: hostKey(victim.host.id) });
    await f.work.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.work.root, victim.host.id), { force: true });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.hub.root, victim.host.id)).toBeUndefined();
  });

  it.each([
    ["file", "host", "missing"], ["sqlite", "host", "missing"],
    ["file", "participant", "missing"], ["sqlite", "participant", "missing"],
    ["file", "host", "foreign"], ["sqlite", "host", "foreign"],
    ["file", "participant", "foreign"], ["sqlite", "participant", "foreign"],
  ] as const)("%s treats peer-shaped %s with %s bridgeVia as partial and preserves the mirror", async (backend, kind, via) => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture(`peer-provenance-victim-${kind}-${via}`, "B", NOW - 1_000, NOW + 120_000);
    const progress = fixture(`peer-provenance-progress-${kind}-${via}`, "U", NOW, NOW + 120_000);
    await installNative(f.work, victim);
    await f.bridge.syncPresence();
    const before = { host: f.hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id)) };

    const peerMetadata = { remoteHost: "foreign-origin", ...(via === "foreign" ? { bridgeVia: "other-hub" } : {}) };
    const hostRecord = kind === "host" ? { ...victim.host, ...peerMetadata } : victim.host;
    const participantRecord = kind === "participant" ? { ...victim.participant, ...peerMetadata } : undefined;
    const claimed: BridgePresence = {
      hosts: kind === "host"
        ? [{ record: hostRecord, expiresAt: victim.host.expiresAt }, ...progress.presence.hosts]
        : [...victim.presence.hosts, ...progress.presence.hosts],
      participants: kind === "participant"
        ? [participantRecord!, ...progress.presence.participants]
        : [...progress.presence.participants],
      partial: false,
      droppedCount: 0,
      reserved: [],
    };
    const response = vi.spyOn(f.agent, "presence").mockResolvedValue(claimed);
    const mirrored = vi.spyOn(f.local, "mirror");
    await f.bridge.syncPresence();

    expect(response).toHaveBeenCalledTimes(1);
    if (kind === "host") {
      expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
      expect(fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id))).toEqual(before.lease);
    } else {
      expect(readHostLease(f.hub.root, victim.host.id)).toMatchObject({ id: victim.host.id,
        rootId: victim.host.rootId, identityId: victim.identity.id });
    }
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(mirrored.mock.lastCall?.[0]).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    expect(f.hub.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
    expect(f.hub.get(participantKey(progress.participant.id), { fresh: true })?.value).toMatchObject({ id: progress.participant.id });

    response.mockRestore();
    mirrored.mockRestore();
    await f.work.delete({ key: hostKey(victim.host.id) });
    await f.work.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.work.root, victim.host.id), { force: true });
    await f.bridge.syncPresence();
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.hub.root, victim.host.id)).toBeUndefined();
  });

  it.each(["file", "sqlite"] as const)("treats every non-object remote record as a partial drop across RPC (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("peer-nonobject-victim", "B", NOW - 1_000, NOW + 120_000);
    await installNative(f.work, victim);
    await f.bridge.syncPresence();
    const before = { host: f.hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id)) };
    const malformed = { hosts: [null, "not-a-host"], participants: [null], partial: false,
      droppedCount: 0, reserved: [] } as unknown as BridgePresence;
    const response = vi.spyOn(f.agent, "presence").mockResolvedValue(malformed);

    await expect(f.remote.presence()).resolves.toMatchObject({ hosts: [], participants: [], partial: true, droppedCount: 3 });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(response).toHaveBeenCalledTimes(2);
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id))).toEqual(before.lease);

    response.mockRestore();
    await f.work.delete({ key: hostKey(victim.host.id) });
    await f.work.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.work.root, victim.host.id), { force: true });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.hub.root, victim.host.id)).toBeUndefined();
  });

  it.each(["file", "sqlite"] as const)("admits raw hosts:[null] RPC as partial without expiry dereference (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const hub = openStore(backend);
    const local = new StoreBridgeSide(hub, "intel1", () => NOW);
    const victim = fixture("raw-null-host-victim", "B", NOW - 1_000, NOW + 120_000);
    await local.mirror(victim.presence, NOW);
    const before = { host: hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(hub.root, victim.host.id)) };
    const fromPeer = new PassThrough();
    const toPeer = new PassThrough();
    const remote = new RemoteBridgeSide(fromPeer, toPeer);
    let buffered = "";
    toPeer.on("data", chunk => {
      buffered += String(chunk);
      for (;;) {
        const newline = buffered.indexOf("\n");
        if (newline < 0) break;
        const request = JSON.parse(buffered.slice(0, newline)) as { id: number; op: string };
        buffered = buffered.slice(newline + 1);
        const result = request.op === "presence"
          ? { hosts: [null], participants: [], partial: false, droppedCount: 0, reserved: [] }
          : request.op === "hello"
            ? { version: BRIDGE_PROTOCOL_VERSION, changes: true, partialPresence: true, independentRpc: true }
            : {};
        fromPeer.write(`${JSON.stringify({ id: request.id, ok: true, result })}\n`);
      }
    });
    const bridge = new MeshBridge({ localName: "hub", remoteName: "intel1", local, remote,
      cursorPath: path.join(hub.root, "cursor.json"), presenceMs: 0 });
    closers.push(async () => { await bridge.stop(); remote.close(); fromPeer.end(); toPeer.end(); });

    const result = await bridge.syncPresence().then(() => "ok", error => error);
    if (result !== "ok") await bridge.stop(); // mirror the CLI's error teardown

    expect(result).toBe("ok");
    expect(hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(hub.root, victim.host.id))).toEqual(before.lease);
  });

  it.each(["file", "sqlite"] as const)("keeps pending unlease recovery pending across a partial pass (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const hub = openStore(backend);
    const victim = fixture("partial-recovery-unlease", "B", NOW - 1_000, NOW + 120_000);
    const lease = leaseFor(victim);
    writeHostLease(hub.root, lease);
    const identity: MeshIdentity = { id: "bridge:peer", name: "peer", kind: "main" };
    const outbox = new CommitOutbox(hub, "bridge/peer", identity, {});
    const gone = { key: hostKey(victim.host.id), id: victim.host.id, rootId: victim.host.rootId,
      identityId: victim.identity.id, startedAt: victim.host.startedAt, incarnationToken: victim.host.incarnationToken };
    const plan = outbox.plan();
    await hub.writeBatch({ identity, ops: [], prepare: () => plan.stage([], [
      { kind: "unlease", key: `lease:${victim.host.id}`, payload: gone },
    ], { durable: true }) });
    const rowKey = outbox.rowKey(`lease:${victim.host.id}`);
    const pending = hub.get(rowKey, { fresh: true });
    const side = new StoreBridgeSide(hub, "peer", () => NOW);

    await side.mirror({ hosts: [], participants: [], partial: true, droppedCount: 1 }, NOW);
    expect(hub.get(rowKey, { fresh: true })).toEqual(pending);
    expect(readHostLease(hub.root, victim.host.id)).toEqual(lease);

    await side.mirror({ hosts: [], participants: [] }, NOW);
    expect(hub.get(rowKey, { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, victim.host.id)).toBeUndefined();
  });

  it.each([false, true] as const)("deduplicates identical participant copies and forwards resident/runtime hosts without partialing (legacy=%s)", async legacy => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, "sqlite", legacy);
    const root = fixture("normal-presence-shape", "R", NOW - 1_000, NOW + 120_000);
    const remoteRoot = fixture("remote-nonroot-parent", "S", NOW - 1_000, NOW + 120_000);
    root.participant.updatedAt = NOW;
    await installNative(f.hub, root);
    installParticipantFile(f.hub, root);
    await installNative(f.work, remoteRoot);
    const remoteChildId = "runtime:remote-child-activation";
    const remoteChildIdentity: MeshIdentity = { id: remoteChildId, name: "remote-child", kind: "agent" };
    await f.work.put({ key: hostKey(remoteChildId), identity: remoteChildIdentity,
      value: { format: 1, id: remoteChildId, rootId: remoteRoot.host.id, identity: remoteChildIdentity,
        startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 120_000 } satisfies FabricHostRecord });
    const nonRootHosts = [
      { id: `resident:${"a".repeat(24)}`, name: "resident" },
      { id: "runtime:child-activation", name: "runtime" },
    ];
    for (const { id, name } of nonRootHosts) {
      const identity: MeshIdentity = { id, name, kind: "agent" };
      const record: FabricHostRecord = { format: 1, id, rootId: root.host.id, identity,
        startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 120_000 };
      await f.hub.put({ key: hostKey(id), identity, value: record });
    }
    await f.remote.hello();
    const mirrors: BridgePresence[] = [];
    const mirror = f.remote.mirror.bind(f.remote);
    vi.spyOn(f.remote, "mirror").mockImplementation(async presence => {
      mirrors.push(presence as BridgePresence);
      await mirror(presence);
    });

    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const sampled = await f.local.presence();
    expect(sampled).not.toHaveProperty("partial");
    expect(sampled).not.toHaveProperty("droppedCount");
    expect(sampled.hosts.map(host => host.record.id)).toEqual(expect.arrayContaining([
      root.host.id, ...nonRootHosts.map(host => host.id),
    ]));
    expect(sampled.reserved).toEqual(expect.arrayContaining(nonRootHosts.map(host => host.id)));
    expect(mirrors).toHaveLength(1);
    expect(mirrors[0]).not.toHaveProperty("partial");
    expect(mirrors[0]!.hosts.map(host => host.record.id)).toEqual(expect.arrayContaining([
      root.host.id, ...nonRootHosts.map(host => host.id),
    ]));
    expect(mirrors[0]!.participants).toEqual([root.participant]);
    for (const host of nonRootHosts) {
      expect(f.work.get(hostKey(host.id), { fresh: true })?.value).toMatchObject({
        id: host.id, rootId: root.host.id, remoteHost: "hub",
      });
      expect(readHostLease(f.work.root, host.id)?.expiresAt).toBeGreaterThan(NOW);
    }
    expect(f.hub.get(hostKey(remoteChildId), { fresh: true })?.value).toMatchObject({
      id: remoteChildId, rootId: remoteRoot.host.id, remoteHost: "intel1",
    });
    expect(readHostLease(f.hub.root, remoteChildId)?.expiresAt).toBeGreaterThan(NOW);
    expect(f.remote.legacyCompletedPartials).toBe(0);
  });

  it.each(["file-only-invalid", "conflicting-copy"] as const)("keeps genuinely invalid or conflicting participant copies partial (%s)", async shape => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, "sqlite");
    const root = fixture(`participant-copy-${shape}`, "R", NOW - 1_000, NOW + 120_000);
    root.participant.updatedAt = NOW;
    await installNative(f.hub, root);
    if (shape === "file-only-invalid") {
      const invalid = { ...root.participant, id: "session:file-only-invalid-00000001", ownerHostId: undefined };
      writeParticipantFile(f.hub.root, { key: participantKey(invalid.id), version: 1, updatedAt: NOW,
        updatedBy: root.identity, value: invalid as unknown as FabricParticipantRecord });
    } else {
      const conflicting = { ...root.participant, ownerHostId: "session:other-owner-00000001",
        ownerIdentityId: "session:other-owner-00000001", updatedAt: NOW + 1 };
      writeParticipantFile(f.hub.root, { key: participantKey(conflicting.id), version: 1, updatedAt: conflicting.updatedAt,
        updatedBy: root.identity, value: conflicting });
    }
    const presence = await f.local.presence();
    expect(presence.partial).toBe(true);
    expect(presence.droppedCount ?? 0).toBeGreaterThan(0);
  });

  it.each(["file", "sqlite"] as const)("legacy partial cycles hold omissions on both sides and later full cycles mirror normally (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend, true);
    const victim = fixture("legacy-peer-outbound", "B", NOW - 2_000, NOW + 120_000);
    const peerRoot = fixture("legacy-peer-inbound", "P", NOW - 1_000, NOW + 120_000);
    await installNative(f.hub, victim);
    await installNative(f.work, peerRoot);
    await f.remote.hello();
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const snapshot = () => ({ hubHost: f.hub.get(hostKey(peerRoot.host.id), { fresh: true }),
      hubParticipant: f.hub.get(participantKey(peerRoot.participant.id), { fresh: true }),
      hubLease: fs.readFileSync(hostLeasePath(f.hub.root, peerRoot.host.id)),
      workHost: f.work.get(hostKey(victim.host.id), { fresh: true }),
      workParticipant: f.work.get(participantKey(victim.participant.id), { fresh: true }),
      workLease: fs.readFileSync(hostLeasePath(f.work.root, victim.host.id)) });
    const before = snapshot();
    expect(before.hubHost).toBeDefined();
    expect(before.workHost).toBeDefined();
    const malformed = fixture("legacy-peer-malformed", undefined, NOW, NOW + 120_000);
    await f.hub.put({ key: hostKey(victim.host.id), identity: victim.identity,
      value: { ...victim.host, rootId: malformed.host.rootId } });
    await f.hub.put({ key: participantKey(victim.participant.id), identity: victim.identity,
      value: { ...victim.participant, rootId: malformed.host.rootId } });
    const outboundProgress = fixture("legacy-peer-outbound-progress", "U", NOW, NOW + 120_000);
    const inboundProgress = fixture("legacy-peer-inbound-progress", "V", NOW, NOW + 120_000);
    await installNative(f.hub, outboundProgress);
    await installNative(f.work, inboundProgress);
    expect(await f.local.presence()).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    // Omit the old peer root too: partial from either direction must hold BOTH omission sets.
    const peerPresence = vi.spyOn(f.agent, "presence").mockResolvedValue({ ...inboundProgress.presence, reserved: [] });
    const remoteMirror = vi.spyOn(f.agent, "mirror");
    for (let cycle = 1; cycle <= 2; cycle += 1) {
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      const after = snapshot();
      expect(after.hubHost?.value).toMatchObject(before.hubHost?.value as Record<string, unknown>);
      expect(after.hubParticipant?.value).toMatchObject(before.hubParticipant?.value as Record<string, unknown>);
      expect(after.workHost?.value).toMatchObject({ id: victim.host.id, rootId: victim.host.id,
        incarnationToken: "B", remoteHost: "hub" });
      expect(after.workParticipant?.value).toMatchObject(before.workParticipant?.value as Record<string, unknown>);
      expect(f.remote.legacyCompletedPartials).toBe(cycle);
      const outbound = remoteMirror.mock.calls[cycle - 1]![0] as Record<string, unknown> & { hosts: Array<{ record: FabricHostRecord }> };
      expect(outbound).not.toHaveProperty("partial");
      expect(outbound).not.toHaveProperty("dropped");
      expect(outbound).not.toHaveProperty("droppedCount");
      expect(outbound.hosts.map(host => host.record.id)).toEqual(expect.arrayContaining([victim.host.id, outboundProgress.host.id]));
      expect(outbound.hosts.map(host => host.record.id)).not.toContain(malformed.host.id);
      expect(f.hub.get(hostKey(inboundProgress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "V" });
      expect(f.work.get(hostKey(outboundProgress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
      expect(readHostLease(f.work.root, victim.host.id)?.expiresAt).toBeGreaterThan(Date.now());
      await expect(f.remote.latestSequence()).resolves.toBeTypeOf("number");
    }

    peerPresence.mockRestore();
    await f.hub.delete({ key: hostKey(victim.host.id) });
    await f.hub.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.hub.root, victim.host.id), { force: true });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(f.remote.legacyCompletedPartials).toBe(2);
    expect(remoteMirror).toHaveBeenCalledTimes(3);
    expect(f.work.get(hostKey(outboundProgress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
    expect(f.work.get(participantKey(outboundProgress.host.id), { fresh: true })?.value).toMatchObject({ id: outboundProgress.host.id });
    expect(f.work.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(participantKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.work.root, victim.host.id)).toBeUndefined();
    expect(f.hub.get(hostKey(peerRoot.host.id), { fresh: true })).toBeDefined();
  });

  it.each(["file", "sqlite"] as const)("renews a previously mirrored hub root through 20 persistent frame-budget partials and delivers a later follow-up (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend, true);
    const target = fixture("legacy-budget-target", "H", NOW - 1_000, NOW + 120_000);
    const peerRoot = fixture("legacy-budget-peer", "P", NOW - 1_000, NOW + 120_000);
    await installNative(f.hub, target);
    await installNative(f.work, peerRoot);
    await f.bridge.start();
    const mirroredHubIds = () => f.work.listAll("topology/hosts/", { fresh: true })
      .filter(entry => (entry.value as Record<string, unknown>).remoteHost === "hub")
      .map(entry => (entry.value as FabricHostRecord).id).sort();
    const mirroredParticipantIds = () => f.work.listAll("topology/participants/", { fresh: true })
      .filter(entry => (entry.value as Record<string, unknown>).remoteHost === "hub")
      .map(entry => (entry.value as FabricParticipantRecord).id).sort();
    const beforeHosts = mirroredHubIds();
    const beforeParticipants = mirroredParticipantIds();
    expect(beforeHosts).toEqual([target.host.id]);
    expect(beforeParticipants).toEqual([target.participant.id]);

    // Keep the persisted row small, but make its live bridge sample exceed the production 8 MiB
    // frame. This deterministically reproduces the same persistent boundPresence drop as the hub.
    const presence = f.local.presence.bind(f.local);
    const diagnosticPayload = "x".repeat(8 * 1024 * 1024);
    vi.spyOn(f.local, "presence").mockImplementation(async () => {
      const sampled = await presence();
      return { ...sampled, hosts: sampled.hosts.map(host => host.record.id === target.host.id
        ? { ...host, record: { ...host.record, diagnosticPayload } as FabricHostRecord } : host) };
    });
    for (let cycle = 1; cycle <= 20; cycle += 1) {
      const now = NOW + cycle * 2_500;
      vi.setSystemTime(now);
      await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
      expect(f.remote.legacyCompletedPartials).toBe(cycle);
      expect(f.work.get(hostKey(target.host.id), { fresh: true })).toBeDefined();
      expect(f.work.get(participantKey(target.participant.id), { fresh: true })).toBeDefined();
      expect(readHostLease(f.work.root, target.host.id)?.expiresAt).toBeGreaterThanOrEqual(now + BRIDGE_LEASE_MS);
      expect(readHostLease(f.hub.root, peerRoot.host.id)?.expiresAt).toBeGreaterThanOrEqual(now + BRIDGE_LEASE_MS);
      expect(mirroredHubIds()).toEqual(beforeHosts);
      expect(mirroredParticipantIds()).toEqual(beforeParticipants);
    }

    const now = NOW + 50_000;
    vi.setSystemTime(now); // More than three 15 s mirror-lease periods after the initial complete mirror.
    const sent = await f.work.publish({ topic: "fabric.control.command", kind: "followUp", from: peerRoot.identity,
      to: target.host.id, data: { version: 1, commandId: "legacy-after-partials", targetId: target.host.id,
        replyTo: peerRoot.host.id, action: "followUp", text: "still usable after persistent partials" } });
    expect((await f.bridge.step()).toLocal).toBe(1);
    expect(f.hub.read({ after: 0 }).filter(event => event.topic === "fabric.control.command")).toMatchObject([
      { data: { bridge: { id: sent.id } } },
    ]);
  });

  it("CLI error before bridge construction leaves an existing mirror for its lease TTL", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const store = openStore("file");
    const side = new StoreBridgeSide(store, "intel1", () => NOW);
    const peerRoot = fixture("cli-error-fallback", "P", NOW - 1_000, NOW + 120_000);
    await side.mirror(peerRoot.presence, NOW);
    const before = { host: store.get(hostKey(peerRoot.host.id), { fresh: true }),
      participant: store.get(participantKey(peerRoot.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(store.root, peerRoot.host.id)) };
    const flags = new Map([["mesh", store.root], ["name", "hub"], ["remote", "intel1"],
      ["cursor", path.join(store.root, "cursor.json")]]);

    await expect(runBridge(flags, [process.execPath, "-e", "setInterval(() => {}, 1e6)"], new AbortController().signal,
      () => { throw new Error("injected failure before bridge construction"); })).resolves.toBe(1);

    expect(store.get(hostKey(peerRoot.host.id), { fresh: true })).toEqual(before.host);
    expect(store.get(participantKey(peerRoot.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(store.root, peerRoot.host.id))).toEqual(before.lease);
  });

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

  it.each(["file", "sqlite"] as const)("partial outbound admission cannot withdraw a previously mirrored root (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("partial-outbound-victim", "B", NOW - 1_000, NOW + 120_000);
    await installNative(f.hub, victim);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const before = { host: f.work.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.work.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.work.root, victim.host.id)) };
    const malformed = fixture("partial-outbound-forged", undefined, NOW, NOW + 120_000);
    const progress = fixture("partial-outbound-progress", "U", NOW, NOW + 120_000);
    await f.hub.put({ key: hostKey(victim.host.id), identity: victim.identity,
      value: { ...victim.host, rootId: malformed.host.rootId } });
    await f.hub.put({ key: participantKey(victim.participant.id), identity: victim.identity,
      value: { ...victim.participant, rootId: malformed.host.rootId } });
    await installNative(f.hub, progress);
    const mirrored = vi.spyOn(f.agent, "mirror");

    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(mirrored.mock.lastCall?.[0]).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    expect((mirrored.mock.lastCall?.[0].droppedCount ?? 0)).toBeGreaterThan(0);
    expect(f.work.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, victim.host.id))).toEqual(before.lease);
    expect(f.work.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });

    await f.hub.delete({ key: hostKey(victim.host.id) });
    await f.hub.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.hub.root, victim.host.id), { force: true });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(f.work.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(participantKey(victim.participant.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.work.root, victim.host.id)).toBeUndefined();
  });

  it.each(["file", "sqlite"] as const)("partial inbound admission cannot withdraw a previously mirrored root (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("partial-inbound-victim", "B", NOW - 1_000, NOW + 120_000);
    await installNative(f.work, victim);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const before = { host: f.hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id)) };
    const malformed = fixture("partial-inbound-forged", undefined, NOW, NOW + 120_000);
    const progress = fixture("partial-inbound-progress", "U", NOW, NOW + 120_000);
    await f.work.put({ key: hostKey(victim.host.id), identity: victim.identity,
      value: { ...victim.host, rootId: malformed.host.rootId } });
    await f.work.put({ key: participantKey(victim.participant.id), identity: victim.identity,
      value: { ...victim.participant, rootId: malformed.host.rootId } });
    await installNative(f.work, progress);
    const mirrored = vi.spyOn(f.local, "mirror");

    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(mirrored.mock.lastCall?.[0]).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    expect((mirrored.mock.lastCall?.[0].droppedCount ?? 0)).toBeGreaterThan(0);
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id))).toEqual(before.lease);
    expect(f.hub.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });

    await f.work.delete({ key: hostKey(victim.host.id) });
    await f.work.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.work.root, victim.host.id), { force: true });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.hub.root, victim.host.id)).toBeUndefined();
  });

  it.each(["file", "sqlite"] as const)("raw unidentifiable presence is partial and cannot withdraw prior authority (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("partial-unidentifiable-victim", "B", NOW - 1_000, NOW + 120_000);
    await installNative(f.work, victim);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const before = { host: f.hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id)) };
    const { rootId: _rootId, identity: _identity, ...unidentifiable } = victim.host;
    await f.work.put({ key: hostKey(victim.host.id), identity: victim.identity, value: unidentifiable });
    const source = await f.agent.presence();
    expect(source).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    expect((source.droppedCount ?? 0)).toBeGreaterThan(0);
    const mirrored = vi.spyOn(f.local, "mirror");

    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(mirrored.mock.lastCall?.[0]).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id))).toEqual(before.lease);
  });

  it.each(["file", "sqlite"] as const)("raw mirror RPC drops a participant with a missing required timestamp without withdrawing prior mirrors (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("rpc-participant-victim", "B", NOW - 1_000, NOW + 120_000);
    await f.agent.mirror(victim.presence);
    const before = { host: f.work.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.work.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.work.root, victim.host.id)) };

    const malformed = fixture("rpc-participant-malformed", "M", NOW, NOW + 120_000);
    const invalidParticipant = { ...malformed.participant, updatedAt: null } as unknown as FabricParticipantRecord;
    const progress = fixture("rpc-participant-progress", "U", NOW, NOW + 120_000);
    await f.remote.mirror({ hosts: [...malformed.presence.hosts, ...progress.presence.hosts],
      participants: [invalidParticipant, progress.participant] });

    expect(f.work.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, victim.host.id))).toEqual(before.lease);
    expect(f.work.get(participantKey(malformed.participant.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
    expect(f.work.get(participantKey(progress.participant.id), { fresh: true })?.value).toMatchObject({ id: progress.participant.id });

    // A clean, fully valid snapshot still authorizes omission cleanup.
    await f.remote.mirror(progress.presence);
    expect(f.work.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(participantKey(victim.participant.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.work.root, victim.host.id)).toBeUndefined();
  });

  it.each(["file", "sqlite"] as const)("source scan marks a malformed participant timestamp partial while valid roots progress (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victim = fixture("scan-participant-victim", "B", NOW - 1_000, NOW + 120_000);
    await installNative(f.work, victim);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const before = { host: f.hub.get(hostKey(victim.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victim.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id)) };

    await f.work.delete({ key: hostKey(victim.host.id) });
    await f.work.delete({ key: participantKey(victim.participant.id) });
    fs.rmSync(hostLeasePath(f.work.root, victim.host.id), { force: true });
    const malformed = fixture("scan-participant-malformed", "M", NOW, NOW + 120_000);
    const invalidParticipant = { ...malformed.participant, updatedAt: null } as unknown as FabricParticipantRecord;
    await f.work.put({ key: hostKey(malformed.host.id), identity: malformed.identity, value: malformed.host });
    await f.work.put({ key: participantKey(invalidParticipant.id), identity: malformed.identity, value: invalidParticipant });
    writeHostLease(f.work.root, leaseFor(malformed));
    const progress = fixture("scan-participant-progress", "U", NOW, NOW + 120_000);
    await installNative(f.work, progress);

    const source = await f.remote.presence();
    expect(source).toMatchObject({ partial: true, droppedCount: expect.any(Number) });
    expect((source.droppedCount ?? 0)).toBeGreaterThan(0);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victim.host.id))).toEqual(before.lease);
    expect(f.hub.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
    expect(f.hub.get(participantKey(progress.participant.id), { fresh: true })?.value).toMatchObject({ id: progress.participant.id });
    expect(f.hub.get(participantKey(malformed.participant.id), { fresh: true })).toBeUndefined();

    // A later complete snapshot still withdraws the genuinely absent victim.
    await f.work.delete({ key: hostKey(malformed.host.id) });
    await f.work.delete({ key: participantKey(malformed.participant.id) });
    fs.rmSync(hostLeasePath(f.work.root, malformed.host.id), { force: true });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    expect(f.hub.get(hostKey(victim.host.id), { fresh: true })).toBeUndefined();
    expect(f.hub.get(participantKey(victim.participant.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.hub.root, victim.host.id)).toBeUndefined();
  });

  it.each(["file", "sqlite"] as const)("real sync does not turn a dropped malformed cross-root host into withdrawal (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victimA = fixture("sync-cross-root-victim", "A", NOW - 2_000, NOW + 120_000);
    const victimB = fixture("sync-cross-root-victim", "B", NOW - 1_000, NOW + 120_000);
    await installNative(f.work, victimA);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    await installNative(f.work, victimB);
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();
    const before = { host: f.hub.get(hostKey(victimB.host.id), { fresh: true }),
      participant: f.hub.get(participantKey(victimB.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.hub.root, victimB.host.id)) };
    expect(before.host?.value).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"] });

    const forgedRoot = fixture("sync-forged-cross-root", undefined, NOW, NOW + 120_000);
    const malformedHost: FabricHostRecord = { ...victimB.host, rootId: forgedRoot.host.rootId, updatedAt: Number.NaN };
    const malformedParticipant: FabricParticipantRecord = { ...victimB.participant, rootId: forgedRoot.host.rootId };
    await installNative(f.work, { ...victimB, host: malformedHost, participant: malformedParticipant });
    expect(f.work.get(hostKey(victimB.host.id), { fresh: true })?.value).toMatchObject({ rootId: forgedRoot.host.rootId });
    await expect(f.bridge.syncPresence()).resolves.toBeUndefined();

    expect(f.hub.get(hostKey(victimB.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victimB.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victimB.host.id))).toEqual(before.lease);

    await installNative(f.work, victimA);
    await expect(f.bridge.syncPresence()).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
    expect(f.hub.get(hostKey(victimB.host.id), { fresh: true })).toEqual(before.host);
    expect(f.hub.get(participantKey(victimB.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.hub.root, victimB.host.id))).toEqual(before.lease);
  });

  it.each(["file", "sqlite"] as const)("preserves every root touched by a malformed cross-root RPC claim (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const victimA = fixture("cross-root-victim", "A", NOW - 2_000, NOW + 120_000);
    const victimB = fixture("cross-root-victim", "B", NOW - 1_000, NOW + 120_000);
    await f.agent.mirror(victimA.presence);
    await f.agent.mirror(victimB.presence);
    const before = { host: f.work.get(hostKey(victimB.host.id), { fresh: true }),
      participant: f.work.get(participantKey(victimB.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.work.root, victimB.host.id)) };
    expect(before.host?.value).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"] });

    const forgedRoot = fixture("forged-cross-root", undefined, NOW, NOW + 120_000);
    const { incarnationToken: _victimToken, ...victimWithoutToken } = victimB.host;
    const forgedHost: FabricHostRecord = { ...victimWithoutToken, rootId: forgedRoot.host.rootId,
      updatedAt: Number.NaN };
    const unrelated = fixture("cross-root-unrelated", "D", NOW, NOW + 120_000);
    const progress = fixture("cross-root-progress", "U", NOW, NOW + 120_000);
    const spoofedParticipant: FabricParticipantRecord = { ...unrelated.participant, id: victimB.participant.id,
      ownerHostId: unrelated.host.id, ownerIdentityId: unrelated.identity.id, rootId: unrelated.host.rootId,
      name: "spoofed-victim-participant" };
    expect(JSON.stringify(Number.NaN)).toBe("null");
    await f.remote.mirror({
      hosts: [{ record: forgedHost, expiresAt: NOW + 120_000, observedAt: NOW },
        ...unrelated.presence.hosts, ...progress.presence.hosts],
      participants: [spoofedParticipant, progress.participant],
    });

    expect(f.work.get(hostKey(victimB.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(victimB.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, victimB.host.id))).toEqual(before.lease);
    expect(f.work.get(hostKey(unrelated.host.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(participantKey(unrelated.participant.id), { fresh: true })).toBeUndefined();
    expect(f.work.get(hostKey(progress.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "U" });
    expect(f.work.get(participantKey(progress.participant.id), { fresh: true })?.value).toMatchObject({ rootId: progress.host.rootId });

    await expect(f.remote.mirror(victimA.presence)).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
    expect(f.work.get(hostKey(victimB.host.id), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(victimB.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, victimB.host.id))).toEqual(before.lease);
  });

  it.each(["file", "sqlite"] as const)("preserves the owner of a claimed root alias before omission (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const f = ownRootLink(false, backend);
    const aliasId = "session:claimed-root-alias-00000001";
    const alias = (value: ReturnType<typeof fixture>) => {
      const identity: MeshIdentity = { ...value.identity, id: aliasId };
      const host: FabricHostRecord = { ...value.host, id: aliasId, identity };
      const participant: FabricParticipantRecord = { ...value.participant, id: aliasId,
        ownerHostId: aliasId, ownerIdentityId: aliasId, rootId: value.host.rootId };
      return { identity, host, participant, presence: { hosts: [{ record: host, expiresAt: host.expiresAt }], participants: [participant] } };
    };
    const ownerA = alias(fixture("claimed-root-owner", "A", NOW - 2_000, NOW + 120_000));
    const ownerB = alias(fixture("claimed-root-owner", "B", NOW - 1_000, NOW + 120_000));
    await f.agent.mirror(ownerA.presence);
    await f.agent.mirror(ownerB.presence);
    const before = { host: f.work.get(hostKey(aliasId), { fresh: true }),
      participant: f.work.get(participantKey(aliasId), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(f.work.root, aliasId)) };
    expect(before.host?.value).toMatchObject({ rootId: ownerB.host.rootId, incarnationToken: "B", bridgeSupersededTokens: ["A"] });

    const malformed = fixture("claimed-root-malformed", undefined, NOW, NOW + 120_000);
    const claimedRoot = aliasId;
    const malformedHost: FabricHostRecord = { ...malformed.host, rootId: claimedRoot, updatedAt: Number.NaN };
    await f.remote.mirror({ hosts: [{ record: malformedHost, expiresAt: NOW + 120_000, observedAt: NOW }], participants: [] });
    expect(f.work.get(hostKey(aliasId), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(aliasId), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, aliasId))).toEqual(before.lease);

    await expect(f.remote.mirror(ownerA.presence)).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
    expect(f.work.get(hostKey(aliasId), { fresh: true })).toEqual(before.host);
    expect(f.work.get(participantKey(aliasId), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(f.work.root, aliasId))).toEqual(before.lease);
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
    .map(({ label, value }) => ({ field, label, value })));
const expiredTimestampCases = [
  { field: "expiresAt", label: "expired", value: NOW - 1 },
  { field: "expiresAt", label: "at-observation", value: NOW + 20_000 },
  { field: "observedAt", label: "after-expiry", value: NOW + 120_001 },
];

for (const backend of ["file", "sqlite"] as const) {
  for (const direction of ["mirror RPC", "native presence RPC"] as const) {
    describe(`malformed direct timestamps (${backend}, ${direction})`, () => {
      it.each([...invalidTimestampCases.flatMap(sample => [undefined, "B"].map(token => ({ ...sample, token,
        writer: token ?? "tokenless" }))), ...expiredTimestampCases.map(sample => ({ ...sample, token: undefined,
        writer: "tokenless" }))])("preserves B, participants, lease and A fence for $field=$label ($writer)", async ({ field, value, token }) => {
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

      it.each(expiredTimestampCases)("same-token real RPC preserves malformed, renews valid, then withdraws $field=$label", async ({ field, value }) => {
        vi.useFakeTimers({ toFake: ["Date"] });
        vi.setSystemTime(NOW);
        const f = ownRootLink(false, backend);
        const receiver = direction === "mirror RPC" ? f.agent : f.local;
        const store = direction === "mirror RPC" ? f.work : f.hub;
        const a = fixture("withdrawal-root", "A", NOW - 2_000, NOW + 5_000);
        const b = fixture("withdrawal-root", "B", NOW - 1_000, NOW + 5_000);
        await receiver.mirror(a.presence);
        await receiver.mirror(b.presence);
        const before = { host: store.get(hostKey(b.host.id), { fresh: true }),
          participant: store.get(participantKey(b.host.id), { fresh: true }),
          lease: fs.readFileSync(hostLeasePath(store.root, b.host.id)) };
        vi.setSystemTime(NOW + 20_000);
        const same = fixture("withdrawal-root", "B", b.host.startedAt, NOW + 120_000);
        same.host.updatedAt = NOW + 20_000;
        same.participant.name = "valid-same-B";
        const send = async (outer: { expiresAt: number; observedAt: number }) => {
          const hosts = [{ record: same.host, ...outer }];
          if (direction === "mirror RPC") await f.remote.mirror({ hosts, participants: [same.participant] });
          else {
            await installNative(f.work, same);
            vi.spyOn(f.agent, "presence").mockResolvedValueOnce({ hosts, participants: [same.participant], reserved: [] });
            await f.bridge.syncPresence(); // real native presence reply -> JSON -> hub receiver
          }
        };
        await send({ expiresAt: NaN, observedAt: NOW + 20_000 });
        expect(store.get(hostKey(b.host.id), { fresh: true })).toEqual(before.host);
        expect(store.get(participantKey(b.host.id), { fresh: true })).toEqual(before.participant);
        expect(fs.readFileSync(hostLeasePath(store.root, b.host.id))).toEqual(before.lease);
        await send({ expiresAt: same.host.expiresAt, observedAt: NOW + 20_000 });
        expect(store.get(hostKey(b.host.id), { fresh: true })?.value).toMatchObject({ incarnationToken: "B",
          bridgeSupersededTokens: ["A"], expiresAt: NOW + 20_000 + BRIDGE_LEASE_MS });
        expect(store.get(participantKey(b.host.id), { fresh: true })?.value).toMatchObject({ name: "valid-same-B" });
        expect(readHostLease(store.root, b.host.id)).toMatchObject({ incarnationToken: "B", bridgeSupersededTokens: ["A"],
          expiresAt: NOW + 20_000 + BRIDGE_LEASE_MS });
        expect(fs.readFileSync(hostLeasePath(store.root, b.host.id))).not.toEqual(before.lease);
        await send({ expiresAt: same.host.expiresAt, observedAt: NOW + 20_000, [field]: value });
        expect(store.get(hostKey(b.host.id), { fresh: true })).toBeUndefined();
        expect(store.get(participantKey(b.host.id), { fresh: true })).toBeUndefined();
        expect(readHostLease(store.root, b.host.id)).toBeUndefined();
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

describe("commit-time lease identity digest", () => {
  it.each(["file", "sqlite"] as const)("detects an identity-only lease retarget before applying a colliding plan (%s)", async backend => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    const store = openStore(backend);
    const side = new StoreBridgeSide(store, "intel1", () => Date.now());
    const ownerBefore = fixture("identity-digest-owner", "B0", NOW - 2_000, NOW + 120_000);
    await side.mirror(ownerBefore.presence, NOW);
    const ownerNext = fixture("identity-digest-owner", "B1", NOW - 1_000, NOW + 120_000);
    const claimant = fixture("identity-digest-claimant", "X", NOW - 1_000, NOW + 120_000);
    const unrelatedIdentityId = "session:identity-digest-before-00000001";
    const ghostLease = { id: claimant.host.id, rootId: claimant.host.rootId, identityId: unrelatedIdentityId,
      startedAt: claimant.host.startedAt, updatedAt: claimant.host.updatedAt, expiresAt: claimant.host.expiresAt };
    writeHostLease(store.root, ghostLease);
    const retargetedLease = { ...ghostLease, identityId: ownerBefore.host.id };
    const before = { host: store.get(hostKey(ownerBefore.host.id), { fresh: true }),
      participant: store.get(participantKey(ownerBefore.participant.id), { fresh: true }),
      lease: fs.readFileSync(hostLeasePath(store.root, ownerBefore.host.id)) };
    const candidate: BridgePresence = {
      hosts: [...claimant.presence.hosts, ...ownerNext.presence.hosts],
      participants: [...claimant.presence.participants, ...ownerNext.presence.participants],
      reserved: [],
    };
    const write = store.writeBatch.bind(store);
    let batches = 0;
    vi.spyOn(store, "writeBatch").mockImplementation(async input => {
      batches += 1;
      if (batches === 1) writeHostLease(store.root, retargetedLease);
      return write(input);
    });

    await expect(side.mirror(candidate, NOW)).resolves.toBeUndefined();

    expect(store.get(hostKey(ownerBefore.host.id), { fresh: true })).toEqual(before.host);
    expect(store.get(participantKey(ownerBefore.participant.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(store.root, ownerBefore.host.id))).toEqual(before.lease);
    expect(readHostLease(store.root, claimant.host.id)).toMatchObject(retargetedLease);
    expect(store.get(hostKey(claimant.host.id), { fresh: true })).toBeUndefined();
    expect(batches).toBe(backend === "sqlite" ? 2 : 1);
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
