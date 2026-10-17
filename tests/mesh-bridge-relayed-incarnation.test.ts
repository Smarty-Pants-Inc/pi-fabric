import { createHash } from "node:crypto";
import { PassThrough } from "node:stream";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [], stores: MeshStore[] = [], closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const store of stores.splice(0).reverse()) store.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => `${prefix}${createHash("sha256").update(id).digest("hex")}`;
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const fixture = (name: string, token: string | undefined, now: number, relayed = false) => {
  const identity: MeshIdentity = { id: `session:${name}-00000001`, name, kind: "main", sessionId: name };
  const attribution = relayed ? { remoteHost: "origin", bridgeVia: "hub" } : {};
  const host: FabricHostRecord = { format: 1, id: identity.id, rootId: identity.id, identity,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000, ...attribution,
    ...(token === undefined ? {} : { incarnationToken: token }) };
  const participant: FabricParticipantRecord = { format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, ownerIncarnation: `fixture:${token}`, name,
    label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1", ...attribution };
  return { identity, host, participant, presence: { hosts: [{ record: host, expiresAt: host.expiresAt }], participants: [participant] } };
};
const leaseFor = ({ host }: ReturnType<typeof fixture>) => ({ id: host.id, rootId: host.rootId,
  identityId: host.identity.id, startedAt: host.startedAt, updatedAt: host.updatedAt,
  expiresAt: host.expiresAt, ...(host.incarnationToken ? { incarnationToken: host.incarnationToken } : {}) });
const openStore = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-relayed-incarnation-"));
  roots.push(root);
  const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend: "sqlite" });
  stores.push(store);
  return store;
};
const native = async (store: MeshStore, name: string) => {
  const value = fixture(name, "normal-A", Date.now());
  await store.put({ key: hostKey(value.host.id), identity: value.identity, value: value.host });
  writeHostLease(store.root, leaseFor(value));
  writeParticipantFile(store.root, { key: participantKey(value.host.id), version: 1, updatedAt: value.host.updatedAt,
    updatedBy: value.identity, value: value.participant });
  return value;
};
const makeBridge = () => {
  const hub = openStore(), spoke = openStore(), input = new PassThrough(), output = new PassThrough();
  const serving = serveBridgeAgent(new StoreBridgeSide(spoke, "hub"), input, output);
  const remote = new RemoteBridgeSide(output, input);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "spoke", local: new StoreBridgeSide(hub, "spoke"),
    remote, cursorPath: path.join(hub.root, "spoke.cursor.json"), presenceMs: 0 });
  closers.push(async () => { await bridge.stop(); remote.close(); input.end(); await serving; output.end(); });
  return { hub, spoke, bridge };
};
const replaceHub = async (hub: MeshStore, token: string | undefined, now: number) => {
  const value = fixture("relayed-root", token, now);
  await hub.put({ key: hostKey(value.host.id), identity: value.identity, value: { ...value.host, remoteHost: "origin" } });
  return value;
};
const expectNormals = (store: MeshStore, normals: ReturnType<typeof fixture>[]) => {
  for (const { host } of normals) {
    for (const k of [hostKey(host.id), participantKey(host.id)])
      expect(store.get(k, { fresh: true })?.value).toMatchObject({ id: host.id, remoteHost: "hub" });
    expect(readHostLease(store.root, host.id)).toMatchObject({ incarnationToken: "normal-A" });
  }
};

describe("trusted-hub relayed incarnation skips", () => {
  // Original mixed-release test-first coverage: real MeshBridge + PassThrough RPC + SQLite/FILE.
  it.each(["tokenless", "older"] as const)("does not reject the whole presence batch for a %s relayed root", async staleKind => {
    const { hub, spoke, bridge } = makeBridge(), now = Date.now();
    const relay = fixture("relayed-root", "relay-A", now), id = relay.host.id;
    await new StoreBridgeSide(hub, "origin").mirror(relay.presence);
    await bridge.start();
    if (staleKind === "older") {
      const newer = await replaceHub(hub, "relay-B", now + 1_000);
      writeHostLease(hub.root, leaseFor(newer));
      await bridge.syncPresence();
    }
    const beforeHost = spoke.get(hostKey(id), { fresh: true }), beforeParticipant = spoke.get(participantKey(id), { fresh: true });
    const beforeLease = fs.readFileSync(hostLeasePath(spoke.root, id));
    expect(beforeHost?.value).toMatchObject({ remoteHost: "origin", bridgeVia: "hub" });
    expect(beforeParticipant?.value).toMatchObject({ remoteHost: "origin", bridgeVia: "hub" });
    expect(readHostLease(spoke.root, id)).toBeDefined();
    await replaceHub(hub, staleKind === "tokenless" ? undefined : "relay-A", now + (staleKind === "tokenless" ? 2_000 : 0));
    if (staleKind === "older") writeHostLease(hub.root, { ...leaseFor(fixture("relayed-root", "relay-B", now + 1_000)),
      bridgeSupersededTokens: ["relay-A"] });
    const normals = [await native(hub, "normal-one"), await native(hub, "normal-two")];
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await expect(bridge.syncPresence()).resolves.toBeUndefined();
    expect(warning.mock.calls.map(([message]) => String(message))).toEqual([expect.stringContaining(id)]);
    expect(spoke.get(hostKey(id), { fresh: true })).toEqual(beforeHost);
    expect(spoke.get(participantKey(id), { fresh: true })).toEqual(beforeParticipant);
    expect(fs.readFileSync(hostLeasePath(spoke.root, id))).toEqual(beforeLease);
    expectNormals(spoke, normals);
  });

  it.each(["unlease", "lease", "failed-retry"].flatMap(effect => ["older", "tokenless"].map(staleKind => ({ effect, staleKind }))))(
    "retains $staleKind skipped-root $effect work across recovery/recompute and afterCommit", async ({ effect, staleKind }) => {
    const store = openStore(), now = Date.now(), b = fixture("relayed-root", "relay-B", now, true), id = b.host.id;
    const cleanup = fixture("cleanup-root", "cleanup-B", now, true);
    const authority = { ...leaseFor(b), updatedAt: now - 1_000, expiresAt: now + 5_000,
      bridgeSupersededTokens: ["relay-A"], bridgeSupersededTokenSeen: { "relay-A": 0 } };
    writeHostLease(store.root, authority);
    writeHostLease(store.root, leaseFor(cleanup));
    const side = new StoreBridgeSide(store, "hub"), identity: MeshIdentity = { id: "bridge:hub", name: "hub", kind: "main" };
    const pending = new CommitOutbox(store, "bridge/hub", identity, {});
    const unlease = (value: ReturnType<typeof fixture>) => ({ kind: "unlease", key: `lease:${value.host.id}`,
      payload: { ...leaseFor(value), key: hostKey(value.host.id) } });
    if (effect === "failed-retry") {
      const rename = fs.renameSync;
      const fault = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(to) === hostLeasePath(store.root, id)) throw new Error("injected lease effect failure");
        return rename(from, to);
      });
      await side.mirror({ hosts: [...b.presence.hosts, ...cleanup.presence.hosts],
        participants: [b.participant, cleanup.participant] }, undefined, true);
      expect(fault.mock.calls.filter(([, to]) => String(to) === hostLeasePath(store.root, id)).length).toBeGreaterThan(0);
      fault.mockRestore();
    } else {
      // Exact G-1 crash image for unlease: state removal committed, B lease/history and its real outbox row survive.
      await store.writeBatch({ identity, ops: [], prepare: () => pending.plan().stage(effect === "lease" ? [
        { kind: "put", key: hostKey(id), identity: b.identity, value: b.host },
        { kind: "put", key: participantKey(id), identity: b.identity, value: b.participant },
      ] : [], [effect === "unlease" ? unlease(b) : { kind: "lease", key: `lease:${id}`,
        payload: { ...authority, updatedAt: now, expiresAt: now + 60_000 } }, unlease(cleanup)], { durable: true }) });
    }
    const rowKey = pending.rowKey(`lease:${id}`), beforeRow = store.get(rowKey, { fresh: true });
    const beforeHost = store.get(hostKey(id), { fresh: true }), beforeParticipant = store.get(participantKey(id), { fresh: true });
    const beforeLease = fs.readFileSync(hostLeasePath(store.root, id));
    expect(beforeRow?.value).toMatchObject({ kind: effect === "unlease" ? "unlease" : "lease" });
    if (effect === "unlease") { expect(beforeHost).toBeUndefined(); expect(beforeParticipant).toBeUndefined(); }
    const normals = [fixture("normal-one", "normal-A", now), fixture("normal-two", "normal-A", now)];
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    // First reconnect and repeat, both tokenless and proven older; the operation must never latch the link.
    for (const [pass, token] of [staleKind === "older" ? "relay-A" : undefined, undefined, "relay-A"].entries()) {
      const stale = fixture("relayed-root", token, now, true);
      // Force the warmed early plan to retry on the second pass, without touching lease evidence.
      const write = store.writeBatch.bind(store);
      const retry = pass === 1 ? vi.spyOn(store, "writeBatch").mockImplementationOnce(async input => {
        await write({ identity, ops: [{ kind: "put", key: "topology/hosts/retry-probe", value: {} }] });
        return write(input);
      }) : undefined;
      await expect(side.mirror({ hosts: [...stale.presence.hosts, ...normals.flatMap(v => v.presence.hosts)],
        participants: [stale.participant, ...normals.map(v => v.participant)] }, undefined, true)).resolves.toBeUndefined();
      if (retry) { expect(retry).toHaveBeenCalledTimes(2); retry.mockRestore(); }
      expect(store.get(hostKey(id), { fresh: true })).toEqual(beforeHost);
      expect(store.get(participantKey(id), { fresh: true })).toEqual(beforeParticipant);
      expect(fs.readFileSync(hostLeasePath(store.root, id))).toEqual(beforeLease);
      expect(store.get(rowKey, { fresh: true })).toEqual(beforeRow);
      expectNormals(store, normals);
      expect(readHostLease(store.root, cleanup.host.id)).toBeUndefined();
      expect(store.get(hostKey(cleanup.host.id), { fresh: true })).toBeUndefined();
    }
    expect(warning.mock.calls.map(([message]) => String(message))).toEqual(Array(3).fill(expect.stringContaining(id)));
    // Allowed counterexamples: same-token recovery/renewal, then a genuine new token; pending work may be superseded.
    for (const token of ["relay-B", "relay-C"]) {
      const allowed = fixture("relayed-root", token, now + 1_000, true);
      await side.mirror({ hosts: [...allowed.presence.hosts, ...normals.flatMap(v => v.presence.hosts)],
        participants: [allowed.participant, ...normals.map(v => v.participant)] }, undefined, true);
      expect(store.get(hostKey(id), { fresh: true })?.value).toMatchObject({ incarnationToken: token });
      expect(readHostLease(store.root, id)).toMatchObject({ incarnationToken: token, bridgeSupersededTokens: expect.arrayContaining(["relay-A"]) });
      expect(store.get(rowKey, { fresh: true })?.value).toMatchObject({ kind: "lease", payload: { incarnationToken: token } });
    }
    expect(warning).toHaveBeenCalledTimes(3);
  });
});
