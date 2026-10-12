import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { hostLiveness, LIVENESS_POLICY_KEY, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#6477: a hub with links to two spokes. A dead origin's mirrored host lease and root
// must lapse on every mesh within one BRIDGE_LEASE_MS of the origin's own expiry (+ one sync),
// and no mirror may ever outlive the origin's expiry.
const roots: string[] = [];
const links: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of links.splice(0)) await close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const SYNC_MS = 5_000;
const ORIGIN_TTL_MS = 15_000;

// A file-only native Main, as the fleet deploys it; `renew` is its heartbeat.
const native = async (store: MeshStore, name: string, now: number) => {
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  await store.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files", participants: "files" } });
  await store.put({ key: hostKey(identity.id), identity, value: {
    format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + ORIGIN_TTL_MS,
  } });
  const entry: MeshStateEntry = { key: participantKey(identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } };
  writeParticipantFile(store.root, entry);
  const renew = (at: number) => writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt: now, updatedAt: at, expiresAt: at + ORIGIN_TTL_MS });
  renew(now);
  return { identity, renew };
};

const fixture = async () => {
  let now = 1_800_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lease-loop-"));
  roots.push(dir);
  const store = (name: string) => new MeshStore(path.join(dir, name), 64 * 1024, 100);
  const hub = store("ryzen1");
  const spokes = { ryzen2: store("ryzen2"), ryzen3: store("ryzen3") };
  const clock = () => now;
  const bridges = Object.entries(spokes).map(([name, far]) => {
    const input = new PassThrough();
    const output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(far, "ryzen1", clock), input, output);
    const remote = new RemoteBridgeSide(output, input);
    links.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    return new MeshBridge({ localName: "ryzen1", remoteName: name, local: new StoreBridgeSide(hub, name, clock), remote,
      cursorPath: path.join(dir, `cursor-${name}.json`), presenceMs: SYNC_MS });
  });
  return { hub, spokes, bridges, now: () => now, advance: (ms: number) => { now += ms; } };
};

/** The effective lease a mesh holds for a host id (state record merged with its lease file). */
const liveness = (store: MeshStore, id: string) => {
  const entry = store.get(hostKey(id), { fresh: true });
  if (!entry) return undefined;
  const value = entry.value as { id: string; rootId: string; identity: { id: string }; startedAt?: number; updatedAt: number; expiresAt: number; remoteHost?: string };
  return { remoteHost: value.remoteHost, ...hostLiveness(readHostLeases(store.root), value) };
};

describe("mirrored leases keep their origin expiry (smarty-dev#6477)", () => {
  it("lapses a dead origin's host lease and root on every mesh, never outliving the origin", async () => {
    const f = await fixture();
    const origin = await native(f.spokes.ryzen3, "ghost", f.now());
    const witness = await native(f.spokes.ryzen2, "witness", f.now());
    const lead = await native(f.hub, "lead", f.now());
    for (const bridge of f.bridges) await bridge.start();
    const meshes = { ryzen1: f.hub, ...f.spokes };
    const trace: string[] = [];
    const violations: string[] = [];
    const originExpiry = () => hostLiveness(readHostLeases(f.spokes.ryzen3.root),
      f.spokes.ryzen3.get(hostKey(origin.identity.id))!.value as never).expiresAt;
    const diedAt = f.now() + 30_000;
    let deadline = Infinity;
    // One minute after the death, sampled every second; each bridge syncs on its own cadence.
    for (let tick = 0; tick <= 90; tick++) {
      const at = f.now();
      if (at < diedAt) origin.renew(at);
      for (const keep of [witness, lead]) keep.renew(at);
      if (at === diedAt) deadline = originExpiry() + BRIDGE_LEASE_MS + SYNC_MS;
      for (const bridge of f.bridges) await bridge.step();
      const expiry = originExpiry();
      for (const [name, mesh] of Object.entries(meshes)) {
        if (mesh === f.spokes.ryzen3) continue;
        const seen = liveness(mesh, origin.identity.id);
        const root = mesh.get(participantKey(origin.identity.id), { fresh: true });
        if (tick % 5 === 0) trace.push(`t+${(at - diedAt) / 1000}s ${name}: lease ${seen ? `${seen.remoteHost} expires t+${(seen.expiresAt - diedAt) / 1000}s` : "none"}, root ${root ? "present" : "none"} (origin expires t+${(expiry - diedAt) / 1000}s)`);
        if (seen && seen.expiresAt > expiry) violations.push(`${name} at t+${(at - diedAt) / 1000}s: mirror expires ${(seen.expiresAt - expiry) / 1000}s after its origin`);
        if (at >= deadline && seen && seen.expiresAt > at) violations.push(`${name} at t+${(at - diedAt) / 1000}s: lease still live`);
        if (at >= deadline && root) violations.push(`${name} at t+${(at - diedAt) / 1000}s: root still mirrored`);
      }
      f.advance(1_000);
    }
    console.log(trace.join("\n"));
    expect(violations).toEqual([]);
    // The live witnesses still cross through the hub, so the links stayed up throughout.
    expect(liveness(f.hub, witness.identity.id)).toMatchObject({ remoteHost: "ryzen2" });
    expect(liveness(f.spokes.ryzen2, lead.identity.id)).toMatchObject({ remoteHost: "ryzen1" });
    // A spoke never sees another spoke's root through the hub (no relay of mirrors).
    expect(f.spokes.ryzen2.get(hostKey(origin.identity.id))).toBeUndefined();
  });

  it("never mirrors a record another link marked, even when a caller hands one in", async () => {
    const f = await fixture();
    const now = f.now();
    const id = "session:relayed-00000000";
    const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: "relayed" };
    const record = { format: 1 as const, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + ORIGIN_TTL_MS,
      remoteHost: "ryzen3" };
    const participant = { format: 1 as const, id, kind: "root" as const, rootId: id, ownerHostId: id, ownerIdentityId: id,
      name: "relayed", status: "idle" as const, runner: "pi" as const, transport: "host" as const, capabilities: [],
      sessionId: "relayed", startedAt: now, updatedAt: now, remoteHost: "ryzen3" };
    await new StoreBridgeSide(f.hub, "ryzen2", f.now).mirror({ hosts: [{ record: record as never, expiresAt: now + ORIGIN_TTL_MS }],
      participants: [participant as never] });
    expect(f.hub.get(hostKey(id))).toBeUndefined();
    expect(f.hub.get(participantKey(id))).toBeUndefined();
    expect(readHostLeases(f.hub.root).has(id)).toBe(false);
  });
});
