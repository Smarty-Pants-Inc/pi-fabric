import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { importMeshStateToNats, type MeshNatsCommitCheck } from "../src/mesh/backend-migration-nats.js";
import { proveFabricReader } from "../src/mesh/reader-proof.js";
import { MeshStateNatsInsecureError, natsStreamName, type NatsLookup } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream, type FakeJetStreamServer } from "./helpers/nats-jetstream-fake.js";

// The nats reader proof reaches the server only through connectNatsEndpoint (pi-fabric#918 round 8) and proves only this
// root's stream (pi-fabric#937). Fake ports and recorded calls only: no network, no deletes.
const dirs: string[] = [];
afterEach(() => { vi.unstubAllEnvs(); for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const tempDir = (label: string): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-nats-proof-security-${label}-`));
  dirs.push(dir);
  return dir;
};
const SWITCH = "0123456789abcdef0123456789abcdef";

/** A root with one entry, staged (imported, not committed) on `server`: the switch's check. */
const staged = async (label: string, server: FakeJetStreamServer): Promise<{ root: string; check: MeshNatsCommitCheck }> => {
  const root = tempDir(label);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "a", value: 1, identity });
  store.closeState();
  const checks: MeshNatsCommitCheck[] = [];
  await expect(importMeshStateToNats(root, { port: server.port(), beforeCommit: (check) => { checks.push(check); throw new Error("gate: unready"); } }))
    .rejects.toThrow("gate: unready");
  return { root, check: checks[0]! };
};

describe("nats reader proof endpoint security", () => {
  it("refuses a foreign root's stream before any server call or proof write", async () => {
    const root = tempDir("own");
    const other = tempDir("other");
    for (const stream of [`${natsStreamName(other)}_${SWITCH}`, natsStreamName(other), "FABRIC_MESH_missing",
      `${natsStreamName(root)}_XYZ`, `${natsStreamName(root)}_${SWITCH}.x`]) {
      const server = fakeJetStream();
      const port = server.port();
      await expect(proveFabricReader(root, { backend: "nats", nats: { stream, port } })).rejects.toThrow(/not this mesh root's/);
      expect(port.calls).toEqual([]);
    }
    expect(fs.existsSync(path.join(root, "readers"))).toBe(false);
  });

  it("refuses a non-loopback server without authentication before any stream read", async () => {
    const server = fakeJetStream();
    const { root, check } = await staged("remote", server);
    vi.stubEnv("PI_FABRIC_RELEASE_SHA", "relA");
    server.authRequired = false;
    const port = server.port();
    await expect(proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port, servers: ["tls://nats.example.com:4222"] } }))
      .rejects.toThrow(/does not require authentication/);
    expect(port.calls).toEqual(["server"]);
    expect(fs.existsSync(path.join(root, "readers"))).toBe(false);
    // The same remote server requiring authentication: the proof reads and binds the staged stream.
    server.authRequired = true;
    const proved = await proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: server.port(),
      servers: ["tls://nats.example.com:4222"], replicas: 1 } });
    expect(proved.entries).toBe(1);
    expect(proved.proof).toMatchObject({ backends: ["nats"], switchId: check.switchId, stream: check.stream, head: check.head, digest: check.digest });
  });

  it("refuses an unsynced or oversized-payload server before any stream read, unless sync is accepted explicitly", async () => {
    const server = fakeJetStream();
    const { root, check } = await staged("sync", server);
    vi.stubEnv("PI_FABRIC_RELEASE_SHA", "relA");
    server.syncAlways = false;
    const unsynced = server.port();
    await expect(proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: unsynced } })).rejects.toThrow(/sync_always/);
    expect(unsynced.calls).toEqual(["server"]);
    server.maxPayload = 64 * 1024 * 1024;
    const oversized = server.port();
    await expect(proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: oversized, acceptPeriodicSync: true } }))
      .rejects.toThrow(/max_payload/);
    expect(oversized.calls).toEqual(["server"]);
    server.maxPayload = 1024 * 1024;
    const accepted = await proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: server.port(), acceptPeriodicSync: true } });
    expect(accepted.entries).toBe(1);
  });

  it("awaits plaintext/DNS endpoint validation before port.server()", async () => {
    const server = fakeJetStream();
    const { root, check } = await staged("dns", server);
    const lookups: string[] = [];
    // A slow resolver to a non-loopback address: an unawaited check would let the read start first.
    const remote: NatsLookup = async (host) => { lookups.push(host); await new Promise(resolve => setTimeout(resolve, 20)); return [{ address: "10.0.0.5", family: 4 }]; };
    const mixed: NatsLookup = async () => [{ address: "127.0.0.1", family: 4 }, { address: "10.0.0.5", family: 4 }];
    const failing: NatsLookup = async () => { throw new Error("ENOTFOUND"); };
    const cases: { servers?: string[]; monitoringUrl?: string; lookup?: NatsLookup }[] = [
      { servers: ["nats://nats.example.com:4222"], lookup: remote },
      { servers: ["nats://nats.example.com:4222"], lookup: mixed },
      { servers: ["nats://nats.example.com:4222"], lookup: failing },
      { servers: ["nats://10.0.0.1:4222"] },
      { servers: ["tls://127.0.0.1:4222"], monitoringUrl: "http://monitor.example.com:8222", lookup: remote },
    ];
    for (const endpoint of cases) {
      const port = server.port();
      await expect(proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port, ...endpoint } }))
        .rejects.toBeInstanceOf(MeshStateNatsInsecureError);
      expect(port.calls).toEqual([]);
    }
    expect(lookups).toEqual(["nats.example.com", "monitor.example.com"]);
    expect(fs.existsSync(path.join(root, "readers"))).toBe(false);
    // A plaintext name that resolves only to loopback is pinned and allowed.
    vi.stubEnv("PI_FABRIC_RELEASE_SHA", "relA");
    const loopback: NatsLookup = async () => [{ address: "127.0.0.1", family: 4 }];
    const proved = await proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: server.port(),
      servers: ["nats://localhost:4222"], lookup: loopback } });
    expect(proved.entries).toBe(1);
  });
});
