import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { importMeshStateToNats, rollbackMeshStateFromNats } from "../src/mesh/backend-migration-nats.js";
import { closeNatsStateBackend, MeshStateNatsInsecureError, MeshStateNatsRefusedError, openNatsStateBackend, type NatsLookup } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";

// pi-fabric#918 round 8 (Astra P1): the migration reaches nats-server through the backend's own endpoint path
// (connectNatsEndpoint): plaintext URLs pinned to their one loopback resolution, a refusal BEFORE any connect, and a
// remote server without auth_required refused before any write. The official client is replaced by a recorder that
// hands back a fake JetStream port.
const connectJetStream = vi.hoisted(() => vi.fn());
vi.mock("../src/mesh/state-backend-nats-client.js", () => ({ connectJetStream }));

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) { closeNatsStateBackend(root); fs.rmSync(root, { recursive: true, force: true }); }
});
beforeEach(() => connectJetStream.mockReset());
const seededRoot = async (): Promise<string> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-endpoint-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "seed/one", value: 1, identity });
  store.closeState();
  return root;
};
/** `nats.internal` resolves to `address` (a fake resolver; the real one is never asked). */
const resolvesTo = (address: string): NatsLookup => async (host) => host === "nats.internal" ? [{ address, family: 4 }] : [];
const plaintext = { servers: ["nats://nats.internal:4222"], monitoringUrl: "http://nats.internal:8222" };

describe("NATS endpoint path shared by the backend and the migration (pi-fabric#918 round 8)", () => {
  it("connects the migration and the backend to the PINNED loopback address, never the hostname", async () => {
    const server = fakeJetStream();
    connectJetStream.mockImplementation(async () => server.port());
    const root = await seededRoot();
    await importMeshStateToNats(root, { ...plaintext, lookup: resolvesTo("127.0.0.2") });
    expect(readMeshStateNatsMarker(root)).toBeDefined();
    await openNatsStateBackend({ root, ...plaintext, lookup: resolvesTo("127.0.0.2") });
    await rollbackMeshStateFromNats(root, { ...plaintext, lookup: resolvesTo("127.0.0.2") });
    expect(connectJetStream).toHaveBeenCalledTimes(3);
    for (const [servers, options] of connectJetStream.mock.calls) {
      expect(servers).toEqual(["nats://127.0.0.2:4222"]);
      expect(options).toMatchObject({ monitoringUrl: "http://127.0.0.2:8222/" });
    }
  });

  it("refuses an unsafe endpoint before any connect: a rejection, never a connection (import, rollback, open)", async () => {
    const root = await seededRoot();
    const before = fs.readFileSync(path.join(root, "state.json"));
    for (const unsafe of [
      { ...plaintext, lookup: resolvesTo("10.0.0.5") },                                         // resolves off loopback
      { servers: ["nats://10.0.0.5:4222"], lookup: resolvesTo("127.0.0.1") },                   // a remote IP, plaintext
      { servers: ["nats://127.0.0.1:4222"], monitoringUrl: "http://nats.internal:8222", lookup: resolvesTo("10.0.0.5") },
    ]) {
      await expect(importMeshStateToNats(root, unsafe)).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
      await expect(rollbackMeshStateFromNats(root, unsafe)).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
      await expect(openNatsStateBackend({ root, ...unsafe })).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
    }
    expect(connectJetStream).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(root, "state.json"))).toEqual(before);
  });

  it("refuses a remote server without auth_required before any write; with auth_required the import proceeds", async () => {
    const server = fakeJetStream();
    connectJetStream.mockImplementation(async () => server.port());
    const remote = { servers: ["tls://nats.example:4222"] };            // TLS: no pin, and a hostname is remote
    const root = await seededRoot();
    const before = fs.readFileSync(path.join(root, "state.json"));
    await expect(importMeshStateToNats(root, remote)).rejects.toThrow(MeshStateNatsRefusedError);
    await expect(importMeshStateToNats(root, remote)).rejects.toThrow(/non-loopback nats-server that does not require authentication/);
    await expect(rollbackMeshStateFromNats(root, remote)).rejects.toThrow(/does not require authentication/);
    expect(server.messages).toHaveLength(0);
    expect(fs.readFileSync(path.join(root, "state.json"))).toEqual(before);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    // The counterexample: the same remote server requiring authentication is accepted.
    server.authRequired = true;
    await importMeshStateToNats(root, remote);
    expect(readMeshStateNatsMarker(root)).toBeDefined();
  });
});
