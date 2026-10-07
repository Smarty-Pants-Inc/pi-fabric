import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const context = {} as FabricInvocationContext;

const identity: MeshIdentity = {
  id: "session:test",
  name: "main",
  kind: "main",
  sessionId: "test",
};

const participant = (id: string): FabricParticipantInfo => ({
  format: 1,
  id,
  kind: "actor",
  rootId: identity.id,
  ownerHostId: identity.id,
  ownerIdentityId: identity.id,
  parentId: identity.id,
  name: id,
  status: "idle",
  runner: "pi",
  transport: "host",
  capabilities: ["steer", "followUp", "stop", "fabric"],
  startedAt: 1,
  updatedAt: 2,
  controlProtocol: "v1",
  local: true,
  stale: false,
});

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("targeted publish outcomes", () => {
  it("records unknown-target refusal at the sender, but leaves broadcast publication unchanged", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-publish-failed-")); roots.push(root);
    const source = { get: () => undefined, list: () => [] } as unknown as FabricParticipantSource;
    const provider = new MeshProvider(new MeshStore(path.join(root, "mesh"), 64 * 1024, 100), identity, source);
    await expect(provider.invoke("publish", { topic: "work", to: "missing", text: "hello" }, context)).rejects.toThrow("Unknown Fabric target");
    const directory = path.join(provider.store.root, "delivery-outcomes");
    const rows = fs.readdirSync(directory).flatMap(file => fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(rows).toEqual([{ eventId: expect.any(String), to: "missing", from: identity.id, mode: "publish", outcome: "failed", reason: "Unknown Fabric target: missing", at: expect.any(Number) }]);
    expect(provider.store.latestSequence()).toBe(0);
    await expect(provider.invoke("publish", { topic: "work", text: "broadcast" }, context)).resolves.toMatchObject({ topic: "work", text: "broadcast" });
  });
});

describe("MeshProvider membership", () => {
  it("reserves topology state and acknowledged control topics for the host", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-provider-"));
    roots.push(root);
    const source: FabricParticipantSource = {
      list: () => [],
      get: () => undefined,
      self: () => participant("actor:self"),
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const provider = new MeshProvider(
      new MeshStore(path.join(root, "mesh"), 64 * 1024, 100),
      identity,
      source,
    );

    await expect(
      provider.invoke("publish", { topic: "fabric.control.ack", data: {} }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("publish", { topic: "fabric.control.command.v2", data: {} }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("publish", { topic: "fabric.participant.lifecycle", data: {} }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("publish", { topic: "fabric.actor.host-event", data: {} }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("put", { key: "topology/hosts/forged", value: {} }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("delete", { key: "sessions/peer" }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("put", { key: "residency/deliveries/forged", value: {} }, context),
    ).rejects.toThrow("reserved for host coordination");
    await expect(
      provider.invoke("get", { key: "residency/deliveries/private" }, context),
    ).rejects.toThrow("private host state");
    await expect(
      provider.invoke("list", { prefix: "residency/" }, context),
    ).rejects.toThrow("private host state");
  });

  it("uses the unified participant source with scope and kind filters", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-provider-"));
    roots.push(root);
    const list = vi.fn(() => [participant("actor:a"), participant("actor:b")]);
    const source: FabricParticipantSource = {
      list,
      get: () => undefined,
      self: () => participant("actor:self"),
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const provider = new MeshProvider(
      new MeshStore(path.join(root, "mesh"), 64 * 1024, 100),
      identity,
      source,
    );

    await expect(
      provider.invoke(
        "members",
        { scope: "lineage", kinds: ["actor"], includeStale: true, limit: 1 },
        context,
      ),
    ).resolves.toMatchObject([{ id: "actor:a", kind: "actor" }]);
    expect(list).toHaveBeenCalledWith({
      scope: "lineage",
      kinds: ["actor"],
      includeStale: true,
    });
    await expect(provider.invoke("self", {}, context)).resolves.toEqual(identity);
  });

  it("returns every member unless the caller asks for fewer (smarty-dev#1241)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-provider-"));
    roots.push(root);
    const all = Array.from({ length: 150 }, (_, index) => participant(`actor:${index}`));
    const source: FabricParticipantSource = {
      list: () => all,
      get: () => undefined,
      self: () => participant("actor:self"),
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const provider = new MeshProvider(new MeshStore(path.join(root, "mesh"), 64 * 1024, 100), identity, source);
    await expect(provider.invoke("members", {}, context)).resolves.toHaveLength(150);
    await expect(provider.invoke("members", { limit: 10 }, context)).resolves.toHaveLength(10);
  });
});
