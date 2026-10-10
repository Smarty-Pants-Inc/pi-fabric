import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, MeshBatchConflictError, type MeshStateEntry } from "../src/mesh/store.js";
import { openNatsMeshProvider } from "../src/mesh/state-async.js";
import { MeshListingIncompleteError } from "../src/mesh/listing.js";
import type { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const servers = process.env.FABRIC_NATS_TEST_SERVERS;
const identity = { id: "live-async-tools", name: "live async tools", kind: "main" as const };
const context = {} as FabricInvocationContext;
const participants = { list: () => [] } as unknown as FabricParticipantSource;
const roots: string[] = [], locals: MeshStore[] = [], providers: MeshProvider[] = [];
const open = async (root?: string) => {
  if (!root) { root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-live-async-tools-")); roots.push(root); }
  const local = new MeshStore(root, 128 * 1024, 100); locals.push(local);
  const provider = await openNatsMeshProvider(local, identity, participants, {
    backend: "nats-kv", nats: { servers: servers!.split(","), experimentalNatsKv: true },
  }); providers.push(provider); return { local, provider, root };
};
afterEach(async () => {
  for (const provider of providers.splice(0)) await provider.close();
  for (const local of locals.splice(0)) local.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
describe.skipIf(!servers)("real loopback R3 async mesh provider (no mocks)", () => {
  it("awaits leader read-your-writes through the public factory and isolates local typed/host state", async () => {
    const { local, provider } = await open(); const key = "shared/value";
    expect(await provider.invoke("get", { key }, context)).toBeNull();
    const first = await provider.invoke("put", { key, value: { text: "x".repeat(100 * 1024) }, ifVersion: 0 }, context) as MeshStateEntry;
    expect(await provider.invoke("get", { key }, context)).toEqual(first); expect(local.get(key)).toBeUndefined();
    await provider.invoke("put", { key: "state/current", value: "local authority" }, context);
    expect(local.get("state/current")?.value).toBe("local authority");
    await local.put({ key: "topology/hosts/local", value: "host", identity });
    await local.put({ key: "residency/private", value: "private", identity });
    expect((await provider.invoke("list", {}, context) as MeshStateEntry[]).map(e => e.key)).toEqual([key, "state/current", "topology/hosts/local"]);
    const deleted = await provider.invoke("delete", { key, ifVersion: first.version }, context) as { deleted: boolean; version: number };
    expect(deleted.deleted).toBe(true); expect(deleted.version).toBeGreaterThan(first.version);
    expect(await provider.invoke("get", { key }, context)).toBeNull();
    await expect(provider.invoke("put", { key, value: 2, ifVersion: 0 }, context)).rejects.toBeInstanceOf(MeshBatchConflictError);
    const recreated = await provider.invoke("put", { key, value: 2, ifVersion: deleted.version }, context) as MeshStateEntry;
    expect(await provider.invoke("get", { key }, context)).toEqual(recreated);
  });
  it.each(["sibling", "tombstone"])("raises a typed incomplete-list error when an earlier %s hides a later partial-prefix match", async mode => {
    const { local, provider } = await open();
    const earlier = await provider.invoke("put", { key: "shared/a", value: "earlier", ifVersion: 0 }, context) as MeshStateEntry;
    if (mode === "tombstone") await provider.invoke("delete", { key: earlier.key, ifVersion: earlier.version }, context);
    const match = await provider.invoke("put", { key: "shared/match", value: "later match", ifVersion: 0 }, context) as MeshStateEntry;
    expect(await provider.invoke("get", { key: match.key }, context)).toEqual(match);
    const pending = provider.invoke("list", { prefix: "shared/m", limit: 1 }, context);
    await expect(pending).rejects.toBeInstanceOf(MeshListingIncompleteError);
    await expect(pending).rejects.toMatchObject({ code: "MESH_LISTING_INCOMPLETE", prefix: "shared/m", limit: 1, examined: 1, nextRevision: match.version });
    expect(await provider.invoke("list", { prefix: "shared/m", limit: 2 }, context)).toEqual([match]);
    expect(await provider.invoke("list", { prefix: "shared/missing/", limit: 1 }, context)).toEqual([]);
    await local.put({ key: "local/a", value: "must not mask incomplete remote scan", identity });
    await expect(provider.invoke("list", { prefix: "", limit: 1 }, context)).rejects.toBeInstanceOf(MeshListingIncompleteError);
    if (process.env.FABRIC_NATS_EVIDENCE_DIR) fs.writeFileSync(path.join(process.env.FABRIC_NATS_EVIDENCE_DIR, `provider-list-incomplete-${mode}.json`), JSON.stringify({ mode, earlier, match, code: "MESH_LISTING_INCOMPLETE", examined: 1, nextRevision: match.version, completeAtLimit: 2, mixedRejected: true }, null, 2) + "\n");
  });
  it("fences eight independent provider connections, retaining only one stale-CAS winner", async () => {
    const first = await open(); const peers = [first, ...await Promise.all(Array.from({ length: 7 }, () => open(first.root)))];
    const key = "shared/race";
    const original = await first.provider.invoke("put", { key, value: -1, ifVersion: 0 }, context) as MeshStateEntry;
    const results = await Promise.allSettled(peers.map(({ provider }, value) => provider.invoke("put", { key, value, ifVersion: original.version }, context)));
    const winners = results.filter((result): result is PromiseFulfilledResult<unknown> => result.status === "fulfilled");
    expect(winners).toHaveLength(1);
    for (const result of results) if (result.status === "rejected") expect(result.reason).toBeInstanceOf(MeshBatchConflictError);
    for (const { provider } of peers) expect(await provider.invoke("get", { key }, context)).toEqual(winners[0]!.value);
    if (process.env.FABRIC_NATS_EVIDENCE_DIR) fs.writeFileSync(path.join(process.env.FABRIC_NATS_EVIDENCE_DIR, "provider-fencing.json"), JSON.stringify({ connections: 8, original, winner: winners[0]!.value, rejected: 7 }, null, 2) + "\n");
  });
});
