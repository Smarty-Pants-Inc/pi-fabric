import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import * as asyncState from "../src/mesh/state-async.js";
import type { AsyncMeshStateStore } from "../src/mesh/state-async.js";
import { MeshListingIncompleteError } from "../src/mesh/listing.js";
import type { NatsKvListPage } from "../src/mesh/state-nats-kv.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const identity = { id: "async-tools", name: "async tools", kind: "main" as const };
const context = {} as FabricInvocationContext;
const participants = { list: () => [] } as unknown as FabricParticipantSource;
const options = { backend: "nats-kv" as const, nats: { servers: "nats://127.0.0.1:1", experimentalNatsKv: true } };
const entry = (key: string, value: unknown = 1): MeshStateEntry => ({ key, value, version: 7, updatedAt: 1, updatedBy: identity });
const deferred = <T>() => { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; };
let root: string;
let local: MeshStore;
let remote: AsyncMeshStateStore & Required<Pick<AsyncMeshStateStore, "listPage">>;
const providers: MeshProvider[] = [];
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-async-tools-"));
  local = new MeshStore(root, 128 * 1024, 100);
  remote = { kind: "nats-kv", get: vi.fn(async () => undefined), list: vi.fn(async () => []),
    listPage: vi.fn(async () => ({ entries: [], examined: 0 })), listAll: vi.fn(async () => []),
    put: vi.fn(async input => entry(input.key, input.value)), delete: vi.fn(async () => ({ deleted: false })), close: vi.fn(async () => {}) };
  vi.spyOn(asyncState, "openAsyncMeshStateStore").mockResolvedValue(remote);
});
afterEach(async () => {
  for (const provider of providers.splice(0)) await provider.close();
  local.closeState(); fs.rmSync(root, { recursive: true, force: true }); vi.restoreAllMocks();
});
const open = async () => { const provider = await MeshProvider.withStateBackend(local, identity, participants, options); providers.push(provider); return provider; };

describe("explicit NATS async mesh-tool seam (mock adapter, no live durability claim)", () => {
  it("keeps ordinary construction local and refuses unsupported selectors/flag before opening", async () => {
    await local.put({ key: "shared/a", value: "local", identity });
    const provider = new MeshProvider(local, identity, participants); providers.push(provider);
    expect(await provider.invoke("get", { key: "shared/a" }, context)).toMatchObject({ value: "local" });
    expect(asyncState.openAsyncMeshStateStore).not.toHaveBeenCalled();
    await expect(MeshProvider.withStateBackend(local, identity, participants, { ...options, nats: { ...options.nats, experimentalNatsKv: false } })).rejects.toThrow(/experimentalNatsKv/);
    await expect(asyncState.openNatsMeshProvider(local, identity, participants, { ...options, backend: "file" } as unknown as typeof options)).rejects.toThrow(/nats-kv/);
    expect(asyncState.openAsyncMeshStateStore).not.toHaveBeenCalled();
  });
  it("closes an incompatible async handle rather than silently using unpaged list", async () => {
    const unpaged: AsyncMeshStateStore = { ...remote }; delete unpaged.listPage;
    vi.mocked(asyncState.openAsyncMeshStateStore).mockResolvedValueOnce(unpaged);
    await expect(open()).rejects.toThrow(/bounded listPage capability/);
    expect(remote.close).toHaveBeenCalledOnce(); expect(remote.list).not.toHaveBeenCalled();
  });
  it("awaits get before null coalescing and does not fall back to old local shared state", async () => {
    const provider = await open(); await local.put({ key: "shared/a", value: "stale local", identity });
    const gate = deferred<MeshStateEntry | undefined>(); vi.mocked(remote.get).mockReturnValueOnce(gate.promise);
    let settled = false; const pending = provider.invoke("get", { key: "shared/a" }, context).then(value => { settled = true; return value; });
    await Promise.resolve(); expect(settled).toBe(false); gate.resolve(undefined);
    expect(await pending).toBeNull(); expect(remote.get).toHaveBeenCalledWith("shared/a");
    vi.mocked(remote.get).mockResolvedValueOnce(entry("shared/a", "authoritative"));
    expect(await provider.invoke("get", { key: "shared/a" }, context)).toMatchObject({ value: "authoritative" });
  });
  it("awaits list, merges locale order, filters private/remote host keys and clamps after filtering", async () => {
    const provider = await open();
    for (const key of ["local/z", "shared/stale", "topology/hosts/a", "residency/private"]) await local.put({ key, value: key, identity });
    const gate = deferred<NatsKvListPage>(); vi.mocked(remote.listPage).mockReturnValueOnce(gate.promise);
    const pending = provider.invoke("list", {}, context);
    gate.resolve({ entries: [entry("shared/b"), entry("shared/a"), entry("residency/forged"), entry("topology/hosts/forged")], examined: 4 });
    expect((await pending as MeshStateEntry[]).map(e => e.key)).toEqual(["local/z", "shared/a", "shared/b", "topology/hosts/a"]);
    expect(remote.listPage).toHaveBeenCalledWith("shared/", 100);
    expect(remote.list).not.toHaveBeenCalled(); expect(remote.listAll).not.toHaveBeenCalled();
    vi.mocked(remote.listPage).mockResolvedValue({ entries: [entry("shared/b"), entry("shared/a")], examined: 2 });
    expect((await provider.invoke("list", { prefix: "sha", limit: 1 }, context) as MeshStateEntry[]).map(e => e.key)).toEqual(["shared/a"]);
    expect((await provider.invoke("list", { prefix: "shared/b" }, context) as MeshStateEntry[]).map(e => e.key)).toEqual(["shared/b"]);
  });
  it("passes a limit-ten examined page to the remote store and rejects rather than scanning 10,000 entries", async () => {
    const provider = await open();
    const corpus = Array.from({ length: 10_000 }, (_, n) => entry(`shared/item-${String(n).padStart(5, "0")}`));
    vi.mocked(remote.listAll).mockRejectedValue(new Error("unbounded remote scan"));
    vi.mocked(remote.listPage).mockImplementation(async (_prefix, limit = 100) => ({ entries: corpus.slice(0, limit), examined: limit, nextRevision: limit + 1 }));
    for (const prefix of ["shared/", "sha", ""]) {
      await expect(provider.invoke("list", { prefix, limit: 10 }, context)).rejects.toMatchObject({ code: "MESH_LISTING_INCOMPLETE", limit: 10, examined: 10, nextRevision: 11 });
      expect(remote.listPage).toHaveBeenLastCalledWith("shared/", 10);
    }
    expect(remote.list).not.toHaveBeenCalled(); expect(remote.listAll).not.toHaveBeenCalled();
    await expect(provider.invoke("list", { prefix: "shared/", limit: 10_000 }, context)).rejects.toMatchObject({ code: "MESH_LISTING_INCOMPLETE", limit: local.maxReadEvents });
    expect(remote.listPage).toHaveBeenLastCalledWith("shared/", local.maxReadEvents);
  });
  it("does not consult legacy state at all for a wholly shared remote listing", async () => {
    const provider = await open(); vi.mocked(remote.listPage).mockResolvedValueOnce({ entries: [entry("shared/a")], examined: 1 });
    const read = vi.spyOn(local, "listAll").mockImplementationOnce(() => { throw new Error("broken unrelated local state"); });
    expect(await provider.invoke("list", { prefix: "shared/" }, context)).toEqual([entry("shared/a")]);
    expect(read).not.toHaveBeenCalled();
  });
  it.each(["shared/m", "sha", ""])("rejects an incomplete examined page for prefix %j instead of a silent short list", async prefix => {
    const provider = await open();
    await local.put({ key: "local/a", value: "not a complete merged result", identity });
    // The later matching key is outside the first page, occupied by a sibling or tombstone.
    vi.mocked(remote.listPage).mockResolvedValueOnce({ entries: [], examined: 1, nextRevision: 8 });
    const result = provider.invoke("list", { prefix, limit: 1 }, context);
    await expect(result).rejects.toBeInstanceOf(MeshListingIncompleteError);
    await expect(result).rejects.toMatchObject({
      name: "MeshListingIncompleteError", code: "MESH_LISTING_INCOMPLETE", prefix: prefix === "shared/m" ? prefix : "shared/", limit: 1, examined: 1, nextRevision: 8,
    });
    expect(remote.listPage).toHaveBeenCalledWith(prefix === "shared/m" ? prefix : "shared/", 1);
    expect(remote.listPage).toHaveBeenCalledOnce(); expect(remote.list).not.toHaveBeenCalled(); expect(remote.listAll).not.toHaveBeenCalled();
  });
  it("returns exhausted empty and short examined pages as arrays, not incomplete errors", async () => {
    const provider = await open();
    for (const page of [{ entries: [], examined: 1 }, { entries: [entry("shared/match")], examined: 2 }]) {
      vi.mocked(remote.listPage).mockResolvedValueOnce(page);
      expect(await provider.invoke("list", { prefix: "shared/m", limit: 2 }, context)).toEqual(page.entries);
    }
  });
  it("keeps non-shared typed/host state on the original backend and preserves reserved guards", async () => {
    const provider = await open();
    await provider.invoke("put", { key: "state/current", value: "local typed state" }, context);
    expect(local.get("state/current")?.value).toBe("local typed state"); expect(remote.put).not.toHaveBeenCalled();
    expect(await provider.invoke("get", { key: "state/current" }, context)).toMatchObject({ value: "local typed state" });
    expect(remote.get).not.toHaveBeenCalled();
    await expect(provider.invoke("put", { key: "topology/hosts/a", value: 1 }, context)).rejects.toThrow(/reserved/);
    await expect(provider.invoke("delete", { key: "actors/a" }, context)).rejects.toThrow(/reserved/);
    await expect(provider.invoke("get", { key: "residency/a" }, context)).rejects.toThrow(/private/);
    await expect(provider.invoke("list", { prefix: "residency/" }, context)).rejects.toThrow(/private/);
    await provider.invoke("list", { prefix: "topology/" }, context);
    expect(remote.listPage).not.toHaveBeenCalled(); expect(remote.list).not.toHaveBeenCalled(); expect(remote.listAll).not.toHaveBeenCalled();
  });
  it("routes only shared single-key writes with their CAS token and propagates remote failures", async () => {
    const provider = await open();
    expect(await provider.invoke("put", { key: "shared/a", value: 2, ifVersion: 3 }, context)).toMatchObject({ value: 2 });
    expect(remote.put).toHaveBeenCalledWith({ key: "shared/a", value: 2, identity, ifVersion: 3 }); expect(local.get("shared/a")).toBeUndefined();
    await provider.invoke("delete", { key: "shared/a", ifVersion: 7 }, context); expect(remote.delete).toHaveBeenCalledWith({ key: "shared/a", ifVersion: 7 });
    const error = new Error("remote unavailable");
    vi.mocked(remote.get).mockRejectedValueOnce(error); await expect(provider.invoke("get", { key: "shared/a" }, context)).rejects.toBe(error);
    vi.mocked(remote.listPage).mockRejectedValueOnce(error); await expect(provider.invoke("list", {}, context)).rejects.toBe(error);
    vi.mocked(remote.put).mockRejectedValueOnce(error); await expect(provider.invoke("put", { key: "shared/a", value: 3 }, context)).rejects.toBe(error);
    expect(remote.put).toHaveBeenCalledTimes(2); expect(local.get("shared/a")).toBeUndefined();
  });
  it("joins owned shutdown once, refuses use after close, and leaves the local MeshStore open", async () => {
    const provider = await open(); const gate = deferred<void>(); vi.mocked(remote.close).mockReturnValueOnce(gate.promise);
    const first = provider.close(), second = provider.close(); expect(first).toBe(second);
    let finished = false; second.then(() => { finished = true; }); await Promise.resolve(); expect(finished).toBe(false);
    await expect(provider.invoke("get", { key: "shared/a" }, context)).rejects.toThrow(/closed/);
    gate.resolve(); await Promise.all([first, second]); expect(remote.close).toHaveBeenCalledOnce();
    await local.put({ key: "local/still-open", value: true, identity }); expect(local.get("local/still-open")?.value).toBe(true);
  });
});
