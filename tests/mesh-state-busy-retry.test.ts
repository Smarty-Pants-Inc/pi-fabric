import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { withStateFence } from "../src/mesh/commit-outbox.js";
import { isMeshRetryableBusy, isMeshStateBusy, MeshStateBusyError } from "../src/mesh/state-backend.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// #9805's routing refresh no longer acquires a state writer (see routing-refresh-removal.test.ts).
// Actual writes retain the caller's bounded budget: refusal must run no body and change no state;
// an explicit later attempt may commit once. Real private state.db and a second writer connection.

const identity: MeshIdentity = { id: "lead", name: "lead", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const sqliteMesh = async (label: string): Promise<{ root: string; mesh: MeshStore }> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-state-busy-${label}-`));
  roots.push(root);
  const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
  stores.push(mesh);
  await mesh.put({ key: "seed", value: 0, identity });
  return { root, mesh };
};

const holdWriter = (root: string): (() => void) => {
  const holder = openNodeSqlite(path.join(root, "state.db"));
  holder.exec("BEGIN IMMEDIATE");
  return () => { holder.exec("ROLLBACK"); holder.close(); };
};

const settle = <T>(promise: Promise<T>): Promise<{ value?: T; error?: unknown }> =>
  promise.then(value => ({ value }), (error: unknown) => ({ error }));

describe("SQLite state busy budget (smarty-dev#9805)", () => {
  it("an expired state fence never runs its body or changes state", async () => {
    const { root, mesh } = await sqliteMesh("fence");
    const release = holdWriter(root);
    let bodies = 0;
    try {
      const result = await settle(withStateFence(mesh, identity, () => { bodies += 1; }, 2_000));
      expect(result.error).toBeInstanceOf(MeshStateBusyError);
      expect(isMeshRetryableBusy(result.error)).toBe(true);
      expect(bodies).toBe(0);
      expect(mesh.get("seed")?.value).toBe(0);
    } finally { release(); }
    await withStateFence(mesh, identity, () => { bodies += 1; }, 2_000);
    expect(bodies).toBe(1);
  });

  it("a refused directory write changes no version; a later explicit write commits once", async () => {
    const { root, mesh } = await sqliteMesh("directory");
    const before = await mesh.put({ key: "topology/participants/lead", value: { heartbeat: 0 }, identity });
    const release = holdWriter(root);
    let prepared = 0;
    const write = () => mesh.withTryLock(() => mesh.writeBatch({ identity, ops: [], prepare: view => {
      prepared += 1;
      return [{ kind: "put", key: before.key, value: { heartbeat: 1 }, ifVersion: view.version(before.key) }];
    } }), 2_000);
    try {
      const result = await settle(write());
      expect(isMeshStateBusy(result.error)).toBe(true);
      expect(prepared).toBe(0);
      expect(mesh.get(before.key)).toEqual(before);
    } finally { release(); }
    await expect(write()).resolves.toEqual([{ key: before.key, applied: true, version: expect.any(Number) }]);
    expect(prepared).toBe(1);
    expect(mesh.get(before.key)).toMatchObject({ value: { heartbeat: 1 } });
    expect(mesh.get(before.key)!.version).toBeGreaterThan(before.version);
  });

  it.each([0, 50, 1_000, 2_000])("a %s ms try passes only its remaining caller budget and never commits while busy", async timeoutMs => {
    const { root, mesh } = await sqliteMesh("bounded");
    const release = holdWriter(root);
    const acquisition = vi.spyOn(SqliteStateStore.prototype, "withTryLock");
    try {
      const result = await settle(mesh.withTryLock(() => mesh.put({ key: "a", value: 1, identity }), timeoutMs));
      expect(result.error).toBeInstanceOf(MeshStateBusyError);
      expect(acquisition).toHaveBeenCalledOnce();
      expect(acquisition.mock.calls[0]![1]).toBeGreaterThanOrEqual(0);
      expect(acquisition.mock.calls[0]![1]).toBeLessThanOrEqual(timeoutMs);
      expect(mesh.get("a")).toBeUndefined();
    } finally { release(); }
  });

  it("a body error inside the fence is thrown once, never retried", async () => {
    const { mesh } = await sqliteMesh("body-error");
    let bodies = 0;
    const result = await settle(withStateFence(mesh, identity, () => { bodies += 1; throw new Error("not busy"); }, 2_000));
    expect((result.error as Error).message).toBe("not busy");
    expect(bodies).toBe(1);
  });
});
