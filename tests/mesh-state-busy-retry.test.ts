import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { withStateFence } from "../src/mesh/commit-outbox.js";
import { isMeshRetryableBusy, isMeshStateBusy, MeshStateBusyError } from "../src/mesh/state-backend.js";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ROUTING_REFRESH_LOCK_BUDGET_MS } from "../src/topology/participant-directory.js";

// smarty-dev#9805: a caller-visible state write (the routing refresh before a send, a bounded directory write) must
// not fail at the first 2 s of a busy state.db writer. It waits up to ~6 s inside the same call, runs its body once,
// and only then fails with the typed FABRIC_MESH_STATE_BUSY. Real temp state.db; a second connection holds the lock.

const identity: MeshIdentity = { id: "lead", name: "lead", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];

afterEach(() => {
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

// A second connection holds the state.db write lock for `ms`, as a long writer in another process does.
const holdWriter = (root: string, ms: number): Promise<void> => {
  const holder = openNodeSqlite(path.join(root, "state.db"));
  holder.exec("BEGIN IMMEDIATE");
  return new Promise((resolve) => setTimeout(() => {
    holder.exec("ROLLBACK");
    holder.close();
    resolve();
  }, ms));
};

const settle = <T>(promise: Promise<T>): Promise<{ value?: T; error?: unknown; ms: number }> => {
  const started = performance.now();
  return promise.then((value) => ({ value, ms: performance.now() - started }),
    (error: unknown) => ({ error, ms: performance.now() - started }));
};

describe("SQLite state busy budget (smarty-dev#9805)", () => {
  it("a send's routing refresh and its publish succeed after a 2.5 s writer releases", async () => {
    const { root, mesh } = await sqliteMesh("refresh");
    const held = holdWriter(root, 2_500);
    let bodies = 0;
    // The exact fence ParticipantDirectory.refreshRoutingView runs before agents.followUp/steer route.
    const refresh = await settle(withStateFence(mesh, identity, () => { bodies += 1; return "fresh"; }, ROUTING_REFRESH_LOCK_BUDGET_MS));
    await held;
    expect(refresh.error).toBeUndefined();
    expect(refresh.value).toBe("fresh");
    expect(bodies).toBe(1);
    expect(refresh.ms).toBeGreaterThanOrEqual(2_400);
    const event = await mesh.publish({ topic: "fleet.work.test", text: "hello", from: identity });
    expect(mesh.read({ after: event.sequence - 1 }).map((read) => read.text)).toEqual(["hello"]);
  }, 20_000);

  it("a bounded directory write commits exactly once after a 2.5 s writer releases", async () => {
    const { root, mesh } = await sqliteMesh("directory");
    const before = await mesh.put({ key: "topology/participants/lead", value: { heartbeat: 0 }, identity });
    const held = holdWriter(root, 2_500);
    let prepared = 0;
    const write = await settle(mesh.withTryLock(() => mesh.writeBatch({
      identity, ops: [], prepare: (view) => {
        prepared += 1;
        return [{ kind: "put", key: "topology/participants/lead", value: { heartbeat: 1 }, ifVersion: view.version("topology/participants/lead") }];
      },
    }), 2_000));
    await held;
    expect(write.error).toBeUndefined();
    expect(prepared).toBe(1);
    expect(write.value).toEqual([{ key: "topology/participants/lead", applied: true, version: expect.any(Number) }]);
    const after = mesh.get("topology/participants/lead");
    expect(after).toMatchObject({ value: { heartbeat: 1 } });
    expect(after!.version).toBeGreaterThan(before.version);
  }, 20_000);

  it("a 10 s writer still fails the send with the typed STATE_BUSY after the bounded wait", async () => {
    const { root, mesh } = await sqliteMesh("bounded");
    const held = holdWriter(root, 10_000);
    let bodies = 0;
    const refresh = await settle(withStateFence(mesh, identity, () => { bodies += 1; }, ROUTING_REFRESH_LOCK_BUDGET_MS));
    expect(refresh.error).toBeInstanceOf(MeshStateBusyError);
    expect(isMeshStateBusy(refresh.error)).toBe(true);
    expect(isMeshRetryableBusy(refresh.error)).toBe(true);
    expect(bodies).toBe(0);
    expect(refresh.ms).toBeGreaterThanOrEqual(5_500);
    // Before the 10 s holder lets go (slack for a loaded runner).
    expect(refresh.ms).toBeLessThan(9_500);
    await held;
  }, 30_000);

  it("a short registry-fenced try still fails fast while the writer holds", async () => {
    const { root, mesh } = await sqliteMesh("short");
    const held = holdWriter(root, 3_000);
    const write = await settle(mesh.withTryLock(() => mesh.put({ key: "a", value: 1, identity }), 50));
    expect(isMeshStateBusy(write.error)).toBe(true);
    // Well before the holder lets go (slack for a loaded runner).
    expect(write.ms).toBeLessThan(2_500);
    await held;
  }, 20_000);

  it("a body error inside the fence is thrown once, never retried", async () => {
    const { mesh } = await sqliteMesh("body-error");
    let bodies = 0;
    const result = await settle(withStateFence(mesh, identity, () => { bodies += 1; throw new Error("not busy"); }, ROUTING_REFRESH_LOCK_BUDGET_MS));
    expect((result.error as Error).message).toBe("not busy");
    expect(bodies).toBe(1);
  });
});
