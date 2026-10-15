import { expect } from "vitest";
import type { MeshSyncNotDurableCode } from "../../src/mesh/not-durable.js";

// pi-fabric#894: EventLog.sync() fails closed on win32 (no directory fsync barrier for events.jsonl's
// renames or new archive segments), and MeshStore.sync() and host mesh.sync run that event barrier
// before the state barrier. So on win32 every mesh sync refuses with FABRIC_MESH_EVENTS_NOT_DURABLE,
// whatever the state backend; elsewhere the barrier succeeds unless the state backend itself refuses.
// This module is the one place tests encode that platform contract.

/** True where mesh sync has no namespace barrier and so always refuses (win32). */
export const meshSyncRefusedOnThisPlatform = (): boolean => process.platform === "win32";

/** The code a mesh sync refusal carries on this platform: the event barrier's on win32, else the backend's. */
export const meshSyncRefusalCode = (backendCode: MeshSyncNotDurableCode): MeshSyncNotDurableCode =>
  meshSyncRefusedOnThisPlatform() ? "FABRIC_MESH_EVENTS_NOT_DURABLE" : backendCode;

/**
 * A success-path mesh sync (MeshStore.sync, provider sync, host mesh.sync): resolves to `durable` where the
 * platform has a namespace barrier; on win32 it must instead reject with the typed, non-retryable
 * not-durable refusal, so a caller never advances its cursor there.
 */
export const expectSyncDurableOrRefused = async (sync: Promise<unknown>, durable: unknown): Promise<void> => {
  if (meshSyncRefusedOnThisPlatform()) {
    await expect(sync).rejects.toMatchObject({
      code: "FABRIC_MESH_EVENTS_NOT_DURABLE", retryable: false, message: expect.stringContaining("not durable") });
  } else {
    await expect(sync).resolves.toEqual(durable);
  }
};
