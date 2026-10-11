import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { STATE_BACKEND_FACTORIES } from "../src/mesh/state-backend.js";
import { StateFile } from "../src/mesh/state-file.js";
import { MeshStateWalCapError } from "../src/mesh/state-sqlite.js";
import { MESH_WAL_CAP_ALARM_TOPIC, MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#7451 item 6: a write the SQLite WAL cap refuses raises an in-product alarm, not only a bridge log line.
const roots: string[] = [];
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:wal-cap", name: "main", kind: "main" };

// A store whose state writes the cap refuses while `pinned` is true (a reader pins the WAL above the cap).
const cappedStore = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wal-cap-alarm-"));
  roots.push(root);
  const state = { pinned: true };
  const create = STATE_BACKEND_FACTORIES.file.create;
  vi.spyOn(STATE_BACKEND_FACTORIES.file, "create").mockImplementation((context, options) => {
    const backend = create(context, options) as StateFile;
    const put = backend.put.bind(backend);
    backend.put = (input) => state.pinned
      ? Promise.reject(new MeshStateWalCapError(120 * 1024 * 1024, 96 * 1024 * 1024, "reader pids 4242")) : put(input);
    return backend;
  });
  return { store: new MeshStore(root, 64 * 1024, 100), state };
};

describe("mesh WAL-cap alarm (smarty-dev#7451 item 6)", () => {
  it("a refused write raises one log line and one fabric.alarm.mesh-wal-cap event per 10 min, and still rejects", async () => {
    const { store, state } = cappedStore();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const alarms = () => store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC });
    for (let i = 0; i < 5; i++) {
      await expect(store.put({ key: "a/b", value: i, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    }
    await vi.waitFor(() => expect(alarms()).toHaveLength(1));
    expect(alarms()[0]).toMatchObject({ topic: MESH_WAL_CAP_ALARM_TOPIC, kind: "mesh-wal-cap", from: identity,
      data: { walBytes: 120 * 1024 * 1024, capBytes: 96 * 1024 * 1024, readers: "reader pids 4242" } });
    expect(warn.mock.calls.filter(([line]) => String(line).includes("WAL cap"))).toHaveLength(1);
    // Ten minutes later a refusal that still lasts alarms again; a write the cap admits raises nothing.
    const later = Date.now() + 10 * 60 * 1_000;
    vi.spyOn(Date, "now").mockReturnValue(later);
    await expect(store.put({ key: "a/b", value: 9, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await vi.waitFor(() => expect(alarms()).toHaveLength(2));
    state.pinned = false;
    await store.put({ key: "a/b", value: 10, identity });
    expect(alarms()).toHaveLength(2);
  });
});
