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
    const remove = backend.delete.bind(backend);
    const refused = () => new MeshStateWalCapError(120 * 1024 * 1024, 96 * 1024 * 1024, "pid 4242 (S, 3m, node secret-tool) slot 0", 1);
    backend.put = (input) => state.pinned ? Promise.reject(refused()) : put(input);
    backend.delete = (input) => state.pinned ? Promise.reject(refused()) : remove(input);
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
      data: { walBytes: 120 * 1024 * 1024, capBytes: 96 * 1024 * 1024, readerCount: 1, writer: identity.id } });
    expect(warn.mock.calls.filter(([line]) => String(line).includes("WAL cap"))).toHaveLength(1);
    // smarty-dev#9513: a count, never the pinning reader's pid or command line, in the event or the log line.
    expect(JSON.stringify(alarms()[0])).not.toMatch(/4242|secret-tool/);
    expect(warn.mock.calls.map(([line]) => String(line)).join("\n")).not.toMatch(/4242|secret-tool/);
    // Ten minutes later a refusal that still lasts alarms again; a write the cap admits raises nothing.
    const later = Date.now() + 10 * 60 * 1_000;
    vi.spyOn(Date, "now").mockReturnValue(later);
    await expect(store.put({ key: "a/b", value: 9, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await vi.waitFor(() => expect(alarms()).toHaveLength(2));
    state.pinned = false;
    await store.put({ key: "a/b", value: 10, identity });
    expect(alarms()).toHaveLength(2);
  });

  // smarty-dev#9513: the identity is the refused operation's own, and a write without one still alarms.
  it("names each refused operation's own writer, and alarms a delete that carries no identity", async () => {
    const { store } = cappedStore();
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const other: MeshIdentity = { id: "agent:other", name: "other", kind: "agent" };
    await expect(store.put({ key: "a/b", value: 1, identity: other })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await vi.waitFor(() => expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })).toHaveLength(1));
    expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })[0]).toMatchObject({ from: other, data: { writer: other.id } });
    // Past the dedupe window, a delete (no identity) still alarms, as the store's process, never as `other`.
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 10 * 60 * 1_000);
    await expect(store.delete({ key: "a/b" })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await vi.waitFor(() => expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })).toHaveLength(2));
    expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })[1]).toMatchObject({
      from: { id: `mesh-store:${process.pid}`, kind: "agent" }, data: { writer: null, readerCount: 1 } });
  });

  it("an alarm publish that throws synchronously never replaces the WAL-cap error", async () => {
    const { store } = cappedStore();
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(store, "publish").mockImplementation(() => { throw new Error("event too large"); });
    const refused = store.put({ key: "a/b", value: 1, identity });
    await expect(refused).rejects.toBeInstanceOf(MeshStateWalCapError);
    await expect(refused).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP", walBytes: 120 * 1024 * 1024 });
    expect(store.publish).toHaveBeenCalledOnce();
  });
});
