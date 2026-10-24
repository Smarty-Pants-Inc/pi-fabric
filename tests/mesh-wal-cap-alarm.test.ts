import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { STATE_BACKEND_FACTORIES } from "../src/mesh/state-backend.js";
import { StateFile } from "../src/mesh/state-file.js";
import { MeshStateWalCapError } from "../src/mesh/state-sqlite.js";
import { MESH_WAL_CAP_ALARM_TOPIC, MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { parse9729 } from "./helpers/alarm-contract.js";

// smarty-dev#7451 item 6: a write the SQLite WAL cap refuses raises an in-product alarm, not only a bridge log line.
const roots: string[] = [];
// pi-fabric#893 round 3: an alarm needs an explicit recipient (here a configured one).
const CHIEF = "session:0a1b2c3d-0000-4000-8000-00000000c0f5";
beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", CHIEF); });
afterEach(() => {
  vi.unstubAllEnvs();
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:wal-cap", name: "main", kind: "main" };

// A store whose state writes the cap refuses while `pinned` is true (a reader pins the WAL above the cap).
const cappedStore = (refused: () => Error = () => new MeshStateWalCapError(120 * 1024 * 1024, 96 * 1024 * 1024,
  "pid 4242 (S, 3m, node secret-tool) slot 0", 1)) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wal-cap-alarm-"));
  roots.push(root);
  const state = { pinned: true };
  const create = STATE_BACKEND_FACTORIES.file.create;
  vi.spyOn(STATE_BACKEND_FACTORIES.file, "create").mockImplementation((context, options) => {
    const backend = create(context, options) as StateFile;
    const put = backend.put.bind(backend);
    const remove = backend.delete.bind(backend);
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
    // The WAL-cap journal line is in smarty-dev#9729's contract.
    const line = warn.mock.calls.map(([entry]) => String(entry)).find((entry) => entry.includes("WAL cap"))!;
    expect(parse9729(line)).toMatchObject({ label: "mesh state writes", state: "outage", readerCount: 1 });
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

  it("reports unavailable sizes for a bridge-reconstructed cap error without logging undefined", async () => {
    const { store } = cappedStore(() => Object.assign(new Error("bridge-reconstructed WAL refusal"), {
      code: "FABRIC_MESH_STATE_WAL_CAP",
    }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await expect(store.put({ key: "a/b", value: 1, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await vi.waitFor(() => expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })).toHaveLength(1));
    const alarm = store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })[0]!;
    const text = `${alarm.text}\n${warn.mock.calls.map(([line]) => String(line)).join("\n")}`;
    expect(text).toContain("sizes not available");
    expect(text).not.toContain("undefined");
    expect(text).not.toContain("secret-tool");
  });

  it("does not raise or log a WAL-cap alarm for a non-cap state write error", async () => {
    const failure = Object.assign(new Error("SQLite returned SQLITE_BUSY"), {
      code: "SQLITE_BUSY", walBytes: 120 * 1024 * 1024, capBytes: 96 * 1024 * 1024,
      readerCount: 1, readers: "pid 4242 (S, 3m, node secret-tool) slot 0",
    });
    const { store } = cappedStore(() => failure);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await expect(store.put({ key: "a/b", value: 1, identity })).rejects.toBe(failure);
    expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })).toHaveLength(0);
    expect(warn.mock.calls.map(([line]) => String(line)).join("\n")).not.toMatch(/mesh-wal-cap|WAL cap/);
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

  it("addresses the alarm to its recipient, and publishes nothing without one (the log line only)", async () => {
    const { store } = cappedStore();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await expect(store.put({ key: "a/b", value: 1, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await vi.waitFor(() => expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })).toHaveLength(1));
    expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })[0]!.to).toBe(CHIEF);
    // pi-fabric#893 round 4: the log line names no mesh root path.
    expect(warn.mock.calls.map(([line]) => String(line)).join("\n")).not.toContain(store.root);
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", "");
    vi.stubEnv("SMARTY_LEAD_SESSION", "");
    vi.spyOn(process, "cwd").mockReturnValue(os.tmpdir()); // no .local/lead above it
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 10 * 60 * 1_000);
    await expect(store.put({ key: "a/b", value: 2, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_WAL_CAP" });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(store.read({ topic: MESH_WAL_CAP_ALARM_TOPIC })).toHaveLength(1);
    expect(warn.mock.calls.filter(([line]) => String(line).includes("WAL cap"))).toHaveLength(2);
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
