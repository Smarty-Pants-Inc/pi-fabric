import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { deadHostRecords, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const HOUR = 60 * 60 * 1000;
const writer: MeshIdentity = { id: "session:writer", name: "main", kind: "main", sessionId: "writer" };
const store = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-host-reaper-"));
  roots.push(root);
  return new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
};
const host = (mesh: MeshStore, id: string, expiresAt: number) =>
  mesh.put({ key: `topology/hosts/${id}`, value: { format: 1, id, expiresAt }, identity: writer });
const participant = (mesh: MeshStore, id: string, ownerHostId: string) =>
  mesh.put({ key: `topology/participants/${id}`, value: { format: 1, id, ownerHostId }, identity: writer });
const keys = (entries: Array<{ key: string }>) => entries.map((entry) => entry.key).sort();

// smarty-dev#367: 160 of 199 host records in the fleet's shared state belonged to dead hosts,
// with their participants; a host removes its own records only on a clean shutdown.
describe("records of dead hosts", () => {
  it("selects hosts expired longer than the window, their participants, and orphans past the window", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "dead", now - 7 * HOUR);
    await participant(mesh, "dead-root", "dead");
    await participant(mesh, "dead-agent", "dead");
    await host(mesh, "recent", now - HOUR);                       // expired, but within the window
    await participant(mesh, "recent-root", "recent");
    await host(mesh, "live", now + 10_000);
    await participant(mesh, "live-root", "live");
    await host(mesh, "own", now - 7 * HOUR);                      // the caller itself: never
    await participant(mesh, "own-root", "own");
    await participant(mesh, "orphan", "vanished");                // no host record, written just now
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now })))
      .toEqual(["topology/hosts/dead", "topology/participants/dead-agent", "topology/participants/dead-root"]);
    // Past the window, the orphan without a host record goes too.
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now: now + 7 * HOUR })))
      .toContain("topology/participants/orphan");
  });

  it("deletes in one batch fenced to the versions it saw, and writes nothing when none are dead", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "dead", now - 7 * HOUR);
    await participant(mesh, "dead-root", "dead");
    const original = mesh.writeBatch.bind(mesh);
    const batch = vi.spyOn(mesh, "writeBatch");
    batch.mockImplementationOnce(async (input) => {
      await participant(mesh, "dead-root", "dead");              // rewritten meanwhile: kept
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(1);
    expect(mesh.get("topology/hosts/dead")).toBeUndefined();
    expect(mesh.get("topology/participants/dead-root")).toBeDefined();
    batch.mockClear();
    await mesh.delete({ key: "topology/participants/dead-root" });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(batch).not.toHaveBeenCalled();
  });

  it("is swept by a directory after its heartbeat, at most once per sweep interval, never its own records", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "dead", now - 7 * HOUR);
    await participant(mesh, "dead-root", "dead");
    const identity: MeshIdentity = { id: "session:live", name: "main", kind: "main", sessionId: "live" };
    const make = (reapDeadHosts: false | { sweepMs: number }) => {
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 300, reapDeadHosts,
      });
      directory.registerSource(() => [directory.root({
        id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
        updatedAt: 1, pendingMessages: false, local: true,
      })]);
      directories.push(directory);
      return directory;
    };
    const off = make(false);
    await off.start();
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(mesh.get("topology/hosts/dead")).toBeDefined();         // disabled: no sweep
    await off.close();
    directories.length = 0;
    const batches = vi.spyOn(mesh, "writeBatch");
    const on = make({ sweepMs: 600 });
    await on.start();
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(mesh.get("topology/hosts/dead")).toBeDefined();         // the first sweep waits an interval too
    await vi.waitFor(() => expect(mesh.get("topology/hosts/dead")).toBeUndefined(), { timeout: 3_000, interval: 20 });
    expect(mesh.get("topology/participants/dead-root")).toBeUndefined();
    expect(on.list({ scope: "project" }).map((entry) => entry.id)).toEqual([identity.id]);   // its own records stay
    const sweeps = () => batches.mock.calls.filter(([input]) => input.ops.every((op) => op.kind === "delete")).length;
    expect(sweeps()).toBe(1);
    await new Promise((resolve) => setTimeout(resolve, 700));
    expect(sweeps()).toBe(1);                                      // nothing dead since: no more writes
  });
});
