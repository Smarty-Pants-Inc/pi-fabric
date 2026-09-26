import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { deadHostRecords, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
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
const hostKey = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
const host = (mesh: MeshStore, id: string, expiresAt: number) =>
  mesh.put({ key: hostKey(id), value: { format: 1, id, expiresAt }, identity: writer });
const participant = (mesh: MeshStore, id: string, ownerHostId: string) =>
  mesh.put({ key: `topology/participants/${id}`, value: { format: 1, id, ownerHostId }, identity: writer });
const keys = (records: Array<{ entry: { key: string } }>) => records.map((record) => record.entry.key).sort();

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
      .toEqual([hostKey("dead"), "topology/participants/dead-agent", "topology/participants/dead-root"].sort());
    // Past the window, the orphan without a host record goes too.
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now: now + 7 * HOUR })))
      .toContain("topology/participants/orphan");
  });

  // smarty-dev#816: hosts renew a file lease outside the shared state, and under the fleet
  // owner's policy renew their shared record only every few minutes.
  it("keeps a host whose file lease is fresh, and removes a dead host's file lease with it", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "filed", now - 7 * HOUR);                    // shared lease old ...
    await participant(mesh, "filed-root", "filed");
    writeHostLease(mesh.root, { id: "filed", rootId: "filed", identityId: "filed", updatedAt: now, expiresAt: now + 10_000 });
    await host(mesh, "dead", now - 7 * HOUR);
    writeHostLease(mesh.root, { id: "dead", rootId: "dead", identityId: "dead", updatedAt: now - 8 * HOUR, expiresAt: now - 7 * HOUR });
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now }))).toEqual([hostKey("dead")]);   // ... file lease fresh
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(1);
    expect(mesh.get(hostKey("filed"))).toBeDefined();
    expect([...readHostLeases(mesh.root).keys()]).toEqual(["filed"]);
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
    expect(mesh.get(hostKey("dead"))).toBeUndefined();
    expect(mesh.get("topology/participants/dead-root")).toBeDefined();
    batch.mockClear();
    await mesh.delete({ key: "topology/participants/dead-root" });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(batch).not.toHaveBeenCalled();
  });

  // review/astra on #63: the two scans are separate reads, and per-record version fences did not
  // protect the host's liveness, so a host renewing in between lost its participants.
  it("keeps a host and its participants when the host renews between the host and participant scans", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "back", now - 7 * HOUR);
    await participant(mesh, "back-root", "back");
    // The host's heartbeat (a live lease and its participant) lands after the host scan and before
    // the participant scan, so the participant is read at its new version.
    const listAll = mesh.listAll.bind(mesh);
    let heartbeat: Promise<unknown> | undefined;
    vi.spyOn(mesh, "listAll").mockImplementation((prefix, options) => {
      if (prefix === "topology/participants/" && !heartbeat) {
        heartbeat = mesh.writeBatch({ identity: writer, ops: [
          { kind: "put", key: hostKey("back"), value: { format: 1, id: "back", expiresAt: now + 60_000 } },
          { kind: "put", key: "topology/participants/back-root", value: { format: 1, id: "back-root", ownerHostId: "back" } },
        ] });
      }
      return listAll(prefix, options);
    });
    const writeBatch = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementation(async (input) => {
      if (input.ops.every((op) => op.kind === "delete")) await heartbeat;
      return writeBatch(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(mesh.get(hostKey("back"))).toBeDefined();
    expect(mesh.get("topology/participants/back-root")).toBeDefined();
  });

  it("keeps a host's participants when only its lease renews before the delete commits", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "back", now - 7 * HOUR);
    await participant(mesh, "back-root", "back");                // unchanged: not rewritten by the renewal
    const original = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async (input) => {
      await host(mesh, "back", now + 60_000);                    // a lease-only heartbeat
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(mesh.get(hostKey("back"))).toBeDefined();
    expect(mesh.get("topology/participants/back-root")).toBeDefined();
  });

  it("keeps an orphan participant whose host appears before the delete commits", async () => {
    const mesh = store();
    const now = Date.now();
    await participant(mesh, "orphan", "returning");
    const original = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async (input) => {
      await host(mesh, "returning", now + 7 * HOUR + 60_000);    // the host comes back with a live lease
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now: now + 7 * HOUR })).toBe(0);
    expect(mesh.get("topology/participants/orphan")).toBeDefined();
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
    expect(mesh.get(hostKey("dead"))).toBeDefined();         // disabled: no sweep
    await off.close();
    directories.length = 0;
    const batches = vi.spyOn(mesh, "writeBatch");
    const on = make({ sweepMs: 600 });
    await on.start();
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(mesh.get(hostKey("dead"))).toBeDefined();         // the first sweep waits an interval too
    await vi.waitFor(() => expect(mesh.get(hostKey("dead"))).toBeUndefined(), { timeout: 3_000, interval: 20 });
    expect(mesh.get("topology/participants/dead-root")).toBeUndefined();
    expect(on.list({ scope: "project" }).map((entry) => entry.id)).toEqual([identity.id]);   // its own records stay
    const sweeps = () => batches.mock.calls.filter(([input]) => input.ops.every((op) => op.kind === "delete")).length;
    expect(sweeps()).toBe(1);
    await new Promise((resolve) => setTimeout(resolve, 700));
    expect(sweeps()).toBe(1);                                      // nothing dead since: no more writes
  });
});
