import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeaseCurrent } from "../src/topology/host-leases.js";

const directories: ParticipantDirectory[] = [], stores: MeshStore[] = [], roots: string[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const store of stores.splice(0)) store.closeState();
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const hostId = "7313-host";
const hostKey = "topology/hosts/" + createHash("sha256").update(hostId).digest("hex");
const setup = async (stateBackend: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "lease-renew-fence-")); roots.push(root);
  let now = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => now);
  const make = async (identityId = "7313-owner", clockStep = 10) => {
    now += clockStep;
    const mesh = new MeshStore(root, 65_536, 100, { stateBackend, readCacheMs: 60_000 }); stores.push(mesh);
    expect(mesh.stateBackend).toBe(stateBackend);
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId, rootId: "7313-root",
      identity: { id: identityId, name: identityId, kind: "agent" }, reapDeadHosts: false,
      heartbeatMs: 100, waitForPublicationRetry: async () => {} });
    directories.push(directory); await directory.start();
    return { directory, mesh };
  };
  const old = await make();
  const read = () => readHostLeaseCurrent(root, hostId)!;
  const predecessor = read();
  return { root, old, make, read, predecessor, advance: (ms: number) => { now += ms; } };
};

describe.each(["file", "sqlite"] as const)("%s host-lease owner fence (smarty-dev#7313)", stateBackend => {
  it.each(["7313-owner", "7313-other"])("rejects a superseded explicit heartbeat (successor identity %s)", async identity => {
    const s = await setup(stateBackend);
    const successor = await s.make(identity);
    const lease = s.read(), host = successor.mesh.get(hostKey, { fresh: true });
    expect(lease.startedAt).not.toBe(s.predecessor.startedAt);
    const prior = s.old.directory.confirmedAt(); s.advance(100);
    const error = await s.old.directory.refresh().then(() => undefined, (error: unknown) => error);
    expect(s.read(), "superseded instance overwrote successor lease").toEqual(lease);
    expect(successor.mesh.get(hostKey, { fresh: true })).toEqual(host);
    expect(error).toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED", retryable: false });
    expect(s.old.directory.confirmedAt()).toBe(prior);
    expect(s.old.directory.canConsumeMesh()).toBe(false);
  });

  it.each([0, -100])("fences same-identity incarnations despite a repeated/backwards clock (%s ms)", async clockStep => {
    const s = await setup(stateBackend);
    const successor = await s.make("7313-owner", clockStep);
    const lease = s.read(), host = successor.mesh.get(hostKey, { fresh: true });
    expect(lease.startedAt).toBe(s.predecessor.startedAt! + clockStep);
    expect(lease.incarnationToken).not.toBe(s.predecessor.incarnationToken);
    expect(lease.incarnationToken).toMatch(/^[0-9a-f-]{36}$/);
    const prior = s.old.directory.confirmedAt();
    await expect(s.old.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
    expect(s.old.directory.confirmedAt()).toBe(prior);
    expect(s.old.directory.canConsumeMesh()).toBe(false);
    await s.old.directory.close();
    expect(s.read()).toEqual(lease); expect(successor.mesh.get(hostKey, { fresh: true })).toEqual(host);
  });

  it("fences an incarnation constructed before a successor but not yet started", async () => {
    const s = await setup(stateBackend);
    const stale = new ParticipantDirectory(s.old.mesh, { ...s.old.directory.options });
    directories.push(stale);
    const successor = await s.make("7313-owner", 0);
    const lease = s.read(), host = successor.mesh.get(hostKey, { fresh: true });
    await expect(stale.start()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
    expect(stale.canConsumeMesh()).toBe(false);
    expect(s.read()).toEqual(lease); expect(successor.mesh.get(hostKey, { fresh: true })).toEqual(host);
  });
  it("does not renew from a superseded timer, and close keeps the successor lease", async () => {
    const s = await setup(stateBackend);
    const successor = await s.make(); const lease = s.read();
    const prior = s.old.directory.confirmedAt();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    // Only the predecessor's real timer runs; the successor's lifecycle token is retired.
    successor.directory.options.live = () => false;
    s.advance(100); await new Promise(resolve => setTimeout(resolve, 160));
    expect(s.read(), "superseded timer overwrote successor lease").toEqual(lease);
    expect(s.old.directory.confirmedAt()).toBe(prior);
    expect(s.old.directory.canConsumeMesh()).toBe(false);
    await s.old.directory.close();
    expect(s.read(), "predecessor close removed successor lease").toEqual(lease);
    expect(successor.mesh.get(hostKey, { fresh: true })?.value).toMatchObject({ startedAt: lease.startedAt });
  });

  it("close before the next heartbeat preserves the successor", async () => {
    const s = await setup(stateBackend); const successor = await s.make();
    const lease = s.read(), host = successor.mesh.get(hostKey, { fresh: true });
    await s.old.directory.close();
    expect(s.read()).toEqual(lease);
    expect(successor.mesh.get(hostKey, { fresh: true })).toEqual(host);
    await expect(s.old.directory.start()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
  });

  it("cancels the queued publication retry when it discovers a successor", async () => {
    const s = await setup(stateBackend); const prior = s.old.directory.confirmedAt();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] }); vi.spyOn(Math, "random").mockReturnValue(0);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const admission = vi.spyOn(s.old.directory.options, "waitForPublicationRetry");
    const confirm = vi.spyOn(s.old.mesh, "confirmWritable").mockRejectedValue(new MeshLockTimeoutError(" initial outage", 1, 0));
    await expect(s.old.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    const successor = await s.make(); successor.directory.options.live = () => false;
    const lease = s.read(); await vi.advanceTimersByTimeAsync(50);
    expect(admission).toHaveBeenCalledOnce(); expect(confirm).toHaveBeenCalledOnce();
    expect(s.read()).toEqual(lease); expect(s.old.directory.confirmedAt()).toBe(prior);
    await expect(s.old.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
    await vi.advanceTimersByTimeAsync(5_000);
    expect(admission).toHaveBeenCalledOnce(); expect(s.read()).toEqual(lease);
    expect(s.old.directory.canConsumeMesh()).toBe(false);
  });

  it.each([false, true])("fences a delayed confirmation (timeout: %s) after the successor claims", async fail => {
    const s = await setup(stateBackend);
    const prior = s.old.directory.confirmedAt();
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    const confirm = vi.spyOn(s.old.mesh, "confirmWritable").mockImplementation(async callback => {
      entered(); await gate;
      if (fail) throw new MeshLockTimeoutError(" delayed confirmation", 1, 0);
      callback?.(Date.now());
    });
    const pending = s.old.directory.refresh().then(() => undefined, (error: unknown) => error);
    try {
      await waiting;
      const successor = await s.make(); const lease = s.read(), host = successor.mesh.get(hostKey, { fresh: true });
      s.advance(100); release();
      expect(await pending).toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED", retryable: false });
      expect(s.read()).toEqual(lease);
      expect(successor.mesh.get(hostKey, { fresh: true })).toEqual(host);
      expect(s.old.directory.confirmedAt()).toBe(prior);
      expect(s.old.directory.canConsumeMesh()).toBe(false);
      await expect(s.old.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
      expect(confirm).toHaveBeenCalledOnce();
    } finally { release(); await pending; }
  });

  it("retry-side renewal cannot confirm or overwrite a successor after a mesh timeout", async () => {
    const s = await setup(stateBackend);
    const successor = await s.make(); const lease = s.read();
    const prior = s.old.directory.confirmedAt();
    vi.spyOn(s.old.mesh, "writeBatch").mockRejectedValue(new MeshLockTimeoutError(" test", 1, 0));
    s.advance(100);
    const error = await s.old.directory.refresh().then(() => undefined, (error: unknown) => error);
    expect(s.read(), "retry-side renewal overwrote successor lease").toEqual(lease);
    expect(error).toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
    expect(s.old.directory.confirmedAt()).toBe(prior);
    expect(s.old.directory.canConsumeMesh()).toBe(false);
  });
});
