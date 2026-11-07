// Keep: a mesh-lock outage previously expired healthy hosts. Fake time makes a >15 s
// publication wait deterministic while exercising real lease CAS, custody and file I/O.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const directories: ParticipantDirectory[] = [], stores: MeshStore[] = [], roots: string[] = [];
afterEach(async () => {
  for (const directory of directories.splice(0)) await directory.close();
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const setup = async (stateBackend: "file" | "sqlite", resident: boolean) => {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  vi.spyOn(performance, "now").mockImplementation(() => Date.now());
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "lease-renew-cadence-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend }); stores.push(mesh);
  expect(mesh.stateBackend).toBe(stateBackend);
  const identity = { id: "session:cadence", name: "main", kind: "main" as const, sessionId: "cadence" };
  let live = true;
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
    heartbeatMs: 5_000, leaseMs: 15_000, random: () => 0.5, reapDeadHosts: false, live: () => live,
    ...(resident ? { withPublicationFence: <T>(operation: () => Promise<T>) => operation(), actorRenewalAllowed: () => true } : {}),
  });
  directories.push(directory);
  const record: FabricParticipantRecord = { format: 1, id: identity.id, rootId: identity.id, kind: "root",
    name: "main", status: "idle", ownerHostId: identity.id, ownerIdentityId: identity.id,
    runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1", sessionId: "cadence",
    startedAt: Date.now(), updatedAt: Date.now() };
  directory.registerSource(() => [record]);
  await directory.start();
  const read = () => readHostLeaseCurrent(root, identity.id)!;
  return { root, mesh, directory, record, read, retire: () => { live = false; },
    hostKey: key("topology/hosts/", identity.id), participantKey: key("topology/participants/", identity.id) };
};

const blockPublication = (s: Awaited<ReturnType<typeof setup>>, kind: "writeBatch" | "confirmWritable") => {
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const waiting = new Promise<void>(resolve => { entered = resolve; });
  const write = s.mesh.writeBatch.bind(s.mesh), confirm = s.mesh.confirmWritable.bind(s.mesh);
  const calls = kind === "writeBatch"
    ? vi.spyOn(s.mesh, "writeBatch").mockImplementation(async (...args) => { entered(); await gate; return write(...args); })
    : vi.spyOn(s.mesh, "confirmWritable").mockImplementation(async (...args) => { entered(); await gate; return confirm(...args); });
  if (kind === "writeBatch") s.record.status = "running";
  const pending = s.directory.refresh();
  pending.catch(() => undefined);
  return { release, waiting, calls, pending };
};

describe.each(["file", "sqlite"] as const)("%s independent host-lease cadence", stateBackend => {
  it.each([false, true])("renews every heartbeat through a >15 s writeBatch wait (resident: %s)", async resident => {
    const s = await setup(stateBackend, resident);
    const confirmed = s.directory.confirmedAt(), initial = s.read();
    const host = s.mesh.get(s.hostKey, { fresh: true }), participant = s.mesh.get(s.participantKey, { fresh: true });
    const b = blockPublication(s, "writeBatch");
    try {
      await b.waiting;
      // Publication retains physical custody: neither a successor claim nor another
      // gate user may enter while the healthy owner renews its existing UUID.
      await expect(renewHostLease(s.mesh, { ...initial, incarnationToken: randomUUID() }, { claim: true }))
        .rejects.toMatchObject({ busyCode: "FABRIC_HOST_LEASE_LOCK_BUSY" });
      for (let tick = 1; tick <= 4; tick++) {
        await vi.advanceTimersByTimeAsync(5_000);
        expect(s.read()).toMatchObject({ incarnationToken: initial.incarnationToken,
          updatedAt: initial.updatedAt + tick * 5_000, expiresAt: Date.now() + 15_000,
          session: { updatedAt: Date.now(), expiresAt: Date.now() + 15_000 } });
        expect(s.directory.sessions().some(session => session.id === s.record.id)).toBe(true);
        expect(s.directory.confirmedAt()).toBe(confirmed);
        expect(s.mesh.get(s.hostKey, { fresh: true })).toEqual(host);
        expect(s.mesh.get(s.participantKey, { fresh: true })).toEqual(participant);
        expect(b.calls).toHaveBeenCalledOnce(); // publication is coalesced, not queued per heartbeat
      }
      expect(s.directory.canConsumeMesh()).toBe(false); // renewal is not shared admission
      b.release(); await b.pending;
      expect(s.mesh.get(s.participantKey, { fresh: true })?.value).toMatchObject({ status: "running" });
      expect(s.directory.confirmedAt()).toBe(Date.now());
      expect(s.directory.canConsumeMesh()).toBe(true);
    } finally { b.release(); await b.pending.catch(() => undefined); }
  });

  it("renews independently while unchanged publication waits for confirmation", async () => {
    const s = await setup(stateBackend, false), initial = s.read(), confirmed = s.directory.confirmedAt();
    const b = blockPublication(s, "confirmWritable");
    try {
      await b.waiting;
      for (let tick = 1; tick <= 4; tick++) {
        await vi.advanceTimersByTimeAsync(5_000);
        expect(s.read().updatedAt).toBe(initial.updatedAt + tick * 5_000);
        expect(s.directory.sessions().some(session => session.id === s.record.id)).toBe(true);
        expect(s.directory.confirmedAt()).toBe(confirmed);
        expect(b.calls).toHaveBeenCalledOnce();
      }
      b.release(); await b.pending;
      expect(s.directory.confirmedAt()).toBe(Date.now());
    } finally { b.release(); await b.pending.catch(() => undefined); }
  });

  it.each(["retired", "suspended", "stalled"] as const)("a %s renewer still expires during blocked publication", async stop => {
    const s = await setup(stateBackend, true), initial = s.read();
    const b = blockPublication(s, "writeBatch");
    try {
      await b.waiting;
      if (stop === "retired") s.retire();
      if (stop === "suspended") s.directory.suspend();
      if (stop === "stalled") vi.setSystemTime(Date.now() + 15_001); // event loop does not run its timers
      else await vi.advanceTimersByTimeAsync(15_001);
      expect(s.read()).toEqual(initial);
      expect(s.read().expiresAt).toBeLessThan(Date.now());
      expect(s.directory.sessions().some(session => session.id === s.record.id)).toBe(false);
    } finally { b.release(); await b.pending.catch(() => undefined); }
  });

  it("never recreates a missing lease even under its own publication custody", async () => {
    const s = await setup(stateBackend, true), confirmed = s.directory.confirmedAt();
    const b = blockPublication(s, "writeBatch");
    try {
      await b.waiting;
      fs.rmSync(hostLeasePath(s.root, s.record.id));
      await vi.advanceTimersByTimeAsync(5_000);
      expect(s.read()).toBeUndefined();
      expect(s.directory.canConsumeMesh()).toBe(false);
      expect(s.directory.heartbeatArmed).toBe(false);
      expect(s.directory.confirmedAt()).toBe(confirmed);
      b.release();
      await expect(b.pending).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
      expect(s.read()).toBeUndefined();
    } finally { b.release(); await b.pending.catch(() => undefined); }
  });
});
