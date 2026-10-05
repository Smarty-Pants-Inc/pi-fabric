import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, MeshLockTimeoutError, type MeshBatchView, type MeshStateEntry } from "../src/mesh/store.js";
import { compactExpiredHostRecords, HOST_RECORD_COMPACTION_BATCH, HOST_RECORD_RETENTION_MS } from "../src/topology/host-record-compaction.js";
import { writeHostLease, readHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
const roots: string[] = [], directories: ParticipantDirectory[] = [];
const root = () => { const r = fs.mkdtempSync(path.join(os.tmpdir(), "host-record-retention-")); roots.push(r); return r; };
const identity = { id: "session:observer", name: "observer", kind: "main" as const };
const key = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
const entry = (id: string, now: number, expiresAt = now - HOST_RECORD_RETENTION_MS - 1): MeshStateEntry => ({
  key: key(id), version: 1, updatedAt: expiresAt - 1000, updatedBy: identity,
  value: { format: 1, id, rootId: id.startsWith("resident:") ? "session:parent" : id, identity,
    startedAt: 1, updatedAt: expiresAt - 1000, expiresAt },
});
const view = (entries: MeshStateEntry[]): MeshBatchView => ({ listAll: prefix => entries.filter(e => e.key.startsWith(prefix)),
  get: k => entries.find(e => e.key === k), version: () => 1 });
afterEach(async () => { vi.restoreAllMocks(); vi.useRealTimers(); for (const d of directories.splice(0)) await d.close(); for (const r of roots.splice(0)) fs.rmSync(r, { recursive: true, force: true }); });
describe("native/resident topology host compaction", () => {
  it("removes only host records older than six hours, in bounded batches", () => {
    const r = root(), now = Date.now();
    const entries = Array.from({ length: HOST_RECORD_COMPACTION_BATCH + 9 }, (_, n) => entry(n % 2 ? `resident:${n}` : `session:${n}`, now));
    const protectedRows = [entry(identity.id, now), entry("session:boundary", now, now - HOST_RECORD_RETENTION_MS),
      { ...entry("session:invalid", now), value: { format: 1, expiresAt: 0 } },
      { ...entry("session:delivery", now), key: "residency/deliveries/kept" },
      { ...entry("session:participant", now), key: "topology/participants/kept" }];
    const ops = compactExpiredHostRecords(view([...protectedRows, ...entries]), r, identity.id, now);
    expect(ops).toHaveLength(HOST_RECORD_COMPACTION_BATCH);
    expect(ops.every(op => op.key.startsWith("topology/hosts/") && !protectedRows.some(e => e.key === op.key))).toBe(true);
  });
  it("retains matching renewed file leases, rejects unrelated incarnations, and fences late renewals", () => {
    const r = root(), now = Date.now(), stored = entry("resident:broker", now);
    const lease = { id: "resident:broker", rootId: "session:parent", identityId: identity.id, startedAt: 1, updatedAt: now, expiresAt: now + 15000 };
    writeHostLease(r, { ...lease, startedAt: 2 });
    const ops = compactExpiredHostRecords(view([stored]), r, identity.id, now); expect(ops).toHaveLength(1);
    writeHostLease(r, lease);
    expect(ops[0]?.kind === "delete" && ops[0].condition?.(() => stored)).toBe(false);
    expect(compactExpiredHostRecords(view([stored]), r, identity.id, now)).toEqual([]);
  });
  it("joins one existing verified write; does not remove participants, deliveries, or recent returning hosts", async () => {
    const r = root(), store = new MeshStore(r, 256 * 1024, 1000), now = Date.now();
    const dead = entry("resident:old", now), recent = entry("session:recent", now, now + 15000);
    // Seed historical commit times directly; a newly written record must be retained.
    fs.writeFileSync(path.join(r, "state.json"), JSON.stringify({ format: 1, entries: {
      [dead.key]: dead, [recent.key]: recent,
      "topology/participants/keep": { ...dead, key: "topology/participants/keep" },
      "residency/deliveries/keep": { ...dead, key: "residency/deliveries/keep" },
    } }));
    await store.writeBatch({ identity, ops: [{ kind: "put", key: "field/write/needed", value: 1 }],
      prepare: current => compactExpiredHostRecords(current, r, identity.id, now) });
    expect(store.get(dead.key, { fresh: true })).toBeUndefined();
    expect(store.get(recent.key, { fresh: true })).toBeDefined();
    expect(store.get("topology/participants/keep")).toBeDefined(); expect(store.get("residency/deliveries/keep")).toBeDefined();
  });
});
describe("established heartbeat failure is not death", () => {
  const directory = (r: string) => {
    const mesh = new MeshStore(r, 256 * 1024, 1000);
    const d = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 5000, leaseMs: 15000, reapDeadHosts: false }); directories.push(d);
    d.registerSource((): FabricParticipantRecord[] => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "observer", label: "Observer-1", status: "idle",
      runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"], cwd: r, sessionId: "observer",
      startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }]); return d;
  };
  it("renews only the established liveness file after a lock timeout; confirmation and admission stay failed", async () => {
    const r = root(), d = directory(r); await d.refresh(); const confirmed = d.confirmedAt();
    const previous = readHostLease(r, identity.id)!;
    // Advance beyond the prior TTL, not a simulated dead process; failed writes must not publish death.
    vi.spyOn(Date, "now").mockReturnValue(previous.expiresAt + 1);
    const error = new MeshLockTimeoutError(" held by a live fixture", 1, 10000);
    vi.spyOn(d.mesh, "confirmWritable").mockRejectedValue(error);
    await expect(d.refresh()).rejects.toBe(error);
    const after = readHostLease(r, identity.id)!;
    expect(after.startedAt).toBe(previous.startedAt); expect(after.identityId).toBe(previous.identityId);
    expect(after.expiresAt).toBeGreaterThan(Date.now()); expect(d.confirmedAt()).toBe(confirmed);
    expect(d.canConsumeMesh()).toBe(false); expect(d.routingUnavailable()).toContain("Timed out");
    expect(d.get(identity.id, undefined, { fresh: true })?.stale).toBe(false);
    expect(d.lineageAlive(identity.id)).toBe(true);
  });
  it("does not grant initial admission or committed ownership after an initial write failure", async () => {
    const r = root(), d = directory(r), error = new MeshLockTimeoutError(" fixture", 1, 10000);
    vi.spyOn(d.mesh, "writeBatch").mockRejectedValue(error);
    await expect(d.refresh()).rejects.toBe(error);
    expect(d.canConsumeMesh()).toBe(false);
    expect(d.routingUnavailable()).toContain("Timed out");
    expect(d.mesh.get(key(identity.id), { fresh: true })).toBeUndefined();
    // Initial per-key preparation may advertise liveness, but that file is not admission.
  });
});
