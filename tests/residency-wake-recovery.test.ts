import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import * as wake from "../src/residency/wake.js";
import * as index from "../src/residency/wake-index.js";

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-recovery-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:recovery", sessionId: "recovery", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:recovery"),
    fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused",
    piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  return { root, config, from: { id: "publisher", name: "publisher", kind: "main" as const } };
};
const overflow = (meshRoot: string, root: string, id: string, sequence = 1) => {
  const directory = path.join(meshRoot, "wake-overflow");
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, `${path.basename(root)}.json`), JSON.stringify({
    format: 1, op: "pending", root, delivery: { id, sequence, deferredAt: sequence },
  }));
};

describe("bounded own-root resident startup recovery", () => {
  it("publishes readiness after at most 256 of 10,000 overflow entries, then visits every entry exactly once in yielding batches", async () => {
    const f = fixture();
    const host = new ResidentHost(f.config, () => {});
    const seen = new Map<string, number>();
    const batches: Array<{ entries: number; bytes: number; ready: boolean }> = [];
    for (let n = 0; n < 10_000; n++) overflow(f.config.meshRoot,
      residentRoot(f.config.meshRoot, `session:overflow-${n}`), `event-${n}`);
    const next = index.ResidentWakeRecoveryReader.prototype.next;
    vi.spyOn(index.ResidentWakeRecoveryReader.prototype, "next").mockImplementation(function(this: index.ResidentWakeRecoveryReader) {
      const batch = next.call(this);
      batches.push({ entries: batch.entries, bytes: batch.bytes,
        ready: fs.existsSync(path.join(f.config.residencyRoot, "maintenance-ready.json")) });
      for (const delivery of batch.pending.values()) seen.set(delivery.id, (seen.get(delivery.id) ?? 0) + 1);
      return batch;
    });
    let complete!: () => void;
    const completed = new Promise<void>(resolve => { complete = resolve; });
    const warn = vi.spyOn(console, "warn").mockImplementation(message => {
      if (String(message).includes("deferred entries processed after readiness")) complete();
    });
    const launch = vi.fn(async () => {});
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery) => dispatch(mesh, events, launch, recovery));
    try {
      await host.start();
      expect(fs.existsSync(path.join(f.config.residencyRoot, "maintenance-ready.json"))).toBe(true);
      expect(seen.size).toBe(256);
      expect(batches).toHaveLength(1);
      expect(batches[0]!.ready).toBe(false);
      await completed;
      expect(seen.size).toBe(10_000);
      expect([...seen.values()].every(count => count === 1)).toBe(true);
      expect(batches.slice(1).every(batch => batch.ready)).toBe(true);
      expect(batches.every(batch => batch.entries <= index.RESIDENT_WAKE_RECOVERY_MAX_ENTRIES &&
        batch.bytes <= index.RESIDENT_WAKE_RECOVERY_MAX_BYTES)).toBe(true);
      expect(warn.mock.calls.filter(([message]) => String(message).includes("9744 deferred entries processed after readiness; 10000 ignored"))).toHaveLength(1);
      expect(launch).not.toHaveBeenCalled(); // foreign entries are visited, not authorized
      expect(fs.readdirSync(path.join(f.config.meshRoot, "wake-overflow"))).toHaveLength(10_000); // nothing dropped
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(f.root, { recursive: true, force: true }); }
  }, 30_000);

  it("never dispatches a forged journal record naming another in-mesh resident", async () => {
    const f = fixture();
    const foreign = residentRoot(f.config.meshRoot, "session:foreign");
    const saved = { rootId: "session:foreign", residencyRoot: foreign, cwd: f.root };
    fs.mkdirSync(foreign, { recursive: true });
    fs.writeFileSync(path.join(foreign, "config.json"), JSON.stringify(saved));
    fs.writeFileSync(path.join(foreign, "wake-routes.json"), JSON.stringify({ format: 1, rootId: saved.rootId,
      hostId: "host:foreign", configJson: index.canonicalResidentWakeConfig(saved),
      actors: [{ id: "foreign-listener", name: "foreign-listener", topics: ["foreign.topic"] }] }));
    fs.writeFileSync(index.residentWakeIndexPath(f.config.meshRoot), JSON.stringify({
      format: 1, op: "pending", root: foreign, delivery: { id: "forged", sequence: 42 },
    }) + "\n");
    const launch = vi.fn(async () => {});
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery) => dispatch(mesh, events, launch, recovery));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const host = new ResidentHost(f.config, () => {});
    try {
      await host.start();
      await host.close();
      expect(launch).not.toHaveBeenCalled();
      expect(fs.existsSync(wake.residentWakeRequestPath(foreign))).toBe(false);
      expect(index.residentUnacknowledgedDeliveries(f.config.meshRoot).get(foreign)?.id).toBe("forged");
      expect(warn.mock.calls.some(([message]) => String(message).includes("1 ignored (foreign, unmatched or not pending)"))).toBe(true);
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });

  it.each(["valid", "missing-sequence", "wrong-id", "missing-event", "route-mismatch", "consumed", "missing-cursor", "malformed-cursor", "foreign-registry"] as const)(
    "validates archived event, route, registry ownership and cursor: %s", async mode => {
      const f = fixture();
      const seed = new ResidentHost(f.config, () => {});
      const mesh = new MeshStore(f.config.meshRoot, 65_536, 100, { canWakeResidents: () => false });
      const launch = vi.fn(async () => {});
      vi.spyOn(console, "warn").mockImplementation(() => {});
      try {
        await seed.start();
        const actor = await seed.actors.create({ name: "listener", instructions: "listen", residency: "durable", topics: ["recovery.topic"] });
        await vi.waitFor(() => expect(index.retainedRoutesAt(f.config.residencyRoot)?.actors.some(route => route.id === actor.id)).toBe(true), { timeout: 3_000 });
        const ownedActors = seed.actors.listOwned(true);
        seed.actors.pauseForRelease(); await seed.actors.checkpointForRelease(); await seed.close();
        await wake.ensureResidentWakeArchive(mesh);
        const event = await mesh.publish({ topic: mode === "route-mismatch" ? "unrelated" : "recovery.topic", from: f.from });
        const delivery: index.WakeDelivery = { id: event.id, sequence: event.sequence };
        if (mode === "missing-sequence") delete delivery.sequence;
        if (mode === "wrong-id") delivery.id = "forged";
        if (mode === "missing-event") delivery.sequence = event.sequence + 10_000;
        if (mode === "consumed") fs.writeFileSync(path.join(f.config.residencyRoot, `actor-mesh-cursor.json.${actor.scope}`),
          JSON.stringify({ format: 1, cursor: 0, last: { id: event.id, sequence: event.sequence } }));
        if (mode === "missing-cursor") {
          for (const suffix of ["", ".project", ".session"]) fs.rmSync(path.join(f.config.residencyRoot, `actor-mesh-cursor.json${suffix}`), { force: true });
        }
        if (mode === "malformed-cursor") {
          fs.writeFileSync(path.join(f.config.residencyRoot, "actor-mesh-cursor.json"), JSON.stringify({ format: 1, cursor: 0 }));
          fs.writeFileSync(path.join(f.config.residencyRoot, `actor-mesh-cursor.json.${actor.scope}`), "not-json");
        }
        if (mode === "foreign-registry") ownedActors[0]!.rootId = "session:foreign";
        const recovery: wake.ResidentWakeRecovery = { config: f.config, ownedActors,
          batch: { pending: new Map([[f.config.residencyRoot, delivery]]), entries: 1, bytes: 1024 }, ignored: 0 };
        await wake.wakeResidentActors(mesh, [], launch, recovery);
        expect(launch).toHaveBeenCalledTimes(mode === "valid" ? 1 : 0);
        expect(recovery.ignored).toBe(mode === "valid" ? 0 : 1);
        expect(fs.existsSync(wake.residentWakeRequestPath(f.config.residencyRoot))).toBe(mode === "valid");
        if (mode === "valid") expect(index.residentUnacknowledgedDeliveries(mesh.root).size).toBe(0);
      } finally { await seed.close(); mesh.closeState(); vi.restoreAllMocks(); fs.rmSync(f.root, { recursive: true, force: true }); }
    });

  it("refuses an archived event larger than the recovery byte budget before opening its event bytes", async () => {
    const f = fixture();
    const mesh = new MeshStore(f.config.meshRoot, 65_536, 100);
    try {
      await wake.ensureResidentWakeArchive(mesh);
      const event = await mesh.publish({ topic: "budget", from: f.from, data: "payload" });
      const archive = MeshArchive.fromRoot(mesh.root)!;
      const open = vi.spyOn(fs, "openSync");
      expect(() => archive.lookupEntry(event.sequence, 1)).toThrow("caller byte budget");
      expect(open.mock.calls.some(([file]) => String(file).endsWith(".jsonl"))).toBe(false);
    } finally { mesh.closeState(); vi.restoreAllMocks(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });
});
