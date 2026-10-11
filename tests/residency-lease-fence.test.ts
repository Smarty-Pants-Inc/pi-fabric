import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(r => { resolve = r; }); return { promise, resolve }; };
const pause = (ms: number) => new Promise(r => setTimeout(r, ms));
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-lease-fence-"));
  const identity = { id: "owner", name: "owner", kind: "main" as const };
  const mesh = new MeshStore(path.join(root, "mesh"), 65536, 1000);
  let healthy = true;
  const owner = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "owner", pollMs: 20, acknowledgementTimeoutMs: 5000, canConsumeMesh: () => healthy });
  const sender = new FabricControlPlane(new MeshStore(mesh.root, 65536, 1000), { ...identity, id: "sender" }, { enabled: true, hostId: "sender", pollMs: 20, acknowledgementTimeoutMs: 5000 });
  sender.start(() => ({ accepted: false }));
  const seenRoot = path.join(mesh.root, "control-seen", createHash("sha256").update("owner").digest("hex").slice(0, 32));
  const seen = new MeshStore(seenRoot, 65536, 1000);
  return { root, owner, sender, mesh, seen, seenRoot, lease: (value: boolean) => { healthy = value; }, close: async () => { await owner.close(); await sender.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); } };
};

describe("Astra F2 resident control lease fences", () => {
  it.each(["shared", "local"] as const)("does not admit/deliver or commit a sequence after the %s claim lock wait loses its lease", async stage => {
    const f = setup(); const entered = deferred(), release = deferred(); let gated = false;
    const shouldGate = (store: MeshStore) => stage === "shared" ? store.root === f.mesh.root : store.root === f.seenRoot;
    const put = MeshStore.prototype.put;
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
      if (!gated && shouldGate(this) && input.key.startsWith("topology/control-seen/")) { gated = true; entered.resolve(); await release.promise; }
      return put.call(this, input);
    });
    const batch = MeshStore.prototype.writeBatch;
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: MeshStore, input) {
      if (!gated && shouldGate(this) && (input.ops.some(op => op.key.startsWith("topology/control-seen/")) || input.prepare)) { gated = true; entered.resolve(); await release.promise; }
      return batch.call(this, input);
    });
    const handler = vi.fn(() => ({ accepted: true, messageId: "once" })); f.owner.start(handler);
    const result = f.sender.request("owner", "actor", "followUp", { message: "work", ownerIncarnation: f.owner.incarnation }).catch(error => error);
    try {
      await entered.promise; f.lease(false); release.resolve(); await pause(120);
      expect(handler).not.toHaveBeenCalled();
      expect(f.seen.listAll("topology/control-seen/").every(entry => !(entry.value as { sequence?: number }).sequence)).toBe(true);
      if (stage === "shared") expect(f.mesh.listAll("topology/control-seen/")).toHaveLength(0);
      f.lease(true); expect(await result).toMatchObject({ acknowledged: true, messageId: "once" }); expect(handler).toHaveBeenCalledOnce();
    } finally { release.resolve(); f.lease(true); await result; await f.close(); }
  });

  it.each(["outcome", "ack"] as const)("fences the actual %s commit after its lock waiter loses the lease, retaining the completed handler", async stage => {
    const f = setup(); const entered = deferred(), release = deferred(); let gated = false, localWrites = 0;
    const batch = MeshStore.prototype.writeBatch;
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: MeshStore, input) {
      if (this.root === f.seenRoot && ++localWrites === 2 && stage === "outcome") { gated = true; entered.resolve(); await release.promise; }
      return batch.call(this, input);
    });
    const publish = MeshStore.prototype.publish;
    vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
      if (!gated && stage === "ack" && input.topic === "fabric.control.ack") { gated = true; entered.resolve(); await release.promise; }
      return publish.call(this, input);
    });
    const handler = vi.fn(() => ({ accepted: true, messageId: "once" })); f.owner.start(handler);
    const result = f.sender.request("owner", "actor", "followUp", { message: "work", ownerIncarnation: f.owner.incarnation }).catch(error => error);
    try {
      await entered.promise; f.lease(false); release.resolve(); await pause(120);
      expect(f.mesh.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(0);
      if (stage === "outcome") expect(f.seen.listAll("topology/control-seen/")[0]!.value).not.toHaveProperty("sequence");
      expect(handler).toHaveBeenCalledOnce();
      f.lease(true); expect(await result).toMatchObject({ acknowledged: true, messageId: "once" }); expect(handler).toHaveBeenCalledOnce();
    } finally { release.resolve(); f.lease(true); await result; await f.close(); }
  });

  it("keeps a completed sequence-free outcome across a fenced host restart without running the handler again", async () => {
    const f = setup(); const entered = deferred(), release = deferred();
    const handler = vi.fn(async () => { entered.resolve(); await release.promise; return { accepted: true, messageId: "original" }; }); f.owner.start(handler);
    const result = f.sender.request("owner", "actor", "followUp", { message: "work", ownerIncarnation: f.owner.incarnation }).catch(error => error);
    let successor: FabricControlPlane | undefined;
    try {
      await entered.promise; f.lease(false); release.resolve();
      await vi.waitFor(() => expect(f.seen.listAll("topology/control-seen/")[0]?.value).toMatchObject({ acceptance: { accepted: true, messageId: "original" } }));
      expect(f.seen.listAll("topology/control-seen/")[0]!.value).not.toHaveProperty("sequence");
      await f.owner.close();
      successor = new FabricControlPlane(new MeshStore(f.mesh.root, 65536, 1000), { id: "owner", name: "owner", kind: "main" }, { enabled: true, hostId: "owner", pollMs: 20, acknowledgementTimeoutMs: 5000, canConsumeMesh: () => true });
      const replay = vi.fn(() => ({ accepted: false, error: "must not run" })); successor.start(replay);
      expect(await result).toMatchObject({ acknowledged: true, messageId: "original" });
      expect(handler).toHaveBeenCalledOnce(); expect(replay).not.toHaveBeenCalled();
    } finally { release.resolve(); f.lease(true); await result; await successor?.close(); await f.close(); }
  });

  it.each(["followUp", "ask"] as const)("retains the completed %s outcome without a sequence/ACK commit or replay across lease loss", async operation => {
    const f = setup(); const entered = deferred(), release = deferred();
    const handler = vi.fn(async () => { entered.resolve(); await release.promise; return { accepted: true, messageId: "once" }; }); f.owner.start(handler);
    const result = f.sender.request("owner", "actor", operation, { message: "work", ownerIncarnation: f.owner.incarnation }).catch(error => error);
    try {
      await entered.promise; f.lease(false); release.resolve(); await pause(120);
      expect(f.seen.listAll("topology/control-seen/").every(entry => !(entry.value as { sequence?: number }).sequence)).toBe(true);
      expect(f.mesh.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(0);
      f.lease(true); expect(await result).toMatchObject({ acknowledged: true, messageId: "once" }); expect(handler).toHaveBeenCalledOnce();
    } finally { release.resolve(); f.lease(true); await result; await f.close(); }
  });
});
