// Keep reason: PR #58 / smarty-dev#643 is an exactly-once and private-outcome boundary.
// Competing real file readers forward their actual CAS to real file/SQLite state stores.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

const ownerId = "host:one-claim";
const epoch = "epoch:one-claim";
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const key = (id: string) => "topology/control-seen/" + createHash("sha256").update(`${ownerId}\0${id}`).digest("hex");
const gate = () => {
  let open!: () => void;
  const promise = new Promise<void>(resolve => { open = resolve; });
  return { promise, open };
};
const until = (check: () => void) => vi.waitFor(check, { timeout: 5_000, interval: 10 });
const planes: FabricControlPlane[] = [];
const roots: string[] = [];
const stores: MeshStore[] = [];
const setup = (backend: "file" | "sqlite", fenced: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-one-claim-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 65_536, 1_000, { stateBackend: backend });
  stores.push(mesh);
  expect(mesh.stateBackend).toBe(backend); // Never label a fallback as a SQLite race.
  const makeOwner = () => {
    const replica = new MeshStore(meshRoot, 65_536, 1_000, { stateBackend: backend });
    stores.push(replica);
    expect(replica.stateBackend).toBe(backend);
    const owner = new FabricControlPlane(replica, identity(ownerId), {
      enabled: true, hostId: ownerId, ownerIncarnation: epoch, controlIncarnationFence: "enforce",
      pollMs: 20, acknowledgementTimeoutMs: 60_000, ...(fenced ? { canConsumeMesh: () => true } : {}),
    });
    planes.push(owner); return owner;
  };
  const own = new MeshStore(path.join(meshRoot, "control-seen", createHash("sha256").update(ownerId).digest("hex").slice(0, 32)),
    65_536, 1_000, { stateBackend: "file" });
  stores.push(own);
  const publish = (id: string) => mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: identity("host:sender"), to: ownerId,
    data: { version: 1, commandId: id, targetId: "agent:target", operation: "followUp", replyTo: "host:sender",
      ownerIncarnation: epoch, message: "PRIVATE-INPUT", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 } });
  const acks = (id: string) => mesh.read({ topic: "fabric.control.ack", limit: 100 })
    .filter(event => (event.data as { commandId?: string }).commandId === id);
  return { mesh, own, makeOwner, publish, acks };
};

afterEach(async () => {
  for (const owner of planes.splice(0)) { owner.pause(); await owner.close(); }
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("one authoritative control claim", () => {
  it.each([
    ["file", false], ["file", true], ["sqlite", false], ["sqlite", true],
  ] as const)("two actual drainers racing one command run exactly one handler (%s, fenced=%s)", async (backend, fenced) => {
    const f = setup(backend, fenced), release = gate();
    const first = f.makeOwner(), second = f.makeOwner();
    await f.publish("race");
    let contenders = 0;
    const completed: string[] = [];
    const put = MeshStore.prototype.put, batch = MeshStore.prototype.writeBatch;
    // Hold BOTH readers after their duplicate reads, before either real shared CAS.
    // Counting successful ACKs or a native-message deduper would miss duplicate handlers.
    const enter = async <T,>(commit: () => Promise<T>): Promise<T> => {
      contenders++; await release.promise;
      try { const result = await commit(); completed.push("won"); return result; }
      catch (error) { completed.push("lost"); throw error; }
    };
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(function (this: MeshStore, input) {
      if (!fenced && this.root === f.mesh.root && input.key === key("race") && input.ifVersion === 0) {
        return enter(() => put.call(this, input));
      }
      return put.call(this, input);
    });
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(function (this: MeshStore, input) {
      if (fenced && this.root === f.mesh.root && input.ops.some(op => op.kind === "put" && op.key === key("race") && op.ifVersion === 0)) {
        return enter(() => batch.call(this, input));
      }
      return batch.call(this, input);
    });
    const receive = vi.fn(() => ({ accepted: true, messageId: "only-handler", result: { secret: "PRIVATE-OUTCOME" } }));
    first.start(receive); second.start(receive);
    try {
      await until(() => expect(contenders).toBe(2));
      expect(receive).not.toHaveBeenCalled();
      release.open();
      await until(() => expect(completed.sort()).toEqual(["lost", "won"]));
      await until(() => expect(f.acks("race").some(event => (event.data as { accepted?: boolean }).accepted)).toBe(true));
      first.pause(); second.pause();
      await first.checkpointForRelease(); await second.checkpointForRelease();
      expect(receive).toHaveBeenCalledOnce();
      expect(f.mesh.get(key("race"), { fresh: true })?.value).not.toHaveProperty("acceptance");
      expect(JSON.stringify(f.mesh.listAll("topology/control-seen/", { fresh: true }))).not.toContain("PRIVATE-OUTCOME");
      expect(f.own.get(key("race"), { fresh: true })?.value).toMatchObject({ acceptance: {
        accepted: true, messageId: "only-handler", result: { secret: "PRIVATE-OUTCOME" },
      } });
    } finally { release.open(); }
  });

  it("writes only the private outcome, not a second pre-handler claim", async () => {
    const f = setup("sqlite", true), owner = f.makeOwner(), entered = gate(), finish = gate();
    const receive = vi.fn(async () => { entered.open(); await finish.promise; return { accepted: true, messageId: "private-result" }; });
    const event = await f.publish("outcome-only"); owner.start(receive);
    try {
      await entered.promise;
      expect(f.mesh.get(key("outcome-only"), { fresh: true })?.value).toMatchObject({ commandId: "outcome-only", ownerIncarnation: epoch });
      expect(f.own.get(key("outcome-only"), { fresh: true })).toBeUndefined();
    } finally { finish.open(); }
    await until(() => expect(f.acks("outcome-only").some(event => (event.data as { accepted?: boolean }).accepted)).toBe(true));
    owner.pause(); await owner.checkpointForRelease();
    expect(f.own.get(key("outcome-only"), { fresh: true })).toMatchObject({ version: 1, value: {
      sequence: event.sequence, acceptance: { accepted: true, messageId: "private-result" },
    } });
    expect(receive).toHaveBeenCalledOnce();
  });
});
