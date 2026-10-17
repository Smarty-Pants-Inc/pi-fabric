// Keep reason: PR #58 / smarty-dev#643 is an exactly-once and private-outcome boundary.
// Competing real file readers forward their actual CAS to real file/SQLite state stores.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { assertMeshConsumption } from "../src/topology/mesh-consumption.js";
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
const setup = (backend: "file" | "sqlite", fenced: boolean, readCacheMs = 0) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-one-claim-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 65_536, 1_000, { stateBackend: backend });
  stores.push(mesh);
  expect(mesh.stateBackend).toBe(backend); // Never label a fallback as a SQLite race.
  const makeOwner = () => {
    const replica = new MeshStore(meshRoot, 65_536, 1_000, { stateBackend: backend, readCacheMs });
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
  const publish = (id: string, requestedAt = Date.now(), deadlineAt = requestedAt + 60_000) => mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: identity("host:sender"), to: ownerId,
    data: { version: 1, commandId: id, targetId: "agent:target", operation: "followUp", replyTo: "host:sender",
      ownerIncarnation: epoch, message: "PRIVATE-INPUT", requestedAt, deadlineAt } });
  const acks = (id: string) => mesh.read({ topic: "fabric.control.ack", limit: 100 })
    .filter(event => (event.data as { commandId?: string }).commandId === id);
  return { mesh, own, makeOwner, publish, acks };
};

// Bounded claim-only compatibility fixture, NOT an installed historical Pi/class.
// Commit/source pins (control-plane.ts; full source retained in compatibility evidence):
// private-only: 5f7eebcae69e042355aad03f492b13602f1cae56,
//   sha256 105abca80b427a18cbe8dee747ce82efa7e5049d911651fdd92ad5cc15aa7706,
//   #acceptCommand:449-544, #seenRecord:632-634 (read both, CAS private only).
// oldest supplied deployed release by ancestry: 41a73f8588e5351d9409736f725fee218810b78c,
//   sha256 22c8fa889b484680f1373b9dfed400f672a43d59759d6563e5ccca8e8015fe7d,
//   #putFenced:342-349, #acceptCommand:1077-1152, #seenRecord:1305-1307.
// Only valid, in-deadline, ordinary commands and fresh runtime custody are modeled;
// old timers/ACKs/outcome persistence are not. Modern receivers exercise those below.
const historicalClaimant = (f: ReturnType<typeof setup>, protocol: "private-only" | "shared-private", fenced: boolean) => {
  const shared = new MeshStore(f.mesh.root, 65_536, 1_000, { stateBackend: f.mesh.stateBackend });
  const own = new MeshStore(f.own.root, 65_536, 1_000, { stateBackend: "file" });
  stores.push(shared, own);
  expect(shared.stateBackend).toBe(f.mesh.stateBackend);
  expect(own.stateBackend).toBe("file");
  const sharedClaims = new Map<string, number>();
  const trace: string[] = [];
  const putFenced = async (store: MeshStore, input: Parameters<MeshStore["put"]>[0]) => {
    if (!fenced) return store.put(input);
    const results = await store.writeBatch({ identity: identity(ownerId), ops: [], prepare: () => {
      assertMeshConsumption(() => true);
      return [{ kind: "put", ...input }];
    } });
    return { version: results[0]!.version };
  };
  return { trace, async accept(event: MeshEvent, afterRead: () => Promise<void>, handler: () => void) {
    const command = event.data as { commandId: string; targetId: string; deadlineAt: number };
    const k = key(command.commandId);
    const duplicate = sharedClaims.has(k) ? own.get(k)?.value : own.get(k)?.value ?? shared.get(k)?.value;
    if (duplicate) { trace.push("duplicate"); return "duplicate"; }
    trace.push("read-empty");
    await afterRead(); // Forced stale-read / pre-CAS interleaving, never a mocked CAS winner.
    const value = { format: 1, hostId: ownerId, commandId: command.commandId,
      targetId: command.targetId, expiresAt: command.deadlineAt + 60_000 };
    try {
      if (protocol === "shared-private") {
        if (!sharedClaims.has(k)) {
          trace.push("shared-attempt");
          await putFenced(shared, { key: k, value: { ...value, explicitDeadline: true }, identity: identity(ownerId), ifVersion: 0 });
          trace.push("shared-won");
        }
        sharedClaims.set(k, command.deadlineAt + 60_000);
      }
      trace.push("private-attempt");
      const input = { key: k, value: { ...value, ...(protocol === "private-only" ? { sequence: event.sequence } : {}) },
        identity: identity(ownerId), ifVersion: 0 };
      // 5f7 used direct put; 41a used its optional under-lock lease fence.
      if (protocol === "private-only") await own.put(input); else await putFenced(own, input);
      trace.push("private-won");
    } catch (error) {
      sharedClaims.delete(k);
      // Distinguish a real create-only CAS conflict from a fixture/IO/lock failure.
      expect(String(error)).toContain(`Mesh compare-and-swap failed for ${k}: expected version 0, found 1`);
      trace.push("cas-lost");
      return "lost";
    }
    sharedClaims.delete(k);
    trace.push("handler"); handler(); return "won";
  } };
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

  describe.each(["file", "sqlite"] as const)("historical writer boundary (%s)", backend => {
    it("UNSUPPORTED 5f7 private-only stale read permits two handlers (F1 counterexample)", async () => {
      const f = setup(backend, false), owner = f.makeOwner();
      const old = historicalClaimant(f, "private-only", false);
      const read = gate(), resume = gate(), entered = gate(), finish = gate();
      const event = await f.publish("private-stale");
      const olderHandler = vi.fn();
      const oldRun = old.accept(event, async () => { read.open(); await resume.promise; }, olderHandler);
      const currentHandler = vi.fn(async () => { entered.open(); await finish.promise; return { accepted: true }; });
      try {
        await read.promise; owner.start(currentHandler); await entered.promise;
        expect(f.mesh.get(key("private-stale"), { fresh: true })?.version).toBe(1);
        expect(f.own.get(key("private-stale"), { fresh: true })).toBeUndefined();
        resume.open(); expect(await oldRun).toBe("won");
        expect(old.trace).toEqual(["read-empty", "private-attempt", "private-won", "handler"]);
        expect(currentHandler.mock.calls.length + olderHandler.mock.calls.length).toBe(2);
        expect(f.own.get(key("private-stale"), { fresh: true })?.version).toBe(1);
        // Candidate outcome is still unsaved: F1 is real duplication, not ACK counting.
      } finally { resume.open(); finish.open(); await oldRun; }
    });

    it.each(["private-only", "shared-private"] as const)("older %s wins first: current handler is refused", async protocol => {
      const f = setup(backend, true), owner = f.makeOwner();
      const old = historicalClaimant(f, protocol, true), olderHandler = vi.fn();
      const event = await f.publish("old-first");
      expect(await old.accept(event, async () => {}, olderHandler)).toBe("won");
      const currentHandler = vi.fn(() => ({ accepted: true })); owner.start(currentHandler);
      await until(() => expect(f.acks("old-first").length).toBeGreaterThan(0));
      owner.pause(); await owner.close();
      expect(olderHandler).toHaveBeenCalledOnce();
      expect(currentHandler).not.toHaveBeenCalled();
      expect(currentHandler.mock.calls.length + olderHandler.mock.calls.length).toBe(1);
    });

    it.each([false, true])("oldest deployed 41a stale read loses shared CAS, never private claim/handler (fenced=%s)", async fenced => {
      const f = setup(backend, fenced), owner = f.makeOwner();
      const old = historicalClaimant(f, "shared-private", fenced);
      const read = gate(), resume = gate(), entered = gate(), finish = gate();
      const event = await f.publish("shared-stale"), olderHandler = vi.fn();
      const oldRun = old.accept(event, async () => { read.open(); await resume.promise; }, olderHandler);
      const currentHandler = vi.fn(async () => { entered.open(); await finish.promise; return { accepted: true, messageId: "shared-winner" }; });
      try {
        await read.promise; owner.start(currentHandler); await entered.promise;
        expect(f.mesh.get(key("shared-stale"), { fresh: true })?.version).toBe(1);
        expect(f.own.get(key("shared-stale"), { fresh: true })).toBeUndefined();
        resume.open(); expect(await oldRun).toBe("lost");
        expect(old.trace).toEqual(["read-empty", "shared-attempt", "cas-lost"]);
        expect(olderHandler).not.toHaveBeenCalled();
        expect(f.own.get(key("shared-stale"), { fresh: true })).toBeUndefined();
        expect(currentHandler.mock.calls.length + olderHandler.mock.calls.length).toBe(1);
      } finally { resume.open(); finish.open(); await oldRun; }
      await until(() => expect(f.acks("shared-stale").some(event => (event.data as { accepted?: boolean }).accepted)).toBe(true));
      owner.pause(); await owner.checkpointForRelease();
      expect(f.own.get(key("shared-stale"), { fresh: true })).toMatchObject({ version: 1, value: {
        sequence: event.sequence, acceptance: { accepted: true, messageId: "shared-winner" },
      } });
    });
  });

  it.each(["late", "future"] as const)("%s commands use the shared claim and private expiry outcome, including restart", async timing => {
    const f = setup("file", true), owner = f.makeOwner();
    const now = Date.now(), requestedAt = timing === "late" ? now - 2_000 : now + 120_000;
    const event = await f.publish(timing, requestedAt, requestedAt + 1_000);
    if (timing === "future") {
      const batch = MeshStore.prototype.writeBatch;
      vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: MeshStore, input) {
        const result = await batch.call(this, input);
        if (this.root === f.mesh.root && input.ops.some(op => op.kind === "put" && op.key === key(timing))) {
          vi.spyOn(Date, "now").mockReturnValue(requestedAt); // Skew passes while the claim awaits; refusal must stick.
        }
        return result;
      });
    }
    const receive = vi.fn(() => ({ accepted: true })); owner.start(receive);
    await until(() => expect(f.acks(timing)).toHaveLength(1));
    owner.pause(); await owner.checkpointForRelease(); await owner.close();
    const expiry = { accepted: false, error: "Fabric control command expired", notRun: true };
    expect(f.mesh.get(key(timing), { fresh: true })?.value).not.toHaveProperty("acceptance");
    expect(f.own.get(key(timing), { fresh: true })).toMatchObject({ version: 1, value: {
      ownerIncarnation: epoch, sequence: event.sequence, acceptance: expiry,
    } });
    const restarted = f.makeOwner(); restarted.start(receive);
    await until(() => expect(f.acks(timing)).toHaveLength(2));
    expect(f.acks(timing).map(ack => ack.data)).toEqual([
      expect.objectContaining({ ...expiry, ownerIncarnation: epoch }), expect.objectContaining({ ...expiry, ownerIncarnation: epoch }),
    ]);
    expect(receive).not.toHaveBeenCalled();
  });

  it.each(["shared-result", "private-result", "unknown"] as const)("late cached reader loses the real shared CAS and ACKs %s, never notRun", async outcome => {
    const f = setup("file", false, 60_000), owner = f.makeOwner(), entered = gate(), resume = gate();
    await f.mesh.put({ key: "warm", value: 1, identity: identity(ownerId) });
    await f.publish("cached-late", Date.now() - 2_000, Date.now() - 1_000);
    const put = MeshStore.prototype.put;
    let attempts = 0;
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
      if (this.root === f.mesh.root && this !== f.mesh && input.key === key("cached-late") && input.ifVersion === 0) {
        attempts++; entered.open(); await resume.promise;
      }
      return put.call(this, input);
    });
    const receive = vi.fn(() => ({ accepted: true })); owner.start(receive);
    try {
      await entered.promise;
      const acceptance = { accepted: true, messageId: "earlier-delivery" };
      const record = { format: 1, hostId: ownerId, commandId: "cached-late", targetId: "agent:target",
        ownerIncarnation: epoch, expiresAt: Date.now() + 60_000 };
      const claimed = await f.mesh.put({ key: key("cached-late"), identity: identity(ownerId), ifVersion: 0,
        value: { ...record, ...(outcome === "shared-result" ? { acceptance } : {}) } });
      if (outcome === "private-result") await f.own.put({ key: key("cached-late"), identity: identity(ownerId), ifVersion: 0,
        value: { ...record, acceptance } });
      expect(owner.mesh.get(key("cached-late"))).toBeUndefined(); // Actual warm cache misses a concurrent claimant.
      resume.open();
      await until(() => expect(f.acks("cached-late")).toHaveLength(1));
      const ack = f.acks("cached-late")[0]!.data;
      expect(ack).not.toHaveProperty("notRun");
      if (outcome === "unknown") expect(ack).toMatchObject({ accepted: false, error: expect.stringContaining("indeterminate") });
      else expect(ack).toMatchObject(acceptance);
      expect(attempts).toBe(1); expect(receive).not.toHaveBeenCalled();
      expect(f.mesh.get(key("cached-late"), { fresh: true })?.version).toBe(claimed.version);
    } finally { resume.open(); }
  });

  it.each(["save", "ack"] as const)("sender timeout stays unknown while late expiry %s retains custody across timeout/retry", async held => {
    const f = setup("file", false), owner = f.makeOwner(), entered = gate(), resume = gate();
    const sender = new FabricControlPlane(f.mesh, identity("host:sender"), {
      enabled: true, hostId: "host:sender", pollMs: 20, acknowledgementTimeoutMs: 400,
    });
    planes.push(sender); sender.start(() => ({ accepted: false }));
    const outcome = sender.request(ownerId, "agent:target", "followUp", { message: "late", ownerIncarnation: epoch })
      .then(() => undefined, (error: Error) => error);
    let command!: { commandId: string; deadlineAt: number };
    await until(() => {
      command = f.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]?.data as typeof command;
      expect(command).toBeDefined();
    });
    await until(() => expect(Date.now()).toBeGreaterThan(command.deadlineAt));
    let failed = false;
    const failOnce = async () => {
      failed = true; entered.open(); await resume.promise;
      throw Object.assign(new Error("expiry lock timeout"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    };
    const put = MeshStore.prototype.put, publish = MeshStore.prototype.publish;
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(function (this: MeshStore, input) {
      if (held === "save" && !failed && this.root === f.own.root && input.key === key(command.commandId)) return failOnce();
      return put.call(this, input);
    });
    vi.spyOn(MeshStore.prototype, "publish").mockImplementation(function (this: MeshStore, input) {
      if (held === "ack" && !failed && this.root === f.mesh.root && input.topic === "fabric.control.ack") return failOnce();
      return publish.call(this, input);
    });
    const receive = vi.fn(() => ({ accepted: true })); owner.start(receive);
    try {
      await entered.promise;
      const error = await outcome;
      expect(error?.message).toContain("the outcome is unknown");
      expect(error?.message).not.toContain("safe");
      expect(error).not.toHaveProperty("notDelivered"); expect(error).not.toHaveProperty("notRun", true);
      expect(f.mesh.get(key(command.commandId), { fresh: true })?.value).not.toHaveProperty("acceptance");
      expect(f.acks(command.commandId)).toHaveLength(0);
      resume.open();
      // A reader retry may ACK both retained custody and the still-uncommitted event.
      await until(() => expect(f.acks(command.commandId).length).toBeGreaterThan(0));
      owner.pause(); await owner.checkpointForRelease();
      expect(f.acks(command.commandId).every(ack => (ack.data as { notRun?: boolean }).notRun === true)).toBe(true);
      expect(f.own.get(key(command.commandId), { fresh: true })).toMatchObject({ version: 1, value: {
        acceptance: { accepted: false, error: "Fabric control command expired", notRun: true },
      } });
      expect(f.mesh.get(key(command.commandId), { fresh: true })?.version).toBe(1);
      expect(f.mesh.read({ topic: "fabric.control.command", limit: 10 }).filter(event => event.kind === "followUp")).toHaveLength(1);
      expect(receive).not.toHaveBeenCalled();
    } finally { resume.open(); }
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
