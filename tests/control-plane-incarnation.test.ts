import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane, STALE_INCARNATION_ERROR } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const directories: ParticipantDirectory[] = [];
const bridges: MeshBridge[] = [];
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-incarnation-")); roots.push(root); return root; };
const store = (root: string) => new MeshStore(root, 65_536, 1_000);
const plane = (mesh: MeshStore, id: string, directory?: ParticipantDirectory) => {
  const value = new FabricControlPlane(mesh, identity(id), { enabled: true, hostId: id, pollMs: 20,
    acknowledgementTimeoutMs: 1_000, ...(directory ? { ownerIncarnation: directory.ownerIncarnation } : {}) });
  planes.push(value); return value;
};
const directory = (mesh: MeshStore, id: string) => {
  const value = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity: identity(id), reapDeadHosts: false });
  value.registerSource(() => [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: "source:forged", name: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "stop", "ask"],
    sessionId: id, cwd: mesh.root, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  directories.push(value); return value;
};
const commands = (mesh: MeshStore) => mesh.read({ topic: "fabric.control.command", limit: 1_000 });
const acks = (mesh: MeshStore) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 });
afterEach(async () => {
  await Promise.all(bridges.splice(0).map(value => value.stop()));
  await Promise.all(planes.splice(0).map(value => value.close()));
  await Promise.all(directories.splice(0).map(value => value.close()));
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("owner incarnation admission and durable receipts", () => {
  it.each(["stop", "ask"] as const)("refuses queued %s after a reload, tells the sender, and never resends", async operation => {
    const mesh = store(temp());
    const oldDirectory = directory(mesh, "session:owner");
    await oldDirectory.refresh();
    const previous = plane(mesh, "session:owner", oldDirectory);
    previous.start(() => ({ accepted: true })); previous.pause();
    const sender = plane(mesh, "session:sender"); sender.start(() => ({ accepted: false }));
    const input = { message: "queued before reload", ownerIncarnation: oldDirectory.self().ownerIncarnation };
    const pending = operation === "ask"
      ? sender.requestResult("session:owner", "session:owner", operation, input).catch(error => error)
      : sender.request("session:owner", "session:owner", operation, input).catch(error => error);
    await vi.waitFor(() => expect(commands(mesh)).toHaveLength(1));
    await previous.close();
    const newDirectory = directory(mesh, "session:owner");
    const restarted = plane(mesh, "session:owner", newDirectory);
    expect(restarted.incarnation).not.toBe(previous.incarnation);
    const handler = vi.fn(() => ({ accepted: true })); restarted.start(handler);
    expect(await pending).toMatchObject({ name: "FabricControlStaleIncarnationError", code: CONTROL_STALE_INCARNATION, message: STALE_INCARNATION_ERROR });
    expect(handler).not.toHaveBeenCalled();
    expect(commands(mesh)).toHaveLength(1);
    expect(mesh.listAll("topology/control-seen/")).toEqual([]);
    expect(acks(mesh)).toContainEqual(expect.objectContaining({ data: expect.objectContaining({ accepted: false,
      ownerIncarnation: restarted.incarnation, staleIncarnation: previous.incarnation, errorCode: CONTROL_STALE_INCARNATION }) }));
  });

  it.each(["steer", "followUp"] as const)("delivers queued explicitly bound session %s once across Main/actor reload", async operation => {
    for (const targetId of ["session:owner", "actor:target"]) {
      const mesh = store(temp());
      const previous = plane(mesh, "session:owner"); previous.start(() => ({ accepted: true })); previous.pause();
      const sender = plane(mesh, "session:sender"); sender.start(() => ({ accepted: false }));
      const pending = sender.request("session:owner", targetId, operation,
        { message: "same session", ownerIncarnation: previous.incarnation }, "session:owner", { idempotencyKey: "same" });
      await vi.waitFor(() => expect(commands(mesh)).toHaveLength(1)); await previous.close();
      const restarted = plane(mesh, "session:owner");
      const handler = vi.fn(() => ({ accepted: true, messageId: "once" })); restarted.start(handler);
      await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "once" });
      await expect(sender.request("session:owner", targetId, operation,
        { message: "same session", ownerIncarnation: previous.incarnation }, "session:owner", { idempotencyKey: "same" }))
        .resolves.toMatchObject({ acknowledged: true, messageId: "once" });
      expect(handler).toHaveBeenCalledOnce();
      expect(acks(mesh).at(-1)!.data).toMatchObject({ ownerIncarnation: restarted.incarnation });
    }
  });

  it.each(["steer", "followUp"] as const)("does not retarget queued %s to a different Main session ID", async operation => {
    const mesh = store(temp());
    const previous = plane(mesh, "session:old"); previous.start(() => ({ accepted: true })); previous.pause();
    const sender = plane(mesh, "session:sender"); sender.start(() => ({ accepted: false }));
    const pending = sender.request("session:old", "session:old", operation,
      { message: "old session only", ownerIncarnation: previous.incarnation }, "session:old", { timeoutMs: 100 }).catch(error => error);
    await vi.waitFor(() => expect(commands(mesh)).toHaveLength(1)); await previous.close();
    const replacement = plane(mesh, "session:new"); const handler = vi.fn(() => ({ accepted: true })); replacement.start(handler);
    expect(await pending).toMatchObject({ notDelivered: true });
    expect(handler).not.toHaveBeenCalled();
    expect(acks(mesh)).toEqual([]);
    expect(commands(mesh)[0]).toMatchObject({ to: "session:old", data: { targetId: "session:old" } });
  });

  it.each(["steer", "followUp", "stop", "ask"] as const)("admits live %s, stamps claims and ACKs, and delivers a same-key replay only once", async operation => {
    const mesh = store(temp());
    const owner = plane(mesh, "session:owner"); const sender = plane(mesh, "session:sender");
    const handler = vi.fn(() => ({ accepted: true, messageId: "delivered", result: "answer" }));
    owner.start(handler); sender.start(() => ({ accepted: false }));
    const input = { message: "live", ownerIncarnation: owner.incarnation };
    for (let i = 0; i < 2; i++) {
      const result = operation === "ask"
        ? await sender.requestResult("session:owner", "actor:target", operation, input, "session:owner", { idempotencyKey: "same" })
        : await sender.request("session:owner", "actor:target", operation, input, "session:owner", { idempotencyKey: "same" });
      expect(result).toEqual(operation === "ask" ? "answer" : { queued: true, routed: "mesh", acknowledged: true, messageId: "delivered" });
    }
    expect(handler).toHaveBeenCalledTimes(1);
    expect(mesh.listAll("topology/control-seen/")[0]!.value).toMatchObject({ ownerIncarnation: owner.incarnation });
    expect(acks(mesh).every(event => (event.data as { ownerIncarnation: string }).ownerIncarnation === owner.incarnation)).toBe(true);
  });

  it("preserves a completed predecessor outcome and its epoch after restart without execution", async () => {
    const mesh = store(temp()); const previous = plane(mesh, "session:owner"); const sender = plane(mesh, "session:sender");
    previous.start(() => ({ accepted: true, messageId: "original" })); sender.start(() => ({ accepted: false }));
    const input = { message: "same input", ownerIncarnation: previous.incarnation };
    await sender.request("session:owner", "actor:target", "steer", input, "session:owner", { idempotencyKey: "same" });
    await previous.close();
    const restarted = plane(mesh, "session:owner"); const handler = vi.fn(() => ({ accepted: true })); restarted.start(handler);
    await expect(sender.request("session:owner", "actor:target", "steer", input, "session:owner", { idempotencyKey: "same" })).resolves.toMatchObject({ messageId: "original" });
    expect(handler).not.toHaveBeenCalled();
    expect((acks(mesh).at(-1)!.data as { ownerIncarnation: string }).ownerIncarnation).toBe(previous.incarnation);
  });

  it("never attests a legacy completed claim as a new activation's outcome", async () => {
    const mesh = store(temp()); const owner = plane(mesh, "session:owner"); const sender = plane(mesh, "session:sender");
    sender.start(() => ({ accepted: false }));
    const pending = sender.request("session:owner", "actor:target", "stop", { ownerIncarnation: owner.incarnation }, "session:owner",
      { timeoutMs: 100, idempotencyKey: "legacy-outcome" }).catch(error => error);
    await vi.waitFor(() => expect(commands(mesh)).toHaveLength(1));
    const command = commands(mesh)[0]!.data as { commandId: string };
    const { createHash } = await import("node:crypto");
    const key = "topology/control-seen/" + createHash("sha256").update(`session:owner\0${command.commandId}`).digest("hex");
    await mesh.put({ key, identity: identity("session:owner"), value: { format: 1, hostId: "session:owner", commandId: command.commandId,
      targetId: "actor:target", expiresAt: Date.now() + 10_000, acceptance: { accepted: true, messageId: "legacy-completed" } } });
    const handler = vi.fn(() => ({ accepted: true })); owner.start(handler);
    await vi.waitFor(() => expect(acks(mesh)).toHaveLength(1));
    expect(acks(mesh)[0]!.data).not.toHaveProperty("ownerIncarnation");
    expect(await pending).toMatchObject({ message: expect.stringContaining("outcome is unknown") });
    expect(handler).not.toHaveBeenCalled();
  });

  it("ignores another epoch's success, missing/malformed epoch, and unrelated fencing refusal", async () => {
    const mesh = store(temp()); const owner = plane(mesh, "session:owner"); const sender = plane(mesh, "session:sender");
    sender.start(() => ({ accepted: false }));
    owner.start(async command => {
      for (const extra of [
        { ownerIncarnation: "other", accepted: true }, { accepted: true }, { ownerIncarnation: 7, accepted: true },
        { ownerIncarnation: "other", accepted: false, errorCode: CONTROL_STALE_INCARNATION, error: STALE_INCARNATION_ERROR, staleIncarnation: "unrelated" },
        { ownerIncarnation: "other", accepted: false, errorCode: CONTROL_STALE_INCARNATION, error: STALE_INCARNATION_ERROR },
        { ownerIncarnation: "other", accepted: true, errorCode: CONTROL_STALE_INCARNATION, error: STALE_INCARNATION_ERROR, staleIncarnation: null },
        { ownerIncarnation: "other", accepted: false, error: STALE_INCARNATION_ERROR, staleIncarnation: null },
        { accepted: false, errorCode: CONTROL_STALE_INCARNATION, error: STALE_INCARNATION_ERROR, staleIncarnation: null },
      ]) await mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("session:owner"), to: "session:sender",
        data: { version: 1, commandId: command.commandId, targetId: command.targetId, messageId: "forged", ...extra } });
      return { accepted: true, messageId: "current" };
    });
    await expect(sender.request("session:owner", "actor:target", "stop", { message: "live", ownerIncarnation: owner.incarnation })).resolves.toMatchObject({ messageId: "current" });
  });

  it("captures the resolved epoch before a publish await, rejecting malformed input before publication", async () => {
    const mesh = store(temp()); const owner = plane(mesh, "session:owner"); const sender = plane(mesh, "session:sender");
    owner.start(() => ({ accepted: true })); sender.start(() => ({ accepted: false }));
    const publish = mesh.publish.bind(mesh); let release!: () => void;
    const gate = new Promise<void>(done => { release = done; });
    vi.spyOn(mesh, "publish").mockImplementation(async (...args) => { await gate; return publish(...args); });
    const input = { message: "live", ownerIncarnation: owner.incarnation };
    const pending = sender.request("session:owner", "actor:target", "steer", input);
    input.ownerIncarnation = "changed while publishing"; release();
    await expect(pending).resolves.toMatchObject({ acknowledged: true });
    expect(commands(mesh)[0]!.data).toMatchObject({ ownerIncarnation: owner.incarnation });
    await expect(sender.request("session:owner", "actor:target", "stop", { ownerIncarnation: "x".repeat(129) })).rejects.toThrow("Invalid Fabric owner incarnation");
    expect(commands(mesh)).toHaveLength(1);
  });

  it.each(["before", "equal", "bridge"])("refuses an unbound legacy command whose age is %s, even with a future requestedAt", async variant => {
    const mesh = store(temp()); const at = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(at);
    if (variant === "before") await mesh.publish({ topic: "fabric.control.command", kind: "stop", from: identity("session:sender"), to: "session:owner",
      data: { version: 1, commandId: "legacy", targetId: "actor:target", operation: "stop", replyTo: "session:sender", requestedAt: at + 20, deadlineAt: at + 5_000 } });
    if (variant === "before") vi.mocked(Date.now).mockReturnValue(at + 1);
    const owner = plane(mesh, "session:owner"); const handler = vi.fn(() => ({ accepted: true })); owner.start(handler);
    if (variant !== "before") {
      vi.mocked(Date.now).mockReturnValue(at + (variant === "bridge" ? 20 : 0));
      await mesh.publish({ topic: "fabric.control.command", kind: "stop", from: identity("session:sender"), to: "session:owner",
        data: { version: 1, commandId: "legacy", targetId: "actor:target", operation: "stop", replyTo: "session:sender", requestedAt: at + 20, deadlineAt: at + 5_000,
          ...(variant === "bridge" ? { bridge: { from: "Forge", id: "original" } } : {}) } });
    }
    await vi.waitFor(() => expect(acks(mesh)).toHaveLength(1));
    expect(handler).not.toHaveBeenCalled(); expect(acks(mesh)[0]!.data).toMatchObject({ errorCode: CONTROL_STALE_INCARNATION });
  });

  it.each(["origin", "legacy"] as const)("fences an unbound %s request created before reload but published after it", async wire => {
    const mesh = store(temp()); const at = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    const previous = plane(mesh, "session:owner");
    previous.start(() => ({ accepted: true })); previous.pause();
    const sender = plane(mesh, "session:sender"); sender.start(() => ({ accepted: false }));
    const publish = mesh.publish.bind(mesh); let release!: () => void;
    const gate = new Promise<void>(done => { release = done; });
    const blocked = vi.spyOn(mesh, "publish").mockImplementation(async input => {
      if (input.topic !== "fabric.control.command" || input.kind === "cancel") return publish(input);
      await gate;
      // A pre-upgrade sender has no immutable origin field and stamps requestedAt at commit.
      const data = input.data;
      return publish(wire === "legacy" ? { ...input, data: (committedAt: number) => {
        const { requestCreatedAt: _origin, ...legacy } = (typeof data === "function" ? data(committedAt) : data) as Record<string, unknown>;
        return legacy;
      } } : input);
    });
    clock.mockReturnValue(at + 10);
    const pending = sender.request("session:owner", "actor:target", "stop", { message: "created before reload" }).catch(error => error);
    expect(commands(mesh)).toHaveLength(0);
    clock.mockReturnValue(at + 20);
    const restarted = plane(mesh, "session:owner");
    const handler = vi.fn(() => ({ accepted: true })); restarted.start(handler);
    clock.mockReturnValue(at + 30); release();
    try {
      expect(await pending).toMatchObject({ name: "FabricControlStaleIncarnationError", code: CONTROL_STALE_INCARNATION });
      expect(commands(mesh)).toHaveLength(1);
      expect(commands(mesh)[0]!.data).toMatchObject({ requestedAt: at + 30, deadlineAt: at + 1_030 });
      if (wire === "origin") expect(commands(mesh)[0]!.data).toMatchObject({ requestCreatedAt: at + 10 });
      else expect(commands(mesh)[0]!.data).not.toHaveProperty("requestCreatedAt");
      expect(handler).not.toHaveBeenCalled();
      expect(mesh.listAll("topology/control-seen/")).toEqual([]);
      expect(acks(mesh)[0]!.data).toMatchObject({ accepted: false, ownerIncarnation: restarted.incarnation, errorCode: CONTROL_STALE_INCARNATION });
    } finally { release(); await pending; blocked.mockRestore(); clock.mockRestore(); }
  });

  it("keeps a session request's origin through a proven-notRun resend without losing reload delivery", async () => {
    const mesh = store(temp()); const at = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    const previous = plane(mesh, "session:owner"); previous.start(() => ({ accepted: true })); previous.pause();
    const sender = plane(mesh, "session:sender"); sender.start(() => ({ accepted: false }));
    clock.mockReturnValue(at + 10);
    const pending = sender.request("session:owner", "actor:target", "steer", { message: "logical request", ownerIncarnation: previous.incarnation }).catch(error => error);
    await vi.waitFor(() => expect(commands(mesh)).toHaveLength(1)); sender.pause();
    const original = commands(mesh)[0]!.data as { commandId: string };
    // A notRun receipt must be backed by a terminal create-only claim. Otherwise
    // the fabricated ACK would falsely permit replay of the still-live original.
    const { createHash } = await import("node:crypto");
    await mesh.put({ key: "topology/control-seen/" + createHash("sha256").update(`session:owner\0${original.commandId}`).digest("hex"),
      identity: identity("session:owner"), value: { format: 1, hostId: "session:owner", commandId: original.commandId,
        targetId: "actor:target", ownerIncarnation: previous.incarnation, expiresAt: at + 10_000,
        acceptance: { accepted: false, error: "Fabric control command expired", notRun: true } } });
    await mesh.publish({ topic: "fabric.control.ack", kind: "rejected", from: identity("session:owner"), to: "session:sender",
      data: { version: 1, commandId: original.commandId, targetId: "actor:target", accepted: false,
        error: "Fabric control command expired", notRun: true, ownerIncarnation: previous.incarnation } });
    clock.mockReturnValue(at + 20);
    const restarted = plane(mesh, "session:owner"); const handler = vi.fn(() => ({ accepted: true })); restarted.start(handler); restarted.pause();
    clock.mockReturnValue(at + 30); sender.resume();
    try {
      await vi.waitFor(() => expect(commands(mesh)).toHaveLength(2));
      expect(commands(mesh).map(event => (event.data as { requestCreatedAt: number }).requestCreatedAt)).toEqual([at + 10, at + 10]);
      restarted.resume();
      expect(await pending).toMatchObject({ acknowledged: true });
      expect(handler).toHaveBeenCalledOnce();
      expect(mesh.listAll("topology/control-seen/")).toHaveLength(2);
    } finally { restarted.resume(); await pending; clock.mockRestore(); }
  });

  it.each([-1, 0, 1])("refuses origin-less native legacy controls even beyond the former maximum window (boundary %s)", async boundary => {
    const mesh = store(temp()); const at = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    const owner = plane(mesh, "session:owner"); const handler = vi.fn(() => ({ accepted: true })); owner.start(handler);
    const requestedAt = at + 24 * 60 * 60 * 1_000 + 60_000 + boundary;
    clock.mockReturnValue(requestedAt);
    await mesh.publish({ topic: "fabric.control.command", kind: "stop", from: identity("session:sender"), to: "session:owner",
      data: { version: 1, commandId: "legacy-window", targetId: "actor:target", operation: "stop", replyTo: "session:sender", requestedAt, deadlineAt: requestedAt + 5_000 } });
    await vi.waitFor(() => expect(acks(mesh)).toHaveLength(1));
    expect(handler).not.toHaveBeenCalled();
    expect(acks(mesh)[0]!.data).toMatchObject({ accepted: false, errorCode: CONTROL_STALE_INCARNATION, staleIncarnation: null });
  });

  it.each(["stop", "ask", "steer", "followUp"] as const)("refuses fresh native %s without an epoch, with or without an immutable origin", async operation => {
    for (const origin of [true, false]) {
      const mesh = store(temp()); const owner = plane(mesh, "session:owner"); const sender = plane(mesh, "session:sender");
      const handler = vi.fn(() => ({ accepted: true })); owner.start(handler); sender.start(() => ({ accepted: false }));
      const publish = mesh.publish.bind(mesh);
      vi.spyOn(mesh, "publish").mockImplementation(input => {
        if (origin || input.topic !== "fabric.control.command") return publish(input);
        const data = input.data;
        return publish({ ...input, data: (at: number) => {
          const { requestCreatedAt: _origin, ...wire } = (typeof data === "function" ? data(at) : data) as Record<string, unknown>;
          return wire;
        } });
      });
      await new Promise(done => setTimeout(done, 5));
      await expect(sender.request("session:owner", "actor:target", operation, { message: "unverifiable" }))
        .rejects.toMatchObject({ name: "FabricControlStaleIncarnationError", code: CONTROL_STALE_INCARNATION });
      expect(handler).not.toHaveBeenCalled();
      expect(commands(mesh)).toHaveLength(1);
      expect(mesh.listAll("topology/control-seen/")).toEqual([]);
      expect(acks(mesh)[0]!.data).toMatchObject({ accepted: false, errorCode: CONTROL_STALE_INCARNATION, staleIncarnation: null });
      vi.restoreAllMocks();
    }
  });
});

it("publishes, validates and replaces the owner epoch; source records cannot override it", async () => {
  const mesh = store(temp()); const old = directory(mesh, "session:owner"); await old.refresh();
  const viewer = directory(mesh, "session:viewer"); const first = viewer.get("session:owner")!;
  expect(first.ownerIncarnation).toBe(old.ownerIncarnation);
  expect(old.self().ownerIncarnation).toBe(old.ownerIncarnation);
  const replacement = directory(mesh, "session:owner"); await replacement.refresh();
  expect(replacement.ownerIncarnation).not.toBe(old.ownerIncarnation);
  expect(viewer.get("session:owner", undefined, { fresh: true })!.ownerIncarnation).toBe(replacement.ownerIncarnation);
  const entry = mesh.listAll("topology/participants/").find(entry => (entry.value as FabricParticipantRecord).id === "session:owner")!;
  // Isolate shared-state parser validation: the native participant file would otherwise
  // correctly shadow a malformed shared-state copy.
  const validationMesh = store(temp()); const validationViewer = directory(validationMesh, "session:validator");
  for (const ownerIncarnation of ["", "x".repeat(129), 42]) {
    await validationMesh.put({ key: entry.key, value: { ...(entry.value as object), ownerIncarnation }, identity: identity("session:owner") });
    expect(validationViewer.get("session:owner", undefined, { fresh: true })).toBeUndefined();
  }
});

it("actual bridge: a mirrored owner restart refuses the queued epoch and a fresh epoch succeeds exactly once", async () => {
  const root = temp(); const local = store(path.join(root, "hub")); const remote = store(path.join(root, "remote"));
  const hubDirectory = directory(local, "session:hub00000"); const previousDirectory = directory(remote, "session:remote00");
  await hubDirectory.refresh(); await previousDirectory.refresh();
  const previous = plane(remote, "session:remote00", previousDirectory); previous.start(() => ({ accepted: true })); previous.pause();
  const sender = new FabricControlPlane(local, identity("session:hub00000"), { enabled: true, hostId: "session:hub00000", pollMs: 20,
    acknowledgementTimeoutMs: 1_000, readMirroredOwner: (...args) => hubDirectory.mirroredControlOwner(...args) });
  planes.push(sender); sender.start(() => ({ accepted: false }));
  const bridge = new MeshBridge({ localName: "Dev1", remoteName: "Forge", local: new StoreBridgeSide(local, "Forge"),
    remote: new StoreBridgeSide(remote, "Dev1"), cursorPath: path.join(root, "bridge.cursor.json"), presenceMs: 60_000 });
  bridges.push(bridge); await bridge.start(); await bridge.syncPresence();
  const old = hubDirectory.get("session:remote00", undefined, { fresh: true })!;
  expect(old).toMatchObject({ remoteHost: "Forge", ownerIncarnation: previous.incarnation });
  const pending = sender.request("session:remote00", old.id, "stop", { message: "old", ownerIncarnation: old.ownerIncarnation }, old.ownerIdentityId,
    { routedRemoteHost: "Forge" }).catch(error => error);
  await vi.waitFor(() => expect(commands(local)).toHaveLength(1));
  await previous.close();
  const replacementDirectory = directory(remote, "session:remote00"); await replacementDirectory.refresh();
  const replacement = plane(remote, "session:remote00", replacementDirectory); const handler = vi.fn(() => ({ accepted: true, messageId: "fresh" })); replacement.start(handler);
  await bridge.syncPresence(); await bridge.step();
  await vi.waitFor(() => expect(acks(remote)).toHaveLength(1)); await bridge.step();
  expect(await pending).toMatchObject({ code: CONTROL_STALE_INCARNATION }); expect(handler).not.toHaveBeenCalled();
  const original = commands(local)[0]!.data as { ownerIncarnation: string; requestCreatedAt: number; requestedAt: number; deadlineAt: number };
  expect(commands(remote)[0]!.data).toMatchObject({ ownerIncarnation: original.ownerIncarnation,
    requestCreatedAt: original.requestCreatedAt, requestedAt: original.requestedAt, deadlineAt: original.deadlineAt, bridge: { from: "Dev1" } });
  const fresh = hubDirectory.get("session:remote00", undefined, { fresh: true })!;
  expect(fresh.ownerIncarnation).toBe(replacement.incarnation);
  const live = sender.request("session:remote00", fresh.id, "stop", { message: "fresh", ownerIncarnation: fresh.ownerIncarnation }, fresh.ownerIdentityId,
    { routedRemoteHost: "Forge", idempotencyKey: "fresh" });
  await vi.waitFor(() => expect(commands(local)).toHaveLength(2)); await bridge.step();
  await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce()); await bridge.step();
  await expect(live).resolves.toMatchObject({ messageId: "fresh", acknowledged: true });
  await bridge.step(); await bridge.step(); expect(handler).toHaveBeenCalledOnce(); expect(commands(remote)).toHaveLength(2);
});
