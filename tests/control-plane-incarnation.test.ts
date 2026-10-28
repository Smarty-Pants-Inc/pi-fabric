import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane, STALE_INCARNATION_ERROR, type FabricControlCommand } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { controlProviderParticipant } from "../src/topology/provider-participants.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const directories: ParticipantDirectory[] = [];
const bridges: MeshBridge[] = [];
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-incarnation-")); roots.push(root); return root; };
const store = (root: string) => new MeshStore(root, 65_536, 1_000);
const plane = (mesh: MeshStore, id: string, directory?: ParticipantDirectory, controlIncarnationFence?: "warn" | "enforce") => {
  const value = new FabricControlPlane(mesh, identity(id), { enabled: true, hostId: id, pollMs: 20,
    acknowledgementTimeoutMs: 1_000, ...(directory ? { ownerIncarnation: directory.ownerIncarnation } : {}),
    ...(controlIncarnationFence ? { controlIncarnationFence } : {}) });
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

describe("owner incarnation rollout gate", () => {
  it.each([undefined, "warn", "enforce"] as const)("handles an actual epochless legacy sender in mode %s (default warn)", async mode => {
    const mesh = store(temp()); const owner = plane(mesh, "session:owner", undefined, mode);
    const handler = vi.fn(() => ({ accepted: true, messageId: "legacy", result: "answer" }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined); owner.start(handler);
    for (const [index, operation] of (["steer", "followUp", "stop", "ask"] as const).entries()) {
      // Old releases have neither an epoch nor an immutable request origin.
      await mesh.publish({ topic: "fabric.control.command", kind: operation, from: identity("session:legacy"), to: "session:owner",
        data: { version: 1, commandId: `legacy:${operation}`, targetId: "actor:target", operation, replyTo: "session:legacy",
          message: "old sender", requestedAt: Date.now(), deadlineAt: Date.now() + 5_000 } });
      await vi.waitFor(() => expect(acks(mesh)).toHaveLength(index + 1));
      expect(acks(mesh).at(-1)!.data).toMatchObject(mode === "enforce"
        ? { accepted: false, errorCode: CONTROL_STALE_INCARNATION, staleIncarnation: null }
        : { accepted: true, ownerIncarnation: owner.incarnation, messageId: "legacy" });
    }
    expect(handler).toHaveBeenCalledTimes(mode === "enforce" ? 0 : 4);
    expect(mesh.listAll("topology/control-seen/")).toHaveLength(mode === "enforce" ? 0 : 4);
    expect(warn).toHaveBeenCalledTimes(mode === "enforce" ? 0 : 1);
    if (mode !== "enforce") expect(warn.mock.calls[0]![0]).toContain('count=1 sender="session:legacy"');
  });

  it.each(["warn", "enforce"] as const)("refuses explicit stale epochs for every operation in %s mode", async mode => {
    const mesh = store(temp()); const owner = plane(mesh, "session:owner", undefined, mode);
    const handler = vi.fn(() => ({ accepted: true })); const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    owner.start(handler);
    for (const [index, operation] of (["steer", "followUp", "stop", "ask"] as const).entries()) {
      await mesh.publish({ topic: "fabric.control.command", kind: operation, from: identity("session:stale"), to: "session:owner",
        data: { version: 1, commandId: `stale:${operation}`, targetId: "actor:target", operation, replyTo: "session:stale",
          ownerIncarnation: "predecessor:epoch", requestedAt: Date.now(), deadlineAt: Date.now() + 5_000 } });
      await vi.waitFor(() => expect(acks(mesh)).toHaveLength(index + 1));
      expect(acks(mesh).at(-1)!.data).toMatchObject({ accepted: false, errorCode: CONTROL_STALE_INCARNATION, staleIncarnation: "predecessor:epoch" });
    }
    expect(handler).not.toHaveBeenCalled(); expect(warn).not.toHaveBeenCalled();
    expect(mesh.listAll("topology/control-seen/")).toEqual([]);
  });

  it("counts real dispatches, rate-limits by envelope sender for one hour, and does not count duplicate outcomes", async () => {
    const mesh = store(temp()); const owner = plane(mesh, "session:owner");
    const handler = vi.fn(() => ({ accepted: true })); const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const at = Date.now(); const clock = vi.spyOn(Date, "now").mockReturnValue(at); owner.start(handler);
    let published = 0;
    const send = async (id: string, commandId: string) => {
      await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: identity(id), to: "session:owner",
        data: { version: 1, commandId, targetId: "actor:target", operation: "steer", replyTo: id, requestedAt: Date.now(), deadlineAt: Date.now() + 5_000,
          data: { sender: "forged:sender", ownerIncarnation: "forged:epoch" } } });
      published++;
      await vi.waitFor(() => expect(acks(mesh)).toHaveLength(published));
    };
    await send("session:a", "a:1"); await send("session:a", "a:2");
    expect(warn).toHaveBeenCalledOnce();
    await send("session:a", "a:2"); expect(handler).toHaveBeenCalledTimes(2);
    await send("session:b", "b:1"); expect(warn).toHaveBeenCalledTimes(2);
    clock.mockReturnValue(at + 60 * 60 * 1_000 - 1);
    await send("session:a", "a:3"); expect(warn).toHaveBeenCalledTimes(2);
    clock.mockReturnValue(at + 60 * 60 * 1_000);
    await send("session:a", "a:4"); expect(warn).toHaveBeenCalledTimes(3);
    expect(warn.mock.calls[0]![0]).toContain('count=1 sender="session:a"');
    expect(warn.mock.calls[1]![0]).toContain('count=1 sender="session:b"');
    expect(warn.mock.calls[2]![0]).toContain('count=4 sender="session:a"');
    expect(warn.mock.calls.flat().join(" ")).not.toContain("forged:sender");
  });
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

  it.each(["steer", "followUp"] as const)("refuses unclaimed fenced %s across Main/actor reload, then delivers a fresh epoch once", async operation => {
    for (const targetId of ["session:owner", "actor:target"]) {
      const mesh = store(temp());
      const previous = plane(mesh, "session:owner"); previous.start(() => ({ accepted: true })); previous.pause();
      const sender = plane(mesh, "session:sender"); sender.start(() => ({ accepted: false }));
      const pending = sender.request("session:owner", targetId, operation,
        { message: "same session", ownerIncarnation: previous.incarnation }, "session:owner", { idempotencyKey: "same" }).catch(error => error);
      await vi.waitFor(() => expect(commands(mesh)).toHaveLength(1)); await previous.close();
      const restarted = plane(mesh, "session:owner");
      const handler = vi.fn(() => ({ accepted: true, messageId: "once" })); restarted.start(handler);
      expect(await pending).toMatchObject({ name: "FabricControlStaleIncarnationError", code: CONTROL_STALE_INCARNATION });
      expect(handler).not.toHaveBeenCalled();
      expect(mesh.listAll("topology/control-seen/")).toEqual([]);
      expect(commands(mesh)).toHaveLength(1);
      for (let i = 0; i < 2; i++) await expect(sender.request("session:owner", targetId, operation,
        { message: "fresh decision", ownerIncarnation: restarted.incarnation }, "session:owner", { idempotencyKey: "fresh" }))
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
    const owner = plane(mesh, "session:owner", undefined, "enforce"); const handler = vi.fn(() => ({ accepted: true })); owner.start(handler);
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
      // Strip the epoch AFTER sender admission to model an old/adversarial wire producer.
      return publish({ ...input, data: (committedAt: number) => {
        const { ownerIncarnation: _epoch, ...unbound } = (typeof data === "function" ? data(committedAt) : data) as Record<string, unknown>;
        if (wire !== "legacy") return unbound;
        const { requestCreatedAt: _origin, ...legacy } = unbound;
        return legacy;
      } });
    });
    clock.mockReturnValue(at + 10);
    const pending = sender.request("session:owner", "actor:target", "stop", { message: "created before reload", ownerIncarnation: previous.incarnation }).catch(error => error);
    expect(commands(mesh)).toHaveLength(0);
    clock.mockReturnValue(at + 20);
    const restarted = plane(mesh, "session:owner", undefined, "enforce");
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

  it("keeps request origin through a proven-notRun resend and refuses the stale epoch after reload", async () => {
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
      expect(await pending).toMatchObject({ code: CONTROL_STALE_INCARNATION });
      expect(handler).not.toHaveBeenCalled();
      expect(mesh.listAll("topology/control-seen/")).toHaveLength(1);
    } finally { restarted.resume(); await pending; clock.mockRestore(); }
  });

  it.each([-1, 0, 1])("refuses origin-less native legacy controls even beyond the former maximum window (boundary %s)", async boundary => {
    const mesh = store(temp()); const at = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    const owner = plane(mesh, "session:owner", undefined, "enforce"); const handler = vi.fn(() => ({ accepted: true })); owner.start(handler);
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
      const mesh = store(temp()); const owner = plane(mesh, "session:owner", undefined, "enforce"); const sender = plane(mesh, "session:sender");
      const handler = vi.fn(() => ({ accepted: true })); owner.start(handler); sender.start(() => ({ accepted: false }));
      const publish = mesh.publish.bind(mesh);
      vi.spyOn(mesh, "publish").mockImplementation(input => {
        if (input.topic !== "fabric.control.command") return publish(input);
        const data = input.data;
        return publish({ ...input, data: (at: number) => {
          // Receiver regression: corrupt the admitted wire, not the now-fenced sender input.
          const { ownerIncarnation: _epoch, ...unbound } = (typeof data === "function" ? data(at) : data) as Record<string, unknown>;
          if (origin) return unbound;
          const { requestCreatedAt: _origin, ...wire } = unbound;
          return wire;
        } });
      });
      await new Promise(done => setTimeout(done, 5));
      await expect(sender.request("session:owner", "actor:target", operation, { message: "unverifiable", ownerIncarnation: owner.incarnation }))
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

// Keep reason: late expiry confirmation crosses the file claim, bridge and ACK trust boundaries.
describe("late bridged message confirmation", () => {
  const gate = () => {
    let open!: () => void;
    const promise = new Promise<void>(resolve => { open = resolve; });
    return { promise, open };
  };
  const until = (check: () => void) => vi.waitFor(check, { timeout: 3_000, interval: 20 });
  const setup = async (operation: "steer" | "followUp" = "followUp", accepted = true) => {
    const root = temp(), local = store(path.join(root, "local")), remote = store(path.join(root, "remote"));
    const senderId = "session:sender0000", ownerId = "session:owner00000";
    const localDirectory = directory(local, senderId), remoteDirectory = directory(remote, ownerId);
    await localDirectory.refresh(); await remoteDirectory.refresh();
    const owner = plane(remote, ownerId, remoteDirectory);
    const sender = new FabricControlPlane(local, identity(senderId), { enabled: true, hostId: senderId, pollMs: 20,
      acknowledgementTimeoutMs: 5_000,
      // Hold the source's healthy route constant while independently delaying command/ACK progress.
      readMirroredOwner: () => ({ remoteHost: "Remote", expiresAt: Date.now() + 120_000, ownerIncarnation: owner.incarnation }) });
    planes.push(sender); sender.start(() => ({ accepted: false }));
    const receive = vi.fn(() => accepted ? { accepted: true, messageId: "delivered-once" }
      : { accepted: false, error: "handler rejected after execution" });
    const bridge = new MeshBridge({ localName: "Local", remoteName: "Remote", local: new StoreBridgeSide(local, "Remote"),
      remote: new StoreBridgeSide(remote, "Local"), cursorPath: path.join(root, "bridge.cursor.json"), presenceMs: 60_000 });
    bridges.push(bridge); await bridge.start();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const resendLogs = () => warning.mock.calls.map(args => args.join(" ")).filter(line => line.includes("Control notRun resend"));
    const startedAt = Date.now();
    const advanceTo = (elapsedMs: number) => vi.advanceTimersByTimeAsync(startedAt + elapsedMs - Date.now());
    const step = async () => {
      // Real presence and bridge authority are refreshed before each manually driven transport pass.
      await localDirectory.refresh(); await remoteDirectory.refresh();
      await bridge.syncPresence(); await bridge.step(); await vi.advanceTimersByTimeAsync(0);
    };
    const messages = (mesh = local) => commands(mesh).filter(event => event.kind === operation);
    const send = (signal?: AbortSignal) => {
      let settled = false;
      const outcome = sender.request(ownerId, ownerId, operation, { message: "deliver once", ownerIncarnation: owner.incarnation }, ownerId,
        { routedRemoteHost: "Remote", idempotencyKey: "late-confirmation", ...(signal ? { signal } : {}) }).then(
        value => ({ value, error: undefined }), (error: Error) => ({ value: undefined, error }),
      ).then(result => { settled = true; return result; });
      return { outcome, settled: () => settled };
    };
    const holdClaims = (count: number) => {
      const held = Array.from({ length: count }, gate), put = remote.put.bind(remote);
      let entered = 0;
      vi.spyOn(remote, "put").mockImplementation(async input => {
        const result = await put(input); // The real create-only shared claim commits first.
        if (input.key.startsWith("topology/control-seen/") && input.ifVersion === 0 && entered < held.length) {
          const pause = held[entered++]!;
          await pause.promise; // Model a paused owner after CAS, before its deadline recheck.
        }
        return result;
      });
      return { held, entered: () => entered, release: () => held.forEach(pause => pause.open()) };
    };
    return { local, remote, owner, sender, receive, operation, senderId, ownerId, startedAt, advanceTo, step, messages, send, holdClaims, resendLogs };
  };

  it.each([
    { operation: "steer", renew: false }, { operation: "steer", renew: true },
    { operation: "followUp", renew: false }, { operation: "followUp", renew: true },
  ] as const)("late notRun resends $operation exactly once with a fresh deadline and one handler delivery (commit renewal=$renew)", async ({ operation, renew }) => {
    const f = await setup(operation), claims = f.holdClaims(1);
    f.owner.start(f.receive);
    const request = f.send();
    try {
      await until(() => expect(f.messages()).toHaveLength(1));
      await f.advanceTo(29_700); // Cross the real bridge with less than one second left.
      await f.step(); expect(f.messages(f.remote)).toHaveLength(1);
      await until(() => expect(claims.entered()).toBe(1));
      const first = f.messages()[0]!.data as FabricControlCommand;
      expect(first.deadlineAt! - first.requestedAt).toBe(30_000);
      await f.advanceTo(45_000);
      expect(request.settled()).toBe(false);
      await f.advanceTo(46_000); claims.release();
      await until(() => expect(acks(f.remote)).toHaveLength(1));
      expect(acks(f.remote)[0]!.data).toMatchObject({ accepted: false, notRun: true, error: "Fabric control command expired" });
      expect(f.receive).not.toHaveBeenCalled(); // The post-CAS owner deadline fence really ran.
      // The retry passes admission, then sees a lapsed mirror under the publication
      // lock. Renewal of the same route/epoch must retain THIS attempt's wait budget,
      // not inherit the logical origin's already exhausted admission/ACK window.
      const readMirror = f.sender.options.readMirroredOwner!;
      let mirrorReads = 0;
      f.sender.options.readMirroredOwner = (...args) => {
        const current = readMirror(...args)!;
        return ++mirrorReads === 2 && renew ? { ...current, expiresAt: Date.now() - 1 } : current;
      };
      await f.step(); await vi.advanceTimersByTimeAsync(100);
      await until(() => expect(f.messages()).toHaveLength(2));
      const second = f.messages()[1]!.data as FabricControlCommand;
      expect(mirrorReads).toBeGreaterThanOrEqual(renew ? 4 : 2);
      expect(second.deadlineAt! - Date.now()).toBeGreaterThan(29_000);
      expect(request.settled()).toBe(false);
      expect(second.requestedAt).toBeGreaterThan(first.deadlineAt!);
      expect(second.deadlineAt! - second.requestedAt).toBe(29_950);
      expect(second).toMatchObject({ requestCreatedAt: first.requestCreatedAt, ownerIncarnation: first.ownerIncarnation,
        destinationRemoteHost: "Remote", message: first.message });
      // Both stable attempt IDs derive from the same caller key, not two logical sends.
      for (const [attempt, command] of [first, second].entries()) expect(command.commandId).toBe(createHash("sha256")
        .update(JSON.stringify([f.senderId, f.ownerId, f.ownerId, f.ownerId, operation, "late-confirmation", attempt])).digest("hex"));
      await f.step(); await until(() => expect(f.receive).toHaveBeenCalledOnce());
      await until(() => expect(acks(f.remote)).toHaveLength(2)); await f.step();
      expect(await request.outcome).toMatchObject({ value: { acknowledged: true, messageId: "delivered-once" } });
      await vi.advanceTimersByTimeAsync(60_000); await f.step();
      expect(f.messages()).toHaveLength(2); expect(f.messages(f.remote)).toHaveLength(2);
      expect(f.receive).toHaveBeenCalledOnce();
      expect(commands(f.local).filter(event => event.kind === "cancel")).toEqual([]);
      expect(f.resendLogs()).toEqual([expect.stringContaining("outcome=retry"), expect.stringContaining("outcome=accepted")]);
    } finally { claims.release(); await vi.advanceTimersByTimeAsync(0); }
  });

  it.each([true, false])("late executed outcome (accepted=%s) stays unknown without retry", async accepted => {
    const f = await setup("followUp", accepted), release = gate(), publish = f.remote.publish.bind(f.remote);
    vi.spyOn(f.remote, "publish").mockImplementation(async input => {
      if (input.topic === "fabric.control.ack") await release.promise;
      return publish(input);
    });
    f.owner.start(f.receive);
    const request = f.send();
    try {
      await until(() => expect(f.messages()).toHaveLength(1)); await f.step();
      await until(() => expect(f.receive).toHaveBeenCalledOnce());
      await f.advanceTo(45_000); expect(request.settled()).toBe(false);
      await f.advanceTo(46_000); release.open();
      await until(() => expect(acks(f.remote)).toHaveLength(1)); await f.step();
      await until(() => expect(request.settled()).toBe(true));
      const result = await request.outcome;
      expect(result.value).toBeUndefined();
      expect(result.error?.message).toContain("the outcome is unknown");
      expect(acks(f.remote)[0]!.data).not.toHaveProperty("notRun");
      await vi.advanceTimersByTimeAsync(60_000); await f.step();
      expect(f.messages()).toHaveLength(1); expect(f.receive).toHaveBeenCalledOnce();
      expect(f.resendLogs()).toEqual([]);
    } finally { release.open(); await vi.advanceTimersByTimeAsync(0); }
  });

  it("no ACK ends with bounded unknown and a later notRun cannot revive it", async () => {
    const f = await setup(); f.owner.start(f.receive);
    const request = f.send();
    await until(() => expect(f.messages()).toHaveLength(1));
    await f.advanceTo(45_000); expect(request.settled()).toBe(false);
    await f.advanceTo(49_999); expect(request.settled()).toBe(false);
    await f.advanceTo(50_000); expect(request.settled()).toBe(true);
    const result = await request.outcome;
    expect(result.error?.message).toContain("the outcome is unknown");
    expect(result.error).not.toHaveProperty("notRun");
    expect(result.error).toHaveProperty("idempotencyKey", "late-confirmation");
    await vi.advanceTimersByTimeAsync(1); await f.step();
    await until(() => expect(acks(f.remote)).toHaveLength(1)); await f.step();
    expect(acks(f.remote)[0]!.data).toMatchObject({ accepted: false, notRun: true });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.messages()).toHaveLength(1); expect(f.receive).not.toHaveBeenCalled();
    expect(commands(f.local).filter(event => event.kind === "cancel")).toHaveLength(1);
    expect(await request.outcome).toBe(result);
    expect(f.resendLogs()).toEqual([]);
  });

  it.each(["abort", "close"] as const)("%s during late confirmation prevents a late notRun resend", async winner => {
    const f = await setup(), controller = new AbortController(); f.owner.start(f.receive);
    const request = f.send(controller.signal);
    await until(() => expect(f.messages()).toHaveLength(1));
    await f.advanceTo(45_000); expect(request.settled()).toBe(false);
    if (winner === "abort") controller.abort();
    else await f.sender.close();
    expect((await request.outcome).error?.message).toContain(winner === "abort" ? "cancelled" : "closed");
    await f.advanceTo(46_000); await f.step();
    await until(() => expect(acks(f.remote)).toHaveLength(1)); await f.step();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.messages()).toHaveLength(1); expect(f.receive).not.toHaveBeenCalled();
    expect(f.resendLogs()).toEqual([]);
  });

  it("ignores wrong owner, route, incarnation and target proofs during late confirmation", async () => {
    const f = await setup(), request = f.send();
    await until(() => expect(f.messages()).toHaveLength(1));
    const command = f.messages()[0]!.data as FabricControlCommand;
    await f.advanceTo(45_000);
    const proof = { version: 1, commandId: command.commandId, targetId: f.ownerId, ownerIncarnation: f.owner.incarnation,
      accepted: false, error: "Fabric control command expired", notRun: true, bridge: { from: "Remote" } };
    for (const [from, data] of [[identity("session:unrelated"), proof], [identity(f.ownerId), { ...proof, bridge: { from: "Other" } }],
      [identity(f.ownerId), { ...proof, ownerIncarnation: "unrelated" }], [identity(f.ownerId), { ...proof, targetId: "session:unrelated" }]] as const) {
      await f.local.publish({ topic: "fabric.control.ack", kind: "rejected", from, to: f.senderId, data });
    }
    // This valid ordinary ACK is a reader barrier behind every forged proof, not permission to retry.
    await f.local.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity(f.ownerId), to: f.senderId,
      data: { ...proof, accepted: true, notRun: undefined, messageId: "too-late" } });
    await until(() => expect(request.settled()).toBe(true));
    expect((await request.outcome).error?.message).toContain("the outcome is unknown");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.messages()).toHaveLength(1); expect(f.resendLogs()).toEqual([]);
  });

  it("a second late notRun cannot trigger a third attempt", async () => {
    const f = await setup(), claims = f.holdClaims(2);
    f.owner.start(f.receive);
    const request = f.send();
    try {
      await until(() => expect(f.messages()).toHaveLength(1)); await f.step();
      await until(() => expect(claims.entered()).toBe(1));
      await f.advanceTo(46_000); claims.held[0]!.open();
      await until(() => expect(acks(f.remote)).toHaveLength(1)); await f.step();
      await vi.advanceTimersByTimeAsync(100); await until(() => expect(f.messages()).toHaveLength(2));
      await f.step(); await until(() => expect(claims.entered()).toBe(2));
      const second = f.messages()[1]!.data as FabricControlCommand;
      await f.advanceTo(second.requestedAt - f.startedAt + 46_000);
      expect(request.settled()).toBe(false);
      claims.held[1]!.open(); await until(() => expect(acks(f.remote)).toHaveLength(2)); await f.step();
      await until(() => expect(request.settled()).toBe(true));
      expect((await request.outcome).error).toMatchObject({ notRun: true, message: "Fabric control command expired; not delivered, safe to resend." });
      await vi.advanceTimersByTimeAsync(60_000); await f.step();
      expect(f.messages()).toHaveLength(2); expect(f.messages(f.remote)).toHaveLength(2);
      expect(f.receive).not.toHaveBeenCalled();
      expect(f.resendLogs()).toEqual([expect.stringContaining("outcome=retry"), expect.stringContaining("outcome=not-run")]);
    } finally { claims.release(); await vi.advanceTimersByTimeAsync(0); }
  });
});

// Upstream 0.110.1 (a8d2714b) owner-incarnation cases, fitted to main's per-activation
// epoch (#709): the participant directory owns the epoch and its control plane adopts it.
describe("upstream owner incarnation cases", () => {
  const upMesh = (): string => path.join(temp(), "mesh");
  const upStore = (meshRoot: string): MeshStore => new MeshStore(meshRoot, 64 * 1024, 1_000);
  const upPlane = (meshRoot: string, id: string, ownerIncarnation?: string): FabricControlPlane => {
    const value = new FabricControlPlane(upStore(meshRoot), identity(id), {
      enabled: true,
      hostId: id,
      pollMs: 20,
      acknowledgementTimeoutMs: 1_000,
      ...(ownerIncarnation ? { ownerIncarnation } : {}),
    });
    planes.push(value);
    return value;
  };
  const upProviderRecord = (id: string, hostId: string): FabricParticipantRecord => ({
    format: 1,
    id,
    kind: "provider",
    rootId: hostId,
    ownerHostId: hostId,
    ownerIdentityId: hostId,
    parentId: hostId,
    name: "demo",
    status: "running",
    provider: "demo",
    transport: "host",
    capabilities: ["stop", "steer"],
    startedAt: 1,
    updatedAt: 1,
    controlProtocol: "v1",
  });
  /** A directory that never heartbeats, so a test controls when records change. */
  const upOwnerDirectory = (meshRoot: string, hostId: string, records: () => FabricParticipantRecord[]): ParticipantDirectory => {
    const value = new ParticipantDirectory(upStore(meshRoot), { enabled: true, hostId, rootId: hostId, identity: identity(hostId) });
    value.registerSource(records);
    directories.push(value);
    return value;
  };
  const upViewer = (meshRoot: string, hostId: string): ParticipantDirectory => {
    const value = new ParticipantDirectory(upStore(meshRoot), { enabled: true, hostId, rootId: hostId, identity: identity(hostId) });
    directories.push(value);
    return value;
  };

  it("gives every plane instance a distinct incarnation", () => {
    const meshRoot = upMesh();
    const first = upPlane(meshRoot, "host:owner");
    const second = upPlane(meshRoot, "host:owner");
    expect(first.incarnation).toMatch(/^[0-9a-f-]{36}$/);
    expect(second.incarnation).not.toBe(first.incarnation);
  });

  it("executes a command addressed to the current incarnation", async () => {
    const meshRoot = upMesh();
    const sender = upPlane(meshRoot, "host:sender");
    const owner = upPlane(meshRoot, "host:owner");
    const handler = vi.fn((command: { commandId: string }) => ({ accepted: true, messageId: "local:" + command.commandId }));
    sender.start(() => ({ accepted: false }));
    owner.start(handler);
    await expect(
      sender.request("host:owner", "agent:target", "steer", { message: "focus", ownerIncarnation: owner.incarnation }),
    ).resolves.toMatchObject({ acknowledged: true, messageId: expect.stringMatching(/^local:/) });
    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ ownerIncarnation: owner.incarnation }),
      expect.anything(),
      expect.anything(),
      // Our control plane also passes the admitted verification (mesh or bridge).
      expect.anything(),
    );
    expect(upStore(meshRoot).read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({ data: expect.objectContaining({ accepted: true, ownerIncarnation: owner.incarnation }) }),
    );
  });

  it("refuses a replayed command naming a previous incarnation without executing it", async () => {
    const meshRoot = upMesh();
    const mesh = upStore(meshRoot);
    const previous = upPlane(meshRoot, "host:owner");
    const previousIncarnation = previous.incarnation;
    await previous.close();
    // Published to the old process, never claimed; the new process replays from offset 0.
    await mesh.publish({
      topic: "fabric.control.command",
      kind: "stop",
      from: identity("host:sender"),
      to: "host:owner",
      data: {
        version: 1,
        commandId: "command:stale",
        targetId: "actor:durable",
        operation: "stop",
        replyTo: "host:sender",
        ownerIncarnation: previousIncarnation,
        requestedAt: Date.now(),
      },
    });
    const restarted = upPlane(meshRoot, "host:owner");
    const handler = vi.fn(() => ({ accepted: true }));
    restarted.start(handler);
    await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(1));
    expect(handler).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:stale",
          accepted: false,
          error: STALE_INCARNATION_ERROR,
          errorCode: CONTROL_STALE_INCARNATION,
          ownerIncarnation: restarted.incarnation,
          staleIncarnation: previousIncarnation,
        }),
      }),
    );
    expect(mesh.listAll("topology/control-seen/")).toEqual([]);
  });

  it("fails a live request to a restarted owner fast with the fencing error", async () => {
    const meshRoot = upMesh();
    const sender = upPlane(meshRoot, "host:sender");
    const previous = upPlane(meshRoot, "host:owner");
    const previousIncarnation = previous.incarnation;
    await previous.close();
    const restarted = upPlane(meshRoot, "host:owner");
    const handler = vi.fn(() => ({ accepted: true }));
    sender.start(() => ({ accepted: false }));
    restarted.start(handler);
    const startedAt = Date.now();
    await expect(
      sender.requestResult("host:owner", "actor:durable", "ask", { message: "late", ownerIncarnation: previousIncarnation }),
    ).rejects.toThrow(STALE_INCARNATION_ERROR);
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(handler).not.toHaveBeenCalled();
  });

  it("still executes commands that name no incarnation after a restart (warn rollout)", async () => {
    const meshRoot = upMesh();
    const sender = upPlane(meshRoot, "host:sender");
    await upPlane(meshRoot, "host:owner").close();
    const restarted = upPlane(meshRoot, "host:owner");
    const handler = vi.fn((_command: { ownerIncarnation?: string | undefined }) => ({ accepted: true, messageId: "legacy" }));
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    sender.start(() => ({ accepted: false }));
    restarted.start(handler);
    await expect(
      sender.request("host:owner", "agent:target", "followUp", { message: "older sender" }),
    ).resolves.toMatchObject({ messageId: "legacy" });
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0]![0]).not.toHaveProperty("ownerIncarnation");
  });

  it("ignores an acknowledgement from a different incarnation of the owner", async () => {
    const meshRoot = upMesh();
    const mesh = upStore(meshRoot);
    const sender = upPlane(meshRoot, "host:sender");
    const owner = upPlane(meshRoot, "host:owner");
    sender.start(() => ({ accepted: false }));
    owner.start(async (command) => {
      // Same identity, other incarnation: neither a result nor a refusal of this target counts.
      for (const extra of [
        { accepted: true, messageId: "other-process" },
        { accepted: false, error: "not ours", staleIncarnation: "incarnation:unrelated" },
      ]) {
        await mesh.publish({
          topic: "fabric.control.ack",
          kind: extra.accepted ? "accepted" : "rejected",
          from: identity("host:owner"),
          to: "host:sender",
          data: { version: 1, commandId: command.commandId, targetId: command.targetId, ownerIncarnation: "incarnation:other", ...extra },
        });
      }
      return { accepted: true, messageId: "current:" + command.commandId };
    });
    await expect(
      sender.request("host:owner", "agent:target", "steer", { message: "focus", ownerIncarnation: owner.incarnation }),
    ).resolves.toMatchObject({ messageId: expect.stringMatching(/^current:/) });
  });

  it("rejects a malformed incarnation before publishing", async () => {
    const meshRoot = upMesh();
    const sender = upPlane(meshRoot, "host:sender");
    sender.start(() => ({ accepted: false }));
    await expect(
      sender.request("host:owner", "agent:target", "stop", { ownerIncarnation: "x".repeat(129) }),
    ).rejects.toThrow("Invalid Fabric owner incarnation");
    expect(upStore(meshRoot).read({ topic: "fabric.control.command", limit: 10 })).toEqual([]);
  });

  it("stamps, validates and re-resolves a provider participant's incarnation across an owner restart", async () => {
    const meshRoot = upMesh();
    const ref = "provider:demo:1";
    const requester = upPlane(meshRoot, "host:requester");
    requester.start(() => ({ accepted: false }));
    const viewer = upViewer(meshRoot, "host:requester");
    const previousDirectory = upOwnerDirectory(meshRoot, "host:owner", () => [upProviderRecord(ref, "host:owner")]);
    const previous = upPlane(meshRoot, "host:owner", previousDirectory.ownerIncarnation);
    await previousDirectory.refresh();
    expect(viewer.get(ref)).toMatchObject({ ownerIncarnation: previous.incarnation });
    // The owner crashes: its records outlive the process until the new one republishes.
    await previous.close();

    const restartedDirectory = upOwnerDirectory(meshRoot, "host:owner", () => [upProviderRecord(ref, "host:owner")]);
    const restarted = upPlane(meshRoot, "host:owner", restartedDirectory.ownerIncarnation);
    expect(restarted.incarnation).not.toBe(previous.incarnation);
    const handler = vi.fn(() => ({ accepted: true, result: { stopped: true } }));
    restarted.start(handler);
    await expect(controlProviderParticipant(undefined, viewer, requester, ref, "stop")).rejects.toThrow(STALE_INCARNATION_ERROR);
    expect(handler).not.toHaveBeenCalled();

    await restartedDirectory.refresh();
    expect(viewer.get(ref)).toMatchObject({ ownerIncarnation: restarted.incarnation });
    await expect(controlProviderParticipant(undefined, viewer, requester, ref, "stop")).resolves.toMatchObject({ acknowledged: true });
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("keeps records without an incarnation valid and drops malformed ones", async () => {
    const meshRoot = upMesh();
    const mesh = upStore(meshRoot);
    await upOwnerDirectory(meshRoot, "host:legacy", () => [upProviderRecord("provider:legacy:1", "host:legacy")]).refresh();
    const entry = mesh.listAll("topology/participants/")
      .find((candidate) => (candidate.value as { id?: string }).id === "provider:legacy:1")!;
    // An older owner's record: no epoch at all.
    const { ownerIncarnation: _epoch, ...legacyValue } = entry.value as Record<string, unknown>;
    const legacyEntry = await mesh.put({ key: entry.key, value: legacyValue, identity: identity("host:legacy"), ifVersion: entry.version });
    const viewer = upViewer(meshRoot, "host:viewer");
    const legacy = viewer.get("provider:legacy:1");
    expect(legacy).toBeDefined();
    expect(legacy).not.toHaveProperty("ownerIncarnation");
    await mesh.put({ key: entry.key, value: { ...legacyValue, ownerIncarnation: 42 }, identity: identity("host:legacy"), ifVersion: legacyEntry.version });
    expect(viewer.get("provider:legacy:1")).toBeUndefined();
  });

  it("does not let a source record override the owner's incarnation", async () => {
    const meshRoot = upMesh();
    const owner = upOwnerDirectory(meshRoot, "host:owner", () => [
      { ...upProviderRecord("provider:demo:2", "host:owner"), ownerIncarnation: "incarnation:forged" },
    ]);
    await owner.refresh();
    expect(upViewer(meshRoot, "host:viewer").get("provider:demo:2")).toMatchObject({ ownerIncarnation: owner.ownerIncarnation });
  });
});
