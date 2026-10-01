import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MeshStore,
  type MeshIdentity,
  type MeshEvent,
  type MeshStoreOptions,
} from "../src/mesh/store.js";
import { CONTROL_CLAIMS_POLICY_KEY, FabricControlPlane, type FabricControlCommand, type FabricControlPlaneOptions } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { createMainExecutionCeilingError } from "../src/async-settlement.js";
import { removeHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];

const identity = (id: string): MeshIdentity => ({
  id,
  name: id,
  kind: "main",
  sessionId: id,
});

const plane = (
  meshRoot: string,
  id: string,
  storeOptions: MeshStoreOptions = {},
  controlOptions: Pick<FabricControlPlaneOptions, "pollMs" | "acknowledgementTimeoutMs" | "readMirroredOwner"> = {},
): FabricControlPlane => {
  const value = new FabricControlPlane(
    new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions),
    identity(id),
    {
      enabled: true,
      hostId: id,
      pollMs: 20,
      acknowledgementTimeoutMs: 1_000,
      ...controlOptions,
    },
  );
  planes.push(value);
  return value;
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map((value) => value.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("FabricControlPlane", () => {
  it("retries cancellation beyond the production 10-second lock timeout with commit-time ACK timing", { timeout: 25_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "control-default-lock-")); roots.push(root);
    // No store timeout override and no ACK override: production defaults are 10s and 5s.
    const sender = new FabricControlPlane(new MeshStore(path.join(root, "mesh"), 65536, 1000), identity("session:sender"), { enabled: true, hostId: "session:sender" });
    planes.push(sender);
    const controller = new AbortController();
    const observed = sender.requestResult("session:owner", "actor:target", "ask", {}, "session:owner", { signal: controller.signal }).catch(error => error);
    await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(1));
    const lock = path.join(sender.mesh.root, ".lock");
    fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), `cancel-test\n${process.pid}\n${Date.now()}\n`);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const abortedAt = Date.now();
    try {
      controller.abort();
      expect(await observed).toMatchObject({ message: expect.stringContaining("cancelled") });
      await sender.close(); // final retry obligation must survive control-plane closure too
      await vi.waitFor(() => expect(warn.mock.calls.some(call => String(call[0]).includes("control cancellation: mesh lock timeout"))).toBe(true), { timeout: 12_000, interval: 25 });
      expect(Date.now() - abortedAt).toBeGreaterThanOrEqual(10_000);
      const releasedAt = Date.now(); fs.rmSync(lock, { recursive: true });
      await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" }).filter(event => event.kind === "cancel")).toHaveLength(1), { timeout: 3_000 });
      const event = sender.mesh.read({ topic: "fabric.control.command" }).find(event => event.kind === "cancel")!;
      const command = event.data as FabricControlCommand;
      expect(command.requestedAt).toBeGreaterThanOrEqual(releasedAt);
      expect(command.requestedAt).toBe(event.createdAt);
      expect(command.deadlineAt! - command.requestedAt).toBe(5_000);
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(sender.mesh.read({ topic: "fabric.control.command" }).filter(event => event.kind === "cancel")).toHaveLength(1);
    } finally { fs.rmSync(lock, { recursive: true, force: true }); warn.mockRestore(); }
  });
  it.each(["Escape", "ordinary timeout", "forged ceiling", "cloned ceiling", "ceiling without policy"])("still cancels the owner for %s, never from guest ceiling text", async cause => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-ceiling-")); roots.push(root);
    const owner = plane(path.join(root, "mesh"), "session:owner0000");
    const sender = plane(path.join(root, "mesh"), "session:sender000");
    const aborted = vi.fn();
    let entered = false;
    owner.start((_command, _from, signal) => new Promise(resolve => {
      entered = true;
      signal!.addEventListener("abort", () => { aborted(); resolve({ accepted: false, error: "owner cancelled" }); }, { once: true });
    }));
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();
    const observation = sender.requestResult("session:owner0000", "actor:target", "ask", { message: "accepted" }, "session:owner0000", { signal: controller.signal, timeoutMs: 5_000, detachOnMainCeiling: cause !== "ceiling without policy" }).catch(error => error);
    await vi.waitFor(() => expect(entered).toBe(true));
    const genuine = createMainExecutionCeilingError(700);
    const reason = cause === "ceiling without policy" ? genuine : cause === "cloned ceiling" ? structuredClone(genuine) : cause === "forged ceiling" ? Object.assign(new Error(genuine.message), { name: "MainExecutionCeilingError" }) : new Error(cause);
    controller.abort(reason);
    expect(await observation).toMatchObject({ message: expect.stringContaining("Remote Fabric request cancelled") });
    await vi.waitFor(() => expect(aborted).toHaveBeenCalledOnce());
    expect(sender.mesh.read({ topic: "fabric.control.command", limit: 20 }).filter(event => event.kind === "cancel")).toHaveLength(1);
  });

  it("retains the remote owner's independent ASK deadline after Main stops observing", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-owner-deadline-")); roots.push(root);
    const owner = plane(path.join(root, "mesh"), "session:owner0000");
    const sender = plane(path.join(root, "mesh"), "session:sender000");
    const aborted = vi.fn(); let entered = false;
    owner.start((_command, _from, signal) => new Promise(resolve => {
      entered = true;
      signal!.addEventListener("abort", () => { aborted(); resolve({ accepted: false, error: "owner budget" }); }, { once: true });
    }));
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();
    const observation = sender.requestResult("session:owner0000", "actor:target", "ask", { message: "accepted" }, "session:owner0000", { signal: controller.signal, timeoutMs: 500, detachOnMainCeiling: true }).catch(error => error);
    await vi.waitFor(() => expect(entered).toBe(true));
    const ceiling = createMainExecutionCeilingError(100); controller.abort(ceiling);
    expect(await observation).toBe(ceiling);
    expect(aborted).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(aborted).toHaveBeenCalledOnce(), { timeout: 2_000 });
    expect(sender.mesh.read({ topic: "fabric.control.command", limit: 20 }).filter(event => event.kind === "cancel")).toHaveLength(0);
  });
  describe("pending mirrored owners", () => {
    const setup = async (timeoutMs = 1_000) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-mirror-"));
      roots.push(root);
      let lease: { remoteHost: string; expiresAt: number } | undefined = {
        remoteHost: "forge", expiresAt: Date.now() + 60_000,
      };
      const readMirroredOwner = vi.fn(() => lease);
      const sender = plane(path.join(root, "mesh"), "host:sender", {}, {
        acknowledgementTimeoutMs: timeoutMs, readMirroredOwner,
      });
      await sender.mesh.put({
        key: "topology/hosts/" + createHash("sha256").update("host:owner").digest("hex"),
        identity: identity("host:owner"), value: { id: "host:owner", remoteHost: "forge" },
      });
      sender.start(() => ({ accepted: false }));
      const events = () => sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      const command = async () => {
        await vi.waitFor(() => expect(events().some((event) => event.kind !== "cancel")).toBe(true));
        return events().find((event) => event.kind !== "cancel")!.data as { commandId: string };
      };
      const ack = async (commandId: string, remoteHost = "forge", from = "identity:owner", targetId = "agent:target") => {
        await sender.mesh.publish({
          topic: "fabric.control.ack", kind: "accepted", from: identity(from), to: "host:sender",
          data: { version: 1, commandId, targetId, accepted: true, messageId: "delivered", result: "done", bridge: { from: remoteHost } },
        });
      };
      const once = async (cancels: number) => {
        await vi.waitFor(() => expect(events().filter((event) => event.kind === "cancel")).toHaveLength(cancels));
        expect(events().filter((event) => event.kind !== "cancel")).toHaveLength(1);
        expect(events().map((event) => (event.data as FabricControlCommand).destinationRemoteHost))
          .toEqual(Array(events().length).fill("forge"));
      };
      return { sender, readMirroredOwner, command, ack, once, setLease: (value: typeof lease) => { lease = value; } };
    };
    const settle = <T>(promise: Promise<T>) => promise.then(
      (value) => ({ value, error: undefined }),
      (error: Error) => ({ value: undefined, error }),
    );
    const unknown = "the outcome is unknown and it may still be delivered, so a retry can deliver it twice";

    it("observes a dynamic lapse after a live send, publishes once and cancels once", async () => {
      const f = await setup();
      const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner"));
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      const { error } = await outcome;
      expect(error?.message).toContain("remote host forge lapsed for agent:target");
      expect(error?.message).toContain("mesh bridge is down or the owner is unavailable");
      expect(error?.message).toContain(unknown);
      expect(error).not.toHaveProperty("notRun");
      expect(f.readMirroredOwner).toHaveBeenCalledWith("host:owner", "identity:owner", "agent:target");
      await f.ack(commandId);
      await f.sender.close();
      await f.once(1);
      const reads = f.readMirroredOwner.mock.calls.length;
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(f.readMirroredOwner).toHaveBeenCalledTimes(reads); // no idle watchdog
    });

    it("keeps waiting beyond the captured expiry when the lease is renewed, then accepts the valid bridge ACK", async () => {
      const f = await setup();
      const oldExpiry = Date.now() + 200;
      f.setLease({ remoteHost: "forge", expiresAt: oldExpiry });
      const outcome = f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner");
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 60_000 });
      const { commandId } = await f.command();
      await new Promise((resolve) => setTimeout(resolve, Math.max(1, oldExpiry - Date.now() + 60)));
      expect(f.readMirroredOwner.mock.calls.length).toBeGreaterThan(1);
      await f.ack(commandId);
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "delivered" });
      await f.once(0);
    });

    it("names a healthy bridge with a lost ACK without claiming link death or retry safety", async () => {
      const f = await setup(100);
      const { error } = await settle(f.sender.request("host:owner", "agent:target", "followUp", {}, "identity:owner"));
      expect(error?.message).toContain("Fabric mesh bridge to remote host forge is not responding for agent:target");
      expect(error?.message).toContain(unknown);
      expect(error?.message).not.toMatch(/lapsed|is down|not run/i);
      expect(error).not.toHaveProperty("notRun");
      await f.once(1);
    }, 15_000);

    it("does not trust wrong bridge, identity or target ACKs while watching a lease", async () => {
      const f = await setup();
      let settled = false;
      const outcome = f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner")
        .finally(() => { settled = true; });
      const { commandId } = await f.command();
      await f.ack(commandId, "other");
      await f.ack(commandId, "forge", "identity:forged");
      await f.ack(commandId, "forge", "identity:owner", "agent:other");
      await new Promise((resolve) => setTimeout(resolve, 80));
      expect(settled).toBe(false);
      await f.ack(commandId);
      await expect(outcome).resolves.toMatchObject({ acknowledged: true });
      await f.once(0);
    });

    it.each(["lapse", "abort", "close"] as const)("settles %s during publish without waiting, then cancels the committed command once", async (winner) => {
      const f = await setup();
      const controller = new AbortController();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return publish(input);
      });
      const outcome = settle(f.sender.requestResult("host:owner", "agent:target", "ask", {}, "identity:owner", { signal: controller.signal }));
      if (winner === "lapse") f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      if (winner === "abort") controller.abort();
      if (winner === "close") await f.sender.close();
      try {
        // This must settle before the original publish gate opens.
        const { error } = await outcome;
        expect(error?.message).toContain(winner === "lapse" ? "lapsed" : winner === "abort" ? "cancelled" : "closed");
        controller.abort();
        await f.sender.close();
      } finally {
        release();
      }
      await f.once(1);
    });

    it("lets a valid ACK win during publish and does not cancel on later lapse, abort or close", async () => {
      const f = await setup();
      const controller = new AbortController();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        const event = await publish(input);
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return event;
      });
      const outcome = f.sender.requestResult("host:owner", "agent:target", "ask", {}, "identity:owner", { signal: controller.signal });
      try {
        const { commandId } = await f.command();
        await f.ack(commandId);
        await expect(outcome).resolves.toBe("done");
        f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
        controller.abort();
        await f.sender.close();
      } finally {
        release();
      }
      await f.once(0);
    });

    it("returns the lapse error without waiting for an in-flight cancellation", async () => {
      const f = await setup();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      const spy = vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        if (input.topic === "fabric.control.command" && input.kind === "cancel") await gate;
        return publish(input);
      });
      const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner"));
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      try {
        const { error } = await outcome;
        expect(error?.message).toContain("lapsed");
        await f.ack(commandId);
        await f.sender.close();
        expect(spy.mock.calls.filter(([input]) => input.kind === "cancel")).toHaveLength(1);
      } finally {
        release();
      }
      await f.once(1);
    });

    it.each(["forge", null] as const)("freezes cancellation destination %s while publication is blocked and ownership changes", async (routedRemoteHost) => {
      const f = await setup();
      if (routedRemoteHost === null) f.setLease(undefined);
      const controller = new AbortController();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return publish(input);
      });
      const outcome = settle(f.sender.requestResult("host:owner", "agent:target", "ask",
        { message: "private", data: { destinationRemoteHost: "ryzen2" } }, "identity:owner",
        { signal: controller.signal, routedRemoteHost }));
      try {
        f.setLease({ remoteHost: "ryzen2", expiresAt: Date.now() + 60_000 });
        controller.abort();
        expect((await outcome).error?.message).toContain("cancelled");
      } finally {
        release();
      }
      const commands = () => f.sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      await vi.waitFor(() => expect(commands()).toHaveLength(2));
      expect(commands().map((event) => event.kind)).toEqual(["ask", "cancel"]);
      expect(commands().map((event) => (event.data as FabricControlCommand).destinationRemoteHost))
        .toEqual([routedRemoteHost, routedRemoteHost]);
      expect((commands()[1]!.data as FabricControlCommand).cancelCommandId)
        .toBe((commands()[0]!.data as FabricControlCommand).commandId);
    });

    it("refuses a known native routing snapshot when fresh admission finds a mirror", async () => {
      const f = await setup();
      const publish = vi.spyOn(f.sender.mesh, "publish");
      await expect(f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner",
        { routedRemoteHost: null })).rejects.toThrow("Fabric native routing is unavailable for agent:target; the routed owner changed; this attempt was not published.");
      expect(publish).not.toHaveBeenCalled();
    });

    it("never retries a known native send into a newly mirrored owner", async () => {
      const f = await setup();
      f.setLease(undefined);
      const outcome = settle(f.sender.request("host:owner", "agent:target", "followUp", {}, "identity:owner",
        { routedRemoteHost: null }));
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "ryzen2", expiresAt: Date.now() + 60_000 });
      await f.sender.mesh.publish({ topic: "fabric.control.ack", kind: "rejected",
        from: identity("identity:owner"), to: "host:sender", data: {
          version: 1, commandId, targetId: "agent:target", accepted: false,
          error: "native notRun", notRun: true,
        } });
      expect((await outcome).error?.message).toContain("Fabric native routing is unavailable");
      const commands = f.sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      expect(commands).toHaveLength(1);
      expect(commands[0]!.data).toMatchObject({ destinationRemoteHost: null });
    });

    it("binds known native commands and ignores foreign stamped notRun after mirror takeover", async () => {
      const f = await setup();
      f.setLease(undefined);
      let settled = false;
      const outcome = f.sender.request("host:owner", "agent:target", "followUp", {}, "identity:owner",
        { routedRemoteHost: null }).finally(() => { settled = true; });
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "ryzen2", expiresAt: Date.now() + 60_000 });
      await f.sender.mesh.put({
        key: "topology/hosts/" + createHash("sha256").update("host:owner").digest("hex"),
        identity: identity("host:owner"), value: { id: "host:owner", remoteHost: "ryzen2" },
      });
      const tail = vi.spyOn(f.sender.mesh, "tail");
      await f.sender.mesh.publish({ topic: "fabric.control.ack", kind: "rejected",
        from: identity("identity:owner"), to: "host:sender", data: {
          version: 1, commandId, targetId: "agent:target", accepted: false,
          error: "foreign notRun", notRun: true, bridge: { from: "ryzen2" },
        } });
      await vi.waitFor(() => expect(tail.mock.results.some((result) => result.type === "return" &&
        result.value.events.some((event) => event.topic === "fabric.control.ack"))).toBe(true));
      tail.mockRestore();
      expect(settled).toBe(false);
      await f.sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted",
        from: identity("identity:owner"), to: "host:sender", data: {
          version: 1, commandId, targetId: "agent:target", accepted: true, messageId: "native",
        } });
      await expect(outcome).resolves.toMatchObject({ messageId: "native" });
      const events = f.sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toMatchObject({ destinationRemoteHost: null });
    });

    it("does not start a watchdog or alter native ACKs when the read port returns no mirror", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-native-"));
      roots.push(root);
      const readMirroredOwner = vi.fn(() => undefined);
      const sender = plane(path.join(root, "mesh"), "host:sender", {}, { readMirroredOwner });
      const owner = plane(path.join(root, "mesh"), "host:owner");
      sender.start(() => ({ accepted: false }));
      owner.start(() => ({ accepted: true, messageId: "native" }));
      await expect(sender.request("host:owner", "agent:target", "steer"))
        .resolves.toMatchObject({ acknowledged: true, messageId: "native" });
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(readMirroredOwner).toHaveBeenCalledTimes(1);
      expect(sender.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]!.data)
        .toMatchObject({ destinationRemoteHost: null });
    });

    it("keeps legacy no-port commands unbound but refuses an unrevalidated explicit mirror", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-legacy-route-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      const owner = plane(meshRoot, "host:owner");
      sender.start(() => ({ accepted: false }));
      owner.start(() => ({ accepted: true }));
      await expect(sender.request("host:owner", "agent:target", "steer"))
        .resolves.toMatchObject({ acknowledged: true });
      expect(sender.mesh.read({ topic: "fabric.control.command", limit: 100 })[0]!.data)
        .not.toHaveProperty("destinationRemoteHost");
      const publish = vi.spyOn(sender.mesh, "publish");
      await expect(sender.request("host:owner", "agent:target", "steer", {}, "host:owner",
        { routedRemoteHost: "forge" })).rejects.toThrow("Fabric mesh bridge routing to remote host forge is unavailable");
      expect(publish).not.toHaveBeenCalled();
    });

    it("never adopts an unvalidated replacement link label", async () => {
      const f = await setup();
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner"));
      f.setLease({ remoteHost: "forged-label", expiresAt: Date.now() + 60_000 });
      const { error } = await outcome;
      expect(error?.message).toContain("remote host forge");
      expect(error?.message).not.toContain("forged-label");
      await f.once(1);
    });
  });
  describe("mirrored message admission budget (production path, fake clock)", () => {
    // The transport gate models the mesh lock; request admission, timers, ACK authority,
    // cancellation and log consumption all run through the production control plane.
    const setup = async (blocked = false, mirror = true, port = true) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-budget-"));
      roots.push(root);
      let lease = mirror ? { remoteHost: "forge", expiresAt: Date.now() + 15_000 } : undefined;
      let readFails = false;
      const readMirroredOwner = vi.fn(() => {
        if (readFails) throw new Error("directory read failed");
        return lease;
      });
      const sender = plane(path.join(root, "mesh"), "host:sender", {}, {
        acknowledgementTimeoutMs: 5_000,
        ...(port ? { readMirroredOwner } : {}),
      });
      // Obtain the transport envelope without depending on its private shape.
      const envelope = await sender.mesh.publish({ topic: "fixture", from: identity("host:sender") });
      vi.useFakeTimers();
      const admittedAt = Date.now();
      if (mirror) lease = { remoteHost: "forge", expiresAt: admittedAt + 15_000 };
      const events: MeshEvent[] = [];
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      const publish = vi.spyOn(sender.mesh, "publish").mockImplementation(async (input) => {
        if (blocked && input.topic === "fabric.control.command" && input.kind !== "cancel") await gate;
        const committedAt = Date.now();
        const event: MeshEvent = {
          ...envelope, ...input,
          data: typeof input.data === "function" ? input.data(committedAt) : input.data,
          sequence: events.length + 1, createdAt: committedAt,
        };
        events.push(event);
        return event;
      });
      const tail = vi.spyOn(sender.mesh, "tail").mockImplementation((offset) => ({
        events: events.filter((event) => event.sequence > offset), nextOffset: events.length,
      }));
      sender.start(() => ({ accepted: false }));
      const commands = () => events.filter((event) => event.topic === "fabric.control.command");
      const ack = async (remoteHost: string | undefined = mirror ? "forge" : undefined) => {
        const command = commands().find((event) => event.kind !== "cancel")!.data as FabricControlCommand;
        await sender.mesh.publish({
          topic: "fabric.control.ack", kind: "accepted", from: identity("identity:owner"), to: "host:sender",
          data: { version: 1, commandId: command.commandId, targetId: "agent:target", accepted: true,
            messageId: "original", result: "done", ...(remoteHost ? { bridge: { from: remoteHost } } : {}) },
        });
        await vi.advanceTimersByTimeAsync(20);
      };
      const dispose = async () => {
        release();
        await Promise.resolve();
        await sender.close();
        publish.mockRestore();
        tail.mockRestore();
        vi.useRealTimers();
      };
      return { sender, admittedAt, publish, commands, ack, release, dispose, readMirroredOwner,
        failReads: () => { readFails = true; },
        setLease: (value: typeof lease) => { lease = value; } };
    };
    const settle = <T>(promise: Promise<T>) => promise.then(
      (value) => ({ value, error: undefined }), (error: Error) => ({ value: undefined, error }),
    );

    it.each(["steer", "followUp"] as const)("settles %s at admission + 9 s during a 10 s lock, even with failed reads", async (operation) => {
      const f = await setup(true);
      try {
        let settled = false;
        const outcome = settle(f.sender.request("host:owner", "agent:target", operation, {}, "identity:owner"))
          .then((value) => { settled = true; return value; });
        f.failReads(); // Still-live 15 s captured lease; no lapse evidence and no readable directory.
        await vi.advanceTimersByTimeAsync(8_999);
        expect(settled).toBe(false);
        expect(f.commands()).toHaveLength(0);
        await vi.advanceTimersByTimeAsync(1);
        expect(settled).toBe(true);
        const { error } = await outcome;
        expect(Date.now() - f.admittedAt).toBe(9_000);
        expect(error?.constructor).toBe(Error);
        expect(error?.message).toBe("Fabric mesh bridge to remote host forge is not responding for agent:target; the outcome is unknown and it may still be delivered, so a retry can deliver it twice.");
        expect(error).not.toHaveProperty("notRun");
        expect(f.publish).toHaveBeenCalledTimes(1); // No replay, no cancellation before commit.
        await vi.advanceTimersByTimeAsync(1_000);
        f.setLease({ remoteHost: "replacement", expiresAt: Date.now() + 60_000 });
        f.release();
        await vi.advanceTimersByTimeAsync(0);
        expect(f.commands().map((event) => event.kind)).toEqual([operation, "cancel"]);
        const [command, cancellation] = f.commands().map((event) => event.data as FabricControlCommand);
        expect(command).toMatchObject({ requestedAt: f.admittedAt + 10_000, deadlineAt: f.admittedAt + 15_000, destinationRemoteHost: "forge" });
        expect(cancellation).toMatchObject({ cancelCommandId: command!.commandId, destinationRemoteHost: "forge" });
        await f.ack(); // Original ACK is too late and cannot revive the settled request.
        expect(await outcome).toEqual({ value: undefined, error });
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0); // Late commit must not arm another ACK timer.
        expect(f.commands()).toHaveLength(2);
      } finally { await f.dispose(); }
    });

    it("allows the original ACK inside 9 s despite replacement, and clears the prearmed timer", async () => {
      const f = await setup();
      try {
        let settled = false;
        const outcome = f.sender.request("host:owner", "agent:target", "followUp", {}, "identity:owner")
          .finally(() => { settled = true; });
        await vi.advanceTimersByTimeAsync(8_000);
        f.setLease({ remoteHost: "replacement", expiresAt: Date.now() + 60_000 });
        await f.ack("replacement");
        expect(settled).toBe(false);
        await f.ack("forge");
        await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "original" });
        await vi.advanceTimersByTimeAsync(12_000);
        await f.sender.close();
        expect(f.commands()).toHaveLength(1);
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it("does not let same-origin renewal extend the 9 s budget", async () => {
      const f = await setup();
      try {
        const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner"));
        await vi.advanceTimersByTimeAsync(8_000);
        f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 60_000 });
        await vi.advanceTimersByTimeAsync(1_000);
        expect((await outcome).error?.message).toContain("is not responding");
        expect(f.commands().map((event) => event.kind)).toEqual(["steer", "cancel"]);
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it.each(["abort", "close", "lapse"] as const)("clears admission timer on %s while blocked, then cancels once after commit", async (winner) => {
      const f = await setup(true);
      const controller = new AbortController();
      try {
        const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", {}, "identity:owner", { signal: controller.signal }));
        await vi.advanceTimersByTimeAsync(100);
        if (winner === "abort") controller.abort();
        if (winner === "close") await f.sender.close();
        if (winner === "lapse") {
          f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
          await vi.advanceTimersByTimeAsync(20);
        }
        expect((await outcome).error?.message).toContain(winner === "abort" ? "cancelled" : winner === "close" ? "closed" : "lapsed");
        controller.abort();
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0);
        await vi.advanceTimersByTimeAsync(10_000);
        f.release();
        await vi.advanceTimersByTimeAsync(0);
        expect(f.commands().map((event) => event.kind)).toEqual(["steer", "cancel"]);
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it.each(["native", "legacy", "ask", "result-message", "stop"] as const)("preserves postcommit waits beyond 9 s for %s", async (mode) => {
      const mirrored = mode !== "native" && mode !== "legacy";
      const f = await setup(true, mirrored, mode !== "legacy");
      try {
        let settled = false;
        const request = mode === "ask" || mode === "result-message"
          ? f.sender.requestResult("host:owner", "agent:target", mode === "ask" ? "ask" : "steer", {}, "identity:owner", { timeoutMs: 60_000 })
          : f.sender.request("host:owner", "agent:target", mode === "stop" ? "stop" : "steer", {}, "identity:owner");
        const outcome = request.finally(() => { settled = true; });
        // Renew same-origin lease for long result operations; no global lease cutoff.
        f.setLease(mirrored ? { remoteHost: "forge", expiresAt: Date.now() + 120_000 } : undefined);
        await vi.advanceTimersByTimeAsync(10_000);
        expect(settled).toBe(false);
        f.release();
        await vi.advanceTimersByTimeAsync(0);
        const command = f.commands()[0]!.data as FabricControlCommand;
        expect(command.requestedAt).toBe(f.admittedAt + 10_000);
        expect(command.deadlineAt).toBe(command.requestedAt + (mode === "ask" || mode === "result-message" ? 60_000 : 5_000));
        if (mode === "legacy") expect(command).not.toHaveProperty("destinationRemoteHost");
        if (mode === "native") expect(command.destinationRemoteHost).toBeNull();
        // Native/stop ACKs after their wire deadline still get the original grace.
        await vi.advanceTimersByTimeAsync(mode === "ask" || mode === "result-message" ? 20_000 : 6_000);
        expect(settled).toBe(false);
        await f.ack();
        if (mode === "ask" || mode === "result-message") await expect(outcome).resolves.toBe("done");
        else await expect(outcome).resolves.toMatchObject({ acknowledged: true });
        await f.sender.close();
        expect(f.commands()).toHaveLength(1);
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });
  });

  describe("captured directory-backed ACK authority", () => {
    const setup = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-authority-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const writer = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const ownerId = "X"; // Canonical bridge root: host, root and identity are all X.
      const owner = identity(ownerId);
      const expiresAt = Date.now() + 60_000;
      const keyFor = (prefix: string) => prefix + createHash("sha256").update(ownerId).digest("hex");
      const hostKey = keyFor("topology/hosts/");
      const participantKey = keyFor("topology/participants/");
      const participant: FabricParticipantRecord = {
        format: 1, id: ownerId, kind: "root", rootId: ownerId,
        ownerHostId: ownerId, ownerIdentityId: ownerId, name: "main", status: "idle",
        runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
        cwd: "/tmp/project", sessionId: ownerId, startedAt: 1, updatedAt: 2,
        pendingMessages: false, controlProtocol: "v1",
      };
      const advertise = async (remoteHost: string, expiry = expiresAt) => {
        await writer.writeBatch({ identity: owner, ops: [
          { kind: "put", key: hostKey, value: {
            format: 1, id: ownerId, rootId: ownerId, identity: owner,
            startedAt: 1, updatedAt: 2, expiresAt: expiry, remoteHost,
          } },
          { kind: "put", key: participantKey, value: { ...participant, remoteHost } },
        ] });
        writeHostLease(meshRoot, {
          id: ownerId, rootId: ownerId, identityId: ownerId,
          updatedAt: Date.now(), expiresAt: expiry,
        });
      };
      const withdraw = async () => {
        await writer.writeBatch({ identity: owner, ops: [
          { kind: "delete", key: hostKey }, { kind: "delete", key: participantKey },
        ] });
        removeHostLease(meshRoot, ownerId);
      };
      const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: 2_000 }), {
        enabled: true, hostId: "host:sender", rootId: "host:sender", identity: identity("host:sender"),
      });
      const readMirroredOwner = vi.fn((host: string, ownerIdentity: string | undefined, target: string) =>
        directory.mirroredControlOwner(host, ownerIdentity, target));
      await advertise("forge");
      expect(readMirroredOwner(ownerId, ownerId, ownerId)).toEqual({ remoteHost: "forge", expiresAt });
      readMirroredOwner.mockClear();
      const sender = plane(meshRoot, "host:sender", { readCacheMs: 2_000 }, { readMirroredOwner });
      sender.start(() => ({ accepted: false }));
      const commands = () => writer.read({ topic: "fabric.control.command", limit: 100 });
      const command = async () => {
        await vi.waitFor(() => expect(commands().filter((event) => event.kind !== "cancel")).toHaveLength(1));
        expect(readMirroredOwner).toHaveBeenCalledWith(ownerId, ownerId, ownerId);
        return commands().find((event) => event.kind !== "cancel")!.data as { commandId: string };
      };
      const ack = (commandId: string, remoteHost: string | undefined, accepted = true) => writer.publish({
        topic: "fabric.control.ack", kind: accepted ? "accepted" : "rejected", from: owner, to: "host:sender",
        data: {
          version: 1, commandId, targetId: ownerId, accepted,
          ...(accepted ? { messageId: "original-forge" } : { error: "replacement says not run", notRun: true }),
          ...(remoteHost === undefined ? {} : { bridge: { from: remoteHost } }),
        },
      });
      return { sender, ownerId, expiresAt, readMirroredOwner, advertise, withdraw, commands, command, ack };
    };

    it.each([
      ["replacement", "ryzen2"], ["replacement", undefined],
      ["withdrawal", "ryzen2"], ["withdrawal", undefined],
    ] as const)("rejects %s ACK origin %s with notRun without retry or native downgrade", async (metadata, stamp) => {
      const f = await setup();
      let settled = false;
      const outcome = f.sender.request(f.ownerId, f.ownerId, "followUp")
        .finally(() => { settled = true; });
      const { commandId } = await f.command();
      await f.withdraw();
      if (metadata === "replacement") await f.advertise("ryzen2");
      expect(f.readMirroredOwner(f.ownerId, f.ownerId, f.ownerId)?.remoteHost)
        .toBe(metadata === "replacement" ? "ryzen2" : undefined);
      // This is the exact pending UUID, target and identity: only the origin is wrong.
      const tail = vi.spyOn(f.sender.mesh, "tail");
      await f.ack(commandId, stamp, false);
      await vi.waitFor(() => expect(tail.mock.results.some((result) =>
        result.type === "return" && result.value.events.some((event) => event.topic === "fabric.control.ack"),
      )).toBe(true));
      tail.mockRestore();
      expect(settled).toBe(false);
      expect(f.commands().filter((event) => event.kind !== "cancel")).toHaveLength(1);
      expect(f.commands().filter((event) => event.kind === "cancel")).toHaveLength(0);
      await f.ack(commandId, "forge");
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "original-forge" });
      expect(f.commands().filter((event) => event.kind !== "cancel")).toHaveLength(1);
    });

    it.each([undefined, "forge"] as const)("does not adopt a replacement on bounded retry with routing snapshot %s", async (routedRemoteHost) => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "followUp", {}, f.ownerId,
        routedRemoteHost === undefined ? {} : { routedRemoteHost }).then(() => undefined, (error: Error) => error);
      const { commandId } = await f.command();
      await f.withdraw();
      await f.advertise("ryzen2");
      // Even a valid ORIGINAL rejection only authorizes retry to the original link.
      await f.ack(commandId, "forge", false);
      const error = await outcome;
      expect(error?.message).toBe("Fabric mesh bridge routing to remote host forge is unavailable for X; the routed owner could not be revalidated; this attempt was not published.");
      expect(error).not.toHaveProperty("notRun");
      expect(f.commands()).toHaveLength(1);
      expect(f.commands()[0]!.data).toMatchObject({ destinationRemoteHost: "forge" });
    });

    it("keeps the original destination on a healthy mirrored notRun retry", async () => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "followUp");
      const { commandId } = await f.command();
      await f.ack(commandId, "forge", false);
      await vi.waitFor(() => expect(f.commands()).toHaveLength(2));
      const retry = f.commands().find((event) => (event.data as FabricControlCommand).commandId !== commandId)!;
      expect(f.commands().map((event) => (event.data as FabricControlCommand).destinationRemoteHost))
        .toEqual(["forge", "forge"]);
      await f.ack((retry.data as FabricControlCommand).commandId, "forge");
      await expect(outcome).resolves.toMatchObject({ acknowledged: true });
    });

    it("accepts the original Forge ACK committed before withdrawal but consumed after it", async () => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "steer");
      const { commandId } = await f.command();
      // Hold the sender's actual log drain, not ACK publication or directory reads.
      const tail = vi.spyOn(f.sender.mesh, "tail").mockImplementation((offset) => ({ events: [], nextOffset: offset }));
      try {
        await f.ack(commandId, "forge");
        await f.withdraw();
        expect(f.readMirroredOwner(f.ownerId, f.ownerId, f.ownerId)).toBeUndefined();
        expect(Date.now()).toBeLessThan(f.expiresAt);
      } finally {
        tail.mockRestore();
      }
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "original-forge" });
      await f.sender.close();
      expect(f.commands().filter((event) => event.kind === "cancel")).toHaveLength(0);
    });

    it("does not revive a settled mirrored request when its original ACK arrives", async () => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "steer").then(
        () => undefined, (error: Error) => error,
      );
      const { commandId } = await f.command();
      await f.advertise("forge", Date.now() - 1);
      const error = await outcome;
      expect(error?.message).toContain("remote host forge lapsed for X");
      expect(error).not.toHaveProperty("notRun");
      await f.withdraw();
      await f.advertise("forge");
      await f.ack(commandId, "forge");
      await f.sender.close();
      expect(f.commands().filter((event) => event.kind !== "cancel")).toHaveLength(1);
      await vi.waitFor(() => expect(f.commands().filter((event) => event.kind === "cancel")).toHaveLength(1));
      expect(await outcome).toBe(error);
    });
  });
  // smarty-dev#816: the deadline was stamped before the sender waited for the mesh lock (2-3 s
  // under load), so the owner got only what was left of the 5 s, or nothing. review/astra F1 on
  // #70: the sender's own wait must start at commit too.
  describe("with a publish that waits for the mesh lock", () => {
    const run = async (lockWaitMs: number, handlerMs: number) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s timeout
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, handlerMs));
        return { accepted: true, messageId: "delivered" };
      });
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      const original = MeshStore.prototype.publish;
      vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
        if (input.topic === "fabric.control.command" && input.kind === "steer") {
          await new Promise((resolve) => setTimeout(resolve, lockWaitMs));
        }
        return original.call(this, input);
      });
      const result = await sender.request("host:receiver", "agent:target", "steer", { message: "late" });
      const event = new MeshStore(meshRoot, 64 * 1024, 1_000).read({ topic: "fabric.control.command", limit: 10 })
        .find((candidate) => candidate.kind === "steer")!;
      return { result, receive, event };
    };

    it("stamps the command's deadline at commit, and waits for a slow owner from there", async () => {
      // 2.5 s lock wait + 750 ms handler: the old sender timer (1 s + 2 s grace from the start) fired first.
      const { result, receive, event } = await run(2_500, 750);
      expect(result.messageId).toBe("delivered");
      expect(receive).toHaveBeenCalledTimes(1);
      expect((event.data as { requestedAt: number }).requestedAt).toBe(event.createdAt);
    }, 15_000);

    it("does not time out before its publish commits, however long the lock wait", async () => {
      const { result, receive } = await run(3_500, 0);                    // longer than the old 3 s timer
      expect(result.messageId).toBe("delivered");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });

  // smarty-dev#424: a lock timeout while the owner claimed, recorded or acknowledged a command
  // dropped it without an acknowledgement or a retry (dev-lead: 30 of 61 commands in 6 h).
  describe("after a lock timeout", () => {
    const lockTimeout = () => Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), {
      code: "FABRIC_MESH_LOCK_TIMEOUT",
    });
    const failOnce = <K extends "put" | "publish">(method: K, when: (store: MeshStore, input: never) => boolean, afterMs = 0) => {
      const original = MeshStore.prototype[method] as (...args: unknown[]) => unknown;
      let failed = false;
      return vi.spyOn(MeshStore.prototype, method).mockImplementation(function (this: MeshStore, input: never) {
        if (!failed && when(this, input)) {
          failed = true;
          return new Promise((_resolve, reject) => setTimeout(() => reject(lockTimeout()), afterMs));
        }
        return original.call(this, input);
      } as never);
    };
    const run = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn((received: { commandId: string }) => ({ accepted: true, messageId: "local:" + received.commandId }));
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 100));
      return { meshRoot, receive, request: () => sender.request("host:receiver", "agent:target", "steer", { message: "once" }) };
    };
    const shared = (store: MeshStore) => !store.root.includes(`${path.sep}control-seen${path.sep}`);
    const isClaim = (input: { key?: string }) => input.key?.startsWith("topology/control-seen/") === true;

    it("on the shared claim, the command is claimed on the next poll and runs once", async () => {
      const { receive, request } = await run();
      const spy = failOnce("put", (store, input: { key?: string }) => shared(store) && isClaim(input));
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
      expect(spy.mock.results.some((entry) => entry.type === "return")).toBe(true);
    });

    it("between the shared and the own claim, the retry keeps its shared claim and runs once", async () => {
      const { receive, request } = await run();
      failOnce("put", (store, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(store) && isClaim(input) && input.value?.acceptance === undefined);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);                           // not "indeterminate"
      expect(receive).toHaveBeenCalledTimes(1);
    });

    it("while recording the outcome, the sender still gets it", async () => {
      const { receive, request } = await run();
      failOnce("put", (store, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(store) && isClaim(input) && input.value?.acceptance !== undefined);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
    });

    // A detached ask has passed the cursor: the bounded owned queue retries only its ACK,
    // not the handler. A later replay still consults the durable claim/outcome.
    it("of a detached ask, the bounded notification queue retries its outcome without re-execution", async () => {
      const { meshRoot, receive } = await run();
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const ask = {
        topic: "fabric.control.command", kind: "ask", from: identity("host:sender"), to: "host:receiver",
        data: { version: 1, commandId: "command:ask", targetId: "agent:target", operation: "ask", replyTo: "host:sender",
          message: "inspect", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
      };
      const acks = () => store.read({ topic: "fabric.control.ack", limit: 100 })
        .filter((event) => (event.data as { commandId?: string }).commandId === "command:ask");
      failOnce("put", (owner, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(owner) && isClaim(input) && input.value?.acceptance !== undefined);
      failOnce("publish", (_owner, input: { topic?: string }) => input.topic === "fabric.control.ack");
      await store.publish(ask);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      await vi.waitFor(() => expect(acks()).toHaveLength(1), { timeout: 3_000, interval: 20 });
      expect(acks()[0]!.data).toMatchObject({ accepted: true });
      await store.publish(ask); // the retried durable outcome answers a replay too
      await vi.waitFor(() => expect(acks()).toHaveLength(2), { timeout: 3_000, interval: 20 });
      expect(acks()[1]!.data).toMatchObject({ accepted: true });
      expect(receive).toHaveBeenCalledTimes(1);
    });

    it("on the acknowledgement, the next poll publishes the real outcome, even past the deadline", async () => {
      const { receive, request } = await run();
      // A real lock wait (10 s) outlasts the deadline (1 s here): the retry must not answer "expired".
      failOnce("publish", (_store, input: { topic?: string }) => input.topic === "fabric.control.ack", 1_200);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
    });
  });

  // smarty-dev#643: dedupe records lived in the shared state (26% of it on the fleet), and each
  // received command rewrote that whole file twice.
  describe("dedupe records", () => {
    const seenKey = (hostId: string, commandId: string) =>
      "topology/control-seen/" + createHash("sha256").update(`${hostId}\0${commandId}`).digest("hex");
    const ownStore = (meshRoot: string, hostId: string) =>
      new MeshStore(path.join(meshRoot, "control-seen", createHash("sha256").update(hostId).digest("hex").slice(0, 32)), 64 * 1024, 1_000);
    const command = (commandId: string, to: string) => ({
      topic: "fabric.control.command", kind: "steer", from: identity("host:sender"), to,
      data: { version: 1, commandId, targetId: "agent:target", operation: "steer", replyTo: "host:sender", message: "m", requestedAt: Date.now() },
    });
    const ackFor = (store: MeshStore, commandId: string) =>
      store.read({ topic: "fabric.control.ack", limit: 100 }).find((event) => (event.data as { commandId?: string }).commandId === commandId);

    it("keep the outcome in the owner's own store; the shared state holds only the claim", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const sender = plane(meshRoot, "host:sender");
      const receiver = plane(meshRoot, "host:receiver");
      sender.start(() => ({ accepted: false }));
      receiver.start((received) => ({ accepted: true, messageId: "local:" + received.commandId }));
      await new Promise((resolve) => setTimeout(resolve, 100));             // past its one-time legacy move
      const result = await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
      const commandEvent = store.read({ topic: "fabric.control.command", limit: 10 }).at(-1)!;
      const commandId = (commandEvent.data as { commandId: string }).commandId;
      expect(result.messageId).toBe("local:" + commandId);
      const shared = store.listAll("topology/control-seen/");
      expect(shared.map((entry) => entry.key)).toEqual([seenKey("host:receiver", commandId)]);
      expect(shared[0]!.value).not.toHaveProperty("acceptance");            // one shared write, no outcome
      expect(shared[0]!.version).toBe(store.get(shared[0]!.key)!.version);
      expect(ownStore(meshRoot, "host:receiver").get(seenKey("host:receiver", commandId))?.value).toMatchObject({
        hostId: "host:receiver", commandId, sequence: commandEvent.sequence, acceptance: { accepted: true },
      });
    });

    it("left in the shared state by older runtimes still answer a replay, and stay there", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:old", "host:receiver"));          // replayed at startup
      await store.put({
        key: seenKey("host:receiver", "command:old"), identity: identity("host:receiver"), ifVersion: 0, value: {
          format: 1, hostId: "host:receiver", commandId: "command:old", targetId: "agent:target", expiresAt: Date.now() + 60_000,
          acceptance: { accepted: true, messageId: "earlier:command:old" },
        },
      });
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:old")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(ackFor(store, "command:old")?.data).toMatchObject({ accepted: true, messageId: "earlier:command:old" });
      await store.publish(command("command:old", "host:receiver"));          // republished
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(receive).not.toHaveBeenCalled();
      expect(store.get(seenKey("host:receiver", "command:old"))).toBeDefined(); // older runtimes still rely on it
    });

    // review/astra on #58, F2: a runtime before this change claims only the shared key and never
    // reads the owner's store. Its admission is get(key) then put(key, ifVersion 0).
    const olderRuntimeAdmits = (store: MeshStore, commandId: string) => ({
      checked: store.get(seenKey("host:receiver", commandId), { fresh: true }) === undefined,
      claim: () => store.put({
        key: seenKey("host:receiver", commandId), identity: identity("host:receiver"), ifVersion: 0,
        value: { format: 1, hostId: "host:receiver", commandId, targetId: "agent:target", expiresAt: Date.now() + 60_000 },
      }).then(() => true, () => false),
    });

    it("run a command once when an older runtime of the same host checked it first and claims it later", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:race", "host:receiver"));
      const older = olderRuntimeAdmits(store, "command:race");               // paused after its check
      expect(older.checked).toBe(true);
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      const olderRuns = (await older.claim()) ? 1 : 0;                       // it resumes and claims
      expect(receive.mock.calls.length + olderRuns).toBe(1);
    });

    it("run a command once when an older runtime of the same host claimed it first", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:taken", "host:receiver"));
      const older = olderRuntimeAdmits(store, "command:taken");
      expect(older.checked && await older.claim()).toBe(true);               // it runs the command
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:taken")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:taken")?.data).toMatchObject({ accepted: false });
    });

    // review/astra on #58, F1: the deadline can pass while the claim waits for a lock, or after it
    // commits and before its promise resumes; the handler must not run then.
    const expiringCommand = async (meshRoot: string, commandId: string, deadlineInMs: number) => {
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const requestedAt = Date.now();
      await store.publish({ ...command(commandId, "host:receiver"),
        data: { ...command(commandId, "host:receiver").data, requestedAt, deadlineAt: requestedAt + deadlineInMs } });
      return store;
    };

    it("do not run a command whose claim returned after its deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = await expiringCommand(meshRoot, "command:late", 400);
      const put = MeshStore.prototype.put;
      vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
        const result = await put.call(this, input);                           // the claim commits ...
        if (this.root.includes("control-seen") && input.ifVersion === 0) await new Promise((resolve) => setTimeout(resolve, 600));
        return result;                                                        // ... and returns late
      });
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:late")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:late")?.data).toMatchObject({ accepted: false, error: "Fabric control command expired" });
      expect(ownStore(meshRoot, "host:receiver").get(seenKey("host:receiver", "command:late"))?.value)
        .toMatchObject({ acceptance: { accepted: false, error: "Fabric control command expired" } });
    });

    it("do not run a command whose claim waited for the owner's store lock past its deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = await expiringCommand(meshRoot, "command:locked", 400);
      const own = ownStore(meshRoot, "host:receiver");
      await own.put({ key: "warm", value: 1, identity: identity("host:receiver") });   // the store exists
      const lock = path.join(own.root, ".lock");
      fs.mkdirSync(lock);                                                     // another writer holds it
      fs.writeFileSync(path.join(lock, "owner"), `held\n${process.pid}\n${Date.now()}\n`);   // a live owner
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 700));              // past the deadline
      fs.rmSync(lock, { recursive: true, force: true });                     // the lock is released
      await vi.waitFor(() => expect(ackFor(store, "command:locked")).toBeDefined(), { timeout: 5_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:locked")?.data).toMatchObject({ accepted: false, error: "Fabric control command expired" });
    });

    // smarty-dev#816: shared claims were kept until their command left the event log (compacted
    // only at 64 MiB); 2,858 expired claims made up 46% of the shared state and saturated the lock.
    it("in the shared state go once expired plus a grace when the fleet owner allows it and the command had its own deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const start = Date.now();
      const MINUTE = 60_000;
      const later = start + 16 * MINUTE;                                   // past the 15-min sweep interval
      const claim = (commandId: string, value: Record<string, unknown>) => store.put({
        key: seenKey("host:other", commandId), identity: identity("host:other"), ifVersion: 0,
        value: { format: 1, hostId: "host:other", commandId, targetId: "agent:target", ...value },
      });
      for (const id of ["command:flagged", "command:grace", "command:legacy"]) {
        await store.publish(command(id, "host:other"));                    // all still in the log
      }
      await claim("command:flagged", { expiresAt: start, explicitDeadline: true });
      await claim("command:grace", { expiresAt: later - 5 * MINUTE, explicitDeadline: true });   // inside the grace
      await claim("command:legacy", { expiresAt: start });                // an older writer: no flag
      await store.put({ key: CONTROL_CLAIMS_POLICY_KEY, value: { version: 1, sharedClaims: "expiry" }, identity: identity("host:owner") });
      const receiver = plane(meshRoot, "host:receiver");
      receiver.start(() => ({ accepted: true }));
      const sender = plane(meshRoot, "host:sender");
      sender.start(() => ({ accepted: false }));
      await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
      const fresh = store.listAll("topology/control-seen/").find((entry) =>
        (entry.value as { hostId?: string }).hostId === "host:receiver");
      expect(fresh?.value).toMatchObject({ explicitDeadline: true });    // new claims carry the flag
      const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
      const now = Date.now;
      vi.spyOn(Date, "now").mockImplementation(() => now() - start + later);
      await vi.waitFor(() => expect(store.get(seenKey("host:other", "command:flagged"))).toBeUndefined(),
        { timeout: 3_000, interval: 20 });
      expect(store.get(seenKey("host:other", "command:grace"))).toBeDefined();
      expect(store.get(seenKey("host:other", "command:legacy"))).toBeDefined();   // still in the log
      const sweeps = batches.mock.calls.filter(([input]) => input.ops.every((op) => op.kind === "delete")
        && input.ops.some((op) => op.key.startsWith("topology/control-seen/")));
      expect(sweeps.length).toBeGreaterThanOrEqual(1);
      expect(sweeps[0]![0].ops.every((op) => op.ifVersion !== undefined)).toBe(true);   // one fenced batch
    });

    // review/astra on #65: a runtime before phase 1 checks the deadline only at admission and knows
    // only the shared claim. Without the fleet owner's policy, the shared claim must outlive expiry.
    it("keep the shared claim for a paused older runtime through the sweep and tombstone eviction", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const requestedAt = Date.now();
      await store.publish({ ...command("command:paused", "host:receiver"),
        data: { ...command("command:paused", "host:receiver").data, requestedAt, deadlineAt: requestedAt + 5_000 } });
      const older = olderRuntimeAdmits(store, "command:paused");            // passed admission, then pauses
      expect(older.checked).toBe(true);
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      expect(store.get(seenKey("host:receiver", "command:paused"))?.value).toMatchObject({ explicitDeadline: true });
      const now = Date.now;
      vi.spyOn(Date, "now").mockImplementation(() => now() + 16 * 60_000);   // past expiry, grace and sweep
      await new Promise((resolve) => setTimeout(resolve, 300));             // sweeps run
      for (let index = 0; index < 1_010; index++) {                         // evict every older tombstone
        await store.writeBatch({ identity: identity("host:noise"), ops: [
          { kind: "put", key: `noise/${index}`, value: index }, { kind: "delete", key: `noise/${index}` },
        ] });
      }
      const olderRuns = (await older.claim()) ? 1 : 0;                      // the older runtime resumes
      expect(receive.mock.calls.length + olderRuns).toBe(1);
      expect(store.get(seenKey("host:receiver", "command:paused"))).toBeDefined();
    }, 60_000);

    it("are deleted only once expired and their command has left the log", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const storeOptions: MeshStoreOptions = { maxEventLogBytes: 80_000, retainedEventLogBytes: 40_000 };
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
      // smarty-dev#883: a 200 ms deadline expired before a slow Windows runner claimed the command.
      const sender = plane(meshRoot, "host:sender", storeOptions, { acknowledgementTimeoutMs: 1_000 });
      const receiver = plane(meshRoot, "host:receiver", storeOptions, { acknowledgementTimeoutMs: 1_000 });
      const receive = vi.fn(() => ({ accepted: true }));
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
      const commandId = (store.read({ topic: "fabric.control.command", limit: 10 }).at(-1)!.data as { commandId: string }).commandId;
      const own = ownStore(meshRoot, "host:receiver");
      // Expired (at deadline + 1 s) and past one more 1 s cleanup pass, but still in the log.
      await new Promise((resolve) => setTimeout(resolve, 3_200));
      expect(own.get(seenKey("host:receiver", commandId))).toBeDefined();
      for (let index = 0; index < 100; index++) {                          // rotate it out of the log
        await store.publish({ topic: "compact", from: identity("host:publisher"), text: "x".repeat(900) });
      }
      expect(store.oldestSequence()).toBeGreaterThan(1);
      await vi.waitFor(() => expect(own.get(seenKey("host:receiver", commandId))).toBeUndefined(), { timeout: 5_000, interval: 20 });
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });

  it("routes to one execution owner and returns its acknowledgement", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const bystander = plane(meshRoot, "host:bystander");
    const receive = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: "local:" + command.commandId,
    }));
    const observe = vi.fn(() => ({ accepted: true }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);
    bystander.start(observe);

    await expect(
      sender.request("host:receiver", "agent:target", "steer", {
        message: "focus",
        triggerTurn: false,
      }),
    ).resolves.toMatchObject({
      queued: true,
      routed: "mesh",
      acknowledged: true,
      messageId: expect.stringMatching(/^local:/),
    });
    expect(receive).toHaveBeenCalledWith(
      expect.objectContaining({
        targetId: "agent:target",
        operation: "steer",
        message: "focus",
        triggerTurn: false,
        replyTo: "host:sender",
      }),
      expect.objectContaining({ id: "host:sender" }),
      expect.any(AbortSignal),
      "mesh",
    );
    expect(observe).not.toHaveBeenCalled();
  });

  // smarty-dev#1495: a followUp through the owner reports the target Main's queue, so the
  // sender can switch to steer; a malformed count from the owner is dropped, not passed on.
  it("passes the owner's followUp queue depth to the sender, and drops a malformed one", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const replies = [
      { accepted: true, messageId: "m1", pendingFollowUps: 3, oldestAgeS: 140 },
      { accepted: true, messageId: "m2", pendingFollowUps: -1, oldestAgeS: 2 },
      { accepted: true, messageId: "m3", pendingFollowUps: 1.5, oldestAgeS: "9" },
      { accepted: true, messageId: "m4" },
      { accepted: true, messageId: "m5", pendingFollowUps: 1, oldestAgeS: 9, coalesced: true, replacedMessageId: "m4" },
      { accepted: true, messageId: "m6", coalesced: "yes", replacedMessageId: 4 },
      { accepted: true, messageId: "m7", pendingFollowUps: 4, oldestAgeS: 700, stalled: true },   // smarty-dev#1826
      { accepted: true, messageId: "m8", pendingFollowUps: 4, oldestAgeS: 700, stalled: "yes" },
    ];
    sender.start(() => ({ accepted: false }));
    receiver.start(() => replies.shift() as never);
    const send = () => sender.request("host:receiver", "session:main", "followUp", { message: "later" });
    await expect(send()).resolves.toEqual({ queued: true, messageId: "m1", routed: "mesh", acknowledged: true, pendingFollowUps: 3, oldestAgeS: 140 });
    for (const messageId of ["m2", "m3", "m4"]) {
      await expect(send()).resolves.toEqual({ queued: true, messageId, routed: "mesh", acknowledged: true });
    }
    // A coalesced followUp (smarty-dev#1495) says which held one it replaced; a malformed report is dropped.
    await expect(send()).resolves.toEqual({
      queued: true, messageId: "m5", routed: "mesh", acknowledged: true, pendingFollowUps: 1, oldestAgeS: 9, coalesced: true, replacedMessageId: "m4",
    });
    await expect(send()).resolves.toEqual({ queued: true, messageId: "m6", routed: "mesh", acknowledged: true });
    await expect(send()).resolves.toEqual({
      queued: true, messageId: "m7", routed: "mesh", acknowledged: true, pendingFollowUps: 4, oldestAgeS: 700, stalled: true,
    });
    await expect(send()).resolves.toEqual({ queued: true, messageId: "m8", routed: "mesh", acknowledged: true, pendingFollowUps: 4, oldestAgeS: 700 });
  });

  it("returns an authenticated result with the caller's actor binding", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const response = { id: "message:1", text: "done" };
    const receive = vi.fn((command: { binding?: unknown }) => ({
      accepted: true,
      messageId: "message:1",
      result: response,
    }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);

    await expect(
      sender.requestResult(
        "host:receiver",
        "actor:target",
        "ask",
        {
          message: "inspect",
          binding: { model: "provider/session-b", thinking: "high" },
        },
      ),
    ).resolves.toEqual(response);
    expect(receive).toHaveBeenCalledWith(
      expect.objectContaining({
        targetId: "actor:target",
        operation: "ask",
        binding: { model: "provider/session-b", thinking: "high" },
        deadlineAt: expect.any(Number),
      }),
      expect.objectContaining({ id: "host:sender" }),
      expect.any(AbortSignal),
      "mesh",
    );
  });
  it("ignores an acknowledgement forged by a different mesh identity", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    sender.start(() => ({ accepted: false }));
    receiver.start(async (command) => {
      await new Promise((resolve) => setTimeout(resolve, 120));
      return { accepted: true, messageId: "real:" + command.commandId };
    });

    const request = sender.request("host:receiver", "agent:target", "steer", {
      message: "focus",
    });
    await new Promise((resolve) => setTimeout(resolve, 35));
    const command = store.read({ topic: "fabric.control.command", limit: 1 })[0];
    const commandId = (command?.data as { commandId?: string } | undefined)?.commandId;
    expect(commandId).toBeTypeOf("string");
    await store.publish({
      topic: "fabric.control.ack",
      kind: "accepted",
      from: identity("host:bystander"),
      to: "host:sender",
      data: {
        version: 1,
        commandId,
        targetId: "agent:target",
        accepted: true,
        messageId: "forged",
      },
    });

    await expect(request).resolves.toMatchObject({
      acknowledged: true,
      messageId: expect.stringMatching(/^real:/),
    });
  });

  it("recovers an unexpired command published before owner startup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:before-start",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "recover",
        requestedAt: Date.now(),
      },
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true, messageId: "recovered" }));
    receiver.start(receive);
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).toHaveBeenCalledTimes(1);
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:before-start",
          accepted: true,
        }),
      }),
    );
  });

  it("rejects an interrupted durable claim as indeterminate after restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const commandId = "command:interrupted";
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId,
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "unknown outcome",
        requestedAt: Date.now(),
      },
    });
    const seenKey =
      "topology/control-seen/" +
      createHash("sha256").update(`host:receiver\0${commandId}`).digest("hex");
    await store.put({
      key: seenKey,
      value: {
        format: 1,
        hostId: "host:receiver",
        commandId,
        targetId: "agent:target",
        expiresAt: Date.now() + 1_000,
      },
      identity: identity("host:receiver"),
      ifVersion: 0,
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId,
          accepted: false,
          error: "Fabric control outcome is indeterminate after owner restart",
        }),
      }),
    );
  });

  it("does not re-execute a command republished after owner restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const firstOwner = plane(meshRoot, "host:receiver");
    const firstHandler = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    firstOwner.start(firstHandler);
    await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
    expect(firstHandler).toHaveBeenCalledTimes(1);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const original = store.read({ topic: "fabric.control.command", limit: 10 })[0];
    expect(original).toBeDefined();
    await firstOwner.close();

    const restartedOwner = plane(meshRoot, "host:receiver");
    const restartedHandler = vi.fn(() => ({ accepted: true }));
    restartedOwner.start(restartedHandler);
    await store.publish({
      topic: "fabric.control.command",
      kind: original!.kind,
      from: original!.from,
      ...(original!.to ? { to: original!.to } : {}),
      ...(original!.data === undefined ? {} : { data: original!.data }),
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(restartedHandler).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 }).length).toBeGreaterThan(1);
  });

  it("rejects a replay outside the acknowledgement lifetime", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:replayed",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "stale",
        requestedAt: Date.now() - 5_000,
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:replayed",
          accepted: false,
          error: "Fabric control command expired",
        }),
      }),
    );
  });

  // smarty-dev#367: every restarting owner replays the retained log from its start; it
  // answered each long-expired command again, 21,479 acks nobody waited for.
  it("does not answer a command whose sender stopped waiting long ago", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:history",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "old",
        requestedAt: Date.now() - 10 * 60_000,
      },
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);                                     // a restart replays the log
    await new Promise((resolve) => setTimeout(resolve, 150));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toEqual([]);
  });

  it("final-drains a command published immediately before close", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true, messageId: "drained" }));
    receiver.start(receive);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:before-close",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "finish",
        requestedAt: Date.now(),
      },
    });

    await receiver.close();

    expect(receive).toHaveBeenCalledTimes(1);
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        to: "host:sender",
        data: expect.objectContaining({
          commandId: "command:before-close",
          accepted: true,
        }),
      }),
    );
  });

  it("does not re-execute a retained command after event-log compaction", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const storeOptions: MeshStoreOptions = {
      maxEventLogBytes: 80_000,
      retainedEventLogBytes: 75_000,
    };
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
    for (let index = 0; index < 8; index++) {
      await store.publish({
        topic: "prefill",
        from: identity("host:prefill"),
        text: "p".repeat(900),
      });
    }
    const sender = plane(meshRoot, "host:sender", storeOptions);
    const receiver = plane(meshRoot, "host:receiver", storeOptions);
    const receive = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);

    await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
    expect(receive).toHaveBeenCalledTimes(1);
    for (let index = 0; index < 62; index++) {
      await store.publish({
        topic: "compact",
        from: identity("host:publisher"),
        text: "x".repeat(900),
      });
    }
    await new Promise((resolve) => setTimeout(resolve, 160));

    expect(receive).toHaveBeenCalledTimes(1);
  });

  it("keeps control traffic responsive while an ask is running and cancels the owner", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const controller = new AbortController();
    let askStarted = false;
    let askAborted = false;
    receiver.start((command, _from, signal) => {
      if (command.operation !== "ask") {
        return { accepted: true, messageId: `accepted:${command.operation}` };
      }
      askStarted = true;
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          askAborted = true;
          resolve({ accepted: false, error: "actor request cancelled" });
        }, { once: true });
      });
    });
    sender.start(() => ({ accepted: false }));

    const ask = sender.requestResult(
      "host:receiver",
      "actor:target",
      "ask",
      { message: "inspect" },
      "host:receiver",
      { timeoutMs: 2_000, signal: controller.signal },
    );
    await vi.waitFor(() => expect(askStarted).toBe(true));
    await expect(
      sender.request("host:receiver", "actor:target", "stop"),
    ).resolves.toMatchObject({ acknowledged: true });

    controller.abort();
    await expect(ask).rejects.toThrow("Remote Fabric request cancelled");
    await vi.waitFor(() => expect(askAborted).toBe(true));
  });

  it("publishes an immediately cancelled command before its cancellation", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const publish = sender.mesh.publish.bind(sender.mesh);
    vi.spyOn(sender.mesh, "publish").mockImplementation(async (input) => {
      if (input.topic === "fabric.control.command" && input.kind === "ask") {
        await new Promise((resolve) => setTimeout(resolve, 60));
      }
      return publish(input);
    });
    let ownerAborted = false;
    receiver.start((command, _from, signal) => {
      if (command.operation !== "ask") return { accepted: true };
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          ownerAborted = true;
          resolve({ accepted: false, error: "cancelled" });
        }, { once: true });
      });
    });
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();

    const request = sender.requestResult(
      "host:receiver",
      "actor:target",
      "ask",
      { message: "inspect" },
      "host:receiver",
      { timeoutMs: 1_000, signal: controller.signal },
    );
    controller.abort();

    await expect(request).rejects.toThrow("Remote Fabric request cancelled");
    await vi.waitFor(() => expect(ownerAborted).toBe(true));
    const kinds = new MeshStore(meshRoot, 64 * 1024, 1_000)
      .tail(0, 10)
      .events.filter((event) => event.topic === "fabric.control.command")
      .map((event) => event.kind);
    expect(kinds.indexOf("ask")).toBeLessThan(kinds.indexOf("cancel"));
  });

  it("retains a completed ask outcome through its request deadline", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(
      meshRoot,
      "host:receiver",
      {},
      { pollMs: 20, acknowledgementTimeoutMs: 80 },
    );
    const receive = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 120));
      return { accepted: true, result: { id: "message:1", text: "done" } };
    });
    receiver.start(receive);
    sender.start(() => ({ accepted: false }));

    await expect(
      sender.requestResult(
        "host:receiver",
        "actor:target",
        "ask",
        { message: "inspect" },
        "host:receiver",
        { timeoutMs: 500 },
      ),
    ).resolves.toMatchObject({ text: "done" });
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const original = store.read({ topic: "fabric.control.command", limit: 10 })
      .find((event) => event.kind === "ask");
    expect(original).toBeDefined();
    await store.publish({
      topic: "fabric.control.command",
      kind: original!.kind,
      from: original!.from,
      ...(original!.to ? { to: original!.to } : {}),
      ...(original!.data === undefined ? {} : { data: original!.data }),
    });
    await new Promise((resolve) => setTimeout(resolve, 180));

    expect(receive).toHaveBeenCalledTimes(1);
  });

  // smarty-dev#367: a retry after an acknowledgement timeout delivered the message twice.
  it("waits past the deadline for the acknowledgement of a command the owner admitted in time", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // A 400 ms deadline leaves a slow runner (windows-latest) time to admit the command;
    // the handler then acknowledges after the deadline but inside the 2 x 400 ms grace.
    const sender = plane(meshRoot, "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 400 });
    const receiver = plane(meshRoot, "host:receiver", {}, { pollMs: 20, acknowledgementTimeoutMs: 400 });
    // Admitted within the deadline, acknowledged after it (a slow handler or a contended
    // mesh lock): the sender must report the delivery, not a timeout.
    receiver.start(async () => {
      await new Promise((resolve) => setTimeout(resolve, 500));
      return { accepted: true, messageId: "late-but-delivered" };
    });
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "slow ack" }))
      .resolves.toMatchObject({ acknowledged: true, messageId: "late-but-delivered" });
  });

  it("still times out, after the deadline plus a bounded grace, when no owner answers", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 100 });
    sender.start(() => ({ accepted: false }));
    const started = Date.now();
    // No receiver runs. The outcome is unknown, so the error must not promise a safe retry.
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "nobody home" }))
      .rejects.toThrow("the outcome is unknown and it may still be delivered, so a retry can deliver it twice");
    const waited = Date.now() - started;
    expect(waited).toBeGreaterThanOrEqual(100 + 200 - 20);   // the deadline plus 2 x 100 ms of grace
    expect(waited).toBeLessThan(2_000);
  });

  it("surfaces owner rejection instead of reporting an unverified queue", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    sender.start(() => ({ accepted: false }));
    receiver.start(() => ({ accepted: false, error: "target already settled" }));

    await expect(
      sender.request("host:receiver", "agent:missing", "stop"),
    ).rejects.toThrow("target already settled");
  });

  // smarty-dev#816: an owner that picked a command up after its deadline rejected it as expired
  // (4 of 2,802 commands, mostly in a fleet relaunch); the sender's manual retry worked.
  describe("when the owner rejects a command as expired", () => {
    const run = async (operation: "followUp" | "stop", rejection?: string) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s deadline
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn((command: { commandId: string }) => rejection
        ? { accepted: false, error: rejection }
        : { accepted: true, messageId: "delivered:" + command.commandId });
      sender.start(() => ({ accepted: false }));
      const outcome = sender.request("host:receiver", "agent:target", operation, { message: "late" })
        .then(
          (value) => ({ value, error: undefined as Error | undefined }),
          (error: Error) => ({ value: undefined, error: error as Error | undefined }),
        );
      // The owner polls only after the first command's deadline; later commands are on time.
      if (!rejection) await new Promise((resolve) => setTimeout(resolve, 1_300));
      receiver.start(receive);
      const settled = await outcome;
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const commands = store
        .read({ topic: "fabric.control.command", limit: 100 })
        .filter((event) => event.kind === operation);
      const acks = store.read({ topic: "fabric.control.ack", limit: 100 })
        .map((event) => event.data as { commandId: string });
      return { ...settled, receive, commands, acks, store };
    };

    const settle = <T>(promise: Promise<T>) => promise.then(
      (value) => ({ value, error: undefined as Error | undefined }),
      (error: Error) => ({ value: undefined as T | undefined, error: error as Error | undefined }),
    );

    // review/astra F1 on #121: the owner delivers, then dies before its acknowledgement; a new
    // runtime of the same host replays the command past its deadline while the sender still waits.
    const restartPastDeadline = async (crash: "acknowledgement" | "outcome") => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s deadline, 2 s grace
      const firstOwner = plane(meshRoot, "host:receiver");
      const receive = vi.fn((command: { commandId: string }) =>
        ({ accepted: true, messageId: "delivered:" + command.commandId }));
      let crashed = true;
      const publish = MeshStore.prototype.publish;
      vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
        if (crashed && input.topic === "fabric.control.ack") throw new Error("owner crashed");
        return publish.call(this, input);
      });
      const put = MeshStore.prototype.put;
      vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
        // "outcome": the claim commits, the handler runs, the outcome is never recorded.
        if (crashed && crash === "outcome" && this.root.includes("control-seen") && input.ifVersion !== 0) {
          throw new Error("owner crashed");
        }
        return put.call(this, input);
      });
      sender.start(() => ({ accepted: false }));
      firstOwner.start(receive);
      const outcome = settle(sender.request("host:receiver", "agent:target", "followUp", { message: "once" }));
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 900, interval: 10 });
      await firstOwner.close();
      crashed = false;
      await new Promise((resolve) => setTimeout(resolve, 1_300));         // past the deadline
      const restarted = plane(meshRoot, "host:receiver");                 // fresh process state
      restarted.start(receive);
      const settled = await outcome;
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const commands = store.read({ topic: "fabric.control.command", limit: 100 })
        .filter((event) => event.kind === "followUp");
      const acks = store.read({ topic: "fabric.control.ack", limit: 100 }).map((event) => event.data);
      return { ...settled, receive, commands, acks };
    };

    it("answers a delivered message's recorded outcome after an owner restart past the deadline, without a retry", async () => {
      const { value, error, receive, commands, acks } = await restartPastDeadline("acknowledgement");
      expect(error).toBeUndefined();
      expect(commands).toHaveLength(1);
      const first = commands[0]!.data as { commandId: string };
      expect(value).toMatchObject({ acknowledged: true, messageId: "delivered:" + first.commandId });
      expect(acks).toEqual([expect.objectContaining({ commandId: first.commandId, accepted: true })]);
      expect(acks[0]).not.toHaveProperty("notRun");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);

    it("reports a claimed message without an outcome as indeterminate after a restart past the deadline, without a retry", async () => {
      const { error, receive, commands, acks } = await restartPastDeadline("outcome");
      expect(error?.message).toBe("Fabric control outcome is indeterminate after owner restart");
      expect(commands).toHaveLength(1);
      expect(acks).toHaveLength(1);
      expect(acks[0]).not.toHaveProperty("notRun");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);

    // review/astra F1 round 2 on #121: a runtime before smarty-dev#643 claims only the shared key.
    const sharedKey = (commandId: string) =>
      "topology/control-seen/" + createHash("sha256").update(`host:receiver\0${commandId}`).digest("hex");
    const olderRuntimeClaims = (store: MeshStore, commandId: string, acceptance?: object) => store.put({
      key: sharedKey(commandId), identity: identity("host:receiver"), ifVersion: 0, value: {
        format: 1, hostId: "host:receiver", commandId, targetId: "agent:target", expiresAt: Date.now() + 60_000,
        ...(acceptance ? { acceptance } : {}),
      },
    });

    it.each([
      ["its outcome", { accepted: true, messageId: "older:delivered" }],
      ["no outcome yet", undefined],
    ])("(e) does not report notRun or retry a message an older runtime claimed and delivered, with %s", async (_, recorded) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const sender = plane(meshRoot, "host:sender");                     // a 1 s deadline
      sender.start(() => ({ accepted: false }));
      const outcome = settle(sender.request("host:receiver", "agent:target", "followUp", { message: "once" }));
      let commandId = "";
      await vi.waitFor(() => {
        commandId = (store.read({ topic: "fabric.control.command", limit: 10 })[0]?.data as { commandId: string }).commandId;
        expect(commandId).toBeTruthy();
      }, { timeout: 900, interval: 10 });
      await olderRuntimeClaims(store, commandId, recorded);               // it claims and delivers
      const olderDeliveries = 1;
      await new Promise((resolve) => setTimeout(resolve, 1_300));         // past the deadline
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      const { value, error } = await outcome;
      if (recorded) expect(value).toMatchObject({ acknowledged: true, messageId: "older:delivered" });
      else expect(error?.message).toBe("Fabric control outcome is indeterminate after owner restart");
      const acks = store.read({ topic: "fabric.control.ack", limit: 100 }).map((event) => event.data);
      expect(acks).toHaveLength(1);
      expect(acks[0]).not.toHaveProperty("notRun");
      expect(store.read({ topic: "fabric.control.command", limit: 10 }).filter((event) => event.kind === "followUp"))
        .toHaveLength(1);
      expect(receive.mock.calls.length + olderDeliveries).toBe(1);
    }, 15_000);

    it("(f) loses the shared claim and does not report notRun when its cached read missed a later claim", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.put({ key: "unrelated", identity: identity("host:other"), value: 1 }); // a state file to cache
      const receiver = plane(meshRoot, "host:receiver", { readCacheMs: 60_000 });
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 150));           // past its one-time legacy move
      expect(receiver.mesh.get(sharedKey("command:warm"))).toBeUndefined(); // warm: no claim
      await olderRuntimeClaims(store, "command:warm");                     // another runtime claims, delivers
      const olderDeliveries = 1;
      expect(receiver.mesh.get(sharedKey("command:warm"))).toBeUndefined(); // the cache still misses it
      const requestedAt = Date.now() - 2_000;                             // past its deadline, still answerable
      await store.publish({
        topic: "fabric.control.command", kind: "followUp", from: identity("host:sender"), to: "host:receiver",
        data: { version: 1, commandId: "command:warm", targetId: "agent:target", operation: "followUp", replyTo: "host:sender",
          message: "m", requestedAt, deadlineAt: requestedAt + 1_000 },
      });
      await vi.waitFor(() => expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(1),
        { timeout: 3_000, interval: 20 });
      const ack = store.read({ topic: "fabric.control.ack", limit: 10 })[0]!.data;
      expect(ack).toMatchObject({ accepted: false, error: "Fabric control outcome is indeterminate after owner restart" });
      expect(ack).not.toHaveProperty("notRun");
      expect(receive.mock.calls.length + olderDeliveries).toBe(1);
    }, 15_000);

    // An owner before #121 answers the same text without reading its claim: never retried.
    it("does not retry an expiry acknowledgement without notRun, as an older owner sends", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      sender.start(() => ({ accepted: false }));
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const outcome = settle(sender.request("host:receiver", "agent:target", "followUp", { message: "old" }));
      let command: { commandId: string } | undefined;
      await vi.waitFor(() => {
        command = store.read({ topic: "fabric.control.command", limit: 10 })[0]?.data as { commandId: string };
        expect(command).toBeDefined();
      }, { timeout: 900, interval: 10 });
      await store.publish({
        topic: "fabric.control.ack",
        kind: "rejected",
        from: identity("host:receiver"),
        to: "host:sender",
        data: { version: 1, commandId: command!.commandId, targetId: "agent:target", accepted: false, error: "Fabric control command expired" },
      });
      const { error } = await outcome;
      expect(error?.message).toBe("Fabric control command expired");
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(store.read({ topic: "fabric.control.command", limit: 10 }).filter((event) => event.kind === "followUp"))
        .toHaveLength(1);
    }, 15_000);

    it("retries a message once with a new command, and the owner delivers it once", async () => {
      const { value, error, receive, commands, acks, store } = await run("followUp");
      expect(error).toBeUndefined();
      expect(commands).toHaveLength(2);
      const first = commands[0]!.data as { commandId: string };
      const retry = commands[1]!.data as { commandId: string };
      expect(first.commandId).not.toBe(retry.commandId);
      // The owner won the shared claim for the first command, so nothing ran it: it proves notRun,
      // and records the expiry in that claim, where every runtime, new or old, finds it.
      expect(acks.find((ack) => ack.commandId === first.commandId))
        .toMatchObject({ accepted: false, error: "Fabric control command expired", notRun: true });
      expect(store.get(sharedKey(first.commandId))?.value).toMatchObject({
        hostId: "host:receiver", commandId: first.commandId,
        acceptance: { accepted: false, error: "Fabric control command expired", notRun: true },
      });
      expect(acks.find((ack) => ack.commandId === retry.commandId)).toMatchObject({ accepted: true });
      expect(value).toMatchObject({ acknowledged: true, messageId: "delivered:" + retry.commandId });
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);

    it("does not retry a command that is not a message", async () => {
      const { error, receive, commands } = await run("stop");
      expect(error?.message).toBe("Fabric control command expired");
      expect(commands).toHaveLength(1);
      expect(receive).not.toHaveBeenCalled();
    }, 15_000);

    it("does not retry a message the owner rejected for another reason", async () => {
      const { error, receive, commands } = await run("followUp", "target already settled");
      expect(error?.message).toBe("target already settled");
      expect(commands).toHaveLength(1);
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });
});
