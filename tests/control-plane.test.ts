import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MeshStore,
  type MeshIdentity,
  type MeshStoreOptions,
} from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

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
  controlOptions: { pollMs?: number; acknowledgementTimeoutMs?: number; now?: () => number } = {},
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
    );
    expect(observe).not.toHaveBeenCalled();
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

  // smarty-dev#266: seen records stayed until their command left the log, so they grew
  // without bound in the shared state file.
  const MINUTE = 60 * 1_000;
  const seenKey = (hostId: string, commandId: string) =>
    "topology/control-seen/" + createHash("sha256").update(`${hostId}\0${commandId}`).digest("hex");
  const seed = async (
    store: MeshStore,
    commandId: string,
    expiresAt: number,
    options: { hostId?: string; explicitDeadline?: boolean } = {},
  ) => {
    const hostId = options.hostId ?? "host:receiver";
    const explicit = options.explicitDeadline ?? true;
    const event = await store.publish({
      topic: "fabric.control.command", kind: "steer", from: identity("host:sender"), to: hostId,
      data: { version: 1, commandId, targetId: "agent:target", operation: "steer", replyTo: "host:sender",
        message: "old", requestedAt: expiresAt - 2_000, ...(explicit ? { deadlineAt: expiresAt - 1_000 } : {}) },
    });
    await store.put({
      key: seenKey(hostId, commandId),
      value: { format: 1, hostId, commandId, targetId: "agent:target", expiresAt,
        ...(options.explicitDeadline === undefined ? { explicitDeadline: true } : options.explicitDeadline ? { explicitDeadline: true } : {}),
        acceptance: { accepted: true } },
      identity: identity(hostId), ifVersion: 0,
    });
    return event;
  };
  const ackAfter = (store: MeshStore, commandId: string, sequence: number, error?: string) =>
    store.read({ topic: "fabric.control.ack", limit: 1_000 }).some((event) =>
      event.sequence > sequence &&
      (event.data as { commandId?: string }).commandId === commandId &&
      (error === undefined || (event.data as { error?: string }).error === error));
  const republish = (store: MeshStore, event: { topic: string; kind: string; from: MeshIdentity; to?: string; data?: unknown }) =>
    store.publish({ topic: event.topic, kind: event.kind, from: event.from, to: event.to!, data: event.data });
  const trigger = async (meshRoot: string, target: string) => {
    const sender = plane(meshRoot, "host:sender");
    sender.start(() => ({ accepted: false }));
    await sender.request(target, "agent:target", "steer", { message: "trigger cleanup" });
  };

  it("prunes its own long-expired records in one write and keeps their tombstones", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const now = Date.now();
    for (let index = 0; index < 20; index++) await seed(store, `command:old-${index}`, now - 11 * MINUTE);
    await seed(store, "command:grace", now - 10 * MINUTE);              // equality: inside the grace
    await seed(store, "command:live", now + MINUTE);
    await seed(store, "command:other-host", now - 11 * MINUTE, { hostId: "host:other" });
    await store.put({ key: "topology/control-seen-archive/x", identity: identity("host:x"),
      value: { format: 1, hostId: "host:receiver", commandId: "x", targetId: "t", expiresAt: 1, explicitDeadline: true } });
    const writes = vi.spyOn(MeshStore.prototype, "deleteMany");
    const receiver = plane(meshRoot, "host:receiver", {}, { now: () => now });
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    await trigger(meshRoot, "host:receiver");
    await expect.poll(() => store.get(seenKey("host:receiver", "command:old-0"))).toBeUndefined();

    expect(writes).toHaveBeenCalledTimes(1);
    writes.mockRestore();
    for (const kept of ["command:grace", "command:live"]) expect(store.get(seenKey("host:receiver", kept))).toBeDefined();
    expect(store.get(seenKey("host:other", "command:other-host"))).toBeDefined();   // never another host's
    expect(store.get("topology/control-seen-archive/x")).toBeDefined();             // never another prefix
    const state = JSON.parse(fs.readFileSync(path.join(meshRoot, "state.json"), "utf8"));
    expect(state.versions[seenKey("host:receiver", "command:old-0")]).toBe(1);      // tombstone kept

    // A pruned command replayed from the log is rejected as expired, never re-executed.
    const replay = store.read({ topic: "fabric.control.command", limit: 100 })
      .find((event) => (event.data as { commandId?: string }).commandId === "command:old-0")!;
    const republished = await republish(store, replay);
    await expect.poll(() => ackAfter(store, "command:old-0", republished.sequence, "Fabric control command expired")).toBe(true);
    expect(receive).toHaveBeenCalledTimes(1);                                          // only the trigger
  });

  // Review D1: a command without deadlineAt has a deadline that depends on the receiver's
  // acknowledgement timeout, which a restart can raise.
  it("keeps no-deadline records until no configurable deadline can reach them", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const now = Date.now();
    const legacy = await seed(store, "command:legacy", now - 11 * MINUTE, { explicitDeadline: false });
    await seed(store, "command:ancient", now - 25 * 60 * MINUTE, { explicitDeadline: false });
    const receiver = plane(meshRoot, "host:receiver", {}, { now: () => now, acknowledgementTimeoutMs: 20 * MINUTE });
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    await trigger(meshRoot, "host:receiver");
    await expect.poll(() => store.get(seenKey("host:receiver", "command:ancient"))).toBeUndefined();
    expect(store.get(seenKey("host:receiver", "command:legacy"))).toBeDefined();

    // The same owner restarted with a larger timeout sees the legacy command as live again,
    // but its record is still there: the outcome is re-acknowledged, not re-executed.
    const republished = await republish(store, legacy);
    await expect.poll(() => ackAfter(store, "command:legacy", republished.sequence)).toBe(true);
    expect(receive).toHaveBeenCalledTimes(1);
  });

  // Review D3/D4: the clock must be read after cleanup and again under the claim's lock.
  it("rejects a command whose deadline passes while cleanup runs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    let clock = Date.now();
    const receiver = plane(meshRoot, "host:receiver", {}, { now: () => clock });
    const receive = vi.fn(() => ({ accepted: true }));
    const listAll = receiver.mesh.listAll.bind(receiver.mesh);
    vi.spyOn(receiver.mesh, "listAll").mockImplementation((prefix) => {
      if (prefix === "topology/control-seen/") clock += 10 * MINUTE;   // cleanup takes "10 minutes"
      return listAll(prefix);
    });
    const claims = vi.spyOn(receiver.mesh, "put");
    receiver.start(receive);
    const sender = plane(meshRoot, "host:sender");
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "late" }))
      .rejects.toThrow("Fabric control command expired");
    expect(receive).not.toHaveBeenCalled();
    // Rejected at admission on the fresh clock, before any claim is attempted.
    expect(claims.mock.calls.filter(([input]) => input.key.startsWith("topology/control-seen/"))).toHaveLength(0);
  });

  it("rejects a command whose deadline passes while its claim waits for the lock, without committing it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    let clock = Date.now();
    const receiver = plane(meshRoot, "host:receiver", {}, { now: () => clock });
    const receive = vi.fn(() => ({ accepted: true }));
    const put = receiver.mesh.put.bind(receiver.mesh);
    let claimedKey: string | undefined;
    vi.spyOn(receiver.mesh, "put").mockImplementation(async (input) => {
      if (input.key.startsWith("topology/control-seen/") && input.ifVersion === 0) {
        claimedKey = input.key;
        clock += 10 * MINUTE;                                           // the lock wait "takes 10 minutes"
      }
      return put(input);
    });
    receiver.start(receive);
    const sender = plane(meshRoot, "host:sender");
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "late" }))
      .rejects.toThrow("Fabric control command expired");
    expect(receive).not.toHaveBeenCalled();
    expect(claimedKey && receiver.mesh.get(claimedKey)).toBeUndefined();   // nothing committed
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
});
