import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import {
  FabricControlPlane,
  STALE_INCARNATION_ERROR,
} from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { controlProviderParticipant } from "../src/topology/provider-participants.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];

const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });

const tempMesh = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-incarnation-"));
  roots.push(root);
  return path.join(root, "mesh");
};

const store = (meshRoot: string): MeshStore => new MeshStore(meshRoot, 64 * 1024, 1_000);

const plane = (meshRoot: string, id: string): FabricControlPlane => {
  const value = new FabricControlPlane(store(meshRoot), identity(id), {
    enabled: true,
    hostId: id,
    pollMs: 20,
    acknowledgementTimeoutMs: 1_000,
  });
  planes.push(value);
  return value;
};

const providerRecord = (id: string, hostId: string): FabricParticipantRecord => ({
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
const ownerDirectory = (
  meshRoot: string,
  hostId: string,
  ownerIncarnation: string | undefined,
  records: () => FabricParticipantRecord[],
): ParticipantDirectory => {
  const directory = new ParticipantDirectory(store(meshRoot), {
    enabled: true,
    hostId,
    rootId: hostId,
    identity: identity(hostId),
    ...(ownerIncarnation ? { ownerIncarnation } : {}),
  });
  directory.registerSource(records);
  return directory;
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map((value) => value.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("FabricControlPlane owner incarnation", () => {
  it("gives every plane instance a distinct incarnation", () => {
    const meshRoot = tempMesh();
    const first = plane(meshRoot, "host:owner");
    const second = plane(meshRoot, "host:owner");
    expect(first.incarnation).toMatch(/^[0-9a-f-]{36}$/);
    expect(second.incarnation).not.toBe(first.incarnation);
  });

  it("executes a command addressed to the current incarnation", async () => {
    const meshRoot = tempMesh();
    const sender = plane(meshRoot, "host:sender");
    const owner = plane(meshRoot, "host:owner");
    const handler = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: "local:" + command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    owner.start(handler);

    await expect(
      sender.request("host:owner", "agent:target", "steer", {
        message: "focus",
        ownerIncarnation: owner.incarnation,
      }),
    ).resolves.toMatchObject({ acknowledged: true, messageId: expect.stringMatching(/^local:/) });
    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ ownerIncarnation: owner.incarnation }),
      expect.anything(),
      expect.anything(),
      "mesh",
    );
    expect(store(meshRoot).read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({ accepted: true, ownerIncarnation: owner.incarnation }),
      }),
    );
  });

  it("refuses a replayed command naming a previous incarnation without executing it", async () => {
    const meshRoot = tempMesh();
    const mesh = store(meshRoot);
    const previous = plane(meshRoot, "host:owner");
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
    const restarted = plane(meshRoot, "host:owner");
    const handler = vi.fn(() => ({ accepted: true }));
    restarted.start(handler);
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(handler).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:stale",
          accepted: false,
          error: STALE_INCARNATION_ERROR,
          ownerIncarnation: restarted.incarnation,
          staleIncarnation: previousIncarnation,
        }),
      }),
    );
    expect(mesh.listAll("topology/control-seen/")).toEqual([]);
  });

  it("fails a live request to a restarted owner fast with the fencing error", async () => {
    const meshRoot = tempMesh();
    const sender = plane(meshRoot, "host:sender");
    const previous = plane(meshRoot, "host:owner");
    const previousIncarnation = previous.incarnation;
    await previous.close();
    const restarted = plane(meshRoot, "host:owner");
    const handler = vi.fn(() => ({ accepted: true }));
    sender.start(() => ({ accepted: false }));
    restarted.start(handler);

    const startedAt = Date.now();
    await expect(
      sender.requestResult("host:owner", "actor:durable", "ask", {
        message: "late",
        ownerIncarnation: previousIncarnation,
      }),
    ).rejects.toThrow(STALE_INCARNATION_ERROR);
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(handler).not.toHaveBeenCalled();
  });

  it("still executes commands that name no incarnation after a restart", async () => {
    const meshRoot = tempMesh();
    const sender = plane(meshRoot, "host:sender");
    await plane(meshRoot, "host:owner").close();
    const restarted = plane(meshRoot, "host:owner");
    const handler = vi.fn((_command: { ownerIncarnation?: string }) => ({
      accepted: true,
      messageId: "legacy",
    }));
    sender.start(() => ({ accepted: false }));
    restarted.start(handler);

    await expect(
      sender.request("host:owner", "agent:target", "followUp", { message: "older sender" }),
    ).resolves.toMatchObject({ messageId: "legacy" });
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0]![0]).not.toHaveProperty("ownerIncarnation");
  });

  it("ignores an acknowledgement from a different incarnation of the owner", async () => {
    const meshRoot = tempMesh();
    const mesh = store(meshRoot);
    const sender = plane(meshRoot, "host:sender");
    const owner = plane(meshRoot, "host:owner");
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
          data: {
            version: 1,
            commandId: command.commandId,
            targetId: command.targetId,
            ownerIncarnation: "incarnation:other",
            ...extra,
          },
        });
      }
      return { accepted: true, messageId: "current:" + command.commandId };
    });

    await expect(
      sender.request("host:owner", "agent:target", "steer", {
        message: "focus",
        ownerIncarnation: owner.incarnation,
      }),
    ).resolves.toMatchObject({ messageId: expect.stringMatching(/^current:/) });
  });

  it("rejects a malformed incarnation before publishing", async () => {
    const meshRoot = tempMesh();
    const sender = plane(meshRoot, "host:sender");
    sender.start(() => ({ accepted: false }));
    await expect(
      sender.request("host:owner", "agent:target", "stop", { ownerIncarnation: "x".repeat(129) }),
    ).rejects.toThrow("Invalid Fabric owner incarnation");
    expect(store(meshRoot).read({ topic: "fabric.control.command", limit: 10 })).toEqual([]);
  });
});

describe("participant records carry the owner incarnation", () => {
  it("stamps, validates and re-resolves the incarnation across an owner restart", async () => {
    const meshRoot = tempMesh();
    const ref = "provider:demo:1";
    const requester = plane(meshRoot, "host:requester");
    requester.start(() => ({ accepted: false }));
    const viewer = new ParticipantDirectory(store(meshRoot), {
      enabled: true,
      hostId: "host:requester",
      rootId: "host:requester",
      identity: identity("host:requester"),
    });

    const previous = plane(meshRoot, "host:owner");
    await ownerDirectory(meshRoot, "host:owner", previous.incarnation, () => [
      providerRecord(ref, "host:owner"),
    ]).refresh();
    expect(viewer.get(ref)).toMatchObject({ ownerIncarnation: previous.incarnation });
    // The owner crashes: its records outlive the process until the new one republishes.
    await previous.close();

    const restarted = plane(meshRoot, "host:owner");
    const handler = vi.fn(() => ({ accepted: true, result: { stopped: true } }));
    restarted.start(handler);
    await expect(
      controlProviderParticipant(undefined, viewer, requester, ref, "stop"),
    ).rejects.toThrow(STALE_INCARNATION_ERROR);
    expect(handler).not.toHaveBeenCalled();

    await ownerDirectory(meshRoot, "host:owner", restarted.incarnation, () => [
      providerRecord(ref, "host:owner"),
    ]).refresh();
    expect(viewer.get(ref)).toMatchObject({ ownerIncarnation: restarted.incarnation });
    await expect(
      controlProviderParticipant(undefined, viewer, requester, ref, "stop"),
    ).resolves.toMatchObject({ acknowledged: true });
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("keeps records without an incarnation valid and drops malformed ones", async () => {
    const meshRoot = tempMesh();
    const mesh = store(meshRoot);
    await ownerDirectory(meshRoot, "host:legacy", undefined, () => [
      providerRecord("provider:legacy:1", "host:legacy"),
    ]).refresh();
    const viewer = new ParticipantDirectory(store(meshRoot), {
      enabled: true,
      hostId: "host:viewer",
      rootId: "host:viewer",
      identity: identity("host:viewer"),
    });
    const legacy = viewer.get("provider:legacy:1");
    expect(legacy).toBeDefined();
    expect(legacy).not.toHaveProperty("ownerIncarnation");

    const entry = mesh
      .listAll("topology/participants/")
      .find((candidate) => (candidate.value as { id?: string }).id === "provider:legacy:1")!;
    await mesh.put({
      key: entry.key,
      value: { ...(entry.value as object), ownerIncarnation: 42 },
      identity: identity("host:legacy"),
      ifVersion: entry.version,
    });
    expect(viewer.get("provider:legacy:1")).toBeUndefined();
  });

  it("does not let a source record override the owner's incarnation", async () => {
    const meshRoot = tempMesh();
    await ownerDirectory(meshRoot, "host:owner", "incarnation:current", () => [
      { ...providerRecord("provider:demo:2", "host:owner"), ownerIncarnation: "incarnation:forged" },
    ]).refresh();
    const viewer = new ParticipantDirectory(store(meshRoot), {
      enabled: true,
      hostId: "host:viewer",
      rootId: "host:viewer",
      identity: identity("host:viewer"),
    });
    expect(viewer.get("provider:demo:2")).toMatchObject({ ownerIncarnation: "incarnation:current" });
  });
});
