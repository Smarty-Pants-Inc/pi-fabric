import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity, type MeshPublishInput } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import {
  CALLER_PUBLISH_KEY_MAX_LENGTH,
  CALLER_PUBLISH_KEY_RETENTION_MS,
  namespacedPublishKey,
  publishWithCallerKey,
} from "../src/actors/caller-publish-key.js";

const roots: string[] = [];
const context = {} as FabricInvocationContext;
const source: FabricParticipantSource = {
  list: () => [], get: () => undefined, self: () => undefined as never, peers: () => [],
  async refresh() {}, scheduleRefresh() {},
};
const actor = (id: string): MeshIdentity => ({ id, name: id, kind: "actor" });

const meshRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-caller-key-"));
  roots.push(root);
  return path.join(root, "mesh");
};
const storeAt = (root: string): MeshStore => new MeshStore(root, 64 * 1024, 100);

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("caller-keyed idempotent actor publish (smarty-dev#5138)", () => {
  it("gives one event and the same receipt for a duplicate key", async () => {
    const store = storeAt(meshRoot());
    const provider = new MeshProvider(store, actor("actor:oom"), source);
    const args = { topic: "ops.oom", to: "session:main", kind: "notice", text: "OOM on ryzen5", key: "oom:ryzen5:1" };
    const first = await provider.invoke("publish", args, context) as MeshEvent;
    const second = await provider.invoke("publish", args, context) as MeshEvent;
    expect(second.id).toBe(first.id);
    expect(second.sequence).toBe(first.sequence);
    expect(second.createdAt).toBe(first.createdAt);
    expect(store.read({ topic: "ops.oom" })).toHaveLength(1);
    // Unkeyed publishes are unchanged: each one is a new event.
    await provider.invoke("publish", { topic: "ops.oom", text: "a" }, context);
    await provider.invoke("publish", { topic: "ops.oom", text: "a" }, context);
    expect(store.read({ topic: "ops.oom" })).toHaveLength(3);
  });

  it("delivers once when the publish took effect and then raised, and the caller retries", async () => {
    const root = meshRoot();
    class RaisingStore extends MeshStore {
      raised = false;
      override async publish(input: MeshPublishInput): Promise<MeshEvent> {
        const event = await super.publish(input);
        if (!this.raised) { this.raised = true; throw new Error("transport lost after commit"); }
        return event;
      }
    }
    const store = new RaisingStore(root, 64 * 1024, 100);
    const provider = new MeshProvider(store, actor("actor:oom"), source);
    const args = { topic: "ops.oom", text: "OOM", key: "k1" };
    await expect(provider.invoke("publish", args, context)).rejects.toThrow("transport lost");
    const retried = await provider.invoke("publish", args, context) as MeshEvent;
    const events = store.read({ topic: "ops.oom" });
    expect(events).toHaveLength(1);
    expect(retried.id).toBe(events[0]!.id);
  });

  it("delivers once when the caller restarts before saving its receipt and retries", async () => {
    const root = meshRoot();
    const before = await new MeshProvider(storeAt(root), actor("actor:oom"), source)
      .invoke("publish", { topic: "ops.oom", text: "OOM", key: "k1" }, context) as MeshEvent;
    // A new process: a fresh store and provider on the same mesh root.
    const store = storeAt(root);
    const after = await new MeshProvider(store, actor("actor:oom"), source)
      .invoke("publish", { topic: "ops.oom", text: "OOM", key: "k1" }, context) as MeshEvent;
    expect(after.id).toBe(before.id);
    expect(store.read({ topic: "ops.oom" })).toHaveLength(1);
  });

  it("does not collide keys from different actors", async () => {
    const store = storeAt(meshRoot());
    const a = new MeshProvider(store, actor("actor:a"), source);
    const b = new MeshProvider(store, actor("actor:b"), source);
    const fromA = await a.invoke("publish", { topic: "team.x", text: "a", key: "same" }, context) as MeshEvent;
    const fromB = await b.invoke("publish", { topic: "team.x", text: "b", key: "same" }, context) as MeshEvent;
    expect(fromB.id).not.toBe(fromA.id);
    expect(fromB.from.id).toBe("actor:b");
    const events = store.read({ topic: "team.x" });
    expect(events.map((event) => event.text)).toEqual(["a", "b"]);
    // The namespace is JSON-encoded: no id/key spelling can forge another actor's namespace.
    expect(namespacedPublishKey(actor("actor:a\",\"x"), "k")).not.toBe(namespacedPublishKey(actor("actor:a"), "x\",\"k"));
    expect(namespacedPublishKey({ id: "x", kind: "main" }, "k")).not.toBe(namespacedPublishKey(actor("x"), "k"));
  });

  it("refuses an over-long or empty key without publishing", async () => {
    const store = storeAt(meshRoot());
    const provider = new MeshProvider(store, actor("actor:a"), source);
    await expect(provider.invoke("publish", { topic: "team.x", text: "t", key: "k".repeat(CALLER_PUBLISH_KEY_MAX_LENGTH + 1) }, context))
      .rejects.toThrow(`exceeds ${CALLER_PUBLISH_KEY_MAX_LENGTH} characters`);
    await expect(provider.invoke("publish", { topic: "team.x", text: "t", key: "  " }, context)).rejects.toThrow("non-empty string");
    await expect(provider.invoke("publish", { topic: "team.x", text: "t", key: 7 }, context)).rejects.toThrow("non-empty string");
    expect(store.read({ topic: "team.x" })).toHaveLength(0);
    await provider.invoke("publish", { topic: "team.x", text: "t", key: "k".repeat(CALLER_PUBLISH_KEY_MAX_LENGTH) }, context);
    expect(store.read({ topic: "team.x" })).toHaveLength(1);
    const schema = (await provider.describe("publish", context))!.inputSchema as { properties: Record<string, { maxLength?: number }> };
    expect(schema.properties.key!.maxLength).toBe(CALLER_PUBLISH_KEY_MAX_LENGTH);
  });

  it("refuses a live key reused for a different route", async () => {
    const store = storeAt(meshRoot());
    const provider = new MeshProvider(store, actor("actor:a"), source);
    await provider.invoke("publish", { topic: "team.x", text: "t", key: "k" }, context);
    await expect(provider.invoke("publish", { topic: "team.y", text: "t", key: "k" }, context)).rejects.toThrow("already used");
    await expect(provider.invoke("publish", { topic: "team.x", to: "session:m", text: "t", key: "k" }, context)).rejects.toThrow("already used");
    expect(store.read({ topic: "team.y" })).toHaveLength(0);
  });

  it("honors a key for its retention window and publishes again after it", async () => {
    expect(CALLER_PUBLISH_KEY_RETENTION_MS).toBeGreaterThanOrEqual(24 * 60 * 60 * 1000);
    const store = storeAt(meshRoot());
    const input = { topic: "team.x", text: "t", from: actor("actor:a") };
    const publish = (keyed: MeshPublishInput) => store.publish(keyed);
    const first = await publishWithCallerKey(publish, input, "k");
    const inside = await publishWithCallerKey(publish, input, "k", { now: () => first.createdAt + CALLER_PUBLISH_KEY_RETENTION_MS - 1 });
    expect(inside.id).toBe(first.id);
    const later = first.createdAt + CALLER_PUBLISH_KEY_RETENTION_MS + 1;
    const after = await publishWithCallerKey(publish, input, "k", { now: () => later });
    expect(after.id).not.toBe(first.id);
    // The new generation is itself idempotent.
    const again = await publishWithCallerKey(publish, input, "k", { now: () => later });
    expect(again.id).toBe(after.id);
    expect(store.read({ topic: "team.x" })).toHaveLength(2);
  });
});
