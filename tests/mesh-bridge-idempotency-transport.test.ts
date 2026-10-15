import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { controlBodyFingerprint, FABRIC_IDEMPOTENCY_KEY_REUSED, FabricIdempotencyKeyReusedError } from "../src/mesh/control-idempotency.js";
import { MeshStore } from "../src/mesh/store.js";

// Trust-boundary/data-safety regression for final-audit's same-key/different-input finding.
// Isolated scratch only; no bridge lifecycle, product stop/withdraw or file-removal calls.
// Scratch cleanup belongs to the parent runner's temporary-directory scope.
const fixture = () => {
  const mesh = new MeshStore(fs.mkdtempSync(path.join(os.tmpdir(), "bridge-idempotency-transport-")), 65536, 500);
  return { mesh, side: new StoreBridgeSide(mesh, "hub") };
};
const command = (attempt = 1): BridgePublish => ({
  topic: "fabric.control.command", kind: "followUp",
  from: { id: "session:source-id", name: "source", kind: "main" }, to: "session:destination-id",
  principal: { id: "principal:source", binding: "herdr-client" },
  data: {
    version: 1, commandId: "stable-command-key", targetId: "session:destination-id", operation: "followUp",
    ownerIncarnation: "owner-epoch", destinationRemoteHost: "destination",
    message: "PRIVATE original body", data: { config: { alpha: 1, omega: 2 }, list: [{ first: true, second: false }, 3] },
    replyTo: "session:source-id", requestedAt: 1000 + attempt, requestCreatedAt: 900 + attempt, deadlineAt: 5000 + attempt,
    bridge: { from: "source", id: `source-attempt-${attempt}`, via: "hub" },
  },
});
const reverseKeys = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(reverseKeys);
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).reverse().map(([key, item]) => [key, reverseKeys(item)]));
  }
  return value;
};
const equivalentRetry = (): BridgePublish => reverseKeys(command(2)) as BridgePublish;
const conflict = async (publication: Promise<unknown>): Promise<FabricIdempotencyKeyReusedError> => {
  const result = await publication.then(() => undefined, (error: unknown) => error);
  expect(result).toBeInstanceOf(FabricIdempotencyKeyReusedError);
  expect(result).toMatchObject({ name: "FabricIdempotencyKeyReusedError", code: FABRIC_IDEMPOTENCY_KEY_REUSED,
    retryable: false, message: "Fabric idempotency key reused with different input" });
  return result as FabricIdempotencyKeyReusedError;
};

const mutations: Array<[string, (event: BridgePublish) => BridgePublish]> = [
  ["message", event => ({ ...event, data: { ...event.data, message: "PRIVATE changed body" } })],
  ["nested data", event => ({ ...event, data: { ...event.data, data: { config: { alpha: 1, omega: 99 }, list: [{ first: true, second: false }, 3] } } })],
  ["principal", event => ({ ...event, principal: { id: "principal:other", binding: "herdr-client" } })],
];

describe("destination control idempotency transport", () => {
  it("atomically dedupes the same canonical body across key order, attempt metadata and an untrusted fingerprint", async () => {
    const { mesh, side } = fixture();
    const original = command();
    const retry = Object.assign(equivalentRetry(), { dedupeFingerprint: "untrusted-wire-value" });
    const [first, second] = await Promise.all([side.publish(original, [], true), side.publish(retry, [], true)]);
    expect(first.sequence).toBeGreaterThan(0);
    expect(second).toEqual(first);
    const events = mesh.read();
    expect(events).toHaveLength(1);
    expect(events[0]!.sequence).toBe(first.sequence);
    expect(events[0]!.dedupeFingerprint).toBe(controlBodyFingerprint(original.data, original.principal));
    expect(events[0]!.dedupeFingerprint).toMatch(/^[a-f0-9]{64}$/);
    expect(events[0]!.dedupeFingerprint).not.toBe("untrusted-wire-value");
  });

  it.each(mutations)("rejects a changed %s under the stable key, without replacing the original", async (_field, mutate) => {
    const { mesh, side } = fixture();
    const first = await side.publish(command(), [], true);
    const before = mesh.read();
    await conflict(side.publish(mutate(command(2)), [], true));
    expect(mesh.read()).toEqual(before);
    expect(await side.publish(equivalentRetry(), [], true)).toEqual(first);
    expect(mesh.read()).toEqual(before);
  });

  it("does not add a body fingerprint to noncommands", async () => {
    const { mesh, side } = fixture();
    await side.publish({ ...command(), topic: "fabric.control.ack", kind: "ack" }, [], true);
    expect(mesh.read()).toHaveLength(1);
    expect(mesh.read()[0]).not.toHaveProperty("dedupeFingerprint");
  });

  it.each(["singleton", "batch"] as const)("preserves typed body-free conflicts through the real stdio %s RPC", async route => {
    const { mesh, side } = fixture();
    const input = new PassThrough(), output = new PassThrough();
    const frames: Array<Record<string, unknown>> = [];
    output.on("data", (chunk: string | Buffer) => {
      for (const line of String(chunk).trim().split("\n")) if (line) frames.push(JSON.parse(line) as Record<string, unknown>);
    });
    const serving = serveBridgeAgent(side, input, output);
    const remote = new RemoteBridgeSide(output, input);
    try {
      await remote.hello();
      expect(remote.publishBatch).toBeTypeOf("function");
      const publish = (event: BridgePublish): Promise<unknown> => route === "singleton"
        ? remote.publish(event) : remote.publishBatch!([{ event }]);
      const first = await publish(command());
      const before = mesh.read();
      expect(before).toHaveLength(1);
      expect(await publish(equivalentRetry())).toEqual(first);
      const changed = mutations[1]![1](command(3));
      const error = await conflict(publish(changed));
      expect(error).not.toHaveProperty("cause");
      expect(error).not.toHaveProperty("data");
      expect(error).not.toHaveProperty("dedupeFingerprint");
      expect((error as Error & { idempotencyKey?: unknown }).idempotencyKey).toBeUndefined();
      const refusal = [...frames].reverse().find(frame => frame.ok === false);
      expect(refusal).toEqual({ id: expect.any(Number), ok: false, code: FABRIC_IDEMPOTENCY_KEY_REUSED,
        error: "Fabric idempotency key reused with different input" });
      expect(JSON.stringify(refusal)).not.toContain("PRIVATE");
      expect(JSON.stringify(refusal)).not.toContain(before[0]!.dedupeFingerprint!);
      expect(mesh.read()).toEqual(before);
      // A conflict neither closes the transport nor poisons unchanged retries.
      expect(await publish(equivalentRetry())).toEqual(first);
      expect(mesh.read()).toEqual(before);
    } finally {
      input.end();
      await serving;
      output.end();
    }
  });

  it("does not silently acknowledge a conflicting batch suffix after a matching receipt", async () => {
    // Keep the suffix inside the batch's work-time budget on a busy parent runner.
    const clock = vi.spyOn(performance, "now").mockReturnValue(0);
    const { mesh, side } = fixture();
    const input = new PassThrough(), output = new PassThrough();
    const serving = serveBridgeAgent(side, input, output);
    const remote = new RemoteBridgeSide(output, input);
    try {
      await remote.hello();
      await remote.publish(command());
      const before = mesh.read();
      // A matching first item cannot convert the second item's typed conflict into a
      // successful prefix response. The store must preserve the exact failure code.
      await conflict(remote.publishBatch!([
        { event: equivalentRetry() }, { event: mutations[1]![1](command(3)) },
      ]));
      expect(mesh.read()).toEqual(before);
      expect(await remote.publish(equivalentRetry())).toEqual({ sequence: before[0]!.sequence });
    } finally { clock.mockRestore(); input.end(); await serving; output.end(); }
  });

  it("does not copy peer error properties or message into an exact-code typed RPC refusal", async () => {
    const input = new PassThrough(), output = new PassThrough();
    const remote = new RemoteBridgeSide(output, input);
    input.once("data", (chunk: string | Buffer) => {
      const request = JSON.parse(String(chunk)) as { id: number };
      output.write(`${JSON.stringify({ id: request.id, ok: false, code: FABRIC_IDEMPOTENCY_KEY_REUSED,
        error: "PRIVATE arbitrary peer message", retryable: true, cause: { secret: "PRIVATE" }, idempotencyKey: "PRIVATE key" })}\n`);
    });
    try {
      const error = await conflict(remote.publish(command()));
      expect(error).not.toHaveProperty("cause");
      expect((error as Error & { idempotencyKey?: unknown }).idempotencyKey).toBeUndefined();
      expect(error.message).not.toContain("PRIVATE");
    } finally { input.end(); output.end(); }
  });
});
