import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  controlBodyFingerprint,
  FABRIC_IDEMPOTENCY_KEY_REUSED,
  FabricIdempotencyKeyReusedError,
} from "../src/mesh/control-idempotency.js";
import { MeshStore, type MeshEvent, type MeshIdentity, type MeshPublishInput } from "../src/mesh/store.js";

// Keep reason: data safety/trust boundary. A durable receipt must never ACK another body.
// One real file-backed MeshStore, isolated in scratch. No product teardown, close, compaction
// invocation or fixture removal; the parent owns execution and scratch lifecycle.
const mesh = new MeshStore(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-idempotency-")), 64 * 1024, 100,
  { stateBackend: "file", canWakeResidents: () => false });
const from: MeshIdentity = { id: "session:idempotency-test", name: "idempotency-test", kind: "main" };
const body = { targetId: "actor:owner", operation: "followUp", message: "original-private-body", data: { a: 1, b: { c: 2 } } };
let keyCounter = 0;
const packet = (command: unknown = body): MeshPublishInput & { dedupeKey: string } => ({
  topic: "mesh.control.idempotency", from, data: command,
  dedupeKey: `control-idempotency-${++keyCounter}`, dedupeFingerprint: controlBodyFingerprint(command),
});
const receipt = (key: string, suffix = ".json"): string =>
  path.join(mesh.root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const liveText = (): string => fs.readFileSync(path.join(mesh.root, "events.jsonl"), "utf8");
const eventsFor = (key: string): MeshEvent[] => mesh.read({ after: 0 }).filter(event => event.dedupeKey === key);
const mismatch = async (result: Promise<unknown>): Promise<void> => {
  await expect(result).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
  await expect(result).rejects.toMatchObject({
    name: "FabricIdempotencyKeyReusedError", code: FABRIC_IDEMPOTENCY_KEY_REUSED, retryable: false,
    message: "Fabric idempotency key reused with different input",
  });
};

/** Fail only receipt installation after a complete real append; leave its crash intent intact. */
const strandIntent = async (input: MeshPublishInput & { dedupeKey: string }): Promise<MeshEvent> => {
  const rename = fs.renameSync.bind(fs);
  const fail = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
    if (target === receipt(input.dedupeKey)) throw new Error("test receipt installation failure");
    rename(source, target);
  });
  try { await expect(mesh.publish(input)).rejects.toThrow("test receipt installation failure"); }
  finally { fail.mockRestore(); }
  expect(fs.existsSync(receipt(input.dedupeKey))).toBe(false);
  expect(fs.existsSync(receipt(input.dedupeKey, ".pending.json"))).toBe(true);
  const committed = eventsFor(input.dedupeKey);
  expect(committed).toHaveLength(1);
  return committed[0]!;
};

afterEach(() => { vi.restoreAllMocks(); });

describe("controlBodyFingerprint", () => {
  it("hashes recursively sorted wire JSON, not input property order", () => {
    const first = { targetId: "actor:owner", operation: "followUp", message: "hi", data: { b: 2, a: 1 } };
    const second = { data: { a: 1, b: 2 }, message: "hi", operation: "followUp", targetId: "actor:owner" };
    const canonical = '{"data":{"a":1,"b":2},"message":"hi","operation":"followUp","principal":{"a":"first","z":"last"},"targetId":"actor:owner"}';
    expect(controlBodyFingerprint(first, { z: "last", a: "first" })).toBe(createHash("sha256").update(canonical).digest("hex"));
    expect(controlBodyFingerprint(second, { a: "first", z: "last" })).toBe(controlBodyFingerprint(first, { z: "last", a: "first" }));
    expect(controlBodyFingerprint(first)).toMatch(/^[a-f0-9]{64}$/);
  });

  it("sorts nested objects and bindings while retaining array order and literal special keys", () => {
    const data = JSON.parse('{"10":"ten","2":"two","__proto__":{"z":2,"a":1},"items":[{"z":2,"a":1},3]}');
    const reordered = JSON.parse('{"items":[{"a":1,"z":2},3],"__proto__":{"a":1,"z":2},"2":"two","10":"ten"}');
    const first = { ...body, data, binding: { z: { b: 2, a: 1 }, a: 0 }, bindingProvenance: { rootId: "r", kind: "owner-defaults" } };
    const second = { ...body, data: reordered, binding: { a: 0, z: { a: 1, b: 2 } }, bindingProvenance: { kind: "owner-defaults", rootId: "r" } };
    expect(controlBodyFingerprint(first)).toBe(controlBodyFingerprint(second));
    expect(controlBodyFingerprint({ ...body, data: [1, 2] })).not.toBe(controlBodyFingerprint({ ...body, data: [2, 1] }));
    const canonical = '{"data":{"10":"ten","2":"two","__proto__":{"a":1,"z":2},"items":[{"a":1,"z":2},3]}}';
    expect(controlBodyFingerprint({ data })).toBe(createHash("sha256").update(canonical).digest("hex"));
  });

  it("retains JSON serialization semantics before sorting", () => {
    const wire = {
      data: { list: [null, null, null, null], date: "2026-01-02T03:04:05.000Z", number: null },
      binding: { a: 1, z: 2 },
    };
    const input = {
      message: undefined,
      data: { omitted: undefined, number: NaN, list: [undefined, NaN, Infinity, ,], date: new Date(wire.data.date) },
      binding: { toJSON: () => ({ z: 2, a: 1 }) },
    };
    expect(controlBodyFingerprint(input)).toBe(controlBodyFingerprint(wire));
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(() => controlBodyFingerprint({ data: cyclic })).toThrow(TypeError);
    expect(() => controlBodyFingerprint({ data: 1n })).toThrow(TypeError);
  });

  it("matches fresh attempt stamps/times and separately resolved epochs/routes", () => {
    const attempt = {
      ...body, commandId: "attempt-a", eventId: "event-a", replyTo: "reply-a", attemptId: "uuid-a",
      requestedAt: 1, requestCreatedAt: 2, deadlineAt: 3, bridge: { origin: "a" },
      sender: { sessionId: "a" }, ownerIncarnation: "old-epoch", destinationRemoteHost: "remote-a",
    };
    const retry = {
      ...body, commandId: "attempt-b", eventId: "event-b", replyTo: "reply-b", attemptId: "uuid-b",
      requestedAt: 4, requestCreatedAt: 5, deadlineAt: 6, bridge: { origin: "b" },
      sender: { sessionId: "b" }, ownerIncarnation: "new-epoch", destinationRemoteHost: null,
    };
    expect(controlBodyFingerprint(attempt)).toBe(controlBodyFingerprint(retry));
  });

  it.each([
    ["targetId", "actor:other"], ["operation", "steer"], ["message", "changed-private-body"],
    ["data", { a: 1, b: { c: 3 } }], ["wakeCause", { kind: "other" }], ["triggerTurn", false],
    ["priority", "urgent"], ["binding", { rootId: "other" }], ["bindingProvenance", { rootId: "other" }],
  ])("binds semantic field %s", (field, value) => {
    expect(controlBodyFingerprint({ ...body, [field as string]: value })).not.toBe(controlBodyFingerprint(body));
  });

  it("binds the separately captured principal", () => {
    expect(controlBodyFingerprint(body, { id: "a" })).not.toBe(controlBodyFingerprint(body, { id: "b" }));
  });

  it("exports the exact body-free nonretryable error with only a bounded optional key", () => {
    const error = new FabricIdempotencyKeyReusedError("k".repeat(500));
    expect(error.name).toBe("FabricIdempotencyKeyReusedError");
    expect(error.code).toBe("FABRIC_IDEMPOTENCY_KEY_REUSED");
    expect(error.retryable).toBe(false);
    expect(error.message).toBe("Fabric idempotency key reused with different input");
    expect(error.idempotencyKey).toBe("k".repeat(200));
    expect(Object.hasOwn(error, "cause")).toBe(false);
    expect(new FabricIdempotencyKeyReusedError().idempotencyKey).toBeUndefined();
  });
});

describe("MeshStore fingerprinted durable dedupe", () => {
  it.each([false, true])("persists fingerprint with exact event-line and receipt parity (stamp=%s)", async stamp => {
    const input = packet();
    if (stamp) input.data = (createdAt: number) => ({ ...body, requestedAt: createdAt });
    const event = await mesh.publish(input);
    expect(event.dedupeFingerprint).toBe(input.dedupeFingerprint);
    const line = liveText().trimEnd().split("\n").at(-1)!;
    expect(line).toBe(JSON.stringify(event));
    expect(fs.readFileSync(receipt(input.dedupeKey), "utf8")).toBe(line);
    expect(eventsFor(input.dedupeKey)).toEqual([event]);
  });

  it("same key and reordered body dedupe; changed nested body rejects without append or receipt change", async () => {
    const input = packet();
    const first = await mesh.publish(input);
    const before = liveText(), beforeSequence = mesh.latestSequence();
    const beforeReceipt = fs.readFileSync(receipt(input.dedupeKey), "utf8");
    const reordered = { data: { b: { c: 2 }, a: 1 }, message: body.message, operation: body.operation, targetId: body.targetId, requestedAt: 999 };
    expect(await mesh.publish({ ...input, data: reordered, dedupeFingerprint: controlBodyFingerprint(reordered) })).toEqual(first);
    const changed = { ...body, data: { a: 1, b: { c: 3 } } };
    await mismatch(mesh.publish({ ...input, data: changed, dedupeFingerprint: controlBodyFingerprint(changed) }));
    expect(liveText()).toBe(before);
    expect(mesh.latestSequence()).toBe(beforeSequence);
    expect(fs.readFileSync(receipt(input.dedupeKey), "utf8")).toBe(beforeReceipt);
    expect(eventsFor(input.dedupeKey)).toEqual([first]);
    expect(first.data).toEqual(body);
  });

  it("a legacy receipt without fingerprint cannot match a fingerprinted retry", async () => {
    const input = packet();
    const { dedupeFingerprint: _fingerprint, ...legacy } = input;
    const first = await mesh.publish(legacy);
    const before = liveText();
    await mismatch(mesh.publish(input));
    expect(liveText()).toBe(before);
    expect(eventsFor(input.dedupeKey)).toEqual([first]);
    expect(JSON.parse(fs.readFileSync(receipt(input.dedupeKey), "utf8"))).toEqual(first);
  });

  it.each([false, true])("generic callers without incoming fingerprint retain key-only behavior (saved=%s)", async saved => {
    const input = packet();
    const { dedupeFingerprint: _fingerprint, ...generic } = input;
    const first = await mesh.publish(saved ? input : generic);
    expect(await mesh.publish({ ...generic, data: { changed: true } })).toEqual(first);
    expect(eventsFor(input.dedupeKey)).toEqual([first]);
  });

  it.each(["", "A".repeat(64), "g".repeat(64), "a".repeat(63), "a".repeat(65), 42, null])("validates fingerprint %s before append", async fingerprint => {
    const input = packet();
    const before = liveText(), beforeSequence = mesh.latestSequence();
    await expect(mesh.publish({ ...input, dedupeFingerprint: fingerprint as string })).rejects.toThrow("Mesh dedupe fingerprint");
    expect(liveText()).toBe(before);
    expect(mesh.latestSequence()).toBe(beforeSequence);
    expect(fs.existsSync(receipt(input.dedupeKey))).toBe(false);
    expect(fs.existsSync(receipt(input.dedupeKey, ".pending.json"))).toBe(false);
  });

  it("requires a nonempty dedupe key when a fingerprint is supplied", async () => {
    const input = packet(), before = liveText();
    const { dedupeKey: _key, ...unkeyed } = input;
    await expect(mesh.publish(unkeyed)).rejects.toThrow("requires a dedupe key");
    await expect(mesh.publish({ ...input, dedupeKey: "" })).rejects.toThrow("requires a dedupe key");
    expect(liveText()).toBe(before);
  });

  it("checks recovered live intent before accepting another body, then retains fingerprint in its recovered receipt", async () => {
    const input = packet(), first = await strandIntent(input);
    const before = liveText(), beforeSequence = mesh.latestSequence();
    const pendingPath = receipt(input.dedupeKey, ".pending.json"), pendingBefore = fs.readFileSync(pendingPath, "utf8");
    const changed = { ...body, message: "changed-private-body" };
    await mismatch(mesh.publish({ ...input, data: changed, dedupeFingerprint: controlBodyFingerprint(changed) }));
    expect(liveText()).toBe(before);
    expect(mesh.latestSequence()).toBe(beforeSequence);
    expect(fs.readFileSync(pendingPath, "utf8")).toBe(pendingBefore);
    expect(fs.existsSync(receipt(input.dedupeKey))).toBe(false);
    expect(await mesh.publish(input)).toEqual(first);
    expect(liveText()).toBe(before);
    expect(JSON.parse(fs.readFileSync(receipt(input.dedupeKey), "utf8"))).toEqual(first);
    expect(first.dedupeFingerprint).toBe(input.dedupeFingerprint);
    expect(eventsFor(input.dedupeKey)).toEqual([first]);
  });

  it("fails closed for a legacy live intent with absent fingerprint", async () => {
    const input = packet();
    const { dedupeFingerprint: _fingerprint, ...legacy } = input;
    const first = await strandIntent(legacy);
    const before = liveText(), pendingPath = receipt(input.dedupeKey, ".pending.json");
    const pendingBefore = fs.readFileSync(pendingPath, "utf8");
    await mismatch(mesh.publish(input));
    expect(liveText()).toBe(before);
    expect(fs.readFileSync(pendingPath, "utf8")).toBe(pendingBefore);
    expect(eventsFor(input.dedupeKey)).toEqual([first]);
  });

  it("also checks a receipt appearing between the initial receipt lookup and intent recovery", async () => {
    const input = packet(), first = await strandIntent(input);
    const pendingPath = receipt(input.dedupeKey, ".pending.json"), before = liveText();
    const pendingBefore = fs.readFileSync(pendingPath, "utf8");
    const read = fs.readFileSync.bind(fs) as (...args: unknown[]) => string | Buffer;
    let installed = false;
    const race = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
      if (!installed && args[0] === pendingPath && fs.existsSync(path.join(mesh.root, ".lock"))) {
        fs.writeFileSync(receipt(input.dedupeKey), JSON.stringify(first));
        installed = true;
      }
      return read(...args);
    }) as typeof fs.readFileSync);
    const changed = { ...body, message: "changed-private-body" };
    try { await mismatch(mesh.publish({ ...input, data: changed, dedupeFingerprint: controlBodyFingerprint(changed) })); }
    finally { race.mockRestore(); }
    expect(installed).toBe(true);
    expect(liveText()).toBe(before);
    expect(fs.readFileSync(pendingPath, "utf8")).toBe(pendingBefore);
    expect(JSON.parse(fs.readFileSync(receipt(input.dedupeKey), "utf8"))).toEqual(first);
    expect(await mesh.publish(input)).toEqual(first);
    expect(liveText()).toBe(before);
  });

  it("concurrent same-key/same-fingerprint attempts reuse one original receipt", async () => {
    const input = packet();
    const results = await Promise.all(Array.from({ length: 6 }, (_, requestedAt) =>
      mesh.publish({ ...input, data: { ...body, requestedAt } })));
    for (const result of results) expect(result).toEqual(results[0]);
    expect(eventsFor(input.dedupeKey)).toEqual([results[0]]);
    expect(JSON.parse(fs.readFileSync(receipt(input.dedupeKey), "utf8"))).toEqual(results[0]);
  });

  it("concurrent same-key/different-fingerprint attempts admit exactly one body", async () => {
    const input = packet(), changed = { ...body, data: { a: 1, b: { c: 3 } } };
    const results = await Promise.allSettled([
      mesh.publish(input), mesh.publish({ ...input, data: changed, dedupeFingerprint: controlBodyFingerprint(changed) }),
    ]);
    const accepted = results.filter((result): result is PromiseFulfilledResult<MeshEvent> => result.status === "fulfilled");
    const refused = results.filter((result): result is PromiseRejectedResult => result.status === "rejected");
    expect(accepted).toHaveLength(1);
    expect(refused).toHaveLength(1);
    expect(refused[0]!.reason).toBeInstanceOf(FabricIdempotencyKeyReusedError);
    expect(refused[0]!.reason).toMatchObject({ code: FABRIC_IDEMPOTENCY_KEY_REUSED, retryable: false });
    expect(eventsFor(input.dedupeKey)).toEqual([accepted[0]!.value]);
    expect(JSON.parse(fs.readFileSync(receipt(input.dedupeKey), "utf8"))).toEqual(accepted[0]!.value);
  });
});
