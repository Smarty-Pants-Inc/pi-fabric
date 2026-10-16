// Fix A (smarty-dev#13112): each JetStream publish attempt is bounded by min(remaining write budget, 1s cap), so a
// silent attempt during an R3 stream-leader transition returns control for a same-ID retry inside the unchanged
// overall write budget. Virtual time: performance.now is a test clock that a silent attempt advances by its timeout.
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const sdk = vi.hoisted(() => ({ publish: vi.fn() }));
vi.mock("@nats-io/transport-node", () => ({
  connect: async () => ({ info: { version: "2.14.7" }, drain: async () => undefined }),
}));
vi.mock("@nats-io/jetstream", async importOriginal => ({
  ...await importOriginal<typeof import("@nats-io/jetstream")>(),
  jetstreamManager: async () => ({}),
  jetstream: () => ({ publish: sdk.publish }),
}));

import { JetStreamApiError } from "@nats-io/jetstream";
import {
  closeNatsStateBackend, MeshStateNatsOutcomeUnknownError, NatsWrongLastSequenceError,
  openNatsStateBackend, type JetStreamPort,
} from "../src/mesh/state-backend-nats.js";
import { connectJetStream } from "../src/mesh/state-backend-nats-client.js";
import { fakeJetStream, type FakeJetStreamServer } from "./helpers/nats-jetstream-fake.js";

const identity = { id: "tester", name: "tester", kind: "agent" as const };
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) closeNatsStateBackend(root); });
const root = () => { const value = path.join(os.tmpdir(), `nats-budget-${Math.random().toString(36).slice(2)}`); roots.push(value); return value; };
const silent = () => Object.assign(new Error("timeout"), { name: "TimeoutError", transient: true });

type Attempt = { timeoutMs: number | undefined; start: number; msgID: string; expect: number; bytes: number[] };
type Behaviour = (attempt: number, call: () => ReturnType<JetStreamPort["publish"]>) => ReturnType<JetStreamPort["publish"]>;
/** A backend over the fake server whose publishes run `behaviour`; returns the attempts it saw on the virtual clock. */
const harness = async (server: FakeJetStreamServer, lockTimeoutMs: number | undefined) => {
  const underlying = server.port();
  const attempts: Attempt[] = [];
  const clock = { now: 1_000_000 };
  let behaviour: Behaviour = (_attempt, call) => call();
  const port: JetStreamPort = { ...underlying, publish: (subject, data, options) => {
    attempts.push({ timeoutMs: options.timeoutMs, start: clock.now, msgID: options.msgID,
      expect: options.expectLastSequence, bytes: [...data] });
    return behaviour(attempts.length, () => underlying.publish(subject, data, options));
  } };
  const backend = await openNatsStateBackend({ root: root(), port, ...(lockTimeoutMs === undefined ? {} : { lockTimeoutMs }) });
  await backend.put({ key: "d/count", value: 0, identity });
  attempts.length = 0;
  vi.spyOn(performance, "now").mockImplementation(() => clock.now);
  const write = () => backend.writeBatch({ identity, ops: [], prepare: view => [{ kind: "put", key: "d/count", value: (view.get("d/count")?.value as number) + 1 }] });
  /** The SDK request timed out: virtual time advances by exactly the attempt's native timeout. */
  const elapse = (attempt: number) => { clock.now += attempts[attempt - 1]!.timeoutMs ?? 5_000; };
  return { backend, attempts, clock, write, elapse, set: (next: Behaviour) => { behaviour = next; } };
};

describe("NATS publish attempt budget (fix A, smarty-dev#13112)", () => {
  it("a silent, uncommitted first attempt is retried with the same id and commits once inside the 5s default", async () => {
    const server = fakeJetStream();
    const h = await harness(server, undefined);
    const before = server.messages.length;
    h.set(async (attempt, call) => { if (attempt === 1) { h.elapse(1); throw silent(); } return call(); });
    await h.write();
    expect(h.attempts.map(attempt => attempt.timeoutMs)).toEqual([1_000, 1_000]);
    expect(h.attempts[1]!.start - h.attempts[0]!.start).toBe(1_000);
    expect(h.attempts[1]).toMatchObject({ msgID: h.attempts[0]!.msgID, expect: h.attempts[0]!.expect, bytes: h.attempts[0]!.bytes });
    expect(server.messages.length).toBe(before + 1);
    expect(h.backend.get("d/count")?.value).toBe(1);
  });

  it("a committed first attempt whose reply was lost gets a duplicate ACK at the same sequence: no extra apply", async () => {
    const server = fakeJetStream();
    const h = await harness(server, undefined);
    const before = server.messages.length;
    // Real clustered 2.14.7 order: the dedupe map answers before the expected-last-sequence check.
    let stored: number | undefined;
    const acks: { seq: number; duplicate: boolean }[] = [];
    h.set(async (attempt, call) => {
      if (attempt === 1) { stored = (await call()).seq; h.elapse(1); throw silent(); }
      const ack = { seq: stored!, duplicate: true };
      acks.push(ack);
      return ack;
    });
    let prepares = 0;
    await h.backend.writeBatch({ identity, ops: [], prepare: view => { prepares++; return [{ kind: "put", key: "d/count", value: (view.get("d/count")?.value as number) + 1 }]; } });
    expect(prepares).toBe(1);
    expect(acks).toEqual([{ seq: before + 1, duplicate: true }]);
    expect(h.attempts[1]).toMatchObject({ msgID: h.attempts[0]!.msgID, expect: h.attempts[0]!.expect, bytes: h.attempts[0]!.bytes });
    expect(server.messages.length).toBe(before + 1);
    expect(h.backend.get("d/count")?.value).toBe(1);
    expect(h.backend.diagnostics().nats?.casRetries).toBe(0);
  });

  it("a permanently silent publish exhausts the unchanged 5s budget as OutcomeUnknown, never past the deadline", async () => {
    const server = fakeJetStream();
    const h = await harness(server, undefined);
    const start = h.clock.now;
    h.set(async attempt => { h.elapse(attempt); throw silent(); });
    await expect(h.write()).rejects.toBeInstanceOf(MeshStateNatsOutcomeUnknownError);
    expect(h.attempts.map(attempt => attempt.timeoutMs)).toEqual([1_000, 1_000, 1_000, 1_000, 1_000]);
    expect(h.attempts.every(attempt => attempt.start < start + 5_000)).toBe(true);
    expect(h.clock.now - start).toBe(5_000);
  });

  it("a non-default short budget caps the last attempt at the remaining budget and starts none after it", async () => {
    const server = fakeJetStream();
    const h = await harness(server, 2_500);
    const start = h.clock.now;
    h.set(async attempt => { h.elapse(attempt); throw silent(); });
    await expect(h.write()).rejects.toBeInstanceOf(MeshStateNatsOutcomeUnknownError);
    expect(h.attempts.map(attempt => attempt.timeoutMs)).toEqual([1_000, 1_000, 500]);
    expect(h.clock.now - start).toBe(2_500);
  });

  it("a permanent publish error is final (no retry)", async () => {
    const server = fakeJetStream();
    const h = await harness(server, undefined);
    h.set(async () => { throw new Error("permission denied"); });
    await expect(h.write()).rejects.toThrow("permission denied");
    expect(h.attempts.length).toBe(1);
  });
});

describe("NATS client publish adapter forwards the attempt timeout", () => {
  it("passes timeoutMs as the SDK timeout and keeps the permanent/transient/CAS classification", async () => {
    const port = await connectJetStream(["nats://127.0.0.1:4222"], { replicas: 1 });
    sdk.publish.mockResolvedValueOnce({ seq: 7, duplicate: true });
    await expect(port.publish("S.state", new Uint8Array([1]), { expectLastSequence: 6, msgID: "m", timeoutMs: 750 }))
      .resolves.toEqual({ seq: 7, duplicate: true });
    expect(sdk.publish).toHaveBeenLastCalledWith("S.state", new Uint8Array([1]), { msgID: "m", expect: { lastSequence: 6 }, timeout: 750 });
    sdk.publish.mockRejectedValueOnce(Object.assign(new Error("timeout"), { name: "TimeoutError" }));
    await expect(port.publish("S.state", new Uint8Array([1]), { expectLastSequence: 6, msgID: "m", timeoutMs: 1_000 }))
      .rejects.toMatchObject({ transient: true });
    sdk.publish.mockRejectedValueOnce(Object.assign(new Error("denied"), { name: "PermissionViolationError" }));
    const permanent = await port.publish("S.state", new Uint8Array([1]), { expectLastSequence: 6, msgID: "m", timeoutMs: 1_000 }).catch(error => error);
    expect(permanent).toBeInstanceOf(Error);
    expect((permanent as { transient?: unknown }).transient).toBeUndefined();
    sdk.publish.mockRejectedValueOnce(new JetStreamApiError({ code: 400, err_code: 10071, description: "wrong last sequence: 7" }));
    await expect(port.publish("S.state", new Uint8Array([1]), { expectLastSequence: 6, msgID: "m", timeoutMs: 1_000 }))
      .rejects.toBeInstanceOf(NatsWrongLastSequenceError);
  });
});
