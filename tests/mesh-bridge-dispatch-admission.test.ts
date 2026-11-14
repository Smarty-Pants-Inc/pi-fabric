// KEEP: #16378 trust boundary/data safety. Real stdio RPC client; virtual time tests
// the unchanged deadlines. These are not native SQLite COMMIT or full CLI proofs.
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_PROTOCOL_VERSION, BridgeDispatchDeferredError, BridgeMirrorDeferredError,
  DEFAULT_CALL_TIMEOUT_MS, RemoteBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";

interface Request { id: number; op: string; args?: unknown }
const remotes: RemoteBridgeSide[] = [];
afterEach(() => { for (const remote of remotes.splice(0)) remote.close(); vi.useRealTimers(); });
const sample = { hosts: [], participants: [] };
const event: BridgePublish = { topic: "fleet.work.test", kind: "probe", from: { id: "session:sender-00000000", name: "sender", kind: "main" },
  to: "session:target-00000000", text: "private-body-never-in-errors", data: { bridge: { from: "hub", id: "event-1" } } };
const capabilities = { version: BRIDGE_PROTOCOL_VERSION, changes: true, tail: true, publishBatch: true, publishReceipts: true,
  independentRpc: true, responsivePing: true, busyTail: true, partialPresence: true };
const fixture = async (hello: unknown = capabilities, negotiate = true) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setImmediate", "clearImmediate"] });
  const input = new PassThrough(), output = new PassThrough();
  const requests: Request[] = [];
  const reply = (request: Request, result?: unknown, error?: { error: string; code?: string }) =>
    input.write(`${JSON.stringify({ id: request.id, ok: !error, ...(error ?? { result }) })}\n`);
  output.on("data", chunk => {
    const request = JSON.parse(String(chunk)) as Request;
    requests.push(request);
    if (request.op === "hello") reply(request, hello);
  });
  const remote = new RemoteBridgeSide(input, output);
  remotes.push(remote);
  if (negotiate) await remote.hello();
  return { remote, requests, reply, input, output };
};
const unsent = async (work: Promise<unknown>, reason: "mirror-pending" | "traffic-in-flight") => {
  const error = await work.catch(error => error);
  expect(error).toBeInstanceOf(BridgeDispatchDeferredError);
  expect(error).not.toBeInstanceOf(BridgeMirrorDeferredError);
  expect(error).toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false, reason });
  expect((error as BridgeDispatchDeferredError).message).not.toMatch(/exceeds|not allowed|no bridge stamp|no recipient|no sender|Invalid mesh topic|private-body/i);
  return error;
};

describe("reciprocal nonqueueing bridge dispatch admission", () => {
  it("mirror-first refuses every protected RPC before ID/timer/write, including cached-tail -> head", async () => {
    const f = await fixture();
    const mirror = f.remote.mirror(sample);
    expect(f.requests.at(-1)?.op).toBe("mirror"); // no added await on a negotiated idle path
    const slot = f.requests.at(-1)!;
    const timers = vi.getTimerCount();
    const calls = [() => f.remote.publish(event), () => f.remote.publishWithReceipt!(event),
      () => f.remote.publishBatch!([{ event }]), () => f.remote.publishBatchWithReceipt!([{ event }]),
      () => f.remote.presence(), () => f.remote.latestSequence(), () => f.remote.latestCursor(),
      () => f.remote.read(0), () => f.remote.bridgedIds(0), () => f.remote.hello(), () => f.remote.watchdogHead(5_000)];
    for (const call of calls) await unsent(call(), "mirror-pending");
    expect(f.requests.at(-1)).toBe(slot);
    expect(vi.getTimerCount()).toBe(timers);
    const ping = f.remote.watchdogPing!(5_000);
    f.reply(f.requests.at(-1)!, { busy: true });
    await expect(ping).resolves.toEqual({ busy: true });
    const tail = f.remote.tail(7, 91);
    f.reply(f.requests.at(-1)!, { events: [{ ...event, sequence: 8 }], through: 8, offset: 122 });
    await expect(tail).resolves.toMatchObject({ through: 8 });
    await unsent(f.remote.latestSequence(), "mirror-pending"); // cached control page is not a worker head exemption
    f.reply(slot);
    await mirror;
    const publish = f.remote.publish(event);
    expect(f.requests.at(-1)).toMatchObject({ id: slot.id + 3, op: "publish" }); // only ping/tail used IDs
    f.reply(f.requests.at(-1)!, { sequence: 1 });
    await expect(publish).resolves.toEqual({ sequence: 1 });
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["publish", "publishWithReceipt", "publishBatch", "presence", "latestSequence", "read", "bridgedIds"] as const)(
    "%s-first synchronously excludes mirror, releases on ordinary error, and permits fresh mirror", async method => {
      const f = await fixture();
      const work = method === "publishBatch" ? f.remote.publishBatch!([{ event }])
        : method === "publish" || method === "publishWithReceipt" ? f.remote[method]!(event)
        : method === "read" || method === "bridgedIds" ? f.remote[method](0) : f.remote[method]();
      const request = f.requests.at(-1)!;
      const observed = work.catch(error => error);
      await unsent(f.remote.mirror(sample), "traffic-in-flight");
      expect(f.requests.at(-1)).toBe(request);
      f.reply(request, undefined, { error: "normal store error" });
      expect(await observed).toMatchObject({ message: "Bridge agent: normal store error" });
      const mirror = f.remote.mirror(sample);
      expect(f.requests.at(-1)).toMatchObject({ id: request.id + 1, op: "mirror" });
      f.reply(f.requests.at(-1)!);
      await mirror;
      expect(vi.getTimerCount()).toBe(0);
    });

  it.each([1, 2])("composite holds ONE reservation across ACK/receipt gap and %s-item prefix", async count => {
    const f = await fixture();
    const composite = f.remote.publishBatchWithReceipt!([{ event }, { event: { ...event, data: { bridge: { from: "hub", id: "event-2" } } } }]);
    const publish = f.requests.at(-1)!;
    expect(publish.op).toBe("publishBatch");
    f.reply(publish, Array.from({ length: count }, (_, index) => ({ sequence: index + 1 })));
    // This continuation races BEFORE the composite has issued its receipt lookup.
    await unsent(f.remote.mirror(sample), "traffic-in-flight");
    const receipt = f.requests.at(-1)!;
    expect(receipt).toMatchObject({ op: "publishReceiptTimes", args: Array.from({ length: count }, (_, index) => index + 1) });
    await unsent(f.remote.mirror(sample), "traffic-in-flight");
    f.reply(receipt, Array.from({ length: count }, (_, index) => ({ sequence: index + 1, createdAt: 10 })));
    await expect(composite).resolves.toHaveLength(count);
    const mirror = f.remote.mirror(sample); f.reply(f.requests.at(-1)!); await mirror;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not relabel a sent composite's receipt error as unsent; close releases all owned reservations", async () => {
    const f = await fixture();
    const work = f.remote.publishBatchWithReceipt!([{ event }]).catch(error => error);
    f.reply(f.requests.at(-1)!, [{ sequence: 1 }]);
    await Promise.resolve();
    f.reply(f.requests.at(-1)!, undefined, { code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", error: "Bridge dispatch deferred: mirror pending" });
    const error = await work;
    expect(error).not.toBeInstanceOf(BridgeDispatchDeferredError);
    expect(error).not.toHaveProperty("sent");
    const publish = f.remote.publish(event).catch(error => error);
    f.remote.close();
    await publish;
    expect(vi.getTimerCount()).toBe(0);
    await expect(f.remote.mirror(sample)).rejects.toMatchObject({ message: "Bridge transport closed" });
  });

  it.each([{}, { independentRpc: true }, { responsivePing: false, busyTail: false },
    { responsivePing: "true", busyTail: 1 }])("grants no control exemptions for absent/false/malformed capabilities %j", async extras => {
    const f = await fixture({ version: 1, tail: true, ...extras });
    const mirror = f.remote.mirror(sample);
    expect(f.remote.watchdogPing).toBeUndefined();
    await unsent(f.remote.tail(7, 91), "mirror-pending");
    await unsent(f.remote.watchdogHead(5_000), "mirror-pending");
    expect(f.requests.map(request => request.op)).toEqual(["hello", "mirror"]);
    f.reply(f.requests.at(-1)!); await mirror;
  });

  it("permits negotiated legacy-v1 controls without independentRpc, but keeps protected calls unsent", async () => {
    const f = await fixture({ version: 1, tail: true, responsivePing: true, busyTail: true });
    expect(f.remote.protocolVersion).toBe(1);
    expect(f.requests[0]).toMatchObject({ op: "hello", args: { version: BRIDGE_PROTOCOL_VERSION, busyTail: true } });

    const mirror = f.remote.mirror(sample);
    const mirrorSlot = f.requests.at(-1)!;
    expect(mirrorSlot.op).toBe("mirror");
    const ping = f.remote.watchdogPing!(5_000);
    const pingRequest = f.requests.at(-1)!;
    expect(pingRequest.op).toBe("ping");
    const tail = f.remote.tail(7, 91);
    const tailRequest = f.requests.at(-1)!;
    expect(tailRequest).toMatchObject({ op: "tail", args: { after: 7, offset: 91 } });

    f.reply(pingRequest, { busy: true });
    f.reply(tailRequest, { events: [{ ...event, sequence: 8 }], through: 8, offset: 122 });
    await expect(ping).resolves.toEqual({ busy: true });
    await expect(tail).resolves.toMatchObject({ through: 8, offset: 122 });

    const requestsBeforeProtected = f.requests.map(request => request.op);
    const timersBeforeProtected = vi.getTimerCount();
    const protectedCalls = [() => f.remote.publish(event), () => f.remote.presence(), () => f.remote.latestSequence(),
      () => f.remote.latestCursor(), () => f.remote.read(0), () => f.remote.bridgedIds(0), () => f.remote.watchdogHead(5_000)];
    for (const call of protectedCalls) await unsent(call(), "mirror-pending");
    expect(f.requests.map(request => request.op)).toEqual(requestsBeforeProtected);
    expect(vi.getTimerCount()).toBe(timersBeforeProtected);

    f.reply(mirrorSlot);
    await mirror;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a failed hello never grants advertised responsive controls", async () => {
    const f = await fixture({ ...capabilities, changes: false }, false);
    await expect(f.remote.hello()).rejects.toThrow("requires changes:true");
    expect(f.remote.protocolVersion).toBeUndefined();
    expect(f.remote.watchdogPing).toBeUndefined();
    // A second attempted bootstrap also fails; there is never a sent mirror or control capability.
    await expect(f.remote.mirror(sample)).rejects.toThrow("requires changes:true");
    expect(f.requests.map(request => request.op)).toEqual(["hello", "hello"]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("revokes previous control exemptions on a received but invalid hello renegotiation", async () => {
    const hello = { ...capabilities };
    const f = await fixture(hello);
    hello.changes = false;
    await expect(f.remote.hello()).rejects.toThrow("requires changes:true");
    expect(f.remote.watchdogPing).toBeUndefined();
    const mirror = f.remote.mirror(sample);
    await unsent(f.remote.tail(7, 91), "mirror-pending");
    f.reply(f.requests.at(-1)!); await mirror;
  });

  it("closes/releases on synchronous transport-write failure without declaring an UNSENT replayable message", async () => {
    const f = await fixture();
    const failure = new Error("synchronous write failed");
    vi.spyOn(f.output, "write").mockImplementationOnce(() => { throw failure; });
    await expect(f.remote.publish(event)).rejects.toBe(failure);
    await expect(f.remote.closed).resolves.toBe(failure);
    expect(failure).not.toHaveProperty("sent");
    expect(f.requests.filter(request => request.op === "publish")).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("keeps a nonfatal protected HEAD reservation after its caller deadline until the actual late reply", async () => {
    const f = await fixture();
    const head = f.remote.watchdogHead(1_000).catch(error => error);
    const request = f.requests.at(-1)!;
    await vi.advanceTimersByTimeAsync(1_001); // include the established I/O/setImmediate grace turn
    expect(await head).toMatchObject({ message: "Bridge agent did not answer latestSequence within 1000 ms" });
    await unsent(f.remote.mirror(sample), "traffic-in-flight");
    expect(f.requests.at(-1)).toBe(request);
    f.reply(request, 10);
    const mirror = f.remote.mirror(sample); f.reply(f.requests.at(-1)!); await mirror;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a mirror becoming degraded DURING the short wait cannot grant relay admission on its late reply", async () => {
    const f = await fixture();
    const mirror = f.remote.mirror(sample).catch(error => error), request = f.requests.at(-1)!;
    await vi.advanceTimersByTimeAsync(29_900);
    const waiting = f.remote.waitForPendingMirror({ signal: new AbortController().signal, remaining: () => 4_000 });
    await vi.advanceTimersByTimeAsync(100);
    expect(await mirror).toMatchObject({ timedOut: true });
    f.reply(request);
    await expect(waiting).resolves.toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("never follows even an immediately completed REPLACEMENT mirror when observing the original slot", async () => {
    const f = await fixture();
    const original = f.remote.mirror(sample), request = f.requests.at(-1)!;
    const waiting = f.remote.waitForPendingMirror({ signal: new AbortController().signal, remaining: () => 4_000 });
    f.reply(request);
    const replacement = f.remote.mirror(sample);
    f.reply(f.requests.at(-1)!); // even if BOTH replies arrive before any promise continuation
    await expect(waiting).resolves.toBe(false);
    await original; await replacement;
    expect(f.requests.filter(request => request.op === "publish")).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retains the EXACT late mirror slot after nonfatal 30000 ms and releases only on its ACK", async () => {
    const f = await fixture();
    expect(DEFAULT_CALL_TIMEOUT_MS).toBe(30_000);
    const mirror = f.remote.mirror(sample).catch(error => error);
    const slot = f.requests.at(-1)!;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(await mirror).toMatchObject({ timedOut: true });
    await unsent(f.remote.publish(event), "mirror-pending");
    f.reply({ id: slot.id + 900, op: "mirror" }); // unrelated/forged response cannot release
    await unsent(f.remote.publish(event), "mirror-pending");
    expect(f.requests.filter(request => request.op === "publish")).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1_000);
    f.reply(slot);
    const publish = f.remote.publish(event); f.reply(f.requests.at(-1)!, { sequence: 1 });
    await publish;
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["FABRIC_BRIDGE_STALE_INCARNATION", "permanent-peer-error"])("late mirror %s still closes, never turns into unsent authority", async code => {
    const f = await fixture();
    const mirror = f.remote.mirror(sample).catch(error => error);
    const slot = f.requests.at(-1)!;
    await vi.advanceTimersByTimeAsync(30_000); await mirror;
    f.reply(slot, undefined, { code, error: "permanent refused mirror" });
    const closed = await f.remote.closed;
    await expect(f.remote.publish(event)).rejects.toBe(closed);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("an independently SENT publication keeps its fatal 30000 ms deadline and is never replayed", async () => {
    const f = await fixture();
    const publish = f.remote.publish(event).catch(error => error);
    await vi.advanceTimersByTimeAsync(30_000);
    const closed = await f.remote.closed;
    expect(closed.message).toBe("Bridge agent did not answer publish within 30000 ms");
    expect(await publish).toBe(closed);
    expect(closed).not.toHaveProperty("sent");
    expect(f.requests.filter(request => request.op === "publish")).toHaveLength(1);
    await expect(f.remote.publish(event)).rejects.toBe(closed);
    expect(vi.getTimerCount()).toBe(0);
  });
});
