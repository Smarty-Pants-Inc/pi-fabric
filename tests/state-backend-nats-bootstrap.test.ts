import { afterEach, describe, expect, it, vi } from "vitest";
import { MAX_MESSAGE_BYTES, MeshStateNatsBusyError, NatsStateBackend, type NatsStreamMessage } from "../src/mesh/state-backend-nats.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";

// Keep: #11950/#13112's resolved-but-silent initial receiver and the data-safety races of concurrent history.
// The normal event source is the original consumer callback; fake time only reaches its original 5s deadline.
const identity = { id: "bootstrap-test", name: "bootstrap-test", kind: "agent" as const };
const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const opened: NatsStateBackend[] = [];
afterEach(async () => {
  for (const backend of opened.splice(0)) backend.close();
  await Promise.resolve();
  vi.useRealTimers();
});
const options = (port: ReturnType<ReturnType<typeof fakeJetStream>["port"]>) => ({
  port, stream: "BOOTSTRAP", subject: "BOOTSTRAP.state", snapshotSubject: "BOOTSTRAP.snapshot",
  maxEventBytes: 64 * 1024, snapshotEvery: 0,
});
const seed = async () => {
  const server = fakeJetStream();
  const backend = await NatsStateBackend.open(options(server.port()));
  for (let index = 0; index < 3; index++) {
    await backend.put({ key: `bootstrap/${index}`, value: { index }, identity });
  }
  const expected = backend.listAll();
  backend.close();
  const port = server.port();
  // A real receiver handle resolves normally. Only its deliveries are held, not consume/get/CREATE.
  port.pause();
  const native = port.consume.bind(port);
  const delivered = deferred<void>();
  const seen: number[] = [];
  let callback!: (message: NatsStreamMessage) => void;
  let consumeCount = 0;
  const stop = vi.fn(async () => undefined);
  const close = vi.spyOn(port, "close");
  port.consume = async (stream, after, onMessage, onError) => {
    consumeCount++;
    callback = onMessage;
    const receiver = await native(stream, after, message => {
      onMessage(message);
      seen.push(message.seq);
      if (message.seq === 3) delivered.resolve();
    }, onError);
    return { stop: async () => { await stop(); await receiver.stop(); } };
  };
  // Reuse the helper's genuine normal-put records, but do not make a historical read wait on setImmediate.
  const history = vi.fn(async (_stream: string, seq: number) => server.messages[seq - 1]);
  port.getMessage = history;
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  const start = () => {
    const t0 = Date.now();
    let settled = false;
    const result = NatsStateBackend.open(options(port)).then(backend => {
      opened.push(backend); settled = true;
      return { backend, error: undefined, elapsedMs: Date.now() - t0 };
    }, error => {
      settled = true;
      return { backend: undefined, error, elapsedMs: Date.now() - t0 };
    });
    return { result, settled: () => settled };
  };
  return { server, port, expected, history, start, stop, close, seen,
    consumeCount: () => consumeCount,
    emit: (seq: number) => callback(server.messages[seq - 1]!),
    resume: async () => { port.resume(); await delivered.promise; },
  };
};
const successful = async (trial: ReturnType<Awaited<ReturnType<typeof seed>>["start"]>) => {
  const result = await trial.result;
  if (result.error) throw result.error;
  return result.backend!;
};

describe("NATS initial known-head bootstrap (#11950/#13112)", () => {
  it("materializes three committed records before the original 5s BUSY, then resumes the SAME receiver and CAS", async () => {
    const f = await seed();
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(5_000);
    // BASE throws its actual MeshStateNatsBusyError here: applied0/known3 at exactly 5000ms, not a fake CREATE error.
    const backend = await successful(trial);
    expect((await trial.result).elapsedMs).toBeLessThan(5_000);
    expect(backend.listAll()).toEqual(f.expected);
    expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 3, knownSeq: 3 });
    expect(f.seen).toEqual([]);
    expect(f.history.mock.calls.map(call => call[1])).toEqual([1, 2, 3]);
    await f.resume();
    expect(f.seen).toEqual([1, 2, 3]);
    expect(f.consumeCount()).toBe(1);
    expect(f.stop).not.toHaveBeenCalled();
    expect(backend.listAll()).toEqual(f.expected);
    expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 3, knownSeq: 3 });
    const entry = f.expected[0]!;
    await backend.put({ key: entry.key, ifVersion: entry.version, value: { resumed: true }, identity });
    expect(backend.get(entry.key)).toMatchObject({ value: { resumed: true }, version: entry.version + 1 });
    expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 4, knownSeq: 4 });
    expect(f.history).toHaveBeenCalledTimes(3); // Ordinary write waiters do not acquire a new bootstrap reader.
  });

  it("adds no GET when healthy consume delivers H before returning its handle", async () => {
    const f = await seed();
    const consume = f.port.consume.bind(f.port);
    f.port.consume = async (...args) => {
      const handle = await consume(...args);
      for (const message of f.server.messages) args[2](message);
      return handle;
    };
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(0);
    const backend = await successful(trial);
    expect(backend.listAll()).toEqual(f.expected);
    expect(f.history).not.toHaveBeenCalled();
  });

  it.each(["resolve", "reject"] as const)("ignores a deferred history %s after live delivery wins, without joining it", async completion => {
    const f = await seed();
    const pending = deferred<NatsStreamMessage | undefined>();
    f.port.getMessage = vi.fn(() => pending.promise);
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    await f.resume();
    const backend = await successful(trial); // Must return while GET is still pending.
    if (completion === "resolve") pending.resolve({ seq: NaN, data: "malformed late envelope" as unknown as Uint8Array });
    else pending.reject(new Error("late denied history"));
    await vi.advanceTimersByTimeAsync(5_001);
    expect(backend.listAll()).toEqual(f.expected);
    expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 3, knownSeq: 3 });
    expect(backend.diagnostics().nats?.failure).toBeUndefined();
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
  });

  it.each(["resolve", "reject"] as const)("discards a redundant requested record's %s and continues from the actual live cursor", async completion => {
    const f = await seed();
    const pending = deferred<NatsStreamMessage | undefined>();
    const get = vi.fn((_stream: string, seq: number) => seq === 1 ? pending.promise : Promise.resolve(f.server.messages[seq - 1]));
    f.port.getMessage = get;
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(get).toHaveBeenCalledTimes(1);
    f.emit(1); // Only requested n became redundant; H has not yet been reached.
    if (completion === "resolve") pending.resolve({ seq: NaN, data: Buffer.from("{") });
    else pending.reject(new Error("redundant permission failure"));
    await vi.advanceTimersByTimeAsync(0);
    const backend = await successful(trial);
    expect(get.mock.calls.map(call => call[1])).toEqual([1, 2, 3]);
    expect(backend.listAll()).toEqual(f.expected);
    expect(backend.diagnostics().nats?.failure).toBeUndefined();
    await f.resume();
    expect(backend.listAll()).toEqual(f.expected);
  });

  it.each(["resolve", "reject"] as const)("fences history %s across a native gap/resync generation change", async completion => {
    const f = await seed();
    const pending = deferred<NatsStreamMessage | undefined>();
    f.port.getMessage = vi.fn(() => pending.promise);
    const consume = f.port.consume.bind(f.port);
    let calls = 0;
    f.port.consume = async (...args) => {
      calls++;
      return consume(...args);
    };
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    f.emit(3); // Native observed gap, not a purge or a historical-reader-triggered resync.
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toBe(2);
    // Resolve a VALID old value (or a permanent old error) before the successor has delivered any data.
    if (completion === "resolve") pending.resolve(f.server.messages[0]);
    else pending.reject(new Error("old generation denied"));
    await vi.advanceTimersByTimeAsync(0);
    expect(trial.settled()).toBe(false);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    f.emit(1); f.emit(2); f.emit(3); // Current generation's native callbacks, not another bootstrap reader.
    const backend = await successful(trial);
    expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 3, knownSeq: 3, resyncs: 1 });
    await vi.advanceTimersByTimeAsync(5_001);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    expect(backend.listAll()).toEqual(f.expected);
    expect(backend.diagnostics().nats?.failure).toBeUndefined();
  });

  it.each(["resolve", "reject"] as const)("retires a deferred history %s at original D and stops only its receiver", async completion => {
    const f = await seed();
    const pending = deferred<NatsStreamMessage | undefined>();
    const get = vi.fn(() => pending.promise);
    f.port.getMessage = get; // Deliberately ignores any supplied budget; backend fencing is independently required.
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(4_999);
    expect(trial.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect((await trial.result).error).toBeInstanceOf(MeshStateNatsBusyError);
    expect((await trial.result).error.message).toMatch(/sequence 3 within 5000 ms \(at 0\)/);
    expect((await trial.result).elapsedMs).toBe(5_000);
    if (completion === "resolve") pending.resolve(f.server.messages[0]);
    else pending.reject(new Error("late permission error"));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(get).toHaveBeenCalledTimes(1);
    expect(f.stop).toHaveBeenCalledTimes(1);
    expect(f.close).not.toHaveBeenCalled(); // Static open does not own an injected port.
    await f.port.close();
  });

  it("leaves a never-answering GET under the original BUSY timer rather than awaiting it", async () => {
    const f = await seed();
    f.port.getMessage = vi.fn(() => new Promise<NatsStreamMessage | undefined>(() => undefined));
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(4_999);
    expect(trial.settled()).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect((await trial.result).error).toBeInstanceOf(MeshStateNatsBusyError);
    expect((await trial.result).elapsedMs).toBe(5_000);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    await f.port.close();
  });

  it("yields undefined history to native delivery without advancing, failing, or polling GET", async () => {
    const f = await seed();
    f.port.getMessage = vi.fn(async () => undefined);
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(trial.settled()).toBe(false);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    await f.resume();
    const backend = await successful(trial);
    expect(backend.listAll()).toEqual(f.expected);
    expect(backend.diagnostics().nats?.failure).toBeUndefined();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
  });

  it.each(["permission", "wrong-sequence", "not-bytes", "oversize", "json", "type", "base"] as const)(
    "fails closed for active historical %s, without launching another GET", async kind => {
      const f = await seed();
      const get = vi.fn(async (): Promise<NatsStreamMessage> => {
        if (kind === "permission") throw Object.assign(new Error("permission denied"), { name: "PermissionViolationError" });
        if (kind === "wrong-sequence") return { ...f.server.messages[0]!, seq: 2 };
        if (kind === "not-bytes") return { seq: 1, data: "bad" as unknown as Uint8Array };
        if (kind === "oversize") return { seq: 1, data: new Uint8Array(MAX_MESSAGE_BYTES + 1) };
        if (kind === "json") return { seq: 1, data: Buffer.from("{") };
        const record = JSON.parse(Buffer.from(f.server.messages[0]!.data).toString("utf8"));
        if (kind === "type") record.type = "foreign";
        else record.base = 1;
        return { seq: 1, data: Buffer.from(JSON.stringify(record)) };
      });
      f.port.getMessage = get;
      const trial = f.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(trial.settled()).toBe(true);
      const { error, backend } = await trial.result;
      expect(backend).toBeUndefined();
      expect(error).toBeInstanceOf(Error);
      expect(error).not.toBeInstanceOf(MeshStateNatsBusyError);
      await vi.advanceTimersByTimeAsync(5_001);
      expect(get).toHaveBeenCalledTimes(1);
      expect(f.close).not.toHaveBeenCalled();
      await f.port.close();
    },
  );

  it.each(["resolve", "reject"] as const)("ignores deferred history %s after public close", async completion => {
    const f = await seed();
    const pending = deferred<NatsStreamMessage | undefined>();
    f.port.getMessage = vi.fn(() => pending.promise);
    const trial = f.start();
    await vi.advanceTimersByTimeAsync(0);
    await f.resume();
    const backend = await successful(trial);
    backend.close();
    const before = backend.diagnostics();
    if (completion === "resolve") pending.resolve({ seq: 999, data: Buffer.from("{") });
    else pending.reject(new Error("late after close"));
    await vi.advanceTimersByTimeAsync(5_001);
    expect(backend.diagnostics()).toEqual(before);
    expect(f.port.getMessage).toHaveBeenCalledTimes(1);
    expect(() => backend.listAll()).toThrow(/closed/);
  });
});
