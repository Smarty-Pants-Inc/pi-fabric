import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { meshDeferredSummary } from "../src/core/atomic-write.js";
import {
  BridgeMirrorDeferredError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide,
} from "../src/mesh/bridge.js";
import { isMeshRetryableBusy, isMeshStateWalCap, MESH_STATE_WAL_CAP_CODE } from "../src/mesh/state-backend.js";
import { MeshStateWalCapError } from "../src/mesh/state-sqlite.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep reason: the untrusted JSON-stream boundary caused smarty-dev#15875's false local WAL page.
const REMOTE_CODE = "FABRIC_REMOTE_MESH_STATE_WAL_CAP";
const READERS = "pid 4242 (pinner --token=READER_SECRET)";
const cleanups: Array<() => unknown> = [];
const roots: string[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-wal-cap-"));
  roots.push(root);
  return root;
};
const capped = (): MeshStateWalCapError => new MeshStateWalCapError(9_000, 8_000, READERS, 2);
const localHost = (): string => /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,252}$/.test(os.hostname()) ? os.hostname() : "unknown";
const noReaderSecrets = (summary: string): void => {
  expect(summary).not.toMatch(/4242|pinner|READER_SECRET|UNTRUSTED|\u001b/);
};

const link = (timeoutMs = 1_000) => {
  const store = new MeshStore(scratch(), 64 * 1024, 100);
  const agent = new StoreBridgeSide(store, "ryzen1");
  const input = new PassThrough(), output = new PassThrough();
  const frames: Record<string, unknown>[] = [];
  output.on("data", chunk => {
    for (const line of String(chunk).trim().split("\n")) frames.push(JSON.parse(line));
  });
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, timeoutMs, "ryzen8");
  cleanups.push(async () => { remote.close(); await serving; output.end(); store.closeState(); });
  return { agent, remote, frames };
};

const refusingPeer = (fields: Record<string, unknown>, peer?: string) => {
  const input = new PassThrough(), output = new PassThrough();
  const remote = new RemoteBridgeSide(input, output, 1_000, peer);
  output.on("data", chunk => {
    const request = JSON.parse(String(chunk));
    // Split frames too: the receiver must cross its actual line-buffer/JSON parser, not a direct error cast.
    const frame = JSON.stringify({ id: request.id, ok: false, code: MESH_STATE_WAL_CAP_CODE,
      error: "UNTRUSTED\u001b[31m --token=READER_SECRET", ...fields }) + "\n";
    input.write(frame.slice(0, 17));
    input.write(frame.slice(17));
  });
  cleanups.push(() => remote.close());
  return remote;
};

describe("log-safe WAL summaries", () => {
  it.each([false, true])("never turns a mirror deferral into a WAL alarm (timedOut=%s)", timedOut => {
    const error = new BridgeMirrorDeferredError(timedOut, 30_000);
    expect(meshDeferredSummary(error)).toBe(error.message);
    expect(meshDeferredSummary(error)).not.toContain("WAL_CAP");
  });

  it("keeps a real local WAL refusal locally alarmable without exposing its readers", () => {
    const error = Object.assign(capped(), { origin: "remote", peer: "UNTRUSTED" });
    expect(isMeshRetryableBusy(error)).toBe(true);
    const summary = meshDeferredSummary(error);
    expect(summary).toBe(`LOCAL ${MESH_STATE_WAL_CAP_CODE} (host ${localHost()}): `
      + "the SQLite WAL is 9000 bytes, above the 8000-byte cap; 2 WAL reader(s)");
    noReaderSecrets(summary);
  });

  it.each([undefined, null, "9000", -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, { bytes: 9000 }])(
    "renders absent/invalid legacy numeric metadata as unknown (%s)", value => {
      const error = Object.assign(new Error("READER_SECRET"), { code: MESH_STATE_WAL_CAP_CODE,
        walBytes: value, capBytes: value, readerCount: value });
      const summary = meshDeferredSummary(error);
      expect(summary).toContain("the SQLite WAL is unknown bytes, above the unknown-byte cap; unknown WAL reader(s)");
      expect(summary).not.toMatch(/undefined|NaN|Infinity|\[object Object\]/);
      noReaderSecrets(summary);
    },
  );

  it("does not classify other errors or non-errors as WAL caps", () => {
    expect(meshDeferredSummary(new Error("mirror pending"))).toBe("mirror pending");
    for (const error of [undefined, null, "pending", {}]) expect(meshDeferredSummary(error)).not.toContain("WAL_CAP");
  });
});

describe("WAL refusals over the JSON-stream boundary", () => {
  it("preserves real sizes and bounded reader diagnostics, but reconstructs remote origin and a safe message", async () => {
    const f = link();
    f.agent.latestSequence = async () => { throw capped(); };
    const error = await f.remote.latestSequence().catch((value: unknown) => value);
    expect(f.frames.at(-1)).toMatchObject({ ok: false, code: MESH_STATE_WAL_CAP_CODE,
      walBytes: 9_000, capBytes: 8_000, readers: READERS, readerCount: 2 });
    noReaderSecrets(String(f.frames.at(-1)!.error));
    expect(error).toBeInstanceOf(Error);
    expect(error).toMatchObject({ code: REMOTE_CODE, origin: "remote", peer: "ryzen8", retryable: true,
      walBytes: 9_000, capBytes: 8_000, readers: READERS, readerCount: 2 });
    expect(isMeshStateWalCap(error)).toBe(false); // Never a local WAL alarm.
    expect((error as Error).message).toBe(`REMOTE ${REMOTE_CODE} (peer ryzen8): `
      + "the SQLite WAL is 9000 bytes, above the 8000-byte cap; 2 WAL reader(s)");
    expect(meshDeferredSummary(error)).toBe((error as Error).message);
    expect(meshDeferredSummary(error)).not.toContain(MESH_STATE_WAL_CAP_CODE);
    noReaderSecrets(meshDeferredSummary(error));
  });

  it("ignores spoofed local origin, peer, messages and arbitrary properties", async () => {
    const remote = refusingPeer({ origin: "local", peer: "ryzen1\nLOCAL FABRIC_MESH_STATE_WAL_CAP",
      walBytes: 9_000, capBytes: 8_000, readerCount: 2, readers: "reader\u001b\n" + "x".repeat(5_000),
      retryable: false, recoveryArgv: ["UNTRUSTED"], cause: { secret: "READER_SECRET" } }, "ryzen8");
    const error = await remote.latestSequence().catch((value: unknown) => value);
    expect(error).toMatchObject({ code: REMOTE_CODE, origin: "remote", peer: "ryzen8", retryable: true });
    expect(error).not.toHaveProperty("recoveryArgv");
    expect(error).not.toHaveProperty("cause");
    const diagnostics = (error as { readers: string }).readers;
    expect(diagnostics).toHaveLength(4_096);
    expect(diagnostics).not.toMatch(/[\x00-\x1f\x7f-\x9f]/);
    noReaderSecrets(meshDeferredSummary(error));
    expect(meshDeferredSummary(error)).not.toContain(MESH_STATE_WAL_CAP_CODE);
  });

  it.each([
    {},
    { walBytes: "9000", capBytes: -1, readerCount: 1.5, readers: ["UNTRUSTED"] },
    { walBytes: Number.MAX_SAFE_INTEGER + 1, capBytes: null, readerCount: {}, readers: { cmd: "UNTRUSTED" } },
  ])("accepts a legacy refusal without inventing numbers or trusting the claimed peer (%j)", async fields => {
    const remote = refusingPeer({ ...fields, origin: "local", peer: "UNTRUSTED" });
    const error = await remote.latestSequence().catch((value: unknown) => value);
    expect(error).toMatchObject({ code: REMOTE_CODE, origin: "remote", retryable: true });
    expect((error as Error).message).toBe(`REMOTE ${REMOTE_CODE} (peer unknown): `
      + "the SQLite WAL is unknown bytes, above the unknown-byte cap; unknown WAL reader(s)");
    expect((error as { readers?: unknown }).readers).toBeUndefined();
    noReaderSecrets(meshDeferredSummary(error));
  });

  it("validates independent metadata fields instead of discarding valid siblings", async () => {
    const remote = refusingPeer({ walBytes: 0, capBytes: "invalid", readerCount: 0, readers: READERS }, "ryzen8");
    const error = await remote.latestSequence().catch((value: unknown) => value);
    expect(error).toMatchObject({ walBytes: 0, readerCount: 0, readers: READERS });
    expect((error as Error).message).toContain("WAL is 0 bytes, above the unknown-byte cap; 0 WAL reader(s)");
  });

  it("keeps late WAL mirror refusals retryable without a success wake or transport closure", async () => {
    vi.useFakeTimers();
    const f = link(10);
    const changed = vi.fn(), failed = vi.fn();
    await f.remote.subscribeChanges(changed, failed);
    let rejectMirror!: (error: Error) => void;
    f.agent.mirror = () => new Promise<void>((_resolve, reject) => { rejectMirror = reject; });
    cleanups.push(() => rejectMirror?.(capped()));
    const first = f.remote.mirror({ hosts: [], participants: [] }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(11);
    const timeout = await first;
    expect(timeout).toBeInstanceOf(BridgeMirrorDeferredError);
    expect(meshDeferredSummary(timeout)).not.toContain("WAL_CAP");
    const pending = await f.remote.mirror({ hosts: [], participants: [] }).catch((error: unknown) => error);
    expect(pending).toBeInstanceOf(BridgeMirrorDeferredError);
    expect(meshDeferredSummary(pending)).not.toContain("WAL_CAP");
    changed.mockClear();
    rejectMirror(capped());
    await vi.advanceTimersByTimeAsync(0);
    expect(f.frames.at(-1)).toMatchObject({ code: MESH_STATE_WAL_CAP_CODE });
    expect(changed).not.toHaveBeenCalled(); // A refusal must not bypass the existing mirror backoff.
    expect(failed).not.toHaveBeenCalled();
    expect(await f.remote.latestSequence()).toBe(0); // The late response released the gate, not the transport.
    f.agent.mirror = async () => undefined;
    await expect(f.remote.mirror({ hosts: [], participants: [] })).resolves.toBeUndefined();
  });
});

describe("bridge retry/log paths", () => {
  it.each([
    ["local", "latestCursor"], ["remote", "latestCursor"], ["local", "tail"], ["remote", "tail"],
  ] as const)("backs off %s WAL caps on %s without leaking readers or changing their origin", async (where, op) => {
    const f = link();
    await f.remote.hello();
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    const local = new StoreBridgeSide(hub, "ryzen8");
    cleanups.push(() => hub.closeState());
    const target = where === "local" ? local : f.agent;
    const at: number[] = [];
    let recovered!: () => void, failRecovery!: (error: unknown) => void;
    const recovery = new Promise<void>((resolve, reject) => { recovered = resolve; failRecovery = reject; });
    // These are real store-side failures: remote cases must cross serveBridgeAgent and the JSON decoder.
    if (op === "latestCursor") {
      const original = target.latestCursor.bind(target);
      target.latestCursor = async () => {
        at.push(Date.now());
        if (at.length <= 2) throw capped();
        recovered();
        return original();
      };
    } else {
      const original = target.tail.bind(target);
      target.tail = async (...args) => {
        at.push(Date.now());
        if (at.length <= 2) throw capped();
        recovered();
        return original(...args);
      };
    }
    const logs: string[] = [];
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: "ryzen8", local, remote: f.remote,
      cursorPath: path.join(scratch(), "cursor.json"), walCapRetryMinMs: 20, stopMs: 100,
      log: message => logs.push(message) });
    let failure: unknown;
    const running = bridge.run().catch(error => { failure = error; failRecovery(error); });
    cleanups.push(async () => { await bridge.stop(); await running; });
    await recovery;
    expect(at[1]! - at[0]!).toBeGreaterThanOrEqual(15);
    expect(at[2]! - at[1]!).toBeGreaterThanOrEqual(35);
    const refusals = logs.filter(line => line.includes("WAL_CAP"));
    expect(refusals).toHaveLength(1);
    expect(refusals[0]).toContain("retrying in 20 ms");
    expect(refusals[0]).toContain(where === "remote" ? `REMOTE ${REMOTE_CODE} (peer ryzen8)`
      : `LOCAL ${MESH_STATE_WAL_CAP_CODE} (host ${localHost()})`);
    if (where === "remote") expect(refusals[0]).not.toContain(MESH_STATE_WAL_CAP_CODE);
    noReaderSecrets(logs.join("\n"));
    await bridge.stop();
    await running;
    expect(failure).toBeUndefined();
  });

  it("retains backoff after a timed-out mirror's late WAL refusal and retries the next real cap", async () => {
    const f = link(500);
    await f.remote.hello();
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    const local = new StoreBridgeSide(hub, "ryzen8");
    cleanups.push(() => hub.closeState());
    const at: number[] = [];
    const original = f.agent.mirror.bind(f.agent);
    let rejectMirror!: (error: Error) => void;
    let recovered!: () => void, failRecovery!: (error: unknown) => void;
    const recovery = new Promise<void>((resolve, reject) => { recovered = resolve; failRecovery = reject; });
    f.agent.mirror = async (...args) => {
      at.push(Date.now());
      if (at.length === 1) return new Promise<void>((_resolve, reject) => { rejectMirror = reject; });
      if (at.length === 2) throw capped();
      recovered();
      return original(...args);
    };
    let timeoutAt = 0, sawTimeout!: () => void;
    const timeout = new Promise<void>(resolve => { sawTimeout = resolve; });
    const logs: string[] = [];
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: "ryzen8", local, remote: f.remote,
      cursorPath: path.join(scratch(), "cursor.json"), walCapRetryMinMs: 80, stopMs: 100,
      log: message => {
        logs.push(message);
        if (message.includes("bridge mirror degraded")) { timeoutAt = Date.now(); sawTimeout(); }
      } });
    let failure: unknown;
    const running = bridge.run().catch(error => { failure = error; failRecovery(error); });
    cleanups.push(async () => { rejectMirror?.(capped()); await bridge.stop(); await running; });
    await Promise.race([timeout, recovery]);
    rejectMirror(capped());
    await recovery;
    expect(at[1]! - timeoutAt).toBeGreaterThanOrEqual(70);
    expect(at[2]! - at[1]!).toBeGreaterThanOrEqual(150);
    expect(logs.find(line => line.includes("bridge mirror degraded"))).not.toContain("WAL_CAP");
    noReaderSecrets(logs.join("\n"));
    await bridge.stop();
    await running;
    expect(failure).toBeUndefined();
  });
});
