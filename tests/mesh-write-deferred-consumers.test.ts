import path from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { MeshStore, MeshStateEntry, MeshEvent } from "../src/mesh/store.js";
import type { FabricHostLease } from "../src/topology/host-leases.js";
type Batch = Parameters<MeshStore["writeBatch"]>[0];
type Put = Parameters<MeshStore["put"]>[0];
type Publish = Parameters<MeshStore["publish"]>[0];

// Keep reason: #12438/#12036 data safety. Real public consumers, recorded stores,
// file custody and notifications; no native deletion, writes or process launches.
const io = vi.hoisted(() => ({
  native: [] as string[],
  calls: [] as string[],
  stores: new Map<string, { entries: Map<string, MeshStateEntry>; events: MeshEvent[] }>(),
  leases: new Map<string, FabricHostLease>(),
  failure: undefined as { stage: string; error: unknown } | undefined,
  copyError: undefined as Error | undefined,
}));
vi.mock("node:fs", async importOriginal => {
  const native = await importOriginal<typeof import("node:fs")>();
  const guarded = { ...native };
  for (const name of ["mkdirSync", "writeFileSync", "appendFileSync", "renameSync", "rmSync", "unlinkSync",
    "rmdirSync", "copyFileSync", "cpSync", "truncateSync", "chmodSync", "utimesSync", "ftruncateSync"] as const) {
    Object.assign(guarded, { [name]: () => { io.native.push(name); throw new Error(`Unexpected native ${name}`); } });
  }
  guarded.openSync = ((file, flags, mode) => {
    if (typeof flags === "string" ? /[wa+]/.test(flags) : (flags & (native.constants.O_WRONLY | native.constants.O_RDWR | native.constants.O_CREAT)) !== 0) {
      io.native.push("open-write"); throw new Error("Unexpected writable open");
    }
    return native.openSync(file, flags, mode);
  }) as typeof native.openSync;
  return { ...guarded, default: guarded };
});
vi.mock("node:child_process", async importOriginal => {
  const native = await importOriginal<typeof import("node:child_process")>();
  const guarded = { ...native };
  for (const name of ["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync", "fork"] as const) {
    Object.assign(guarded, { [name]: () => { io.native.push(name); throw new Error(`Unexpected native ${name}`); } });
  }
  return { ...guarded, default: guarded };
});
vi.mock("../src/mesh/change-notifier.js", () => ({ watchBridgeStore: () => () => {} }));
vi.mock("../src/topology/host-lease-lock.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/host-lease-lock.js")>(),
  withHostLeaseLock: vi.fn(async (_mesh, _file, operation: () => unknown) => operation()),
}));
vi.mock("../src/topology/host-leases.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/host-leases.js")>(),
  hostLeasePredecessor: (_root: string, id: string) => io.leases.get(id),
  readHostLeaseCurrent: (_root: string, id: string) => io.leases.get(id),
  readHostLeases: () => io.leases,
  renewHostLease: vi.fn(async (_mesh, lease: FabricHostLease) => { io.leases.set(lease.id, lease); }),
  assertHostLeaseOwner: vi.fn(),
  withOwnedHostLease: vi.fn(async (_mesh, _owner, operation: () => unknown) => operation()),
  removeOwnedHostLease: vi.fn(async (_mesh, owner: FabricHostLease) => io.leases.delete(owner.id)),
}));
vi.mock("../src/topology/participant-files.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/participant-files.js")>(),
  readParticipantFiles: () => [],
  participantFilesOnly: () => false,
  prepareParticipantFileLocks: vi.fn(async () => {}),
  removeParticipantFileIf: vi.fn(async () => true),
  writeParticipantFileIf: vi.fn(async () => {
    io.calls.push("copy");
    if (io.copyError) { const error = io.copyError; io.copyError = undefined; throw error; }
    return true;
  }),
}));
vi.mock("../src/mesh/store.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/mesh/store.js")>();
  const attempt = (stage: string) => {
    io.calls.push(stage);
    if (io.failure?.stage === stage) { const error = io.failure.error; io.failure = undefined; throw error; }
  };
  return { ...actual, MeshStore: class {
    maxEventBytes = 65_536; maxReadEvents = 100; lockProtocol = "dual";
    readonly state = { entries: new Map<string, MeshStateEntry>(), events: [] as MeshEvent[] };
    constructor(readonly root: string) { io.stores.set(root, this.state); }
    get = vi.fn((key: string) => this.state.entries.get(key));
    listAll = vi.fn((prefix = "") => [...this.state.entries.values()].filter(entry => entry.key.startsWith(prefix)));
    listAllShared = this.listAll;
    stateToken = vi.fn(() => this.state);
    latestOffset = vi.fn(() => this.state.events.length);
    tail = vi.fn((offset: number) => ({ events: this.state.events.slice(offset), nextOffset: this.state.events.length }));
    subscribeStateChanges = vi.fn(() => () => {});
    closeState = vi.fn();
    confirmWritable = vi.fn(async (acquired?: (at: number) => void) => { acquired?.(Date.now()); });
    put = vi.fn(async (input: Put) => {
      const value = input.value as Record<string, unknown>;
      const local = this.root.includes("control-seen");
      const stage = local ? (value.acceptance ? "outcome" : "local-claim") :
        value.acceptance ? "expired-claim" : "shared-claim";
      attempt(stage);
      const prior = this.state.entries.get(input.key);
      if (input.ifVersion !== undefined && input.ifVersion !== (prior?.version ?? 0)) throw new Error("recorded version conflict");
      const version = (prior?.version ?? 0) + 1;
      this.state.entries.set(input.key, { key: input.key, value: input.value, version, updatedAt: Date.now(), updatedBy: input.identity });
      return { version };
    });
    writeBatch = vi.fn(async (input: Batch) => {
      attempt("batch");
      const view = { get: (key: string) => this.state.entries.get(key),
        listAll: (prefix = "") => this.listAll(prefix), version: (key: string) => this.state.entries.get(key)?.version ?? 0 };
      const ops = [...input.ops, ...(input.prepare?.(view) ?? [])];
      input.beforeCommit?.();
      const results = ops.map(op => {
        const version = (this.state.entries.get(op.key)?.version ?? 0) + 1;
        if (op.kind === "delete") this.state.entries.delete(op.key);
        else this.state.entries.set(op.key, { key: op.key,
          value: typeof op.value === "function" ? (op.value as (at: number) => unknown)(Date.now()) : op.value,
          version, updatedAt: Date.now(), updatedBy: input.identity });
        return { key: op.key, applied: true, version };
      });
      input.commitOutbox?.({ backend: "file", results, changed: results.map(result => result.key), stamp: undefined, view });
      return results;
    });
    publish = vi.fn(async (input: Publish) => {
      attempt(input.topic === "fabric.control.ack" ? "ack" : "publish");
      const event = { id: `recorded-${this.state.events.length}`, sequence: this.state.events.length + 1,
        createdAt: Date.now(), from: input.from, to: input.to, topic: input.topic, kind: input.kind,
        verification: "mesh", data: typeof input.data === "function" ? input.data() : input.data } as MeshEvent;
      this.state.events.push(event); return event;
    });
  } };
});

import { MeshIncarnationPendingError, MeshSnapshotRetriesExhaustedError, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore as RecordedStore } from "../src/mesh/store.js";
import { ParticipantDirectory, confirmWitnessPlatform } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { writeParticipantFileIf } from "../src/topology/participant-files.js";

const root = path.join(process.cwd(), ".local", "deferred-consumers-virtual");
const identity = { id: "recorded-owner", name: "recorded-owner", kind: "main" as const };
const errors = [
  ["pending", () => new MeshIncarnationPendingError()],
  ["snapshot", () => new MeshSnapshotRetriesExhaustedError(3)],
  ["timeout", () => new MeshLockTimeoutError("", 1, 0)],
  ["legacy", () => new Error("Timed out waiting for the Fabric mesh lock (legacy receipt)")],
  ["permanent", () => new Error("permanent storage refusal")],
] as const;
const cleanups: Array<() => Promise<void>> = [];
let witnessConstants: typeof confirmWitnessPlatform.constants;
beforeEach(() => {
  vi.useFakeTimers(); vi.clearAllMocks();
  io.native.length = 0; io.calls.length = 0; io.stores.clear(); io.leases.clear();
  io.failure = undefined; io.copyError = undefined;
  witnessConstants = confirmWitnessPlatform.constants;
  confirmWitnessPlatform.constants = { ...witnessConstants, O_NOFOLLOW: undefined }; // recorded confirmation, never a native witness write
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(Math, "random").mockReturnValue(0);
});
afterEach(async () => {
  io.failure = undefined; io.copyError = undefined;
  for (const close of cleanups.splice(0).reverse()) await close();
  expect(io.native).toEqual([]);
  confirmWitnessPlatform.constants = witnessConstants;
  vi.restoreAllMocks(); vi.useRealTimers();
});
function directory(withActor = false) {
  const mesh = new RecordedStore(root, 65_536, 100);
  const onWithdrawalFailure = vi.fn();
  const value = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id,
    rootId: identity.id, identity, reapDeadHosts: false, onWithdrawalFailure });
  if (withActor) value.registerSource(() => [{ format: 1, id: "actor:recorded", kind: "actor", name: "recorded",
    rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    status: "idle", capabilities: ["followUp"], runner: "pi", transport: "host", controlProtocol: "v1",
    startedAt: Date.now(), updatedAt: Date.now() }]);
  cleanups.push(() => value.close());
  return { mesh, value, onWithdrawalFailure };
}

it.each(errors)("close withdrawal retries %s without losing records; permanent refusal lapses instead", async (name, makeError) => {
  const { mesh, value, onWithdrawalFailure } = directory();
  await value.refresh();
  const state = io.stores.get(root)!;
  const before = [...state.entries.values()];
  expect(before.length).toBeGreaterThan(0);
  vi.mocked(mesh.writeBatch).mockClear();
  io.failure = { stage: "batch", error: makeError() };
  await value.close();
  expect(mesh.writeBatch).toHaveBeenCalledTimes(name === "permanent" ? 1 : 2);
  expect([...state.entries.values()]).toEqual(name === "permanent" ? before : []);
  expect(onWithdrawalFailure).toHaveBeenCalledTimes(name === "permanent" ? 1 : 0);
});

it.each(errors)("file-copy %s unwinds deferred publication, but keeps single-key permanent failures best-effort", async (name, makeError) => {
  const { value } = directory(true);
  const error = makeError(); io.copyError = error;
  const prior = value.confirmedAt();
  if (name === "permanent") {
    await value.refresh(); expect(value.canConsumeMesh()).toBe(true);
  } else {
    await expect(value.refresh()).rejects.toBe(error);
    expect(value.confirmedAt()).toBe(prior);
    expect(value.canConsumeMesh()).toBe(false);
    expect(value.routingUnavailable()).toBe(error.message);
    // Readiness is not a timeout diagnostic (#12438 option A).
    expect(value.writeStalled() !== undefined).toBe(name === "timeout" || name === "legacy");
  }
  await vi.advanceTimersByTimeAsync(1);
  await value.refresh();
  expect(value.canConsumeMesh()).toBe(true);
  expect(value.routingUnavailable()).toBeUndefined();
  expect(writeParticipantFileIf).toHaveBeenCalledTimes(2);
});

const controlErrors = errors.filter(([name]) => name !== "legacy");
const controlCases = ["shared-claim", "local-claim", "outcome", "ack", "expired-claim"] as const;
it.each(controlCases.flatMap(stage => controlErrors.map(([name, makeError]) => ({ stage, name, makeError }))))(
  "control $stage retains $name work and retries without re-executing a completed handler", async ({ stage, name, makeError }) => {
    const mesh = new RecordedStore(root, 65_536, 100);
    const plane = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id,
      pollMs: 20, acknowledgementTimeoutMs: 1_000, ownerIncarnation: "recorded:epoch" });
    cleanups.push(() => plane.close());
    const state = io.stores.get(root)!;
    const now = Date.now();
    state.events.push({ id: "command", sequence: 1, topic: "fabric.control.command", kind: "steer",
      createdAt: now, verification: "mesh", from: { id: "sender", name: "sender", kind: "main" }, to: identity.id,
      data: { version: 1, commandId: "command", targetId: "actor:recorded", operation: stage === "outcome" || stage === "ack" ? "ask" : "steer", replyTo: "sender",
        ownerIncarnation: plane.incarnation, requestedAt: now - (stage === "expired-claim" ? 100 : 0),
        deadlineAt: now + (stage === "expired-claim" ? -1 : 10_000) } } as MeshEvent);
    const handler = vi.fn(() => ({ accepted: true, messageId: "delivered-once" }));
    io.failure = { stage, error: makeError() };
    plane.start(handler); // only recorded store/watch ports, never a process/service start
    await vi.advanceTimersByTimeAsync(0);
    const acks = () => state.events.filter(event => event.topic === "fabric.control.ack");
    if (name !== "permanent") {
      expect(acks()).toEqual([]);
      expect(handler).toHaveBeenCalledTimes(stage === "outcome" || stage === "ack" ? 1 : 0);
      expect(io.calls.filter(call => call === stage)).toHaveLength(1);
      // Detached no-write refusals use bounded generic backoff, not timeout-only accounting.
      await vi.advanceTimersByTimeAsync(100);
      expect(io.calls.filter(call => call === stage)).toHaveLength(2);
      expect(acks()).toHaveLength(1);
      expect(acks()[0]!.data).toMatchObject(stage === "expired-claim"
        ? { accepted: false, notRun: true } : { accepted: true, messageId: "delivered-once" });
      expect(handler).toHaveBeenCalledTimes(stage === "expired-claim" ? 0 : 1);
    } else {
      await vi.advanceTimersByTimeAsync(20);
      expect(io.calls.filter(call => call === stage)).toHaveLength(1);
      expect(handler).toHaveBeenCalledTimes(stage === "outcome" || stage === "ack" ? 1 : 0);
      expect(acks()).toHaveLength(stage === "expired-claim" || stage === "local-claim" ? 1 : 0);
      if (stage === "expired-claim" || stage === "local-claim") expect(acks()[0]!.data).toMatchObject({ accepted: false });
    }
    plane.pause();
  });

function ackPlane() {
  const mesh = new RecordedStore(root, 65_536, 100);
  const plane = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id,
    pollMs: 20, acknowledgementTimeoutMs: 1_000, ownerIncarnation: "recorded:epoch" });
  cleanups.push(() => plane.close());
  const state = io.stores.get(root)!;
  const send = (commandId: string) => state.events.push({ id: commandId, sequence: state.events.length + 1,
    topic: "fabric.control.command", kind: "steer", createdAt: Date.now(), verification: "mesh",
    from: { id: "sender", name: "sender", kind: "main" }, to: identity.id,
    data: { version: 1, commandId, targetId: "actor:recorded", operation: "ask", replyTo: "sender",
      ownerIncarnation: plane.incarnation, requestedAt: Date.now(), deadlineAt: Date.now() + 10_000 } } as MeshEvent);
  const acks = () => state.events.filter(event => event.topic === "fabric.control.ack").map(event => event.data);
  return { mesh, plane, send, acks };
}

// A generic ACK failure is indeterminate: here the recorded event commits and then the call
// throws, so even the readback would pass. Known no-write refusals are recoverable.
// Does not address per-command ACK reconciliation (#12634).
it.each(["indeterminate", "known no-write"] as const)("ack %s failure versus an unrelated later command's confirmed ACK at release", async kind => {
  const { mesh, plane, send, acks } = ackPlane();
  send("first");
  const handler = vi.fn((command: { commandId: string }) => ({ accepted: true, messageId: `done-${command.commandId}` }));
  if (kind === "known no-write") io.failure = { stage: "ack", error: new MeshLockTimeoutError("", 1, 0) };
  else {
    const publish = vi.mocked(mesh.publish).getMockImplementation()!;
    vi.mocked(mesh.publish).mockImplementationOnce(async input => {
      await publish(input); throw new Error("generic ACK failure after commit, outcome unknown");
    });
  }
  plane.start(handler); await vi.advanceTimersByTimeAsync(0);
  expect(acks()).toHaveLength(kind === "indeterminate" ? 1 : 0);
  send("second"); plane.pause(true); plane.resume(); await vi.advanceTimersByTimeAsync(100);
  plane.pause(true);
  expect(handler.mock.calls.map(([command]) => command.commandId)).toEqual(["first", "second"]);
  const seen = [...io.stores.entries()].find(([store]) => store.includes("control-seen"))![1];
  expect([...seen.entries.values()].map(entry => (entry.value as { acceptance?: unknown }).acceptance)).toEqual(
    expect.arrayContaining([{ accepted: true, messageId: "done-first" }, { accepted: true, messageId: "done-second" }]));
  expect(acks()).toEqual([expect.objectContaining({ commandId: "first", accepted: true, messageId: "done-first" }),
    expect.objectContaining({ commandId: "second", accepted: true, messageId: "done-second" })]);
  if (kind === "indeterminate") {
    await expect(plane.checkpointForRelease()).rejects.toThrow(/unsettled publication/);
    expect(() => plane.releaseCheckpointReceipt()).toThrow(/unavailable or unsettled/);
  } else {
    await plane.checkpointForRelease();
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  }
});

it("control preserves the historical plain code-shaped timeout receipt", async () => {
  const mesh = new RecordedStore(root, 65_536, 100);
  const plane = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id,
    pollMs: 20, ownerIncarnation: "recorded:epoch" });
  cleanups.push(() => plane.close());
  const state = io.stores.get(root)!;
  state.events.push({ id: "plain-command", sequence: 1, topic: "fabric.control.command", createdAt: Date.now(),
    verification: "mesh", from: { id: "sender", name: "sender", kind: "main" }, to: identity.id,
    data: { version: 1, commandId: "plain-command", targetId: "actor:recorded", operation: "steer", replyTo: "sender",
      ownerIncarnation: plane.incarnation, requestedAt: Date.now(), deadlineAt: Date.now() + 10_000 } } as MeshEvent);
  const handler = vi.fn(() => ({ accepted: true, messageId: "plain-recovered" }));
  io.failure = { stage: "shared-claim", error: { code: "FABRIC_MESH_LOCK_TIMEOUT" } };
  plane.start(handler); await vi.advanceTimersByTimeAsync(0);
  expect(handler).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(20);
  expect(handler).toHaveBeenCalledOnce();
  expect(state.events.filter(event => event.topic === "fabric.control.ack")[0]?.data).toMatchObject({ messageId: "plain-recovered" });
  plane.pause();
});
