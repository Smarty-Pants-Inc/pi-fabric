import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { MainDrainBindingRequest } from "../src/main-drain/binding.js";
import { MainDrainControl } from "../src/main-drain/control.js";
import { callMainDrain, mainDrainSocket } from "../src/main-drain/client.js";
import { actorRestoreRequest } from "../src/main-drain/actor-restore.js";
import { MainDrainServer } from "../src/main-drain/server.js";
import { peekDrainSlot } from "../src/main-drain/slot.js";
import { createSeat, SID, type Seat } from "./seat-drain-fixture.js";

const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
afterEach(async () => {
  for (const seat of seats.splice(0)) await seat.close();
});

const bindingFor = (seat: Seat, phase: MainDrainBindingRequest["phase"] = "drain"): MainDrainBindingRequest => ({
  runDir: seat.runDir, run: seat.run, runSha256: seat.runSha256, phase, operationId: seat.operationId,
  generation: "a".repeat(32), lockFd: seat.lockFd(), podUid: null,
});
const socketOf = (seat: Seat) => mainDrainSocket(seat.meshRoot, SID, process.getuid!());
const observe = <T>(promise: Promise<T>) => promise.then(
  reply => ({ reply }),
  error => ({ error }),
);

class SixSecondStopControl extends MainDrainControl {
  readonly handleOrder: Array<Parameters<MainDrainControl["handle"]>[0]> = [];
  readonly stopEntered: Promise<void>;
  readonly settled: Promise<void>;
  hasEnteredStop = false;
  #markStopEntered!: () => void;
  #markSettled!: () => void;

  constructor(host: ConstructorParameters<typeof MainDrainControl>[0]) {
    super(host);
    this.stopEntered = new Promise(resolve => { this.#markStopEntered = resolve; });
    this.settled = new Promise(resolve => { this.#markSettled = resolve; });
  }

  override async handle(...args: Parameters<MainDrainControl["handle"]>): Promise<Awaited<ReturnType<MainDrainControl["handle"]>>> {
    const [op] = args;
    this.handleOrder.push(op);
    if (op !== "stop-actors") return super.handle(...args);
    this.hasEnteredStop = true;
    this.#markStopEntered();
    try {
      await new Promise<void>(resolve => setTimeout(resolve, 6_000));
      return await super.handle(...args);
    } finally {
      this.#markSettled();
    }
  }
}

class HeldStopControl extends MainDrainControl {
  readonly handleOrder: Array<Parameters<MainDrainControl["handle"]>[0]> = [];
  readonly stopEntered: Promise<void>;
  readonly settled: Promise<void>;
  hasEnteredStop = false;
  lateCompletionCount = 0;
  #releasePromise!: Promise<void>;
  #release!: () => void;
  #markStopEntered!: () => void;
  #markSettled!: () => void;

  constructor(host: ConstructorParameters<typeof MainDrainControl>[0]) {
    super(host);
    this.#releasePromise = new Promise(resolve => { this.#release = resolve; });
    this.stopEntered = new Promise(resolve => { this.#markStopEntered = resolve; });
    this.settled = new Promise(resolve => { this.#markSettled = resolve; });
  }

  releaseStop(): void { this.#release(); }

  override async handle(...args: Parameters<MainDrainControl["handle"]>): Promise<Awaited<ReturnType<MainDrainControl["handle"]>>> {
    const [op] = args;
    this.handleOrder.push(op);
    if (op !== "stop-actors") return super.handle(...args);

    this.hasEnteredStop = true;
    this.#markStopEntered();
    await this.#releasePromise;
    try {
      return await super.handle(...args);
    } finally {
      this.lateCompletionCount++;
      this.#markSettled();
    }
  }
}

describe.skipIf(!linux)("Main drain server operation lifetime", () => {
  it("keeps a real stop-actors reply alive through six seconds and serves queued status afterward", async () => {
    const seat = await createSeat({ start: false });
    seats.push(seat);
    const control = new SixSecondStopControl(seat.host);
    const server = new MainDrainServer(control);
    try {
      expect(await server.start()).toBe("listening");
      const socket = socketOf(seat);
      const binding = bindingFor(seat);
      expect(await callMainDrain(socket, SID, "drain", binding, 30_000)).toMatchObject({ ok: true, op: "drain" });

      const completionOrder: string[] = [];
      const stop = observe(callMainDrain(socket, SID, "stop-actors", binding, 30_000)).then(outcome => {
        completionOrder.push("stop");
        return outcome;
      });
      await control.stopEntered;
      const status = observe(callMainDrain(socket, SID, "status", null, 30_000)).then(outcome => {
        completionOrder.push("status");
        return outcome;
      });
      const [stopOutcome, statusOutcome] = await Promise.all([stop, status]);

      expect(stopOutcome).toMatchObject({ reply: { ok: true, op: "stop-actors" } });
      expect(statusOutcome).toMatchObject({ reply: { ok: true, op: "status", sessionId: SID } });
      expect(control.handleOrder).toEqual(["drain", "stop-actors", "status"]);
      expect(completionOrder).toEqual(["stop", "status"]);
    } finally {
      if (control.hasEnteredStop) await control.settled;
      await server.close();
    }
  }, 20_000);

  it("times out a non-settling operation, serves status, blocks mutation until late completion, then permits undo", async () => {
    const seat = await createSeat({ start: false });
    seats.push(seat);
    const control = new HeldStopControl(seat.host);
    const options = { operationTimeoutMs: 500 };
    const server = new MainDrainServer(control, options);
    let replacement: MainDrainServer | undefined;
    try {
      expect(await server.start()).toBe("listening");
      const socket = socketOf(seat);
      const binding = bindingFor(seat);
      expect(await callMainDrain(socket, SID, "drain", binding, 30_000)).toMatchObject({ ok: true, op: "drain" });

      const stop = observe(callMainDrain(socket, SID, "stop-actors", binding, 30_000));
      await control.stopEntered;
      const status = observe(callMainDrain(socket, SID, "status", null, 30_000));
      const [stopOutcome, statusOutcome] = await Promise.all([stop, status]);

      expect(stopOutcome).toMatchObject({ error: { code: "MAIN_DRAIN_REFUSED_OPERATION_TIMEOUT" } });
      expect(statusOutcome).toMatchObject({ reply: { ok: true, op: "status", sessionId: SID } });

      const undoBinding = bindingFor(seat, "drain-undo");
      const pendingMutation = observe(callMainDrain(socket, SID, "undo", undoBinding, 30_000));
      expect(await pendingMutation).toMatchObject({ error: { code: "MAIN_DRAIN_REFUSED_OPERATION_PENDING" } });
      expect(control.handleOrder).toEqual(["drain", "stop-actors", "status"]);
      expect(control.lateCompletionCount).toBe(0);
      const pendingOperation = peekDrainSlot(seat.host.rootId)?.pendingOperation;
      expect(pendingOperation).toBeDefined();

      await server.close();
      replacement = new MainDrainServer(control, options);
      expect(await replacement.start()).toBe("listening");
      expect(peekDrainSlot(seat.host.rootId)?.pendingOperation).toBe(pendingOperation);
      const replacementSocket = socketOf(seat);
      const stillPending = observe(callMainDrain(replacementSocket, SID, "undo", undoBinding, 30_000));
      expect(await stillPending).toMatchObject({ error: { code: "MAIN_DRAIN_REFUSED_OPERATION_PENDING" } });
      expect(await callMainDrain(replacementSocket, SID, "status", null, 30_000)).toMatchObject({ ok: true, op: "status", sessionId: SID });
      expect(control.handleOrder).toEqual(["drain", "stop-actors", "status", "status"]);

      control.releaseStop();
      await control.settled;
      expect(control.lateCompletionCount).toBe(1);
      const undo = await callMainDrain(replacementSocket, SID, "undo", undoBinding, 30_000);
      expect(undo).toMatchObject({ ok: true, op: "undo", undo: { fenceReleased: true, inboxReopened: true, actorsRestored: true } });
      expect(control.handleOrder).toEqual(["drain", "stop-actors", "status", "status", "undo"]);
    } finally {
      control.releaseStop();
      if (control.hasEnteredStop) await control.settled;
      await replacement?.close();
      await server.close();
    }
  }, 40_000);

  it("retains the idle-before-newline request timeout", async () => {
    const seat = await createSeat({ start: false });
    seats.push(seat);
    const server = new MainDrainServer(seat.control);
    let client: net.Socket | undefined;
    let directoryFd: number | undefined;
    let safetyTimer: ReturnType<typeof setTimeout> | undefined;
    try {
      expect(await server.start()).toBe("listening");
      directoryFd = fs.openSync(path.dirname(server.socketPath!), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
      client = net.connect(`/proc/self/fd/${directoryFd}/${path.basename(server.socketPath!)}`);
      client.on("error", () => {});
      await new Promise<void>((resolve, reject) => {
        client!.once("connect", resolve);
        client!.once("error", reject);
      });
      const closed = new Promise<boolean>(resolve => {
        client!.once("close", () => resolve(true));
        safetyTimer = setTimeout(() => resolve(false), 8_000);
      });
      client.write(JSON.stringify({ v: 1, op: "status", sessionId: SID, binding: null }));
      expect(await closed).toBe(true);
    } finally {
      if (safetyTimer) clearTimeout(safetyTimer);
      client?.destroy();
      if (directoryFd !== undefined) fs.closeSync(directoryFd);
      await server.close();
    }
  }, 12_000);

  it("times out while peer credentials are pending and discards the late mutation", async () => {
    const seat = await createSeat({ start: false });
    seats.push(seat);
    let releasePeer!: (peer: { pid: number; uid: number } | undefined) => void;
    const peer = new Promise<{ pid: number; uid: number } | undefined>(resolve => { releasePeer = resolve; });
    const options = { operationTimeoutMs: 500, peer: () => peer };
    const server = new MainDrainServer(seat.control, options);
    const handleOrder: Array<Parameters<MainDrainControl["handle"]>[0]> = [];
    const handle = seat.control.handle.bind(seat.control);
    seat.control.handle = async (...args) => {
      handleOrder.push(args[0]);
      return handle(...args);
    };
    try {
      expect(await server.start()).toBe("listening");
      const socket = socketOf(seat);
      const timedMutation = observe(callMainDrain(socket, SID, "drain", bindingFor(seat), 10_000));
      expect(await timedMutation).toMatchObject({ error: { code: "MAIN_DRAIN_REFUSED_OPERATION_TIMEOUT" } });
      const pendingOperation = peekDrainSlot(seat.host.rootId)?.pendingOperation;
      expect(pendingOperation).toBeDefined();
      expect(await callMainDrain(socket, SID, "status", null, 2_000)).toMatchObject({ ok: true, op: "status", sessionId: SID });

      releasePeer({ pid: process.pid, uid: process.getuid!() });
      expect(await callMainDrain(socket, SID, "status", null, 3_000)).toMatchObject({ ok: true, op: "status", sessionId: SID });
      expect(peekDrainSlot(seat.host.rootId)?.pendingOperation).toBeUndefined();
      expect(handleOrder).toEqual(["status", "status"]);
    } finally {
      releasePeer({ pid: process.pid, uid: process.getuid!() });
      await server.close();
    }
  }, 15_000);

  it("times out in real actor restoration without losing the late create effect", async () => {
    const seat = await createSeat({ start: false });
    seats.push(seat);
    const actorId = "d".repeat(32);
    const actorSpec = { id: actorId, name: "supervisor", rootId: seat.host.rootId, instructions: "Supervise.", residency: "session",
      events: [], topics: [], status: "idle" };
    let actorStatus = "idle";
    seat.writeActors([actorSpec]);
    seat.actors.listOwned = () => [{ id: actorId, name: actorSpec.name, rootId: seat.host.rootId, status: actorStatus, residency: "session" }];
    seat.actors.drainQuiescence = () => [];
    seat.actors.drainWorker = () => null;
    seat.actors.stop = async () => {
      actorStatus = "stopped";
      seat.writeActors([{ ...actorSpec, status: "stopped" }]);
      return {};
    };
    const create = seat.actors.create!;
    let releaseCreate!: () => void;
    let markCreateEntered!: () => void;
    let markCreateSettled!: () => void;
    const createGate = new Promise<void>(resolve => { releaseCreate = resolve; });
    const createEntered = new Promise<void>(resolve => { markCreateEntered = resolve; });
    const createSettled = new Promise<void>(resolve => { markCreateSettled = resolve; });
    let createWasEntered = false;
    seat.actors.create = async request => {
      createWasEntered = true;
      markCreateEntered();
      await createGate;
      try { return await create(request); }
      finally { markCreateSettled(); }
    };

    const options = { operationTimeoutMs: 500 };
    const server = new MainDrainServer(seat.control, options);
    const handleOrder: Array<Parameters<MainDrainControl["handle"]>[0]> = [];
    let firstUndoSettled!: () => void;
    const undoSettled = new Promise<void>(resolve => { firstUndoSettled = resolve; });
    let undoCount = 0;
    const handle = seat.control.handle.bind(seat.control);
    seat.control.handle = async (...args) => {
      handleOrder.push(args[0]);
      try { return await handle(...args); }
      finally { if (args[0] === "undo" && ++undoCount === 1) firstUndoSettled(); }
    };
    try {
      expect(await server.start()).toBe("listening");
      const socket = socketOf(seat);
      const binding = bindingFor(seat);
      const undoBinding = bindingFor(seat, "drain-undo");
      expect(await callMainDrain(socket, SID, "drain", binding, 10_000)).toMatchObject({ ok: true, op: "drain" });
      expect(await callMainDrain(socket, SID, "stop-actors", binding, 10_000)).toMatchObject({ ok: true, op: "stop-actors", stopRefused: null });

      const timedUndo = observe(callMainDrain(socket, SID, "undo", undoBinding, 10_000));
      await createEntered;
      const [undoOutcome, status] = await Promise.all([
        timedUndo,
        callMainDrain(socket, SID, "status", null, 3_000),
      ]);
      expect(undoOutcome).toMatchObject({ error: { code: "MAIN_DRAIN_REFUSED_OPERATION_TIMEOUT" } });
      expect(status).toMatchObject({ ok: true, op: "status", sessionId: SID });
      expect(handleOrder).toEqual(["drain", "stop-actors", "undo", "status"]);

      const repeatedUndo = observe(callMainDrain(socket, SID, "undo", undoBinding, 3_000));
      expect(await repeatedUndo).toMatchObject({ error: { code: "MAIN_DRAIN_REFUSED_OPERATION_PENDING" } });
      expect(handleOrder).toEqual(["drain", "stop-actors", "undo", "status"]);

      releaseCreate();
      await createSettled;
      await undoSettled;
      const undo = await callMainDrain(socket, SID, "undo", undoBinding, 10_000);
      expect(undo).toMatchObject({ ok: true, op: "undo", undo: { actorsRestored: true } });
      const restoredId = undo.restored?.find(row => row.id === actorId)?.restoredId;
      expect(restoredId).toBeTruthy();
      expect(restoredId).not.toBe(actorId);
      expect(actorRestoreRequest(seat.actors.drainSpec!(restoredId!))).toEqual(actorRestoreRequest(actorSpec));
      expect(handleOrder).toEqual(["drain", "stop-actors", "undo", "status", "undo"]);
    } finally {
      releaseCreate();
      if (createWasEntered) {
        await createSettled;
        await undoSettled;
      }
      await server.close();
    }
  }, 40_000);
});
