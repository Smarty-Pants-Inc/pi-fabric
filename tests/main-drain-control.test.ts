import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { MainDrainBindingRequest } from "../src/main-drain/binding.js";
import { callMainDrain, mainDrainSocket } from "../src/main-drain/client.js";
import { mainInboxDrainGate } from "../src/main-drain/gate.js";
import { createSeat, ROOT, SID, type Seat } from "./seat-drain-fixture.js";

// D3 seat drain (smarty-dev#15310) M10: the live Main's owner-only control op. Mutating ops need a run binding
// whose caller holds that run's exclusive seat lock; a same-UID process without it, a replay, another run or a
// pod binding on a host Main are refused before anything closes.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
afterEach(async () => { for (const seat of seats.splice(0)) await seat.close(); });
const hex = (char: string): string => char.repeat(32);
const bindingFor = (seat: Seat, overrides: Partial<MainDrainBindingRequest> = {}): MainDrainBindingRequest => ({
  runDir: seat.runDir, run: seat.run, runSha256: seat.runSha256, phase: "drain", operationId: seat.operationId, generation: hex("a"),
  lockFd: seat.lockFd(), podUid: null, ...overrides,
});
const socketOf = (seat: Seat) => mainDrainSocket(seat.meshRoot, SID, process.getuid!());

describe.skipIf(!linux)("owner-only drain control (M10)", () => {
  it("observes without a binding, but never mutates without one", async () => {
    const seat = await createSeat(); seats.push(seat);
    const status = await callMainDrain(socketOf(seat), SID, "status", null);
    expect(status).toMatchObject({ ok: true, sessionId: SID, rootId: ROOT, binding: null, inbox: { closed: false } });
    await expect(callMainDrain(socketOf(seat), SID, "drain", null)).rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_REQUEST_INVALID" });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });

  it("refuses a same-UID caller that does not hold the seat lock (an FD number alone grants nothing)", async () => {
    const seat = await createSeat(); seats.push(seat);
    const unlocked = fs.openSync(path.join(seat.runDir, "seat.lock"), fs.constants.O_RDONLY);
    try {
      await expect(callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat, { lockFd: unlocked })))
        .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_LOCK_NOT_HELD" });
      await expect(callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat, { lockFd: 1 })))
        .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_LOCK_FD_OTHER_FILE" });
    } finally { fs.closeSync(unlocked); }
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });

  it("refuses a request bound to another run's bytes, another session, or a pod on a host Main", async () => {
    const seat = await createSeat(); seats.push(seat);
    await expect(callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat, { runSha256: "f".repeat(64) })))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_RUN_SHA256_MISMATCH" });
    await expect(callMainDrain(socketOf(seat), SID, "drain", { ...bindingFor(seat), runDir: null, lockFd: null, podUid: "00000000-0000-4000-8000-0000000000ee" }))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_POD_BINDING_UNPROVEN" });
    const other = "00000000-0000-4000-8000-0000000000ef";
    await expect(callMainDrain(socketOf(seat), other, "status", null)).rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_SESSION_MISMATCH" });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });

  it("binds one operation: the same one is idempotent, another is refused and a stale undo cannot lift it", async () => {
    const seat = await createSeat(); seats.push(seat);
    const first = await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat));
    expect(first.binding).toMatchObject({ run: seat.run, operationId: seat.operationId, generation: hex("a") });
    expect(mainInboxDrainGate(ROOT)).toBeDefined();
    expect((await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat))).binding?.boundAt).toBe(first.binding?.boundAt);
    await expect(callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat, { operationId: hex("b") })))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_DRAIN_BOUND_ELSEWHERE" });
    await expect(callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo", generation: hex("c") })))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_DRAIN_BOUND_ELSEWHERE" });
    expect(mainInboxDrainGate(ROOT)).toBeDefined();
    const undo = await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo" }));
    expect(undo.undo).toMatchObject({ inboxReopened: true, restartRequired: false });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });

  it("stage 1 review finding 2: a copied RUN with its own lock cannot replay the operation's tuple", async () => {
    const seat = await createSeat(); seats.push(seat);
    await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat));
    const copy = path.join(seat.base, "copied-run");
    fs.mkdirSync(copy, { mode: 0o700 });
    for (const name of ["seat-run.json", "seat.lock"]) fs.copyFileSync(path.join(seat.runDir, name), path.join(copy, name));
    fs.chmodSync(path.join(copy, "seat.lock"), 0o600);
    const lock = fs.openSync(path.join(copy, "seat.lock"), fs.constants.O_RDWR);
    try {
      const taken = (await import("node:child_process")).spawnSync("python3", ["-c", "import fcntl; fcntl.flock(3, fcntl.LOCK_EX | fcntl.LOCK_NB)"],
        { stdio: ["ignore", "ignore", "pipe", lock] });
      expect(taken.status).toBe(0);
      await expect(callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo", runDir: copy, lockFd: lock })))
        .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_DRAIN_BOUND_ELSEWHERE" });
    } finally { fs.closeSync(lock); }
    expect(mainInboxDrainGate(ROOT)).toBeDefined();
  });

  it("the channel is a 0700 seat-owned directory, reachable through its held descriptor whatever the path length", async () => {
    const seat = await createSeat(); seats.push(seat);
    const directory = path.join(fs.realpathSync(seat.meshRoot), "main-drain");
    expect(fs.lstatSync(directory).mode & 0o7777).toBe(0o700);
    expect(fs.lstatSync(seat.server.socketPath!).mode & 0o777).toBe(0o600);
    expect(Buffer.byteLength(seat.server.socketPath!)).toBeGreaterThan(0);
    await seat.server.close();
    expect(fs.existsSync(seat.server.socketPath!)).toBe(false);
  });
});
