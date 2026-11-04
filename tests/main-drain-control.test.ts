import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { sameActorSpec } from "../src/main-drain/actor-restore.js";
import { MainDrainControl } from "../src/main-drain/control.js";
import { MainDrainServer } from "../src/main-drain/server.js";
import { clearBinding } from "../src/main-drain/binding.js";
import { reopenMainInbox } from "../src/main-drain/gate.js";
import { releaseNativeInput } from "../src/main-drain/native-port.js";
import { main as runDrain } from "../src/seat-drain/seam.js";
import type { MainDrainBindingRequest } from "../src/main-drain/binding.js";
import { callMainDrain, mainDrainSocket } from "../src/main-drain/client.js";
import { mainInboxDrainGate } from "../src/main-drain/gate.js";
import { createSeat, ROOT, SID, type Seat } from "./seat-drain-fixture.js";

// D3 seat drain (smarty-dev#15310) M10: the live Main's owner-only control op. Mutating ops need a run binding
// whose caller holds that run's exclusive seat lock; a same-UID process without it, a replay, another run or a
// pod binding on a host Main are refused before anything closes.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
const actorManagers: ActorManager[] = [], agentManagers: AgentManager[] = [];
afterEach(async () => {
  for (const actors of actorManagers.splice(0)) await actors.close();
  for (const agents of agentManagers.splice(0)) await agents.close();
  for (const seat of seats.splice(0)) await seat.close();
});
const hex = (char: string): string => char.repeat(32);
const bindingFor = (seat: Seat, overrides: Partial<MainDrainBindingRequest> = {}): MainDrainBindingRequest => ({
  runDir: seat.runDir, run: seat.run, runSha256: seat.runSha256, phase: "drain", operationId: seat.operationId, generation: hex("a"),
  lockFd: seat.lockFd(), podUid: null, ...overrides,
});
const socketOf = (seat: Seat) => mainDrainSocket(seat.meshRoot, SID, process.getuid!());

const realActors = (seat: Seat): ActorManager => {
  const identity = { id: ROOT, name: "Main", kind: "main" as const, sessionId: SID };
  const agents = new AgentManager(seat.base, { ...DEFAULT_FABRIC_CONFIG.agents, model: "test/project", thinking: "low" },
    { runRoot: path.join(seat.base, "runs") });
  agentManagers.push(agents);
  const mesh = new MeshStore(seat.meshRoot, 256 * 1024, 100);
  const actors = new ActorManager(SID, identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    actorRoot: path.join(seat.meshRoot, "actors", SID), persistent: true, rootId: ROOT, claimResidency: "session", reapDeadSessionPresence: false,
  });
  actorManagers.push(actors);
  seat.host.actors = () => actors;
  return actors;
};
const supervisor = async (actors: ActorManager) => {
  const actor = await actors.create({ name: "supervisor", instructions: "Supervise this Main without losing its configuration.",
    responseMode: "directive", events: ["agent_settled"], delivery: "steer", triggerTurn: true, residency: "session", tools: ["read"],
    nice: 3, model: "test/project", thinking: "low" });
  await actors.setModel(actor.id, "test/live-override", "session", undefined, "Saved live override #18491");
  await actors.setThinking(actor.id, "high", "session");
  return actor.id;
};
const seam = async (seat: Seat, phase: string, inputs: Record<string, unknown> = {}) => {
  let stdout = "", stderr = "";
  const code = await runDrain(["--run-dir", seat.runDir, "--phase", phase], {
    stdin: () => Buffer.from(seat.request(phase, inputs)), stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(seat.lockFd()) },
  }, { deadlineMs: 12_000, undoBudgetMs: 3_000, sleep: async () => {} });
  expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
  return { stdout, receipt: JSON.parse(stdout) };
};

// Keep: data safety and recorded cutover incidents #18489/#18490/#18491. Real manager, registry and owner socket.
describe.skipIf(!linux)("reversible resident supervisor drain", () => {
  it("drains an idle resident supervisor, then undo recreates the identical spec and live binding", async () => {
    const seat = await createSeat(); seats.push(seat);
    const actors = realActors(seat), id = await supervisor(actors), saved = actors.drainSpec(id);
    const drained = await seam(seat, "drain");
    expect(drained.receipt).toMatchObject({ result: "pass", observed: { childrenTerminal: true, inputsFenced: true } });
    expect(actors.status(id).status).toBe("stopped");
    const undo = await seam(seat, "drain-undo", { attempt: 1, drainReceiptSha256: null });
    expect(undo.receipt).toMatchObject({ result: "pass", observed: { inputsFenced: false, admissionRestored: true } });
    const restored = actors.listOwned().find(row => row.name === "supervisor")!;
    expect(restored).toMatchObject({ status: "idle", supervisorFor: ROOT, model: "test/live-override", thinking: "high",
      modelReason: "Saved live override #18491" });
    expect(sameActorSpec(saved, actors.drainSpec(restored.id))).toBe(true);
    const evidence = JSON.parse(fs.readFileSync(path.join(seat.runDir, "fabric-v2-drain-undo-1-outcome.json"), "utf8"));
    expect(evidence.actorsRestored).toBe(true);
    // A second control undo must find the exact replacement, not create another supervisor.
    await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { generation: JSON.parse(
      fs.readFileSync(path.join(seat.runDir, "fabric-v2-drain-intent.json"), "utf8")).generation, phase: "drain-undo" }));
    expect(actors.listOwned().filter(row => row.name === "supervisor")).toHaveLength(1);
  });

  it("a forced post-stop refusal restores the supervisor inside undo and its stored receipt validates", async () => {
    const seat = await createSeat(); seats.push(seat);
    const actors = realActors(seat), id = await supervisor(actors), saved = actors.drainSpec(id);
    const stop = actors.stop.bind(actors);
    actors.stop = async (...args) => {
      const result = await stop(...args);
      seat.fabric.held = 1; seat.fabric.halted = true;
      return result;
    };
    const refused = await seam(seat, "drain");
    expect(refused.receipt).toMatchObject({ result: "refused", observed: { inputsFenced: false } });
    expect(Object.keys(refused.receipt).sort()).toEqual(["observed", "owner", "phase", "result", "run", "schema"]);
    const written = JSON.parse(fs.readFileSync(path.join(seat.runDir, "fabric-v2-drain-outcome.json"), "utf8"));
    expect(written).toMatchObject({ actorsRestored: true, undo: { admissionRestored: true } });
    const restored = actors.listOwned().find(row => row.name === "supervisor")!;
    expect(restored.status).toBe("idle");
    expect(sameActorSpec(saved, actors.drainSpec(restored.id))).toBe(true);
    fs.mkdirSync(path.join(seat.runDir, "receipts"), { mode: 0o700 });
    fs.writeFileSync(path.join(seat.runDir, "receipts", "drain.json"), refused.stdout);
    const digest = createHash("sha256").update(refused.stdout).digest("hex");
    expect((await seam(seat, "drain-undo", { attempt: 1, drainReceiptSha256: digest })).receipt)
      .toMatchObject({ result: "pass", observed: { admissionRestored: true } });
  });

  it("undo with no saved actor effects does not require an actor manager", async () => {
    const seat = await createSeat(); seats.push(seat);
    seat.host.actors = () => undefined;
    await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat));
    const undo = await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo" }));
    expect(undo.undo?.actorsRestored).toBe(true);
    expect(undo.binding).toBeNull();
  });

  it("never stops a supervisor when the runtime has no ordinary recreation path", async () => {
    const seat = await createSeat(); seats.push(seat);
    const actors = realActors(seat), id = await supervisor(actors), saved = actors.drainSpec(id);
    seat.host.actors = () => ({ listOwned: () => actors.listOwned(), drainQuiescence: key => actors.drainQuiescence(key),
      drainWorker: key => actors.drainWorker(key), drainSpec: key => actors.drainSpec(key), stop: (...args) => actors.stop(...args) });
    await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat));
    expect((await callMainDrain(socketOf(seat), SID, "stop-actors", bindingFor(seat))).stopRefused).toBe("ACTORS_UNRESTORABLE");
    expect((await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo" }))).undo?.actorsRestored).toBe(true);
    expect(actors.status(id).status).toBe("idle");
    expect(sameActorSpec(saved, actors.drainSpec(id))).toBe(true);
  });

  it("does not stop when a reason-only live binding override cannot be replayed", async () => {
    const seat = await createSeat(); seats.push(seat);
    const id = "5".repeat(32);
    seat.actors.listOwned = () => [{ id, name: "supervisor", rootId: ROOT, status: "idle" }];
    seat.actors.drainQuiescence = () => [];
    seat.actors.drainSpec = () => ({ id, name: "supervisor", rootId: ROOT, instructions: "Supervise.", residency: "session",
      model: "test/project", modelReason: "Project reason", resolvedBinding: { model: "test/project", modelReason: "Live reason" } });
    seat.actors.stop = async () => { throw new Error("must not stop"); };
    await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat));
    const stop = await callMainDrain(socketOf(seat), SID, "stop-actors", bindingFor(seat));
    expect(stop.stopRefused).toBe("ACTORS_UNRESTORABLE");
    expect(stop.stopAttempted).toEqual([]);
    expect((await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo" }))).undo?.actorsRestored).toBe(true);
  });

  it("a timed-out stop cannot race undo's restoration readback", async () => {
    const seat = await createSeat(); seats.push(seat);
    const id = "4".repeat(32), state = { status: "idle" };
    seat.writeActors([{ id, name: "supervisor", rootId: ROOT, status: "idle" }]);
    seat.actors.listOwned = () => [{ id, name: "supervisor", rootId: ROOT, status: state.status }];
    seat.actors.drainQuiescence = () => [];
    seat.actors.drainWorker = () => null;
    let release!: () => void;
    const done = new Promise<void>(resolve => { release = resolve; });
    let stopped!: Promise<unknown>;
    seat.actors.stop = () => (stopped = done.then(() => {
      state.status = "stopped";
      seat.writeActors([{ id, name: "supervisor", rootId: ROOT, status: "stopped" }]);
    }));
    seat.host.stopWaitMs = 10;
    await callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat));
    expect((await callMainDrain(socketOf(seat), SID, "stop-actors", bindingFor(seat))).stopRefused).toBe("ACTOR_STOP_TIMEOUT");
    try {
      expect((await callMainDrain(socketOf(seat), SID, "stop-actors", bindingFor(seat))).stopRefused).toBe("ACTOR_STOP_PENDING");
      const undo = await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo" }));
      expect(undo.undo?.actorsRestored).toBe(false);
      expect(undo.restored).toEqual([]);
    } finally { release(); await stopped; }
    expect((await callMainDrain(socketOf(seat), SID, "undo", bindingFor(seat, { phase: "drain-undo" }))).undo?.actorsRestored).toBe(true);
  });

  it("after cutover, the new Main restores from durable saved rows as one repeatable owned step", async () => {
    const seat = await createSeat(); seats.push(seat);
    const actors = realActors(seat), id = await supervisor(actors), saved = actors.drainSpec(id);
    const drained = await seam(seat, "drain");
    expect(drained.receipt.result).toBe("pass");
    fs.mkdirSync(path.join(seat.runDir, "receipts"), { mode: 0o700 });
    fs.writeFileSync(path.join(seat.runDir, "receipts", "drain.json"), drained.stdout);
    fs.writeFileSync(path.join(seat.runDir, "receipts", "fence.json"), JSON.stringify({ schema: "d3-seam-receipt-v1", run: seat.run,
      phase: "fence", owner: "synthetic-fence-owner", result: "pass", observed: {} }));
    // Restoration cannot run on the still-bound v1 Main.
    expect((await seam(seat, "drain-restore")).receipt.result).toBe("refused");
    await seat.server.close(); await actors.close();
    releaseNativeInput(ROOT); reopenMainInbox(ROOT); clearBinding(ROOT);
    ((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.main.drain-slot.v1")] as Map<string, unknown>).delete(ROOT);
    const next = realActors(seat);
    seat.server = new MainDrainServer(new MainDrainControl(seat.host));
    expect(await seat.server.start()).toBe("listening");
    expect((await seam(seat, "drain-restore")).receipt).toMatchObject({ result: "pass", observed: { actorsRestored: true } });
    const restored = next.listOwned().find(row => row.name === "supervisor")!;
    expect(restored.status).toBe("idle");
    expect(sameActorSpec(saved, next.drainSpec(restored.id))).toBe(true);
    expect((await seam(seat, "drain-restore")).receipt.result).toBe("pass");
    expect(next.listOwned().filter(row => row.name === "supervisor")).toHaveLength(1);
  });
});

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

  it("the Main's SO_PEERCRED helper is the fixed /usr/bin/python3: a python3 planted first in its PATH cannot answer", async () => {
    const seat = await createSeat(); seats.push(seat);
    const planted = path.join(seat.base, "planted-bin");
    fs.mkdirSync(planted, { mode: 0o700 });
    // A lying helper: it would name pid 1 as root, which the lock proof refuses.
    fs.writeFileSync(path.join(planted, "python3"), "#!/bin/sh\necho 1 0 0\n", { mode: 0o700 });
    const binding = bindingFor(seat); // takes the synthetic lock first, with the real python3
    const saved = process.env.PATH;
    process.env.PATH = `${planted}:${saved ?? ""}`;
    try {
      const reply = await callMainDrain(socketOf(seat), SID, "drain", binding);
      expect(reply.binding).toMatchObject({ kind: "host", run: seat.run, operationId: seat.operationId });
    } finally { process.env.PATH = saved; }
  });

  it("refuses a request bound to another run's bytes, another session, or a pod on a host Main", async () => {
    const seat = await createSeat(); seats.push(seat);
    await expect(callMainDrain(socketOf(seat), SID, "drain", bindingFor(seat, { runSha256: "f".repeat(64) })))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_RUN_SHA256_MISMATCH" });
    await expect(callMainDrain(socketOf(seat), SID, "drain", { ...bindingFor(seat), runDir: null, lockFd: null, podUid: "00000000-0000-4000-8000-0000000000ee" }))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_POD_BINDING_OUTSIDE_POD" });
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

  it("the channel is a 0700 seat-owned SIBLING of the mesh root, reachable through its held descriptor whatever the path length", async () => {
    const seat = await createSeat(); seats.push(seat);
    const root = fs.realpathSync(seat.meshRoot);
    const directory = path.join(path.dirname(root), `.${path.basename(root)}.main-drain`);
    expect(fs.lstatSync(directory).mode & 0o7777).toBe(0o700);
    expect(path.dirname(seat.server.socketPath!)).toBe(directory);
    expect(fs.lstatSync(seat.server.socketPath!).mode & 0o777).toBe(0o600);
    expect(Buffer.byteLength(seat.server.socketPath!)).toBeGreaterThan(0);
    await seat.server.close();
    expect(fs.existsSync(seat.server.socketPath!)).toBe(false);
  });

  it("#1607 CI: the mesh root holds no socket while the channel listens (a tool that reads every mesh file never meets one)", async () => {
    const seat = await createSeat(); seats.push(seat);
    expect(await callMainDrain(socketOf(seat), SID, "status", null)).toMatchObject({ ok: true });
    const walk = (dir: string): string[] => fs.readdirSync(dir, { withFileTypes: true })
      .flatMap(entry => entry.isDirectory() ? walk(path.join(dir, entry.name)) : [path.join(dir, entry.name)]);
    const files = walk(seat.meshRoot);
    expect(files.filter(file => !fs.lstatSync(file).isFile())).toEqual([]);
    // Exactly what the provider-dead leak scan does: read every entry under the mesh root.
    expect(() => files.forEach(file => fs.readFileSync(file))).not.toThrow();
  });

  it("refuses a sibling directory that is not private (another mode), and never contacts a socket there", async () => {
    const seat = await createSeat(); seats.push(seat);
    const root = fs.realpathSync(seat.meshRoot);
    const directory = path.join(path.dirname(root), `.${path.basename(root)}.main-drain`);
    fs.chmodSync(directory, 0o750);
    try { expect(() => socketOf(seat)).toThrow(expect.objectContaining({ code: "MAIN_DRAIN_DIRECTORY_UNSAFE" })); }
    finally { fs.chmodSync(directory, 0o700); }
  });
});
