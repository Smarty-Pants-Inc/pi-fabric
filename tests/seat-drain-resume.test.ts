import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runSessionDrainCli } from "../src/cli/session-drain.js";
import { mainInboxDrainGate } from "../src/main-drain/gate.js";
import { createSeat, ROOT, SID, type Seat } from "./seat-drain-fixture.js";

// The host CLI implementation remains supported. The raw Pod no longer calls it:
// CLI-free orchestration and its original real-image regression live in seat-drain-resume-cli-free.test.ts.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
afterEach(async () => { for (const seat of seats.splice(0)) await seat.close(); });
const POD = { name: "org-d3synth-raw", uid: "00000000-0000-4000-8000-0000000000aa" };

describe.skipIf(!linux)("the in-pod CLI half: pi-fabric session drain --exit against a pod-bound Main", () => {
  const exitArgs = (seat: Seat, extra: string[] = []) => [SID, "--mesh", seat.meshRoot, "--exit", "--pod-uid", POD.uid, "--run", seat.run,
    "--run-sha256", seat.runSha256, "--operation-id", seat.operationId, "--generation", "a".repeat(32), "--deadline-s", "5", ...extra];
  const cli = async (seat: Seat, args: string[]) => {
    let stdout = ""; let stderr = "";
    const code = await runSessionDrainCli(args, { stdout: { write: (text: string) => { stdout += text; return true; } },
      stderr: { write: (text: string) => { stderr += text; return true; } }, sleep: async () => {} });
    return { code, stderr, line: stdout ? JSON.parse(stdout) as Record<string, any> : undefined };
  };

  // The raw-Pod caller proof (review finding 1, ruling 10-25): only a process the runtime injected (kubectl exec)
  // reads PPid 0 inside the Pod's PID namespace. The test stands in a fake /proc and a fixed SO_PEERCRED peer.
  const EXEC_PID = 4242;
  const podChannel = async (seat: Seat, ppid: string, peer: { pid: number; uid: number } | null = { pid: EXEC_PID, uid: process.getuid!() }) => {
    const proc = path.join(seat.base, "proc", String(EXEC_PID));
    fs.mkdirSync(proc, { recursive: true });
    const fields = ["S", ppid, ...Array.from({ length: 17 }, () => "1"), "12345", "0"];
    fs.writeFileSync(path.join(proc, "stat"), `${EXEC_PID} (node index.js) ${fields.join(" ")}\n`);
    seat.host.environment = { KUBERNETES_SERVICE_HOST: "10.0.0.1" };
    seat.host.procRoot = path.join(seat.base, "proc");
    await seat.server.close();
    const { MainDrainServer } = await import("../src/main-drain/server.js");
    seat.server = new MainDrainServer(seat.control, { peer: async () => peer ?? undefined });
    expect(await seat.server.start()).toBe("listening");
  };

  it("refuses a pod binding from a process the Main forked (PPid not 0), from the Main itself, or without peer creds", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "77");
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_REFUSED_POD_CALLER_NOT_EXEC" } });
    await podChannel(seat, "0", { pid: process.pid, uid: process.getuid!() });
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_REFUSED_POD_CALLER_IS_MAIN" } });
    await podChannel(seat, "0", null);
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_REFUSED_PEER_UNVERIFIED" } });
    await podChannel(seat, "0", { pid: EXEC_PID, uid: process.getuid!() + 1 });
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_REFUSED_PEER_UNVERIFIED" } });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });

  it("RFL P2: the in-Pod CLI reports a stop refusal in its result line, and undoes", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "0");
    const id = "4".repeat(32);
    seat.actors.listOwned = () => [{ id, name: "stuck", status: "idle", rootId: ROOT }];
    seat.actors.drainQuiescence = () => [];
    seat.actors.drainWorker = () => null;
    seat.actors.stop = async () => { throw new Error("synthetic stop failure"); };
    fs.mkdirSync(path.join(seat.meshRoot, "actors", SID), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(seat.meshRoot, "actors", SID, "actors.json"), JSON.stringify({ format: 1,
      actors: [{ id, name: "stuck", instructions: "Exercise the ordinary stop refusal.", rootId: ROOT, status: "idle" }] }), { mode: 0o600 });
    const result = await cli(seat, exitArgs(seat));
    expect(result.line).toMatchObject({ op: "exit", drained: false, exiting: false, stopRefused: "ACTOR_STOP_FAILED" });
    expect(result.code).toBe(1);
    expect(seat.native.state.shutdown).not.toHaveBeenCalled();
  });

  it("drains, then the Main accepts a clean exit and shuts down after its reply", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "0");
    const result = await cli(seat, exitArgs(seat));
    expect(result.line).toMatchObject({ format: 1, sessionId: SID, op: "exit", drained: true, exiting: true, inputsFenced: true });
    expect(result.code).toBe(0);
    expect(seat.native.state.shutdown).not.toHaveBeenCalled();
    // The accepted exit is final: the same operation cannot undo inside the shutdown grace, so the fence stays held.
    const { callMainDrain, mainDrainSocket } = await import("../src/main-drain/client.js");
    const binding = { runDir: null, run: seat.run, runSha256: seat.runSha256, phase: "resume-drain" as const, operationId: seat.operationId,
      generation: "a".repeat(32), lockFd: null, podUid: POD.uid };
    await expect(callMainDrain(mainDrainSocket(seat.meshRoot, SID, process.getuid!()), SID, "undo", binding))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_EXITING" });
    expect(seat.native.state.fenced).toBe(true);
    await new Promise(resolve => setTimeout(resolve, 1_300));
    expect(seat.native.state.shutdown).toHaveBeenCalledTimes(1);
  });

  it("the in-Pod CLI refuses a server that is not this session's inbox owner, before writing any request", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "0");
    const owner = path.join(seat.meshRoot, "main-followups", `${SID}.owner.json`);
    const record = JSON.parse(fs.readFileSync(owner, "utf8"));
    fs.writeFileSync(owner, JSON.stringify({ ...record, pid: process.pid + 100_000 }));
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_SERVER_NOT_OWNER" } });
    fs.writeFileSync(owner, JSON.stringify({ ...record, processStartedAt: "1" }));
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_SERVER_NOT_OWNER" } });
    fs.rmSync(owner);
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_OWNER_UNREADABLE" } });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
    expect(seat.native.state.fenced).toBe(false);
  });

  it("a host Main (no Kubernetes env) refuses a pod binding, and a later op must carry the SAME pod UID", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "0");
    seat.host.environment = {};
    expect(await cli(seat, exitArgs(seat))).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_REFUSED_POD_BINDING_OUTSIDE_POD" } });
    seat.host.environment = { KUBERNETES_SERVICE_HOST: "10.0.0.1" };
    const { callMainDrain, mainDrainSocket } = await import("../src/main-drain/client.js");
    const socket = mainDrainSocket(seat.meshRoot, SID, process.getuid!());
    const binding = { runDir: null, run: seat.run, runSha256: seat.runSha256, phase: "resume-drain" as const, operationId: seat.operationId,
      generation: "a".repeat(32), lockFd: null, podUid: POD.uid };
    await callMainDrain(socket, SID, "drain", binding);
    await expect(callMainDrain(socket, SID, "undo", { ...binding, podUid: "00000000-0000-4000-8000-0000000000ac" }))
      .rejects.toMatchObject({ code: "MAIN_DRAIN_REFUSED_DRAIN_BOUND_ELSEWHERE" });
    await callMainDrain(socket, SID, "undo", binding);
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });

  it("a Pod Main that does not drain undoes itself and reports inputsFenced false; it never exits", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "0");
    seat.writeJournal([{ id: "replayed-1" }]);
    const result = await cli(seat, exitArgs(seat));
    expect(result.line).toMatchObject({ op: "exit", drained: false, exiting: false, inputsFenced: false });
    expect(result.code).toBe(1);
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
    expect(seat.native.state.shutdown).not.toHaveBeenCalled();
  });
});
