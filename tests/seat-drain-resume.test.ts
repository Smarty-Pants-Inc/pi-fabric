import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runSessionDrainCli } from "../src/cli/session-drain.js";
import { mainInboxDrainGate } from "../src/main-drain/gate.js";
import { main } from "../src/seat-drain/seam.js";
import { createSeat, ROOT, SID, type Seat } from "./seat-drain-fixture.js";

// D3 resume-drain (smarty-dev#15310, DRAIN-DESIGN.r1 §1.13, challenge r1 M9): the raw Pod's Main is drained ONLY
// through kubectl exec of the in-image CLI (a synthetic kubectl here; no cluster), bound to the pod UID, and
// `exited` is the pi container's own exit status. The in-pod CLI half runs against a real pod-bound drain channel.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
afterEach(async () => { for (const seat of seats.splice(0)) await seat.close(); });
const POD = { name: "org-d3synth-raw", uid: "00000000-0000-4000-8000-0000000000aa" };
const CA = Buffer.from("synthetic CA bytes");
const SERVER = "https://synthetic.invalid:6443";

const kubeSeat = async (exec: { line: string; terminate?: boolean }, pod: Record<string, unknown> = {}, options: { cliPresent?: boolean } = {}) => {
  const seat = await createSeat({ kube: { kubeconfig: "/placeholder", server: SERVER, caSha256: createHash("sha256").update(CA).digest("hex") } });
  seats.push(seat);
  const kube = path.join(seat.base, "kube");
  fs.mkdirSync(kube, { mode: 0o700 });
  // The kubeconfig path is sealed in seat-inputs.json; rewrite the packet with the real path (re-pinning its sha).
  const inputsFile = path.join(seat.runDir, "seat-inputs.json");
  const inputs = JSON.parse(fs.readFileSync(inputsFile, "utf8"));
  inputs.kube.kubeconfig = path.join(kube, "config");
  fs.chmodSync(inputsFile, 0o600); fs.writeFileSync(inputsFile, JSON.stringify(inputs)); fs.chmodSync(inputsFile, 0o400);
  const journal = path.join(seat.runDir, "seat-journal.jsonl");
  const records = fs.readFileSync(journal, "utf8").trim().split("\n").map(line => JSON.parse(line));
  records[1].sha256 = createHash("sha256").update(fs.readFileSync(inputsFile)).digest("hex");
  fs.writeFileSync(journal, records.map(record => `${JSON.stringify(record)}\n`).join(""));
  fs.writeFileSync(path.join(kube, "config"), "synthetic", { mode: 0o600 });
  fs.writeFileSync(path.join(kube, "kube-state.json"), JSON.stringify({ server: SERVER, caB64: CA.toString("base64"),
    cliPresent: options.cliPresent ?? true,
    pod: { name: POD.name, uid: POD.uid, meshRoot: "/var/lib/fabric/mesh", running: true, terminated: null, ...pod }, exec }));
  const kubectl = path.join(kube, "kubectl");
  fs.copyFileSync(path.join("tests", "fixtures", "fake-kubectl.py"), kubectl);
  fs.chmodSync(kubectl, 0o700);
  const calls = () => fs.existsSync(path.join(kube, "kube-calls.jsonl"))
    ? fs.readFileSync(path.join(kube, "kube-calls.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line) as { argv: string[]; env: string[] }) : [];
  return { seat, kubectl, calls };
};
const resume = async (seat: Seat, kubectl: string, podCli: string | undefined, deps: Record<string, unknown> = {}) => {
  let stdout = ""; let stderr = "";
  const code = await main(["--run-dir", seat.runDir, "--phase", "resume-drain"], {
    stdin: () => Buffer.from(seat.request("resume-drain", { pod: POD })),
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(seat.lockFd()) },
  }, { kubectl, ...(podCli ? { podCli } : {}), deadlineMs: 20_000, sleep: async () => {},
    onError: error => { if (!(error instanceof Error && error.name === "SeamGap")) throw error; }, ...deps });
  return { code, stdout, stderr, receipt: stdout ? JSON.parse(stdout) as Record<string, any> : undefined };
};
const exiting = JSON.stringify({ format: 1, sessionId: SID, op: "exit", drained: true, exiting: true, inputsFenced: true });

describe.skipIf(!linux)("resume-drain through kubectl exec (M9)", () => {
  it("drains the raw Pod with a fixed argv and the minimal env, and passes only on the pi container's own exit 0", async () => {
    const { seat, kubectl, calls } = await kubeSeat({ line: exiting, terminate: true });
    const result = await resume(seat, kubectl, "/opt/pi-fabric/dist/cli/index.js");
    expect(result).toMatchObject({ code: 0, stderr: "" });
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: seat.run, phase: "resume-drain", owner: "fabric-v2", result: "pass",
      observed: { podUid: POD.uid, inputsFenced: true, exited: true } });
    const exec = calls().find(call => call.argv.includes("drain"))!;
    expect(exec.argv).toEqual(["--kubeconfig", expect.stringMatching(/\/kube\/config$/), "--context", "synthetic", "-n", "org-synthetic", "exec", POD.name,
      "-c", "pi", "--", "/opt/pi-fabric/dist/cli/index.js", "session", "drain", SID, "--mesh", "/var/lib/fabric/mesh", "--exit", "--pod-uid", POD.uid,
      "--run", seat.run, "--run-sha256", seat.runSha256, "--operation-id", seat.operationId, "--generation", expect.stringMatching(/^[0-9a-f]{32}$/),
      "--deadline-s", expect.stringMatching(/^[0-9]+$/)]);
    for (const call of calls()) expect(call.env.filter(name => !name.startsWith("LC_") || name === "LC_ALL")).toEqual(["HOME", "KUBECONFIG", "LANG", "LC_ALL", "PATH"]);
    expect(calls()[0]!.argv.slice(4, 8)).toEqual(["config", "view", "--minify", "--raw"]);
  });

  it("stays refused until the in-image CLI path is confirmed, and never execs without it", async () => {
    const { seat, kubectl, calls } = await kubeSeat({ line: exiting, terminate: true });
    const result = await resume(seat, kubectl, undefined);
    expect(result.receipt).toMatchObject({ result: "refused", observed: { podUid: POD.uid, inputsFenced: false, exited: false } });
    expect(calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it("checks the CLI exists in the Pod first (test -x), else refused without draining", async () => {
    const { seat, kubectl, calls } = await kubeSeat({ line: exiting, terminate: true }, {}, { cliPresent: false });
    const result = await resume(seat, kubectl, "/opt/pi-fabric/dist/cli/index.js");
    expect(result.receipt).toMatchObject({ result: "refused", observed: { inputsFenced: false, exited: false } });
    expect(calls().some(call => call.argv.includes("drain"))).toBe(false);
  });

  it("a Pod Main that is not drained (it undid itself) is refused, never passed", async () => {
    const { seat, kubectl } = await kubeSeat({ line: JSON.stringify({ format: 1, sessionId: SID, op: "exit", drained: false, exiting: false, inputsFenced: false }) });
    const result = await resume(seat, kubectl, "/opt/pi-fabric/dist/cli/index.js");
    expect(result.receipt).toMatchObject({ result: "refused", observed: { podUid: POD.uid, inputsFenced: false, exited: false } });
  });

  it("an exit that never terminates the container is refused with inputsFenced true, exited false", async () => {
    const { seat, kubectl } = await kubeSeat({ line: exiting, terminate: false });
    const result = await resume(seat, kubectl, "/opt/pi-fabric/dist/cli/index.js", { deadlineMs: 3_000 });
    expect(result.receipt).toMatchObject({ result: "refused", observed: { podUid: POD.uid, inputsFenced: true, exited: false } });
  });

  it("a pod whose UID is not the pin, or an unpinned CA, is UNKNOWN: exit 2, no receipt", async () => {
    const replaced = await kubeSeat({ line: exiting, terminate: true }, { uid: "00000000-0000-4000-8000-0000000000ab" });
    expect(await resume(replaced.seat, replaced.kubectl, "/opt/pi-fabric/dist/cli/index.js")).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: pod-uid-mismatch [owner: fabric-v2]\n" });
    const unpinned = await kubeSeat({ line: exiting, terminate: true });
    const state = path.join(path.dirname(unpinned.kubectl), "kube-state.json");
    fs.writeFileSync(state, JSON.stringify({ ...JSON.parse(fs.readFileSync(state, "utf8")), caB64: Buffer.from("other CA").toString("base64") }));
    expect(await resume(unpinned.seat, unpinned.kubectl, "/opt/pi-fabric/dist/cli/index.js")).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: kubeconfig-ca-mismatch [owner: fabric-v2]\n" });
  });
});

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

  it("drains, then the Main accepts a clean exit and shuts down after its reply", async () => {
    const seat = await createSeat(); seats.push(seat);
    await podChannel(seat, "0");
    const result = await cli(seat, exitArgs(seat));
    expect(result.line).toMatchObject({ format: 1, sessionId: SID, op: "exit", drained: true, exiting: true, inputsFenced: true });
    expect(result.code).toBe(0);
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(seat.native.state.shutdown).toHaveBeenCalledTimes(1);
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
