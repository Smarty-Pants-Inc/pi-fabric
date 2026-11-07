import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { main, RESUME_DRAIN_SCRIPT } from "../src/seat-drain/seam.js";
import { createSeat, SID, type Seat } from "./seat-drain-fixture.js";

// Keep: real image pi-fabric 0.112.0 has no `session` command (P GAP 6), and the
// journal/receipt and Pod/process identity boundaries must fail closed.
const linux = process.platform === "linux" && process.getuid?.() === 1000;
const seats: Seat[] = [];
const children: ChildProcess[] = [];
afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) { const exit = once(child, "exit"); child.kill("SIGKILL"); await exit; }
  }
  for (const seat of seats.splice(0)) await seat.close();
});
const POD = { name: "org-d3synth-raw", uid: "00000000-0000-4000-8000-0000000000aa" };
const CA = Buffer.from("synthetic CA bytes");
const sha = (bytes: string | Buffer): string => createHash("sha256").update(bytes).digest("hex");
const JOURNAL = "/var/lib/smarty/state/pi/sessions/--test--/session.jsonl";

// Models only the API boundary. The CLI path fails EXACTLY like the saved real
// image; the replacement exec returns a configurable measured helper result.
const KUBECTL = `#!/usr/bin/python3
import json, os, sys
from pathlib import Path
root = Path(__file__).parent
state = json.loads((root / 'state.json').read_text())
args = sys.argv[1:]
with (root / 'calls.jsonl').open('a') as out:
    out.write(json.dumps({'argv': args, 'env': sorted(os.environ)}) + '\\n')
if 'config' in args:
    print(json.dumps({'clusters': [{'cluster': {'server': 'https://synthetic.invalid:6443', 'certificate-authority-data': '${CA.toString("base64")}'}}]}))
elif 'get' in args:
    state['gets'] = state.get('gets', 0) + 1
    (root / 'state.json').write_text(json.dumps(state))
    after = state.get('execAttempted', False)
    terminated = after and state.get('terminate', True)
    uid = state.get('afterUid', '${POD.uid}') if after else state.get('uid', '${POD.uid}')
    print(json.dumps({'metadata': {'uid': uid}, 'spec': {'nodeName': state.get('node', 'synthetic-node'), 'restartPolicy': 'Never',
        'containers': [{'name': 'pi', 'env': [{'name': 'PI_FABRIC_MESH_ROOT', 'value': '/var/lib/fabric/mesh'}], 'args': state.get('args', ['--mode', 'rpc', '--session-dir', '/unused', '--session', '${JOURNAL}'])}]},
        'status': {'containerStatuses': [{'name': 'pi', 'containerID': state.get('afterContainer', 'containerd://pi') if after else 'containerd://pi',
        'restartCount': 0, 'state': {'terminated': {'exitCode': state.get('exitCode', 143)}} if terminated else {'running': {'startedAt': '2026-10-31T00:00:00Z'}}}]}}))
elif 'exec' in args:
    if 'test' in args:
        sys.exit(0)
    if 'session' in args:
        print('error: unknown command "session"', file=sys.stderr)
        sys.exit(2)
    state['execAttempted'] = True
    (root / 'state.json').write_text(json.dumps(state))
    print(state.get('line', json.dumps(state['helper'])))
    sys.exit(state.get('execCode', 0))
else:
    sys.exit(2)
`;

const setup = async (options: Record<string, unknown> = {}) => {
  const seat = await createSeat({ kube: { kubeconfig: "/placeholder", server: "https://synthetic.invalid:6443", caSha256: sha(CA) } });
  seats.push(seat);
  const root = path.join(seat.base, "kube");
  fs.mkdirSync(root, { mode: 0o700 });
  const inputFile = path.join(seat.runDir, "seat-inputs.json");
  const input = JSON.parse(fs.readFileSync(inputFile, "utf8"));
  input.kube.kubeconfig = path.join(root, "config");
  fs.chmodSync(inputFile, 0o600); fs.writeFileSync(inputFile, JSON.stringify(input)); fs.chmodSync(inputFile, 0o400);
  const journalFile = path.join(seat.runDir, "seat-journal.jsonl");
  const records = fs.readFileSync(journalFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
  records[1].sha256 = sha(fs.readFileSync(inputFile));
  const observed = { sessionId: SID, journal: JOURNAL, leafId: "12345678", podUid: POD.uid, appendedSinceSeal: [],
    journalSha256: "a".repeat(64), suffixBase64: "" };
  const accepted = { schema: "d3-seam-receipt-v1", run: seat.run, phase: "resume", owner: "fabric-v2", result: "pass", observed };
  fs.mkdirSync(path.join(seat.runDir, "receipts"), { mode: 0o700 });
  const receipt = JSON.stringify(accepted);
  fs.writeFileSync(path.join(seat.runDir, "receipts", "resume.json"), receipt, { mode: 0o600 });
  records.push({ event: "seam", seam: "bridge", phase: "resume", sha256: sha(receipt) }, { event: "done", step: "resume" });
  fs.writeFileSync(journalFile, records.map(record => `${JSON.stringify(record)}\n`).join(""));
  fs.writeFileSync(path.join(seat.runDir, "fabric-v2-bridge-resume-outcome.json"), JSON.stringify({
    schema: "fabric-v2-bridge-resume-outcome-v1", owner: "fabric-v2", run: seat.run, phase: "resume", sessionId: SID,
    result: "pass", observed, measured: { journal: { journalBytes: 123, journalSha256: observed.journalSha256 } },
  }), { mode: 0o600 });
  const helper = { format: 1, op: "resume-drain", inputsFenced: true, signalled: true, journalBytes: 123, journalSha256: "a".repeat(64),
    piPid: 1, piStartTime: "7654321", signalAt: "2026-10-31T19:00:00.000000Z", reason: null, errorClass: null, errorMessage: null };
  fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ helper, ...options }));
  const kubectl = path.join(root, "kubectl");
  fs.writeFileSync(kubectl, KUBECTL, { mode: 0o700 });
  const calls = () => fs.readFileSync(path.join(root, "calls.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
  const outcome = () => JSON.parse(fs.readFileSync(path.join(seat.runDir, "fabric-v2-resume-drain-outcome.json"), "utf8"));
  return { seat, root, kubectl, calls, helper, outcome };
};
const resume = async (target: Awaited<ReturnType<typeof setup>>, extra: Parameters<typeof main>[2] = {}) => {
  let stdout = "", stderr = "";
  const code = await main(["--run-dir", target.seat.runDir, "--phase", "resume-drain"], {
    stdin: () => Buffer.from(target.seat.request("resume-drain", { pod: POD })),
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; }, env: { D3_SEAT_LOCK_FD: String(target.seat.lockFd()) },
  }, { kubectl: target.kubectl, deadlineMs: 20_000, ...extra });
  return { code, stderr, stdout, receipt: stdout ? JSON.parse(stdout) : undefined };
};

describe.skipIf(!linux)("P GAP 6: CLI-free resume-drain", () => {
  it("passes without the nonexistent session CLI: unchanged journal and signal -> same Pod terminated 143", async () => {
    const target = await setup();
    const result = await resume(target);
    expect(result, JSON.stringify({ outcome: target.outcome(), calls: target.calls() })).toMatchObject({ code: 0, stderr: "", receipt: { result: "pass",
      observed: { podUid: POD.uid, inputsFenced: true, exited: true } } });
    expect(target.outcome()).toMatchObject({ reason: null, measured: { journalBytes: 123, journalSha256: "a".repeat(64),
      piPid: 1, piStartTime: "7654321", signalAt: "2026-10-31T19:00:00.000000Z", exitCode: 143, journalAfterExitChecked: false } });
    const execs = target.calls().filter(call => call.argv.includes("exec"));
    expect(execs).toHaveLength(1);
    expect(execs[0].argv.slice(4, 13)).toEqual(["-n", "org-synthetic", "exec", POD.name, "-c", "pi", "--", "/usr/bin/python3", "-I"]);
    expect(execs[0].argv.slice(-3)).toEqual([JOURNAL, "123", "a".repeat(64)]);
    expect(execs[0].argv).not.toContain("session");
    expect(Object.keys(result.receipt.observed).sort()).toEqual(["exited", "inputsFenced", "podUid"]);
    for (const call of target.calls()) expect(call.env.filter((key: string) => !key.startsWith("LC_") || key === "LC_ALL"))
      .toEqual(["HOME", "KUBECONFIG", "LANG", "LC_ALL", "PATH"]);
  });

  it("also records and accepts EOF exit 0 after the seam's signal", async () => {
    const target = await setup({ exitCode: 0 });
    expect((await resume(target)).receipt.result).toBe("pass");
    expect(target.outcome().measured.exitCode).toBe(0);
  });

  for (const [label, patch, reason] of [
    ["journal bytes differ", { journalSha256: "b".repeat(64), inputsFenced: false, signalled: false, reason: "journal-changed" }, "journal-changed"],
    ["two pi processes", { piPid: null, piStartTime: null, inputsFenced: false, signalled: false, reason: "pi-process-count" }, "pi-process-count"],
    ["no signal proof", { signalled: false, signalAt: null }, "pod-check-signal-unreadable"],
    ["missing start time", { piStartTime: null }, "pod-check-signal-unreadable"],
    ["wrong measured hash despite true flag", { journalSha256: "b".repeat(64) }, "pod-check-signal-unreadable"],
    ["wrong measured size despite true flag", { journalBytes: 124 }, "pod-check-signal-unreadable"],
  ] as const) {
    it(`${label} refuses with diagnostics`, async () => {
      const target = await setup();
      const file = path.join(target.root, "state.json");
      const state = JSON.parse(fs.readFileSync(file, "utf8"));
      state.helper = { ...target.helper, ...patch };
      fs.writeFileSync(file, JSON.stringify(state));
      expect((await resume(target)).receipt).toMatchObject({ result: "refused", observed: { exited: false } });
      expect(target.outcome()).toMatchObject({ reason, errorClass: "ResumeDrainRefused", errorMessage: reason });
    });
  }

  for (const [options, reason] of [
    [{ afterUid: "00000000-0000-4000-8000-0000000000bb" }, "pod-uid-changed"],
    [{ exitCode: 1 }, "pod-exit-code-unaccepted"],
    [{ afterContainer: "containerd://replacement" }, "pod-container-changed"],
    [{ execCode: 2 }, "pod-exec-failed"],
    [{ line: "not json" }, "pod-check-signal-unreadable"],
    [{ line: "noise\n{}\n" }, "pod-check-signal-unreadable"],
    [{ line: "null" }, "pod-check-signal-unreadable"],
  ] as const) {
    it(`refuses ${JSON.stringify(options)}`, async () => {
      const target = await setup(options);
      expect((await resume(target)).receipt.result).toBe("refused");
      expect(target.outcome()).toMatchObject({ reason, errorClass: "ResumeDrainRefused", errorMessage: reason });
    });
  }

  it("waits only inside the original phase budget, and never signals twice", async () => {
    const target = await setup({ terminate: false });
    let now = 0;
    expect((await resume(target, { now: () => now, sleep: async ms => { now += ms; }, deadlineMs: 3_000 })).receipt)
      .toMatchObject({ result: "refused", observed: { inputsFenced: true, exited: false } });
    expect(now).toBeLessThanOrEqual(3_000);
    expect(target.calls().filter(call => call.argv.includes("exec"))).toHaveLength(1);
    expect(target.outcome()).toMatchObject({ reason: "phase-deadline", errorMessage: "phase-deadline" });
  });

  for (const args of [[], ["--session-dir", "/only/a/directory"], ["--session"], ["--session", JOURNAL, "--session", JOURNAL],
    ["--session", "relative.jsonl"], ["--session", "/a/../b"]]) {
    it(`never infers a journal from ambiguous/missing session args: ${JSON.stringify(args)}`, async () => {
      const target = await setup({ args });
      expect((await resume(target)).receipt.result).toBe("refused");
      expect(target.outcome()).toMatchObject({ reason: "pod-session-arg-unknown" });
      expect(target.calls().some(call => call.argv.includes("exec"))).toBe(false);
    });
  }

  for (const mutation of ["missing", "different-bytes", "wrong-row", "not-accepted", "wrong-pod", "wrong-path", "wrong-run", "wrong-owner", "wrong-size-hash"] as const) {
    it(`refuses before exec when the accepted resume witness is ${mutation}`, async () => {
      const target = await setup();
      const file = path.join(target.seat.runDir, "receipts", "resume.json");
      const rowFile = path.join(target.seat.runDir, "seat-journal.jsonl");
      const rows = fs.readFileSync(rowFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
      if (mutation === "missing") fs.renameSync(file, `${file}.saved`);
      else if (mutation === "different-bytes") fs.appendFileSync(file, " ");
      else if (mutation === "wrong-row") rows.find(row => row.phase === "resume").sha256 = "0".repeat(64);
      else if (mutation === "not-accepted") rows.pop();
      else if (mutation === "wrong-size-hash") {
        const outcomeFile = path.join(target.seat.runDir, "fabric-v2-bridge-resume-outcome.json");
        const value = JSON.parse(fs.readFileSync(outcomeFile, "utf8"));
        value.measured.journal.journalSha256 = "b".repeat(64);
        fs.writeFileSync(outcomeFile, JSON.stringify(value));
      } else {
        const value = JSON.parse(fs.readFileSync(file, "utf8"));
        if (mutation === "wrong-pod") value.observed.podUid = "other";
        if (mutation === "wrong-path") value.observed.journal = "/wrong/file";
        if (mutation === "wrong-run") value.run = "0".repeat(32);
        if (mutation === "wrong-owner") value.owner = "other";
        fs.writeFileSync(file, JSON.stringify(value));
        rows.find(row => row.phase === "resume").sha256 = sha(fs.readFileSync(file));
      }
      fs.writeFileSync(rowFile, rows.map(row => `${JSON.stringify(row)}\n`).join(""));
      expect((await resume(target)).receipt.result).toBe("refused");
      expect(target.calls().some(call => call.argv.includes("exec"))).toBe(false);
      expect(target.outcome()).toMatchObject({ errorClass: "SeamGap", errorMessage: expect.stringMatching(/^resume-/) });
    });
  }

  it("preserves UNKNOWN/no-receipt on an initial Pod UID or target-node binding mismatch", async () => {
    for (const [options, reason] of [[{ uid: "00000000-0000-4000-8000-0000000000bb" }, "pod-uid-mismatch"],
      [{ node: "other-node" }, "pod-node-mismatch"]] as const) {
      const target = await setup(options);
      expect(await resume(target)).toMatchObject({ code: 2, stdout: "", stderr: `GAP: ${reason} [owner: fabric-v2]\n` });
      expect(target.calls().some(call => call.argv.includes("exec"))).toBe(false);
    }
  });
});

const startPi = async () => {
  const child = spawn("/usr/bin/python3", ["-I", "-c", `import ctypes, os, signal, sys, time
ctypes.CDLL(None).prctl(15, b'pi', 0, 0, 0)
signal.signal(signal.SIGTERM, lambda *args: sys.exit(143))
print(os.getpid(), flush=True)
while True: time.sleep(0.1)
`], { stdio: ["ignore", "pipe", "pipe"] });
  children.push(child);
  const exit = once(child, "exit");
  await once(child.stdout!, "data");
  expect(fs.readFileSync(`/proc/${child.pid}/comm`, "utf8")).toBe("pi\n");
  return { child, exit, pid: child.pid! };
};

// Execute the EXACT production script with real /proc identity reads, journal bytes and os.kill.
// Only /proc's directory listing is scoped to test-owned processes, so no fleet process can be signalled.
const helper = (file: string, size: number, digest: string, pids: number[]) => {
  const bootstrap = `import json, os, sys
pids = json.loads(sys.argv.pop(1))
script = sys.argv.pop(1)
listdir = os.listdir
os.listdir = lambda p: [str(pid) for pid in pids] if p == '/proc' else listdir(p)
exec(compile(script, '<production-resume-drain>', 'exec'))
`;
  const result = spawnSync("/usr/bin/python3", ["-I", "-c", bootstrap, JSON.stringify(pids), RESUME_DRAIN_SCRIPT, file, String(size), digest],
    { encoding: "utf8", timeout: 5_000 });
  expect(result.stderr).toBe("");
  expect(result.stdout.trim().split("\n")).toHaveLength(1);
  return { code: result.status, measured: JSON.parse(result.stdout) };
};

describe.skipIf(!linux)("the exact isolated Python check + signal program", () => {
  it("hashes real bytes and sends real SIGTERM to the discovered non-1 pi pid", async () => {
    const target = await setup();
    const file = path.join(target.root, "journal with spaces; not-a-shell.jsonl");
    const bytes = Buffer.from('immutable accepted session bytes\n');
    fs.writeFileSync(file, bytes);
    const pi = await startPi();
    const result = helper(file, bytes.length, sha(bytes), [pi.pid]);
    expect(result).toMatchObject({ code: 0, measured: { inputsFenced: true, signalled: true, piPid: pi.pid,
      piStartTime: expect.stringMatching(/^[0-9]+$/), journalSha256: sha(bytes), journalBytes: bytes.length,
      signalAt: expect.stringMatching(/^\d{4}-.*Z$/), reason: null } });
    expect(await pi.exit).toEqual([143, null]);
    expect(fs.readFileSync(file)).toEqual(bytes); // Test-owned reader, NOT a seam post-exit claim.
  });

  for (const mutation of ["size", "sha", "missing", "symlink", "two", "zero"] as const) {
    it(`${mutation} refuses WITHOUT signalling any test pi process`, async () => {
      const target = await setup();
      const file = path.join(target.root, "journal.jsonl");
      const bytes = Buffer.from("accepted bytes\n");
      fs.writeFileSync(file, bytes);
      const pi = await startPi();
      let requested = file;
      let pids = [pi.pid];
      if (mutation === "missing") requested = `${file}.absent`;
      if (mutation === "symlink") { requested = `${file}.link`; fs.symlinkSync(file, requested); }
      if (mutation === "two") pids.push((await startPi()).pid);
      if (mutation === "zero") pids = [];
      const result = helper(requested, bytes.length + (mutation === "size" ? 1 : 0), mutation === "sha" ? "0".repeat(64) : sha(bytes), pids);
      expect(result).toMatchObject({ code: 1, measured: { inputsFenced: false, signalled: false, signalAt: null,
        reason: expect.any(String), errorClass: expect.any(String), errorMessage: expect.any(String) } });
      expect(pi.child.exitCode).toBeNull();
      expect(fs.existsSync(`/proc/${pi.pid}`)).toBe(true);
      expect(fs.readFileSync(file)).toEqual(bytes);
    });
  }
});
