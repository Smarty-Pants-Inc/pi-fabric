import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadFabricConfig } from "../src/config.js";
import { normalizeAgentPlacement } from "../src/agents/placement-config.js";
import { launchPlacedTask } from "../src/agents/transports/placement.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";

// Keep reason: launch/result trust boundary and exit/cancellation failure paths.
// This fixture tests the Fabric side of the agreed wire protocol, NOT installed
// factory compatibility, remote enforcement, or actual native worker execution.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
});
const digest = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const launcher = `import fs from 'node:fs'; import path from 'node:path';
const [mode, root, id, behavior, ...argv] = process.argv.slice(2);
const dir = path.join(root, id); fs.mkdirSync(dir, {recursive:true});
if (mode === 'launch') {
 fs.appendFileSync(path.join(dir,'launches'), 'launch\\n');
 fs.writeFileSync(path.join(dir,'argv.json'), JSON.stringify(argv));
 const end = argv.indexOf('--') < 0 ? argv.length : argv.indexOf('--');
 const allowed = new Set(['--host','--cwd','--model','--thinking','--minutes','--require','--worker-packet']);
 for (let i=0; i<end; i+=2) if (!allowed.has(argv[i]) || i+1>=end) throw new Error('unsupported launcher option');
 if (behavior === 'unavailable') { console.error('PLACEMENT_UNAVAILABLE: incompatible installed worker; no task started'); process.exit(3); }
 if (behavior === 'exit3') { console.error('arbitrary failure; no task started'); process.exit(3); }
 if (behavior === 'bad-refusal') { console.error('PLACEMENT_UNAVAILABLE: maybe ran'); process.exit(3); }
 if (behavior === 'wrong-exit') { console.error('PLACEMENT_UNAVAILABLE: incompatible installed worker; no task started'); process.exit(7); }
 if (behavior === 'accepted-refusal') { console.log('RYZEN2_TASK_ACCEPTED '+id+' on ryzen2'); console.error('PLACEMENT_UNAVAILABLE: inconsistent; no task started'); process.exit(3); }
 const index = argv.indexOf('--worker-packet');
 if (index >= 0) fs.copyFileSync(argv[index+1], path.join(dir,'packet.json'));
 if (behavior === 'missing-acceptance') { console.log('nothing authoritative'); process.exit(0); }
 console.log('RYZEN2_TASK_ACCEPTED '+(behavior === 'wrong-id' ? 'other-run' : id)+' on ryzen2 (unit smarty-task-'+id+').');
} else if (mode === 'result') {
 const file = path.join(dir,'receipt.json');
 console.log(fs.existsSync(file) ? fs.readFileSync(file,'utf8') : JSON.stringify({rc:null}));
} else if (mode === 'cancel') {
 fs.appendFileSync(path.join(dir,'cancels'), 'cancel\\n');
 const file = path.join(dir,'cancel-receipt.json');
 if (fs.existsSync(file)) fs.copyFileSync(file,path.join(dir,'receipt.json'));
 console.log('cancellation acknowledged');
}
`;
const fixture = (behavior = "normal", native = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-placement-")); roots.push(root);
  const callerTmp = path.join(root, "caller-tmp"); fs.mkdirSync(callerTmp); vi.stubEnv("TMPDIR", callerTmp);
  const profile = path.join(root, "profile"); fs.mkdirSync(profile); vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  const run = path.join(root, "run"); fs.mkdirSync(run);
  const executable = path.join(root, "launcher.mjs"); fs.writeFileSync(executable, launcher);
  const workerPath = path.join(root, "worker.mjs"); fs.writeFileSync(workerPath, "// exact installed worker entry fixture\r\n");
  const taskFile = path.join(run, "task.txt"); fs.writeFileSync(taskFile, "native task ' ; $(literal)\r\n");
  const results = path.join(root, "results");
  const raw = {
    default: "remote", sharedMounts: [root],
    command: [process.execPath, executable, "launch", results, "{id}", behavior, "--host", "auto", "--cwd", "{cwd}", "--model", "{model}", "--thinking", "{thinking}", "--minutes", "{minutes}", "--", "{task}"],
    resultCommand: [process.execPath, executable, "result", results, "{id}", behavior],
    cancelCommand: [process.execPath, executable, "cancel", results, "{id}", behavior],
    pollIntervalMs: 10, commandTimeoutMs: 5_000,
    ...(native ? { workerProtocol: "fabric-v1" } : {}),
  };
  const config = normalizeAgentPlacement(raw)!;
  const request: AgentTransportLaunch = {
    id: "native-id", name: "native", cwd: root, workerPath,
    workerArguments: ["--id", "native-id", "--name", "native", "--runner", "pi", "--transport", "process", "--cwd", root,
      "--task-file", taskFile, "--status-file", path.join(run, "status.json"), "--log-file", path.join(run, "events.jsonl"),
      "--timeout-ms", "600001", "--model", "test/requested", "--thinking", "xhigh"],
  };
  const resultDir = path.join(results, request.id);
  const readStatus = (): AgentRunRecord => JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
  const readEvents = (): { type: string; [key: string]: unknown }[] => fs.readFileSync(path.join(run, "events.jsonl"), "utf8")
    .trim().split("\n").map(line => JSON.parse(line));
  const writeReceipt = (value: unknown, file = "receipt.json"): void => {
    fs.mkdirSync(resultDir, { recursive: true }); fs.writeFileSync(path.join(resultDir, file), JSON.stringify(value));
  };
  const record = (change: Partial<AgentRunRecord> = {}): AgentRunRecord => ({
    id: request.id, name: "native", task: "native worker task", runner: "pi", transport: "process", cwd: "/target/staged",
    status: "completed", startedAt: 100, updatedAt: 200, finishedAt: 200, turns: 3, toolCalls: 2, text: "native answer",
    usage: { input: 101, output: 23, cacheRead: 17, cacheWrite: 7, cost: 0.0123 },
    model: "test/observed", thinking: "high", admittedModel: "test/admitted", admittedThinking: "xhigh",
    requestedModel: "test/requested", replyVia: "tool", value: { answer: 42 },
    sessionId: "process:target", runnerSessionId: "native-session", runnerSessionIds: ["seed-session", "native-session"],
    fabricRelease: "/target/installed/release", logFile: "/target/run/events.jsonl", placementHost: "ryzen2",
    warnings: ["native warning"], ...change,
  });
  return { root, run, profile, taskFile, workerPath, resultDir, raw, config, request, readStatus, readEvents, writeReceipt, record };
};
const poll = async (handle: AgentTransportHandle): Promise<boolean> => {
  // Advance only the safety-net poll gate, not process timers or child execution.
  vi.spyOn(Date, "now").mockReturnValue(Date.now() + 50);
  return handle.isAlive();
};
const launches = (f: ReturnType<typeof fixture>): string[] => fs.readFileSync(path.join(f.resultDir, "launches"), "utf8").trim().split("\n");

describe("native worker packet placement (host opt-in)", () => {
  it("accepts only fabric-v1 with resultCommand and keeps absent protocol legacy", () => {
    const f = fixture();
    expect(f.config.workerProtocol).toBe("fabric-v1");
    expect(normalizeAgentPlacement({ ...f.raw, workerProtocol: undefined })?.workerProtocol).toBeUndefined();
    for (const workerProtocol of [null, false, 1, "", "fabric-v2", "FABRIC-V1", " fabric-v1"]) {
      expect(() => normalizeAgentPlacement({ ...f.raw, workerProtocol })).toThrow("workerProtocol");
    }
    expect(() => normalizeAgentPlacement({ ...f.raw, resultCommand: undefined, resultDirectory: path.join(f.root, "{id}") })).toThrow("requires resultCommand");
    expect(() => normalizeAgentPlacement({ ...f.raw, resultCommand: [] })).toThrow("resultCommand");
  });

  it("takes the protocol only from host config, never workspace config", () => {
    const f = fixture();
    fs.mkdirSync(path.join(f.root, ".pi"));
    fs.writeFileSync(path.join(f.root, ".pi", "fabric.json"), JSON.stringify({ agents: { placement: f.raw } }));
    expect(loadFabricConfig({ cwd: f.root, agentDir: f.profile, projectTrusted: true }).agents.placement).toBeUndefined();
    fs.writeFileSync(path.join(f.profile, "fabric.json"), JSON.stringify({ agents: { placement: f.raw } }));
    fs.writeFileSync(path.join(f.root, ".pi", "fabric.json"), JSON.stringify({ agents: { placement: { ...f.raw, workerProtocol: "invented", default: "local" } } }));
    expect(loadFabricConfig({ cwd: f.root, agentDir: f.profile, projectTrusted: true }).agents.placement).toEqual(f.config);
  });

  it("sends exact ordered argv and input bytes, with no path/text substitution or scalar loss", async () => {
    const f = fixture();
    const installed = path.join(f.root, "installed"); fs.mkdirSync(installed);
    const entry = path.join(installed, "worker.mjs"); fs.copyFileSync(f.workerPath, entry);
    const alias = path.join(f.root, "installed-link"); fs.symlinkSync(installed, alias, process.platform === "win32" ? "junction" : "dir");
    f.request.workerPath = path.join(alias, "worker.mjs");
    const inputs = new Map<string, Buffer>([
      ["--task-file", fs.readFileSync(f.taskFile)],
      ["--schema-file", Buffer.from('{"type":"object","description":"'+f.taskFile+'"}\r\n')],
      ["--images-file", Buffer.from('[{"type":"image","data":"AAE=","mimeType":"image/png"}]\r\n')],
      ["--session-file", Buffer.from([0xef, 0xbb, 0xbf, 0x0a, 0x00, 0xff])],
    ]);
    for (const [flag, bytes] of inputs) {
      if (flag === "--task-file") continue;
      const file = path.join(f.run, `${flag.slice(2)} {host}.input`); fs.writeFileSync(file, bytes);
      f.request.workerArguments.push(flag, file);
    }
    const systemPrompt = `replace exactly\r\n${f.taskFile} {host} ' "; $(never execute)`;
    const extra = [
      "--tools", "[]", "--extensions", "false", "--full-code-mode", "false", "--judgment", "true", "--reply-tool", "true",
      "--route-header", "", "--model-reason", "", "--system-prompt", systemPrompt,
      "--thinking-bounds", '{"min":"high","max":"xhigh"}', "--model-admission", "strict", "--model-policy-warnings", "[]",
      "--write-policy", JSON.stringify({ readOnly: true, shell: "deny", writableRoots: [path.join(f.root, "allowed")] }),
      "--tmpdir", path.join(f.root, "caller-tmp"), "--scope", JSON.stringify({ literal: f.taskFile }),
      "--session-export-file", path.join(f.run, "export.jsonl"), "--persist-session", "false",
      "--inherited-session-pins", '[{"pool":"test","accountId":"exact"}]', "--inference-context", "fresh",
      "--prompt-path", f.taskFile, "--prompt-bytes", "0", "--max-tokens", "0", "--nice", "0",
      "--main-agent-id", "main", "--spawner-id", "parent", "--depth", "1", "--fabric-session-id", "caller-session",
      "--task-return-address", '{"agentId":"parent","sessionId":"bound"}', "--lineage", "[]",
      "--lifecycle-file", path.join(f.run, "lifecycle.json"), "--steer-file", path.join(f.run, "steer.jsonl"),
      "--run-root", f.run, "--project-root", f.root,
    ];
    f.request.workerArguments.push(...extra);
    f.request.requires = ["/target/only {host} literal"];
    const expectedArguments = [...f.request.workerArguments];
    const handle = await new ProcessTransport(undefined, f.config).launch(f.request);
    expect(handle.controls).toBe(false); expect(handle.relaunchable).toBe(false);
    const argv: string[] = JSON.parse(fs.readFileSync(path.join(f.resultDir, "argv.json"), "utf8"));
    const packetIndex = argv.indexOf("--worker-packet"), separator = argv.indexOf("--");
    expect(packetIndex).toBeGreaterThan(-1); expect(packetIndex + 2).toBe(separator);
    expect(argv.slice(packetIndex - 2, packetIndex)).toEqual(["--require", f.request.requires[0]]);
    expect(argv[separator + 1]).toBe(fs.readFileSync(f.taskFile, "utf8"));
    expect(argv[argv.indexOf("--minutes") + 1]).toBe("11"); // only legacy scalar is rounded; native ms are exact
    const packetFile = argv[packetIndex + 1]!;
    expect(path.dirname(packetFile)).toBe(f.run);
    if (process.platform !== "win32") expect(fs.statSync(packetFile).mode & 0o777).toBe(0o600);
    const packet = JSON.parse(fs.readFileSync(path.join(f.resultDir, "packet.json"), "utf8"));
    expect(packet).toEqual({
      version: 1, workerPath: fs.realpathSync(entry), workerSha256: digest(fs.readFileSync(entry)), workerArguments: expectedArguments,
      inputs: [...inputs].map(([flag, bytes]) => ({ flag, base64: bytes.toString("base64"), sha256: digest(bytes) })),
    });
    expect(f.request.workerArguments).toEqual(expectedArguments);
    f.writeReceipt({ rc: 0, text: "envelope text is not reconstructed provenance", worker: f.record(), sessionExport: { base64: "", sha256: digest(Buffer.alloc(0)) } });
    expect(await poll(handle)).toBe(false);
  });

  it("keeps empty optional file flags without inventing file inputs or export destinations", async () => {
    const f = fixture();
    f.request.workerArguments.push("--schema-file", "", "--images-file", "", "--session-file", "", "--session-export-file", "", "--system-prompt", "");
    const worker = f.record(); const bytes = Buffer.alloc(0);
    f.writeReceipt({ rc: 0, text: worker.text, worker, sessionExport: { base64: "", sha256: digest(bytes) } });
    const handle = await launchPlacedTask(f.request, f.config);
    const packet = JSON.parse(fs.readFileSync(path.join(f.resultDir, "packet.json"), "utf8"));
    expect(packet.workerArguments).toEqual(f.request.workerArguments);
    expect(packet.inputs).toEqual([{ flag: "--task-file", base64: fs.readFileSync(f.taskFile).toString("base64"), sha256: digest(fs.readFileSync(f.taskFile)) }]);
    expect(await poll(handle)).toBe(false); expect(f.readStatus()).toEqual({ ...worker, exitCode: 0 });
  });

  it("returns the full native record, structured provenance/accounting/session fields and exact export bytes", async () => {
    const f = fixture();
    const destination = path.join(f.run, "caller-export.jsonl");
    const bytes = Buffer.from([0xef, 0xbb, 0xbf, 0x7b, 0x7d, 0x0d, 0x0a, 0x00, 0xff]);
    f.request.workerArguments.push("--session-export-file", destination);
    const worker = { ...f.record(), futureNativeField: { retained: true } };
    f.writeReceipt({ rc: "0", text: "not the authoritative native text", stderr: "", worker, sessionExport: { base64: bytes.toString("base64"), sha256: digest(bytes) } });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toEqual({ ...worker, exitCode: 0, stderr: "" });
    expect(fs.readFileSync(destination)).toEqual(bytes);
    expect(handle.lostContact?.()).toBeUndefined();
    expect(launches(f)).toEqual(["launch"]);
  });

  it.each([null, false, "", 0, [], {}].map(value => ({ value })))("preserves structured value $value rather than falling back to prose", async ({ value }) => {
    const f = fixture(); const worker = f.record({ value });
    f.writeReceipt({ rc: 0, text: "legacy prose", worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toEqual({ ...worker, exitCode: 0 });
  });

  it("preserves a native failure despite rc=0, rather than synthesizing completed from launcher success", async () => {
    const f = fixture(); const worker = f.record({ status: "failed", error: "fabric_reply validation failed" });
    delete worker.value; delete worker.replyVia;
    f.writeReceipt({ rc: 0, text: "JSON-looking final prose", worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toMatchObject({ status: "failed", error: worker.error, exitCode: 0, usage: worker.usage });
    expect(f.readStatus().replyVia).toBeUndefined();
  });

  it.each([7, -15])("retains native runner exit %j/stderr rather than replacing them with outer worker metadata", async exitCode => {
    const f = fixture(); const worker = f.record({ status: "failed", exitCode, stderr: "native runner stderr", error: "runner failed" });
    f.writeReceipt({ rc: 1, text: worker.text, stderr: "outer worker stderr", worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false); expect(f.readStatus()).toEqual(worker);
  });

  it.each([-1, "-1", 256, "256"])("retains custody through stop when impossible native exit rc %j persists", async rc => {
    const f = fixture(); const worker = f.record({ status: "failed", exitCode: -15, error: "runner signalled" });
    const onUnconfirmedExit = vi.fn(); f.request.onUnconfirmedExit = onUnconfirmedExit;
    f.writeReceipt({ rc, text: worker.text, worker });
    const handle = await new ProcessTransport(undefined, f.config).launch(f.request);
    expect(await poll(handle)).toBe(true);
    expect(f.readStatus()).toMatchObject({ status: "running", text: "" });
    expect(f.readStatus().value).toBeUndefined(); expect(f.readStatus().exitCode).toBeUndefined();
    expect(f.readEvents().map(event => event.type)).toEqual(["placement.remote"]);
    expect(onUnconfirmedExit).not.toHaveBeenCalled();
    expect(handle.relaunchable).toBe(false);

    // Restore the poll-gate clock so the existing cancellation deadline advances.
    vi.mocked(Date.now).mockRestore();
    f.config.commandTimeoutMs = 100;
    // Cancellation acknowledgement does not repair or supersede the same bad exit receipt.
    await handle.stop();
    const debt = handle.stopDebt?.();
    expect(debt).toContain("exit unconfirmed; custody retained");
    expect(debt).toContain("numeric terminal exit rc in 0..255");
    expect(handle.lostContact?.()).toBe(debt);
    expect(f.readStatus()).toMatchObject({ status: "stopped", error: debt });
    expect(f.readStatus().value).toBeUndefined(); expect(f.readStatus().exitCode).toBeUndefined();
    expect(onUnconfirmedExit).toHaveBeenCalledExactlyOnceWith(debt);
    expect(f.readEvents()).toEqual([
      expect.objectContaining({ type: "placement.remote", host: "ryzen2" }),
      expect.objectContaining({ type: "placement.exit_unconfirmed", reason: debt }),
    ]);
    await handle.stop(); expect(await handle.isAlive()).toBe(false);
    expect(handle.stopDebt?.()).toBe(debt); // logical stop is not native-exit proof
    expect(fs.readFileSync(path.join(f.resultDir, "cancels"), "utf8")).toBe("cancel\n");
    expect(launches(f)).toEqual(["launch"]);
  });

  it.each([0, "0", 255, "255"])("accepts normalized native exit boundary rc %j with its actual worker outcome", async rc => {
    const f = fixture(); const actualRc = Number(rc);
    const worker = f.record(actualRc === 0 ? {} : { status: "failed", error: "worker exited 255" });
    f.writeReceipt({ rc, text: worker.text, worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toEqual({ ...worker, exitCode: actualRc });
    expect(handle.lostContact?.()).toBeUndefined(); expect(handle.stopDebt?.()).toBeUndefined();
    expect(f.readEvents().filter(event => event.type === "placement.result")).toEqual([
      expect.objectContaining({ host: "ryzen2", status: worker.status, rc }),
    ]);
    await handle.stop(); expect(fs.existsSync(path.join(f.resultDir, "cancels"))).toBe(false);
    expect(launches(f)).toEqual(["launch"]);
  });

  it.each(["not-an-exit", " 0 ", 1.5, 7])("does not settle native success with invalid/conflicting receiver rc %j", async rc => {
    const f = fixture(); const worker = f.record();
    f.writeReceipt({ rc, text: worker.text, worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(true); expect(f.readStatus().status).toBe("running");
    expect(f.readStatus().value).toBeUndefined(); expect(launches(f)).toEqual(["launch"]);
    f.writeReceipt({ rc: 0, text: worker.text, worker }); expect(await poll(handle)).toBe(false);
  });

  it("does not consume a terminal-looking worker until the receiver reports actual exit rc", async () => {
    const f = fixture(); const worker = f.record();
    const destination = path.join(f.run, "export.jsonl"); f.request.workerArguments.push("--session-export-file", destination);
    const bytes = Buffer.from("not yet exited\r\n");
    const sessionExport = { base64: bytes.toString("base64"), sha256: digest(bytes) };
    f.writeReceipt({ rc: null, text: worker.text, worker, sessionExport });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(true); expect(f.readStatus().status).toBe("running");
    expect(f.readStatus().exitCode).toBeUndefined(); expect(handle.stopDebt?.()).toBeUndefined();
    expect(f.readEvents().map(event => event.type)).toEqual(["placement.remote"]);
    expect(fs.existsSync(destination)).toBe(false);
    f.writeReceipt({ rc: 0, text: worker.text, worker, sessionExport });
    expect(await poll(handle)).toBe(false); expect(fs.readFileSync(destination)).toEqual(bytes);
  });

  it.each(["legacy", "missing", "running", "wrong-id", "wrong-host", "malformed", "bad-provenance", "bad-usage"])("rejects %s native terminal receipts without settling or re-launching", async kind => {
    const f = fixture(); const worker = f.record();
    const invalid = kind === "legacy" ? { rc: 0, text: "legacy answer" }
      : kind === "missing" ? { rc: 0, text: "missing", worker: null }
      : { rc: 0, text: "untrusted", worker: kind === "running" ? { ...worker, status: "running" }
        : kind === "wrong-id" ? { ...worker, id: "another-run" }
        : kind === "wrong-host" ? { ...worker, placementHost: "ryzen3" }
        : kind === "bad-provenance" ? { ...worker, replyVia: "text" }
        : kind === "bad-usage" ? { ...worker, usage: { ...worker.usage, input: -1 } } : { ...worker, usage: null } };
    f.writeReceipt(invalid);
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(true); expect(f.readStatus().status).toBe("running");
    expect(f.readStatus().value).toBeUndefined(); expect(launches(f)).toEqual(["launch"]);
    f.writeReceipt({ rc: 0, text: worker.text, worker });
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toEqual({ ...worker, exitCode: 0 });
  });

  it.each(["hash", "encoding"])("rejects corrupt export %s without replacing the caller file", async corruption => {
    const f = fixture(); const destination = path.join(f.run, "export.jsonl"); fs.writeFileSync(destination, "prior export");
    f.request.workerArguments.push("--session-export-file", destination);
    const bytes = Buffer.from("new exact export\r\n"); const worker = f.record();
    const sessionExport = { base64: bytes.toString("base64") + (corruption === "encoding" ? "!" : ""), sha256: corruption === "hash" ? "0".repeat(64) : digest(bytes) };
    f.writeReceipt({ rc: 0, text: worker.text, worker, sessionExport });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(true); expect(f.readStatus().status).toBe("running");
    expect(fs.readFileSync(destination, "utf8")).toBe("prior export");
    expect(launches(f)).toEqual(["launch"]);
    f.writeReceipt({ rc: 0, text: worker.text, worker, sessionExport: { base64: bytes.toString("base64"), sha256: digest(bytes) } });
    expect(await poll(handle)).toBe(false); expect(fs.readFileSync(destination)).toEqual(bytes);
  });

  it("does not settle completed native session preservation without the requested export, then accepts exact bytes", async () => {
    const f = fixture(); const destination = path.join(f.run, "requested-export.jsonl"); const worker = f.record();
    f.request.workerArguments.push("--session-export-file", destination);
    f.writeReceipt({ rc: 0, text: worker.text, worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(true); expect(f.readStatus().status).toBe("running");
    expect(f.readStatus().warnings?.join(" ")).not.toMatch(/telemetry|usage accounting/u);
    expect(fs.existsSync(destination)).toBe(false); expect(launches(f)).toEqual(["launch"]);
    const bytes = Buffer.from("{\"type\":\"session\",\"id\":\"exact\"}\r\n");
    f.writeReceipt({ rc: 0, text: worker.text, worker, sessionExport: { base64: bytes.toString("base64"), sha256: digest(bytes) } });
    expect(await poll(handle)).toBe(false); expect(fs.readFileSync(destination)).toEqual(bytes);
    expect(f.readStatus()).toEqual({ ...worker, exitCode: 0 });
    expect(f.readStatus().warnings).toEqual(["native warning"]);
  });

  it.each(["failed", "stopped", "timed_out"] as const)("preserves native %s without inventing an export when no session was created", async status => {
    const f = fixture(); const destination = path.join(f.run, "absent-export.jsonl");
    f.request.workerArguments.push("--session-export-file", destination);
    const worker = f.record({ status, error: "native session was not created" });
    const rc = status === "failed" ? 1 : 124;
    f.writeReceipt({ rc, text: worker.text, worker });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false); expect(fs.existsSync(destination)).toBe(false);
    expect(f.readStatus()).toEqual({ ...worker, exitCode: rc });
    expect(f.readStatus().usage).toEqual(worker.usage);
    expect(f.readStatus().warnings?.join(" ")).not.toMatch(/telemetry|usage accounting/u);
  });

  it("does not let a receipt select an export destination when none was requested", async () => {
    const f = fixture(); const worker = f.record(); const bytes = Buffer.alloc(0);
    const unexpected = path.join(f.root, "not-requested");
    f.writeReceipt({ rc: 0, text: worker.text, worker, sessionExport: { base64: "", sha256: digest(bytes), path: unexpected } });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false); expect(fs.existsSync(unexpected)).toBe(false);
  });

  it("requires a matching native record on stop too and retains unknown exit custody", async () => {
    const f = fixture(); f.writeReceipt({ rc: 0, text: "legacy terminal" });
    const onUnconfirmedExit = vi.fn(); f.request.onUnconfirmedExit = onUnconfirmedExit;
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(true);
    await handle.stop();
    expect(handle.stopDebt?.()).toContain("exit unconfirmed");
    expect(handle.stopDebt?.()).toContain("matching terminal worker record");
    expect(handle.lostContact?.()).toBe(handle.stopDebt?.());
    expect(f.readStatus()).toMatchObject({ status: "stopped" }); expect(f.readStatus().exitCode).toBeUndefined();
    expect(onUnconfirmedExit).toHaveBeenCalledOnce(); expect(launches(f)).toEqual(["launch"]);
  });

  it("settles cancellation only with receiver exit rc and the actual terminal worker record", async () => {
    const f = fixture(); const worker = f.record({ status: "stopped", text: "partial native", value: false });
    f.writeReceipt({ rc: 124, text: "wrapper text", worker }, "cancel-receipt.json");
    const handle = await launchPlacedTask(f.request, f.config);
    await handle.stop();
    expect(f.readStatus()).toEqual({ ...worker, exitCode: 124 });
    expect(handle.stopDebt?.()).toBeUndefined(); expect(await handle.isAlive()).toBe(false);
  });

  it("recognizes only the agreed exit-3 factory known-unlaunched refusal", async () => {
    const f = fixture("unavailable");
    await expect(launchPlacedTask(f.request, f.config)).rejects.toMatchObject({
      code: "FABRIC_AGENT_INPUT_ERROR", field: "placement", launchOutcome: "unlaunched",
      message: expect.stringContaining("placement unavailable; no task started"),
    });
    expect(fs.existsSync(path.join(f.run, "status.json"))).toBe(false);
    expect(launches(f)).toEqual(["launch"]);
  });

  it.each(["exit3", "bad-refusal", "wrong-exit", "accepted-refusal", "missing-acceptance", "wrong-id"])("retains %s unknown launch custody, without retries or a local worker", async behavior => {
    const f = fixture(behavior); const onUnconfirmedExit = vi.fn(); f.request.onUnconfirmedExit = onUnconfirmedExit;
    const handle = await launchPlacedTask(f.request, f.config);
    expect(handle.lostContact?.()).toContain("launch outcome unknown"); expect(handle.stopDebt?.()).toBe(handle.lostContact?.());
    expect(f.readStatus()).toMatchObject({ status: "failed", text: "", error: expect.stringContaining("unknown") });
    expect(onUnconfirmedExit).toHaveBeenCalledOnce(); expect(launches(f)).toEqual(["launch"]);
    await handle.stop(); expect(await handle.isAlive()).toBe(false);
    expect(fs.existsSync(path.join(f.resultDir, "cancels"))).toBe(false);
  });

  it("fails packet preparation before invocation when worker entry or staged input is missing", async () => {
    const f = fixture(); fs.unlinkSync(f.workerPath);
    await expect(launchPlacedTask(f.request, f.config)).rejects.toThrow("ENOENT");
    expect(fs.existsSync(f.resultDir)).toBe(false);
    fs.writeFileSync(f.workerPath, "// available"); f.request.workerArguments.push("--schema-file", path.join(f.run, "missing-schema"));
    await expect(launchPlacedTask(f.request, f.config)).rejects.toThrow("ENOENT");
    expect(fs.existsSync(f.resultDir)).toBe(false);
  });

  it("rejects a mismatched packet id before launcher invocation", async () => {
    const f = fixture(); f.request.workerArguments[1] = "other-run";
    await expect(launchPlacedTask(f.request, f.config)).rejects.toMatchObject({ launchOutcome: "unlaunched", field: "placement" });
    expect(fs.existsSync(f.resultDir)).toBe(false);
  });

  it.each([-1, "-1", 256, "256"])("keeps legacy terminal rc %j compatibility separate from native exit proof", async rc => {
    const f = fixture("normal", false);
    f.writeReceipt({ rc, text: "legacy failure" });
    const handle = await launchPlacedTask(f.request, f.config);
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toMatchObject({ status: "failed", text: "legacy failure", exitCode: Number(rc) });
    expect(handle.stopDebt?.()).toBeUndefined(); expect(handle.lostContact?.()).toBeUndefined();
    expect(f.readEvents().filter(event => event.type === "placement.result")).toEqual([
      expect.objectContaining({ status: "failed", rc }),
    ]);
    expect(launches(f)).toEqual(["launch"]);
  });

  it("leaves legacy argv, raw-text results and zero-accounting behavior unchanged", async () => {
    const f = fixture("normal", false); f.request.workerPath = "legacy never reads workerPath";
    f.request.workerArguments.push("--tools", "[]", "--extensions", "false");
    f.writeReceipt({ rc: "0", text: "LEGACY TEXT" });
    const handle = await new ProcessTransport(undefined, f.config).launch(f.request);
    expect(await poll(handle)).toBe(false);
    expect(f.readStatus()).toMatchObject({ status: "completed", text: "LEGACY TEXT", exitCode: 0, turns: 0, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
    const argv = JSON.parse(fs.readFileSync(path.join(f.resultDir, "argv.json"), "utf8"));
    expect(argv).toEqual(["--host", "auto", "--cwd", f.root, "--model", "test/requested", "--thinking", "xhigh", "--minutes", "11", "--", fs.readFileSync(f.taskFile, "utf8")]);
    expect(fs.existsSync(path.join(f.run, "placement-worker.json"))).toBe(false);
  });
});
