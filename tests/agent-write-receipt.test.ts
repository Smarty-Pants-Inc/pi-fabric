// smarty-dev#8758 item 2: a process task agent's final envelope carries a tool-generated receipt of the paths its
// write/edit tools changed and the shell commands it ran, so the spawning lane trusts the receipt, not the prose.
// (Item 3, denyCommands, is split to smarty-dev#11186.)
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { processStartTime } from "../src/residency/process-identity.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const saved = { behavior: process.env.FAKE_PI_BEHAVIOR };
afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  if (saved.behavior === undefined) delete process.env.FAKE_PI_BEHAVIOR; else process.env.FAKE_PI_BEHAVIOR = saved.behavior;
});
const managerIn = (behavior: string) => {
  process.env.FAKE_PI_BEHAVIOR = behavior;
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-receipt-")));
  roots.push(root);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 20_000 }, {
    workerPath: path.resolve("src/worker.ts"),
    piBinary: path.resolve("tests/fixtures/fake-pi.mjs"),
    runRoot: path.join(root, "runs"),
    fullCodeMode: false,
  });
  managers.push(manager);
  return { root, manager };
};

// pi-fabric#1034 round 6 (Astra P1): a tool event line over the 4 Mi-char cap is dropped by the worker unread, so
// the recorder never sees that start or end. Every such drop marks the receipt truncated. A one-off fake Pi (written
// to this test's temp root) emits one write or bash call whose start or end line is oversized.
const oversizedPi = (which: string) => `#!/usr/bin/env node
import fs from "node:fs";
process.stdin.resume(); process.stdin.on("data", () => {});
const [tool, side] = ${JSON.stringify(which)}.split(":");
const emit = (event) => process.stdout.write(JSON.stringify(event) + String.fromCharCode(10));
const huge = "x".repeat(5 * 1024 * 1024);
const args = tool === "write" ? { path: "big.txt", content: side === "start" ? huge : "B" } : { command: side === "start" ? "echo " + huge : "echo small" };
emit({ type: "tool_execution_start", toolCallId: "c1", toolName: tool, args });
if (tool === "write") fs.writeFileSync("big.txt", "B");
if (side === "garbage") process.stdout.write('{"type":"tool_execution_end","toolCallId":"c1"' + String.fromCharCode(10));
else if (side !== "pending") emit({ type: "tool_execution_end", toolCallId: "c1", toolName: tool, isError: false, durationMs: 1,
  result: { content: [{ type: "text", text: side === "end" ? huge : "ok" }] } });
emit({ type: "message_end", message: { role: "assistant", content: "I changed nothing." } });
emit({ type: "agent_settled" });
// Exit only after the 5 Mi-char line is flushed and the worker has had time to read it, as a live Pi would.
process.stdout.write("", () => setTimeout(() => process.exit(0), 1000));
`;

describe("oversized tool event lines (pi-fabric#1034 round 6)", () => {
  it.each(["write:start", "write:end", "bash:start", "bash:end", "bash:pending", "write:garbage"])("a dropped %s line marks the receipt truncated", async (which) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-receipt-big-")));
    roots.push(root);
    const fake = path.join(root, "oversized-pi.mjs");
    fs.writeFileSync(fake, oversizedPi(which), { mode: 0o700 });
    {
      const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000 }, {
        workerPath: path.resolve("src/worker.ts"), piBinary: fake, runRoot: path.join(root, "runs"), fullCodeMode: false,
      });
      managers.push(manager);
      const result = await manager.run({ task: "oversized", runner: "pi", transport: "process", extensions: false });
      // A call left pending, or an unparsable frame, is not an oversized line: only the truncated flag is asserted.
      if (/:(start|end)$/.test(which)) {
        expect(result.warnings?.join("\n") ?? "", JSON.stringify({ status: result.status, error: result.error, text: result.text })).toContain("Dropped an oversized agent event line");
      }
      expect(result.writeReceipt?.truncated).toBe(true);
    }
  }, 60_000);
});

// pi-fabric#1034 round 7 (Astra P1 c6152886488): terminal results the worker does not write itself. A one-off fake Pi
// writes f.txt through a write call (ended, or left pending), then waits until the run times out or is stopped.
const lifecyclePi = (mode: string) => `#!/usr/bin/env node
import fs from "node:fs";
process.stdin.resume(); process.stdin.on("data", () => {});
const mode = ${JSON.stringify(mode)};
const emit = (event) => process.stdout.write(JSON.stringify(event) + String.fromCharCode(10));
if (mode === "garbage") process.stdout.write('{"type":"tool_execution_start","toolCallId":"w1"' + String.fromCharCode(10));
else emit({ type: "tool_execution_start", toolCallId: "w1", toolName: "write", args: { path: "f.txt", content: "F" } });
fs.writeFileSync("f.txt", "F");
if (!mode.endsWith("pending") && mode !== "garbage") emit({ type: "tool_execution_end", toolCallId: "w1", toolName: "write", isError: false, durationMs: 1, result: { content: [{ type: "text", text: "ok" }] } });
setInterval(() => {}, 1000);
`;

describe("terminal paths keep the write receipt (pi-fabric#1034 round 7)", () => {
  const lifecycleManager = (mode: string, timeoutMs = 30_000) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-receipt-end-")));
    roots.push(root);
    const fake = path.join(root, "lifecycle-pi.mjs");
    fs.writeFileSync(fake, lifecyclePi(mode), { mode: 0o700 });
    process.env.FAKE_PI_BEHAVIOR = "unused";
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs }, {
      workerPath: path.resolve("src/worker.ts"), piBinary: fake, runRoot: path.join(root, "runs"), fullCodeMode: false,
    });
    managers.push(manager);
    return { root, manager };
  };

  // Manager-made results (failedRecord): the test SIGKILLs the worker (its pid is the status record's sessionId) once the
  // receipt checkpoint is stored, so the manager sees a lost worker, not a worker-written terminal record.
  const killWorkerAfterCheckpoint = async (root: string, id: string, ready: (receipt: { paths?: string[]; truncated?: boolean } | undefined) => boolean) => {
    const statusFile = path.join(root, "runs", id, "status.json");
    const until = Date.now() + 20_000;
    let pid: number | undefined;
    while (Date.now() < until) {
      try {
        const status = JSON.parse(fs.readFileSync(statusFile, "utf8"));
        // Only a still-running record names a live worker; its pid must also match its recorded /proc birth time.
        if (status.status === "running" && ready(status.writeReceipt)) {
          const candidate = Number(status.sessionId);
          // No recorded birth time (off Linux): never signal; the test fails instead of risking a reused pid.
          if (candidate > 0 && typeof status.processStartTime === "string" && processStartTime(candidate) === status.processStartTime) pid = candidate;
          break;
        }
      } catch { /* not written yet */ }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(pid).toBeGreaterThan(0);
    process.kill(pid!, "SIGKILL");
  };

  it("a lost worker after a completed write: the manager's result keeps the path, marked truncated", async () => {
    const { root, manager } = lifecycleManager("lost", 8_000);
    const handle = await manager.spawn({ task: "write then hang", runner: "pi", transport: "process", extensions: false });
    await killWorkerAfterCheckpoint(root, handle.id, (receipt) => (receipt?.paths?.length ?? 0) > 0);
    const result = await manager.wait(handle.id);
    // With the worker dead, the manager makes the result: lost transport (failed) or its own deadline (timed_out).
    expect(["failed", "timed_out"]).toContain(result.status);
    expect(result.writeReceipt).toEqual({ paths: [path.join(root, "f.txt")], commands: [], truncated: true });
  }, 60_000);

  it("a lost worker with an unended write call: the manager's result is marked truncated", async () => {
    const { root, manager } = lifecycleManager("lost-pending", 8_000);
    const handle = await manager.spawn({ task: "start a write then hang", runner: "pi", transport: "process", extensions: false });
    // f8090829 never stores a receipt for a start, so wait for the start itself (toolCalls) there.
    const statusFile = path.join(root, "runs", handle.id, "status.json");
    await killWorkerAfterCheckpoint(root, handle.id, () => {
      try { return JSON.parse(fs.readFileSync(statusFile, "utf8")).toolCalls > 0; } catch { return false; }
    });
    const result = await manager.wait(handle.id);
    expect(["failed", "timed_out"]).toContain(result.status);
    expect(result.writeReceipt?.truncated).toBe(true);
  }, 60_000);

  it("an unparsable frame is persisted at once: the running checkpoint already shows truncated", async () => {
    const { root, manager } = lifecycleManager("garbage");
    const handle = await manager.spawn({ task: "cut frame then hang", runner: "pi", transport: "process", extensions: false });
    const statusFile = path.join(root, "runs", handle.id, "status.json");
    const until = Date.now() + 20_000;
    let checkpoint: { status?: string; writeReceipt?: { truncated?: boolean } } = {};
    while (Date.now() < until) {
      try { checkpoint = JSON.parse(fs.readFileSync(statusFile, "utf8")); } catch { /* not written yet */ }
      if (checkpoint.writeReceipt?.truncated) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(checkpoint).toMatchObject({ status: "running", writeReceipt: { truncated: true } });
    await manager.stop(handle.id);
  }, 60_000);

  // Worker-written terminal records (regression guards; green on f8090829 too).
  it("a timed-out child after a completed write keeps the path", async () => {
    const { root, manager } = lifecycleManager("timeout", 4_000);
    const result = await manager.run({ task: "write then hang", runner: "pi", transport: "process", extensions: false });
    expect(result.status).toBe("timed_out");
    expect(result.writeReceipt?.paths).toEqual([path.join(root, "f.txt")]);
  }, 60_000);

  it("a timed-out child with an unended write call: the receipt is marked truncated", async () => {
    const { manager } = lifecycleManager("timeout-pending", 4_000);
    const result = await manager.run({ task: "start a write then hang", runner: "pi", transport: "process", extensions: false });
    expect(result.status).toBe("timed_out");
    expect(result.writeReceipt?.truncated).toBe(true);
  }, 60_000);

  it("an explicit stop after a completed write keeps the path", async () => {
    const { root, manager } = lifecycleManager("stop");
    const handle = await manager.spawn({ task: "write then wait", runner: "pi", transport: "process", extensions: false });
    const statusFile = path.join(root, "runs", handle.id, "status.json");
    const until = Date.now() + 20_000;
    const seen = (): boolean => {
      try { return (JSON.parse(fs.readFileSync(statusFile, "utf8")).writeReceipt?.paths?.length ?? 0) > 0; } catch { return false; }
    };
    while (!seen() && Date.now() < until) await new Promise((resolve) => setTimeout(resolve, 100));
    expect(seen()).toBe(true);
    const result = await manager.stop(handle.id);
    expect(result.status).toBe("stopped");
    expect(result.writeReceipt?.paths).toEqual([path.join(root, "f.txt")]);
  }, 60_000);

  it("ENODEV recovery of a running Pi checkpoint marks its receipt truncated; a terminal record keeps its own", async () => {
    const { goneEnodevOutcome } = await import("../src/agents/enodev-recovery.js");
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-receipt-enodev-")));
    roots.push(root);
    const directory = path.join(root, "0123456789abcdef0123456789abcdef");
    fs.mkdirSync(directory);
    const base = { id: path.basename(directory), runner: "pi", transport: "process", writeReceipt: { paths: ["/w/f.txt"], commands: [] } };
    expect(goneEnodevOutcome(directory, { ...base, status: "running" }).writeReceipt)
      .toEqual({ paths: ["/w/f.txt"], commands: [], truncated: true });
    expect(goneEnodevOutcome(directory, { ...base, status: "completed" }).writeReceipt).toEqual({ paths: ["/w/f.txt"], commands: [] });
    expect(goneEnodevOutcome(directory, { ...base, runner: "claude", status: "running" }).writeReceipt).toEqual({ paths: ["/w/f.txt"], commands: [] });
  });

  it("a Pi worker startup failure carries an empty, complete receipt", async () => {
    const { writeWorkerStartupFailure } = await import("../src/worker/startup.js");
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-receipt-startup-")));
    roots.push(root);
    const file = path.join(root, "status.json");
    writeWorkerStartupFailure(["node", "worker.js", "--id", "probe", "--runner", "pi", "--status-file", file], new Error("missing module"));
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ status: "failed", writeReceipt: { paths: [], commands: [] } });
    expect(JSON.parse(fs.readFileSync(file, "utf8")).writeReceipt.truncated).toBeUndefined();
  });

  it("a worker crash keeps the live receipt: an unended write call is published as truncated", async () => {
    const { createRunningRecord, writeCrashRunRecord } = await import("../src/worker/run-record.js");
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-receipt-crash-")));
    roots.push(root);
    const file = path.join(root, "status.json");
    const record = createRunningRecord({
      id: "id", name: "crash", runner: "pi", transport: "process", cwd: root,
      taskFile: "task.txt", statusFile: file, lifecycleFile: "lifecycle.jsonl", logFile: "events.jsonl",
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda", vedaBackend: "", vedaPersona: "",
      timeoutMs: 1000, depth: 0, fullCodeMode: false, extensions: false, tools: [], grantedRisks: [],
    } as never, "task", undefined, 1);
    record.writeReceipt = { paths: [], commands: [] }; // the last stored receipt, before the start
    writeCrashRunRecord(file, record, new Error("crash"), { paths: [], commands: [], truncated: true });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ status: "failed", writeReceipt: { paths: [], commands: [], truncated: true } });
  });
});

describe("agent write receipt (smarty-dev#8758 item 2)", () => {
  it("a spawned child that writes two files and runs one bash command: the envelope lists exactly those", async () => {
    const { root, manager } = managerIn("write-receipt");
    const result = await manager.run({ task: "edit two files", runner: "pi", transport: "process", extensions: false });
    expect(result.status).toBe("completed");
    expect(result.text).toBe("I changed nothing."); // the prose denies it; the receipt does not
    expect(result.writeReceipt).toEqual({ paths: [path.join(root, "notes/a.txt"), path.join(root, "b.txt")], commands: ["echo receipt"] });
    expect(fs.readFileSync(path.join(root, "b.txt"), "utf8")).toBe("B");
  }, 30_000);

  // pi-fabric#1034 Astra P2: every Pi process child's result carries a receipt, empty when nothing was tracked.
  it("a child that writes nothing and runs no shell gets an empty receipt, not a missing one", async () => {
    const { manager } = managerIn("child-contract");
    const result = await manager.run({ task: "report", runner: "pi", transport: "process", extensions: false });
    expect(result.status).toBe("completed");
    expect(result.writeReceipt).toEqual({ paths: [], commands: [] });
  }, 30_000);

  // Round-2 review P2: a failed Pi child's record carries the empty receipt too (it exists from record creation).
  it("a Pi child that fails without a turn still carries the empty receipt", async () => {
    const { manager } = managerIn("exit-error");
    const result = await manager.run({ task: "fail", runner: "pi", transport: "process", extensions: false });
    expect(result.status).not.toBe("completed");
    expect(result.writeReceipt).toEqual({ paths: [], commands: [] });
  }, 30_000);

  // Round-2 review P2: a nested pi.bash or a durable run's end event has no durationMs; a shell call that ended with
  // an error is recorded either way (over-report rather than miss one that ran). A failed write is still not.
  it("records a shell call that ended with an error and no durationMs; never a failed write", async () => {
    const { WriteReceiptRecorder } = await import("../src/agents/write-receipt.js");
    const recorder = new WriteReceiptRecorder("/work");
    recorder.start({ toolCallId: "n1", toolName: "bash", args: { command: "false" } });
    recorder.end({ toolCallId: "n1", isError: true });
    recorder.start({ toolCallId: "w1", toolName: "write", args: { path: "x.txt", content: "X" } });
    recorder.end({ toolCallId: "w1", isError: true });
    expect(recorder.receipt).toEqual({ paths: [], commands: ["false"] });
  });

  // pi-fabric#1034 security P2: starts without ends are bounded at start (count, command text, id and path length);
  // a start that cannot be held is dropped and the receipt is marked truncated.
  it("bounds calls started but never ended: many large starts hold a capped count and cut text, and mark truncated", async () => {
    const { WriteReceiptRecorder, WRITE_RECEIPT_MAX_PENDING, WRITE_RECEIPT_MAX_COMMAND_CHARS } = await import("../src/agents/write-receipt.js");
    const recorder = new WriteReceiptRecorder("/work");
    const big = "y".repeat(1024 * 1024); // 1 Mi chars per command
    for (let n = 0; n < WRITE_RECEIPT_MAX_PENDING * 4; n++) {
      recorder.start({ toolCallId: `s${n}`, toolName: "bash", args: { command: `${n} ${big}` } });
    }
    recorder.start({ toolCallId: "i".repeat(WRITE_RECEIPT_MAX_COMMAND_CHARS + 1), toolName: "bash", args: { command: "x" } });
    recorder.start({ toolCallId: "p", toolName: "write", args: { path: "z".repeat(WRITE_RECEIPT_MAX_COMMAND_CHARS + 1) } });
    // A huge raw path that would normalize short is refused before resolving (never held as a slice of it).
    recorder.start({ toolCallId: "q", toolName: "write", args: { path: `${"./".repeat(WRITE_RECEIPT_MAX_COMMAND_CHARS)}a.txt` } });
    expect(recorder.pending).toBe(WRITE_RECEIPT_MAX_PENDING);
    expect(recorder.receipt.truncated).toBe(true);
    // A held call's end records its text cut to the limit; the dropped ones never appear.
    expect(recorder.end({ toolCallId: "s0", isError: false, durationMs: 1 })).toBe(true);
    expect(recorder.end({ toolCallId: `s${WRITE_RECEIPT_MAX_PENDING * 4 - 1}`, isError: false, durationMs: 1 })).toBe(false);
    expect(recorder.receipt.commands).toEqual([`0 ${big}`.slice(0, WRITE_RECEIPT_MAX_COMMAND_CHARS)]);
    expect(recorder.pending).toBe(WRITE_RECEIPT_MAX_PENDING - 1);
  });

  // pi-fabric#1034: a child that starts more calls than the cap and exits with no end event still leaves a stored
  // receipt marked truncated (the worker copies the receipt at terminal finalization, not only on end events).
  it("a child that starts more calls than the cap and exits without any end: the stored receipt is truncated", async () => {
    const { manager } = managerIn("write-receipt-overflow");
    const result = await manager.run({ task: "overflow", runner: "pi", transport: "process", extensions: false });
    expect(result.writeReceipt).toEqual({ paths: [], commands: [], truncated: true });
  }, 30_000);

  it("a dropped start is reported by the next end, so the stored receipt shows truncated", async () => {
    const { WriteReceiptRecorder, WRITE_RECEIPT_MAX_PENDING } = await import("../src/agents/write-receipt.js");
    const recorder = new WriteReceiptRecorder("/work");
    for (let n = 0; n <= WRITE_RECEIPT_MAX_PENDING; n++) recorder.start({ toolCallId: `s${n}`, toolName: "bash", args: { command: "c" } });
    expect(recorder.receipt.truncated).toBe(true);
    // The dropped call's own end (unknown id) still reports the change once.
    expect(recorder.end({ toolCallId: `s${WRITE_RECEIPT_MAX_PENDING}`, isError: false })).toBe(true);
    expect(recorder.end({ toolCallId: "never-started", isError: false })).toBe(false);
  });

  it("bounds the receipt: a capped number of paths, cut command text, and truncated marked; no contents", async () => {
    // Imported here, so the other cases still run (and fail) on a tree without the module.
    const { WriteReceiptRecorder, WRITE_RECEIPT_MAX_COMMAND_CHARS, WRITE_RECEIPT_MAX_PATHS } = await import("../src/agents/write-receipt.js");
    const recorder = new WriteReceiptRecorder("/work");
    for (let n = 0; n < WRITE_RECEIPT_MAX_PATHS + 5; n++) {
      recorder.start({ toolCallId: `w${n}`, toolName: "write", args: { path: `f${n}.txt`, content: "secret contents" } });
      recorder.end({ toolCallId: `w${n}`, isError: false, durationMs: 1 });
    }
    recorder.start({ toolCallId: "long", toolName: "bash", args: { command: "x".repeat(WRITE_RECEIPT_MAX_COMMAND_CHARS + 10) } });
    recorder.end({ toolCallId: "long", isError: true, durationMs: 3 }); // ran, exited non-zero: still recorded
    const receipt = recorder.receipt;
    expect(receipt.paths).toHaveLength(WRITE_RECEIPT_MAX_PATHS);
    expect(receipt.paths[0]).toBe(path.resolve("/work", "f0.txt")); // D:\work\f0.txt on Windows
    expect(receipt.commands).toEqual(["x".repeat(WRITE_RECEIPT_MAX_COMMAND_CHARS)]);
    expect(receipt.truncated).toBe(true);
    expect(JSON.stringify(receipt)).not.toContain("secret contents");
  });
});
