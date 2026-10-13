// smarty-dev#8758 item 2: a process task agent's final envelope carries a tool-generated receipt of the paths its
// write/edit tools changed and the shell commands it ran, so the spawning lane trusts the receipt, not the prose.
// (Item 3, denyCommands, is split to smarty-dev#11186.)
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

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
