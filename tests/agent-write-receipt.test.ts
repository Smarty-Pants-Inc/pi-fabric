// smarty-dev#8758: (2) a process task agent's final envelope carries a tool-generated receipt of the paths its
// write/edit tools changed and the shell commands it ran, so the spawning lane trusts the receipt, not the prose;
// (3) agents.spawn carries a denyCommands list to the child as PI_FABRIC_DENY_COMMANDS, which a confined caller can
// neither drop nor shorten (enforcement is the bash guard's, smarty-dev#2754).
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const saved = { behavior: process.env.FAKE_PI_BEHAVIOR, deny: process.env.PI_FABRIC_DENY_COMMANDS };
afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  if (saved.behavior === undefined) delete process.env.FAKE_PI_BEHAVIOR; else process.env.FAKE_PI_BEHAVIOR = saved.behavior;
  if (saved.deny === undefined) delete process.env.PI_FABRIC_DENY_COMMANDS; else process.env.PI_FABRIC_DENY_COMMANDS = saved.deny;
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
    expect(receipt.paths[0]).toBe("/work/f0.txt");
    expect(receipt.commands).toEqual(["x".repeat(WRITE_RECEIPT_MAX_COMMAND_CHARS)]);
    expect(receipt.truncated).toBe(true);
    expect(JSON.stringify(receipt)).not.toContain("secret contents");
  });
});

describe("denyCommands carried to the child (smarty-dev#8758 item 3)", () => {
  const report = (text: string) => JSON.parse(text) as { denyCommands: string | null };
  it("a child spawned with denyCommands sees PI_FABRIC_DENY_COMMANDS; one spawned without it sees none", async () => {
    const { manager } = managerIn("child-contract");
    const denied = await manager.run({ task: "report", runner: "pi", transport: "process", extensions: false, denyCommands: ["sed -i", "git"] });
    expect(denied.status).toBe("completed");
    expect(JSON.parse(report(denied.text).denyCommands!)).toEqual(["git", "sed -i"]);
    const plain = await manager.run({ task: "report", runner: "pi", transport: "process", extensions: false });
    expect(report(plain.text).denyCommands).toBeNull();
  }, 30_000);

  it("a confined caller cannot drop or shorten its list; omitting it inherits, and adding to it is allowed", async () => {
    process.env.PI_FABRIC_DENY_COMMANDS = JSON.stringify(["git"]);
    const { manager } = managerIn("child-contract");
    await expect(manager.run({ task: "report", runner: "pi", transport: "process", extensions: false, denyCommands: [] }))
      .rejects.toThrow("cannot launch a child without its denied commands: git");
    await expect(manager.run({ task: "report", runner: "pi", transport: "process", extensions: false, denyCommands: ["rm"] }))
      .rejects.toThrow("cannot launch a child without its denied commands: git");
    const inherited = await manager.run({ task: "report", runner: "pi", transport: "process", extensions: false });
    expect(JSON.parse(report(inherited.text).denyCommands!)).toEqual(["git"]);
    const widened = await manager.run({ task: "report", runner: "pi", transport: "process", extensions: false, denyCommands: ["git", "rm"] });
    expect(JSON.parse(report(widened.text).denyCommands!)).toEqual(["git", "rm"]);
  }, 30_000);

  it("refuses a malformed list before launch", async () => {
    const { manager } = managerIn("child-contract");
    await expect(manager.run({ task: "report", runner: "pi", transport: "process", denyCommands: [""] })).rejects.toThrow("denyCommands must be");
    await expect(manager.run({ task: "report", runner: "pi", transport: "process", denyCommands: Array.from({ length: 33 }, (_, n) => `c${n}`) }))
      .rejects.toThrow("denyCommands must be");
  });
});
