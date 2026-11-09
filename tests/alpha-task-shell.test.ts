import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import type { FabricWritePolicy } from "../src/agents/write-guard.js";

// Trust-boundary regression: isolate only the helper's home/file probes, never
// read or alter the real user's disable marker. All other filesystem work is real.
const alpha = vi.hoisted(() => ({ home: "", disabled: false }));
vi.mock("node:os", async original => ({
  ...await original<typeof import("node:os")>(), homedir: () => alpha.home,
}));
vi.mock("node:fs", async original => {
  const actual = await original<typeof import("node:fs")>();
  const { join } = await import("node:path");
  return { ...actual, existsSync: (file: fs.PathLike) =>
    file === join(alpha.home, ".config/smarty/bash-guard.disabled") ? alpha.disabled : actual.existsSync(file) };
});

let root: string;
let cwd: string;
const managers: AgentManager[] = [];
beforeEach(() => {
  vi.resetModules();
  root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "alpha-task-shell-")));
  cwd = path.join(root, "work");
  alpha.home = path.join(root, "home");
  alpha.disabled = false;
  fs.mkdirSync(cwd);
  fs.mkdirSync(alpha.home);
  vi.stubEnv("SMARTY_ALPHA", "0"); // The helper defaults ON; legacy expectations must opt out.
  vi.stubEnv("PI_FABRIC_WRITE_POLICY", undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(alpha.home, "profile"));
});
afterEach(async () => {
  try {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    // Only this test's private temp fixture, never a home or checkout.
    fs.rmSync(root, { recursive: true, force: true });
  }
});
const warningSpy = async () => {
  const { diagnostics } = await import("../src/core/agent-dir.js");
  return vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
};
const parentPolicy = (): FabricWritePolicy => ({ readOnly: false, writableRoots: [cwd], shell: "deny" });

describe("alpha task shell confinement", () => {
  it("rechecks the live mode on the same registered hook and warns once before permitting denied shells", async () => {
    const warn = await warningSpy();
    const { default: register, readWritePolicy, writePolicyDenial } = await import("../src/agents/write-guard.js");
    const hooks: Array<(event: { toolName: string; input: unknown }, context: { cwd: string }) => unknown> = [];
    for (const raw of [JSON.stringify(parentPolicy()), JSON.stringify({ readOnly: true, writableRoots: [], shell: "deny" }), "{malformed"]) {
      vi.stubEnv("PI_FABRIC_WRITE_POLICY", raw);
      expect(readWritePolicy()).toBeDefined();
      register({ on: (name: string, hook: typeof hooks[number]) => {
        expect(name).toBe("tool_call");
        hooks.push(hook);
      } } as unknown as ExtensionAPI);
    }
    const check = (enabled: boolean) => {
      for (const hook of hooks) for (const toolName of ["bash", "powershell"]) {
        const result = hook({ toolName, input: { command: "echo harmless" } }, { cwd });
        expect(result).toEqual(enabled ? undefined : { block: true,
          reason: `Fabric write policy refuses ${toolName}: shell commands cannot be confined; the parent must request shell: "unconfined"` });
        if (enabled) expect(warn).toHaveBeenCalledTimes(1);
      }
    };
    check(false);
    vi.stubEnv("SMARTY_ALPHA", undefined);
    expect(writePolicyDenial({ ...parentPolicy(), shell: "unconfined" }, "bash", {}, cwd)).toBeUndefined();
    expect(writePolicyDenial(parentPolicy(), "read", {}, cwd)).toBeUndefined();
    expect(warn).not.toHaveBeenCalled(); // No warning unless an actual refusal is bypassed.
    check(true);
    expect(warn).toHaveBeenCalledWith("src/agents/write-guard.ts", expect.stringContaining("FABRIC_ALPHA_SHELL_CONFINEMENT_BYPASS"));
    for (const value of ["1", "", "true", "00"]) {
      vi.stubEnv("SMARTY_ALPHA", value);
      check(true);
    }
    vi.stubEnv("SMARTY_ALPHA", "0");
    check(false);
    alpha.disabled = true;
    check(true);
    alpha.disabled = false;
    check(false);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("bypasses only inherited shell escalation, with one named warning and live rechecks", async () => {
    const warn = await warningSpy();
    const { checkWritePolicyRequest, resolveChildWritePolicy } = await import("../src/agents/child-env.js");
    const parent = parentPolicy();
    const request = { shell: "unconfined" as const };
    expect(resolveChildWritePolicy({}, parent, cwd)).toBe(parent);
    expect(() => resolveChildWritePolicy(request, parent, cwd)).toThrow('A shell-confined agent cannot launch a child with shell: "unconfined"');
    expect(warn).not.toHaveBeenCalled();
    const allowed = () => {
      expect(resolveChildWritePolicy(request, parent, cwd)).toEqual({ ...parent, shell: "unconfined" });
      expect(parent.shell).toBe("deny");
      expect(warn).toHaveBeenCalledTimes(1);
    };
    vi.stubEnv("SMARTY_ALPHA", undefined);
    allowed();
    allowed();
    expect(warn).toHaveBeenCalledWith("src/agents/child-env.ts", expect.stringContaining("FABRIC_ALPHA_SHELL_INHERITANCE_BYPASS"));
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(() => resolveChildWritePolicy(request, parent, cwd)).toThrow("shell-confined");
    alpha.disabled = true;
    allowed();
    alpha.disabled = false;
    expect(() => resolveChildWritePolicy(request, parent, cwd)).toThrow("shell-confined");
    vi.stubEnv("SMARTY_ALPHA", "1");
    allowed();
    // Alpha does not change option validation or either of the other inheritance checks.
    for (const invalid of [{ shell: "invalid" }, { shell: true }, { readOnly: "yes" }, { writableRoots: [42] },
      { writePolicy: "unrestricted", shell: "unconfined" }]) {
      expect(() => checkWritePolicyRequest(invalid as unknown as AgentRunRequest)).toThrow();
    }
    expect(() => resolveChildWritePolicy({ ...request, readOnly: false }, { ...parent, readOnly: true }, cwd)).toThrow("read-only agent");
    expect(resolveChildWritePolicy(request, { ...parent, readOnly: true, writableRoots: [] }, cwd))
      .toEqual({ readOnly: true, writableRoots: [], shell: "unconfined" });
    const outside = path.join(root, "outside");
    fs.mkdirSync(outside);
    expect(() => resolveChildWritePolicy({ ...request, writableRoots: [outside] }, parent, cwd)).toThrow("outside the caller's writable roots");
    expect(warn).toHaveBeenCalledTimes(1);
  });
});

const recordingManager = async (executorRuntime: string) => {
  const { AgentManager } = await import("../src/agents/manager.js");
  const { ProcessTransport } = await import("../src/agents/transports/process-transport.js");
  const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
  const { writeJsonAtomic } = await import("../src/core/atomic-write.js");
  const launches: Array<{ request: AgentTransportLaunch; args: Map<string, string> }> = [];
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const args = new Map<string, string>();
    for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
    launches.push({ request, args });
    const now = Date.now();
    const record: AgentRunResult = { id: request.id, name: request.name, task: fs.readFileSync(args.get("--task-file")!, "utf8"),
      runner: args.get("--runner")!, transport: "process", cwd: request.cwd, status: "completed", startedAt: now, updatedAt: now,
      finishedAt: now, turns: 1, toolCalls: 0, text: "recorded shell admission", sessionId: "2147483647",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
    writeJsonAtomic(args.get("--status-file")!, record);
    return { kind: "process", sessionId: "2147483647", closed: Promise.resolve(), waitForClose: async () => {},
      isAlive: async () => false, stop: async () => {} } satisfies AgentTransportHandle;
  });
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true,
    sessionExport: false, timeoutMs: 10_000 }, { runRoot: path.join(root, "runs"),
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false, executorRuntime: () => executorRuntime });
  managers.push(manager);
  return { manager, launches };
};

describe("alpha shell-based native executor admission", () => {
  it.each([
    { runtime: "node-process", kernel: "typescript" as const },
    { runtime: "bun-process", kernel: "typescript" as const },
    { runtime: "quickjs", kernel: "python" as const, pythonRuntime: "cpython" as const },
  ])("rechecks $runtime/$kernel on the same manager and warns once without rewriting inherited policy", async ({ runtime, kernel, ...options }) => {
    const warn = await warningSpy();
    const policy: FabricWritePolicy = { readOnly: true, writableRoots: [], shell: "deny" };
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify(policy));
    const { manager, launches } = await recordingManager(runtime);
    const request = { task: "shell-based executor admission", kernel, ...options, transport: "process" as const };
    await expect(manager.run(request)).rejects.toThrow("A confined agent cannot use a native Fabric executor");
    expect(launches).toHaveLength(0);
    expect(warn).not.toHaveBeenCalled();
    const allowed = async (readOnly?: boolean) => {
      const result = await manager.run({ ...request, ...(readOnly === undefined ? {} : { readOnly }) });
      expect(result.status).toBe("completed");
      expect(JSON.parse(launches.at(-1)!.args.get("--write-policy")!)).toEqual(policy);
      expect(warn).toHaveBeenCalledTimes(1);
    };
    vi.stubEnv("SMARTY_ALPHA", undefined);
    await allowed();
    await allowed(true);
    expect(warn).toHaveBeenCalledWith("src/agents/manager.ts", expect.stringContaining("FABRIC_ALPHA_NATIVE_EXECUTOR_ADMISSION_BYPASS"));
    const events = fs.readFileSync(launches[0]!.args.get("--log-file")!, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events.filter(event => event.type === "write_policy.alpha_native_unconfined")).toEqual([
      expect.objectContaining({ policy, warning: expect.stringContaining("native write/edit and shell") }),
    ]);
    vi.stubEnv("SMARTY_ALPHA", "0");
    await expect(manager.run(request)).rejects.toThrow("native Fabric executor");
    alpha.disabled = true;
    await allowed();
    alpha.disabled = false;
    await expect(manager.run(request)).rejects.toThrow("native Fabric executor");
    vi.stubEnv("SMARTY_ALPHA", "1");
    await allowed();
    const count = launches.length;
    for (const mode of ["0", "1"]) {
      vi.stubEnv("SMARTY_ALPHA", mode);
      await expect(manager.run({ ...request, shell: "invalid" as "deny" })).rejects.toThrow('shell must be "deny" or "unconfined"');
      await expect(manager.run({ ...request, readOnly: "yes" as unknown as boolean })).rejects.toThrow("readOnly must be a boolean");
      await expect(manager.run({ ...request, writePolicy: "unrestricted", shell: "unconfined" })).rejects.toThrow("cannot be combined");
    }
    expect(launches).toHaveLength(count);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it.each(["0", "1"])("does not warn for non-bypassed admission with SMARTY_ALPHA=%s", async mode => {
    const warn = await warningSpy();
    vi.stubEnv("SMARTY_ALPHA", mode);
    const { manager } = await recordingManager("node-process");
    expect((await manager.run({ task: "explicit unconfined", kernel: "typescript", readOnly: true, shell: "unconfined" })).status).toBe("completed");
    expect((await manager.run({ task: "non-native", kernel: "python", pythonRuntime: "monty", readOnly: true })).status).toBe("completed");
    expect(warn).not.toHaveBeenCalled();
  });
});
