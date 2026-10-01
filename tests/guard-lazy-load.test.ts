import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { foregroundWaitRefusal } from "../src/guards/foreground-wait.js";

const scanner = vi.hoisted(() => ({ loads: vi.fn(), scan: vi.fn() }));
vi.mock("../src/core/pattern-kill.js", async original => {
  scanner.loads();
  const actual = await original<typeof import("../src/core/pattern-kill.js")>();
  scanner.scan.mockImplementation(actual.scanCommand);
  return { ...actual, scanCommand: scanner.scan };
});

afterEach(() => vi.unstubAllEnvs());

describe("bash guard actual-hook lazy boundary", () => {
  it("stays cold through registration/idle/nonbash, then scans once per bash with budget/name/tmp precedence", async () => {
    const tmp = os.tmpdir();
    fs.mkdirSync(tmp, { recursive: true });
    const cwd = fs.mkdtempSync(path.join(tmp, "guard-lazy-"));
    const agentDir = path.join(cwd, "agent");
    fs.mkdirSync(agentDir);
    fs.mkdirSync(path.join(cwd, ".pi"));
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
      prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [],
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST"]) {
      vi.stubEnv(key, undefined);
    }
    type Handler = (event: unknown, context: ExtensionContext) => unknown;
    const handlers = new Map<string, Handler[]>();
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      on: vi.fn((event: string, handler: Handler) => {
        handlers.set(event, [...(handlers.get(event) ?? []), handler]);
      }),
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
      setActiveTools: vi.fn(), sendMessage: vi.fn(),
    } as unknown as ExtensionAPI;
    const context = {
      cwd, hasUI: false, isProjectTrusted: () => true,
      isIdle: () => true, hasPendingMessages: () => false, getContextUsage: () => undefined,
      sessionManager: { getSessionId: () => "guard-lazy-session", getBranch: () => [], getEntries: () => [] },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const emit = async (name: string, event: unknown = {}) => {
      expect(handlers.get(name)?.length).toBeGreaterThan(0);
      const results: unknown[] = [];
      for (const handler of handlers.get(name) ?? []) results.push(await handler(event, context));
      return results.filter(result => result !== undefined);
    };
    let registered = false;
    try {
      vi.resetModules();
      scanner.loads.mockClear();
      scanner.scan.mockClear();
      const { default: register } = await import("../src/index.js");
      expect(scanner.loads).not.toHaveBeenCalled();
      await register(pi);
      registered = true;
      expect(pi.registerTool).toHaveBeenCalledWith(expect.objectContaining({ name: "fabric_exec" }));
      expect(scanner.loads).not.toHaveBeenCalled();
      for (const event of ["resources_discover", "session_start", "agent_settled"]) {
        await emit(event);
        expect(scanner.loads).not.toHaveBeenCalled();
      }
      // Let deferred registration/idle work run; do not merely inspect import declarations.
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(scanner.loads).not.toHaveBeenCalled();
      for (const [toolName, input] of [["read", { path: "README.md" }], ["bash", { command: 42 }]] as const) {
        expect(await emit("tool_call", { toolName, toolCallId: "non-command", input })).toEqual([]);
        expect(scanner.loads).not.toHaveBeenCalled();
        expect(scanner.scan).not.toHaveBeenCalled();
      }

      // Commands below are scanner DATA, never executed. The first actual bash is unsafe.
      const firstResult = await emit("tool_call", { toolName: "bash", toolCallId: "first", input: { command: "pkill worker" } });
      expect(scanner.loads).toHaveBeenCalledOnce();
      expect(scanner.scan).toHaveBeenCalledExactlyOnceWith("pkill worker");
      const { GUARD_BUDGET_REASON, PATTERN_KILL_REASON, TMP_WIPE_REASON } = await import("../src/core/pattern-kill.js");
      expect(firstResult).toEqual([{ block: true, reason: PATTERN_KILL_REASON }]);
      const cases: Array<[string, string | undefined, number?]> = [
        ["sleep 999", foregroundWaitRefusal("sleep 999")],
        ["sleep 999", undefined, 20],
        ["kill 4242; rm -f .local/own-file", undefined],
        ["kill $(cat .local/server.pid); rm -rf /tmp/tmp.AbC123", undefined],
        ["pkill worker; rm -rf /tmp/*; sleep 999", PATTERN_KILL_REASON],
        ["rm -rf /tmp/*; sleep 999", TMP_WIPE_REASON],
        [`A=${"x".repeat(4096)}; B=${"$A".repeat(1024)}; pkill worker; rm -rf /tmp/*`, GUARD_BUDGET_REASON],
      ];
      for (const [command, reason, timeout] of cases) {
        const before = scanner.scan.mock.calls.length;
        expect(await emit("tool_call", { toolName: "bash", toolCallId: `bash-${before}`, input: { command, timeout } }))
          .toEqual(reason === undefined ? [] : [{ block: true, reason }]);
        expect(scanner.scan).toHaveBeenCalledTimes(before + 1);
        expect(scanner.scan).toHaveBeenLastCalledWith(command);
        expect(scanner.loads).toHaveBeenCalledOnce();
      }
      expect(scanner.scan.mock.results[0]?.value).toEqual({ blocked: true, wipe: false, exhausted: false });
      vi.stubEnv("PI_FABRIC_ACTOR_ID", "guard-test-actor");
      vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", undefined);
      const allowedInput: { command: string; timeout?: number } = { command: "echo marker" };
      expect(await emit("tool_call", { toolName: "bash", toolCallId: "actor-allowed", input: allowedInput })).toEqual([]);
      expect(allowedInput.timeout).toBe(600);
      const waitInput: { command: string; timeout?: number } = { command: "sleep 999" };
      expect(await emit("tool_call", { toolName: "bash", toolCallId: "actor-wait", input: waitInput })).toEqual([{ block: true, reason: foregroundWaitRefusal("sleep 999") }]);
      expect(waitInput.timeout).toBeUndefined();
    } finally {
      if (registered) await emit("session_shutdown");
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });
});
