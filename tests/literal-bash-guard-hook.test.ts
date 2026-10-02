import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";

const loaded = vi.hoisted(() => vi.fn());
const calls = vi.hoisted(() => vi.fn());
vi.mock("../src/core/literal-bash-guard.js", async original => {
  loaded();
  const module = await original<typeof import("../src/core/literal-bash-guard.js")>();
  return { ...module, bashGuardRefusal: (...args: Parameters<typeof module.bashGuardRefusal>) => {
    calls(...args);
    return module.bashGuardRefusal(...args);
  } };
});
afterEach(() => vi.unstubAllEnvs());

it("awaits the literal guard only at first bash use and reads host TMPDIR at each call", async () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-literal-hook-"));
  fs.mkdirSync(path.join(cwd, "agent"));
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
    prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [],
  }));
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST"]) vi.stubEnv(key, undefined);
  type Event = { toolName?: string; input?: { command?: unknown; timeout?: number } };
  type Handler = (event: Event, context: ExtensionContext) => unknown;
  const handlers = new Map<string, Handler[]>();
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on: vi.fn((event: string, handler: Handler) => handlers.set(event, [...(handlers.get(event) ?? []), handler])),
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), registerCommand: vi.fn(),
    registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(), sendMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => true,
    sessionManager: { getSessionId: () => "literal-hook", getBranch: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const lifecycle = async (name: string) => {
    for (const handler of handlers.get(name) ?? []) await handler({}, context);
  };
  try {
    vi.resetModules();
    const { default: register } = await import("../src/index.js");
    await register(pi);
    expect(loaded).not.toHaveBeenCalled();
    for (const name of ["resources_discover", "session_start"]) {
      await lifecycle(name);
      expect(loaded).not.toHaveBeenCalled();
    }
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(loaded).not.toHaveBeenCalled();
    // Select the native bash guard, not the separately registered tool-lifecycle handler.
    const nativeGuard = handlers.get("tool_call")!.find(handler => String(handler).includes("guardReason"))!;
    expect(nativeGuard).toBeTypeOf("function");
    expect(await nativeGuard({ toolName: "read", input: { command: "pkill worker" } }, context)).toBeUndefined();
    expect(await nativeGuard({ toolName: "bash", input: {} }, context)).toBeUndefined();
    expect(loaded).not.toHaveBeenCalled();
    vi.stubEnv("TMPDIR", "/tmp/session-literal-guard");
    const unsafe = { toolName: "bash", input: { command: "kill $P", timeout: 123 } };
    const result = await nativeGuard(unsafe, context);
    expect(result).toEqual({ block: true, reason: "Signal refused: use the PID you recorded (literal integer PIDs only)." });
    expect(unsafe.input.timeout).toBe(123);
    expect(loaded).toHaveBeenCalledOnce();
    expect(calls).toHaveBeenCalledExactlyOnceWith("kill $P", "/tmp/session-literal-guard");
    expect(await nativeGuard({ toolName: "bash", input: { command: "kill 4242" } }, context)).toBeUndefined();
    const deletion = { toolName: "bash", input: { command: "rm -rf /tmp/session-literal-guard/a" } };
    expect(await nativeGuard(deletion, context)).toBeUndefined();
    vi.stubEnv("TMPDIR", "/tmp/another-session");
    expect(await nativeGuard(deletion, context)).toEqual({ block: true, reason: "Recursive delete refused: delete only inside your own TMPDIR using literal absolute paths." });
    expect(loaded).toHaveBeenCalledOnce();
    expect(calls).toHaveBeenCalledTimes(4);
    for (const root of [undefined, "/tmp", "/var/tmp"]) {
      vi.stubEnv("TMPDIR", root);
      expect(await nativeGuard(deletion, context)).toEqual({ block: true, reason: "Recursive delete refused: delete only inside your own TMPDIR using literal absolute paths." });
    }
    for (const command of [
      `N=/path/ORG-NOTES.md; echo "$(date -u +%H:%MZ) 'restored' 'restarted' 'Sessions'" >> "$N"; echo ok`,
      'S=a; T=b; echo "$S $T"', "python3 - <<'EOF2'\nprint('Sessions')\nEOF2",
      "printf '%s\\n' x | ssh host 'cat'", "A=1; echo $A",
      "ls /some/dir ; find /some/dir -maxdepth 4 -name 'relaunch*.sh'",
      "rg foo /x | rg bar ; ls /y",
      "printf 'a → b — c\\n' | cat", 'echo "status → done — ok"',
    ]) {
      const ordinary = { toolName: "bash", input: { command, timeout: 123 } };
      expect(await nativeGuard(ordinary, context), command).toBeUndefined();
      expect(ordinary.input.timeout).toBe(123);
    }
    const opaque = { toolName: "bash", input: { command: 'echo "$(r\\m -rf /x)"', timeout: 123 } };
    const opaqueResult = await nativeGuard(opaque, context);
    expect(opaqueResult).toMatchObject({ block: true, reason: expect.stringMatching(/^Opaque command refused:/) });
    expect(opaqueResult).not.toHaveProperty("reason", "Signal refused: use the PID you recorded (literal integer PIDs only).");
    expect(opaque.input.timeout).toBe(123);
    for (const command of ["find /x -delete", "find /x -exec rm -rf {} +", "find / -name '*.lock' -delete"]) {
      const destructive = { toolName: "bash", input: { command, timeout: 123 } };
      expect(await nativeGuard(destructive, context)).toEqual({ block: true, reason: "Recursive delete refused: delete only inside your own TMPDIR using literal absolute paths." });
      expect(destructive.input.timeout).toBe(123);
    }
    expect(calls).toHaveBeenCalledTimes(20);
    expect(loaded).toHaveBeenCalledOnce();
    expect(await nativeGuard({ toolName: "bash", input: { command: "sleep 600", timeout: 600 } }, context)).toHaveProperty("block", true);
  } finally {
    await lifecycle("session_shutdown");
    fs.rmSync(cwd, { recursive: true, force: true });
  }
});
