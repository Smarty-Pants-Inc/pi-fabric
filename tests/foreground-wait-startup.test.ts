import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
const patternLoads = vi.hoisted(() => vi.fn());
const policyLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/guards/foreground-wait.js", async original => {
  loads(); return original<typeof import("../src/guards/foreground-wait.js")>();
});
vi.mock("../src/core/pattern-kill.js", async original => {
  patternLoads(); return original<typeof import("../src/core/pattern-kill.js")>();
});
vi.mock("../src/guards/bash-guard-policy.js", async original => {
  policyLoads(); return original<typeof import("../src/guards/bash-guard-policy.js")>();
});

describe("foreground shell parser startup", () => {
  it("keeps full parsers cold through default bash, then loads them on the same handler's full-policy call", async () => {
    vi.resetModules(); loads.mockClear(); patternLoads.mockClear(); policyLoads.mockClear();
    const home = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bash-policy-startup-"));
    vi.stubEnv("HOME", home);
    vi.stubEnv("USERPROFILE", home);
    vi.stubEnv("SMARTY_ALPHA", undefined);
    vi.stubEnv("PI_FABRIC_FIXTURE", undefined);
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_BASH_IDLE_S", undefined);
    const expectCold = () => {
      expect(loads).not.toHaveBeenCalled();
      expect(patternLoads).not.toHaveBeenCalled();
      expect(policyLoads).not.toHaveBeenCalled();
    };
    const handlers = new Map<string, Array<(...args: any[]) => unknown>>();
    const pi = { events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      getActiveTools: () => [], getAllTools: () => [], setActiveTools: vi.fn(),
      on: (name: string, handler: (...args: any[]) => unknown) => handlers.set(name, [...handlers.get(name) ?? [], handler]),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    } as unknown as ExtensionAPI;
    try {
      const { default: fabric } = await import("../src/index.js");
      expectCold();
      await fabric(pi); await new Promise(resolve => setTimeout(resolve, 0));
      expectCold();
      const call = async (toolName: string, input: object) => {
        const results: any[] = [];
        for (const handler of handlers.get("tool_call") ?? []) {
          results.push(await handler({ type: "tool_call", toolName, toolCallId: "cold", input }, { cwd: process.cwd(), hasUI: false }));
        }
        return results.find(result => result?.block);
      };
      expect(await call("read", { path: "unused" })).toBeUndefined(); expectCold();
      expect(await call("bash", { command: "sleep 900" })).toBeUndefined();
      expect(policyLoads).toHaveBeenCalledTimes(1);
      expect(loads).not.toHaveBeenCalled();
      expect(patternLoads).not.toHaveBeenCalled();
      vi.stubEnv("SMARTY_ALPHA", "0");
      expect(await call("bash", { command: "sleep 900" })).toMatchObject({ block: true, reason: expect.stringContaining("foreground wait") });
      expect(loads).toHaveBeenCalledTimes(1);
      expect(patternLoads).toHaveBeenCalledTimes(1);
      expect(await call("bash", { command: "sleep 1", timeout: 5 })).toBeUndefined();
      vi.stubEnv("SMARTY_ALPHA", undefined);
      expect(await call("bash", { command: "sleep 900" })).toBeUndefined();
      expect(loads).toHaveBeenCalledTimes(1);
      expect(patternLoads).toHaveBeenCalledTimes(1);
      expect(policyLoads).toHaveBeenCalledTimes(1);
    } finally {
      try { for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown(); }
      finally {
        vi.unstubAllEnvs();
        fs.rmSync(home, { recursive: true, force: true });
      }
    }
  });
});
