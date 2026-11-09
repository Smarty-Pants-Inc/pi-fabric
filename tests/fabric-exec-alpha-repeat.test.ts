import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import type { FabricState } from "../src/fabric-state.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";

// Isolate only alpha's configuration probe; keep the real runtime's fs access.
const fixture = vi.hoisted(() => ({ home: "/fixture-home" }));
vi.mock("node:os", async original => ({
  ...await original<typeof import("node:os")>(), homedir: () => fixture.home,
}));
vi.mock("node:fs", async original => {
  const actual = await original<typeof import("node:fs")>();
  return {
    ...actual,
    existsSync: (path: Parameters<typeof actual.existsSync>[0]) =>
      path === join(fixture.home, ".config/smarty/bash-guard.disabled") ? false : actual.existsSync(path),
  };
});
afterEach(() => vi.unstubAllEnvs());

// Trust boundary: real fabric_exec -> type checker -> QuickJS, with no shell,
// providers, inference, process kills, or deletion commands executed.
describe("fabric_exec alpha repeat boundary", () => {
  it("executes seven identical programs with warn-only output, then honors a live opt-out and reset", async () => {
    vi.stubEnv("SMARTY_ALPHA", undefined);
    const config = normalizeFabricConfig({
      fullCodeMode: false,
      executor: { kernel: "typescript", runtime: "quickjs" },
      jev: { enabled: false },
    });
    const registry = new ActionRegistry();
    const state = {
      bootstrapped: true, config,
      execution: new FabricExecutionService(registry, config),
      ensure: async () => {},
      claimHandoff: async () => undefined,
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const context = {
      cwd: process.cwd(), hasUI: false,
      sessionManager: { getSessionId: () => "alpha-repeat-boundary" },
    };
    const run = (count: number, code = "return π.body") => tool.execute(
      `repeat-${count}`, { code, payloads: { body: `execution-${count}` } },
      undefined, undefined, context as never,
    );
    const text = (result: Awaited<ReturnType<typeof run>>) =>
      result.content.filter(part => part.type === "text").map(part => part.text).join("\n");

    try {
      for (let count = 1; count <= 7; count++) {
        const result = await run(count);
        const output = text(result);
        expect(result.isError, output).not.toBe(true);
        expect(result.details).toMatchObject({ success: true, trace: { outcome: "succeeded" } });
        // Distinct literal payload results prove every identical program ran.
        expect(output.split("\n")[0]).toBe(`execution-${count}`);
        if (count >= 3) {
          expect(output).toContain(`identical fabric_exec code has now run ${count} times in a row`);
          expect(output).toContain("warn-only under alpha mode");
          expect(output).not.toContain("will be blocked");
        } else {
          expect(output).not.toContain("[circuit-breaker]");
        }
      }

      vi.stubEnv("SMARTY_ALPHA", "0");
      const blocked = await run(8);
      expect(blocked.isError).toBe(true);
      expect(text(blocked)).toContain("executed 8 times in a row and was blocked");
      expect(text(blocked)).not.toContain("execution-8");
      expect(blocked.details).toBeUndefined();

      const reset = await run(9, 'return "different-code-executed"');
      expect(reset.isError, text(reset)).not.toBe(true);
      expect(text(reset)).toBe("different-code-executed");
    } finally {
      await registry.close();
    }
  });
});
