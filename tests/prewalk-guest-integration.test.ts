import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { FabricActivityStore } from "../src/activity/store.js";
import { PrewalkController } from "../src/prewalk/controller.js";
import { PrewalkProvider } from "../src/providers/prewalk-provider.js";
import { guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";
import { availablePythonBackends } from "./fixtures/python-backends.js";
import { rmTempSync } from "./fixtures/temp-cleanup.js";

const plan = {
  outcome: "Deliver the direct API",
  steps: ["Expose the provider"],
  verification: ["Check direct dispatch"],
  risks: "Keep host validation",
};
const hasBun = spawnSync("bun", ["--version"], { stdio: "ignore" }).status === 0;
const cases = [
  { name: "QuickJS", executor: { kernel: "typescript", runtime: "quickjs" }, available: true },
  { name: "Node", executor: { kernel: "typescript", runtime: "node-process" }, available: true },
  { name: "Bun", executor: { kernel: "typescript", runtime: "bun-process" }, available: hasBun },
  { name: "Monty", executor: { kernel: "python", pythonRuntime: "monty" }, available: availablePythonBackends.monty },
  { name: "CPython", executor: { kernel: "python", pythonRuntime: "cpython" }, available: availablePythonBackends.cpython },
];

describe("Prewalk direct guest API", () => {
  it("types plan input, readiness and runtime identity, including unavailable globals", () => {
    const declarations = guestTypeDeclarations(true);
    const valid = typeCheckFabricCode(`
const result = await prewalk.plan(${JSON.stringify(plan)});
const recorded: true = result.recorded;
const ready: "ready" = result.readiness;
const text: string = result.plan;
const status = await prewalk.status();
const required: boolean = status.planRequired;
const claimed: "planned" | "disabled" | "unplanned" | null = status.claimedReadiness;
const hash: string | undefined = status.runtime?.entry?.loadedSha256;
return { recorded, ready, text, required, claimed, hash };
`, declarations, true);
    expect(valid.errors).toEqual([]);
    expect(typeCheckFabricCode("return prewalk.plan({ outcome: 'missing fields' });", declarations, true).errors.length).toBeGreaterThan(0);
    expect(typeCheckFabricCode("return prewalk.status({ extra: true });", declarations, true).errors.length).toBeGreaterThan(0);
    expect(typeCheckFabricCode("return prewalk.unknown();", declarations, true).errors.length).toBeGreaterThan(0);
    expect(typeCheckFabricCode("return prewalk.status();", guestTypeDeclarations(true, { excludeGlobals: ["prewalk"] }), true).errors.length).toBeGreaterThan(0);
  });

  for (const { name, executor, available } of cases) {
    it.skipIf(!available)(`records and validates a plan through ${name} host dispatch`, async () => {
      const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prewalk-guest-"));
      const registry = new ActionRegistry();
      try {
        const controller = new PrewalkController();
        controller.arm({ model: "example/executor", sessionId: "prewalk-guest", requirePlan: true });
        registry.register(new PrewalkProvider(controller));
        const config = normalizeFabricConfig({
          executor: { ...executor, memoryLimitBytes: 256 * 1024 * 1024 },
          approvals: { read: "allow", write: "allow" },
        });
        const service = new FabricExecutionService(registry, config, new FabricActivityStore());
        let sequence = 0;
        const run = (code: string) => service.execute({
          code, parentToolCallId: `prewalk-${++sequence}`, signal: undefined,
          context: { cwd, hasUI: false, sessionManager: {
            getSessionId: () => "prewalk-guest", getSessionFile: () => undefined,
          } } as unknown as ExtensionContext,
          onPartial() {},
        });
        const python = executor.kernel === "python";
        const result = await run(python
          ? `before = await prewalk.status()\nrecorded = await prewalk.plan(${JSON.stringify(plan)})\nafter = await prewalk.status()\nreturn {"before": before, "recorded": recorded, "after": after}`
          : `const before = await prewalk.status(); const recorded = await prewalk.plan(${JSON.stringify(plan)}); const after = await prewalk.status(); return { before, recorded, after };`);
        expect(result.success, result.error).toBe(true);
        expect(result.value).toMatchObject({
          before: { state: "armed", planRequired: true, planReady: false },
          recorded: { recorded: true, readiness: "ready", plan: expect.stringContaining(plan.outcome) },
          after: { state: "armed", planRequired: false, planReady: true, claimedReadiness: null },
        });
        expect(result.audits.map((audit) => audit.ref)).toEqual(["prewalk.status", "prewalk.plan", "prewalk.status"]);
        expect(controller.planReady("prewalk-guest")).toBe(true);
        const duplicate = await run(`return await prewalk.plan(${JSON.stringify(plan)})`);
        expect(duplicate.success).toBe(false);
        expect(duplicate.error).toContain("not awaiting a plan");
        controller.arm({ model: "example/executor", sessionId: "prewalk-guest", requirePlan: true });
        const invalid = await run(`return await prewalk.plan(${JSON.stringify({ ...plan, steps: [] })})`);
        expect(invalid.success).toBe(false);
        expect(controller.planReady("prewalk-guest")).toBe(false);
      } finally {
        await registry.close();
        rmTempSync(cwd);
      }
    });
  }
});
