import type { Theme } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import { prepareFabricExecArguments } from "../src/fabric-exec-arguments.js";
import { defaultFabricExecutionGuidance, fabricExecutionKernelGuidance } from "../src/core/system-guidance.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";
import { hostGlobalsGuidance } from "../src/core/system-guidance.js";
import { typeErrorRecoveryHint } from "../src/type-error-guidance.js";
import { guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";

const toolFor = (kernel: "typescript" | "python", pythonRuntime: "cpython" | "monty" = "cpython", mode: { fullCodeMode?: boolean; schema?: "off" | "enforce" } = {}) => {
  const state = {
    bootstrapped: true,
    config: normalizeFabricConfig({
      executor: { kernel, pythonRuntime }, ui: { toolDisplay: "full" },
      ...(mode.fullCodeMode !== undefined ? { fullCodeMode: mode.fullCodeMode } : {}),
      ...(mode.schema ? { schema: { mode: mode.schema } } : {}),
    }),
  } as FabricState;
  return createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), (tool) => tool);
};

describe("exclusive kernel tool surface", () => {
  it("publishes only the configured language with no per-call selector", () => {
    const ts = toolFor("typescript");
    const python = toolFor("python");
    expect(ts.description).toContain("type-checked TypeScript");
    expect(ts.parameters.properties.code.description).toContain("TypeScript function body");
    expect(python.description).toContain("CPython");
    expect(python.description).not.toContain("TypeScript");
    expect(python.parameters.properties.code.description).toContain("Python async function body");
    expect(python.parameters.properties.code.description).not.toContain("TypeScript");
    expect(python.promptGuidelines?.join("\n")).toContain("asyncio.gather");
    expect(python.promptGuidelines?.join("\n")).not.toContain("Promise.all");
    expect(ts.parameters.properties).not.toHaveProperty("kernel");
    expect(python.parameters.properties).not.toHaveProperty("kernel");
    expect(python.parameters.properties).not.toHaveProperty("tokenBudget");
    expect(ts.parameters.properties).toHaveProperty("tokenBudget");
    expect(python.parameters.required).toEqual(["code"]);
  });

  // smarty-dev#459: orchestration-only programs have no `pi` or `extensions`; guidance that
  // advertised them caused 49 "Cannot find name 'pi'" failures in the fleet.
  it.each(["typescript", "python"] as const)("advertises pi.* only where %s programs have it", (kernel) => {
    const orchestration = toolFor(kernel, "cpython", { fullCodeMode: false }).promptGuidelines!.join("\n");
    expect(orchestration).not.toMatch(/\bpi\.[a-z]/);
    expect(orchestration).not.toMatch(/\bextensions\.[a-z]/);
    expect(orchestration).toContain("`pi` and `extensions` do not exist inside `fabric_exec`");
    expect(toolFor(kernel, "cpython", { fullCodeMode: true }).promptGuidelines!.join("\n")).toContain("pi.edit");
    // Schema enforce routes every tool through fabric_exec, so pi.* is available there too.
    expect(toolFor(kernel, "cpython", { fullCodeMode: false, schema: "enforce" }).promptGuidelines!.join("\n")).toContain("pi.edit");
  });

  it("describes Monty's subset without advertising native Python", () => {
    const tool = toolFor("python", "monty");
    expect(tool.description).toContain("Monty");
    expect(tool.parameters.properties.code.description).toContain("sandboxed Python subset");
    expect(tool.parameters.properties.code.description).not.toContain("standard-library imports are available");
    expect(defaultFabricExecutionGuidance(true, "python", "monty")).toContain("arbitrary imports are unavailable");
    expect(fabricExecutionKernelGuidance(true, "python", "monty")).toContain("Monty sandboxed subset");
  });

  it("keeps Python source untouched while applying language-neutral argument normalization", () => {
    const code = "return await pi.read(/tmp/unquoted)";
    const input = { code: [code], strings: '{"body":"π😀\\ntext"}', display: "Probe" };
    expect(prepareFabricExecArguments(input, "python")).toEqual({
      code, payloads: { body: "π😀\ntext" }, display: { name: "Probe" },
    });
    expect(toolFor("python").prepareArguments!(code)).toEqual({ code });
    expect(prepareFabricExecArguments(code)).toEqual({ code: 'return await pi.read("/tmp/unquoted")' });
  });

  it("renders a Python label rather than parsing Python as TypeScript", () => {
    const tool = toolFor("python");
    const args = { code: 'import json\nreturn json.loads(π.body)' };
    const theme = { fg: (_color: string, text: string) => text, bold: (text: string) => text } as Theme;
    const rendered = tool.renderCall!(args, theme, {
      args, state: {}, invalidate: vi.fn(), toolCallId: "python", cwd: process.cwd(),
      executionStarted: false, argsComplete: true, isPartial: false, expanded: true,
      showImages: false, isError: false,
    } as never).render(120).join("\n");
    expect(rendered).toContain("Python · 2 lines");
    expect(rendered).not.toContain("TypeScript");
  });

  it("registers safely before configuration is bootstrapped", () => {
    const state = { bootstrapped: false, get config(): never { throw new Error("not bootstrapped"); } } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), (value) => value);
    expect(tool.description).toContain("TypeScript");
    expect(tool.prepareArguments!("return 1")).toEqual({ code: "return 1" });
  });

  it.each(["typescript", "python"] as const)("retains historical %s labels after a language switch", (kernel) => {
    const currentKernel = kernel === "python" ? "typescript" : "python";
    const tool = toolFor(currentKernel);
    const args = { code: "return 1" };
    const context = {
      args, state: {}, invalidate: vi.fn(), toolCallId: "history", cwd: process.cwd(),
      executionStarted: false, argsComplete: true, isPartial: false, expanded: true,
      showImages: false, isError: false,
    };
    const theme = { fg: (_color: string, text: string) => text, bold: (text: string) => text } as Theme;
    tool.renderResult!(
      { content: [], details: { kernel, success: true, audits: [], phases: [] } } as never,
      { expanded: false, isPartial: false }, theme, context as never,
    );
    const rendered = tool.renderCall!(args, theme, context as never).render(120).join("\n");
    expect(rendered).toContain(kernel === "python" ? "Python · 1 line" : "TypeScript · 1 line");
    expect(context.invalidate).toHaveBeenCalledOnce();
  });

  it("uses Python syntax in turn-stable guidance", () => {
    const guidance = defaultFabricExecutionGuidance(true, "python");
    expect(guidance).toContain('r["output"]');
    expect(guidance).toContain("settle=True");
    expect(guidance).not.toContain("Promise.all");
    expect(fabricExecutionKernelGuidance(true, "python")).toContain("Python (Monty sandboxed subset)");
    expect(fabricExecutionKernelGuidance(true, "python", "cpython")).toContain("Python (CPython)");
    expect(fabricExecutionKernelGuidance(true)).toContain("kernel: TypeScript");
    expect(defaultFabricExecutionGuidance(false, "python")).toContain("unavailable inside fabric_exec");
  });

  // smarty-dev#459: host globals (process, fetch, btoa, TextEncoder, crypto, ...) were unguided.
  it("names the missing host globals only for the QuickJS TypeScript kernel, and the hint repeats that line", () => {
    for (const fullCodeMode of [true, false]) {
      const line = hostGlobalsGuidance(fullCodeMode);
      expect(fabricExecutionKernelGuidance(fullCodeMode, "typescript", "monty", "quickjs")).toContain(line);
      expect(fabricExecutionKernelGuidance(fullCodeMode, "typescript", "monty", "node-process")).not.toContain("no host globals");
      expect(fabricExecutionKernelGuidance(fullCodeMode, "python", "monty")).not.toContain("no host globals");
      expect(fabricExecutionKernelGuidance(fullCodeMode, "python", "cpython")).not.toContain("no host globals");
      for (const name of ["process", "fetch", "btoa", "TextEncoder", "crypto", "Bun"]) {
        const hint = typeErrorRecoveryHint(`return ${name}`, [{ line: 2, column: 8, message: `Cannot find name '${name}'.` }], fullCodeMode);
        expect(hint).toBe(`Recovery hint: ${line}`);
      }
    }
    expect(hostGlobalsGuidance(false)).toContain("native `bash` tool");
    expect(hostGlobalsGuidance(false)).not.toMatch(/\bpi\./);
    expect(hostGlobalsGuidance(true)).toContain("`pi.bash`");
  });

  // smarty-dev#459: 5 of 6 residual `Cannot find name 'pi'` failures came on a turn that started
  // (peer message after /reload) before bootstrap, with guidance from the full-code default.
  it("gives an orchestration-only session orchestration guidance on a turn before bootstrap", async () => {
    const agentDir = mkdtempSync(path.join(tmpdir(), "fabric-mode-"));
    try {
      writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: false }));
      vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
      vi.stubEnv("PI_FABRIC_FULL_CODE_MODE", undefined);
      const handlers = new Map<string, Array<(event: any, context: any) => any>>();
      const registered: ToolDefinition<any, any, any>[] = [];
      const pi = {
        events: { emit: vi.fn(), on: vi.fn(() => () => {}) }, getActiveTools: vi.fn(() => ["fabric_exec"]), getAllTools: vi.fn(() => []),
        on: (event: string, handler: (event: any, context: any) => any) => handlers.set(event, [...(handlers.get(event) ?? []), handler]),
        registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), setActiveTools: vi.fn(),
        registerTool: vi.fn((tool: ToolDefinition<any, any, any>) => registered.push(tool)),
      } as unknown as ExtensionAPI;
      const { default: piFabric } = await import("../src/index.js");
      await piFabric(pi);
      // No session_start yet: this is the state a turn sees before bootstrap completes.
      const tool = registered.filter((candidate) => candidate.name === "fabric_exec").at(-1)!;
      const rules = tool.promptGuidelines!.join("\n");
      expect(rules).toContain("`pi` and `extensions` do not exist inside `fabric_exec`");
      expect(rules).not.toMatch(/\bpi\.[a-z]/);
      const event = { systemPrompt: "Base", prompt: "inspect", systemPromptOptions: { skills: [] } };
      const prompt = await handlers.get("before_agent_start")![0]!(event, {});
      expect(prompt.systemPrompt).toContain("orchestration-only mode");
      expect(prompt.systemPrompt).not.toContain("full code mode: `fabric_exec` is the only way");
      expect(prompt.systemPrompt).toContain(hostGlobalsGuidance(false));
      // The executor type-checks with the same mode: no `pi` in the guest declarations.
      const recovery = typeErrorRecoveryHint("return 1", [{ line: 2, column: 8, message: "Cannot find name 'process'." }], false);
      expect(recovery).toContain("native `bash` tool");
      expect(guestTypeDeclarations(false)).not.toContain("declare const pi:");
    } finally {
      vi.unstubAllEnvs();
      rmSync(agentDir, { recursive: true, force: true });
    }
  });
});
