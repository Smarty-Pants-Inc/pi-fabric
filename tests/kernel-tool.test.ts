import type { Theme } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import { prepareFabricExecArguments } from "../src/fabric-exec-arguments.js";
import { defaultFabricExecutionGuidance, fabricExecutionKernelGuidance } from "../src/core/system-guidance.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";
import { hostGlobalsGuidance } from "../src/core/system-guidance.js";
import { builtinModelGuidance } from "../src/components/builtin-guidance.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { typeErrorRecoveryHint } from "../src/type-error-guidance.js";
import { guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { emitBeforeAgentStart } from "./helpers/emit-before-agent-start.js";

const toolFor = (kernel: "typescript" | "python", pythonRuntime: "cpython" | "monty" = "cpython", mode: { fullCodeMode?: boolean; schema?: "off" | "enforce"; runtime?: "quickjs" | "node-process"; profile?: "additive" | "native" } = {}) => {
  const state = {
    bootstrapped: true,
    config: normalizeFabricConfig({
      executor: { kernel, pythonRuntime, ...(mode.runtime ? { runtime: mode.runtime } : {}), ...(mode.profile ? { codemodeProfile: mode.profile } : {}) }, ui: { toolDisplay: "full" },
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
    expect(ts.promptGuidelines).toBeUndefined();
    expect(python.promptGuidelines).toBeUndefined();
    expect(ts.parameters.properties).not.toHaveProperty("kernel");
    expect(python.parameters.properties).not.toHaveProperty("kernel");
    expect(python.parameters.properties).not.toHaveProperty("tokenBudget");
    expect(ts.parameters.properties).toHaveProperty("tokenBudget");
    expect(python.parameters.required).toEqual(["code"]);
  });

  // smarty-dev#459: orchestration-only programs have no `pi` or `extensions`; guidance that
  // advertised them caused 49 "Cannot find name 'pi'" failures in the fleet.
  it.each(["typescript", "python"] as const)("advertises pi.* only where %s programs have it", (kernel) => {
    const codeDescription = (mode: { fullCodeMode: boolean; schema?: "enforce" }) =>
      toolFor(kernel, "cpython", mode).parameters.properties.code.description;
    expect(codeDescription({ fullCodeMode: false })).toContain("Orchestration-only: pi and extensions are unavailable");
    expect(codeDescription({ fullCodeMode: true })).toContain("pi and extensions are available");
    expect(codeDescription({ fullCodeMode: false, schema: "enforce" })).toContain("pi and extensions are available");
  });

  it("describes Monty's subset without advertising native Python", () => {
    const tool = toolFor("python", "monty");
    expect(tool.description).toContain("Monty");
    expect(tool.description).toContain("sandboxed subset");
    expect(tool.description).toContain("no native imports");
    expect(tool.parameters.properties.code.description).not.toContain("standard-library imports are available");
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

  it("keeps execution helpers and built-in digests empty across the mode/language matrix", () => {
    expect(fabricExecutionKernelGuidance(true)).toBe("");
    expect(defaultFabricExecutionGuidance(true)).toBe("");
    for (const kernel of ["typescript", "python"] as const) {
      for (const pythonRuntime of ["monty", "cpython"] as const) {
        for (const profile of ["additive", "native"] as const) {
          for (const runtime of ["quickjs", "node-process"] as const) {
            for (const mode of [{ fullCodeMode: true }, { fullCodeMode: false }, { fullCodeMode: false, schema: "enforce" }] as const) {
              const tool = toolFor(kernel, pythonRuntime, { ...mode, runtime, profile });
              const contract = [tool.description, tool.parameters.properties.code.description, tool.parameters.properties.payloads.description].join("");
              expect(contract.length, JSON.stringify({ kernel, pythonRuntime, profile, runtime, ...mode })).toBeLessThanOrEqual(1_000);
              expect(Math.ceil(contract.length / 4)).toBeLessThanOrEqual(250);
              expect(tool).not.toHaveProperty("promptGuidelines");
              expect(contract).not.toMatch(/acceptance ledger|batch independent|display\.name|Main.*10 min|Jev supplies/i);
              expect(tool.parameters.properties.code.description).toContain(kernel === "python" ? "Python async function body" : profile === "native" ? "JavaScript async function body" : "TypeScript function body");
              expect(tool.parameters.properties.code.description).toContain(mode.fullCodeMode || "schema" in mode ? "pi and extensions are available" : "pi and extensions are unavailable");
              expect(defaultFabricExecutionGuidance(mode.fullCodeMode, kernel, pythonRuntime, profile)).toBe("");
              expect(fabricExecutionKernelGuidance(mode.fullCodeMode, kernel, pythonRuntime, profile)).toBe("");
              for (const enabled of [true, false]) {
                expect(builtinModelGuidance(normalizeFabricConfig({ jev: { enabled }, schema: { mode: "schema" in mode ? "enforce" : "off" } }))).toEqual([]);
              }
            }
          }
        }
      }
    }
  });

  it("keeps indispensable facts in schema text when the host strips promptGuidelines", () => {
    for (const mode of [{ fullCodeMode: true }, { fullCodeMode: false }, { fullCodeMode: false, schema: "enforce" }] as const) {
      const { promptGuidelines: discarded, ...schemaOnlyTool } = toolFor("typescript", "monty", mode);
      const schemaText = JSON.stringify({ description: schemaOnlyTool.description, parameters: schemaOnlyTool.parameters });
      expect(schemaOnlyTool).not.toHaveProperty("promptGuidelines");
      expect(schemaText).toContain("payloads are literal: read files with native tools first");
      expect(schemaText).toContain("Only exact supplied keys exist: payloads.body becomes π.body");
      expect(schemaText).toContain("QuickJS lacks host globals: process, fetch");
      expect(schemaText).toContain("Buffer");
    }
  });

  it("describes native source and discovery without system guidance", () => {
    const tool = toolFor("typescript", "monty", { profile: "native" });
    expect(tool.description).toContain("no type checking or repair");
    expect(tool.parameters.properties.code.description).toContain("Fabric discovery is fabric.tools");
  });

  it("keeps short QuickJS facts in the contract and longer mode-aware recovery on demand", () => {
    for (const fullCodeMode of [true, false]) {
      const line = hostGlobalsGuidance(fullCodeMode);
      expect(toolFor("typescript", "cpython", { fullCodeMode }).description).toContain("QuickJS lacks host globals: process, fetch");
      expect(toolFor("typescript", "cpython", { fullCodeMode, runtime: "node-process" }).description).not.toContain("host globals");
      expect(toolFor("python", "monty", { fullCodeMode }).description).not.toContain("host globals");
      expect(toolFor("python", "cpython", { fullCodeMode }).description).not.toContain("host globals");
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
      expect(tool.promptGuidelines).toBeUndefined();
      expect(tool.parameters.properties.code.description).toContain("Orchestration-only: pi and extensions are unavailable");
      expect(tool.description).toContain("QuickJS lacks host globals");
      const event = { systemPrompt: "Base", prompt: "inspect", systemPromptOptions: { skills: [] } };
      const prompt = await emitBeforeAgentStart(handlers, event, {});
      expect(prompt.systemPrompt).toBe("Base");
      expect(prompt.systemPromptOptions.sections.fabric_execution).toBe("");
      expect(prompt.systemPrompt).not.toMatch(/fabric_execution|Jev supplies|host globals/);
      // The executor type-checks with the same mode: no `pi` in the guest declarations.
      const recovery = typeErrorRecoveryHint("return 1", [{ line: 2, column: 8, message: "Cannot find name 'process'." }], false);
      expect(recovery).toContain("native `bash` tool");
      expect(guestTypeDeclarations(false)).not.toContain("declare const pi:");
    } finally {
      vi.unstubAllEnvs();
      rmSync(agentDir, { recursive: true, force: true });
    }
  });

  // smarty-dev#2340: every runtime (QuickJS, node process, Monty, CPython) receives
  // payloads only through execute(), so the shared validation must reject before dispatch.
  it.each([
    ["typescript", "cpython", "quickjs"],
    ["typescript", "cpython", "node-process"],
    ["python", "monty", undefined],
    ["python", "cpython", undefined],
  ] as const)("rejects placeholder payloads before %s/%s/%s runtime execution", async (kernel, pythonRuntime, runtime) => {
    const execute = vi.fn(async () => ({ success: true, output: "ran" }));
    const state = {
      bootstrapped: true,
      config: normalizeFabricConfig({
        executor: { kernel, pythonRuntime, ...(runtime ? { runtime } : {}) }, ui: { toolDisplay: "full" },
      }),
      ensure: vi.fn(async () => {}),
      execution: { execute },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), (value) => value);
    const literal = "payloads are literal: read the file with a native tool first and pass its content";
    for (const args of [
      { code: "return 1", payloads: { spec: "@/tmp/spec.md" } },
      { code: "return 1", payloads: '{"spec":"__SPEC__"}' },
      { code: "return 1", strings: { spec: "file:///tmp/spec.md" } },
    ]) {
      expect(() => tool.prepareArguments!(args)).toThrow(literal);
      await expect(tool.execute("call", args as never, undefined, undefined, {} as never)).rejects.toThrow(/"spec".*payloads are literal/s);
    }
    expect(execute).not.toHaveBeenCalled();
  });

  it("keeps the original session artifact writer when shutdown removes the runtime during execute", async () => {
    const closedWriter = vi.fn(async (_content: string): Promise<string> => { throw new Error("session closed"); });
    let writer: typeof closedWriter | undefined = closedWriter;
    const state = {
      bootstrapped: true,
      config: normalizeFabricConfig({ executor: { kernel: "typescript", maxOutputChars: 1000 }, ui: { toolDisplay: "full" } }),
      ensure: vi.fn(async () => {}),
      get outputArtifactWriter() { return writer; },
      execution: { execute: vi.fn(async () => {
        writer = undefined;
        return { success: true, value: "long output with spaces ".repeat(2000), logs: [], audits: [], phases: [], kernel: "typescript",
          trace: { kind: "pi-fabric.execution", version: 1, outcome: "succeeded", phases: [], operations: [],
            counts: { droppedValues: 0, truncatedValues: 0, redactedValues: 0, droppedOperations: 0 } } };
      }) },
      claimHandoff: vi.fn(async () => undefined),
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const result = await tool.execute("late-output", { code: "return data" } as never, undefined, undefined, {
      sessionManager: { getSessionId: () => "session" },
    } as never);
    expect(closedWriter).toHaveBeenCalledOnce();
    expect(result.content).not.toEqual([]);
  });

  it.each([true, false])("recovers through real tool execution after missing-payload and host-global failures (fullCodeMode=%s)", async (fullCodeMode) => {
    const config = normalizeFabricConfig({ fullCodeMode, jev: { enabled: false } });
    const execution = new FabricExecutionService(new ActionRegistry(), config);
    const state = {
      bootstrapped: true, config, execution,
      ensure: async () => {},
      claimHandoff: async () => undefined,
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const context = { cwd: process.cwd(), hasUI: false, sessionManager: { getSessionId: () => "recovery" } };
    const run = (id: string, args: { code: string; payloads?: Record<string, string> }) =>
      tool.execute(id, args, undefined, undefined, context as never);
    const text = (result: Awaited<ReturnType<typeof run>>) => result.content.filter(part => part.type === "text").map(part => part.text).join("\n");

    const missing = await run("missing-payload", { code: "return π.body" });
    expect(missing.isError).toBe(true);
    expect(text(missing)).toContain("Pre-execution check: π.body");
    expect(text(missing)).toContain("Add payloads: { body: '...' }");
    const supplied = await run("retry-payload", { code: "return π.body", payloads: { body: "literal content recovered" } });
    expect(supplied.isError).not.toBe(true);
    expect(text(supplied)).toContain("literal content recovered");

    const hostGlobal = await run("missing-host-global", { code: "return Buffer.from('content')" });
    expect(hostGlobal.isError).toBe(true);
    expect(text(hostGlobal)).toContain("Type errors; code was not executed");
    expect(text(hostGlobal)).toContain(hostGlobalsGuidance(fullCodeMode));
    expect(text(hostGlobal).match(/Recovery hint:/g)).toHaveLength(1);
    const builtin = await run("retry-with-builtin", { code: "return JSON.stringify({ content: 'recovered with a builtin' })" });
    expect(builtin.isError).not.toBe(true);
    expect(text(builtin)).toContain("recovered with a builtin");
  });

  // W9 item 3: native codemode bypasses type checking. The real guest failure
  // must still explain a permitted correction without restoring standing advice.
  it.each([true, false])("recovers native QuickJS host-global errors on demand (fullCodeMode=%s)", async (fullCodeMode) => {
    const config = normalizeFabricConfig({ fullCodeMode, executor: { codemodeProfile: "native" }, jev: { enabled: false } });
    const execution = new FabricExecutionService(new ActionRegistry(), config);
    const dispatch = vi.spyOn(execution, "execute");
    const state = {
      bootstrapped: true, config, execution,
      ensure: async () => {}, claimHandoff: async () => undefined,
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const context = { cwd: process.cwd(), hasUI: false, sessionManager: { getSessionId: () => "native-recovery" } };
    const failed = await tool.execute("native-missing-process", { code: "return process.env" }, undefined, undefined, context as never);
    const text = failed.content.filter(part => part.type === "text").map(part => part.text).join("\n");
    const original = await dispatch.mock.results[0]!.value;
    expect(failed.isError).toBe(true);
    expect(JSON.parse(original.error!)).toMatchObject({ name: "ReferenceError", message: "'process' is not defined" });
    expect(text).toContain(`Runtime error: ${original.error}`);
    expect(text).not.toContain("Type errors; code was not executed");
    expect(text).toContain(`Recovery hint: ${hostGlobalsGuidance(fullCodeMode)}`);
    expect(text.match(/Recovery hint:/g)).toHaveLength(1);
    expect(original.error).not.toContain("Recovery hint:");
    expect(failed.details).toMatchObject({ error: original.error, trace: original.trace, audits: original.audits });
    const theme = { fg: (_color: string, text: string) => text, bold: (text: string) => text } as Theme;
    const rendered = tool.renderResult!(failed, { expanded: true, isPartial: false }, theme, {
      args: { code: "return process.env" }, state: {}, invalidate: vi.fn(), toolCallId: "native-missing-process", cwd: process.cwd(),
    } as never).render(2_000).join("\n");
    expect(rendered).toContain(`Recovery hint: ${hostGlobalsGuidance(fullCodeMode)}`);
    const corrected = await tool.execute("native-retry-builtin", {
      code: "return JSON.stringify({ content: 'native recovered with a builtin' })",
    }, undefined, undefined, context as never);
    expect(corrected.isError).not.toBe(true);
    expect(corrected.content).toEqual([{ type: "text", text: expect.stringContaining("native recovered with a builtin") }]);
    const shadowed = await tool.execute("native-local-process", {
      code: "const process = { env: 'guest-local-name' }; return process.env",
    }, undefined, undefined, context as never);
    expect(shadowed.isError).not.toBe(true);
    expect(shadowed.content).toEqual([{ type: "text", text: "guest-local-name" }]);
  });

  it.each([
    "throw new Error(\"'process' is not defined\")",
    "return unrelatedMissingName",
    "return process; let process = 1",
    "throw { name: 'ReferenceError', message: \"'process' is not defined\" }",
    "throw new Error('ReferenceError: process is not defined; Recovery hint: existing advice')",
  ])("does not diagnose unrelated native QuickJS failures: %s", async (code) => {
    const config = normalizeFabricConfig({ executor: { codemodeProfile: "native" }, jev: { enabled: false } });
    const state = {
      bootstrapped: true, config, execution: new FabricExecutionService(new ActionRegistry(), config),
      ensure: async () => {}, claimHandoff: async () => undefined,
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const result = await tool.execute("native-unrelated", { code }, undefined, undefined, {
      cwd: process.cwd(), hasUI: false, sessionManager: { getSessionId: () => "native-unrelated" },
    } as never);
    expect(result.isError).toBe(true);
    expect(result.content).toEqual([{ type: "text", text: expect.not.stringContaining(hostGlobalsGuidance(true)) }]);
  });

  it.each([
    { runtime: "quickjs", schema: "off", hostFrame: false, priorCall: false, hint: true },
    { runtime: "node-process", schema: "off", hostFrame: false, priorCall: false, hint: false },
    { runtime: "bun-process", schema: "off", hostFrame: false, priorCall: false, hint: false },
    { runtime: "node-process", schema: "enforce", hostFrame: false, priorCall: false, hint: true },
    { runtime: "quickjs", schema: "off", hostFrame: true, priorCall: false, hint: false },
    { runtime: "quickjs", schema: "off", hostFrame: false, priorCall: true, hint: true },
  ] as const)("budgets recovery without changing error metadata: %j", async ({ runtime, schema, hostFrame, priorCall, hint }) => {
    const error = JSON.stringify({ name: "ReferenceError", message: "'process' is not defined", stack:
      (hostFrame ? "    at __call (pi-fabric-setup.js:1:1)\n" : "") + "    at __piFabricMain (pi-fabric-guest.js:2:13)\n" });
    const audits = priorCall ? [{ ref: "fixture.inspect", tool: "inspect", provider: "fixture", success: true, args: {}, result: "completed before error" }] : [];
    const trace = { kind: "pi-fabric.execution", version: 1, outcome: "failed", phases: [], operations: [],
      counts: { droppedValues: 0, truncatedValues: 0, redactedValues: 0, droppedOperations: 0 } };
    const writeArtifact = vi.fn(async (_content: string) => "/session/output.txt");
    const state = {
      bootstrapped: true,
      config: normalizeFabricConfig({ fullCodeMode: true, schema: { mode: schema }, executor: { codemodeProfile: "native", runtime }, ui: { toolDisplay: "full" } }),
      ensure: async () => {}, claimHandoff: async () => undefined,
      outputArtifactWriter: writeArtifact,
      execution: { execute: async () => ({ success: false, kernel: "typescript", value: undefined, logs: [], audits, phases: [], error, trace }) },
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const result = await tool.execute("bounded-native-error", { code: "return process.env", maxOutputTokens: 64 }, undefined, undefined, {
      sessionManager: { getSessionId: () => "bounded-recovery" },
    } as never);
    const text = result.content.filter(part => part.type === "text").map(part => part.text).join("\n");
    expect(result.isError).toBe(true);
    expect(result.details).toMatchObject({ error, trace, audits });
    if (hint) {
      expect(writeArtifact).toHaveBeenCalledExactlyOnceWith(`Runtime error: ${error}\n\nRecovery hint: ${hostGlobalsGuidance(true)}`);
      expect(text.length).toBeLessThanOrEqual(256);
      expect(text).toContain("/session/output.txt");
      const theme = { fg: (_color: string, text: string) => text, bold: (text: string) => text } as Theme;
      const rendered = tool.renderResult!(result, { expanded: true, isPartial: false }, theme, {
        args: { code: "return process.env" }, state: {}, invalidate: vi.fn(), toolCallId: "bounded-native-error", cwd: process.cwd(),
      } as never).render(2_000).join("\n");
      expect(rendered).toContain("/session/output.txt");
      expect(rendered).not.toContain(error);
    } else {
      expect(writeArtifact).not.toHaveBeenCalled();
      expect(text).toBe(`Runtime error: ${error}`);
      expect(text).not.toContain("Recovery hint:");
    }
  });

  it("keeps every failure-progress receipt above a tiny output budget", async () => {
    const state = {
      bootstrapped: true,
      config: normalizeFabricConfig({
        executor: { kernel: "typescript", maxOutputChars: 32 },
        ui: { toolDisplay: "full" },
      }),
      ensure: vi.fn(async () => {}),
      outputArtifactWriter: vi.fn(async () => "failure-progress-output.txt"),
      execution: {
        execute: vi.fn(async () => ({
          success: false,
          value: { answer: "long returned value" },
          logs: ["guest log ".repeat(20)],
          audits: [],
          phases: [],
          kernel: "typescript" as const,
          error: "outer failure",
          trace: {
            kind: "pi-fabric.execution" as const,
            version: 1 as const,
            outcome: "failed" as const,
            phases: [],
            operations: [
              {
                type: "call" as const,
                sequence: 0,
                ref: "agents.followUp",
                args: {},
                outcome: "succeeded" as const,
                result: { messageId: "message-budget", deliveryId: "delivery-budget" },
              },
              {
                type: "call" as const,
                sequence: 1,
                ref: "mesh.publish",
                args: {},
                outcome: "succeeded" as const,
                result: { id: "event-budget", sequence: 23 },
              },
            ],
            counts: { droppedValues: 0, truncatedValues: 0, redactedValues: 0, droppedOperations: 0 },
          },
        })),
      },
      claimHandoff: vi.fn(async () => undefined),
      prewalk: { planRequired: () => false },
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    const result = await tool.execute(
      "tiny-budget",
      { code: "return 1" } as never,
      undefined,
      undefined,
      { sessionManager: { getSessionId: () => "session" } } as never,
    );
    const text = result.content
      .filter((part): part is { type: "text"; text: string } => part.type === "text")
      .map(part => part.text)
      .join("\n");
    expect(text).toContain("messageId=message-budget");
    expect(text).toContain("deliveryId=delivery-budget");
    expect(text).toContain("id=event-budget");
    expect(text).toContain("sequence=23");
    expect(result.details).not.toHaveProperty("outputFormat");
  });

  it.each(["typescript", "python"] as const)("tells %s programs that payloads are literal", (kernel) => {
    for (const fullCodeMode of [true, false]) {
      const line = "payloads are literal: read files with native tools first";
      const tool = toolFor(kernel, "cpython", { fullCodeMode });
      expect(tool.parameters.properties.payloads.description).toContain(line);
      expect(tool.parameters.properties.payloads.description).toContain("Only exact supplied keys exist: payloads.body becomes π.body");
      expect(tool.parameters.properties.payloads.description).toContain("No placeholders or file references");
      expect(tool.promptGuidelines).toBeUndefined();
    }
  });
});
