import {
  createBashToolDefinition,
  createSyntheticSourceInfo,
  ExtensionRunner,
  type ExtensionContext,
  type RegisteredTool,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { powerShellToolDefinitionFactory } from "../src/providers/pi-bash-cwd.js";

type Patch = "none" | "prefix" | "suffix" | "replace" | "deny" | "recover";
type ResultEvent = Parameters<ExtensionRunner["emitToolResult"]>[0];
type ResultPatch = Awaited<ReturnType<ExtensionRunner["emitToolResult"]>>;
const annotation = "✓ middleware annotation";
const command = "printf 'probe-output\\n'; exit 7";
const cwd = process.cwd();

async function run(
  runtime: "quickjs" | "node-process",
  patch: Patch,
  options: { command?: string; captured?: boolean; tool?: "bash" | "powershell"; preflight?: boolean; noRunner?: boolean; settle?: boolean; timeout?: number; signal?: AbortSignal; approvalDenied?: boolean; middleware?: (event: ResultEvent) => ResultPatch } = {},
) {
  const runner = {
    createContext: () => ({ cwd, sessionManager: { getSessionId: () => "settle-regression", getSessionFile: () => undefined } }),
    getActiveTools: () => [],
    emit: async () => undefined,
    emitToolCall: async () => options.preflight
      ? { block: true, reason: "policy denied\n\nCommand exited with code 7" }
      : undefined,
    emitToolResult: async (event: { content: { type: string; text?: string }[]; isError: boolean }) => {
      const note = { type: "text", text: annotation };
      // Real ExtensionRunner returns the effective isError even for content-only patches.
      if (patch === "prefix") return { content: [note, ...event.content], isError: event.isError };
      if (patch === "suffix") return { content: [...event.content, note], isError: event.isError };
      if (patch === "replace" || patch === "deny") return {
        content: [{ type: "text", text: "policy denied\n\nCommand exited with code 9" }], isError: true,
      };
      if (patch === "recover") return { content: [note], isError: false };
      return undefined;
    },
  } as unknown as ExtensionRunner;
  if (options.middleware) {
    // Exercise Pi's real merge semantics: even a content-only handler returns
    // the inherited isError, not a signal that the handler added a new failure.
    const handler = options.middleware;
    const middlewareRunner = {
      createContext: () => runner.createContext(),
      extensions: [{
        path: "/extensions/settle-test/index.ts",
        handlers: new Map([["tool_result", [handler]]]),
      }],
    } as unknown as ExtensionRunner;
    runner.emitToolResult = (event) => ExtensionRunner.prototype.emitToolResult.call(middlewareRunner, event);
  }
  const catalog = new CapturedToolCatalog();
  const shellDefinition = createBashToolDefinition(cwd);
  const capturedDefinition = options.tool === "powershell"
    ? { ...shellDefinition, name: "powershell", label: "powershell" }
    : shellDefinition;
  catalog.replace(options.captured ? [{
    definition: capturedDefinition as RegisteredTool["definition"],
    sourceInfo: createSyntheticSourceInfo("/extensions/bash-override/index.ts", { source: "test" }),
  }] : [], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
  const registry = new ActionRegistry();
  registry.register(new PiToolsProvider(cwd, options.noRunner ? undefined : catalog, new CapturedToolsProvider(catalog),
    { powerShellToolDefinitionFactory, allowUnhookedTools: options.noRunner === true }));
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.executor.runtime = runtime;
  config.approvals.execute = options.approvalDenied ? "deny" : "allow";
  return new FabricExecutionService(registry, config).execute({
    code: `return await pi.${options.tool ?? "bash"}({command: π.command, settle: π.settle === 'true', ...(π.timeout ? {timeout: Number(π.timeout)} : {})});`,
    strings: { command: options.command ?? command, settle: String(options.settle ?? true), timeout: options.timeout === undefined ? "" : String(options.timeout) },
    signal: options.signal,
    parentToolCallId: "settle-regression",
    context: {
      cwd, hasUI: false,
      sessionManager: { getSessionId: () => "settle-regression", getSessionFile: () => undefined },
    } as unknown as ExtensionContext,
    onPartial() {},
  });
}

describe.each(["quickjs", "node-process"] as const)("pi.bash settle via %s", (runtime) => {
  it.each(["none", "prefix", "suffix"] as const)("settles native exit with %s middleware", async (patch) => {
    const result = await run(runtime, patch);
    expect(result.error).toBeUndefined();
    expect(result.success).toBe(true);
    expect(result.value).toMatchObject({ ok: false, exitCode: 7, details: null });
    const value = result.value as { output: string; error: string };
    expect(value.output).toContain("probe-output\n");
    expect(value.output).not.toContain("Command exited with code 7");
    if (patch !== "none") {
      expect(value.output).toContain(annotation);
      expect(value.error).toContain(annotation);
    } else expect(value.output).toBe("probe-output\n");
  });

  it("settles a captured bash override with suffix middleware", async () => {
    const result = await run(runtime, "suffix", { captured: true });
    expect(result.error).toBeUndefined();
    expect(result.value).toMatchObject({ ok: false, exitCode: 7 });
  });

  it("settles a captured PowerShell override with suffix middleware", async () => {
    const result = await run(runtime, "suffix", { captured: true, tool: "powershell" });
    expect(result.error).toBeUndefined();
    expect(result.value).toMatchObject({ ok: false, exitCode: 7 });
  });

  describe.each([false, true])("processed results (captured=%s)", (captured) => {
    it.each([
      ["trim", (text: string): string => text.trim(), "private-value\n"],
      ["split and prefix", (text: string): string => "annotation\n" + text.trim(), "annotation\nprivate-value\n"],
      ["line endings", (text: string): string => text.replaceAll("\n", "\r\n"), "  private-value\r\n"],
      ["redaction", (text: string): string => text.replaceAll("private-value", "[redacted]"), "  [redacted]\n"],
      ["replacement", (_text: string): string => "[output withheld]", "[output withheld]"],
      ["empty replacement", (_text: string): string => "", ""],
    ] as const)("settles after %s without restoring original content", async (_name, transform, output) => {
      const result = await run(runtime, "none", {
        captured,
        command: "printf '  private-value\\n'; exit 7",
        middleware: (event: ResultEvent): ResultPatch => ({
          content: transform(event.content.filter((part) => part.type === "text").map((part) => part.text).join("\n"))
            .split("\n").map((text) => ({ type: "text" as const, text })),
        }),
      });
      expect(result.error).toBeUndefined();
      expect(result.value).toMatchObject({ ok: false, exitCode: 7, output });
      if (!output.includes("private-value")) {
        expect(JSON.stringify(result.value)).not.toContain("private-value");
      }
    });
  });

  it("settles without an extension runner", async () => {
    expect((await run(runtime, "none", { noRunner: true })).value)
      .toMatchObject({ ok: false, exitCode: 7, output: "probe-output\n" });
  });

  it("still rejects without settle", async () => {
    const result = await run(runtime, "suffix", { settle: false });
    expect(result.success).toBe(false);
    expect(result.error).toContain(annotation);
  });

  it("does not classify successful stdout as an exit failure", async () => {
    const result = await run(runtime, "suffix", { command: "printf 'Command exited with code 7'" });
    expect(result.value).toMatchObject({ ok: true });
  });

  it.each([false, true])("does not settle preflight denial (captured=%s)", async (captured) => {
    const result = await run(runtime, "suffix", { preflight: true, captured });
    expect(result.success).toBe(false);
    expect(result.error).toContain("policy denied");
  });

  it("does not settle a middleware failure after native success", async () => {
    const result = await run(runtime, "deny", { command: "true" });
    expect(result.success).toBe(false);
    expect(result.error).toContain("policy denied");
  });

  it("does not replace the native exit code with one from middleware text", async () => {
    // Known interface limitation: after a native failure, replacement text
    // plus effective isError:true cannot identify a new middleware veto.
    const result = await run(runtime, "replace");
    expect(result.success).toBe(true);
    expect(result.value).toMatchObject({
      ok: false, exitCode: 7, output: "policy denied\n\nCommand exited with code 9",
    });
  });

  it("honors explicit middleware recovery", async () => {
    const result = await run(runtime, "recover");
    expect(result.value).toMatchObject({ ok: true, output: annotation });
  });

  it("does not settle approval denial", async () => {
    expect((await run(runtime, "none", { approvalDenied: true })).success).toBe(false);
  });

  it("does not settle a timeout even with an exit marker in stdout", async () => {
    const result = await run(runtime, "suffix", {
      command: "printf 'Command exited with code 7'; sleep 3", timeout: 1,
    });
    expect(result.success).toBe(false);
    expect(result.error).toContain("Command timed out");
  });

  it("does not settle cancellation", async () => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("user cancelled\n\nCommand exited with code 7")), 200);
    try {
      const result = await run(runtime, "suffix", { command: "sleep 3", signal: controller.signal });
      expect(result.success).toBe(false);
      expect(result.trace.outcome).toBe("aborted");
    } finally {
      clearTimeout(timer);
    }
  });
});
