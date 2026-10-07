import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { hostGlobalsGuidance } from "../src/core/system-guidance.js";
import ts from "typescript";
import { normalizeFabricConfig } from "../src/config.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import type { FabricState } from "../src/fabric-state.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";

describe("prewalk prompt isolation", () => {
  it("does not add prewalk state or guidance to before_agent_start", () => {
    const extensionSource = fs.readFileSync(
      path.join(process.cwd(), "src", "extension.ts"),
      "utf8",
    );
    const toolSource = fs.readFileSync(
      path.join(process.cwd(), "src", "fabric-exec-tool.ts"),
      "utf8",
    );
    const source = ts.createSourceFile("extension.ts", extensionSource, ts.ScriptTarget.Latest, true);
    const handlers: string[] = [];
    const visit = (node: ts.Node): void => {
      if (ts.isCallExpression(node) && node.expression.getText(source) === "pi.on" &&
          node.arguments[0] && ts.isStringLiteral(node.arguments[0]) &&
          node.arguments[0].text === "before_agent_start") {
        expect(node.arguments[1]).toBeDefined();
        handlers.push(node.arguments[1]!.getText(source));
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
    expect(handlers.length).toBeGreaterThan(0);
    for (const handler of handlers) expect(handler.toLowerCase()).not.toContain("prewalk");

    const guidelinesStart = toolSource.indexOf("promptGuidelines: [");
    const guidelinesEnd = toolSource.indexOf("parameters:", guidelinesStart);
    expect(guidelinesStart).toBeGreaterThanOrEqual(0);
    expect(guidelinesEnd).toBeGreaterThan(guidelinesStart);
    const guidelines = toolSource.slice(guidelinesStart, guidelinesEnd).toLowerCase();
    expect(guidelines).not.toContain("prewalk");
    expect(guidelines).not.toContain("handoff");
  });

  it("keeps coding guidance outcome-oriented and context-bounded", () => {
    const state = { bootstrapped: true, config: normalizeFabricConfig({}) } as FabricState;
    const allGuidelines = createFabricExecTool(
      state, defaultCodePreviewSettings(), new Map(), (tool) => tool,
    ).promptGuidelines ?? [];
    // The QuickJS host-globals line moved here from the system section (net zero); it is
    // not coding guidance, so it stays outside this budget.
    expect(allGuidelines.at(-1)).toBe(hostGlobalsGuidance(true));
    const visibleGuidelines = allGuidelines.slice(0, -1);
    const guidelines = visibleGuidelines.join("\n");
    const visibleGuidelineChars = visibleGuidelines.reduce((total, line) => total + line.length, 0);

    expect(visibleGuidelines).toHaveLength(6);
    expect(visibleGuidelineChars).toBeLessThanOrEqual(2_200);
    expect(guidelines).toContain("acceptance ledger in reasoning or concise progress notes (no tool call)");
    expect(guidelines).toContain("direct behavioral probes");
    expect(guidelines).toContain("requested public symbols, registrations, and configuration entries");
    expect(guidelines).toContain("smallest checks that cover the ledger");
    expect(guidelines).toContain("instead of rerunning unchanged passing checks");
    expect(guidelines).toContain("A build alone is not completion");
    expect(guidelines).toContain("one `pi.edit({path, edits:[...]})`");
    expect(guidelines).toContain("`all:true` only for intentional repeated exact anchors");
    expect(guidelines).toContain("`literal:true` for exact punctuated text");
    expect(guidelines).toContain("fan-out search limits small");
    expect(guidelines).toContain("`settle:true` for tests or probes");
    expect(guidelines).toContain("`timeout` in seconds once");
    expect(guidelines).toContain("batch only independent, bounded work");
    expect(guidelines).toContain("not raw logs or unused intermediate results");
    expect(guidelines).toContain("pass named payloads through top-level `payloads`");
    expect(guidelines).toContain("prefer `pi.edit`/`pi.write`");
    expect(guidelines).toContain("`pi.bash`: no stdin");
    expect(guidelines).toContain("`display.name` and objective `display.description`");
    expect(guidelines).toContain("pairs them with verified outcomes");
    expect(guidelines).toContain("deterministic compaction");
  });

  it.each([true, false])("folds Main limits into six bounded guidelines (fullCodeMode=%s)", fullCodeMode => {
    const state = { bootstrapped: true, config: normalizeFabricConfig({ fullCodeMode }) } as FabricState;
    const allGuidelines = createFabricExecTool(
      state, defaultCodePreviewSettings(), new Map(), (tool) => tool,
    ).promptGuidelines ?? [];
    expect(allGuidelines.at(-1)).toBe(hostGlobalsGuidance(fullCodeMode));
    const visibleGuidelines = allGuidelines.slice(0, -1);
    expect(visibleGuidelines).toHaveLength(6);
    expect(visibleGuidelines.reduce((total, line) => total + line.length, 0)).toBeLessThanOrEqual(2_200);
    const guidance = visibleGuidelines.join("\n");
    expect(guidance).toContain("Main");
    expect(guidance).toContain("10 min");
    expect(guidance).toContain("agents.run/wait");
    expect(guidance).toContain("60 s");
    expect(guidance).toContain("spawned agents keep running");
  });

  it("runs handoff from finalized outer message_end without aborting nested calls", () => {
    const extensionSource = fs.readFileSync(
      path.join(process.cwd(), "src", "extension.ts"),
      "utf8",
    );
    const toolSource = fs.readFileSync(
      path.join(process.cwd(), "src", "fabric-exec-tool.ts"),
      "utf8",
    );
    const start = extensionSource.indexOf('pi.on("tool_result"');
    const end = extensionSource.indexOf('pi.on("tool_execution_end"', start);
    const boundaryHandlers = extensionSource.slice(start, end);

    expect(boundaryHandlers).toContain('pi.on("message_end"');
    expect(boundaryHandlers).toContain("state.runHandoffAtBoundary");
    expect(toolSource).toContain("state.claimHandoff");
  });

  it("disarms the captured task from the agent_settled lifecycle", () => {
    const source = fs.readFileSync(path.join(process.cwd(), "src", "extension.ts"), "utf8");
    const start = source.indexOf('pi.on("agent_settled"');
    const end = source.indexOf('pi.on("tool_call"', start);

    expect(start).toBeGreaterThanOrEqual(0);
    expect(end).toBeGreaterThan(start);
    expect(source.slice(start, end)).toContain("state.prewalk.settleTask");
  });

  it("restores the borrowed Main model when a session starts", () => {
    const source = fs.readFileSync(path.join(process.cwd(), "src", "extension.ts"), "utf8");
    const start = source.indexOf('pi.on("session_start"');
    const end = source.indexOf('pi.on("session_tree"', start);

    expect(start).toBeGreaterThanOrEqual(0);
    expect(end).toBeGreaterThan(start);
    const handler = source.slice(start, end);
    expect(handler).toContain("restoreBorrowedInPlaceMain");
    // Recovery must precede the eager activation that may auto-arm: a restarted
    // process otherwise arms on the executor before Main can snap back.
    expect(handler.indexOf("restoreBorrowedInPlaceMain")).toBeLessThan(
      handler.indexOf("state.ensure(context)"),
    );
  });
});
