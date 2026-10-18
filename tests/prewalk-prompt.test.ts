import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
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

    expect(toolSource).not.toContain("promptGuidelines:");
  });

  it.each([true, false])("publishes only a short callable contract, not workflow guidelines (fullCodeMode=%s)", fullCodeMode => {
    const state = { bootstrapped: true, config: normalizeFabricConfig({ fullCodeMode }) } as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), value => value);
    expect(tool).not.toHaveProperty("promptGuidelines");
    const contract = [tool.description, tool.parameters.properties.code.description, tool.parameters.properties.payloads.description].join("");
    expect(contract.length).toBeLessThanOrEqual(1_000);
    expect(contract).not.toMatch(/acceptance ledger|batch independent|Search before reading|display\.name|10 min|60 s|Jev supplies/);
    expect(contract).toContain("TypeScript function body");
    expect(contract).toContain("payloads are literal");
    expect(contract).toContain("QuickJS lacks host globals");
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
