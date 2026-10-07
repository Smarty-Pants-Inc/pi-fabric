import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { BUILT_IN_RUNNER_IDS, RUNNER_ID_PATTERN } from "../src/agents/runner-registry.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = (file: string): string => fs.readFileSync(path.join(root, file), "utf8");

// Runtime (non-type) static edges: `import x from`, `import "x"`, `export ... from`.
const RUNTIME_EDGE = /^\s*(?:import|export)\s+(?!type\b)(?:[^"';]*?\sfrom\s+)?["'](\.[^"']+)["']/gm;

const staticClosure = (entry: string): Set<string> => {
  const seen = new Set<string>();
  const stack = [entry];
  while (stack.length > 0) {
    const file = stack.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    for (const match of source(file).matchAll(RUNTIME_EDGE)) {
      // Posix joins keep closure keys stable on Windows.
      const target = path.posix.join(path.posix.dirname(file), match[1]!).replace(/\.js$/, ".ts");
      if (fs.existsSync(path.join(root, target))) stack.push(target);
    }
  }
  return seen;
};

describe("runner registry stays off the startup graph", () => {
  it("is not statically reachable from the extension entry", () => {
    const closure = staticClosure("src/index.ts");
    expect(closure.has("src/config.ts")).toBe(true);
    expect(closure.size).toBeGreaterThan(40);
    for (const lazy of [
      "src/runners.ts",
      "src/durable.ts",
      "src/agents/runner-registry.ts",
      "src/agents/runner-protocol.ts",
      "src/agents/hosted-run.ts",
    ]) {
      expect(closure.has(lazy), lazy).toBe(false);
    }
  });

  it("keeps inlined runner id patterns identical to the canonical one", () => {
    for (const file of ["src/config.ts", "src/lifecycle/types.ts", "src/topology/participant-directory.ts"]) {
      const match = source(file).match(/const RUNNER_ID_PATTERN = (\/[^\n]*\/);/);
      expect(match?.[1], file).toBe(RUNNER_ID_PATTERN.toString());
    }
    const descriptor = source("src/providers/agents-actions.ts").match(/const RUNNER_ID_SOURCE = "([^"]+)";/);
    expect(descriptor?.[1]).toBe(RUNNER_ID_PATTERN.source);
    expect([...BUILT_IN_RUNNER_IDS]).toEqual(["pi", "pi-durable", "claude", "veda"]);
  });
});
