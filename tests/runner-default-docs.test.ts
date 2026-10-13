import fs from "node:fs";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

// PR #864 review P2: this fork keeps agents.runner "pi" as the default; pi-durable is opt-in.
// Copying the documented full reference or reading the action schema must not switch runners.
describe("default runner documentation", () => {
  it("the configuration full reference uses the actual default runner", () => {
    const docs = fs.readFileSync(new URL("../docs/configuration.md", import.meta.url), "utf8");
    const runners = [...docs.matchAll(/^\s*"runner": "([^"]+)",$/gm)].map(match => match[1]);
    expect(DEFAULT_FABRIC_CONFIG.agents.runner).toBe("pi");
    expect(runners).toContain(DEFAULT_FABRIC_CONFIG.agents.runner);
    expect(runners).not.toContain("pi-durable");
  });

  it("agent action schemas name pi as the default and pi-durable as opt-in", () => {
    const descriptions = AGENTS_ACTION_DESCRIPTORS.flatMap(descriptor => {
      const runner = (descriptor.inputSchema as { properties?: Record<string, { description?: string }> }).properties?.runner;
      return runner?.description ? [runner.description] : [];
    });
    expect(descriptions.length).toBeGreaterThan(0);
    for (const description of descriptions) {
      expect(description).not.toMatch(/pi-durable \(default\)/);
      if (/default/.test(description) && /pi-durable/.test(description)) expect(description).toMatch(/\bpi \(default\)/);
    }
  });
});
