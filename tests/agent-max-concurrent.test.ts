import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";

describe("agent maxConcurrent configuration", () => {
  it("defaults to 20 with no config or an empty agents section", () => {
    expect(DEFAULT_FABRIC_CONFIG.agents.maxConcurrent).toBe(20);
    expect(normalizeFabricConfig({}).agents.maxConcurrent).toBe(20);
    expect(normalizeFabricConfig({ agents: {} }).agents.maxConcurrent).toBe(20);
  });

  it.each([1, 4, 32])("preserves an explicit value of %i", maxConcurrent => {
    expect(normalizeFabricConfig({ agents: { maxConcurrent } }).agents.maxConcurrent).toBe(maxConcurrent);
  });

  it.each([[0, 1], [99, 32]])("clamps %i to %i", (configured, expected) => {
    expect(normalizeFabricConfig({ agents: { maxConcurrent: configured } }).agents.maxConcurrent).toBe(expected);
  });

  it("keeps speculation concurrency at 4", () => {
    expect(DEFAULT_FABRIC_CONFIG.speculation.maxConcurrent).toBe(4);
    expect(normalizeFabricConfig({ speculation: {} }).speculation.maxConcurrent).toBe(4);
  });
});
