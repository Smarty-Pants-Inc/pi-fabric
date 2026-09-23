import { defineConfig } from "vitest/config";
import { isolatedTestTemp } from "./scripts/test-temp.js";

export default defineConfig({
  test: {
    environment: "node",
    env: isolatedTestTemp("pi-fabric-vitest-"),
    include: ["tests/**/*.test.ts"],
    maxWorkers: 2,
    // Real worker processes and cold TypeScript compilers share this suite.
    // Behavioral timeouts and performance budgets remain asserted by tests.
    testTimeout: 15_000,
    restoreMocks: true,
  },
});
