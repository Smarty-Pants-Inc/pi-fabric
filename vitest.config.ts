import type { ViteUserConfig as UserConfig } from "vitest/config";
import { isolatedTestTemp, isolateTestFleetEnvironment } from "./scripts/test-temp.js";

// Mutate the parent before Vitest snapshots its environment or forks any workers.
const temp = isolatedTestTemp("pi-fabric-vitest-");
Object.assign(process.env, temp);
const fleet = isolateTestFleetEnvironment();

export default {
  test: {
    environment: "node",
    env: { ...temp, ...fleet },
    setupFiles: ["./tests/fleet-isolation-setup.ts"],
    include: ["tests/**/*.test.ts"],
    maxWorkers: 2,
    // Real worker processes and cold TypeScript compilers share this suite.
    // Behavioral timeouts and performance budgets remain asserted by tests.
    testTimeout: 15_000,
    restoreMocks: true,
  },
} satisfies UserConfig;
