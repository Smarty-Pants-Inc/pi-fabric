import type { ViteUserConfig as UserConfig } from "vitest/config";
import { isolatedTestTemp, isolateTestFleetEnvironment } from "./scripts/test-temp.js";

// Mutate the parent before Vitest snapshots its environment or forks any workers.
const temp = isolatedTestTemp("pi-fabric-vitest-");
Object.assign(process.env, temp);
const fleet = isolateTestFleetEnvironment();

const sharedTests = {
  environment: "node",
  env: { ...temp, ...fleet },
  setupFiles: ["./tests/fleet-isolation-setup.ts"],
  include: ["tests/**/*.test.ts"],
  // Real worker processes and cold TypeScript compilers share this suite.
  // Behavioral timeouts and performance budgets remain asserted by tests.
  testTimeout: 15_000,
  restoreMocks: true,
} satisfies NonNullable<UserConfig["test"]>;

export default {
  test: {
    ...sharedTests,
    maxWorkers: 2,
    // Expose GC only for retention tests: application worker_threads reject this flag.
    // Explicit shared options avoid extends merging the broad include into the GC project.
    projects: [
      {
        test: { ...sharedTests, name: "default", exclude: ["tests/session-entry-retention.test.ts"] },
      },
      {
        test: {
          ...sharedTests,
          name: "history-gc", include: ["tests/session-entry-retention.test.ts"],
          pool: "forks", execArgv: ["--expose-gc"],
        },
      },
    ],
  },
} satisfies UserConfig;
