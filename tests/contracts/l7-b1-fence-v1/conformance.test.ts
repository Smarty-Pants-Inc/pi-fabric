import { createTestOnlyFakeHarness, runConformance } from "./index.js";

await runConformance({ name: "test-only stateful observed-B fake", create: createTestOnlyFakeHarness });
