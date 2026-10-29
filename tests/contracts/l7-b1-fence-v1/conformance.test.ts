import { createTestOnlyFakeHarness, runConformance } from "./index.js";

runConformance({ name: "test-only stateful observed-B fake", create: createTestOnlyFakeHarness });
