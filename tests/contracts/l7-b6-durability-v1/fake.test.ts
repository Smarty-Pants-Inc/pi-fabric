import { runConformance } from "./conformance.js";
import { createTestOnlyFakeHarness } from "./fake.js";

runConformance({ name: "TEST ONLY in-memory behavioral fake", create: createTestOnlyFakeHarness });
