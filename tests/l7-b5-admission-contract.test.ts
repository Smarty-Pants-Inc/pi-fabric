// Keep: trust boundary / acknowledged-work data safety. Same assertions on real B5 and fake.
import { describe } from "vitest";
import { createTestOnlyFakeHarness, runConformance } from "./contracts/l7-b5-admission-v1/index.js";
import { createRealB5Harness } from "./contracts/l7-b5-admission-v1/real.js";

// REQUIRED real durability fails closed on Windows (directory barriers unavailable).
// The portable behavioral fake still runs there; this is NOT Windows durability proof.
describe.skipIf(process.platform === "win32")("real B5 + filesystem", () => {
  runConformance({ name: "real B5", create: createRealB5Harness });
});
runConformance({ name: "TEST-ONLY in-memory fake", create: createTestOnlyFakeHarness });
