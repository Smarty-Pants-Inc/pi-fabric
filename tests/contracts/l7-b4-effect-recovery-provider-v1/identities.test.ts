import { randomUUID } from "node:crypto";
import { describe } from "vitest";
import { runConformance } from "./conformance.js";
import { createFake } from "./fake.js";
import { createAcceptedInput } from "./fixtures.js";

// Same assertions with independently chosen, non-default observations. No B5 or
// native/B6 claim: this detects accidental coupling to the stand-in constants.
describe("fake distinct harness identities / COMPONENT ONLY", () => runConformance(source => {
  const id = randomUUID();
  const acceptedInput = createAcceptedInput(Date.now, { sessionId: `distinct-session:${id}`,
    originalInputId: `distinct-original:${id}` });
  return createFake(source, { acceptedInput, executionBranchEntryId: `distinct-native-anchor:${id}`,
    pin: `distinct-generation:${id}` });
}));
