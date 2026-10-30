import { describe } from "vitest";
import { runConformance } from "./conformance.js";
import { createFake } from "./fake.js";
import { createReal } from "./real.js";

// Select a target to retain a separate terminal log; default runs both and is
// intentionally RED while the unmodified real implementation violates R3/B3.
// No expected-failure conversion, skip, production adapter or test-only authority.
if (process.env.B4_CONFORMANCE_TARGET !== "real") describe("fake / COMPONENT ONLY", () => runConformance(createFake));
if (process.env.B4_CONFORMANCE_TARGET !== "fake") describe("real B4 / boundary probes", () => runConformance(createReal));
