import { describe } from "vitest";
import { runConformance } from "./conformance.js";
import { createRealB5 } from "./real-b5.js";
describe("real B4 + real B5 / probe:native-Pi", () => runConformance(createRealB5));
