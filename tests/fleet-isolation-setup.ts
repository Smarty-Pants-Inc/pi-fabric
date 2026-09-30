import { isolateTestFleetEnvironment } from "../scripts/test-temp.js";

// Vitest runs setup before each test file's static imports. Config already scrubbed
// the parent; give this file (and children spawned by it) its own private state.
// Tests can still opt into specific behavior with vi.stubEnv after this boundary.
isolateTestFleetEnvironment();
