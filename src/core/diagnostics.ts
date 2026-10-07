// Public diagnostic seam; colocated with agent-dir so the built eager graph does not
// gain a facade-only chunk. scripts/build.mjs resolves this facade to its implementation.
export { configureFabricDiagnostics, fabricWarn } from "./agent-dir.js";
