// Public diagnostic seam; colocated with agent-dir so the eager graph gains no facade-only chunk.
// scripts/build.mjs resolves this facade to its implementation.
export { configureFabricDiagnostics, fabricWarn, fabricError, fabricInfo, fabricDebug } from "./agent-dir.js";
