/** Worker-only build boundary: bundle the stateless validator here, never in the Pi extension. */
export { parseStructuredValue, validateAgentResult } from "../agents/result.js";
