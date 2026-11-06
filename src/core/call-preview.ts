// Keep the public key binding here; the utility owner consumes it and owns the
// unchanged headline implementation alongside the transcript runtime.
export const HEADLINE_ARG_KEYS = [
  "task", "path", "query", "message", "search", "pattern", "command", "text",
  "prompt", "question", "input", "content", "expression", "url", "topic",
  "key", "filter", "name", "q",
] as const;

export { headlineArg } from "../util.js";
