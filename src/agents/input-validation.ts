import path from "node:path";
import { Buffer } from "node:buffer";

import { AgentInputError, MAX_AGENT_REQUIRED_INPUTS, MAX_AGENT_REQUIRED_INPUT_BYTES } from "../host-compatibility.js";
export { AgentInputError } from "../host-compatibility.js";

/** Validate and snapshot before model preparation, queuing, or launcher invocation. */
export const normalizeAgentRequires = (value: unknown): string[] | undefined => {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length > MAX_AGENT_REQUIRED_INPUTS) {
    throw new AgentInputError("requires", `Invalid agent requires: expected at most ${MAX_AGENT_REQUIRED_INPUTS} absolute paths`);
  }
  for (const [index, entry] of value.entries()) {
    const invalid = (rule: string): never => { throw new AgentInputError("requires", `Invalid agent requires[${index}]: ${rule}`); };
    if (typeof entry !== "string") return invalid("expected a string absolute path");
    if (/[\u0000-\u001f\u007f-\u009f\u2028\u2029]/u.test(entry)) invalid("must not contain line breaks or control characters");
    if (!path.isAbsolute(entry)) invalid("expected an absolute path");
    if (entry.split(/[\\/]/u).some(segment => segment === "." || segment === "..")) invalid('must not contain "." or ".." path segments');
    if (Buffer.byteLength(entry, "utf8") > MAX_AGENT_REQUIRED_INPUT_BYTES) invalid(`must occupy at most ${MAX_AGENT_REQUIRED_INPUT_BYTES} UTF-8 bytes`);
  }
  return [...value] as string[];
};

/** Only the selected local execution host may check existence; remote inputs are target-local. */
export const assertAgentRequiredInputsExist = (requires: readonly string[] | undefined, exists: (file: string) => boolean): void => {
  for (const [index, file] of (requires ?? []).entries()) {
    if (!exists(file)) throw new AgentInputError("requires", `Missing required agent input requires[${index}]: ${file}; path must exist on the selected host; no worker started`);
  }
};
