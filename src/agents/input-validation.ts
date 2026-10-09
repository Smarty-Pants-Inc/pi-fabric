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
    if (typeof entry !== "string" || entry.includes("\0") || !path.isAbsolute(entry) || Buffer.byteLength(entry, "utf8") > MAX_AGENT_REQUIRED_INPUT_BYTES) {
      throw new AgentInputError("requires", `Invalid agent requires[${index}]: expected an absolute path without NUL, at most ${MAX_AGENT_REQUIRED_INPUT_BYTES} UTF-8 bytes`);
    }
  }
  return [...value] as string[];
};

/** Only the selected local execution host may check existence; remote inputs are target-local. */
export const assertAgentRequiredInputsExist = (requires: readonly string[] | undefined, exists: (file: string) => boolean): void => {
  for (const file of requires ?? []) {
    if (!exists(file)) throw new AgentInputError("requires", `Missing required agent input: ${file}; no worker started`);
  }
};
