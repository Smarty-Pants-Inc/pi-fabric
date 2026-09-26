import fs from "node:fs";

/** The tool a structured Pi run replies through (smarty-dev#967). */
export const REPLY_TOOL_NAME = "fabric_reply";

// review/astra F1 on #85: the worker's run-local reply tool is host-owned, like fabric_exec, so
// Fabric never captures it, and so never hides it (full-code and Schema enforce modes otherwise keep
// only fabric_exec visible). It is recognized only as the tool of that name registered from the
// exact hook file the worker loaded for this run.
export const isRunReplyTool = (
  name: string,
  sourcePath: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): boolean => {
  const hook = env.PI_FABRIC_REPLY_HOOK;
  if (name !== REPLY_TOOL_NAME || !hook || !env.PI_FABRIC_REPLY_FILE || !sourcePath) return false;
  try {
    return fs.realpathSync(sourcePath) === hook;
  } catch {
    return false;
  }
};
