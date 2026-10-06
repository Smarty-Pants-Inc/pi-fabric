import fs from "node:fs";

/** The tool a structured Pi run replies through (smarty-dev#967). */
export const REPLY_TOOL_NAME = "fabric_reply";

// review/astra F1 on #85: the worker's run-local reply tool is host-owned, like fabric_exec. Fabric
// never captures it, and so never hides it (full-code and Schema enforce modes otherwise keep only
// fabric_exec visible), and its native call gate neither authorizes it as a foreign top-level tool
// nor asks approval for it. It is recognized only as the tool of that name registered from the
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

export const ownsRunReplyTool = (
  tools: ReadonlyArray<{ name: string; sourceInfo?: { path: string } }>,
  env: NodeJS.ProcessEnv = process.env,
): boolean => tools.some((tool) => isRunReplyTool(tool.name, tool.sourceInfo?.path, env));
