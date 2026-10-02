import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import { COMMENT_CUT_REASON, cutsCommentList } from "../core/comment-cut.js";
import { REPLY_TOOL_NAME } from "../core/reply-tool-identity.js";

/** The tool a structured Pi run replies through (smarty-dev#967). */
export const REPLY_TOOL = REPLY_TOOL_NAME;

// smarty-dev#967: a directive actor was told to end its reply with one JSON object, and the host
// scraped that object out of the final text, so 1,528 of 7,801 supervisor replies carried prose
// around it. The reply is now the arguments of one tool call, validated against the run's schema
// at the tool boundary; the worker reads only this file, never the final text.
/** Loaded only by a worker run that asks for it; outside one it adds nothing. */
export default function replyTool(pi: ExtensionAPI): void {
  const schemaFile = process.env.PI_FABRIC_REPLY_SCHEMA_FILE;
  const replyFile = process.env.PI_FABRIC_REPLY_FILE;
  if (!schemaFile || !replyFile) return;
  const parameters = JSON.parse(fs.readFileSync(schemaFile, "utf8")) as Record<string, unknown>;
  let replied = false;
  // smarty-dev#1469: a directive actor never cuts an issue or PR comment list. This hook loads only
  // for directive runs, so Mains and task agents are not affected.
  pi.on("tool_call", (event) => {
    if (event.toolName !== "bash") return undefined;
    const command = (event.input as { command?: unknown }).command;
    return typeof command === "string" && cutsCommentList(command)
      ? { block: true, reason: COMMENT_CUT_REASON }
      : undefined;
  });
  pi.registerTool({
    name: REPLY_TOOL,
    label: "Reply",
    description:
      "Deliver your reply to the Fabric host. Call it exactly once, as your last step. " +
      "Text outside this call is not delivered.",
    parameters: parameters as never,
    async execute(_toolCallId, params) {
      if (replied) throw new Error("You already replied; only the first fabric_reply call is delivered.");
      replied = true;
      const temporary = `${replyFile}.${process.pid}.tmp`;
      const fd = fs.openSync(temporary, "w", 0o600);
      try { fs.writeFileSync(fd, JSON.stringify(params)); fs.fsyncSync(fd); }
      finally { fs.closeSync(fd); }
      fs.renameSync(temporary, replyFile);
      // A terminal status may now refer to this reply. Persist its published name first.
      if (process.platform !== "win32") {
        const synced = new Set<string>();
        for (const parent of [path.resolve(path.dirname(replyFile)), fs.realpathSync(path.dirname(replyFile))]) {
          for (let directory = parent; ; directory = path.dirname(directory)) {
            if (!synced.has(directory)) {
              const fd = fs.openSync(directory, "r");
              try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
              synced.add(directory);
            }
            if (directory === path.dirname(directory)) break;
          }
        }
      }
      return { content: [{ type: "text", text: "Reply delivered." }], details: {}, terminate: true };
    },
  });
}
