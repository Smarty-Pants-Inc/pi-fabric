import fs from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** The tool a structured Pi run replies through (smarty-dev#967). */
export const REPLY_TOOL = "fabric_reply";

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
      fs.writeFileSync(temporary, JSON.stringify(params), { mode: 0o600 });
      fs.renameSync(temporary, replyFile);
      return { content: [{ type: "text", text: "Reply delivered." }], details: {}, terminate: true };
    },
  });
}
