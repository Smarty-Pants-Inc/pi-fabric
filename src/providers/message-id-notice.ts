import type { FabricInvocationContext } from "../protocol.js";

export interface OutgoingMessageNotice { text: string; notice?: string }

// Only the cheap prefilter is reachable from providers. The parser/history walk
// is first-use code, never loaded by registration or idle lifecycle hooks.
const possibleIdentifier = /(?:#issuecomment-\d{9,10}|(?<![\w-])(?:session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|01a0[0-9a-f]{4}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|(?:actor:|run:)?[0-9a-f]{32}|comment[ \t:=#-]*\d{9,10}|pid[ \t:=#-]*\d{1,10}))(?![\w-])|(?<!`)`[0-9a-f]{7,40}`(?!`)|\b(?:sha|commit|head|base|revision|rev)(?:["']?[ \t]*[:=][ \t]*["']?|[ \t]+)[0-9a-f]{7,40}(?![\w-])/i;
const failed = "unverified ids: check failed";

/** Advisory only: a failed checker must not turn a valid send into a refusal. */
export async function outgoingMessageNotice(
  text: string,
  context: FabricInvocationContext,
  senderId?: string,
): Promise<OutgoingMessageNotice> {
  let notice: string | undefined;
  try {
    if (text.length > 256 * 1024) notice = failed;
    else if (!possibleIdentifier.test(text)) return { text };
    else {
      const { unverifiedMessageIds } = await import("../coordination/unverified-ids.js");
      const ids = unverifiedMessageIds(text, context.extensionContext?.sessionManager, senderId);
      if (ids.length) notice = `unverified ids: ${ids.join(", ")}`;
    }
  } catch {
    notice = failed;
  }
  return notice ? { text: `${text}\n\n${notice}`, notice } : { text };
}
