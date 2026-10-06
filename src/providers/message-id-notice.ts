import { fabricWarn } from "../core/diagnostics.js";
import type { FabricInvocationContext } from "../protocol.js";

export interface OutgoingMessageNotice { text: string; notice?: string }

// Only the cheap prefilter is reachable from providers. The parser/history walk
// is first-use code, never loaded by registration or idle lifecycle hooks.
const possibleIssueReference = /(?<![\w./-])(?:https:\/\/github\.com\/[a-z0-9][a-z0-9-]*\/[a-z0-9_.-]+\/(?:issues|pull)\/\d+|[a-z0-9][a-z0-9-]*\/[a-z0-9_.-]+#\d+|(?![cf]#)[a-z0-9][a-z0-9_.-]*#\d{3,}|#\d{3,})(?![\w-])/i;
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
    else if (!possibleIdentifier.test(text) && !((text.includes("#") || text.includes("/")) && possibleIssueReference.test(text))) return { text };
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

// These exact host-generated refusals are pre-admission: MeshStore.publish
// checks before reserving/persisting its event; ActorManager validates before
// enqueueing; MainAgentController checks quotas before mutating its held queue.
// A remote owner's explicit rejection preserves these same error messages.
// Do NOT match timeouts, stalled-but-accepted queues or generic delivery errors:
// their outcome may be unknown, so retrying could duplicate a delivered message.
const admissionLimit = /^(?:Mesh event exceeds \d+ bytes|Actor message exceeds \d+ bytes after reserving the Fabric envelope|Main's followUp queue is full \([^\n]*\); Main is busy and reads followUps only at its next tool boundary\. Wait, or send a short steer\.)$/;

// Process-local diagnostic count, shared by all routes. No persistent side store.
let recipientMarkerOmissions = 0;

/** Drop only the optional marker on an explicit pre-admission limit refusal. */
export async function deliverWithMessageNotice<T>(
  original: string,
  checked: OutgoingMessageNotice,
  deliver: (text: string) => T | Promise<T>,
  route: string,
): Promise<T> {
  try {
    return await deliver(checked.text);
  } catch (error) {
    if (!checked.notice || checked.text === original || !(error instanceof Error) || !admissionLimit.test(error.message)) throw error;
    // At most one unmarked attempt, with every original limit still in force.
    const result = await deliver(original);
    checked.notice += " (recipient marker omitted: message at the route size limit)";
    // Use Fabric's existing stderr diagnostic path, one structured line for each
    // successfully delivered unmarked message. Failed original sends do not count.
    fabricWarn(`[pi-fabric] ${JSON.stringify({
      event: "recipient-marker-omitted", route, count: ++recipientMarkerOmissions,
    })}`);
    return result;
  }
}
