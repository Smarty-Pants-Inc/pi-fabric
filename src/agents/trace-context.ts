// Dependency-free: the standalone worker validates its argument with this module.

/** W3C Trace Context headers, exported to a child as TRACEPARENT and TRACESTATE. */
export interface FabricTraceContextV1 {
  traceparent: string;
  tracestate?: string;
}

/** The launch a trace context is requested for. */
export interface FabricChildTraceContextLaunch {
  /** The outer tool call (normally `fabric_exec`) that started the child; absent for actor turns. */
  parentToolCallId?: string;
  /** The nested `agents.*` call, as carried by its audit and proxied `tool_result`. */
  nestedToolCallId?: string;
  /** Absent when a durable resident host mints the id after the request leaves the session. */
  runId?: string;
  runner: string;
  name: string;
}

/**
 * Host-local synchronous request emitted before each child launch and
 * relaunch. The first valid `reply` made before the listener's first await
 * wins; with none, the child keeps the environment it would inherit.
 */
export interface FabricChildTraceContextRequestV1 extends FabricChildTraceContextLaunch {
  version: 1;
  reply: (context: FabricTraceContextV1) => void;
}

const MAX_TRACESTATE_CHARS = 512;
// version-traceid-parentid-flags, lowercase hex; version ff and all-zero ids are invalid.
const TRACEPARENT = /^(?!ff)[0-9a-f]{2}-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}$/;
const TRACESTATE = /^[\x20-\x7e]+$/;

/**
 * Only the two headers, in their wire grammar: a reply reaches a child's
 * environment, so it can never name another variable or span a line.
 */
export const readTraceContext = (value: unknown): FabricTraceContextV1 | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const { traceparent, tracestate } = value as Record<string, unknown>;
  if (typeof traceparent !== "string" || !TRACEPARENT.test(traceparent)) return undefined;
  if (tracestate === undefined) return { traceparent };
  if (typeof tracestate !== "string" || tracestate.length > MAX_TRACESTATE_CHARS || !TRACESTATE.test(tracestate)) {
    return undefined;
  }
  return { traceparent, tracestate };
};
