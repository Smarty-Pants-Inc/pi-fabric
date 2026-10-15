import { FABRIC_CHILD_TRACE_CONTEXT_EVENT } from "../protocol.js";
import {
  readTraceContext,
  type FabricChildTraceContextLaunch,
  type FabricChildTraceContextRequestV1,
  type FabricTraceContextV1,
} from "./trace-context.js";

/** Emits the request and returns the first valid synchronous reply. */
export const requestChildTraceContext = (
  emit: (event: string, value: unknown) => void,
  launch: FabricChildTraceContextLaunch,
): FabricTraceContextV1 | undefined => {
  let context: FabricTraceContextV1 | undefined;
  let closed = false;
  const request: FabricChildTraceContextRequestV1 = {
    version: 1,
    ...launch,
    reply(value) {
      if (closed) throw new Error("Pi Fabric child trace context must be replied synchronously; the child already launched");
      const valid = readTraceContext(value);
      if (!valid) throw new Error("Invalid Pi Fabric child trace context");
      context ??= valid;
    },
  };
  try {
    emit(FABRIC_CHILD_TRACE_CONTEXT_EVENT, request);
  } catch {
    // Tracing is observability: a failing listener never blocks a launch.
  }
  closed = true;
  return context;
};
