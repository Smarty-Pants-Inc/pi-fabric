import { parentPort, workerData } from "node:worker_threads";
import type { FabricInvocationContext, FabricScope } from "../protocol.js";
import { MemoryProvider, type MemoryProviderContext } from "../providers/memory-provider.js";
import type { LiveSessionBranch } from "./lineage.js";

interface MemoryWorkerRequest {
  id: number;
  action: string;
  args: Record<string, unknown>;
  branch?: LiveSessionBranch;
  scope?: FabricScope;
}

export type MemoryWorkerReply =
  | { id: number; type: "progress"; text: string }
  | { id: number; type: "result"; value: unknown }
  | { id: number; type: "error"; name: string; message: string };

const port = parentPort;
if (port) {
  const context = workerData as MemoryProviderContext;
  // One engine per worker preserves bounded continuation/expansion caches across queued calls.
  const provider = new MemoryProvider(context);
  port.on("message", async (request: MemoryWorkerRequest) => {
    if (request.branch) context.getLiveBranch = () => request.branch!;
    else delete context.getLiveBranch;
    try {
      const value = await provider.invoke(request.action, request.args, {
        cwd: context.cwd,
        signal: undefined,
        parentToolCallId: "memory-worker",
        nestedToolCallId: String(request.id),
        extensionContext: {} as FabricInvocationContext["extensionContext"],
        ...(request.scope ? { scope: request.scope } : {}),
        update: text => port.postMessage({ id: request.id, type: "progress", text } satisfies MemoryWorkerReply),
      });
      port.postMessage({ id: request.id, type: "result", value } satisfies MemoryWorkerReply);
    } catch (error) {
      port.postMessage({
        id: request.id, type: "error",
        name: error instanceof Error ? error.name : "Error",
        message: error instanceof Error ? error.message : String(error),
      } satisfies MemoryWorkerReply);
    }
  });
}
