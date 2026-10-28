import { open } from "node:fs/promises";
import path from "node:path";

/** These names describe real runtime boundaries, not model-authored markers.
 * native-result-persisted is owned by Pi's post-append serializer (B6), and
 * receiver-accepted by the durable receiver/native-carrier owner (B5). Calling
 * either from tool_result/message_end or a sender mock proves neither window. */
export type EffectFailpointName =
  | "intent-durable"
  | "file-temp-synced"
  | "file-renamed"
  | "destination-committed"
  | "result-durable"
  | "native-result-persisted"
  | "receiver-accepted";

export interface EffectAdapterControls {
  assertLive(): void | Promise<void>;
  failpoint?(point: EffectFailpointName, details?: Record<string, unknown>): void | Promise<void>;
}

export interface EffectAttemptIdentity {
  logicalSessionId: string;
  podUID: string;
  bootId: string;
  ownerIncarnation: string;
}

export interface EffectFailpointReceipt extends EffectAttemptIdentity {
  format: "fabric.effect-failpoint.v1";
  point: EffectFailpointName;
  key: string;
  toolCallId: string;
  at: string;
  details: Record<string, unknown>;
}

/** Default-off, host-injected fault instrumentation. The caller must have already
 * received the boundary's actual durability receipt. A selected pause appends and
 * fsyncs its raw attempt before notifying the external fault executor. No signal
 * is sent, process killed, success fabricated, or record deduplicated here.
 * The provisioner owns the already-existing attempt directory. */
export const createEffectFailpoints = (options: {
  attemptFile: string;
  identity: EffectAttemptIdentity;
  points?: readonly EffectFailpointName[];
  onReached?: (receipt: Readonly<EffectFailpointReceipt>) => void | Promise<void>;
  /** A test/fault executor may supply a controlled resume; absent it, stay paused. */
  pause?: (receipt: Readonly<EffectFailpointReceipt>) => Promise<void>;
}): ((point: EffectFailpointName, details?: Record<string, unknown>) => Promise<void>) => {
  if (!path.isAbsolute(options.attemptFile)) throw new Error("Effect attempts require an absolute provisioned path");
  const selected = new Set(options.points ?? []);
  const identity = Object.freeze({ ...options.identity });
  let appendTail = Promise.resolve();
  return async (point, details = {}) => {
    if (!selected.has(point)) return;
    if (typeof details.key !== "string" || !/^[a-f0-9]{64}$/.test(details.key) ||
        typeof details.toolCallId !== "string" || !details.toolCallId) {
      throw new Error("Effect failpoint requires the original effect key and physical-call alias");
    }
    const receipt: EffectFailpointReceipt = {
      ...identity,
      format: "fabric.effect-failpoint.v1",
      point,
      key: details.key,
      toolCallId: details.toolCallId,
      at: new Date().toISOString(),
      details: structuredClone(details),
    };
    // Append sequencing covers concurrent effects without making a paused effect
    // own the append lock. Partial writes/errors remain evidence, never success.
    const append = appendTail.then(async () => {
      const file = await open(options.attemptFile, "a", 0o600);
      try { await file.writeFile(`${JSON.stringify(receipt)}\n`, "utf8"); await file.sync(); }
      finally { await file.close(); }
      // A first creation must also survive the directory-entry loss window.
      const directory = await open(path.dirname(options.attemptFile), "r");
      try { await directory.sync(); } finally { await directory.close(); }
    });
    appendTail = append.catch(() => undefined);
    await append;
    await options.onReached?.(Object.freeze(receipt));
    if (options.pause) await options.pause(receipt);
    else await new Promise<void>(() => { /* external executor kills this owned process */ });
  };
};
