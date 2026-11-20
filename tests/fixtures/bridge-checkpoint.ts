import fs from "node:fs";
import { BRIDGE_CURSOR_CHECKPOINT_MS } from "../../src/mesh/bridge.js";

type Cursor = Record<"toRemote" | "toLocal", { after: number; offset?: number; mark: number }>;
/** Delivery is immediate; its dirty cursor is durable within the checkpoint interval.
 * Use real timers only. Fake-timer fixtures should advance their own checkpoint timer. */
export async function waitForBridgeCheckpoint(cursorPath: string, consumed: (cursor: Cursor) => boolean): Promise<void> {
  let expired = false;
  const timer = setTimeout(() => { expired = true; }, BRIDGE_CURSOR_CHECKPOINT_MS + 2_000);
  try {
    while (!consumed(JSON.parse(fs.readFileSync(cursorPath, "utf8")) as Cursor)) {
      if (expired) throw new Error(`Bridge checkpoint did not become durable within interval: ${cursorPath}`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  } finally { clearTimeout(timer); }
}
