import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { runResidentHostFromConfigPath } from "./host.js";

const configPath = process.env.PI_FABRIC_RESIDENT_CONFIG;

export default function (pi: ExtensionAPI): void {
  let controller: AbortController | undefined;
  let host: Promise<void> | undefined;
  let contextNow: ExtensionContext | undefined;

  pi.on("session_start", (_event, ctx) => {
    contextNow = ctx;
    if (host) return;
    if (!configPath) {
      ctx.shutdown();
      return;
    }
    controller = new AbortController();
    // The headless Pi session is not the originating Main session in config.json.
    host = runResidentHostFromConfigPath(configPath, controller.signal, ctx.modelRegistry, ctx.sessionManager.getSessionId())
      .catch(() => undefined)
      .finally(() => {
        // Natural idle completion shuts down the live session. Teardown already
        // owns shutdown during reload/replacement; never call its captured ctx.
        contextNow?.shutdown();
      });
  });

  // Pi awaits this handler before it exits (a SIGTERM exits right after), so
  // wait for the host's close: it stops the durable workers it owns. Returning
  // early let Pi exit first and orphaned them, still writing (smarty-dev#883).
  pi.on("session_shutdown", async () => {
    contextNow = undefined;
    controller?.abort();
    await host;
  });
}
