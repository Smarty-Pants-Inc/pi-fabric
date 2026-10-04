import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { runResidentHostFromConfigPath } from "./host.js";
import { fabricProvenanceSupported } from "../fabric-provenance.js";

const configPath = process.env.PI_FABRIC_RESIDENT_CONFIG;

export default function (pi: ExtensionAPI): void {
  let controller: AbortController | undefined;
  let host: Promise<void> | undefined;

  pi.on("session_start", (_event, ctx) => {
    if (host) return;
    if (!configPath) {
      ctx.shutdown();
      return;
    }
    controller = new AbortController();
    host = runResidentHostFromConfigPath(configPath, controller.signal, ctx.modelRegistry, fabricProvenanceSupported(pi))
      .catch(() => undefined)
      .finally(() => ctx.shutdown());
  });

  // Pi awaits this handler before it exits (a SIGTERM exits right after), so
  // wait for the host's close: it stops the durable workers it owns. Returning
  // early let Pi exit first and orphaned them, still writing (smarty-dev#883).
  pi.on("session_shutdown", async () => {
    controller?.abort();
    await host;
  });
}
