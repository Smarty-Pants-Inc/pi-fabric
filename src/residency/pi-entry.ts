import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { runResidentHostFromConfigPath } from "./host.js";

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
    host = runResidentHostFromConfigPath(configPath, controller.signal, ctx.modelRegistry)
      .catch(() => undefined)
      .finally(() => ctx.shutdown());
  });

  pi.on("session_shutdown", () => controller?.abort());
}
