// Test-only declarative host extension; installed application deployment owns its real bootstrap.
import fs from "node:fs";
import { pathToFileURL } from "node:url";
import { isNativeSdkUnconfigured } from "../helpers/native-sdk-no-model.ts";
const file = process.env.FABRIC_NATIVE_OWNER_PINS;
if (!file) throw new Error("Native owner pins required");
const pins = JSON.parse(fs.readFileSync(file, "utf8"));
const { registerNativeActorHandler } = await import(pathToFileURL(pins.nativeApi).href);
export default function(pi) {
  const unregister = registerNativeActorHandler(pins.binding, async () => {
    throw new Error("Main native declaration must never execute");
  });
  pi.on("session_start", (_event, ctx) => {
    if (`session:${ctx.sessionManager.getSessionId()}` !== pins.binding.rootId ||
        ctx.sessionManager.getSessionFile() !== pins.sessionFile || !isNativeSdkUnconfigured(ctx.model, pins.sdkDirectory)) {
      ctx.shutdown();
    }
  });
  pi.on("session_shutdown", () => unregister());
}
