import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import { applyRunBashDefaults } from "./actor-bash-timeout.js";

// smarty-dev#2184 (review finding 4): the Fabric extension's tool_call hook is the only other place
// that sets a run's bash defaults, and a native-tool run (extensions: false) never loads it. The
// worker loads this defaults-only hook with `-e` for every Pi run it launches (#6137: task agents
// too); `-e` still loads under --no-extensions. It adds no tool and no authority, and it must not
// import the Fabric graph. When Fabric is loaded too, both hooks run; each acts only when no timeout
// is set and the input lacks the hook-set private wrapping metadata, so the second does nothing.
// A marker in caller-supplied command text is never trusted. The worker loads
// this one after Fabric's `-e`, so Fabric's foreground-wait guard still judges the caller's command.
export default function actorBashHook(pi: ExtensionAPI): void {
  pi.on("tool_call", (event) => {
    if (event.toolName !== "bash") return undefined;
    applyRunBashDefaults(process.env, event.input as Parameters<typeof applyRunBashDefaults>[1]);
    return undefined;
  });
}
