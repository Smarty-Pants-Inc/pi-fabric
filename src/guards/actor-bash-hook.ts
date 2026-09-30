import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import { actorBashTimeout } from "./actor-bash-timeout.js";

// smarty-dev#2184 (review finding 4): the Fabric extension's tool_call hook is the only other place
// that sets an actor's bash timeout, and a native-tool actor (extensions: false) never loads it. The
// worker loads this timeout-only hook with `-e` for every Pi actor run; `-e` still loads under
// --no-extensions. It adds no tool and no authority, and it must not import the Fabric graph.
// When Fabric is loaded too, both hooks run; each sets a timeout only when none is set, so the
// second one does nothing. The worker loads this one after Fabric's `-e`, so Fabric's foreground-
// wait guard still judges the caller's own timeout.
export default function actorBashHook(pi: ExtensionAPI): void {
  pi.on("tool_call", (event) => {
    if (event.toolName !== "bash") return undefined;
    const input = event.input as { timeout?: unknown };
    const injected = actorBashTimeout(process.env, input.timeout);
    if (injected !== undefined) input.timeout = injected;
    return undefined;
  });
}
