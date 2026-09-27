import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricShellJobStore } from "../core/shell-jobs.js";
import { installFabricEscapeHalt } from "./escape-halt.js";

const CTRL_B = "\x02";
const CTRL_K = "\x0b";
const CTRL_B_CHORD_MS = 1_000;

interface ShellHangKeyOptions {
  enabled: () => boolean;
  ownsInput: () => boolean;
  jobs: () => FabricShellJobStore | undefined;
}

/** Spill or kill a nested shell that is still waiting, without a jobs API. */
export function installFabricShellHangKeys(
  context: ExtensionContext,
  options: ShellHangKeyOptions,
): () => void {
  if (context.mode !== "tui" || typeof context.ui.onTerminalInput !== "function") return () => {};
  const stopMonitors = installFabricEscapeHalt(context, {
    enabled: options.enabled, ownsInput: options.ownsInput, halted: () => false,
    halt: () => {
      let count = 0;
      for (const job of options.jobs()?.live() ?? []) if (job.options.monitor && job.stop("Main interrupted")) count++;
      return count;
    },
  });
  let chordArmedAt = 0;
  const unsubscribe = context.ui.onTerminalInput((data) => {
    if (options.ownsInput() || !options.enabled()) {
      chordArmedAt = 0;
      return undefined;
    }
    const jobs = options.jobs();
    if (!jobs || jobs.waiting().length === 0) {
      chordArmedAt = 0;
      return undefined;
    }
    if (data === CTRL_B) {
      const now = Date.now();
      if (chordArmedAt > 0 && now - chordArmedAt <= CTRL_B_CHORD_MS) {
        chordArmedAt = 0;
        const count = jobs.spillWaiting();
        if (count > 0) {
          context.ui.notify(
            count === 1
              ? "Fabric: shell still running · output spilled to a live log"
              : `Fabric: ${count} shells still running · output spilled to live logs`,
            "info",
          );
        }
        return { consume: true };
      }
      chordArmedAt = now;
      return { consume: true };
    }
    chordArmedAt = 0;
    if (data === CTRL_K) {
      const count = jobs.killWaiting();
      if (count > 0) {
        context.ui.notify(
          count === 1 ? "Fabric: killed waiting shell" : `Fabric: killed ${count} waiting shells`,
          "warning",
        );
      }
      return { consume: true };
    }
    return undefined;
  });
  return () => { stopMonitors(); unsubscribe(); };
}
