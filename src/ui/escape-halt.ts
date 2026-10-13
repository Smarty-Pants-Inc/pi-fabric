import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

interface EscapeHaltOptions {
  enabled: () => boolean;
  ownsInput: () => boolean;
  halted: () => boolean;
  halt: () => number;
}

/** Observe native Escape without treating navigation inside Fabric UI as an interrupt. */
export function installFabricEscapeHalt(
  context: ExtensionContext,
  options: EscapeHaltOptions,
): () => void {
  if (context.mode !== "tui" || typeof context.ui.onTerminalInput !== "function") return () => {};
  let timer: ReturnType<typeof setTimeout> | undefined;
  const cancel = (): void => {
    if (timer) clearTimeout(timer);
    timer = undefined;
  };
  const unsubscribe = context.ui.onTerminalInput((data) => {
    cancel();
    // Check before the overlay handles Escape and closes: a delayed check alone
    // would mistake that navigation key for a native Main interrupt.
    if (data !== "\x1b" || options.ownsInput() || !options.enabled()) return undefined;
    timer = setTimeout(() => {
      timer = undefined;
      if (!options.enabled() || options.ownsInput()) return;
      let count: number;
      try {
        if (options.halted()) return;
      } catch {
        return;
      }
      try {
        count = options.halt();
      } catch (error) {
        // The halt could not be saved: tell the person who pressed Escape (smarty-dev#7706).
        context.ui.notify(
          `Fabric: halt not saved; work may resume on restart: ${error instanceof Error ? error.message : String(error)}`,
          "warning",
        );
        return;
      }
      if (count > 0) {
        context.ui.notify(
          `Fabric: halted ${count} background worker${count === 1 ? "" : "s"} (Esc)`,
          "warning",
        );
      }
    }, 60);
    timer.unref?.();
    return undefined;
  });
  return () => {
    cancel();
    unsubscribe();
  };
}
