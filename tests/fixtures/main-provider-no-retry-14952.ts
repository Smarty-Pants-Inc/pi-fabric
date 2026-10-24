import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Inject Pi's explicit no-retry decision after a synthetic transient error on the real RPC path. */
export default function (pi: ExtensionAPI): void {
  pi.on("message_end", event => {
    const message = event.message as {
      role?: string;
      stopReason?: unknown;
      errorMessage?: unknown;
      recovery?: { retryable?: unknown };
    };
    if (message.role === "assistant" && message.stopReason === "error" &&
      typeof message.errorMessage === "string" && message.errorMessage.includes("PI_NO_RETRY_14952")) {
      message.recovery = { retryable: false };
    }
  });
}
