import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { buildSessionContext, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
type AgentMessage = ReturnType<typeof buildSessionContext>["messages"][number];

/** A native startup snapshot, never a guessed prompt marker or a journal edit. */
export class ActivationWindow {
  private readonly prior: string[];
  private current: string[] = [];

  constructor(messages: readonly AgentMessage[]) {
    this.prior = messages.map(message => JSON.stringify(message));
  }

  project(messages: AgentMessage[]): AgentMessage[] {
    const encoded = messages.map(message => JSON.stringify(message));
    if (this.prior.some((message, index) => encoded[index] !== message)) {
      throw new Error("Activation window lost its native history boundary");
    }
    const result = messages.slice(this.prior.length);
    const current = encoded.slice(this.prior.length);
    if (result[0]?.role !== "user" || this.current.some((message, index) => current[index] !== message)) {
      throw new Error("Activation window lost current activation messages");
    }
    this.current = current;
    return result;
  }
}

// ponytail: Pi catches extension exceptions and continues with the old context.
// A synchronous exit of this disposable child is the fail-closed boundary, not
// throw/abort/shutdown (which can leave a request or automatic retry runnable).
function failClosed(error: unknown): never {
  // An accidentally loaded hook in an owner/TUI must never exit that owner.
  // Missing launch binding also means no readiness ACK, so its worker cannot prompt.
  if (String(process.ppid) !== process.env.PI_FABRIC_ACTIVATION_WORKER_PID ||
      !process.env.PI_FABRIC_ACTIVATION_NONCE) {
    throw new Error(`Activation window is not in a disposable worker: ${String(error)}`);
  }
  try {
    fs.writeSync(2, `Fabric activation window failed: ${String(error)}\n`);
  } finally {
    process.exit(78);
  }
}

/** Loaded only by an explicitly selected actor worker; adds no tools or trust. */
export default function activationWindow(pi: ExtensionAPI): void {
  let window: ActivationWindow | undefined;
  try {
    if (String(process.ppid) !== process.env.PI_FABRIC_ACTIVATION_WORKER_PID || !process.env.PI_FABRIC_ACTIVATION_NONCE) {
      throw new Error("Activation window requires the worker launch binding");
    }
    pi.on("session_start", (_event, ctx) => {
      try {
        const hook = fs.realpathSync(fileURLToPath(import.meta.url));
        if (window || ctx.mode !== "rpc" || !process.env.PI_FABRIC_PARENT_RUN ||
            !process.env.PI_FABRIC_ACTIVATION_NONCE || process.env.PI_FABRIC_ACTIVATION_HOOK !== hook) {
          throw new Error("Activation window requires a fresh Fabric RPC worker");
        }
        window = new ActivationWindow(buildSessionContext(ctx.sessionManager.getBranch()).messages);
        // Pi redirects ordinary stdout writes to stderr in RPC mode. This bound
        // protocol ACK must use stdout itself, not the redirected logging stream.
        fs.writeSync(1, `${JSON.stringify({
          type: "fabric_activation_window_ready",
          runId: process.env.PI_FABRIC_PARENT_RUN,
          nonce: process.env.PI_FABRIC_ACTIVATION_NONCE,
          policy: "activation",
          protocol: 1,
          hook,
        })}\n`);
      } catch (error) {
        failClosed(error);
      }
    });
    pi.on("context", event => {
      try {
        if (!window) throw new Error("Activation window is not initialized");
        return { messages: window.project(event.messages) };
      } catch (error) {
        return failClosed(error);
      }
    });
    // Native summarization bypasses the context hook. Never summarize either
    // historical messages or incomplete current tool exchanges under this policy.
    pi.on("session_before_compact", () => failClosed("Compaction is unsupported during an activation window"));
    pi.on("session_before_tree", () => ({ cancel: true }));
    pi.on("session_before_switch", () => ({ cancel: true }));
    pi.on("session_before_fork", () => ({ cancel: true }));
  } catch (error) {
    failClosed(error);
  }
}
