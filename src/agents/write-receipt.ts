import { resolveToolPath } from "./write-guard.js";

/**
 * smarty-dev#8758 item 2: the tool-generated receipt of what a process task agent changed, carried in its final
 * envelope (AgentRunResult.writeReceipt), so the spawning lane trusts the receipt, not the child's prose. Recorded by
 * the worker from the Pi child's own tool events (the same tool boundary the write guard sees): every path a write or
 * edit call completed on, and the text of every shell command the child ran or tried. The content a write or edit
 * call carries is never recorded; a shell command's text is recorded as given, so inline data in it is kept.
 * Bounded: at most 256 paths, 64 commands of at most 4,096 characters, 64 KiB in all; `truncated` marks a receipt
 * that dropped or cut anything.
 *
 * Trust: the events come from the child's Pi process stdout, so the receipt catches a child whose report is wrong,
 * not a malicious child process: code running as the child (an extension, or a shell writing to Pi's stdout) can add
 * or hide entries. It is evidence against the child's prose, not an authenticated audit log.
 */
export interface AgentWriteReceipt {
  /** Absolute paths, in first-touch order, deduplicated. */
  paths: string[];
  /** Shell command texts, in run order. */
  commands: string[];
  truncated?: true;
}

export const WRITE_RECEIPT_MAX_PATHS = 256;
export const WRITE_RECEIPT_MAX_COMMANDS = 64;
export const WRITE_RECEIPT_MAX_COMMAND_CHARS = 4_096;
export const WRITE_RECEIPT_MAX_BYTES = 64 * 1024;
/**
 * pi-fabric#1034 security P2: calls started but not yet ended are bounded too, at start. At most this many are held;
 * a command is cut to WRITE_RECEIPT_MAX_COMMAND_CHARS, and an id or path longer than that is not held. A start that
 * cannot be held is dropped and the receipt is marked truncated, so held data never exceeds about
 * 256 x (4,096 + 4,096) characters whatever a child emits.
 */
export const WRITE_RECEIPT_MAX_PENDING = 256;
const WRITE_TOOLS = new Set(["write", "edit"]);
const SHELL_TOOLS = new Set(["bash", "powershell"]);

export class WriteReceiptRecorder {
  readonly #pending = new Map<string, { path?: string; command?: string; cut?: true }>();
  readonly #paths = new Set<string>();
  readonly #commands: string[] = [];
  #bytes = 0;
  #truncated = false;
  // A start dropped since the last end() reported a change: the next end() reports the truncated receipt.
  #unreported = false;

  constructor(readonly cwd: string) {}

  /** Calls started and not yet ended (bounded by WRITE_RECEIPT_MAX_PENDING). */
  get pending(): number { return this.#pending.size; }

  /** A tool_execution_start event: remember what the call names, until it ends. Bounded at start (see above). */
  start(event: { toolCallId?: unknown; toolName?: unknown; args?: unknown }): void {
    if (typeof event.toolCallId !== "string" || typeof event.toolName !== "string") return;
    const args = typeof event.args === "object" && event.args !== null ? event.args as Record<string, unknown> : {};
    let call: { path?: string; command?: string; cut?: true } | undefined;
    if (WRITE_TOOLS.has(event.toolName)) {
      const raw = [args.path, args.file_path, args.filePath].find((value) => typeof value === "string" && value !== "");
      if (typeof raw !== "string") return;
      // Checked before resolving, and stored as a copy: a short result of normalizing a huge path must not keep the
      // child-sized source alive as a V8 slice (round-3 review P2).
      if (raw.length > WRITE_RECEIPT_MAX_COMMAND_CHARS) return this.#drop();
      let resolved: string;
      try { resolved = resolveToolPath(raw, this.cwd); } catch { resolved = raw; }
      if (resolved.length > WRITE_RECEIPT_MAX_COMMAND_CHARS) return this.#drop();
      call = { path: resolved.split("").join("") };
    } else if (SHELL_TOOLS.has(event.toolName) && typeof args.command === "string") {
      // A copy, not a slice: V8 may keep a slice's whole (child-sized) source string alive (round-2 review P2).
      call = args.command.length > WRITE_RECEIPT_MAX_COMMAND_CHARS
        ? { command: args.command.slice(0, WRITE_RECEIPT_MAX_COMMAND_CHARS).split("").join(""), cut: true }
        : { command: args.command };
    }
    if (!call) return;
    if (event.toolCallId.length > WRITE_RECEIPT_MAX_COMMAND_CHARS ||
      (this.#pending.size >= WRITE_RECEIPT_MAX_PENDING && !this.#pending.has(event.toolCallId))) return this.#drop();
    this.#pending.set(event.toolCallId, call);
  }

  #drop(): void {
    if (!this.#truncated) this.#unreported = true;
    this.#truncated = true;
  }

  /**
   * A tool_execution_end event: a write or edit that completed records its path (a blocked or failed one does not). A
   * shell call records its text once it ended, whether it ran or was refused: not every producer says which (a nested
   * pi.bash or a durable run's end event has no durationMs), and a receipt should over-report a command rather than
   * miss one that ran. Returns true when the receipt changed.
   */
  end(event: { toolCallId?: unknown; isError?: unknown; durationMs?: unknown }): boolean {
    // A start dropped since the last reported change makes this end report the (now truncated) receipt.
    const unreported = this.#unreported;
    this.#unreported = false;
    if (typeof event.toolCallId !== "string") return unreported;
    const call = this.#pending.get(event.toolCallId);
    if (!call) return unreported;
    this.#pending.delete(event.toolCallId);
    if (call.cut) this.#truncated = true;
    let changed = false;
    if (call.path !== undefined && event.isError !== true) changed = this.#add("path", call.path);
    else if (call.command !== undefined) changed = this.#add("command", call.command);
    return changed || unreported || call.cut === true;
  }

  #add(kind: "path" | "command", value: string): boolean {
    if (kind === "path" && this.#paths.has(value)) return false;
    let text = value;
    if (kind === "command" && text.length > WRITE_RECEIPT_MAX_COMMAND_CHARS) {
      text = text.slice(0, WRITE_RECEIPT_MAX_COMMAND_CHARS);
      this.#truncated = true;
    }
    const full = kind === "path" ? this.#paths.size >= WRITE_RECEIPT_MAX_PATHS : this.#commands.length >= WRITE_RECEIPT_MAX_COMMANDS;
    const bytes = Buffer.byteLength(text, "utf8") + 4;
    if (full || this.#bytes + bytes > WRITE_RECEIPT_MAX_BYTES) {
      const changed = !this.#truncated;
      this.#truncated = true;
      return changed;
    }
    this.#bytes += bytes;
    if (kind === "path") this.#paths.add(text);
    else this.#commands.push(text);
    return true;
  }

  get receipt(): AgentWriteReceipt {
    return { paths: [...this.#paths], commands: [...this.#commands], ...(this.#truncated ? { truncated: true as const } : {}) };
  }
}
