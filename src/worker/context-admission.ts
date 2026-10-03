import fs from "node:fs";
import * as nodeModule from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

/** Pure estimator belonging to the selected Pi launcher, not a bundled peer/runtime. */
export async function loadActorInputEstimator(binary: string): Promise<((text: string) => number) | undefined> {
  let estimator: string;
  try {
    const selected = fs.realpathSync(binary);
    // Dependency resolution alone does not identify the executable: an opaque
    // launcher can live beside (or import) pi-ai without speaking native history
    // RPC. Only Pi's own enclosing package authorizes this admission protocol.
    let nativeLauncher = false;
    for (let directory = path.dirname(selected);;) {
      const enclosing = path.join(directory, "package.json");
      if (fs.existsSync(enclosing)) {
        nativeLauncher = JSON.parse(fs.readFileSync(enclosing, "utf8")).name === "@earendil-works/pi-coding-agent";
        break;
      }
      const parent = path.dirname(directory);
      if (parent === directory) break;
      directory = parent;
    }
    if (!nativeLauncher) return undefined;
    const base = pathToFileURL(selected);
    const manifest = typeof nodeModule.findPackageJSON === "function"
      ? nodeModule.findPackageJSON("@earendil-works/pi-ai", base) : undefined;
    let packageFile = manifest;
    // Bun source workers lack findPackageJSON and require-only resolution cannot
    // see this import-only package. Search only the selected launcher ancestry.
    for (let directory = path.dirname(fs.realpathSync(binary)); !packageFile;) {
      const candidate = path.join(directory, "node_modules", "@earendil-works", "pi-ai", "package.json");
      if (fs.existsSync(candidate)) packageFile = candidate;
      const parent = path.dirname(directory);
      if (parent === directory) break;
      directory = parent;
    }
    if (!packageFile) return undefined;
    estimator = path.join(path.dirname(packageFile), "dist", "utils", "estimate.js");
  }
  catch { return undefined; } // Opaque launchers retain their native admission policy.
  const native = await import(pathToFileURL(estimator).href) as { estimateTextTokens(text: string): number };
  if (typeof native.estimateTextTokens !== "function") throw new Error("Selected Pi launcher lacks native text estimation");
  return native.estimateTextTokens;
}

const object = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

/** Idle RPC admission for full-history actors. No business prompt until history fits. */
export class ActorContextAdmission {
  private readonly runId: string;
  private readonly task: string;
  private readonly systemPrompt: string;
  private readonly estimate: (text: string) => number;
  private readonly io: {
    send(frame: Record<string, unknown>): void;
    ready(): void;
    fail(error: string): void;
    compact(tokens: number, contextWindow: number, reason: string): void;
  };
  private pending: { id: string; command: string } | undefined;
  private sequence = 0;
  private contextWindow = 0;
  private compacted = false;
  private finished = false;

  constructor(runId: string, task: string, systemPrompt: string, estimate: (text: string) => number, io: ActorContextAdmission["io"]) {
    this.runId = runId; this.task = task; this.systemPrompt = systemPrompt; this.estimate = estimate; this.io = io;
  }

  start(): void { this.send("get_state"); }

  private send(command: string, args: Record<string, unknown> = {}): void {
    if (this.finished) return;
    const id = `fabric-context:${this.runId}:${++this.sequence}`;
    this.pending = { id, command };
    this.io.send({ type: command, id, ...args });
  }

  private fail(error: string): void {
    if (this.finished) return;
    this.finished = true; this.pending = undefined;
    this.io.fail(error);
  }

  /** A dropped RPC history still completes its correlation: compact once, then remeasure. */
  observeOversizedResponse(prefix: string, chars: number): boolean {
    if (!this.pending || this.finished) return false;
    // Pi serializes the response envelope before data. Parse only that small JSON
    // object, never match ids inside historical message content or buffer the body.
    const head = prefix.slice(0, 4096);
    const dataStart = head.indexOf(',"data":');
    if (dataStart < 0) return false;
    let event: Record<string, unknown>;
    try { event = JSON.parse(head.slice(0, dataStart) + "}") as Record<string, unknown>; }
    catch { return false; }
    if (event.type !== "response" || event.id !== this.pending.id) return false;
    const command = this.pending.command;
    this.pending = undefined;
    if (event.command !== command || event.success !== true || command !== "get_messages") {
      this.fail(`Actor context admission ${command} failed: oversized RPC response (${chars} characters)`);
    } else {
      // A wire-size overflow is an explicit recovery reason, not a token estimate.
      // After compact commits, get_messages must fit both the wire and context limits.
      this.recover(0, `native history response exceeds event cap (${chars} characters)`);
    }
    return true;
  }

  private recover(tokens: number, reason: string): void {
    if (this.compacted) {
      this.fail(`Context exceeds window: ${reason}; pre-dispatch compaction did not make this activation fit`);
      return;
    }
    this.compacted = true;
    this.io.compact(tokens, this.contextWindow, reason);
    this.send("compact", { customInstructions: "Preserve the actor's active objectives, user constraints, decisions, pending work and referenced artifact paths. Bound the summary so the next activation fits the model context window." });
  }

  observe(event: Record<string, unknown>): boolean {
    if (event.type !== "response" || !this.pending || event.id !== this.pending.id) return false;
    const command = this.pending.command; this.pending = undefined;
    if (event.command !== command || event.success !== true) {
      this.fail(`Actor context admission ${command} failed: ${String(event.error ?? "invalid RPC response")}`);
      return true;
    }
    const data = object(event.data);
    if (command === "get_state") {
      const window = object(data?.model)?.contextWindow;
      if (typeof window !== "number" || !Number.isFinite(window) || window <= 0 || data?.isStreaming !== false || data?.isCompacting !== false) {
        this.fail("Actor context admission requires an idle Pi child and a model context window");
      } else {
        this.contextWindow = window;
        this.send("get_messages");
      }
    } else if (command === "compact") {
      // Native compact resolves only after its checkpoint has been committed.
      // Measure the resulting context; a failed/ineffective summary never dispatches.
      this.send("get_messages");
    } else {
      if (!Array.isArray(data?.messages)) {
        this.fail("Actor context admission requires native session messages");
        return true;
      }
      // Fresh content estimate, not stale assistant usage. Reserve room for native
      // prompt/tool framing and output; systemPrompt includes the actor persona.
      const tokens = this.estimate(JSON.stringify({ messages: data.messages, task: this.task, systemPrompt: this.systemPrompt }));
      const reserve = Math.min(8192, Math.floor(this.contextWindow * 0.1));
      if (tokens > this.contextWindow - reserve) {
        this.recover(tokens, `estimated ${tokens} input tokens, window ${this.contextWindow}`);
      } else {
        this.finished = true;
        this.io.ready();
      }
    }
    return true;
  }
}
