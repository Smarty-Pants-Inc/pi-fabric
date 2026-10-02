import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { StringDecoder } from "node:string_decoder";

const BANNER_TIMEOUT_MS = 15_000;
const CLOSE_GRACE_MS = 500;
const LINE_MAX_CHARS = 2 * 1024 * 1024;

export interface JevFabricBanner {
  protocol: number;
  version: string;
  features?: string[];
  store?: number;
  timeoutMs?: number;
}

export class JevFabricServeError extends Error {
  constructor(message: string, readonly code: number | null, options?: ErrorOptions) { super(message, options); this.name = "JevFabricServeError"; }
}

type Pending = { resolve: (value: unknown) => void; reject: (error: Error) => void };

const stdinFailure = (error: Error): JevFabricServeError =>
  new JevFabricServeError(`jev-fabric stdin failed: ${error.message.slice(0, 500)}`, null, { cause: error });

const killBackend = (child: ChildProcessWithoutNullStreams): void => {
  try {
    if (process.platform !== "win32" && child.pid) process.kill(-child.pid, "SIGKILL");
    else child.kill("SIGKILL");
  } catch { /* an exit raced the owner-isolated termination */ }
};

/**
 * One `jev-fabric -- serve` connection (protocol 2): JSONL requests with
 * numeric ids, answered possibly out of order. The connection owns its
 * `session` children; closing it ends them. Budgets, deadlines and process
 * semantics live in the binary; this only frames JSON.
 */
export class JevFabricServe {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #pending = new Map<number, Pending>();
  #next = 1;
  #closed: Error | undefined;
  #closing: Promise<void> | undefined;
  readonly exited: Promise<void>;

  private constructor(child: ChildProcessWithoutNullStreams, readonly banner: JevFabricBanner) {
    this.#child = child;
    this.exited = new Promise(resolve => child.once("close", () => resolve()));
  }

  /**
   * Starts a connection. Its Jev budget defaults to zero evaluations
   * (processes and sessions only); a Jev run passes its host ceilings.
   */
  static open(binary: string, options: { home: string; cwd: string; timeoutMs: number; evaluations?: number; tokens?: number; env?: NodeJS.ProcessEnv }): Promise<JevFabricServe> {
    return new Promise((resolve, reject) => {
      const child = spawn(binary, ["--", "serve", "--timeout-ms", String(options.timeoutMs), String(options.evaluations ?? 0), String(options.tokens ?? 0)], {
        cwd: options.cwd,
        env: { ...(options.env ?? process.env), JEV_FABRIC_HOME: options.home },
        stdio: ["pipe", "pipe", "pipe"],
        windowsHide: true,
        detached: process.platform !== "win32",
      });
      let stderr = "";
      let serve: JevFabricServe | undefined;
      let openingFailure: JevFabricServeError | undefined;
      // ChildProcess errors do NOT contain errors emitted by its stdin Socket.
      // Keep this listener for the whole stream lifetime, including owner EOF.
      child.stdin.on("error", error => {
        clearTimeout(timer);
        if (serve) serve.#stdinFailed(error);
        else {
          openingFailure ??= stdinFailure(error);
          killBackend(child);
          // The close handler rejects the banner after confirmed backend exit.
        }
      });
      const decoder = new StringDecoder("utf8");
      let buffer = "";
      const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new JevFabricServeError("jev-fabric serve did not send its banner", null)); }, BANNER_TIMEOUT_MS);
      timer.unref?.();
      child.stderr.on("data", (chunk: Buffer) => { if (stderr.length < 4096) stderr += chunk.toString("utf8"); });
      child.once("error", error => { clearTimeout(timer); reject(error); });
      child.stdout.on("data", (chunk: Buffer) => {
        if (openingFailure) return;
        buffer += decoder.write(chunk);
        let index: number;
        while ((index = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, index).trim();
          buffer = buffer.slice(index + 1);
          if (!line) continue;
          if (!serve) {
            let banner: JevFabricBanner | undefined;
            try { banner = (JSON.parse(line) as { ready?: JevFabricBanner }).ready; } catch { /* reported below */ }
            clearTimeout(timer);
            if (!banner || typeof banner.protocol !== "number") {
              child.kill("SIGKILL");
              reject(new JevFabricServeError("jev-fabric serve sent an unrecognized banner", null));
              return;
            }
            serve = new JevFabricServe(child, banner);
            resolve(serve);
            continue;
          }
          serve.#accept(line);
        }
        if (buffer.length > LINE_MAX_CHARS) {
          child.kill("SIGKILL");
          if (serve) serve.#fail(new JevFabricServeError("jev-fabric serve sent an oversized line", null));
        }
      });
      child.once("close", code => {
        clearTimeout(timer);
        const message = stderr.trim().split("\n").at(-1)?.slice(0, 500);
        const error = openingFailure ?? new JevFabricServeError(`jev-fabric serve ended${code === null ? "" : ` (exit ${code})`}${message ? `: ${message}` : ""}`, code);
        if (!serve) reject(error); else serve.#fail(error);
      });
    });
  }

  #accept(line: string): void {
    let value: { id?: unknown; ok?: unknown; result?: unknown; error?: { code?: number; message?: string } };
    try { value = JSON.parse(line); } catch { return; }
    if (typeof value.id !== "number") return;
    const pending = this.#pending.get(value.id);
    if (!pending) return;
    this.#pending.delete(value.id);
    if (value.ok === true) pending.resolve(value.result);
    else pending.reject(new JevFabricServeError(`jev-fabric: ${value.error?.message ?? "request failed"}`, value.error?.code ?? null));
  }

  #fail(error: Error): void {
    this.#closed ??= error;
    for (const pending of this.#pending.values()) pending.reject(this.#closed);
    this.#pending.clear();
  }

  #stdinFailed(error: Error): void {
    this.#fail(stdinFailure(error));
    // Reject only this connection's work, then bound/confirm its teardown even
    // if the peer closed stdin but kept its process or descendants running.
    void this.close();
  }

  get closed(): boolean { return this.#closed !== undefined; }

  /**
   * Sends one request. Aborting stops waiting locally; the binary still
   * answers (a long-poll ends at its own ceiling), and the answer is dropped.
   */
  request<T>(op: string, fields: Record<string, unknown> = {}, signal?: AbortSignal): Promise<T> {
    if (this.#closed) return Promise.reject(this.#closed);
    signal?.throwIfAborted();
    const id = this.#next++;
    return new Promise<T>((resolve, reject) => {
      // Serialize before recording a pending request: local argument errors
      // must not leave a waiter/abort listener behind.
      const line = `${JSON.stringify({ id, op, ...fields })}\n`;
      const abort = (): void => {
        this.#pending.delete(id);
        reject(signal?.reason instanceof Error ? signal.reason : new Error("aborted"));
      };
      this.#pending.set(id, {
        resolve: value => { signal?.removeEventListener("abort", abort); resolve(value as T); },
        reject: error => { signal?.removeEventListener("abort", abort); reject(error); },
      });
      signal?.addEventListener("abort", abort, { once: true });
      try {
        this.#child.stdin.write(line, error => { if (error) this.#stdinFailed(error); });
      } catch (error) {
        this.#stdinFailed(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  /** Ends the connection: jev-fabric stops its session children; durable jobs are untouched. */
  close(): Promise<void> {
    return this.#closing ??= this.#close();
  }

  async #close(): Promise<void> {
    if (!this.#closed) {
      try { this.#child.stdin.end(); }
      catch (error) { this.#fail(stdinFailure(error instanceof Error ? error : new Error(String(error)))); }
      this.#fail(new JevFabricServeError("jev-fabric connection closed by owner", null));
    }
    // A delayed wire receipt cannot postpone EOF. If the backend does not
    // confirm exit promptly, terminate only its recorded, isolated group.
    const timer = setTimeout(() => killBackend(this.#child), CLOSE_GRACE_MS);
    timer.unref?.();
    await this.exited;
    clearTimeout(timer);
  }
}
