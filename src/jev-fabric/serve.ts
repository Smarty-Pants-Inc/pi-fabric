import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";

const BANNER_TIMEOUT_MS = 15_000;
const CLOSE_GRACE_MS = 5_000;
/** UTF-8 bytes excluding the JSONL newline, for complete AND partial lines. */
export const JEV_FABRIC_LINE_MAX_BYTES = 16 * 1024 * 1024;

export interface JevFabricBanner {
  protocol: number;
  version: string;
  features?: string[];
  store?: number;
  timeoutMs?: number;
}
export class JevFabricServeError extends Error {
  constructor(message: string, readonly code: number | null) { super(message); this.name = "JevFabricServeError"; }
}
type Pending = { resolve: (value: unknown) => void; reject: (error: Error) => void };

/** A session-owned affine client. Closing or cancelling never replenishes its budget. */
export class JevFabricServe {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #pending = new Map<number, Pending>();
  #next = 1;
  #closed: Error | undefined;
  #writes: Promise<void> = Promise.resolve();
  readonly exited: Promise<void>;
  private constructor(child: ChildProcessWithoutNullStreams, readonly banner: JevFabricBanner, exited: Promise<void>) {
    this.#child = child;
    this.exited = exited;
  }

  static open(binary: string, options: { home: string; cwd: string; timeoutMs: number; evaluations?: number; tokens?: number; env?: NodeJS.ProcessEnv }): Promise<JevFabricServe> {
    return new Promise((resolve, reject) => {
      const child = spawn(binary, ["--", "serve", "--timeout-ms", String(options.timeoutMs), String(options.evaluations ?? 0), String(options.tokens ?? 0)], {
        cwd: options.cwd, env: { ...(options.env ?? process.env), JEV_FABRIC_HOME: options.home },
        stdio: ["pipe", "pipe", "pipe"], windowsHide: true,
      });
      const exited = new Promise<void>(done => child.once("close", () => done()));
      let serve: JevFabricServe | undefined;
      let failed = false;
      let chunks: Buffer[] = [];
      let bytes = 0;
      const fail = (error: Error): void => {
        if (failed) return;
        failed = true;
        clearTimeout(timer);
        chunks = []; bytes = 0;
        if (serve) serve.#fail(error);
        reject(error);
        child.kill("SIGKILL");
      };
      const timer = setTimeout(() => fail(new JevFabricServeError("jev-fabric serve did not send its banner", null)), BANNER_TIMEOUT_MS);
      timer.unref?.();
      // Never include private peer stderr in guest-visible errors.
      child.stderr.resume();
      const peerError = (): void => fail(new JevFabricServeError("jev-fabric serve pipe failed", null));
      child.on("error", peerError);
      child.stdin.on("error", peerError);
      child.stdout.on("error", peerError);
      child.stderr.on("error", peerError);
      child.stdout.on("data", (chunk: Buffer) => {
        if (failed) return;
        let start = 0;
        while (start < chunk.length) {
          const newline = chunk.indexOf(10, start);
          const end = newline < 0 ? chunk.length : newline;
          const part = chunk.subarray(start, end);
          bytes += part.length;
          if (bytes > JEV_FABRIC_LINE_MAX_BYTES) { fail(new JevFabricServeError("jev-fabric serve sent an oversized line", null)); return; }
          if (part.length) chunks.push(part);
          if (newline < 0) return;
          let line: string;
          try { line = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, bytes)); }
          catch { fail(new JevFabricServeError("jev-fabric serve sent invalid UTF-8", null)); return; }
          chunks = []; bytes = 0; start = newline + 1;
          if (!line.trim()) continue;
          if (!serve) {
            let banner: JevFabricBanner | undefined;
            try { banner = (JSON.parse(line) as { ready?: JevFabricBanner }).ready; } catch { /* invalid below */ }
            if (!banner || typeof banner.protocol !== "number") { fail(new JevFabricServeError("jev-fabric serve sent an unrecognized banner", null)); return; }
            clearTimeout(timer);
            serve = new JevFabricServe(child, banner, exited);
            resolve(serve);
          } else {
            try { serve.#accept(line); } catch { fail(new JevFabricServeError("jev-fabric serve sent invalid JSONL", null)); return; }
          }
        }
      });
      child.once("close", code => {
        clearTimeout(timer);
        const error = new JevFabricServeError(bytes ? "jev-fabric serve ended with an incomplete line" : `jev-fabric serve ended${code === null ? "" : ` (exit ${code})`}`, code);
        if (!serve) reject(error); else serve.#fail(error);
        chunks = []; bytes = 0;
      });
    });
  }
  #accept(line: string): void {
    const value = JSON.parse(line) as { id?: unknown; ok?: unknown; result?: unknown; error?: { code?: number; message?: string } };
    if (!value || typeof value.id !== "number") throw new Error("Invalid envelope");
    const pending = this.#pending.get(value.id);
    if (!pending) return; // late result of a locally cancelled request
    this.#pending.delete(value.id);
    if (value.ok === true) pending.resolve(value.result);
    else pending.reject(new JevFabricServeError(`jev-fabric: ${value.error?.message ?? "request failed"}`, value.error?.code ?? null));
  }
  #fail(error: Error): void {
    this.#closed ??= error;
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
  }
  get closed(): boolean { return this.#closed !== undefined; }

  /** Abort drops this waiter. An inference owner must close on uncertain cancellation. */
  request<T>(op: string, fields: Record<string, unknown> = {}, signal?: AbortSignal): Promise<T> {
    if (this.#closed) return Promise.reject(this.#closed);
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("aborted"));
    const id = this.#next++;
    let line: string;
    try {
      line = JSON.stringify({ ...fields, id, op });
      if (Buffer.byteLength(line, "utf8") > JEV_FABRIC_LINE_MAX_BYTES) throw new JevFabricServeError("jev-fabric request exceeds 16 MiB UTF-8 JSONL limit", null);
    } catch (error) { return Promise.reject(error); }
    return new Promise<T>((resolve, reject) => {
      const abort = (): void => {
        const pending = this.#pending.get(id);
        this.#pending.delete(id);
        pending?.reject(signal?.reason instanceof Error ? signal.reason : new Error("aborted"));
      };
      this.#pending.set(id, {
        resolve: value => { signal?.removeEventListener("abort", abort); resolve(value as T); },
        reject: error => { signal?.removeEventListener("abort", abort); reject(error); },
      });
      signal?.addEventListener("abort", abort, { once: true });
      // Serialize writes through their completion callback: don't queue unbounded writes
      // into a slow peer's stream, and don't send requests cancelled while queued.
      this.#writes = this.#writes.then(async () => {
        if (this.#closed || !this.#pending.has(id)) return;
        await new Promise<void>((done, failed) => {
          this.#child.stdin.write(line + "\n", error => error ? failed(error) : done());
        });
      }).catch(() => {
        this.#fail(new JevFabricServeError("jev-fabric serve write failed", null));
        this.#child.kill("SIGKILL");
      });
    });
  }
  /** Close stops owned sessions and pending waiters; durable jobs remain untouched. */
  async close(): Promise<void> {
    this.#fail(new JevFabricServeError("jev-fabric serve closed", null));
    this.#child.stdin.end();
    const timer = setTimeout(() => this.#child.kill("SIGKILL"), CLOSE_GRACE_MS);
    timer.unref?.();
    await this.exited;
    clearTimeout(timer);
  }
}
