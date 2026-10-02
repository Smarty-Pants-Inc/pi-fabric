import { spawn, type ChildProcess } from "node:child_process";
import { terminateWindowsTree } from "../child-process-tree.js";
import { runAbortable } from "../async-settlement.js";
import type { FabricJevConfig } from "./config.js";
import type { JevRequest, JevResponse } from "./types.js";
import { JEV_TYPESAFE_ROUTE, resolveJevUpstreamModel, type JevRoute } from "./routes.js";
import { checkRequest, checkResponse, jsonText } from "./validation.js";

export interface JevCredentialSource {
  configured(): boolean;
  resolve(signal: AbortSignal): Promise<string | undefined>;
}
export class JevCredentials {
  #cached: string | undefined;
  /** Command close obligations outlive caller-facing cancellation/results. */
  readonly #pendingCommands = new Set<Promise<void>>();
  constructor(
    readonly command: readonly string[],
    private readonly env: NodeJS.ProcessEnv = process.env,
    private readonly providerAuth?: JevCredentialSource,
    /** Route-specific environment fallbacks, in order. */
    private readonly envKeys: readonly string[] = JEV_TYPESAFE_ROUTE.envKeys,
  ) {}
  #envCredential(): string | undefined {
    for (const key of this.envKeys) {
      const value = this.env[key]?.trim();
      if (value) return value;
    }
    return undefined;
  }
  status() {
    const source = this.providerAuth?.configured() ? "pi" : this.#envCredential() ? "environment" : this.command.length ? "command" : "missing";
    return { configured: source !== "missing", source, verified: false };
  }
  async resolve(signal: AbortSignal): Promise<string> {
    signal.throwIfAborted();
    if (this.providerAuth) {
      try {
        const key = await this.providerAuth.resolve(signal);
        signal.throwIfAborted();
        if (key?.trim()) return key.trim();
      } catch { throw new Error("Jev Pi credential resolution failed"); }
    }
    const env = this.#envCredential();
    if (env) return env;
    if (this.#cached) return this.#cached;
    const [file, ...args] = this.command;
    if (!file) throw new Error(`Jev credentials unavailable: set ${this.envKeys.join(" or ")} or configure jev.credentialCommand`);
    const secret = await new Promise<string>((resolve, reject) => {
      let child: ChildProcess;
      try {
        // execFile does NOT support detached. spawn an argv command (no shell)
        // in its own POSIX group so stopping descendants cannot hit the host.
        child = spawn(file, args, {
          detached: process.platform !== "win32", windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
        });
      } catch {
        reject(new Error("Jev credential resolver failed"));
        return;
      }
      let closed = false;
      let stopping = false;
      let treeStop: Promise<void> | undefined;
      let joined!: () => void;
      const obligation = new Promise<void>(resolve => { joined = resolve; });
      this.#pendingCommands.add(obligation);
      void obligation.then(() => this.#pendingCommands.delete(obligation));
      const stdout: Buffer[] = [];
      let stdoutBytes = 0;
      let stderrBytes = 0;
      const kill = (signal: NodeJS.Signals): void => {
        if (child.pid === undefined) return;
        try { process.kill(-child.pid, signal); }
        catch { try { child.kill(signal); } catch { /* Already exited. */ } }
      };
      const stop = (): void => {
        if (closed || stopping) return;
        stopping = true;
        // Never propagate subprocess errors/output, including on cancellation.
        reject(new Error("Jev credential resolver failed"));
        if (process.platform === "win32") treeStop = terminateWindowsTree(child);
        else {
          kill("SIGTERM");
          treeStop = new Promise<void>(resolve => {
            setTimeout(() => { kill("SIGKILL"); resolve(); }, 500);
          });
        }
      };
      const deadline = setTimeout(stop, 5_000);
      child.stdout!.on("data", (chunk: Buffer) => {
        stdoutBytes += chunk.length;
        if (stdoutBytes > 16_384) stop();
        else if (!stopping) stdout.push(chunk);
      });
      child.stderr!.on("data", (chunk: Buffer) => {
        stderrBytes += chunk.length;
        if (stderrBytes > 16_384) stop();
      });
      child.once("error", () => reject(new Error("Jev credential resolver failed")));
      child.once("close", (code) => {
        closed = true;
        clearTimeout(deadline);
        signal.removeEventListener("abort", stop);
        if (code !== 0 || stopping) reject(new Error("Jev credential resolver failed"));
        else resolve(Buffer.concat(stdout).toString("utf8").trim());
        // close confirms exit AND stream teardown. Also join tree termination:
        // a TERM-exited parent must not abandon a stubborn descendant/helper.
        void Promise.resolve(treeStop).then(joined);
      });
      signal.addEventListener("abort", stop, { once: true });
      if (signal.aborted) stop();
    });
    signal.throwIfAborted();
    if (!secret || /[\r\n]/.test(secret)) throw new Error("Jev credential resolver returned an invalid credential");
    this.#cached = secret;
    return secret;
  }
  async drainCommands(): Promise<void> {
    while (this.#pendingCommands.size) await Promise.allSettled([...this.#pendingCommands]);
  }
  clear(): void { this.#cached = undefined; }
}
export class JevClient {
  readonly credentials: JevCredentials;
  /** Actual operations, not the abort-raced waiters; host credential APIs may not cancel. */
  readonly #pendingCredentials = new Set<Promise<string>>();
  constructor(
    readonly config: FabricJevConfig,
    private readonly fetcher: typeof fetch = fetch,
    credentials?: JevCredentials,
    readonly route: JevRoute = JEV_TYPESAFE_ROUTE,
  ) {
    this.credentials = credentials ?? new JevCredentials(config.credentialCommand, process.env, undefined, route.envKeys);
  }
  async evaluate(request: JevRequest, signal: AbortSignal): Promise<JevResponse> {
    checkRequest(request, this.config.maxRequestBytes);
    const requestedModel = request.model ?? this.config.model;
    const model = resolveJevUpstreamModel(this.route, requestedModel);
    if (!model) throw new Error(`Jev model "${requestedModel}" is not available on the ${this.route.label} route`);
    const timedSignal = AbortSignal.any([signal, AbortSignal.timeout(this.config.requestTimeoutMs)]);
    const key = await runAbortable(timedSignal, () => {
      const pending = this.credentials.resolve(timedSignal);
      this.#pendingCredentials.add(pending);
      void pending.then(() => this.#pendingCredentials.delete(pending), () => this.#pendingCredentials.delete(pending));
      return pending;
    });
    const body = jsonText({ ...request, model }, this.config.maxRequestBytes, "Jev request");
    let response: Response;
    try {
      response = await runAbortable(timedSignal, () => this.fetcher(this.route.endpoint, {
        method: "POST", redirect: "error", signal: timedSignal,
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` }, body,
      }));
    } catch {
      throw new Error(timedSignal.aborted ? "Jev request cancelled or timed out" : "Jev network request failed");
    }
    if (!response.ok) {
      await response.body?.cancel();
      // No automatic retry: the controller decides whether a delayed decision is still useful.
      throw new Error(`${this.route.label} HTTP ${response.status}${[429, 529].includes(response.status) ? ": rate limited; back off before retrying" : ""}`);
    }
    const reader = response.body?.getReader();
    if (!reader) throw new Error(`${this.route.label} returned an empty response`);
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    try {
      for (;;) {
        const { value, done } = await runAbortable(timedSignal, () => reader.read());
        if (done) break;
        bytes += value.byteLength;
        if (bytes > 1_048_576) throw new Error("oversize");
        chunks.push(value);
      }
      return checkResponse(JSON.parse(Buffer.concat(chunks).toString("utf8")), request);
    } catch {
      throw new Error(timedSignal.aborted ? "Jev response cancelled or timed out" : `${this.route.label} returned an invalid or oversized typed response`);
    } finally { await reader.cancel().catch(() => undefined); }
  }
  async drainCredentials(): Promise<void> {
    while (this.#pendingCredentials.size) await Promise.allSettled([...this.#pendingCredentials]);
    await this.credentials.drainCommands();
  }
  close(): void { this.credentials.clear(); }
}
