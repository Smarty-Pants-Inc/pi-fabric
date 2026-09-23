import { execFile } from "node:child_process";
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
      execFile(file, args, { encoding: "utf8", timeout: 5_000, maxBuffer: 16_384, signal, windowsHide: true }, (error, stdout) => {
        // Never propagate subprocess errors: they can contain stdout/stderr secrets.
        if (error) reject(new Error("Jev credential resolver failed"));
        else resolve(stdout.trim());
      });
    });
    signal.throwIfAborted();
    if (!secret || /[\r\n]/.test(secret)) throw new Error("Jev credential resolver returned an invalid credential");
    this.#cached = secret;
    return secret;
  }
  clear(): void { this.#cached = undefined; }
}
export class JevClient {
  readonly credentials: JevCredentials;
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
    const key = await runAbortable(timedSignal, () => this.credentials.resolve(timedSignal));
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
  close(): void { this.credentials.clear(); }
}
