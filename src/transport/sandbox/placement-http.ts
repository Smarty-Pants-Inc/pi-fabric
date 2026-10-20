import { diagnostics } from "../../core/diagnostics.js";
import type { SandboxBinding, SandboxPlacement, SandboxSpawnIntent, SandboxSpawnResult } from "./types.js";

export const DEFAULT_SANDBOX_PLACEMENT_URL = "http://smarty-placement.smarty-island.svc:8080";
const REQUEST_TIMEOUT_MS = 10_000;
const WATCH_TIMEOUT_MS = 30_000;
const MAX_RESPONSE_BYTES = 64 * 1024;
const MAX_NDJSON_LINE_BYTES = 16 * 1024;
const PROFILE_ERROR = /profile/iu;

export class SandboxPlacementHttpError extends Error {
  readonly code: string | undefined;
  constructor(readonly status: number, readonly body: unknown) {
    const record = typeof body === "object" && body !== null && !Array.isArray(body) ? body as Record<string, unknown> : undefined;
    const detail = typeof record?.detail === "string" ? record.detail : `Placement HTTP ${status}`;
    super(detail);
    this.name = "SandboxPlacementHttpError";
    this.code = typeof record?.code === "string" ? record.code : undefined;
  }
}

const operationSignal = (signal: AbortSignal | undefined, timeoutMs: number): AbortSignal =>
  signal ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]) : AbortSignal.timeout(timeoutMs);

const isProfileUnsupported = (error: unknown): error is SandboxPlacementHttpError =>
  error instanceof SandboxPlacementHttpError && (error.status === 400 || error.status === 422) &&
  PROFILE_ERROR.test(`${error.code ?? ""} ${JSON.stringify(error.body)}`);

/** Plain-HTTP Placement API client. It deliberately sends no credentials or auth headers. */
export class SandboxPlacementHttp implements SandboxPlacement {
  readonly #base: URL;
  readonly #profileRejectedRuns = new Set<string>();

  constructor(baseUrl = DEFAULT_SANDBOX_PLACEMENT_URL, readonly onProfileFallback?: (runId: string) => void) {
    let parsed: URL;
    try { parsed = new URL(baseUrl); } catch { throw new Error("Invalid sandbox Placement URL"); }
    if (parsed.protocol !== "http:" || parsed.username || parsed.password || parsed.search || parsed.hash || parsed.pathname !== "/") {
      throw new Error("Sandbox Placement URL must be a plain HTTP origin without credentials, path, query, or fragment");
    }
    this.#base = new URL(parsed.origin);
  }

  async requestSpawn(intent: SandboxSpawnIntent, signal?: AbortSignal): Promise<SandboxSpawnResult> {
    if (!this.#profileRejectedRuns.has(intent.runId)) {
      try { return await this.#post(intent, signal); }
      catch (error) {
        if (!intent.profile || !isProfileUnsupported(error)) throw error;
        this.#profileRejectedRuns.add(intent.runId);
        diagnostics.warn("src/transport/sandbox/placement-http.ts", `[pi-fabric] Placement rejected optional profile for run ${intent.runId}; retrying once without it`);
        this.onProfileFallback?.(intent.runId);
      }
    }
    const { profile: _profile, ...legacyIntent } = intent;
    return this.#post(legacyIntent, signal);
  }

  getSpawn(runId: string, signal?: AbortSignal): Promise<SandboxSpawnResult> {
    return this.#json<SandboxSpawnResult>(`/v1/spawns/${encodeURIComponent(runId)}`, { method: "GET" }, signal, REQUEST_TIMEOUT_MS);
  }

  async *watchSpawn(runId: string, signal?: AbortSignal): AsyncIterable<SandboxSpawnResult> {
    const url = new URL(`/v1/spawns/${encodeURIComponent(runId)}/watch`, this.#base);
    url.searchParams.set("timeoutSeconds", "30");
    url.searchParams.set("maxEvents", "16");
    const response = await fetch(url, {
      method: "GET",
      headers: { accept: "application/x-ndjson" },
      signal: operationSignal(signal, WATCH_TIMEOUT_MS + 1_000),
    });
    if (!response.ok) throw new SandboxPlacementHttpError(response.status, await this.#readErrorBody(response));
    const contentType = response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
    if (contentType !== "application/x-ndjson" || !response.body) throw new Error("Placement watch did not return an NDJSON stream");

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    let count = 0;
    const parse = (line: string): SandboxSpawnResult => {
      if (line.length > MAX_NDJSON_LINE_BYTES) throw new Error("Placement watch line exceeds the response limit");
      count += 1;
      if (count > 16) throw new Error("Placement watch exceeded maxEvents=16");
      try { return JSON.parse(line) as SandboxSpawnResult; }
      catch { throw new Error("Placement watch returned invalid NDJSON"); }
    };
    try {
      while (true) {
        const { value, done } = await reader.read();
        buffered += decoder.decode(value, { stream: !done });
        if (buffered.length > MAX_NDJSON_LINE_BYTES && !buffered.includes("\n")) throw new Error("Placement watch line exceeds the response limit");
        let newline = buffered.indexOf("\n");
        while (newline >= 0) {
          const line = buffered.slice(0, newline).replace(/\r$/u, "");
          buffered = buffered.slice(newline + 1);
          if (line.length) yield parse(line);
          newline = buffered.indexOf("\n");
        }
        if (done) {
          if (buffered.trim()) yield parse(buffered.replace(/\r$/u, ""));
          return;
        }
      }
    } finally {
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
  }

  getBinding(runId: string, signal?: AbortSignal): Promise<SandboxBinding> {
    return this.#json<SandboxBinding>(`/v1/spawns/${encodeURIComponent(runId)}/binding`, { method: "GET" }, signal, REQUEST_TIMEOUT_MS);
  }

  #post(intent: Omit<SandboxSpawnIntent, "profile"> | SandboxSpawnIntent, signal?: AbortSignal): Promise<SandboxSpawnResult> {
    return this.#json<SandboxSpawnResult>("/v1/spawns", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify(intent),
    }, signal, REQUEST_TIMEOUT_MS);
  }

  async #json<T>(path: string, init: RequestInit, signal: AbortSignal | undefined, timeoutMs: number): Promise<T> {
    const response = await fetch(new URL(path, this.#base), {
      ...init,
      signal: operationSignal(signal, timeoutMs),
    });
    const text = await response.text();
    if (text.length > MAX_RESPONSE_BYTES) throw new Error("Placement response exceeds the response limit");
    let body: unknown;
    try { body = text ? JSON.parse(text) : undefined; }
    catch { throw new Error("Placement returned invalid JSON"); }
    if (!response.ok) throw new SandboxPlacementHttpError(response.status, body);
    return body as T;
  }

  async #readErrorBody(response: Response): Promise<unknown> {
    const text = await response.text();
    if (text.length > MAX_RESPONSE_BYTES) return { detail: "Placement error response exceeds the response limit" };
    try { return JSON.parse(text); }
    catch { return text.slice(0, 1_024); }
  }
}
