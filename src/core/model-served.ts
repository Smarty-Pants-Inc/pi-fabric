import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { FabricModelNotServedError } from "./model-served-error.js";

export type ModelVerification = "verified" | "unverified";
type Registry = Pick<ModelRegistry, "find" | "getAvailable"> & Partial<Pick<ModelRegistry,
  "getProvider" | "refresh" | "getApiKeyAndHeaders">>;
const TTL_MS = 10 * 60_000;
const DISCOVERY_TIMEOUT_MS = 3_000;
type Catalog = readonly string[] | undefined;
const catalogs = new WeakMap<object, Map<string, { expiresAt: number; result: Promise<Catalog> }>>();

/** Reuse Pi's provider catalog and request-auth facade; no second credential store or model resolver.
 * Static OpenAI-compatible configurations can be stale, so check their live /models endpoint too.
 * Discovery is bounded, coalesced and cached (including outages). Only a valid list can refuse work.
 */
export async function verifyPiSpawnModel(selector: string, registry: Registry): Promise<ModelVerification> {
  const separator = selector.indexOf("/");
  if (separator <= 0) return "unverified";
  const providerId = selector.slice(0, separator), id = selector.slice(separator + 1);
  let model: ReturnType<Registry["find"]>;
  try {
    model = registry.find(providerId, id);
    if (!model && !registry.getAvailable().some(model => model.provider === providerId && model.id === id)) {
      throw new FabricModelNotServedError(selector);
    }
  } catch (error) {
    if (error instanceof FabricModelNotServedError) throw error;
    return "unverified";
  }
  // Minimal host registries and static non-OpenAI providers use Pi's registered list directly.
  if (!model) return "verified";
  let provider: ReturnType<NonNullable<Registry["getProvider"]>>;
  try { provider = registry.getProvider?.(providerId); } catch { return "unverified"; }
  const dynamic = Boolean(provider?.refreshModels && registry.refresh);
  const openai = model.api === "openai-completions" || model.api === "openai-responses";
  if (!dynamic && !openai) return "verified";
  if (!dynamic && (!model.baseUrl || !registry.getApiKeyAndHeaders)) return "unverified";

  const key = `${providerId}:${model.baseUrl ?? provider?.baseUrl ?? "provider-catalog"}`;
  let cache = catalogs.get(registry);
  if (!cache) { cache = new Map(); catalogs.set(registry, cache); }
  const now = Date.now();
  for (const [key, entry] of cache) if (entry.expiresAt <= now) cache.delete(key);
  let pending = cache.get(key);
  if (!pending) {
    const signal = AbortSignal.timeout(DISCOVERY_TIMEOUT_MS);
    const discover = async (): Promise<Catalog> => {
      if (dynamic) {
        // Pi already owns dynamic discovery (refreshModels, model publication and auth).
        const result = await registry.refresh!({ providers: [providerId], allowNetwork: true, signal });
        if (result.aborted || result.errors.size) return undefined;
        return registry.getProvider!(providerId)?.getModels().map(model => model.id);
      }
      const auth = await registry.getApiKeyAndHeaders!(model!);
      if (!auth.ok) return undefined;
      const url = new URL(`${(auth.baseUrl ?? model!.baseUrl).replace(/\/+$/, "")}/models`);
      if (url.protocol !== "http:" && url.protocol !== "https:") return undefined;
      const headers = new Headers();
      if (auth.apiKey) headers.set("authorization", `Bearer ${auth.apiKey}`);
      for (const [name, value] of Object.entries(auth.headers ?? {})) {
        if (value === null) headers.delete(name); else headers.set(name, value);
      }
      const response = await fetch(url, { headers, signal, redirect: "error" });
      if (!response.ok) return undefined;
      const body: unknown = await response.json();
      if (!body || typeof body !== "object" || !("data" in body) || !Array.isArray(body.data) ||
        !body.data.every(entry => entry && typeof entry === "object" && typeof entry.id === "string")) return undefined;
      return body.data.map(entry => entry.id as string);
    };
    // Bound authentication/third-party refresh too, even if they ignore the fetch signal.
    const result = new Promise<Catalog>(resolve => {
      signal.addEventListener("abort", () => resolve(undefined), { once: true });
      void discover().then(resolve, () => resolve(undefined));
    });
    pending = { expiresAt: now + TTL_MS, result };
    cache.set(key, pending);
  }
  const ids = await pending.result;
  if (!ids) return "unverified";
  if (!ids.includes(id)) throw new FabricModelNotServedError(selector);
  return "verified";
}
