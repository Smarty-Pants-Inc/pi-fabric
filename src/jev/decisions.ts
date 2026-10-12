import type { FabricInvocationContext } from "../protocol.js";
import type { JevFabricServe } from "../jev-fabric/serve.js";
import { runAbortable } from "../async-settlement.js";
import { JevCredentials } from "./client.js";
import type { FabricJevConfig } from "./config.js";
import { DECISION_MAX_BYTES } from "./decision-profiles.js";
import type { DecisionAPI, DecisionModel, DecisionProvider, DecisionRequest, DecisionResolution, DecisionResult, DecisionTarget } from "./decision-types.js";
import { resolveJevModelRoute } from "./routes.js";

const canonical = (provider: string): string => provider === "vercel-ai-gateway" ? "vercel" : provider === "jev" ? "typesafe" : provider;
const authProvider = (provider: string): string => ["vercel", "vercel-evaluate", "vercel-decisions"].includes(provider) ? "vercel-ai-gateway" : provider;
const envKeys: Record<string, string[]> = {
  typesafe: ["TYPESAFE_API_KEY"], openrouter: ["OPENROUTER_API_KEY", "TYPESAFE_OPENROUTER_API_KEY"],
  "vercel-ai-gateway": ["AI_GATEWAY_API_KEY"], openai: ["OPENAI_API_KEY"],
  "cloudflare-workers-ai": ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_API_KEY"], opencode: ["OPENCODE_API_KEY"],
  "llama.cpp": ["LLAMA_API_KEY"], "llama-system-one": ["LLAMA_API_KEY"],
  anthropic: ["ANTHROPIC_API_KEY"], google: ["GEMINI_API_KEY", "GOOGLE_API_KEY"], custom: ["DECISION_API_KEY"],
};
/** Host-owned connections never inherit an ambient target, resolver, or key. */
export function decisionEnvironment(config: FabricJevConfig, source: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const env = { ...source };
  for (const key of ["DECISION_PROVIDER", "DECISION_MODEL", "DECISION_ENDPOINT", "DECISION_PROFILE", "DECISION_PROFILES", "DECISION_PROFILES_FILE", "DECISION_CREDENTIAL_COMMAND", ...Object.values(envKeys).flat()]) delete env[key];
  if (config.decisionProfiles) env.DECISION_PROFILES = JSON.stringify(config.decisionProfiles);
  if (config.decisionProfile) env.DECISION_PROFILE = config.decisionProfile;
  return env;
}

type Registry = FabricInvocationContext["extensionContext"]["modelRegistry"];
type CatalogModel = { provider: string; id: string; name: string; api: string; baseUrl: string; input: readonly string[] };
function registeredTarget(model: CatalogModel, presets: readonly DecisionProvider[]): DecisionTarget | undefined {
  const api: DecisionAPI | undefined = ({ "typesafe-system-one": "system-one", "cloudflare-workers-ai-system-one": "cloudflare", "openai-decisions": "openai-decisions", "llama-cpp-classify": "llama-cpp" } as Record<string, DecisionAPI>)[model.api];
  if (!api || !model.baseUrl) return undefined;
  const provider = canonical(model.provider);
  const preset = presets.find(p => p.provider === provider);
  const nativeProvider = preset?.api === api ? provider : "custom";
  let endpoint = model.baseUrl.replace(/\/+$/, "");
  // Pi's Cloudflare catalog carries an account placeholder. Expanding an ambient
  // non-secret account id is offline; never invoke auth merely to discover it.
  const account = process.env.CLOUDFLARE_ACCOUNT_ID;
  if (api === "cloudflare" && account && /^[a-fA-F0-9]{32}$/.test(account)) endpoint = endpoint.replaceAll("{CLOUDFLARE_ACCOUNT_ID}", account);
  if (api === "system-one") endpoint += "/systemone";
  if (api === "openai-decisions") endpoint += "/decisions";
  if (api === "cloudflare") {
    endpoint += "/run";
    if (["clef", "clef-flash", "@cf/cloudflare/clef", "@cf/cloudflare/clef-flash"].includes(model.id)) endpoint += `/@cf/cloudflare/${model.id.split("/").at(-1)}`;
  }
  if (api === "llama-cpp") endpoint = endpoint.replace(/\/v1$/, "");
  return { provider: nativeProvider, api, model: model.id, endpoint };
}
function catalog(registry: Registry | undefined): readonly CatalogModel[] {
  return registry?.getModelsOfType?.("classifier") ?? [];
}
function freeze<T>(value: T): T {
  if (value && typeof value === "object") { Object.freeze(value); for (const child of Object.values(value)) freeze(child); }
  return value;
}
function snapshot<T>(value: T): T {
  const text = JSON.stringify(value, (_key, item: unknown) => {
    if ((typeof item === "number" && !Number.isFinite(item)) || ["bigint", "function", "symbol", "undefined"].includes(typeof item)) throw new Error("Decision requests must contain finite JSON values");
    return item;
  });
  if (!text || Buffer.byteLength(text, "utf8") > DECISION_MAX_BYTES) throw new Error("Decision request exceeds 16 MiB JSON limit");
  return freeze(JSON.parse(text) as T);
}
/** Unknown counters are not zero. This does not rewrite any returned evidence. */
export function decisionUsage(result: DecisionResult): { input: number | null; output: number | null; exhausted: boolean } {
  const count = (v: unknown): number | null => typeof v === "number" && Number.isSafeInteger(v) && v >= 0 ? v : null;
  const input = count(result?.usage?.input_tokens), output = count(result?.usage?.output_tokens);
  return { input, output, exhausted: input === null || output === null || result?.budget?.exhausted === true };
}

/** Auth and trust adapter only. All decision parsing/provenance stays in the native engine. */
export class JevDecisions {
  readonly #config: FabricJevConfig;
  readonly #commands = new Map<string, JevCredentials>();
  readonly #exhausted = new WeakSet<JevFabricServe>();
  constructor(config: FabricJevConfig) { this.#config = structuredClone(config); }
  /** Freeze the selected default for a newly launched managed program. */
  fork(): JevDecisions { return new JevDecisions(this.#config); }
  /** Trusted UI-only selector update. Existing request snapshots and budgets survive. */
  setProfile(profile: string | null | undefined): void {
    if (profile !== undefined && profile !== null && (!/^[A-Za-z0-9_.-]{1,128}$/.test(profile) || !this.#config.decisionProfiles || !Object.hasOwn(this.#config.decisionProfiles.profiles, profile)))
      throw new Error("jev.decisionProfile must name a known decision profile");
    if (profile === undefined) delete this.#config.decisionProfile;
    else this.#config.decisionProfile = profile;
  }
  async providers(serve: JevFabricServe, signal?: AbortSignal): Promise<DecisionProvider[]> {
    return serve.request("decisionProviders", {}, signal);
  }
  #profile(request: Partial<DecisionRequest>): string | undefined {
    const profile = request.profile ?? (request.provider === undefined ? this.#config.decisionProfile ?? this.#config.decisionProfiles?.defaultProfile : undefined);
    if (profile !== undefined) {
      const profiles = this.#config.decisionProfiles?.profiles;
      const target = profiles && Object.hasOwn(profiles, profile) ? profiles[profile] : undefined;
      if (!target) throw new Error("Unknown decision profile");
      if (request.provider !== undefined && canonical(request.provider) !== canonical(target.provider)) throw new Error("Decision profile and provider disagree");
    }
    return profile;
  }
  async #resolve(serve: JevFabricServe, original: Partial<DecisionRequest>, context: FabricInvocationContext): Promise<{ resolution: DecisionResolution; request: Partial<DecisionRequest>; auth: string }> {
    const request = snapshot(original);
    const profile = this.#profile(request);
    const trustedProfile = profile ? this.#config.decisionProfiles!.profiles[profile] : undefined;
    // Whole-field overlay, matching the standalone contract. In particular,
    // providerOptions is replaced, never deep-merged with a profile's options.
    const effective: Partial<DecisionRequest> = { ...trustedProfile, ...request };
    delete effective.profile;
    const selectedProvider = canonical(effective.provider ?? "typesafe");
    const selectedModel = effective.model;
    const presets = await this.providers(serve, context.signal);
    const models = catalog(context.extensionContext.modelRegistry);
    const registered = models.find(m => canonical(m.provider) === selectedProvider && m.id === selectedModel);
    const target = registered ? registeredTarget(registered, presets) : undefined;
    // A profile's explicit endpoint wins over registry metadata. Guest endpoints never
    // acquire authority merely by sharing an origin with either route.
    const baseline: Partial<DecisionRequest> = { ...effective, provider: selectedProvider };
    delete baseline.endpoint;
    if (trustedProfile?.endpoint) {
      baseline.endpoint = trustedProfile.endpoint;
      // The trusted profile author selected the credential owner. A Pi-only
      // provider is projected to a native adapter, never used as an auth alias.
      if (!presets.some(p => p.provider === selectedProvider)) {
        baseline.provider = "custom";
        if (baseline.api === undefined && target?.api) baseline.api = target.api;
      }
    } else if (target) {
      if (effective.api !== undefined && effective.api !== target.api) throw new Error("Decision API differs from the registered classifier target");
      Object.assign(baseline, target);
    }
    if (selectedProvider === "custom" && !trustedProfile && !target) throw new Error("Custom decision targets require a trusted profile or registered classifier");
    // Native resolution is OFFLINE and precedes every credential lookup.
    const raw = await serve.request<DecisionResolution>("resolveDecision", { request: baseline }, context.signal);
    const resolvedEndpoint = raw.target.endpoint;
    if (request.endpoint !== undefined && request.endpoint !== resolvedEndpoint) throw new Error("Decision endpoint must exactly match a trusted profile, registered classifier route, or preset default");
    // Project only the fixed resolution contract: never echo state/questions/images,
    // auth results, or arbitrary native fields through this read-only operation.
    const cleanTarget: DecisionResolution["target"] = {
      provider: raw.target.provider, api: raw.target.api, model: raw.target.model, endpoint: raw.target.endpoint,
      allowLocal: raw.target.allowLocal, allowGenerated: raw.target.allowGenerated,
      ...(raw.target.providerOptions === undefined ? {} : { providerOptions: raw.target.providerOptions }),
      ...(raw.target.temperature === undefined ? {} : { temperature: raw.target.temperature }),
    };
    const resolution = freeze({ target: cleanTarget, credential: { env: raw.credential.env, resolved: false as const }, imageSupport: raw.imageSupport, generated: raw.generated, ...(profile ? { profile } : {}) });
    const prepared = freeze({ ...effective, ...resolution.target });
    // Never infer an auth provider from a URL/model match. A profile declaring
    // provider:custom uses only custom auth, even if its model resembles another
    // registered vendor. To use Pi login, name that vendor in the trusted profile.
    return { resolution, request: prepared, auth: authProvider(selectedProvider) };
  }
  async resolve(serve: JevFabricServe, request: Partial<DecisionRequest>, context: FabricInvocationContext): Promise<DecisionResolution> {
    return (await this.#resolve(serve, request, context)).resolution;
  }
  async models(serve: JevFabricServe, context: FabricInvocationContext): Promise<DecisionModel[]> {
    const presets = await this.providers(serve, context.signal);
    const registry = context.extensionContext.modelRegistry;
    return Promise.all(catalog(registry).map(async model => {
      const target = registeredTarget(model, presets);
      let capabilities: DecisionResolution | undefined;
      if (target) {
        try {
          // Use the engine's real offline preflight, including endpoint-specific
          // image exclusions and local/generated opt-ins. No model requests or auth.
          capabilities = await serve.request<DecisionResolution>("resolveDecision", { request: {
            ...target, ...(target.api === "llama-cpp" || target.api === "llama-system-one" ? { allowLocal: true } : {}),
            state: {}, questions: { capability: { type: "boolean", instructions: "Offline capability validation only" } },
          } }, context.signal);
        } catch (error) { if (context.signal?.aborted || serve.closed) throw error; }
      }
      const support = capabilities?.imageSupport ?? "unsupported";
      const provider = authProvider(canonical(model.provider));
      // Presence only: don't use getAvailableOfType, getProviderAuth, or key resolvers.
      const configured = (provider === "typesafe" && registry?.getProviderAuthStatus?.("jev")?.configured) || registry?.getProviderAuthStatus?.(provider)?.configured || (envKeys[provider] ?? []).some(key => !!process.env[key]?.trim()) || (provider === authProvider(canonical(resolveJevModelRoute(this.#config.model).route.id)) && this.#config.credentialCommand.length > 0);
      return {
        model: { provider: model.provider, id: model.id }, name: model.name, api: model.api,
        ...(target ? { target: { ...target, provider: model.provider } } : {}), supported: !!capabilities,
        imageSupport: !model.input.includes("image") || support === "unsupported" || canonical(model.provider) === "openrouter" ? "unsupported" : support,
        generated: capabilities?.generated ?? false, logits: target?.api === "llama-cpp",
        credentials: { configured: !!configured, verified: false },
      };
    }));
  }
  async #credential(provider: string, registry: Registry | undefined, signal: AbortSignal, local: boolean): Promise<string> {
    signal.throwIfAborted();
    try {
      // Jev's auth-only TypeSafe login is an alias, never a cross-provider fallback.
      const ids = provider === "typesafe" ? ["jev", "typesafe"] : [provider];
      for (const id of ids) {
        const key = await runAbortable(signal, () => registry?.getApiKeyForProvider?.(id));
        if (key?.trim()) return key.trim();
      }
    } catch { throw new Error("Decision Pi credential resolution failed"); }
    const keys = envKeys[provider] ?? [];
    for (const key of keys) if (process.env[key]?.trim()) return process.env[key]!.trim();
    // The legacy command is explicitly bound to its configured provider, not a
    // wildcard that can accidentally send TypeSafe credentials to another vendor.
    const legacyProvider = authProvider(canonical(resolveJevModelRoute(this.#config.model).route.id));
    if (provider === legacyProvider && this.#config.credentialCommand.length) {
      let credentials = this.#commands.get(provider);
      if (!credentials) { credentials = new JevCredentials(this.#config.credentialCommand, {}, undefined, keys); this.#commands.set(provider, credentials); }
      return credentials.resolve(signal);
    }
    if (local) return "";
    throw new Error("Decision credentials unavailable for the selected provider");
  }
  async decide(serve: JevFabricServe, request: DecisionRequest, context: FabricInvocationContext): Promise<DecisionResult> {
    if (this.#exhausted.has(serve)) throw new Error("Decision inference budget exhausted or prior usage unknown");
    const signal = AbortSignal.any([...(context.signal ? [context.signal] : []), AbortSignal.timeout(this.#config.requestTimeoutMs)]);
    const selected = await this.#resolve(serve, request, { ...context, signal });
    const url = new URL(selected.resolution.target.endpoint);
    const local = selected.resolution.target.allowLocal && ["127.0.0.1", "[::1]"].includes(url.hostname) && !["anthropic", "google"].includes(selected.resolution.target.api);
    const credential = await this.#credential(selected.auth, context.extensionContext.modelRegistry, signal, local);
    signal.throwIfAborted();
    if (this.#exhausted.has(serve)) throw new Error("Decision inference budget exhausted or prior usage unknown");
    try {
      const result = await serve.request<DecisionResult>("decide", { request: selected.request, timeoutMs: this.#config.requestTimeoutMs, credential }, signal);
      if (decisionUsage(result).exhausted) this.#exhausted.add(serve);
      return result; // No legacy checkResponse, projection, truncation or invented counters.
    } catch (error) {
      this.#exhausted.add(serve);
      if (signal.aborted) await serve.close();
      throw error;
    }
  }
  close(): void { for (const credentials of this.#commands.values()) credentials.clear(); this.#commands.clear(); }
}
