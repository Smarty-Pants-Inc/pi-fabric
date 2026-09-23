import {
  JEV_OPENROUTER_MODEL_PREFIX,
  JEV_TYPESAFE_MODEL_PREFIX,
  JEV_VERCEL_MODEL_PREFIX,
  parseJevApprovalModel,
  type JevClassifierRoute,
} from "./model-key.js";

export interface JevRoute {
  readonly id: JevClassifierRoute;
  /** Human label used in host-visible errors and status. */
  readonly label: string;
  /** Fixed HTTPS endpoint receiving the typed System One request. */
  readonly endpoint: string;
  /** Pi auth provider whose auth.json login and environment keys authenticate this route. */
  readonly providerId: string;
  /** Environment fallbacks, checked when Pi provider auth is not configured. */
  readonly envKeys: readonly string[];
  /** Classifier key prefix stored in `approvals.model` for this route. */
  readonly modelPrefix: string;
  /** Aliases offered in host pickers and accepted as upstream overrides. */
  readonly aliases: readonly string[];
}

/** Direct TypeSafe aliases. All of them resolve to `jev-1.13.0` today. */
export const JEV_TYPESAFE_MODELS: readonly string[] = ["jev-latest", "jev-1.13", "jev-1.13.0", "jev-preview"];

export const JEV_TYPESAFE_ROUTE: JevRoute = {
  id: "typesafe",
  label: "TypeSafe",
  endpoint: "https://api.typesafe.ai/v1/systemone",
  providerId: "jev",
  envKeys: ["TYPESAFE_API_KEY"],
  modelPrefix: JEV_TYPESAFE_MODEL_PREFIX,
  aliases: JEV_TYPESAFE_MODELS,
};

/** OpenRouter decisions model IDs by alias. OpenRouter has no `jev-preview` alias. */
export const JEV_OPENROUTER_MODELS: Readonly<Record<string, string>> = {
  "jev-latest": "~typesafe/jev-latest",
  "jev-1.13": "typesafe/jev-1.13",
};

/**
 * OpenRouter serves decisions models on its Decisions API, not chat completions.
 * The user's existing openrouter auth (`/login openrouter`, `OPENROUTER_API_KEY`)
 * is the primary credential; `TYPESAFE_OPENROUTER_API_KEY` is an explicit fallback.
 */
export const JEV_OPENROUTER_ROUTE: JevRoute = {
  id: "openrouter",
  label: "OpenRouter",
  endpoint: "https://openrouter.ai/api/alpha/decisions",
  providerId: "openrouter",
  envKeys: ["OPENROUTER_API_KEY", "TYPESAFE_OPENROUTER_API_KEY"],
  modelPrefix: JEV_OPENROUTER_MODEL_PREFIX,
  aliases: Object.keys(JEV_OPENROUTER_MODELS),
};

/** Vercel AI Gateway evaluation model IDs by alias. The gateway serves one Jev build today. */
export const JEV_VERCEL_MODELS: Readonly<Record<string, string>> = {
  "jev-latest": "typesafe-ai/jev",
};

/**
 * Vercel AI Gateway exposes a TypeSafe-compatible endpoint, so request and
 * response shapes stay TypeSafe's own and only the base URL and credential
 * change. The existing Pi `vercel-ai-gateway` login authenticates it.
 */
export const JEV_VERCEL_ROUTE: JevRoute = {
  id: "vercel-ai-gateway",
  label: "Vercel AI Gateway",
  endpoint: "https://ai-gateway.vercel.sh/typesafe/v1/systemone",
  providerId: "vercel-ai-gateway",
  envKeys: ["AI_GATEWAY_API_KEY"],
  modelPrefix: JEV_VERCEL_MODEL_PREFIX,
  aliases: Object.keys(JEV_VERCEL_MODELS),
};

const JEV_ROUTES: Readonly<Record<JevClassifierRoute, JevRoute>> = {
  "typesafe": JEV_TYPESAFE_ROUTE,
  "openrouter": JEV_OPENROUTER_ROUTE,
  "vercel-ai-gateway": JEV_VERCEL_ROUTE,
};

/** Picker order: direct TypeSafe aliases, then OpenRouter, then Vercel AI Gateway. */
const JEV_ROUTE_ORDER: readonly JevRoute[] = [JEV_TYPESAFE_ROUTE, JEV_OPENROUTER_ROUTE, JEV_VERCEL_ROUTE];

const JEV_PROVIDER_PREFIX = "pi-fabric/";

/** Classifier key stored in `approvals.model` for one route and upstream model. */
export const jevClassifierKey = (route: JevRoute, model: string): string => `${route.modelPrefix}${model}`;

const TYPESAFE_MODEL = /^[a-zA-Z0-9._-]{1,128}$/;
/** OpenRouter decisions IDs are `~typesafe/<family>` or `typesafe/<build>`. */
export const isJevOpenRouterModelId = (model: string): boolean =>
  /^~?typesafe\/[a-zA-Z0-9._-]{1,120}$/.test(model);
/** Vercel AI Gateway model IDs are `typesafe-ai/<family>`. */
export const isJevVercelModelId = (model: string): boolean =>
  /^typesafe-ai\/[a-zA-Z0-9._-]{1,120}$/.test(model);
/** A bare TypeSafe alias or a routable OpenRouter/Vercel AI Gateway model ID. */
export const isJevModelId = (model: string): boolean =>
  TYPESAFE_MODEL.test(model) || isJevOpenRouterModelId(model) || isJevVercelModelId(model);

export interface JevRouteTarget {
  readonly route: JevRoute;
  readonly model: string;
}

/** Route and upstream model ID for a raw `jev.model` value. */
export const resolveJevModelRoute = (model: string): JevRouteTarget => {
  if (isJevOpenRouterModelId(model)) return { route: JEV_OPENROUTER_ROUTE, model };
  if (isJevVercelModelId(model)) return { route: JEV_VERCEL_ROUTE, model };
  return { route: JEV_TYPESAFE_ROUTE, model };
};

/** Upstream model ID for an alias or override on an already-selected route. */
export const resolveJevUpstreamModel = (route: JevRoute, model: string): string | undefined => {
  if (route.id === "openrouter") return JEV_OPENROUTER_MODELS[model] ?? (isJevOpenRouterModelId(model) ? model : undefined);
  if (route.id === "vercel-ai-gateway") return JEV_VERCEL_MODELS[model] ?? (isJevVercelModelId(model) ? model : undefined);
  return TYPESAFE_MODEL.test(model) ? model : undefined;
};

export type JevClassifierTarget =
  | { readonly ok: true; readonly route: JevRoute; readonly model: string }
  | { readonly ok: false; readonly message: string };

const unavailableModelMessage = (route: JevRoute, model: string): string => {
  if (route.id === "openrouter") return `OpenRouter serves ${Object.keys(JEV_OPENROUTER_MODELS).join(", ")}; "${model}" is not available`;
  if (route.id === "vercel-ai-gateway") return `Vercel AI Gateway serves ${Object.keys(JEV_VERCEL_MODELS).join(", ")}; "${model}" is not available`;
  return `Invalid Jev auto-approval model; use ${JEV_TYPESAFE_MODEL_PREFIX}<model-id>`;
};

/** Route and upstream model ID for a stored `pi-fabric/<route>/<model>` approvals key. */
export const resolveJevClassifierTarget = (key: string): JevClassifierTarget => {
  const parsed = parseJevApprovalModel(key);
  if (!parsed) {
    return {
      ok: false,
      message: `Invalid Jev auto-approval model; use ${JEV_TYPESAFE_MODEL_PREFIX}<model-id>, ${JEV_OPENROUTER_MODEL_PREFIX}<model-id>, or ${JEV_VERCEL_MODEL_PREFIX}<model-id>`,
    };
  }
  const route = JEV_ROUTES[parsed.route];
  const model = resolveJevUpstreamModel(route, parsed.model);
  return model ? { ok: true, route, model } : { ok: false, message: unavailableModelMessage(route, parsed.model) };
};

export interface JevClassifierPickerModel {
  readonly provider: "pi-fabric";
  readonly id: string;
  readonly name: string;
}

/** Approvals picker entries: every supported classifier key plus the configured model. */
export const jevClassifierModels = (configuredModel?: string): JevClassifierPickerModel[] => {
  const configured = configuredModel ? resolveJevModelRoute(configuredModel) : undefined;
  return JEV_ROUTE_ORDER.flatMap((route) => {
    const models = new Set(route.aliases);
    if (configured?.route.id === route.id && configuredModel) models.add(configuredModel);
    return [...models].map((model): JevClassifierPickerModel => ({
      provider: "pi-fabric", id: jevClassifierKey(route, model).slice(JEV_PROVIDER_PREFIX.length),
      name: `Jev (${route.label} safety classifier · ${model})`,
    }));
  });
};
