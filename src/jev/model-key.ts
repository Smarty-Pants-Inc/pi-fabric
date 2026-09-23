/** Fabric classifier keys, not Pi chat-provider or upstream TypeSafe/OpenRouter/Vercel model IDs. */
export const JEV_TYPESAFE_MODEL_PREFIX = "pi-fabric/typesafe/";
export const JEV_OPENROUTER_MODEL_PREFIX = "pi-fabric/openrouter/";
export const JEV_VERCEL_MODEL_PREFIX = "pi-fabric/vercel-ai-gateway/";

export type JevClassifierRoute = "typesafe" | "openrouter" | "vercel-ai-gateway";

const ROUTE_PREFIXES: ReadonlyArray<readonly [route: JevClassifierRoute, prefix: string]> = [
  ["typesafe", JEV_TYPESAFE_MODEL_PREFIX],
  ["openrouter", JEV_OPENROUTER_MODEL_PREFIX],
  ["vercel-ai-gateway", JEV_VERCEL_MODEL_PREFIX],
];

/** Keep saved pre-namespace overrides usable without moving /login jev credentials. */
export const normalizeJevApprovalModel = (key: string | undefined): string | undefined =>
  key?.startsWith("jev/") ? `${JEV_TYPESAFE_MODEL_PREFIX}${key.slice(4)}` : key;

export const parseJevApprovalModel = (
  key: string | undefined,
): { route: JevClassifierRoute; model: string } | undefined => {
  const normalized = normalizeJevApprovalModel(key);
  if (!normalized) return undefined;
  for (const [route, prefix] of ROUTE_PREFIXES) {
    if (normalized.startsWith(prefix)) return { route, model: normalized.slice(prefix.length) };
  }
  return undefined;
};

export const isJevApprovalModel = (key: string | undefined): boolean =>
  parseJevApprovalModel(key) !== undefined;
