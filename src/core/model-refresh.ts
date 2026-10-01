// ponytail: a models.json edit must not need a fleet reload before any participant (task agent
// or actor) can use the new model (smarty-dev#1830). On a miss, reload the registry once and
// resolve again. Concurrent misses on one registry share one refresh, and refreshes run at most
// once per 10 s, so a typo in a loop fails fast instead of rereading models.json on every call.
import {
  resolveAvailablePiModel,
  type FabricModelAliases,
  type FabricModelCandidate,
} from "./model-resolution.js";
import { loadModelUsage } from "./model-usage.js";

import { assertFabricModelAllowed, type FabricModelPolicy } from "./model-policy.js";

const PI_MODEL_REFRESH_INTERVAL_MS = 10_000;

interface RefreshableModelRegistry {
  refresh?: () => unknown;
}

const inFlight = new WeakMap<object, Promise<unknown>>();
const lastRefreshAt = new WeakMap<object, number>();

const refreshOnce = (registry: RefreshableModelRegistry): Promise<unknown> | undefined => {
  const pending = inFlight.get(registry);
  if (pending) return pending;
  if (Date.now() - (lastRefreshAt.get(registry) ?? Number.NEGATIVE_INFINITY) < PI_MODEL_REFRESH_INTERVAL_MS) {
    return undefined;
  }
  lastRefreshAt.set(registry, Date.now());
  const refresh = Promise.resolve()
    .then(() => registry.refresh?.())
    .catch(() => undefined)
    .finally(() => inFlight.delete(registry));
  inFlight.set(registry, refresh);
  return refresh;
};

/**
 * Resolve against the registry. The first pass is exact (`resolve(true)` must throw a "not
 * available" miss rather than fuzzy-match an absent exact model), so a model just added to
 * models.json is not replaced by a similar stale one. On that miss, refresh once and resolve the
 * original selector again with fuzzy matching allowed (`resolve(false)`).
 */
export const resolveWithModelRefresh = async <T>(
  registry: RefreshableModelRegistry | undefined,
  resolve: (exact: boolean) => T,
): Promise<T> => {
  if (!registry) return resolve(false);
  try {
    return resolve(true);
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes("is not available to this Pi session")) {
      throw error;
    }
    const refresh = refreshOnce(registry);
    if (refresh) await refresh;
    return resolve(false);
  }
};

/** A Pi `ModelRegistry`: the models this process can run (auth-filtered) and a reload. */
export interface PiModelRegistryView extends RefreshableModelRegistry {
  getAvailable(): readonly { provider: unknown; id: unknown; name?: unknown }[];
}

const registryCandidates = (registry: PiModelRegistryView | undefined): FabricModelCandidate[] => {
  try {
    return (registry?.getAvailable() ?? []).map((model) => ({
      provider: String(model.provider),
      id: String(model.id),
      ...(typeof model.name === "string" ? { name: model.name } : {}),
    }));
  } catch {
    return []; // The authoritative visible set is empty when registry discovery fails.
  }
};

/**
 * The one Pi participant model resolver, shared by the session runtime, the agents provider and
 * the durable resident host: exact first, then one shared refresh of `registry`, then the
 * original selector again. A resident host passes `snapshot` (the session's visible models at
 * the last sync): the exact pass sees only that, and after a miss its own refreshed registry
 * joins it, so a model added to models.json after the host started resolves without a reload,
 * while a model the host process cannot run stays unavailable (pi-fabric#138).
 */
export const resolvePiModel = (options: {
  selector?: string | undefined;
  registry?: PiModelRegistryView | undefined;
  aliases: FabricModelAliases;
  defaultModel?: string | undefined;
  snapshot?: readonly FabricModelCandidate[] | undefined;
  policy?: FabricModelPolicy;
}): Promise<FabricModelCandidate> => {
  const query = options.selector?.trim() || options.defaultModel?.trim() || "";
  assertFabricModelAllowed(query, options.policy);
  return resolveWithModelRefresh(options.registry, (exact) => {
    const live = registryCandidates(options.registry);
    const available = !options.snapshot
      ? live
      : exact
        ? options.snapshot
        : [...options.snapshot, ...live.filter((model) => !options.snapshot!.some((known) =>
            known.provider.toLowerCase() === model.provider.toLowerCase() &&
            known.id.toLowerCase() === model.id.toLowerCase()))];
    const resolved = resolveAvailablePiModel(query, {
      aliases: options.aliases,
      available,
      lastUsed: loadModelUsage(),
      exact,
    });
    assertFabricModelAllowed(`${resolved.provider}/${resolved.id}`, options.policy);
    return resolved;
  });
};
