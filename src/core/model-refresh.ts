// ponytail: a models.json edit must not need a fleet reload before any participant (task agent
// or actor) can use the new model (smarty-dev#1830). On a miss, reload the registry once and
// resolve again. Concurrent misses on one registry share one refresh, and refreshes run at most
// once per 10 s, so a typo in a loop fails fast instead of rereading models.json on every call.
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

/** Resolve against the registry; on a "not available" miss, refresh it once and retry. */
export const resolveWithModelRefresh = async <T>(
  registry: RefreshableModelRegistry | undefined,
  resolve: () => T,
): Promise<T> => {
  try {
    return resolve();
  } catch (error) {
    if (!registry || !(error instanceof Error) || !error.message.includes("is not available to this Pi session")) {
      throw error;
    }
    const refresh = refreshOnce(registry);
    if (!refresh) throw error;
    await refresh;
    return resolve();
  }
};
