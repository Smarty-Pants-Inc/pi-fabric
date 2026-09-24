import { boundedEffectResources, cleanupState, consume } from "../verified/policy.js";
import type {
  FabricComponentDisposer,
  FabricComponentEffect,
  FabricComponentEffectInfo,
  FabricComponentEffectRegistration,
} from "./types.js";

interface FabricEffectFailure {
  label: string;
  error: string;
}

export interface FabricEffectCleanupReport {
  status: "disposed" | "quarantined";
  failures: FabricEffectFailure[];
}

export type FabricEffectGuard = () => boolean | Promise<boolean>;

export interface FabricEffectScopeOptions {
  guard?: FabricEffectGuard;
}

export interface FabricEffectLifecycleHooks {
  beforeCleanup?(): void;
}

export class FabricEffectDivertedError extends Error {
  readonly cleanupError: unknown;

  constructor(
    message = "Fabric effect target changed at an iteration boundary",
    cleanupError?: unknown,
  ) {
    super(message);
    this.name = "FabricEffectDivertedError";
    this.cleanupError = cleanupError;
  }
}

interface EffectRecord {
  label: string;
  effect?: FabricComponentEffectInfo;
  disposers: FabricComponentDisposer[];
  setup: Promise<void>;
  dispose: () => Promise<void>;
  disposed: boolean;
  armed: boolean;
  cleanupStarted: boolean;
  beforeCleanup?: () => void;
}

interface EffectIterator {
  next(): IteratorResult<unknown> | Promise<IteratorResult<unknown>>;
  return?(): IteratorResult<unknown> | Promise<IteratorResult<unknown>>;
}

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const isPromiseLike = (value: unknown): value is PromiseLike<unknown> =>
  typeof value === "object" &&
  value !== null &&
  "then" in value &&
  typeof (value as { then?: unknown }).then === "function";

const isIterable = (value: unknown): value is Iterable<unknown> =>
  typeof value === "object" &&
  value !== null &&
  Symbol.iterator in value &&
  typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function";

const isAsyncIterable = (value: unknown): value is AsyncIterable<unknown> =>
  typeof value === "object" &&
  value !== null &&
  Symbol.asyncIterator in value &&
  typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function";

const collectDisposer = (
  value: unknown,
  disposers: FabricComponentDisposer[],
): void => {
  if (value === undefined || value === null) return;
  if (typeof value !== "function") throw new TypeError("Fabric effect yielded an invalid disposer");
  disposers.push(value as FabricComponentDisposer);
};

const normalizeRegistration = (
  registration: FabricComponentEffectRegistration | undefined,
  fallbackLabel: string,
): { label: string; effect?: FabricComponentEffectInfo } => {
  if (typeof registration === "string") return { label: registration };
  if (!registration) return { label: fallbackLabel };
  const resources = boundedEffectResources(registration.resources);
  const label = registration.label?.trim().slice(0, 256) || fallbackLabel;
  return {
    label,
    effect: {
      label,
      kind: registration.kind ?? "transactional",
      resources,
      ordering: registration.ordering ?? "unknown",
    },
  };
};

const beginCleanup = (record: EffectRecord): void => {
  if (record.cleanupStarted) return;
  record.cleanupStarted = true;
  record.beforeCleanup?.();
};

const closeIterator = async (
  iterator: EffectIterator,
  disposers: FabricComponentDisposer[],
): Promise<void> => {
  if (!iterator.return) return;
  let step = await iterator.return();
  while (!step.done) {
    collectDisposer(step.value, disposers);
    step = await iterator.next();
  }
};

const checkTarget = async (guard: FabricEffectGuard | undefined): Promise<void> => {
  if (guard && !(await guard())) throw new FabricEffectDivertedError();
};

const driveIterator = async (
  iterator: EffectIterator,
  record: EffectRecord,
  guard: FabricEffectGuard | undefined,
): Promise<void> => {
  try {
    for (;;) {
      if (!record.armed) {
        beginCleanup(record);
        await closeIterator(iterator, record.disposers);
        return;
      }
      await checkTarget(guard);
      const step = await iterator.next();
      if (!step.done) collectDisposer(step.value, record.disposers);
      if (!record.armed) {
        beginCleanup(record);
        await closeIterator(iterator, record.disposers);
        return;
      }
      if (step.done) {
        await checkTarget(guard);
        return;
      }
    }
  } catch (error) {
    if (error instanceof FabricEffectDivertedError) {
      try {
        beginCleanup(record);
        await closeIterator(iterator, record.disposers);
      } catch (closeError) {
        throw new FabricEffectDivertedError(
          "Fabric effect target changed and iterator close failed",
          closeError,
        );
      }
    }
    throw error;
  }
};

const collectEffect = async (
  effect: FabricComponentEffect,
  record: EffectRecord,
  guard: FabricEffectGuard | undefined,
): Promise<void> => {
  const resolved = isPromiseLike(effect) ? await effect : effect;
  if (resolved === undefined || resolved === null || typeof resolved === "function") {
    collectDisposer(resolved, record.disposers);
    if (record.armed) await checkTarget(guard);
    return;
  }
  if (isAsyncIterable(resolved)) {
    await driveIterator(resolved[Symbol.asyncIterator](), record, guard);
    return;
  }
  if (isIterable(resolved)) {
    await driveIterator(resolved[Symbol.iterator](), record, guard);
    return;
  }
  throw new TypeError("Fabric effect returned an unsupported value");
};

export class FabricEffectScope {
  readonly #records: EffectRecord[] = [];
  readonly #setupCleanupFailures: FabricEffectFailure[] = [];
  readonly #guard: FabricEffectGuard | undefined;
  #state: "open" | "disposing" | "disposed" = "open";
  #cleanup: Promise<FabricEffectCleanupReport> | undefined;

  constructor(options: FabricEffectScopeOptions = {}) {
    this.#guard = options.guard;
  }

  get state(): "open" | "disposing" | "disposed" {
    return this.#state;
  }

  footprint(limit = Number.POSITIVE_INFINITY): FabricComponentEffectInfo[] {
    const effects: FabricComponentEffectInfo[] = [];
    for (const record of this.#records) {
      if (effects.length >= limit) break;
      if (!record.disposed && record.effect) {
        effects.push({ ...record.effect, resources: [...record.effect.resources] });
      }
    }
    return effects;
  }

  async effect(
    setup: () => FabricComponentEffect,
    registration: FabricComponentEffectRegistration = "anonymous",
    hooks: FabricEffectLifecycleHooks = {},
  ): Promise<FabricComponentDisposer> {
    if (this.#state !== "open") {
      throw new Error("Cannot create an effect on a disposing Fabric scope");
    }

    const normalized = normalizeRegistration(registration, "anonymous");
    const record: EffectRecord = {
      label: normalized.label,
      ...(normalized.effect ? { effect: normalized.effect } : {}),
      disposers: [],
      setup: Promise.resolve(),
      dispose: async () => {},
      disposed: false,
      armed: true,
      cleanupStarted: false,
      ...(hooks.beforeCleanup ? { beforeCleanup: hooks.beforeCleanup } : {}),
    };
    const cleanupDisposers = async (): Promise<void> => {
      beginCleanup(record);
      const failures: unknown[] = [];
      for (const disposer of record.disposers.splice(0).reverse()) {
        try {
          await disposer();
        } catch (error) {
          failures.push(error);
        }
      }
      if (failures.length > 0) {
        throw new AggregateError(failures, `Fabric effect cleanup failed: ${record.label}`);
      }
    };

    let disposal: Promise<void> | undefined;
    record.dispose = async () => {
      const release = consume(!record.disposed);
      if (!release.fst) return disposal;
      record.disposed = !release.snd;
      record.armed = false;
      disposal = (async () => {
        await record.setup.catch(() => undefined);
        await cleanupDisposers();
      })();
      return disposal;
    };

    this.#records.push(record);
    record.setup = (async () => {
      try {
        if (this.#guard) await checkTarget(this.#guard);
        if (!record.armed) return;
        await collectEffect(setup(), record, this.#guard);
      } catch (error) {
        try {
          await cleanupDisposers();
        } catch (cleanupError) {
          const failures = cleanupError instanceof AggregateError
            ? cleanupError.errors
            : [cleanupError];
          for (const failure of failures) {
            this.#setupCleanupFailures.push({ label: record.label, error: errorMessage(failure) });
          }
          throw new AggregateError(
            [error, cleanupError],
            `Fabric effect setup and rollback failed: ${record.label}`,
          );
        }
        throw error;
      }
    })();

    try {
      await record.setup;
      if (this.#state === "open") {
        const index = this.#records.indexOf(record);
        if (index >= 0 && index !== this.#records.length - 1) {
          this.#records.splice(index, 1);
          this.#records.push(record);
        }
      }
    } catch (error) {
      const index = this.#records.indexOf(record);
      if (index >= 0) this.#records.splice(index, 1);
      throw error;
    }
    return record.dispose;
  }

  defer(
    disposer: FabricComponentDisposer,
    registration: FabricComponentEffectRegistration = "deferred",
  ): FabricComponentDisposer {
    if (this.#state !== "open") {
      throw new Error("Cannot defer cleanup on a disposing Fabric scope");
    }
    const normalized = normalizeRegistration(registration, "deferred");
    const record: EffectRecord = {
      label: normalized.label,
      ...(normalized.effect ? { effect: normalized.effect } : {}),
      disposers: [disposer],
      setup: Promise.resolve(),
      dispose: async () => {},
      disposed: false,
      armed: true,
      cleanupStarted: false,
    };
    let disposal: Promise<void> | undefined;
    record.dispose = async () => {
      const release = consume(!record.disposed);
      if (!release.fst) return disposal;
      record.disposed = !release.snd;
      record.armed = false;
      disposal = (async () => {
        const failures: unknown[] = [];
        for (const cleanup of record.disposers.splice(0).reverse()) {
          try {
            await cleanup();
          } catch (error) {
            failures.push(error);
          }
        }
        if (failures.length > 0) {
          throw new AggregateError(failures, `Fabric effect cleanup failed: ${record.label}`);
        }
      })();
      return disposal;
    };
    this.#records.push(record);
    return record.dispose;
  }

  dispose(): Promise<FabricEffectCleanupReport> {
    if (this.#cleanup) return this.#cleanup;
    this.#state = "disposing";
    this.#cleanup = (async () => {
      const failures: FabricEffectFailure[] = this.#setupCleanupFailures.splice(0);
      for (const record of this.#records.splice(0).reverse()) {
        try {
          await record.dispose();
        } catch (error) {
          if (error instanceof AggregateError) {
            for (const nested of error.errors) {
              failures.push({ label: record.label, error: errorMessage(nested) });
            }
          } else {
            failures.push({ label: record.label, error: errorMessage(error) });
          }
        }
      }
      this.#state = "disposed";
      return {
        status: cleanupState(failures.length > 0, false) === 2 ? "quarantined" : "disposed",
        failures,
      };
    })();
    return this.#cleanup;
  }
}
