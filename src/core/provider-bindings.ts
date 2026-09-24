import { randomUUID } from "node:crypto";
import { settleWithin } from "../async-settlement.js";
import { BEND_NAT_MAX } from "../verified/nat.js";
import { consume } from "../verified/policy.js";
import { bindingStep, type BindingLife, type BindingEvent, type BindingOutcome } from "../verified/generated/lifecycle-kernel.js";
import type { FabricComponentProviderLease } from "../components/types.js";
import type { FabricProvider } from "../protocol.js";

type FabricProviderBindingState = "staged" | "active" | "retiring" | "closed";

export interface FabricProviderBinding {
  id: string;
  name: string;
  generation: number;
  provider: FabricProvider;
  state: FabricProviderBindingState;
  ownerRetained: boolean;
  allowReplace: boolean;
  retainers: number;
  inFlight: number;
  closeTask?: Promise<void>;
  closeError?: string;
  unsubscribeCatalog?: () => void;
}

export type FabricProviderBindingEvent =
  | { type: "staged" | "activated" | "retiring" | "closed"; binding: FabricProviderBinding }
  | { type: "catalog"; provider: string };

const snapshot = (binding: FabricProviderBinding): FabricProviderBinding => ({
  ...binding,
  ...(binding.closeTask ? { closeTask: binding.closeTask } : {}),
});

const useOnce = (): (() => boolean) => {
  let active = true;
  return () => {
    const next = consume(active);
    active = next.snd;
    return next.fst;
  };
};

export class FabricProviderBindings {
  readonly #lifecycles = new Map<string, BindingLife>();
  readonly #current = new Map<string, FabricProviderBinding>();
  readonly #staged = new Map<string, FabricProviderBinding>();
  readonly #all = new Map<string, FabricProviderBinding>();
  readonly #generations = new Map<string, number>();
  readonly #pending = new Set<Promise<unknown>>();
  readonly #excluded = new Set<string>();
  readonly #signals = new Map<string, AbortController>();
  readonly #listeners = new Set<(event: FabricProviderBindingEvent) => void>();

  subscribe(listener: (event: FabricProviderBindingEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  current(name: string): FabricProviderBinding | undefined {
    return this.#current.get(name);
  }

  binding(id: string): FabricProviderBinding | undefined {
    const binding = this.#all.get(id);
    const life = this.#lifecycles.get(id);
    return binding && life && bindingStep(life, { $: "Inspect" }).command.$ === "Granted" ? binding : undefined;
  }

  has(name: string): boolean {
    return this.#current.has(name);
  }

  providers(): FabricProvider[] {
    return [...this.#current.values()].map((binding) => binding.provider);
  }

  entries(): FabricProviderBinding[] {
    return [...this.#all.values()].filter((binding) => binding.state !== "closed");
  }

  mount(
    provider: FabricProvider,
    options: { overwrite?: boolean; staged?: boolean } = {},
  ): FabricComponentProviderLease {
    if ([...this.#all.values()].some(binding => binding.provider === provider && binding.closeTask)) {
      throw new Error("Cannot mount a provider instance whose close has begun");
    }
    const current = this.#current.get(provider.name);
    const staged = this.#staged.get(provider.name);
    if ((current || staged) && !options.overwrite) {
      throw new Error(`Fabric provider already registered: ${provider.name}`);
    }
    if (staged && options.overwrite) this.retire(staged.id);
    const generation = (this.#generations.get(provider.name) ?? 0) + 1;
    if (!Number.isSafeInteger(generation)) throw new Error("Provider generation overflow");
    this.#generations.set(provider.name, generation);
    const binding: FabricProviderBinding = {
      id: randomUUID(),
      name: provider.name,
      generation,
      provider,
      state: options.staged ? "staged" : "active",
      ownerRetained: true,
      allowReplace: options.overwrite === true,
      retainers: 0,
      inFlight: 0,
    };
    if (provider.subscribeCatalog) {
      binding.unsubscribeCatalog = provider.subscribeCatalog(() =>
        this.notifyCatalogChanged(provider.name),
      );
    }
    this.#all.set(binding.id, binding);
    this.#lifecycles.set(binding.id, { $: "Life", phase: { $: options.staged ? "Staged" : "Active" }, owner: true, holds: 0n, calls: 0n, revoked: false });
    this.#signals.set(binding.id, new AbortController());
    if (options.staged) {
      this.#staged.set(binding.name, binding);
      this.#emit({ type: "staged", binding: snapshot(binding) });
    } else {
      const replaced = this.#activateOne(binding);
      if (replaced && options.overwrite) void this.releaseOwner(replaced.id).catch(() => undefined);
    }

    const takeRelease = useOnce();
    return {
      bindingId: binding.id,
      name: binding.name,
      generation: binding.generation,
      get active() {
        return binding.state === "active";
      },
      retire: () => this.retire(binding.id),
      release: async () => {
        if (!takeRelease()) return binding.closeTask;
        return this.releaseOwner(binding.id);
      },
    };
  }

  activate(bindingIds: readonly string[]): string[] {
    const bindings = bindingIds.map((id) => {
      const binding = this.#all.get(id);
      if (!binding || binding.state === "closed" || binding.closeTask) {
        throw new Error(`Unknown Fabric provider binding: ${id}`);
      }
      if (binding.state !== "staged" && binding.state !== "active") {
        throw new Error(`Fabric provider binding is ${binding.state}: ${binding.name}`);
      }
      return binding;
    });
    const names = new Set<string>();
    for (const binding of bindings) {
      if (names.has(binding.name)) {
        throw new Error(`Cannot activate multiple Fabric bindings for provider ${binding.name}`);
      }
      names.add(binding.name);
      const current = this.#current.get(binding.name);
      if (current && current.id !== binding.id && !binding.allowReplace) {
        throw new Error(`Fabric provider already registered: ${binding.name}`);
      }
    }
    const replaced: string[] = [];
    for (const binding of bindings) {
      const previous = this.#activateOne(binding);
      if (previous && previous.id !== binding.id) {
        replaced.push(previous.id);
        if (binding.allowReplace) void this.releaseOwner(previous.id).catch(() => undefined);
      }
    }
    return replaced;
  }

  unregister(name: string): FabricProvider | undefined {
    const binding = this.#current.get(name);
    if (!binding) return undefined;
    this.retire(binding.id);
    void this.releaseOwner(binding.id).catch(() => undefined);
    return binding.provider;
  }

  retire(id: string): void {
    const binding = this.#all.get(id);
    if (!binding) return;
    const withdrawing = this.#current.get(binding.name)?.id === id || this.#staged.get(binding.name)?.id === id;
    if (this.#current.get(binding.name)?.id === id) this.#current.delete(binding.name);
    if (this.#staged.get(binding.name)?.id === id) this.#staged.delete(binding.name);
    this.#step(binding, { $: "Retire" });
    if (withdrawing) this.#emit({ type: "retiring", binding: snapshot(binding) });
    void this.#maybeClose(binding).catch(() => undefined);
  }

  retain(ids: Iterable<string>, cleanup = false): () => Promise<void> {
    const retained: FabricProviderBinding[] = [];
    try {
      for (const id of new Set(ids)) {
        const binding = this.#all.get(id);
        if (!binding || this.#step(binding, { $: "Retain", cleanup }) !== "Granted") {
          throw new Error(`Unknown Fabric provider binding: ${id}`);
        }
        retained.push(binding);
      }
    } catch (error) {
      for (const binding of retained) this.#step(binding, { $: "Release" });
      throw error;
    }
    const takeRelease = useOnce();
    return async () => {
      if (!takeRelease()) return;
      await Promise.all(retained.map(async (binding) => {
        this.#step(binding, { $: "Release" });
        await this.#maybeClose(binding);
      }));
    };
  }

  beginInvocation(id: string, cleanup = false): () => Promise<void> {
    const binding = this.#all.get(id);
    if (!binding || this.#step(binding, { $: "Begin", cleanup }) !== "Granted") {
      throw new Error(`Unknown Fabric provider binding: ${id}`);
    }
    const takeEnd = useOnce();
    return async () => {
      if (!takeEnd()) return;
      this.#step(binding, { $: "End" });
      await this.#maybeClose(binding);
    };
  }

  signal(id: string): AbortSignal {
    const controller = this.#signals.get(id);
    if (!controller) throw new Error("Unknown provider authority");
    return controller.signal;
  }

  revoke(id: string): void {
    const binding = this.#all.get(id);
    if (!binding) return;
    this.#step(binding, { $: "Revoke" });
    this.#signals.get(id)?.abort(new Error("Fabric provider authority revoked"));
    this.retire(id);
    void this.#maybeClose(binding).catch(() => undefined);
  }

  quarantine(id: string, error: unknown): void {
    const binding = this.#all.get(id);
    if (!binding) return;
    binding.closeError = error instanceof Error ? error.message : String(error);
    this.#step(binding, { $: "Fail" });
    this.revoke(id);
  }

  /** Hold the real operation, not the caller's cancellation race. */
  track<T>(id: string, operation: () => T | PromiseLike<T>, cleanup = false): Promise<T> {
    if (!cleanup && !this.binding(id)) return Promise.reject(new Error("Fabric provider authority revoked or unavailable"));
    let end: () => Promise<void>;
    try { end = this.beginInvocation(id, cleanup); }
    catch (error) { return Promise.reject(error); }
    let actual: Promise<T>;
    try { actual = Promise.resolve(operation()); }
    catch (error) { actual = Promise.reject(error); }
    this.#pending.add(actual);
    const settled = () => { this.#pending.delete(actual); return end(); };
    void actual.then(settled, settled).catch(() => undefined);
    return actual;
  }

  trackProvider<T>(provider: FabricProvider, operation: () => T | PromiseLike<T>, cleanup = false): Promise<T> {
    const binding = [...this.#all.values()].find(item => item.provider === provider && this.#canBegin(item.id, cleanup));
    if (!binding) return Promise.reject(new Error("Fabric provider binding is unavailable"));
    return this.track(binding.id, operation, cleanup);
  }

  notifyCatalogChanged(provider: string): void {
    if (this.#current.has(provider)) this.#emit({ type: "catalog", provider });
  }

  async close(excludedProviderNames: Set<string> = new Set()): Promise<void> {
    const tasks: Promise<void>[] = [];
    for (const binding of this.#all.values()) {
      if (binding.state === "closed") continue;
      if (excludedProviderNames.has(binding.name)) this.#excluded.add(binding.id);
      this.revoke(binding.id);
      tasks.push(this.#maybeClose(binding));
    }
    const deadline = Date.now() + 1_000;
    await settleWithin([...tasks, ...this.#pending], 1_000);
    await settleWithin([...this.#all.values()].flatMap(binding => binding.closeTask ? [binding.closeTask] : []), Math.max(0, deadline - Date.now()));
    this.#current.clear();
    this.#staged.clear();
  }

  #activateOne(binding: FabricProviderBinding): FabricProviderBinding | undefined {
    const current = this.#current.get(binding.name);
    if (current?.id === binding.id && binding.state === "active") return current;
    if (current && current.id !== binding.id) this.retire(current.id);
    if (this.#staged.get(binding.name)?.id === binding.id) this.#staged.delete(binding.name);
    if (this.#step(binding, { $: "Activate" }) !== "Granted") throw new Error("Provider binding cannot be activated");
    this.#current.set(binding.name, binding);
    this.#emit({ type: "activated", binding: snapshot(binding) });
    return current;
  }

  private async releaseOwner(id: string): Promise<void> {
    const binding = this.#all.get(id);
    if (!binding) return;
    this.retire(id);
    this.#step(binding, { $: "DropOwner" });
    await this.#maybeClose(binding);
  }

  async #maybeClose(binding: FabricProviderBinding): Promise<void> {
    if (binding.closeTask) return binding.closeTask;
    // The reducer reserves Closing synchronously, before arbitrary callbacks.
    if (this.#step(binding, { $: "Close" }) !== "StartClose") return;
    binding.closeTask = Promise.resolve().then(async () => {
      try {
        const unsubscribe = binding.unsubscribeCatalog;
        delete binding.unsubscribeCatalog;
        await unsubscribe?.();
        const shared = [...this.#all.values()].some(other => other.id !== binding.id && other.provider === binding.provider && other.state !== "closed");
        if (!shared && !this.#excluded.has(binding.id)) await binding.provider.close?.();
      } catch (error) {
        this.quarantine(binding.id, error);
        throw error;
      }
      if (this.#step(binding, { $: "Complete" }) !== "Granted") {
        throw new Error(binding.closeError ?? "Provider close completion refused");
      }
      this.#all.delete(binding.id);
      this.#lifecycles.delete(binding.id);
      this.#signals.delete(binding.id);
      this.#excluded.delete(binding.id);
      this.#emit({ type: "closed", binding: snapshot(binding) });
    });
    return binding.closeTask;
  }

  #canBegin(id: string, cleanup: boolean): boolean {
    const life = this.#lifecycles.get(id);
    return !!life && life.calls < BigInt(BEND_NAT_MAX) && bindingStep(life, { $: "Begin", cleanup }).command.$ === "Granted";
  }

  #step(binding: FabricProviderBinding, event: BindingEvent): BindingOutcome["command"]["$"] {
    const life = this.#lifecycles.get(binding.id);
    if (!life) return "Denied";
    if ((event.$ === "Begin" && life.calls >= BigInt(BEND_NAT_MAX)) || (event.$ === "Retain" && life.holds >= BigInt(BEND_NAT_MAX))) {
      throw new Error("Provider lifecycle accounting overflow");
    }
    const outcome = bindingStep(life, event);
    if (outcome.next.holds > BigInt(BEND_NAT_MAX) || outcome.next.calls > BigInt(BEND_NAT_MAX)) {
      throw new Error("Provider lifecycle accounting overflow");
    }
    this.#lifecycles.set(binding.id, outcome.next);
    binding.state = ({ Staged: "staged", Active: "active", Retiring: "retiring", Closing: "retiring", Closed: "closed", Failed: "retiring" } as const)[outcome.next.phase.$];
    binding.ownerRetained = outcome.next.owner;
    binding.retainers = Number(outcome.next.holds);
    binding.inFlight = Number(outcome.next.calls);
    return outcome.command.$;
  }

  #emit(event: FabricProviderBindingEvent): void {
    for (const listener of [...this.#listeners]) {
      try {
        listener(event);
      } catch {
        // Registry listeners are observations; one listener cannot break provider ownership.
      }
    }
  }
}
