import { scopeAllows } from "./scope.js";
import type { Usage, ClassifierContext, ImagesContext, ModelType } from "@earendil-works/pi-ai";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider } from "./protocol.js";

const object = (properties: Record<string, unknown>, required = Object.keys(properties)) => ({ type: "object", properties, required, additionalProperties: false });
const string = { type: "string", maxLength: 262_144 };
const model = { ...object({ provider: string, id: string }), additionalProperties: true };
const type = { enum: ["chat", "classifier", "image"] };
const catalog = object({ type, provider: string }, ["type"]);
const question = { oneOf: [
  object({ type: { const: "choice" }, instructions: string, criteria: { type: "object", additionalProperties: string, minProperties: 1, maxProperties: 1_000 } }),
  object({ type: { const: "score" }, instructions: string, criteria: { type: "array", items: string, minItems: 1, maxItems: 1_000 } }),
  object({ type: { const: "bool" }, instructions: string, criteria: object({ true: string, false: string }) }),
] };
const imageBlock = object({ type: { const: "image" }, data: { type: "string", maxLength: 16_777_216 }, mimeType: string });
const block = { oneOf: [object({ type: { const: "text" }, text: string }), imageBlock] };
const descriptors: FabricActionDescriptor[] = [
  ...["getModelsOfType", "getAvailableOfType"].map(name => ({ name, description: "Native Pi model catalog (credentials remain host-side).", inputSchema: catalog, risk: "read" as const })),
  { name: "getModelOfType", description: "Find a native Pi model.", inputSchema: object({ type, provider: string, id: string }), risk: "read" },
  { name: "classify", description: "Answer typed questions using the native Pi classifier registry.", inputSchema: object({ model, context: object({ state: { type: "object", additionalProperties: true }, questions: { type: "object", additionalProperties: question, minProperties: 1, maxProperties: 1_000 }, images: { type: "array", items: imageBlock, maxItems: 1_000 } }, ["state", "questions"]) }), risk: "network" },
  { name: "generateImages", description: "Generate images using the native Pi image registry.", inputSchema: object({ model, context: object({ input: { type: "array", items: block, maxItems: 1_000 } }) }), risk: "network" },
  { name: "load", description: "Read the branch-local JSON script store.", inputSchema: object({}), risk: "read" },
  { name: "store", description: "Stage branch-local JSON state; committed only after successful execution.", inputSchema: object({ values: { type: "object", additionalProperties: true } }), risk: "write" },
];

export const CODEMODE_STORE_ENTRY = "fabric-codemode-store";
export const STORE_VALUE_LIMIT = 262_144;
export const STORE_TOTAL_LIMIT = 1_048_576;
export function validateScriptStore(values: Record<string, unknown>): Record<string, unknown> {
  if (!values || typeof values !== "object" || Array.isArray(values)) throw new Error("Invalid script store snapshot");
  let total = 0;
  for (const value of Object.values(values)) {
    const json = JSON.stringify(value);
    if (json === undefined) throw new Error("Script store values must be JSON");
    if (json.length > STORE_VALUE_LIMIT) throw new Error("Script store value exceeds 262144 characters");
    total += json.length;
  }
  if (total > STORE_TOTAL_LIMIT) throw new Error("Script store exceeds 1048576 characters");
  return JSON.parse(JSON.stringify(values));
}

// Deliberate allowlist: registry entries may carry credential headers, request
// defaults or executable provider metadata. None crosses the guest boundary.
function modelInfo(value: unknown): unknown {
  if (!value || typeof value !== "object") return undefined;
  const source = value as Record<string, unknown>;
  return Object.fromEntries(["type", "provider", "id", "name", "api", "input", "contextWindow", "maxTokens", "reasoning", "cost"].filter(key => source[key] !== undefined).map(key => [key, source[key]]));
}
interface Transaction {
  context: ExtensionContext;
  session: string;
  leaf: string | null;
  values: Record<string, unknown>;
  staged?: Record<string, unknown>;
  active: number;
  waiters: Array<() => void>;
  usage: Usage[];
}
export class NativeCodemodeProvider implements FabricProvider {
  readonly name = "native";
  readonly description = "Pi native model registry and branch-local codemode state";
  readonly transactions = new Map<string, Transaction>();
  #append: ((type: string, data: unknown) => void) | undefined;
  #generation = 0;
  get persistenceAvailable(): boolean { return Boolean(this.#append); }
  setPersistence(append: ((type: string, data: unknown) => void) | undefined): void { this.#append = append; }
  invalidate(): void { this.#generation++; this.transactions.clear(); }
  begin(id: string, context: ExtensionContext, nativeStore = false): { finish: (success: boolean) => Usage[] } {
    const manager = context.sessionManager;
    let values: Record<string, unknown> = Object.create(null);
    const branch = [...(manager?.getBranch?.() ?? [])];
    for (const entry of nativeStore ? branch : branch.reverse()) {
      if (nativeStore) {
        if (entry.type !== "custom" || entry.customType !== "codemode-store" || !entry.data || typeof entry.data !== "object") continue;
        const delta = entry.data as {set?: Record<string, unknown>; delete?: unknown};
        if (!delta.set || typeof delta.set !== "object" || Array.isArray(delta.set) || !Array.isArray(delta.delete) || !delta.delete.every(key => typeof key === "string")) continue;
        try {
          const next = Object.assign(Object.create(null), values);
          for (const key of delta.delete) delete next[key];
          Object.assign(next, delta.set);
          values = validateScriptStore(next);
        } catch { /* ignore malformed historical deltas */ }
        continue;
      }
      if (entry.type === "custom" && entry.customType === CODEMODE_STORE_ENTRY && entry.data && typeof entry.data === "object") {
        // A malformed old/custom entry must not poison unrelated scripts.
        try { values = validateScriptStore((entry.data as { values: Record<string, unknown> }).values); break; } catch { /* ignore invalid snapshots */ }
      }
    }
    const generation = this.#generation;
    const tx: Transaction = { context, session: manager?.getSessionId?.() ?? "", leaf: manager?.getLeafId?.() ?? null, values, active: 0, waiters: [], usage: [] };
    this.transactions.set(id, tx);
    return { finish: success => {
      this.transactions.delete(id);
      if (success && tx.staged) {
        if (context.signal?.aborted || generation !== this.#generation || manager.getSessionId() !== tx.session || manager.getLeafId() !== tx.leaf) throw new Error("Script store transaction is stale: session or branch changed");
        if (!this.#append) throw new Error("Script store persistence is unavailable");
        if (nativeStore) {
          const set = Object.fromEntries(Object.entries(tx.staged).filter(([key, value]) => JSON.stringify(tx.values[key]) !== JSON.stringify(value)));
          const deleted = Object.keys(tx.values).filter(key => !Object.hasOwn(tx.staged!, key));
          if (Object.keys(set).length || deleted.length) this.#append("codemode-store", {set, delete: deleted});
        } else this.#append(CODEMODE_STORE_ENTRY, { values: tx.staged });
      }
      return tx.usage;
    } };
  }
  #allowed(name: string, context: FabricInvocationContext): boolean {
    if (!context.scope) return true;
    const state = name === "load" || name === "store";
    return scopeAllows(context.scope, state ? "native:store" : "native:models", name === "store" ? "write" : name === "classify" || name === "generateImages" ? "execute" : "read");
  }
  async list(_request: unknown, context: FabricInvocationContext): Promise<FabricActionDescriptor[]> { return descriptors.filter(action => this.#allowed(action.name, context)); }
  async describe(name: string, context: FabricInvocationContext): Promise<FabricActionDescriptor | undefined> { return this.#allowed(name, context) ? descriptors.find(value => value.name === name) : undefined; }
  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    if (!this.#allowed(name, context)) throw new Error(`Native codemode scope denied: ${name}`);
    const tx = this.transactions.get(context.parentToolCallId);
    if (!tx) throw new Error("Native codemode requires an active script invocation");
    context.signal?.throwIfAborted();
    if (name === "load") return structuredClone(tx.values);
    if (name === "store") { tx.staged = validateScriptStore(args.values as Record<string, unknown>); return undefined; }
    const registry = context.extensionContext.modelRegistry;
    if (!registry || typeof registry[name as keyof typeof registry] !== "function" || typeof registry.getModelOfType !== "function") throw new Error(`Native Pi modelRegistry.${name} is unavailable; upgrade Pi to a version supporting native codemode models`);
    const requestChars = JSON.stringify(args).length;
    if (requestChars > 16_777_216) throw new Error("Native model request exceeds 16777216 characters");
    const kind = args.type as ModelType;
    if (name === "getModelsOfType") return registry.getModelsOfType(kind, args.provider as string | undefined).map(modelInfo);
    if (name === "getAvailableOfType") return (await registry.getAvailableOfType(kind, args.provider as string | undefined, context.signal ? { signal: context.signal } : {})).map(modelInfo);
    if (name === "getModelOfType") return modelInfo(registry.getModelOfType(kind, args.provider as string, args.id as string));
    if (name !== "classify" && name !== "generateImages") throw new Error(`Unknown native action: ${name}`);
    // Reserve the slot before awaiting; waking a waiter transfers ownership.
    if (tx.active >= 4) await new Promise<void>((resolve, reject) => {
      const wake = () => { context.signal?.removeEventListener("abort", abort); resolve(); };
      const abort = () => { const index = tx.waiters.indexOf(wake); if (index >= 0) tx.waiters.splice(index, 1); reject(context.signal?.reason ?? new Error("Cancelled")); };
      tx.waiters.push(wake);
      context.signal?.addEventListener("abort", abort, { once: true });
      if (context.signal?.aborted) abort();
    });
    else tx.active++;
    try {
      context.signal?.throwIfAborted();
      const selector = args.model as { provider: string; id: string };
      let result;
      if (name === "classify") {
        const selected = registry.getModelOfType("classifier", selector.provider, selector.id);
        if (!selected) throw new Error(`Unknown classifier model: ${selector.provider}/${selector.id}`);
        result = await registry.classify(selected, args.context as unknown as ClassifierContext, context.signal ? { signal: context.signal } : {});
      } else {
        const selected = registry.getModelOfType("image", selector.provider, selector.id);
        if (!selected) throw new Error(`Unknown image model: ${selector.provider}/${selector.id}`);
        result = await registry.generateImages(selected, args.context as unknown as ImagesContext, context.signal ? { signal: context.signal } : {});
      }
      if (result.usage) {
        const usage = result.usage;
        tx.usage.push({
          input: usage.input ?? 0, output: usage.output ?? 0, totalTokens: usage.totalTokens ?? ((usage.input ?? 0) + (usage.output ?? 0)),
          cacheRead: usage.cacheRead ?? 0, cacheWrite: usage.cacheWrite ?? 0,
          cost: { input: usage.cost?.input ?? 0, output: usage.cost?.output ?? 0, cacheRead: usage.cost?.cacheRead ?? 0, cacheWrite: usage.cost?.cacheWrite ?? 0, total: usage.cost?.total ?? 0 },
        });
      }
      if (JSON.stringify(result).length > 16_777_216) throw new Error("Native model response exceeds 16777216 characters");
      return result;
    } finally {
      const next = tx.waiters.shift();
      if (next) next(); else tx.active--;
    }
  }
}
