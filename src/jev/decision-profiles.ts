import type { DecisionProfiles } from "./decision-types.js";

export const DECISION_MAX_BYTES = 16 * 1024 * 1024;
export const DECISION_PROFILE_MAX_BYTES = 64 * 1024;
export const DECISION_APIS = ["system-one", "cloudflare", "openai-decisions", "vercel-evaluate", "llama-cpp", "llama-system-one", "anthropic", "google"] as const;
export const DECISION_TARGET_KEYS = ["provider", "api", "model", "endpoint", "allowLocal", "allowGenerated", "providerOptions", "temperature"] as const;
const record = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const name = (v: unknown): v is string => typeof v === "string" && /^[A-Za-z0-9_.-]{1,128}$/.test(v);

/** Fail closed rather than silently discarding a malformed trusted routing document. */
export function normalizeDecisionProfiles(value: unknown): DecisionProfiles {
  const invalid = (): never => { throw new Error("Invalid jev.decisionProfiles: expected a bounded version:1 routing-only document (no secrets or evidence)"); };
  let nodes = 0;
  // Pi keeps its existing 8192-node/16-depth ceiling; clients/native have separate work limits.
  // Copy descriptors, not JSON.stringify: never invoke accessors/toJSON or drop source fields.
  const seen = new Set<object>();
  const cloneJSON = (v: unknown, depth: number): unknown => {
    if (++nodes > 8192 || depth > 16) invalid();
    if (v === null || typeof v === "string" || typeof v === "boolean" || (typeof v === "number" && Number.isFinite(v))) return v;
    if (!v || typeof v !== "object" || seen.has(v)) return invalid();
    const array = Array.isArray(v);
    if (!array && ![Object.prototype, null].includes(Object.getPrototypeOf(v))) return invalid();
    seen.add(v);
    const result: Record<string, unknown> | unknown[] = array ? [] : {};
    for (const key of Reflect.ownKeys(v)) {
      if (array && key === "length") continue;
      if (typeof key !== "string") return invalid();
      const descriptor = Object.getOwnPropertyDescriptor(v, key)!;
      if (!descriptor.enumerable || !("value" in descriptor) ||
        (array && (!/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= (v as unknown[]).length))) return invalid();
      if (depth >= 2 && /^(?:state|questions|images|headers|authorization|credential.*|secrets?|api[_-]?key|access[_-]?token|token|password)$/i.test(key)) invalid();
      Object.defineProperty(result, key, { value: cloneJSON(descriptor.value, depth + 1), enumerable: true, writable: true, configurable: true });
    }
    if (array && Object.keys(result).length !== (v as unknown[]).length) return invalid();
    seen.delete(v);
    return result;
  };
  value = cloneJSON(value, 0);
  if (!record(value) || value.version !== 1 || Object.keys(value).some(k => !["version", "defaultProfile", "profiles"].includes(k)) || !record(value.profiles)) return invalid();
  const profiles = value.profiles;
  if (Object.keys(profiles).length > 128 || Buffer.byteLength(JSON.stringify(value), "utf8") > DECISION_PROFILE_MAX_BYTES) return invalid();
  for (const [id, target] of Object.entries(profiles)) {
    if (!name(id) || ["__proto__", "prototype", "constructor"].includes(id) || !record(target) || !name(target.provider) || Object.keys(target).some(k => !(DECISION_TARGET_KEYS as readonly string[]).includes(k))) return invalid();
    if (target.api !== undefined && !(DECISION_APIS as readonly unknown[]).includes(target.api)) return invalid();
    for (const k of ["model", "endpoint"] as const) if (target[k] !== undefined && (typeof target[k] !== "string" || !target[k] || /[\x00-\x1f\x7f]/.test(target[k] as string))) return invalid();
    if (typeof target.model === "string" && (Array.from(target.model).length > 256 || /[\uD800-\uDFFF]/u.test(target.model))) return invalid();
    for (const k of ["allowLocal", "allowGenerated"]) if (target[k] !== undefined && typeof target[k] !== "boolean") return invalid();
    if (target.providerOptions !== undefined && !record(target.providerOptions)) return invalid();
    if (target.temperature !== undefined && (typeof target.temperature !== "number" || !Number.isFinite(target.temperature) || target.temperature < 0 || target.temperature > 2)) return invalid();
    if (typeof target.endpoint === "string") {
      // Structural loopback shape is allowed here; native selected preflight requires allowLocal.
      const endpoint = target.endpoint;
      const match = /^(https?):\/\/([A-Za-z0-9.-]+|\[::1\])(?::([0-9]+))?(?:\/[^]*)?$/.exec(endpoint);
      if (!match || /[^\x21-\x7e]|[?#\\]/.test(endpoint) || Array.from(endpoint).length > 4096 ||
        (match[3] !== undefined && (+match[3] < 1 || +match[3] > 65535)) ||
        (match[1] !== "https" && !["127.0.0.1", "[::1]"].includes(match[2]!))) return invalid();
    }
  }
  if (value.defaultProfile !== undefined && (!name(value.defaultProfile) || !Object.hasOwn(profiles, value.defaultProfile))) return invalid();
  return value as unknown as DecisionProfiles;
}
