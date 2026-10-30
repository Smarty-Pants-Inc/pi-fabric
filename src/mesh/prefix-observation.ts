import type { MeshStateEntry } from "./state-file.js";

export type DeepReadonly<T> = T extends object ? { readonly [K in keyof T]: DeepReadonly<T[K]> } : T;
/** Opaque process-local handle; only the issuer's runtime registration grants authority. */
export type PrefixWitness = object;
export type PrefixEntries = readonly DeepReadonly<MeshStateEntry>[];
export type PrefixObservation =
  | { kind: "unknown"; reason: string }
  | { kind: "snapshot"; witness: PrefixWitness; entries: PrefixEntries }
  | { kind: "unchanged"; witness: PrefixWitness };

export const unknownPrefixObservation = (reason: string): PrefixObservation => ({ kind: "unknown", reason });

/** Only called on a newly decoded, owned JSON graph; never on a hit or on listAll's graph. */
export const freezePrefixEntries = (entries: MeshStateEntry[]): PrefixEntries => {
  const freeze = (value: unknown): void => {
    if (value === null || typeof value !== "object") return;
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  };
  freeze(entries);
  return entries;
};

interface PrefixRecord<T> {
  readonly witness: PrefixWitness;
  readonly entries: PrefixEntries;
  readonly state: T;
}

/** Each layer owns a separate registry. Witness + complete sequence are always one atomic record. */
export class PrefixObservationCache<T> {
  readonly #prefixes = new Map<string, PrefixRecord<T>>();

  get(prefix: string, witness: PrefixWitness | undefined): PrefixRecord<T> | undefined {
    const record = this.#prefixes.get(prefix);
    return witness !== undefined && record?.witness === witness ? record : undefined;
  }

  put(prefix: string, entries: PrefixEntries, state: T): PrefixRecord<T> {
    const record = { witness: Object.freeze({}) as PrefixWitness, entries, state };
    this.#prefixes.delete(prefix);
    this.#prefixes.set(prefix, record);
    if (this.#prefixes.size > 32) this.#prefixes.delete(this.#prefixes.keys().next().value!);
    return record;
  }

  delete(prefix: string): void { this.#prefixes.delete(prefix); }
  clear(): void { this.#prefixes.clear(); }
}

const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const handle = (value: unknown): value is PrefixWitness => object(value) && Object.isFrozen(value);
const integer = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

/** Runtime boundary for optional backends. Cold only: hits never inspect the saved entry graph. */
export const validPrefixSnapshot = (value: unknown, prefix: string): value is PrefixEntries => {
  if (!Array.isArray(value) || !Object.isFrozen(value)) return false;
  const seen = new Set<object>();
  const frozenJson = (node: unknown): boolean => {
    if (node === null || typeof node === "string" || typeof node === "boolean") return true;
    if (typeof node === "number") return Number.isFinite(node);
    if (typeof node !== "object" || !Object.isFrozen(node) || seen.has(node)) return false;
    if (!Array.isArray(node) && Object.getPrototypeOf(node) !== Object.prototype && Object.getPrototypeOf(node) !== null) return false;
    seen.add(node);
    const descriptors = Object.getOwnPropertyDescriptors(node);
    return Object.values(descriptors).every(descriptor => "value" in descriptor && frozenJson(descriptor.value));
  };
  if (!frozenJson(value)) return false;
  let last: string | undefined;
  for (const entry of value) {
    if (!object(entry) || typeof entry.key !== "string" || !entry.key.startsWith(prefix) ||
      !integer(entry.version) || entry.version === 0 || !integer(entry.updatedAt) || !object(entry.updatedBy) ||
      typeof entry.updatedBy.id !== "string" || typeof entry.updatedBy.name !== "string" ||
      !["agent", "actor", "main"].includes(String(entry.updatedBy.kind)) || !("value" in entry) ||
      (entry.updatedBy.sessionId !== undefined && typeof entry.updatedBy.sessionId !== "string") ||
      (entry.updatedBy.verified !== undefined && entry.updatedBy.verified !== "bridge") ||
      (last !== undefined && last.localeCompare(entry.key) >= 0)) return false;
    last = entry.key;
  }
  return true;
};

/** Exhaustive runtime switch; callers must still keep their own witness/sequence pair for replay. */
export const bindPrefixObservation = (cache: PrefixObservationCache<PrefixWitness>, prefix: string,
  previous: PrefixWitness | undefined, observe: (previous?: PrefixWitness) => unknown): PrefixObservation => {
  const saved = cache.get(prefix, previous);
  // Retire the wrapper before backend entry: UNKNOWN or a thrown error cannot preserve old authority.
  cache.delete(prefix);
  // Deliberately no catch: canonical busy/corruption/lifecycle errors keep their ONE existing budget.
  const result = observe(saved?.state);
  const unknown = (reason: string): PrefixObservation => {
    cache.delete(prefix);
    return unknownPrefixObservation(reason);
  };
  if (!object(result) || !Object.hasOwn(result, "kind") ||
    Object.values(Object.getOwnPropertyDescriptors(result)).some(descriptor => !("value" in descriptor))) {
    return unknown("malformed-backend-observation");
  }
  switch (result.kind) {
    case "unknown":
      return unknown(Object.hasOwn(result, "reason") && typeof result.reason === "string" ? result.reason : "malformed-backend-observation");
    case "snapshot": {
      if (!Object.hasOwn(result, "witness") || !Object.hasOwn(result, "entries") ||
        !handle(result.witness) || !validPrefixSnapshot(result.entries, prefix)) return unknown("malformed-backend-snapshot");
      const record = cache.put(prefix, result.entries, result.witness);
      return { kind: "snapshot", witness: record.witness, entries: record.entries };
    }
    case "unchanged": {
      if (!saved || !Object.hasOwn(result, "witness") || !handle(result.witness)) return unknown("unpaired-backend-observation");
      const record = cache.put(prefix, saved.entries, result.witness);
      return { kind: "unchanged", witness: record.witness };
    }
    default:
      return unknown("unrecognized-backend-observation");
  }
};
