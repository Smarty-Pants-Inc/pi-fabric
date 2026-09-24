import {
  resourceSource, resourceNormalize, resourceGroups,
  type BendList, type ResourceName, type ResourceScope, type ResourceEffect,
} from "./generated/kernel.js";

export { resourceGroups };

/** Lossless wire encoding, including NULs and lone surrogates. Never truncate,
 * hash, normalize Unicode, or assign independently interned numeric IDs here. */
export const encodeResourceNames = (values: readonly unknown[] | undefined): BendList<ResourceName> => {
  let names: BendList<ResourceName> = { $: "Nil" };
  const cache = new Map<string, ResourceName>();
  const input = Array.isArray(values) ? values : [];
  for (let i = input.length - 1; i >= 0; i--) {
    const value: unknown = input[i];
    let name: ResourceName = { $: "Nil" };
    if (typeof value === "string") {
      const cached = cache.get(value);
      if (cached) name = cached;
      else {
        for (let j = value.length - 1; j >= 0; j--) {
          name = { $: "Con", head: BigInt(value.charCodeAt(j)), tail: name };
        }
        cache.set(value, name);
      }
    }
    names = { $: "Con", head: name, tail: names };
  }
  return names;
};

/** ABI decoding only. Normalization policy lives in the compiled checker. */
export const decodeResourceScope = (scope: ResourceScope): string[] => {
  if (scope.$ === "Unknown") return ["*"];
  const result: string[] = [];
  for (let names = scope.names; names.$ === "Con"; names = names.tail) {
    const chars: string[] = [];
    for (let name = names.head; name.$ === "Con"; name = name.tail) {
      if (name.head < 0n || name.head > 65535n) throw new Error("Invalid verified resource code unit");
      chars.push(String.fromCharCode(Number(name.head)));
    }
    result.push(chars.join(""));
  }
  return result;
};

export const boundedEffectResources = (resources: readonly string[] | undefined): string[] => {
  // Capture the entire declaration before running the untrusted proposer.
  const declared: unknown[] = Array.isArray(resources) ? Array.from(resources) : [];
  const original = resourceSource(encodeResourceNames(declared));
  const proposal = new Set<unknown>();
  for (const resource of declared) {
    proposal.add(resource);
    if (proposal.size > 64) break;
  }
  return decodeResourceScope(resourceNormalize(original, encodeResourceNames([...proposal])));
};

export const encodeResourceEffects = (
  effects: readonly { kind: string; ordering: string; resources: readonly string[] }[],
): BendList<ResourceEffect> => {
  let result: BendList<ResourceEffect> = { $: "Nil" };
  for (let i = effects.length - 1; i >= 0; i--) {
    const effect = effects[i]!;
    const value: ResourceEffect = effect.kind === "none" ? { $: "Quiet" } : {
      $: "Effect", ordered: effect.ordering !== "commutative",
      scope: resourceSource(encodeResourceNames(effect.resources)),
    };
    result = { $: "Con", head: value, tail: result };
  }
  return result;
};
