import {
  authorityAllows,
  authorityDerive,
  authorityIssue,
  authorityLive,
  authorityRelease,
  type AuthorityGrants,
  type AuthorityState,
  type AuthorityText,
} from "./generated/authority-kernel.js";
import type { FabricCapabilityBindingView } from "../protocol.js";

/** Trusted codec: JSON's fixed-position string/number tuple plus complete UTF-16
 * code units preserves ref, provider, binding identity, generation and descriptor
 * identity without prefix comparisons or truncation. Descriptor hashing itself
 * remains trusted; the kernel proves equality of identities, not hash injectivity.
 */
const encodeGrant = (grant: Readonly<FabricCapabilityBindingView>): AuthorityText => {
  const text = JSON.stringify([
    grant.ref, grant.provider, grant.providerBindingId, grant.generation, grant.descriptorHash,
  ]);
  let encoded: AuthorityText = { $: "Nil" };
  for (let i = text.length - 1; i >= 0; i--) {
    encoded = { $: "Con", head: BigInt(text.charCodeAt(i)), tail: encoded };
  }
  return encoded;
};

const encodeGrants = (grants: readonly Readonly<FabricCapabilityBindingView>[]): AuthorityGrants => {
  let encoded: AuthorityGrants = { $: "Nil" };
  for (let i = grants.length - 1; i >= 0; i--) {
    encoded = { $: "Con", head: encodeGrant(grants[i]!), tail: encoded };
  }
  return encoded;
};

/** Private canonical state behind ActionRegistry's WeakMap-owned public views.
 * Root issuance is a trusted registry operation, not a caller-supplied permission
 * Boolean. Bend proves derivation/use/release over exact grants. JS identity,
 * WeakMap ownership, binding observations and abort delivery remain trusted.
 */
export class CapabilityAuthority {
  #state: AuthorityState;
  readonly #grants: ReadonlyMap<string, Readonly<FabricCapabilityBindingView>>;

  private constructor(grants: readonly FabricCapabilityBindingView[], parent?: CapabilityAuthority) {
    const canonical = grants.map(grant => Object.freeze({ ...grant }));
    this.#grants = new Map(canonical.map(grant => [grant.ref, grant]));
    const encoded = encodeGrants(canonical);
    this.#state = parent ? authorityDerive(parent.#state, encoded) : authorityIssue(encoded);
  }

  static issue(grants: readonly FabricCapabilityBindingView[]): CapabilityAuthority {
    return new CapabilityAuthority(grants);
  }

  derive(grants: readonly FabricCapabilityBindingView[]): CapabilityAuthority {
    return new CapabilityAuthority(grants, this);
  }

  get active(): boolean {
    return authorityLive(this.#state);
  }

  release(): void {
    this.#state = authorityRelease(this.#state);
  }

  resolve(ref: string): FabricCapabilityBindingView | undefined {
    const grant = this.#grants.get(ref);
    return grant && authorityAllows(this.#state, encodeGrant(grant)) ? { ...grant } : undefined;
  }

  bindings(): FabricCapabilityBindingView[] {
    return this.active ? [...this.#grants.values()].map(grant => ({ ...grant })) : [];
  }
}
