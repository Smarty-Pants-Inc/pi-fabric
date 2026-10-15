// Bend's JavaScript backend (2.0.37, as 2.0.36) rejects arithmetic naturals above
// this bound; a Nat crosses to and from JS as a plain number since 2.0.37.
// Comparison-only primitives can compare larger JS-safe integers. Storage uses
// radix limbs to keep its full safe-integer revision arithmetic in this domain.
export const BEND_NAT_MAX = 2 ** 48 - 1;
