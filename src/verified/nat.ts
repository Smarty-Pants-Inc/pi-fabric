// Bend 2.0.26's JavaScript backend rejects arithmetic naturals above this bound.
// Comparison-only primitives can compare larger JS-safe integers. Storage uses
// radix limbs to keep its full safe-integer revision arithmetic in this domain.
export const BEND_NAT_MAX = 2 ** 48 - 1;
