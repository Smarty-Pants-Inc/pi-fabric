export type BendList<T> = { $: "Nil" } | { $: "Con"; head: T; tail: BendList<T> };
export type ResourceName = BendList<bigint>;
export type ResourceScope = { $: "Unknown" } | { $: "Exact"; names: BendList<ResourceName> };
export type ResourceEffect = { $: "Quiet" } | { $: "Effect"; ordered: boolean; scope: ResourceScope };
export interface Span { $: "Span"; first: bigint; last: bigint; paired: boolean }
export interface Cut { $: "Cut"; eligible: boolean; afterPrevious: boolean; retained: bigint; budget: bigint; boundary: bigint; spans: BendList<Span> }
export interface Chunk { $: "Chunk"; start: bigint; end: bigint; total: bigint; expectedStart: bigint; expectedTotal: bigint; length: bigint; complete: boolean }
