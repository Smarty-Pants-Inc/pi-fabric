export type BendList<T> = { $: "Nil" } | { $: "Con"; head: T; tail: BendList<T> };
export type ResourceName = BendList<number>;
export type ResourceScope = { $: "Unknown" } | { $: "Exact"; names: BendList<ResourceName> };
export type ResourceEffect = { $: "Quiet" } | { $: "Effect"; ordered: boolean; scope: ResourceScope };
export interface Span { $: "Span"; first: number; last: number; paired: boolean }
export interface Cut { $: "Cut"; eligible: boolean; afterPrevious: boolean; retained: number; budget: number; boundary: number; spans: BendList<Span> }
export interface Chunk { $: "Chunk"; start: number; end: number; total: number; expectedStart: number; expectedTotal: number; length: number; complete: boolean }
