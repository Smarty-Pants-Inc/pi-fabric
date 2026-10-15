// Generated ABI declarations; see proofs/abi.json.
export type BendList<T> = { $: "Nil" } | { $: "Con"; head: T; tail: BendList<T> };
export type ResourceName = BendList<number>;
export type ResourceScope = { $: "Unknown" } | { $: "Exact"; names: BendList<ResourceName> };
export type ResourceEffect = { $: "Quiet" } | { $: "Effect"; ordered: boolean; scope: ResourceScope };
export interface Span { $: "Span"; first: number; last: number; paired: boolean }
export interface Cut { $: "Cut"; eligible: boolean; afterPrevious: boolean; retained: number; budget: number; boundary: number; spans: BendList<Span> }
export interface Chunk { $: "Chunk"; start: number; end: number; total: number; expectedStart: number; expectedTotal: number; length: number; complete: boolean }

export declare function footprintFits(count: number, limit: number, valid: boolean): boolean;
export declare function unknownConflict(leftUnknown: boolean, leftUnknownOrdered: boolean, leftOrdered: boolean, rightUnknown: boolean, rightUnknownOrdered: boolean, rightOrdered: boolean): boolean;
export declare function transitionCurrent(retired: boolean, sameEpoch: boolean, sameOwner: boolean, closed: boolean): boolean;
export declare function canClose(retiring: boolean, ownerRetained: boolean, hasRetainers: boolean, hasInFlight: boolean): boolean;
export declare function cleanupState(failed: boolean, diverted: boolean): number;
export declare function spansSafe(spans: BendList<Span>, boundary: number): boolean;
export declare function cutAccepted(cut: Cut): boolean;
export declare function pointerCurrent(sourceMatches: boolean, lineageMatches: boolean): boolean;
export declare function chunkAccepted(chunk: Chunk): boolean;
export declare function coverageComplete(sourceComplete: boolean, truncated: boolean): boolean;
export declare function useNormalized(canonical: boolean, plan: boolean, changed: boolean, accepted: boolean): boolean;
export declare function headReadable(sequence: boolean, version: boolean, committed: boolean, pending: boolean, marker: boolean): boolean;
export declare function certificateAccepted(conditions: BendList<boolean>): boolean;
export declare function consume(active: boolean): { $: 'Tuple'; fst: boolean; snd: boolean };
export declare function lineageSelected(allBranches: boolean, member: boolean): boolean;
export declare function knownConflict(shared: boolean, leftOrdered: boolean, rightOrdered: boolean): boolean;
export declare function summaryWithin(bytes: number, byteLimit: number, projected: number, target: number): boolean;
export declare function sampleWithin(total: number, retained: number, omitted: number, limit: number): boolean;
export declare function resourceSource(names: BendList<ResourceName>): ResourceScope;
export declare function resourceNormalize(original: ResourceScope, candidate: BendList<ResourceName>): ResourceScope;
export declare function resourceConflict(left: ResourceScope, right: ResourceScope, leftOrdered: boolean, rightOrdered: boolean): boolean;
export declare function resourceGroups(left: BendList<ResourceEffect>, right: BendList<ResourceEffect>): boolean;
