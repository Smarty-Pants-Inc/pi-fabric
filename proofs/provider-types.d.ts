export type StateText = { $: "Nil" } | { $: "Con"; head: bigint; tail: StateText };
export interface StateGrant { $: "Grant"; active: boolean; key: StateText }
export type StatePlan = { $: "Denied" } | { $: "Read"; key: StateText } | { $: "Write"; key: StateText; expected: StateText; value: StateText };
export interface ProviderTicket { $: "Ticket"; grant: StateGrant; expected: StateText; payload: StateText }
export interface ProviderOutcome { $: "Outcome"; next: ProviderTicket; plan: StatePlan }
