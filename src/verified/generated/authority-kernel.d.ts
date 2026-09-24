// Generated ABI declarations; see proofs/authority-abi.json.
export type AuthorityText = { $: "Nil" } | { $: "Con"; head: bigint; tail: AuthorityText };
export type AuthorityGrants = { $: "Nil" } | { $: "Con"; head: AuthorityText; tail: AuthorityGrants };
export type AuthorityState = { $: "Released" } | { $: "Active"; grants: AuthorityGrants };

export declare function authorityIssue(grants: AuthorityGrants): AuthorityState;
export declare function authorityLive(authority: AuthorityState): boolean;
export declare function authorityAllows(authority: AuthorityState, grant: AuthorityText): boolean;
export declare function authorityDerive(parent: AuthorityState, candidate: AuthorityGrants): AuthorityState;
export declare function authorityRelease(authority: AuthorityState): AuthorityState;
