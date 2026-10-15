export type AuthorityText = { $: "Nil" } | { $: "Con"; head: number; tail: AuthorityText };
export type AuthorityGrants = { $: "Nil" } | { $: "Con"; head: AuthorityText; tail: AuthorityGrants };
export type AuthorityState = { $: "Released" } | { $: "Active"; grants: AuthorityGrants };
