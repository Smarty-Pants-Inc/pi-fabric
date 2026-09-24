export type BindingPhase = { $: "Staged" | "Active" | "Retiring" | "Closing" | "Closed" | "Failed" };
export interface BindingLife { $: "Life"; phase: BindingPhase; owner: boolean; holds: bigint; calls: bigint; revoked: boolean }
export type BindingEvent = { $: "Inspect" | "Activate" | "Retire" | "DropOwner" | "Revoke" | "Release" | "End" | "Close" | "Complete" | "Fail" } | { $: "Retain" | "Begin"; cleanup: boolean };
export interface BindingOutcome { $: "Outcome"; next: BindingLife; command: { $: "Denied" | "Granted" | "StartClose" } }
