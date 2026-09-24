// Generated ABI declarations; see proofs/storage-abi.json.
export type StorageText = { $: "Nil" } | { $: "Con"; head: bigint; tail: StorageText };
/** Canonical radix-2^32 limbs: high <= 2^21-1, low <= 2^32-1. */
export interface StorageRevision { $: "Revision"; high: bigint; low: bigint }
export type StorageExpectation = { $: "Any" } | { $: "At"; version: StorageRevision };
export type StorageChange = { $: "Put"; value: StorageText; identity: StorageText } | { $: "Delete" };
export interface StorageRequest { $: "Request"; key: StorageText; expected: StorageExpectation; change: StorageChange }
export interface StorageSlot { $: "Slot"; present: boolean; version: StorageRevision; highWater: StorageRevision }
export type StoragePlan = { $: "Conflict" } | { $: "Exhausted" } | { $: "Unchanged" } | { $: "PutNext"; key: StorageText; value: StorageText; identity: StorageText; version: StorageRevision; highWater: StorageRevision } | { $: "DeleteNext"; key: StorageText; version: StorageRevision; highWater: StorageRevision };

export declare function storageTransition(request: StorageRequest, slot: StorageSlot): StoragePlan;
