/** Parent-manager/worker CLI, status, lifecycle and steering contract.
 * Bump on an incompatible boundary change; installed releases advertise this
 * value in dist/worker-protocol.json without executing their code in the parent.
 */
// v2 requires the native final-answer receipt, receipt-time manager barrier,
// and attributed per-sender message.refused handling. Mixing v1/v2 can revive
// terminal tasks or silently discard mandatory queued-input refusal notices.
export const WORKER_PROTOCOL_VERSION = 2;
