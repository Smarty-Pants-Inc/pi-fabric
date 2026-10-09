/** Parent-manager/worker CLI, status, lifecycle and steering contract.
 * Bump on an incompatible boundary change; installed releases advertise this
 * value in dist/worker-protocol.json without executing their code in the parent.
 */
export const WORKER_PROTOCOL_VERSION = 1;

export { LEASE_FORMAT_UUID_MIN } from "../core/agent-dir.js";
