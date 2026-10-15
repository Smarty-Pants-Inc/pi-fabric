/** One bounded wait for a lapsed remote mirror; equal to the mesh bridge lease (BRIDGE_LEASE_MS). */
export const PEER_LEASE_WAIT_MS = 15_000;

export const PEER_LEASE_STALE_MESSAGE = "peer lease stale; the peer may be live: retry or use the mesh";

/** A validated remote Main's mirrored lease stayed lapsed for one bounded wait (fv2-11703).
 * Pre-publication: nothing was sent. The peer may be live; it is never reported as Unknown. */
export class FabricPeerLeaseStaleError extends Error {
  override readonly name = "FabricPeerLeaseStaleError";
  readonly code = "FABRIC_PEER_LEASE_STALE";
  readonly retryable = true;
  constructor(readonly targetId: string, readonly remoteHost: string, readonly lapsedMs: number) {
    super(`Fabric peer ${targetId} on remote host ${remoteHost}: ${PEER_LEASE_STALE_MESSAGE} (mirrored lease lapsed ${Math.round(lapsedMs / 1000)} s ago).`);
  }
}
