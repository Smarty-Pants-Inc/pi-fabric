/** One bounded wait for unconfirmed remote mirror authority; equal to BRIDGE_LEASE_MS. */
export const PEER_LEASE_WAIT_MS = 15_000;

export const PEER_LEASE_STALE_MESSAGE = "peer lease stale; the peer may be live: retry or use the mesh";

/** A validated remote Main's mirror authority stayed unconfirmed for one wait (fv2-11703).
 * Pre-publication: nothing was sent. This is not evidence that its source lease lapsed.
 * lapsedMs is only a nonnegative projection-overdue diagnostic, not proof of source death. */
export class FabricPeerLeaseStaleError extends Error {
  override readonly name = "FabricPeerLeaseStaleError";
  readonly code = "FABRIC_PEER_LEASE_STALE";
  readonly retryable = true;
  constructor(readonly targetId: string, readonly remoteHost: string, readonly lapsedMs: number) {
    super(`Fabric peer ${targetId} on remote host ${remoteHost}: ${PEER_LEASE_STALE_MESSAGE} ` +
      `(mirrored authority/freshness unconfirmed; projection overdue ${Math.round(lapsedMs / 1000)} s); this attempt was not published.`);
  }
}
