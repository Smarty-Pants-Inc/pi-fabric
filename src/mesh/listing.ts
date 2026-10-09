/** A bounded remote scan has more candidates; no partial provider result is returned. */
export class MeshListingIncompleteError extends Error {
  readonly code = "MESH_LISTING_INCOMPLETE";
  constructor(
    readonly prefix: string,
    readonly limit: number,
    readonly examined: number,
    readonly nextRevision: number,
  ) {
    super(`Fabric mesh listing incomplete for prefix ${JSON.stringify(prefix)} after ${examined} examined keys (limit ${limit}); increase the limit or enumerate backend listPage from revision ${nextRevision}`);
    this.name = "MeshListingIncompleteError";
  }
}
