import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// Loaded-code provenance for reload-freshness checks. A module's bytes on disk
// equal the bytes the loader consumed at the instant the module evaluates, so a
// hash captured here identifies the code this process actually runs; a rebuild
// changes the disk copy and a later comparison reports the divergence.
export interface FabricLoadedFileIdentity {
  path: string;
  sha256: string;
}

export interface FabricRuntimeIdentity {
  entry: FabricLoadedFileIdentity | null;
  lazyRuntime: FabricLoadedFileIdentity | null;
}

export interface FabricLoadedFileIdentityStatus {
  path: string;
  loadedSha256: string;
  diskSha256: string;
  stale: boolean;
}

const sha256OfFile = (path: string): string =>
  createHash("sha256").update(readFileSync(path)).digest("hex");

export const captureLoadedFileIdentity = (
  moduleUrl: string,
): FabricLoadedFileIdentity | null => {
  try {
    const path = fileURLToPath(moduleUrl);
    return { path, sha256: sha256OfFile(path) };
  } catch {
    // Registration and idle load must not fail because a provenance hash
    // could not be read; prewalk.status already treats a missing identity as null.
    return null;
  }
};

export const loadedFileIdentityStatus = (
  identity: FabricLoadedFileIdentity,
): FabricLoadedFileIdentityStatus => {
  let diskSha256: string;
  try {
    diskSha256 = sha256OfFile(identity.path);
  } catch {
    diskSha256 = "unavailable";
  }
  return {
    path: identity.path,
    loadedSha256: identity.sha256,
    diskSha256,
    stale: diskSha256 !== identity.sha256,
  };
};

export const runtimeIdentityStatus = (identity: FabricRuntimeIdentity): {
  entry: FabricLoadedFileIdentityStatus | null;
  lazyRuntime: FabricLoadedFileIdentityStatus | null;
} => ({
  entry: identity.entry ? loadedFileIdentityStatus(identity.entry) : null,
  lazyRuntime: identity.lazyRuntime ? loadedFileIdentityStatus(identity.lazyRuntime) : null,
});
