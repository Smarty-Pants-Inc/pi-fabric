import type fs from "node:fs";

/** Startup-light refusal policy shared by private-dir and the eager temp-root (no runtime imports). */
export class UnsafeDirectory extends Error {}

/** A root-owned sticky shared directory (/tmp) cannot be passed on a walk without fd pinning (smarty-dev#9527). */
export const refuseSharedSticky = (stat: fs.Stats, walked: string, target: string): void => {
  if (stat.uid === 0 && (stat.mode & 0o1000) !== 0 && (stat.mode & 0o022) !== 0) {
    throw new UnsafeDirectory(`${walked} is a shared sticky directory (mode ${(stat.mode & 0o7777).toString(8)}): on this platform `
      + `Fabric cannot keep ${target} private under it. Use a directory under $HOME or the per-user $TMPDIR (/var/folders/...), `
      + "for example by setting TMPDIR or PI_FABRIC_TMPDIR (smarty-dev#9527)");
  }
};
