import fs from "node:fs";
import path from "node:path";

/** Fixture-only: synthetic logical clocks must also age filesystem activity.
 * Never follows links, and callers supply only the root they just created. */
export const ageRunRoot = (root: string, at: number): void => {
  const stat = fs.lstatSync(root);
  if (stat.isSymbolicLink()) return;
  if (stat.isDirectory()) for (const name of fs.readdirSync(root)) ageRunRoot(path.join(root, name), at);
  fs.utimesSync(root, at / 1000, at / 1000);
};
