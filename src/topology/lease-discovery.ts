import fs from "node:fs";
import { setImmediate as nextTurn } from "node:timers/promises";

/** Limit both outstanding filesystem requests and CPU work between check-phase yields.
 * readdir/stat/readFile run in libuv, not in a timer that later blocks the comms loop. */
export const LEASE_DISCOVERY_BATCH = 32;

/** Complete, uncached enumeration. Directory churn is not an excuse to truncate a listing.
 * Consumers needing a cross-file snapshot must still bracket/fence it in their own commit domain. */
export const hostLeaseFileNames = async (directory: string): Promise<string[]> => {
  const names = await fs.promises.readdir(directory);
  const leases: string[] = [];
  for (let offset = 0; offset < names.length; offset += LEASE_DISCOVERY_BATCH) {
    for (const name of names.slice(offset, offset + LEASE_DISCOVERY_BATCH)) {
      if (name.endsWith(".json")) leases.push(name);
    }
    if (offset + LEASE_DISCOVERY_BATCH < names.length) await nextTurn();
  }
  return leases;
};

/** No Promise.all over the fleet: at most one bounded batch of IO is outstanding. */
export const visitHostLeaseFiles = async (names: readonly string[], visit: (name: string) => Promise<void>): Promise<void> => {
  for (let offset = 0; offset < names.length; offset += LEASE_DISCOVERY_BATCH) {
    await Promise.all(names.slice(offset, offset + LEASE_DISCOVERY_BATCH).map(visit));
    if (offset + LEASE_DISCOVERY_BATCH < names.length) await nextTurn();
  }
};
