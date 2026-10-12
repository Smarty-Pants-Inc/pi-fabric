import path from "node:path";

/** The one Fabric entry Pi loads from this package: package.json `pi.extensions` (pi-fabric#864).
 * Every child Pi, resident worker and handover passes exactly this file with -e, so the profile's
 * discovered package and the explicit entry are the same extension. `dist/index.js` stays the
 * library import (package.json main/exports) and must never be handed to Pi alongside it.
 */
export const FABRIC_EXTENSION_ENTRY_FILE = "extension-bootstrap.js";
export const FABRIC_EXTENSION_ENTRY = `dist/${FABRIC_EXTENSION_ENTRY_FILE}`;

/** Absolute Fabric extension entry of the release rooted at `releaseRoot`. */
export const fabricExtensionEntry = (releaseRoot: string): string => path.join(releaseRoot, FABRIC_EXTENSION_ENTRY);
