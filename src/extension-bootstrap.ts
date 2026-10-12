// Pi's registration entry excludes the public audit/entropy library barrels.
// Register the real tool and lifecycle hooks; never hide work in a background import.
import { createFabricExtension } from "./extension.js";
export { FABRIC_MANAGED_HOST_VERSION } from "./extension.js";
export type { FabricManagedHostOptions } from "./managed-host.js";
export default createFabricExtension(import.meta.url);
