// The local mirror, not the host package: Pi loads this extension as native
// ESM, where a runtime import of the SDK bypasses Pi's jiti virtual modules
// and evaluates a second, unbundled SDK copy inside the bundled CLI.
import { CONFIG_DIR_NAME } from "./agent-dir.js";

// Single source of truth for Fabric's own project state directory. The runtime
// writes caches and stores there, so mutation-drift detection must read those
// writes as runtime bookkeeping, never as user work.
const FABRIC_STATE_DIR_NAME = "fabric";

// Relative-path form for surfaces that only see listing output (no root).
export const isFabricStateRelativePath = (relative: string): boolean => {
  const normalized = relative.replaceAll("\\", "/");
  const prefix = `${CONFIG_DIR_NAME}/${FABRIC_STATE_DIR_NAME}`;
  return normalized === prefix || normalized.startsWith(`${prefix}/`);
};
