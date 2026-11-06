import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { findExecutable } from "../agents/transports/process-utils.js";
import type { ResidentHostConfig } from "./protocol.js";

type ResidentLaunchBinaries = Pick<ResidentHostConfig, "piBinary" | "claudeBinary" | "vedaBinary">;

/** Resolve binaries at persistence or launch; only persistence may defer resolution errors. */
export function sealResidentLaunchBinaries(config: ResidentLaunchBinaries, releaseRoot: string,
  options: { deferErrors?: boolean } = {}): ResidentLaunchBinaries {
  const pin = (binary: string, required = false): string => {
    try { return pinBinary(binary, releaseRoot, required); }
    catch (error) {
      if (!options.deferErrors) throw error;
      // Keep the failing selector for strict launch-time validation.
      return binary;
    }
  };
  return {
    piBinary: pin(config.piBinary, true),
    claudeBinary: pin(config.claudeBinary),
    vedaBinary: pin(config.vedaBinary),
  };
}

function pinBinary(binary: string, releaseRoot: string, required = false): string {
  // Sealed absence must be idempotent when reconstructing an already-pinned
  // specification. A new file here still changes the package JS closure hash.
  if (!required && path.dirname(binary) === path.join(releaseRoot, "dist/residency") &&
      /^unavailable-[0-9a-f]{64}\.js$/.test(path.basename(binary))) return binary;
  const found = path.isAbsolute(binary) ? (fs.existsSync(binary) ? binary : undefined) : findExecutable(binary);
  if (!found) {
    if (required) throw new Error(`Resident handover cannot resolve binary: ${binary}`);
    // Freeze known absence too: a later PATH change/install must not silently
    // supply a different runner to the A fallback. This missing .js path belongs
    // to the sealed closure; creating it later invalidates the artifact digest.
    return path.join(releaseRoot, "dist/residency", `unavailable-${createHash("sha256").update(binary).digest("hex")}.js`);
  }
  return fs.realpathSync(found);
}
