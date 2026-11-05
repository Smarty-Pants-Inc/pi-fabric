import fs from "node:fs";
import { diagnostics } from "../../core/diagnostics.js";
import { normalizeAgentPlacement } from "../placement-config.js";
import type { AgentPlacementConfig } from "../placement-config.js";

/** Placement-only, host-owned policy refresh at launch; no watchers or config migration. */
export const liveAgentPlacement = (file: string, initial?: AgentPlacementConfig): (() => AgentPlacementConfig | undefined) => {
  let current = initial;
  let stamp: string | undefined;
  let lastError: string | undefined;
  return () => {
    try {
      let next: string;
      try {
        const stat = fs.statSync(file);
        // Include identity and ctime/size for atomic replacement and coarse mtime filesystems.
        next = `${stat.dev}:${stat.ino}:${stat.mtimeMs}:${stat.ctimeMs}:${stat.size}`;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        next = "missing";
      }
      if (next === stamp) return current;
      stamp = next; // Do not repeatedly parse/log an unchanged malformed edit.
      if (next === "missing") {
        current = undefined;
      } else {
        const document: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
        if (!document || typeof document !== "object" || Array.isArray(document)) throw new Error("Invalid host Fabric configuration");
        const agents = (document as Record<string, unknown>).agents;
        if (agents !== undefined && (!agents || typeof agents !== "object" || Array.isArray(agents))) throw new Error("Invalid host agents configuration");
        current = normalizeAgentPlacement((agents as Record<string, unknown> | undefined)?.placement);
      }
      lastError = undefined;
    } catch (error) {
      const message = String(error);
      if (message !== lastError) diagnostics.warn("src/agents/transports/placement-policy.ts", `[pi-fabric] agents.placement live refresh failed (${file}); retaining last valid policy: ${message}`);
      lastError = message;
    }
    return current;
  };
};
