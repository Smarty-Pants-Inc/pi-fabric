import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { loadFabricConfig, type FabricConfig } from "./config.js";
import { resolveAgentDir } from "./core/agent-dir.js";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { FABRIC_THINKING_ENTRY_TYPE } from "./thinking.js";
import type { StartingHostClaim } from "./topology/starting-host.js";

/** Cheap file eligibility only; idle non-announcing sessions never import the lease engine. */
export const claimNativeSessionHost = async (context: ExtensionContext,
  id: string, sessionId: string): Promise<StartingHostClaim | undefined> => {
  const config = loadFabricConfig({ cwd: context.cwd, agentDir: resolveAgentDir(), projectTrusted: true });
  if (!config.mesh.enabled || config.schema.mode === "enforce") return undefined;
  const projectRoot = process.env.PI_FABRIC_PROJECT_ROOT ?? context.cwd;
  const meshRoot = process.env.PI_FABRIC_MESH_ROOT ?? (config.mesh.root
    ? path.resolve(projectRoot, config.mesh.root) : path.join(projectRoot, ".pi", "fabric", "mesh"));
  if (!config.mesh.announce && !fs.existsSync(path.join(meshRoot, "host-leases",
    createHash("sha256").update(id).digest("hex").slice(0, 32) + ".json"))) return undefined;
  const { claimStartingHost } = await import("./topology/starting-host.js");
  return claimStartingHost({ meshRoot, cwd: context.cwd, id, sessionId,
    lockProtocol: config.mesh.lockProtocol, stateBackend: config.mesh.stateBackend });
};

/** Persisted overrides are inspected at agent_end first use, not extension registration. */
export const branchHasThinkingOverride = (context: Pick<ExtensionContext, "sessionManager">): boolean => {
  const branch = context.sessionManager.getBranch();
  for (let index = branch.length - 1; index >= 0; index -= 1) {
    const entry = branch[index];
    if (entry?.type !== "custom" || entry.customType !== FABRIC_THINKING_ENTRY_TYPE) continue;
    const data = entry.data as { override?: unknown } | undefined;
    return data?.override !== null && data?.override !== undefined;
  }
  return false;
};

/** First activation only: bootstrap need not load the component configuration engine. */
export const reconcileActivationComponents = async (context: ExtensionContext, config: FabricConfig,
  agentDir: string, assertCurrent: () => void): Promise<void> => {
  const { FabricComponentConfiguration } = await import("./components/configuration.js");
  assertCurrent();
  try {
    config.components = new FabricComponentConfiguration({
      cwd: context.cwd, agentDir, projectTrusted: () => context.isProjectTrusted(),
    }).read().entries;
  } catch (error) {
    // Keep the bootstrap manifest usable for repair; the live control plane
    // records the read failure and watches for a corrected file.
    if (context.hasUI) context.ui.notify(`Pi Fabric component configuration not applied: ${error instanceof Error ? error.message : String(error)}`, "error");
  }
};
