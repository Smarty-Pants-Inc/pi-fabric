import type { CapturedToolCatalog } from "./capture/catalog.js";
import type { ResolvedFabricAction } from "./core/action-registry.js";
import type { nativeMcpIdentity } from "./core/native-mcp-identity.js";

/** Attach real namespace metadata only after the caller has filtered actions
 * through Fabric discovery/scope/capability visibility. Never treat descriptions
 * as instructions, or let an invisible registered tool reveal its namespace. */
export function describeNativeNamespace(
  name: string, visible: ResolvedFabricAction[], captured: CapturedToolCatalog | undefined,
  // Supplied by the already-loaded host; avoid splitting an eager shared chunk.
  identify: typeof nativeMcpIdentity,
) {
  const refs = new Set(visible.map(action => action.ref));
  const metadata = (captured?.registeredTools() ?? []).flatMap(({ definition }) => {
    if (!definition.namespace || definition.namespace.name !== name || definition.exposure === "hidden" || definition.exposure === "model-only") return [];
    const identity = identify(definition);
    const ref = identity ? `mcp.${identity.server}.${identity.tool}` : `extensions.${definition.name}`;
    return refs.has(ref) ? [{ ref, namespace: definition.namespace }] : [];
  });
  const nativeRefs = new Set(metadata.map(item => item.ref));
  const tools = visible.filter(action => action.namespace === name || action.provider === name || nativeRefs.has(action.ref)).map(action => ({ name: action.ref, description: action.description }));
  if (!tools.length) return undefined;
  const namespace = metadata[0]?.namespace;
  return { name, ...(namespace?.description !== undefined ? { description: namespace.description } : {}), ...(namespace?.instructions !== undefined ? { instructions: namespace.instructions } : {}), tools };
}
