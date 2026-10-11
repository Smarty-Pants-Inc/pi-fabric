import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { nativeMcpIdentity } from "./native-mcp-identity.js";

export const NATIVE_CORE_TOOLS = new Set(["read", "bash", "powershell", "edit", "write", "grep", "find", "ls"]);
export const FABRIC_TOOLS_MEMBERS = new Set(["providers", "catalog", "list", "search", "describe", "call", "progress", "models"]);
export const nativeToolIdentifier = (name: string): string => {
  const identifier = name.replace(/[^a-zA-Z0-9_$]/g, "_");
  return /^[a-zA-Z_$]/.test(identifier) ? identifier : `_${identifier}`;
};
export function nativeToolRef(tool: Pick<ToolDefinition, "name" | "label" | "namespace">, identify: typeof nativeMcpIdentity): string {
  if (NATIVE_CORE_TOOLS.has(tool.name)) return `pi.${tool.name}`;
  const identity = identify(tool);
  return identity ? `mcp.${identity.server}.${identity.tool}` : `extensions.${tool.name}`;
}
/** Metadata-only resolution for streaming speculation; ambiguity always skips warming. */
export function speculativeNativeRef(name: string, definitions: readonly ToolDefinition[] = [], identify?: typeof nativeMcpIdentity): string | undefined {
  const refs = new Set<string>();
  if (NATIVE_CORE_TOOLS.has(name)) refs.add(`pi.${name}`);
  for (const tool of definitions) {
    if (tool.exposure === "hidden" || tool.exposure === "model-only" || tool.name === "codemode") continue;
    if (!identify) return undefined;
    if (nativeToolIdentifier(tool.name) === name) refs.add(nativeToolRef(tool, identify));
  }
  return refs.size === 1 ? [...refs][0] : undefined;
}
