import type { ToolDefinition } from "@earendil-works/pi-coding-agent";

/** Pi 0.99 preserves raw MCP identity in namespace/label, not its hashed tool name. */
export function nativeMcpIdentity(
  definition: Pick<ToolDefinition, "name" | "label" | "namespace"> | undefined,
): { server: string; tool: string } | undefined {
  const namespace = definition?.namespace?.name;
  if (!definition || !namespace?.startsWith("mcp__") || !definition.name.startsWith("mcp__")) return undefined;
  const server = namespace.slice(5);
  const prefix = `${server}/`;
  if (!server || !definition.label.startsWith(prefix)) return undefined;
  const tool = definition.label.slice(prefix.length);
  return tool ? { server, tool } : undefined;
}

export function isSelectedNativeMcpTool(
  definition: Pick<ToolDefinition, "name" | "label" | "namespace"> | undefined,
  servers: readonly string[] = [],
): boolean {
  const identity = nativeMcpIdentity(definition);
  return identity !== undefined && servers.includes(identity.server);
}
