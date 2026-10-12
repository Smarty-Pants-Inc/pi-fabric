# MCP surface reference

MCP tools are available inside `fabric_exec` through the `mcp` surface. By default the public `mcporter` runtime owns discovery, imports, OAuth cache, and pooling. Hosts may opt exact server names into Pi-owned connections with `mcp.nativeServers`; the guest call surface stays the same. For the sandbox model and `tools` discovery, see the parent `fabric-exec` skill.

## Call a tool

`mcp.<sanitized_server>.<sanitized_tool>(args)` takes one options object matching the tool input schema. JavaScript-safe names work unchanged; other names replace non-identifier characters with `_` (and gain a leading `_` when needed). Fabric resolves those aliases back to the original MCP names. Use exact names through `mcp.call` when an alias is ambiguous.

```ts
const result = await mcp.context7.resolve_library_id({ libraryName: "react", query: "hooks" });
const imageSchema = await mcp.fal_ai.get_model_schema({ endpoint_id: "openai/gpt-image-2" });
return { result, imageSchema };
```

The second call resolves MCP server `fal-ai` and tool `get-model-schema`.

## Server management

- `mcp.servers()` returns server metadata `{ name, description, transport }` (description may be null; transport is "http" or "stdio"). Credentials are never exposed.
- `mcp.reload()` returns `{ servers }`. Call it after changing mcporter configuration.
- `mcp.register({ name, description?, command?, args?, cwd?, baseUrl?, headers?, env?, overwrite? })` returns `{ registered }`. Register an ephemeral server in the pooled runtime after host approval. HTTP servers use `baseUrl` instead of `command`. Dynamic definitions live until `mcp.reload()` or session shutdown; they are not written to config.

```ts
await mcp.register({ name: "project-docs", command: "npx", args: ["-y", "@example/docs-mcp"], cwd: "." });
return mcp.project_docs.search({ query: "authentication" });
```

## Generic call

`mcp.call({ server, tool, args? })` is for exact names that cannot be expressed unambiguously as property access, or for names computed at runtime.

```ts
return mcp.call({ server: "my-server", tool: "weird-tool-name", args: { q: "x" } });
```

## Introspect an uncertain tool

MCP tools are discoverable through the generic `tools` surface; refs are `mcp.<server>.<tool>`.

```ts
const schema = await tools.describe({ ref: "mcp.context7.resolve_library_id" });
// schema.inputSchema is the tool JSON Schema
```

## Notes

- Pi-owned servers appear in `mcp.servers()` with `transport: "pi"`; manage their connections/authentication with Pi's `/mcp`. `mcp.reload()` and `mcp.register()` manage mcporter only and cannot replace Pi-owned names. There is no automatic transport fallback.
- Results retain `{ text, content, structuredContent }`; native middleware redactions apply before results reach the guest. Tool names/descriptions and schema discovery remain authoritative; do not infer names from Pi's potentially hashed `mcp__...` aliases.
- When `mcp.disableOAuth` is configured, mcporter calls may use cached credentials but cannot launch a new interactive OAuth flow. Pi owns authentication for selected native servers.
- Call timeout is bounded by `mcp.callTimeoutMs`.
- Set `mcp.enabled: false` to disable the MCP surface.
