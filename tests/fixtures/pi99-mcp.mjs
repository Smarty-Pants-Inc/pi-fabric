import { createInterface } from "node:readline";
const lines = createInterface({ input: process.stdin });
for await (const line of lines) {
  const request = JSON.parse(line);
  if (request.id === undefined) continue;
  let result;
  switch (request.method) {
    case "initialize": result = { protocolVersion: "2024-11-05", capabilities: { tools: {} }, serverInfo: { name: "pi99-fixture", version: "1" } }; break;
    case "tools/list": result = { tools: [{ name: "echo", description: "Offline echo", inputSchema: { type: "object", properties: { value: { type: "string" } }, required: ["value"] } }] }; break;
    case "tools/call": result = { content: [{ type: "text", text: `mcp:${request.params.arguments.value}` }], structuredContent: { value: request.params.arguments.value } }; break;
    case "ping": result = {}; break;
    default: process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, error: { code: -32601, message: "Unknown method" } }) + "\n"); continue;
  }
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }) + "\n");
}
