import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { nativeMcpIdentity } from "./core/native-mcp-identity.js";
import type { ResolvedFabricAction } from "./core/action-registry.js";
import { nativeToolIdentifier, nativeToolRef } from "./core/native-tool-names.js";
import { describeFabricActionDeclaration } from "./runtime/dynamic-guest-types.js";

export interface NativeToolEntry {
  name: string;
  rawName: string;
  ref: string;
  description: string;
  searchDescription: string;
  declaration: string;
  inputSchema: Record<string, unknown>;
  namespace?: { name: string; description?: string; instructions?: string };
}
export function nativeToolCatalog(actions: ResolvedFabricAction[], definitions: readonly ToolDefinition[], identify: typeof nativeMcpIdentity): NativeToolEntry[] {
  const entries: NativeToolEntry[] = [];
  for (const action of actions) {
    if (!["pi", "mcp", "extensions"].includes(action.provider)) continue;
    if (action.name.startsWith("$") || ["codemode", "fabric_exec"].includes(action.name)) continue;
    const definition = definitions.find(tool => nativeToolRef(tool, identify) === action.ref || (action.provider === "extensions" && tool.name === action.name));
    if (definition && (definition.exposure === "hidden" || definition.exposure === "model-only")) continue;
    const rawName = definition?.name ?? (action.provider === "mcp" ? `mcp__${action.name.replace(".", "__")}` : action.name);
    const name = nativeToolIdentifier(rawName);
    const namespace = definition?.namespace ?? (action.provider === "mcp" ? { name: `mcp__${action.namespace}` } : undefined);
    const fallbackOutput = action.provider === "pi" && action.name === "read"
      ? {anyOf: [{type: "string"}, {type: "object", properties: {type: {const: "image"}, data: {type: "string"}, mimeType: {type: "string"}, note: {type: "string"}}, required: ["type", "data", "mimeType", "note"]}]}
      : {type: "string"};
    const declaration = describeFabricActionDeclaration(name, action.inputSchema, (definition?.outputSchema ?? action.outputSchema ?? fallbackOutput) as Record<string, unknown>);
    const searchDescription = definition?.description ?? action.description;
    const description = `${searchDescription}\n${declaration}`;
    entries.push({name, rawName, ref: action.ref, description, searchDescription, declaration, inputSchema: action.inputSchema, ...(namespace ? {namespace} : {})});
  }
  return entries;
}
export function resolveNativeTool(entries: NativeToolEntry[], name: unknown): NativeToolEntry {
  if (typeof name !== "string") throw new Error("Native tool name must be a string");
  const matches = entries.filter(entry => entry.name === name || entry.rawName === name);
  if (matches.length !== 1) throw new Error(`Unknown or ambiguous native tool: ${name}; use fabric.tools.describe/call with an exact Fabric ref`);
  return matches[0]!;
}

// Pi's BM25 contract: camel-case tokenization, singular terms and stable ties.
const stopWords = new Set(["a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it", "of", "on", "or", "that", "the", "this", "to", "with"]);
const tokenize = (text: string): string[] => text.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2").toLowerCase().split(/[^a-z0-9]+/).filter(term => term && !stopWords.has(term)).map(term => {
  if (term.length > 4 && term.endsWith("ies")) return `${term.slice(0, -3)}y`;
  if (term.length > 4 && /(ches|shes|sses|xes|zes)$/.test(term)) return term.slice(0, -2);
  return term.length > 3 && term.endsWith("s") && !term.endsWith("ss") ? term.slice(0, -1) : term;
});
function schemaText(schema: unknown): string[] {
  if (!schema || typeof schema !== "object" || Array.isArray(schema)) return [];
  const value = schema as Record<string, unknown>;
  return [typeof value.description === "string" ? value.description : "", ...Object.entries(value.properties ?? {}).flatMap(([key, nested]) => [key, ...schemaText(nested)]), ...schemaText(value.items), ...["anyOf", "oneOf", "allOf"].flatMap(key => Array.isArray(value[key]) ? value[key].flatMap(schemaText) : [])];
}
export function searchNativeTools(entries: NativeToolEntry[], query: unknown, limit: unknown = 8, namespace?: unknown): NativeToolEntry[] {
  if (typeof query !== "string") throw new Error("searchTools() expects a query string");
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit <= 0) throw new Error("searchTools() limit must be a positive integer");
  if (namespace != null && typeof namespace !== "string") throw new Error("searchTools() namespace must be a string");
  const docs = entries.filter(entry => !namespace || entry.namespace?.name === namespace || (entry.namespace && nativeToolIdentifier(entry.namespace.name) === namespace));
  const terms = [...new Set(tokenize(query))];
  const counts = docs.map(entry => {
    const map = new Map<string, number>();
    for (const term of tokenize([entry.rawName, entry.rawName.replaceAll("_", " "), entry.searchDescription, ...schemaText(entry.inputSchema), entry.namespace?.name, entry.namespace?.description, entry.namespace?.instructions].filter(Boolean).join(" "))) map.set(term, (map.get(term) ?? 0) + 1);
    return map;
  });
  const lengths = counts.map(map => [...map.values()].reduce((sum, n) => sum + n, 0));
  const average = lengths.reduce((sum, n) => sum + n, 0) / docs.length || 1;
  return docs.map((entry, i) => ({entry, score: terms.reduce((sum, term) => {
    const count = counts[i]!.get(term) ?? 0;
    const frequency = counts.filter(map => map.has(term)).length;
    return sum + Math.log(1 + (docs.length - frequency + 0.5) / (frequency + 0.5)) * count * 2.2 / (count + 1.2 * (0.25 + 0.75 * lengths[i]! / average));
  }, 0)})).filter(item => item.score > 0).sort((a,b) => b.score - a.score).slice(0, limit).map(item => item.entry);
}
