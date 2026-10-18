import type { FabricKernel } from "../runtime/kernel.js";
import { existsSync, readFileSync } from "node:fs";
import * as path from "node:path";

// Host globals that the QuickJS guest (and its type check) does not have; the fleet's
// residual "Cannot find name" failures were these (smarty-dev#459).
export const MISSING_HOST_GLOBALS = [
  "process", "fetch", "btoa", "atob", "TextEncoder", "TextDecoder", "crypto", "Buffer",
  "URL", "URLSearchParams", "structuredClone", "require", "Bun", "Deno",
] as const;

// smarty-dev#2340: payload values are passed verbatim; placeholder tokens and
// `@/path` / `file://` references are rejected before any runtime executes.
export const PAYLOADS_LITERAL_GUIDANCE = "payloads are literal: read files with native tools first";

export const hostGlobalsGuidance = (fullCodeMode: boolean): string =>
  "The QuickJS fabric_exec kernel has no host globals: `process`, `fetch`, `btoa`/`atob`, `TextEncoder`/`TextDecoder`, `crypto`, `Buffer`, `URL`, `structuredClone`, `require` and `Bun` do not exist. " +
  `For environment, network, encoding or hashing, run a shell command (\`env\`, \`curl\`, \`base64\`, \`sha256sum\`) with ${fullCodeMode ? "`pi.bash`" : "the native `bash` tool, outside `fabric_exec`"}; for data, use the \`JSON\`, \`String\` and \`Array\` builtins.`;

// Kept as public compatibility hooks; execution advice is on demand in the skills.
export const fabricExecutionKernelGuidance = (fullCodeMode: boolean, kernel: FabricKernel = "typescript", pythonRuntime: "cpython" | "monty" = "monty", profile: "additive" | "native" = "additive"): string => "";

export const defaultFabricExecutionGuidance = (fullCodeMode: boolean, kernel: FabricKernel = "typescript", pythonRuntime: "cpython" | "monty" = "monty", profile: "additive" | "native" = "additive"): string => "";

// Shape of CapturedToolCatalog entries this renderer needs (kept structural to avoid a runtime dependency on the capture layer from a guidance module).
export interface ExtensionRosterToolSource {
  name: string;
  sourceInfo?: { source?: string; path?: string };
}

// Namespace labels come from the extension package's own identity: the
// package.json `name` nearest the tool's source file, mirroring how pi names
// npm-installed packages. Raw `source` strings are configured specifiers that
// are often full relative paths, so they are only used when no manifest exists.
const manifestNameCache = new Map<string, string | undefined>();

const packageNameFromManifest = (startPath: string | undefined): string | undefined => {
  if (!startPath) return undefined;
  let directory = path.dirname(path.resolve(startPath));
  while (true) {
    if (manifestNameCache.has(directory)) return manifestNameCache.get(directory);
    const manifestPath = path.join(directory, "package.json");
    let name: string | undefined;
    if (existsSync(manifestPath)) {
      try {
        const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as { name?: unknown };
        if (typeof manifest.name === "string" && manifest.name.trim()) name = manifest.name.trim();
      } catch {
        // Unreadable or invalid manifest; keep walking upward.
      }
    }
    manifestNameCache.set(directory, name);
    if (name) return name;
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
};

// In full code mode the model sees only fabric_exec in its tool list, so
// registered extension tools are invisible unless named up front (#69). The
// roster stays names-only: descriptions and schemas are on demand through the
// tools.list/search/describe discovery surface, so the standing prompt cost is
// a bare name index. Core overrides are excluded: they surface as pi.* via
// coreOverridePromptGuidance.
export const extensionToolRosterGuidance = (
  tools: ReadonlyArray<ExtensionRosterToolSource>,
  coreToolNames: ReadonlySet<string>,
  nativeProfile = false,
): string | undefined => {
  const extensionTools = tools.filter((tool) => !coreToolNames.has(tool.name));
  if (extensionTools.length === 0) return undefined;
  const namespaceLabel = (tool: ExtensionRosterToolSource): string => {
    const source = tool.sourceInfo?.source?.trim();
    if (source?.startsWith("npm:")) return source.slice("npm:".length) || source;
    const manifestName =
      packageNameFromManifest(tool.sourceInfo?.path) ??
      packageNameFromManifest(source && /[\\/]/.test(source) ? source : undefined);
    if (manifestName) return manifestName;
    if (source && !/[\\/]/.test(source)) return source;
    const parts = (tool.sourceInfo?.path ?? source ?? "").split(/[\\/]/).filter(Boolean);
    const base = parts.at(-1)?.trim() ?? "";
    // Entry files like index.js name the package directory, not the source.
    if (/^index\./i.test(base)) return parts.at(-2)?.trim() || base;
    return base || "extensions";
  };
  const groups = new Map<string, string[]>();
  for (const tool of [...extensionTools].sort((left, right) => left.name.localeCompare(right.name))) {
    const label = namespaceLabel(tool);
    const names = groups.get(label);
    if (names) names.push(tool.name);
    else groups.set(label, [tool.name]);
  }
  return [
    `Registered extension tools are callable inside fabric_exec as \`extensions.<name>(args)\`; run \`${nativeProfile ? "fabric.tools" : "tools"}.list\` for full descriptions and schemas before re-implementing an effect with pi.bash.`,
    ...[...groups.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([label, names]) => "- " + label + ": " + names.join(", ")),
  ].join("\n");
};

export const fabricSchemaGuidance = (mode: "off" | "audit" | "enforce"): string | undefined => {
  if (mode === "enforce") {
    return "Schema enforce mode is fixed for this session. Reads remain available, but protected-workspace changes must use schema.hypothesize → schema.verify → schema.commit in the same fabric_exec invocation. Direct pi.edit/write/bash/powershell, agents, state/mesh writes, compaction requests, MCP, extensions, and external providers are blocked by the host gate.";
  }
  if (mode === "audit") {
    return "Schema audit mode reports actions that enforce mode would block, but preserves their current behavior.";
  }
  return undefined;
};
