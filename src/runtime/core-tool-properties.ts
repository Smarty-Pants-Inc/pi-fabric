import {
  GUEST_TYPE_DECLARATIONS,
  PI_CORE_COMPATIBILITY_ARGUMENT_TYPE_NAMES,
} from "./guest-types.js";

// Core tool names come from the compatibility-argument table so a new guest
// tool extends the registry (and the recovery hints) without edits here.
type CoreToolName = keyof typeof PI_CORE_COMPATIBILITY_ARGUMENT_TYPE_NAMES;

export const CORE_TOOL_NAMES: readonly CoreToolName[] = Object.keys(
  PI_CORE_COMPATIBILITY_ARGUMENT_TYPE_NAMES,
) as CoreToolName[];

// The guest declarations are a template-literal string, so the option bags are
// extracted textually. Type aliases are matched with a bracket-depth scan (not
// `[^;]`) because union members like `{ edits: PiEditOperation[]; all?: boolean }`
// contain interior semicolons.
const extractTypeDeclarations = (declarations: string): Map<string, string> => {
  const parsed = new Map<string, string>();
  for (const match of declarations.matchAll(/\btype\s+(\w+)\s*=/g)) {
    const name = match[1];
    if (name === undefined) continue;
    const rhsStart = match.index + match[0].length;
    let depth = 0;
    let end = rhsStart;
    while (end < declarations.length) {
      const character = declarations[end];
      if (character === "(" || character === "{" || character === "[") depth += 1;
      else if (character === ")" || character === "}" || character === "]") depth -= 1;
      else if (character === ";" && depth === 0) break;
      end += 1;
    }
    parsed.set(name, declarations.slice(rhsStart, end));
  }
  return parsed;
};

const matchCaptures = (text: string, pattern: RegExp): string[] =>
  [...text.matchAll(pattern)].flatMap((match) =>
    match[1] === undefined ? [] : [match[1]],
  );

const objectLiteralKeys = (rhs: string): string[] => {
  const withoutMappedKeys = rhs.replace(/\[\s*\w+\s+in\s+keyof\s+\w+\s*\]\s*:/g, " ");
  return matchCaptures(withoutMappedKeys, /([A-Za-z_]\w*)\s*\??:/g);
};

const referencedTypeNames = (rhs: string): string[] =>
  matchCaptures(rhs, /\b(Pi[A-Z]\w*)/g);

const capitalise = (tool: string): string =>
  tool.charAt(0).toUpperCase() + tool.slice(1);

// Property name -> every core tool whose argument or options bag accepts it.
// Walking starts from each tool's argument/compatibility/options aliases and
// follows Pi* references (shared bags, edit operations) transitively.
const collectCoreToolProperties = (declarations: string): Map<string, CoreToolName[]> => {
  const typeDeclarations = extractTypeDeclarations(declarations);
  const owners = new Map<string, Set<CoreToolName>>();
  for (const tool of CORE_TOOL_NAMES) {
    const roots = [
      `Pi${capitalise(tool)}Argument`,
      PI_CORE_COMPATIBILITY_ARGUMENT_TYPE_NAMES[tool],
      `Pi${capitalise(tool)}Options`,
    ];
    const visited = new Set<string>();
    const walk = (name: string): void => {
      if (visited.has(name)) return;
      visited.add(name);
      const rhs = typeDeclarations.get(name);
      if (rhs === undefined) return;
      for (const property of objectLiteralKeys(rhs)) {
        const toolSet = owners.get(property) ?? new Set<CoreToolName>();
        toolSet.add(tool);
        owners.set(property, toolSet);
      }
      for (const reference of referencedTypeNames(rhs)) walk(reference);
    };
    for (const root of roots) walk(root);
  }
  return new Map([...owners].map(([property, toolSet]) => [property, [...toolSet]]));
};

// Memoized at first use: parsing the ~60 KB declarations text at module load
// cost startup time on every Pi launch, and the map is needed only after a
// type check fails.
let coreToolProperties_: ReadonlyMap<string, readonly CoreToolName[]> | undefined;
export const coreToolProperties = (): ReadonlyMap<string, readonly CoreToolName[]> =>
  (coreToolProperties_ ??= collectCoreToolProperties(GUEST_TYPE_DECLARATIONS));
