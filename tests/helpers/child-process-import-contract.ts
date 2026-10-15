import fs from "node:fs";
import { join } from "node:path";
import ts from "typescript";

// Only the preload and the two guarded Bun regression harnesses. Vitest runs the
// other tests in Node, where syncBuiltinESMExports works; do not rewrite src/ or
// the whole test corpus to accommodate Bun's builtin-export limitation.
export const guardedBunFiles = [
  "tests/test-deletion-guard.mjs",
  "tests/test-deletion-guard-bun.test.ts",
  "tests/test-deletion-guard-child-process.test.ts",
];

export function namedChildProcessImportsInSource(file: string, source: string): string[] {
  const findings: string[] = [];
  const ast = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true);
  for (const node of ast.statements) {
    if (!ts.isImportDeclaration(node) || !ts.isStringLiteralLike(node.moduleSpecifier)
      || !["node:child_process", "child_process"].includes(node.moduleSpecifier.text)) continue;
    const clause = node.importClause, bindings = clause?.namedBindings;
    if (clause?.isTypeOnly || !bindings || !ts.isNamedImports(bindings)) continue;
    const symbols = bindings.elements.filter(element => !element.isTypeOnly)
      .map(element => (element.propertyName ?? element.name).text);
    if (symbols.length) {
      const line = ast.getLineAndCharacterOfPosition(node.getStart()).line + 1;
      findings.push(`${file}:${line}: ${symbols.join(", ")} from ${node.moduleSpecifier.text}`);
    }
  }
  return findings;
}

// Parse declarations, not comments or strings. Inline Bun programs are checked
// separately on their actual source immediately before execution by the suites.
export function namedChildProcessImports(root: string, files = guardedBunFiles): string[] {
  return files.flatMap(file => namedChildProcessImportsInSource(file,
    fs.readFileSync(join(root, file), "utf8"))).sort();
}
