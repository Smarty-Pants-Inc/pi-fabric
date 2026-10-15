import fs from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { expect, it } from "vitest";

// smarty-dev#13001: Bun cannot update named builtin exports after the guard patches fs.
// Keep this inventory in step with the guarded calls; parse imports, not comments or probe strings.
const modules = new Set(["fs", "node:fs", "fs/promises", "node:fs/promises"]);
const deletion = new Set(["rm", "rmSync", "unlink", "unlinkSync", "rmdir", "rmdirSync"]);
const extensions = /\.(?:[cm]?[jt]s|[jt]sx)$/;

// ONE exact exception: scripts/verified-kernels.mjs, the declaration below.
// hashed proof-manifest input (proof:artifact). Its rmSync deletes its own mkdtemp directory and, only in --write mode (bun run proof:generate, a developer step, never a test), retired outputs matching ^src/verified/generated/[a-z-]+\.(?:js|d\.ts)$ inside the repository. Convert it at the next trusted Bend 2.0.36 regeneration (smarty-dev#13001).
const proofManifestImport = 'import { mkdtempSync, readFileSync, writeFileSync, mkdirSync, rmSync, existsSync } from "node:fs";';

it("calls deletion functions through the fs namespace so the guard sees them (smarty-dev#13001)", () => {
  const root = fileURLToPath(new URL("../", import.meta.url));
  const findings: string[] = [];
  let exceptions = 0;
  const walk = (directory: string): void => {
    for (const entry of fs.readdirSync(join(root, directory), { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const file = join(directory, entry.name);
      if (entry.isDirectory()) walk(file);
      else if (extensions.test(entry.name)) {
        const source = fs.readFileSync(join(root, file), "utf8");
        const ast = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true);
        for (const node of ast.statements) {
          if (!ts.isImportDeclaration(node) || !ts.isStringLiteralLike(node.moduleSpecifier)
            || !modules.has(node.moduleSpecifier.text)) continue;
          const clause = node.importClause;
          const bindings = clause?.namedBindings;
          if (!bindings || !ts.isNamedImports(bindings) || clause?.isTypeOnly) continue;
          const symbols = bindings.elements.filter((element) => !element.isTypeOnly
            && deletion.has((element.propertyName ?? element.name).text))
            .map((element) => (element.propertyName ?? element.name).text);
          if (symbols.length) {
            if (file === join("scripts", "verified-kernels.mjs") && node.getText(ast) === proofManifestImport) {
              exceptions++;
              continue;
            }
            const line = ast.getLineAndCharacterOfPosition(node.getStart()).line + 1;
            findings.push(`${file}:${line}: ${symbols.join(", ")} from ${node.moduleSpecifier.text}`);
          }
        }
      }
    }
  };
  ["src", "scripts", "tests"].forEach(walk);
  expect(exceptions, "the one exact hashed proof-manifest declaration must remain unchanged").toBe(1);
  expect(findings, `smarty-dev#13001: ${findings.length} named deletion import declarations. Call the function through the fs namespace so the guard sees it.\n${findings.join("\n")}`).toEqual([]);
});
