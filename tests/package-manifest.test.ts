import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import ts from "typescript";

interface PackageManifest {
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
}

const packageName = (specifier: string): string =>
  specifier.startsWith("@")
    ? specifier.split("/").slice(0, 2).join("/")
    : (specifier.split("/")[0] ?? specifier);

const runtimePackages = (source: string): string[] => {
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext, verbatimModuleSyntax: true },
  });
  const emitted = ts.createSourceFile("worker.js", outputText, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
  const packages = new Set<string>();
  const visit = (node: ts.Node): void => {
    const specifier = ts.isImportDeclaration(node) || ts.isExportDeclaration(node)
      ? node.moduleSpecifier
      : ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword
        ? node.arguments[0]
        : undefined;
    if (specifier && ts.isStringLiteralLike(specifier) &&
      !specifier.text.startsWith(".") && !specifier.text.startsWith("node:")) {
      packages.add(packageName(specifier.text));
    }
    ts.forEachChild(node, visit);
  };
  visit(emitted);
  return [...packages];
};

describe("package manifest", () => {
  it("excludes erased type imports rather than requiring host peers at runtime", () => {
    expect(runtimePackages(`
      import type { ImageContent } from "@earendil-works/pi-ai";
      type Spawn = typeof import("type-only-package");
      const misleading = "from 'not-an-import'";
    `)).toEqual([]);
  });

  it("checks static, side-effect, re-export, and dynamic runtime dependencies", () => {
    expect(runtimePackages(`
      import spawn from "cross-spawn";
      import "side-effect-package";
      export { value } from "@example/package/subpath";
      await import("dynamic-package");
      await import("cross-spawn");
      import fs from "node:fs";
      await import("./worker/options.js");
    `)).toEqual(["cross-spawn", "side-effect-package", "@example/package", "dynamic-package"]);
  });
  it("declares Shiki's lazily loaded language package", () => {
    const root = path.resolve(import.meta.dirname, "..");
    const manifest = JSON.parse(
      fs.readFileSync(path.join(root, "package.json"), "utf8"),
    ) as PackageManifest;

    expect(
      manifest.dependencies?.["@shikijs/langs"],
      "Shiki loads bundled languages through this package at runtime",
    ).toBeDefined();
  });

  it("declares Pi's host-provided packages only as \"*\" peers", () => {
    const root = path.resolve(import.meta.dirname, "..");
    const manifest = JSON.parse(
      fs.readFileSync(path.join(root, "package.json"), "utf8"),
    ) as PackageManifest;
    // Pi warns at every launch when these are dependencies: an installed copy
    // bypasses its extension aliases. scripts/build.mjs bundles the ones the
    // standalone worker entries use, because Pi installs without peers.
    for (const name of ["@earendil-works/pi-agent-core", "@earendil-works/pi-ai", "@earendil-works/pi-coding-agent", "@earendil-works/pi-tui", "typebox"]) {
      expect(manifest.dependencies?.[name], `${name} must not be a dependency`).toBeUndefined();
    }
    expect(manifest.peerDependencies).toEqual({
      "@earendil-works/pi-ai": "*",
      "@earendil-works/pi-coding-agent": "*",
      "@earendil-works/pi-tui": "*",
      typebox: "*",
    });
  });

  it("installs every standalone worker import as a runtime dependency", () => {
    const root = path.resolve(import.meta.dirname, "..");
    const manifest = JSON.parse(
      fs.readFileSync(path.join(root, "package.json"), "utf8"),
    ) as PackageManifest;
    const worker = fs.readFileSync(path.join(root, "src", "worker.ts"), "utf8");
<<<<<<< HEAD
    // Type-only imports are erased from the emitted worker.
    const imports = [...worker.matchAll(/\b(?:import|export)\s+(type\s+)?[^;"']*?\bfrom\s+["']([^"']+)["']/g)]
      .filter((match) => !match[1])
      .map((match) => match[2])
      .filter((specifier): specifier is string =>
        Boolean(specifier && !specifier.startsWith(".") && !specifier.startsWith("node:")),
      )
      .map(packageName);

    for (const dependency of new Set(imports)) {
=======
    for (const dependency of runtimePackages(worker)) {
>>>>>>> upstream-v0.105.0
      expect(
        manifest.dependencies?.[dependency],
        `${dependency} is imported by the standalone worker but is not installed at runtime`,
      ).toBeDefined();
    }
  });
});
