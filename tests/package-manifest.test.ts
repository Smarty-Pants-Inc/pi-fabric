import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

interface PackageManifest {
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
}

const packageName = (specifier: string): string =>
  specifier.startsWith("@")
    ? specifier.split("/").slice(0, 2).join("/")
    : (specifier.split("/")[0] ?? specifier);

describe("package manifest", () => {
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
    // Type-only imports are erased from the emitted worker.
    const imports = [...worker.matchAll(/\b(?:import|export)\s+(type\s+)?[^;"']*?\bfrom\s+["']([^"']+)["']/g)]
      .filter((match) => !match[1])
      .map((match) => match[2])
      .filter((specifier): specifier is string =>
        Boolean(specifier && !specifier.startsWith(".") && !specifier.startsWith("node:")),
      )
      .map(packageName);

    for (const dependency of new Set(imports)) {
      expect(
        manifest.dependencies?.[dependency],
        `${dependency} is imported by the standalone worker but is not installed at runtime`,
      ).toBeDefined();
    }
  });
});
