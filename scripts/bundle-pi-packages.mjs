#!/usr/bin/env node
// Prepare bundledDependencies for packing. npm packs a symlinked bundled
// package, but `bun pm pack` silently skips one, and Bun's isolated linker
// installs every package as a symlink into node_modules/.bun. Replace each
// bundled symlink with a real copy of the package so either tool packs it, and
// prune physical Pi host packages a bundled package would otherwise ship: Pi
// supplies them, and a copy would bypass its extension module mapping.
import { cpSync, existsSync, lstatSync, readdirSync, readFileSync, realpathSync, renameSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(process.argv[2] ?? join(dirname(fileURLToPath(import.meta.url)), ".."));
const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const hostPackages = ["@earendil-works/pi-ai", "@earendil-works/pi-agent-core", "@earendil-works/pi-coding-agent", "@earendil-works/pi-tui", "typebox"];

for (const name of manifest.bundledDependencies ?? []) {
  const target = join(root, "node_modules", name);
  if (!existsSync(target)) throw new Error(`Bundled Pi package is not installed: ${name}`);
  if (lstatSync(target).isSymbolicLink()) {
    const source = realpathSync(target);
    const staging = `${target}.bundling`;
    rmSync(staging, { recursive: true, force: true });
    // The package's own files only: its dependencies resolve beside it in the
    // installed tree, and Pi supplies host packages to extensions.
    cpSync(source, staging, { recursive: true, filter: (file) => !file.slice(source.length).split(/[\\/]/).includes("node_modules") });
    rmSync(target, { force: true });
    renameSync(staging, target);
    console.log(`materialized bundled package ${name}`);
  }
  const nested = join(target, "node_modules");
  const scopes = existsSync(nested) ? readdirSync(nested).flatMap((entry) =>
    entry.startsWith("@") ? readdirSync(join(nested, entry)).map((child) => `${entry}/${child}`) : [entry]) : [];
  for (const host of scopes.filter((entry) => hostPackages.includes(entry))) {
    rmSync(join(nested, host), { recursive: true, force: true });
    console.log(`pruned host package ${host} from bundled package ${name}`);
  }
}
