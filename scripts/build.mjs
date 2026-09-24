#!/usr/bin/env node
import { build } from "esbuild";
import { copyFileSync, mkdirSync, readFileSync } from "node:fs";

const primaryEntryPoints = [
  "src/index.ts",
  "src/memory.ts",
  "src/mcp.ts",
  "src/agents.ts",
  "src/jev.ts",
  "src/protocol.ts",
  "src/residency/host.ts",
  "src/residency/launcher.ts",
  "src/residency/pi-entry.ts",
  "src/residency/actor-client.ts",
  "src/compaction/hook.ts",
  "src/core/action-registry.ts",
  "src/entropy/index.ts",
  "src/memory/digest.ts",
  "src/memory/search.ts",
  "src/memory/discovery.ts",
  "src/memory/normalize.ts",
  "src/memory/worker-provider.ts",
  "src/providers/memory-provider.ts",
];

// Every package-local dynamic import is also an entry point. Its stable output
// path lets a session that loaded the previous index resolve delayed modules
// after the installed package is replaced, while preserving lazy evaluation.
const lazyEntryPoints = [
  "src/core/provider-operations.ts",
  "src/agents/claude-cli.ts",
  "src/agents/compact-control.ts",
  "src/agents/result.ts",
  "src/agents/veda-cli.ts",
  "src/fabric-runtime-state.ts",
  "src/components/configuration.ts",
  "src/providers/jev-provider.ts",
  "src/jev/client.ts",
  "src/jev/observation.ts",
  "src/runtime/core-override-guest-types.ts",
  "src/runtime/dynamic-guest-types.ts",
  "src/runtime/guest-types.ts",
  "src/runtime/node-process-runtime.ts",
  "src/runtime/typescript-kernel.ts",
  "src/runtime/cpython-runtime.ts",
  "src/runtime/monty-runtime.ts",
  "src/runtime/quickjs-runtime.ts",
  "src/runtime/type-checker.ts",
  "src/speculation/scanner.ts",
  "src/speculation/python-scanner.ts",
  "src/ui/dashboard.ts",
  "src/ui/conversation.ts",
  "src/ui/conversation-host.ts",
  "src/ui/conversation-targets.ts",
  "src/ui/conversation-chrome.ts",
  "src/ui/conversation-native-reader.ts",
  "src/ui/model-picker.ts",
  "src/ui/settings.ts",
  "src/worker/event-projection.ts",
  "src/worker/activation-window.ts",
  "src/worker/model-control.ts",
  "src/worker/options.ts",
  "src/worker/run-record.ts",
  "src/worker/session-export.ts",
];

const result = await build({
  entryPoints: [...primaryEntryPoints, ...lazyEntryPoints],
  outdir: "dist",
  outbase: "src",
  entryNames: "[dir]/[name]",
  chunkNames: "chunks/[name]-[hash]",
  bundle: true,
  packages: "external",
  platform: "node",
  format: "esm",
  target: "node24",
  splitting: true,
  sourcemap: true,
  metafile: true,
  logLevel: "info",
});

// Pi supplies these packages to extensions through its module aliases, so
// they are peers and must never be bundled into code that Pi loads.
const hostProvided = /^(?:typebox|@sinclair\/typebox|@(?:earendil-works|mariozechner)\/pi-[a-z-]+)(?:\/|$)/;

// Fabric starts these as their own Node process or worker thread. Pi's aliases
// do not reach them, and Pi installs packages without peers, so each one is a
// self-contained file that carries its own copy of the host packages it uses.
// ponytail: typebox (agent-result schema checks) is their only host import
// today; scripts/smoke-package-install.mjs fails if one gains another.
const standalone = await build({
  entryPoints: ["src/worker.ts", "src/memory/file-worker.ts"],
  outdir: "dist",
  outbase: "src",
  entryNames: "[dir]/[name]",
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node24",
  sourcemap: true,
  metafile: true,
  logLevel: "info",
  banner: { js: "// Bundles TypeBox (MIT, Copyright (c) 2017-2026 Haydn Paterson); see THIRD_PARTY_NOTICES.md." },
  plugins: [{
    name: "external-except-host-provided",
    setup(pluginBuild) {
      pluginBuild.onResolve({ filter: /^[^./]/ }, (args) =>
        args.kind === "entry-point" || hostProvided.test(args.path)
          ? undefined
          : { path: args.path, external: true });
    },
  }],
});

// tsc does not copy input .d.ts files; ship the generated kernel ABI and receipt.
mkdirSync("dist/verified/generated", { recursive: true });
const receiptPath = "src/verified/generated/manifest.json";
const receipt = JSON.parse(readFileSync(receiptPath, "utf8"));
for (const source of Object.keys(receipt.outputs)) {
  if (!/^src\/verified\/generated\/[a-z-]+\.(?:js|d\.ts)$/.test(source)) {
    throw new Error(`Unexpected verified artifact path: ${source}`);
  }
  copyFileSync(source, source.replace(/^src\//, "dist/"));
}
copyFileSync(receiptPath, "dist/verified/generated/manifest.json");

const bundledPackages = [
  ...Object.keys(result.metafile.inputs).filter((input) => input.includes("node_modules/")),
  ...Object.keys(standalone.metafile.inputs).filter((input) =>
    input.includes("node_modules/") && !input.includes("node_modules/typebox/")),
];
if (bundledPackages.length > 0) {
  throw new Error(`Package code was bundled unexpectedly:\n${bundledPackages.join("\n")}`);
}

const unstableLazyImports = Object.entries(result.metafile.outputs).flatMap(
  ([output, metadata]) =>
    metadata.imports
      .filter(
        (entry) =>
          entry.kind === "dynamic-import" &&
          !entry.external &&
          entry.path.includes("/chunks/"),
      )
      .map((entry) => `${output} -> ${entry.path}`),
);
if (unstableLazyImports.length > 0) {
  throw new Error(
    `Package-local dynamic imports must use stable entry paths:\n${unstableLazyImports.join("\n")}`,
  );
}
