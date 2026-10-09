#!/usr/bin/env node
import { build } from "esbuild";
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { buildLandlock } from "./build-landlock.mjs";

const primaryEntryPoints = [
  "src/index.ts",
  "src/memory.ts",
  "src/mesh.ts",
  "src/mesh-bridge.ts",
  "src/participants-cli.ts",
  "src/actors-cli.ts",
  "src/judge-cli.ts",
  "src/releases-cli.ts",
  "src/mesh-lock-stats-cli.ts",
  "src/mcp.ts",
  "src/agents.ts",
  "src/agents/worker-protocol.ts",
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
  "src/records/service-main.ts",
];

// Every package-local dynamic import is also an entry point. Its stable output
// path lets a session that loaded the previous index resolve delayed modules
// after the installed package is replaced, while preserving lazy evaluation.
const lazyEntryPoints = [
  "src/residency/launcher-owner.ts",
  "src/mesh/state-projector.ts",
  "src/mesh/state-async.ts",
  "src/providers/mesh-provider.ts",
  "src/judge/agent.ts",
  "src/core/landlock.ts",
  "src/core/pattern-kill.ts",
  "src/lifecycle/reload-target-profile.ts",
  "src/lifecycle/reload-slots.ts",
  "src/coordination/unverified-ids.ts",
  "src/core/provider-operations.ts",
  "src/guards/foreground-wait.ts",
  "src/agents/model-route.ts",
  "src/agents/model-route-prepare.ts",
  "src/agents/claude-cli.ts",
  "src/agents/compact-control.ts",
  "src/agents/result.ts",
  "src/agents/veda-cli.ts",
  "src/agents/transports/placement.ts",
  "src/fabric-runtime-state.ts",
  "src/components/configuration.ts",
  "src/providers/jev-provider.ts",
  "src/records/service.ts",
  "src/jev/client.ts",
  "src/jev/routes.ts",
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
  "src/type-error-guidance.ts",
  "src/speculation/scanner.ts",
  "src/speculation/python-scanner.ts",
  "src/ui/dashboard.ts",
  "src/ui/shell-tasks.ts",
  "src/ui/languages/bend.ts",
  "src/ui/conversation.ts",
  "src/ui/conversation-host.ts",
  "src/ui/conversation-targets.ts",
  "src/ui/conversation-chrome.ts",
  "src/ui/conversation-native-reader.ts",
  "src/ui/model-picker.ts",
  "src/ui/settings.ts",
  "src/worker/event-projection.ts",
  "src/worker/activation-window.ts",
  "src/worker/activation-compaction.ts",
  "src/worker/reply-tool.ts",
  "src/worker/principal-delivery.ts",
  "src/worker/session-id.ts",
  "src/worker/model-control.ts",
  "src/worker/context-admission.ts",
  "src/worker/context-reseed.ts",
  "src/worker/options.ts",
  "src/worker/recovery-watchdog.ts",
  "src/worker/retry-profile.ts",
  "src/worker/task-entry.ts",
  "src/worker/release-entry.ts",
  "src/worker/run-log.ts",
  "src/worker/run-record.ts",
  "src/worker/session-export.ts",
];

buildLandlock();

const result = await build({
  entryPoints: [...primaryEntryPoints, ...lazyEntryPoints],
  // Both facades only re-export host metadata. Resolve to their implementation
  // so empty facade-only chunks do not consume startup graph slots.
  plugins: [{
    name: "host-metadata-facades",
    setup(pluginBuild) {
      pluginBuild.onResolve({ filter: /\/(?:model-policy|fabric-provenance)\.js$/ }, () => ({
        path: resolve("src/host-compatibility.ts"),
      }));
    },
  }],
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

// Advertise the exact manager/worker contract without importing candidate code at spawn.
const { WORKER_PROTOCOL_VERSION } = await import("../dist/agents/worker-protocol.js");
writeFileSync("dist/worker-protocol.json", `${JSON.stringify({ version: WORKER_PROTOCOL_VERSION })}\n`);

// Pi supplies these packages to extensions through its module aliases, so
// they are peers and must never be bundled into code that Pi loads.
const hostProvided = /^(?:typebox|@sinclair\/typebox|@(?:earendil-works|mariozechner)\/pi-[a-z-]+)(?:\/|$)/;

// Fabric starts these as their own Node process or worker thread. Pi's aliases
// do not reach them, and Pi installs packages without peers, so each one is a
// self-contained file that carries its own copy of the host packages it uses.
// ponytail: typebox (agent-result schema checks) is their only host import
// today; scripts/smoke-package-install.mjs fails if one gains another.
// fabric-mesh-backend (smarty-dev#6477 W1) is here too: its writer census shares host-leases.ts with
// index.js, and a split build would carve that out of index.js into one more startup chunk.
const standalone = await build({
  entryPoints: ["src/worker.ts", "src/memory/file-worker.ts", "src/storage/sweep-main.ts", "src/storage/retention-cli.ts",
    "src/mesh/mesh-backend-cli.ts"],
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

// smarty-dev#2184: the worker loads this timeout-only hook into every Pi actor run, native-tool
// ones included. Built on its own, without splitting, so it shares no chunk with index.js.
await build({
  entryPoints: ["src/guards/actor-bash-hook.ts", "src/guards/model-route-hook.ts"],
  outdir: "dist",
  outbase: "src",
  entryNames: "[dir]/[name]",
  bundle: true,
  packages: "external",
  platform: "node",
  format: "esm",
  target: "node24",
  sourcemap: true,
  logLevel: "info",
});

// The records service runs as its own OS user from a root-owned copy of one file:
// inline every package (pg included) so no module resolves outside that copy.
await build({
  entryPoints: ["src/records/service-main.ts"],
  outfile: "dist/records-service/service-main.mjs",
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node24",
  logLevel: "info",
  banner: { js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);" },
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
