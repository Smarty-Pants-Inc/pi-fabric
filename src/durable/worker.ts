#!/usr/bin/env node
import { spawn } from "node:child_process";
import fs from "node:fs";
import module from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseDurableWorkerOptions } from "./worker-options.js";

try {
  // This process-only entry is never imported by extension registration.
  const options = parseDurableWorkerOptions(process.argv.slice(2));
  if (process.versions.bun) {
    // Bun cannot install Node's peer-resolution hooks. Keep the pinned SDK in
    // the package's required Node 24+ runtime; never inject ambient NODE_PATH.
    const child = spawn("node", [fileURLToPath(import.meta.url), ...process.argv.slice(2)], { stdio: "inherit" });
    const code = await new Promise<number>((resolve, reject) => {
      child.once("error", error => reject(new Error(`Bun-launched durable workers require Node.js 24+ on PATH: ${error.message}`, { cause: error })));
      child.once("exit", (code, signal) => resolve(code ?? (signal ? 1 : 0)));
    });
    process.exitCode = code;
  } else {
    if (Number.parseInt(process.versions.node, 10) < 24) throw new Error("Durable Pi workers require Node.js 24+");
    // Managed Pi installs omit host peers. A subprocess cannot use Main's virtual
    // modules, so it owns a pinned SDK under a non-peer name. Resolve all Pi peers
    // through that SDK's dependency tree (also with non-hoisting installers), not
    // an ambient checkout or another extension's SDK. This hook is process-local
    // and must be installed before importing any durable host/engine modules.
    const sdkEntry = import.meta.resolve("pi-fabric-worker-sdk");
    const sdkName = "@earendil-works/pi-coding-agent";
    const peers = ["@earendil-works/pi-agent-core", "@earendil-works/pi-ai", "@earendil-works/pi-tui", "typebox"];
    module.registerHooks({
      resolve(specifier, context, nextResolve) {
        if (specifier === sdkName || specifier.startsWith(`${sdkName}/`)) {
          return nextResolve(`pi-fabric-worker-sdk${specifier.slice(sdkName.length)}`, { ...context, parentURL: import.meta.url });
        }
        if (peers.some(name => specifier === name || specifier.startsWith(`${name}/`))) {
          return nextResolve(specifier, { ...context, parentURL: sdkEntry });
        }
        return nextResolve(specifier, context);
      },
    });
    // Fabric's tool capture patches the ExtensionRunner class of the package named by
    // PI_PACKAGE_DIR. Name the pinned SDK, or capture patches the host copy the durable
    // runtime never instantiates and mutating pi.* tools stay fenced (no tool hooks).
    for (let directory = path.dirname(fileURLToPath(sdkEntry)); directory !== path.dirname(directory); directory = path.dirname(directory)) {
      const manifest = path.join(directory, "package.json");
      if (!fs.existsSync(manifest)) continue;
      try {
        if ((JSON.parse(fs.readFileSync(manifest, "utf8")) as { name?: unknown }).name === sdkName) {
          process.env.PI_PACKAGE_DIR = directory;
          break;
        }
      } catch { /* unreadable manifest: keep searching */ }
    }
    const { runDurableWorker } = await import("./worker-host.js");
    await runDurableWorker(options);
  }
} catch (error) {
  process.stderr.write(`Durable Pi worker failed: ${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
}
