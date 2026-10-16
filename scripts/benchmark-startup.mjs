#!/usr/bin/env node
// Fresh-process module compilation with Pi's bundled-host jiti settings.
// Host startup is intentionally outside the timer; fs/transpile caches stay warm.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const self = fileURLToPath(import.meta.url);
if (process.argv[2] === "--child") {
  const root = resolve(process.argv[3]);
  const manifest = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
  const hostEntry = fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent"));
  const hostRequire = createRequire(hostEntry);
  const jitiManifestPath = hostRequire.resolve("jiti/package.json");
  const jitiManifest = JSON.parse(readFileSync(jitiManifestPath, "utf8"));
  const { createJiti } = await import(pathToFileURL(resolve(dirname(jitiManifestPath), jitiManifest.exports["./static"].import)).href);
  const resolveHost = createJiti(hostEntry);
  const virtualModules = {};
  for (const specifier of ["typebox", "typebox/compile", "typebox/value", "@earendil-works/pi-agent-core", "@earendil-works/pi-tui", "@earendil-works/pi-ai/compat", "@earendil-works/pi-ai/oauth", "@earendil-works/pi-ai/providers/all", "@earendil-works/pi-coding-agent"]) {
    virtualModules[specifier] = await import(resolveHost.esmResolve(specifier));
  }
  virtualModules["@earendil-works/pi-ai"] = virtualModules["@earendil-works/pi-ai/compat"];
  const { loadExtensionFromFactory, createExtensionRuntime } = await import(pathToFileURL(resolve(dirname(hostEntry), "core/extensions/loader.js")).href);
  const { createEventBus } = await import(pathToFileURL(resolve(dirname(hostEntry), "core/event-bus.js")).href);
  const jiti = createJiti(import.meta.url, { moduleCache: false, tryNative: false, virtualModules });
  // Every manifest entry, in order, sharing one bus and runtime as Pi loads them
  // (bundled Pi packages such as ./node_modules/pi-coralbricks-provider included).
  const eventBus = createEventBus();
  const runtime = createExtensionRuntime();
  const entries = [];
  const cpuStart = process.cpuUsage();
  for (const relative of manifest.pi.extensions) {
    const entry = resolve(root, relative);
    const entryCpu = process.cpuUsage();
    const start = performance.now();
    const factory = await jiti.import(entry, { default: true });
    const imported = performance.now();
    const extension = await loadExtensionFromFactory(factory, root, eventBus, runtime, entry);
    const registered = performance.now();
    const cpu = process.cpuUsage(entryCpu);
    entries.push({ entry: relative, importMs: imported - start, factoryMs: registered - imported, totalMs: registered - start, cpuMs: (cpu.user + cpu.system) / 1000, tools: [...extension.tools.keys()], commands: [...extension.commands.keys()] });
  }
  const cpu = process.cpuUsage(cpuStart);
  const sum = key => entries.reduce((total, entry) => total + entry[key], 0);
  console.log(JSON.stringify({ name: manifest.name, entry: manifest.pi.extensions.join(" + "), importMs: sum("importMs"), factoryMs: sum("factoryMs"), totalMs: sum("totalMs"), cpuMs: (cpu.user + cpu.system) / 1000, tools: entries.flatMap(e => e.tools), commands: entries.flatMap(e => e.commands), entries }));
} else {
  const cases = (process.argv.slice(2).length ? process.argv.slice(2) : ["."]).map(root => ({ root, samples: [] }));
  // Interleave and reverse order each round to reduce load/order bias.
  for (let i = 0; i < 4; i++) {
    for (const { root, samples } of i % 2 ? [...cases].reverse() : cases) {
      const child = spawnSync(process.execPath, [self, "--child", resolve(root)], { encoding: "utf8", timeout: 30_000, env: { ...process.env, PI_OFFLINE: "1", CONTOUR_BACKGROUND: "0" } });
      if (child.status !== 0) throw new Error(child.stderr || child.error?.message || child.stdout);
      const sample = JSON.parse(child.stdout.trim().split("\n").at(-1));
      if (i > 0) samples.push(sample);
    }
  }
  const reports = [];
  for (const { root, samples } of cases) {
    const middle = key => [...samples].sort((a, b) => a[key] - b[key])[1][key];
    const perEntry = samples[0].entries.map(({ entry }, index) => {
      const middleOf = key => [...samples].map(s => s.entries[index][key]).sort((a, b) => a - b)[1];
      return { entry, importMs: +middleOf("importMs").toFixed(1), factoryMs: +middleOf("factoryMs").toFixed(1), totalMs: +middleOf("totalMs").toFixed(1), cpuMs: +middleOf("cpuMs").toFixed(1) };
    });
    reports.push({ root: resolve(root), name: samples[0].name, entry: samples[0].entry, importMs: +middle("importMs").toFixed(1), factoryMs: +middle("factoryMs").toFixed(1), totalMs: +middle("totalMs").toFixed(1), cpuMs: +middle("cpuMs").toFixed(1), samplesMs: samples.map(s => +s.totalMs.toFixed(1)), tools: samples[0].tools, commands: samples[0].commands, entries: perEntry });
  }
  console.log(JSON.stringify({ mode: "fresh process, warm jiti filesystem cache, Pi-style jiti options (native ESM may bypass host aliases), median of 3", reports }, null, 2));
}
