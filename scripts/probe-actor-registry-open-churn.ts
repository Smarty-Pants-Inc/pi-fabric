#!/usr/bin/env bun
// smarty-dev#7791 / #8526: registry opens on an UNCHANGED actors.json from the hot read path (list(),
// cleanupObligation(), owns() checks all reach ActorRegistryStore.read()). Measured on Ryzen 1: 7,392 opens/min
// of a 33-byte registry with 0 writes. This probe reads a settled registry in a tight loop for --duration ms
// and reports reads/s, registry opens/s and JSON.parse calls, for a small and a 3 MiB registry.
//   bun scripts/probe-actor-registry-open-churn.ts --duration=3000
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, "").split("=")));
const DURATION = Number(args.duration ?? 3_000);

const probe = (label: string, actors: unknown[]) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-open-churn-probe-"));
  const file = path.join(root, "actors.json");
  fs.writeFileSync(file, JSON.stringify({ format: 1, actors }));
  const old = new Date(Date.now() - 5_000);
  fs.utimesSync(file, old, old);
  const store = new ActorRegistryStore(root);
  let opens = 0, parses = 0;
  const open = fs.openSync, parse = JSON.parse;
  (fs as { openSync: typeof fs.openSync }).openSync = ((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target) === file) opens += 1;
    return Reflect.apply(open, fs, [target, ...rest]);
  }) as typeof fs.openSync;
  JSON.parse = ((text: string, reviver?: Parameters<typeof JSON.parse>[1]) => { parses += 1; return parse(text, reviver); }) as typeof JSON.parse;
  let reads = 0;
  const started = performance.now();
  try {
    while (performance.now() - started < DURATION) { store.read(); reads += 1; }
  } finally {
    (fs as { openSync: typeof fs.openSync }).openSync = open;
    JSON.parse = parse;
    store.releaseReadCache();
    fs.rmSync(root, { recursive: true, force: true });
  }
  const seconds = (performance.now() - started) / 1_000;
  return { registry: label, bytes: JSON.stringify({ format: 1, actors }).length, readsPerSecond: Math.round(reads / seconds),
    opensPerSecond: Math.round(opens / seconds), opensPerRead: Number((opens / reads).toFixed(4)), parses };
};

const big = Array.from({ length: 300 }, (_, i) => ({ id: i.toString(16).padStart(32, "0"), name: `actor-${i}`, instructions: "x".repeat(10_000) }));
console.log(JSON.stringify([probe("empty (33-byte shape)", []), probe("3 MiB", big)], null, 2));
