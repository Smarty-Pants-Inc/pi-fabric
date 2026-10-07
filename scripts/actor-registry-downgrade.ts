#!/usr/bin/env bun
import path from "node:path";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const [root, ...extra] = process.argv.slice(2);
if (!root || extra.length) {
  console.error("Usage: bun scripts/actor-registry-downgrade.ts <actor-root>\nStop all writers for this root before running; archives are retained.");
  process.exitCode = 2;
} else {
  const store = new ActorRegistryStore(path.resolve(root));
  const count = await store.restoreInlineForDowngrade();
  console.log(`Restored ${count} inline actor records. Sidecar archives were retained.`);
}
