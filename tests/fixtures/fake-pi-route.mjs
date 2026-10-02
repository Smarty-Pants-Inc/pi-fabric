#!/usr/bin/env node
// Fake transport peer only: real-worker tests inspect launch argv and inherited attribution.
process.stdout.write(JSON.stringify({ type: "fake_route_launch", argv: process.argv.slice(2),
  header: process.env.PI_FABRIC_ROUTE_HEADER ?? null }) + "\n");
await import("./fake-pi-model.mjs");
