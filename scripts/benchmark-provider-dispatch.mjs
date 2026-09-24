#!/usr/bin/env node
import { performance } from "node:perf_hooks";
import { ActionRegistry } from "../dist/core/action-registry.js";
const iterations = 1000;
const context = { cwd: process.cwd(), signal: undefined, parentToolCallId: "bench", nestedToolCallId: "bench", extensionContext: {}, update() {}, approve: async () => {}, audits: [], maxResultChars: 1000 };
const descriptor = { name: "read", description: "Read", inputSchema: { type: "object" }, risk: "read", effect: { kind: "none", ordering: "commutative", resources: ["bench"] } };
const provider = { name: "bench", description: "Dispatch benchmark", async list() { return [descriptor]; }, async describe() { return descriptor; }, async invoke(_name, args) { return args.value; } };
const registry = new ActionRegistry(); registry.register(provider);
const call = () => registry.invoke("bench.read", { value: 42 }, { ...context, audits: [] });
const start = performance.now(); await call(); const firstUseMs = performance.now() - start;
for (let i = 0; i < 100; i++) await call();
const samples = [];
for (let round = 0; round < 5; round++) {
  const before = performance.now(); for (let i = 0; i < iterations; i++) await call();
  samples.push((performance.now() - before) / iterations);
}
await registry.close();
console.log(JSON.stringify({ node: process.version, iterations, firstUseMs, medianMsPerCall: [...samples].sort((a, b) => a - b)[2], samplesMsPerCall: samples, scope: "full proved registry read dispatch, no provider I/O; not a before/after provider-implementation benchmark" }, null, 2));
