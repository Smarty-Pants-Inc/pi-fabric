#!/usr/bin/env node
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { ActionRegistry } from "../dist/core/action-registry.js";
import { compileFabricSummary } from "../dist/compaction/hook.js";
import * as kernel from "../dist/verified/generated/kernel.js";
import * as authority from "../dist/verified/generated/authority-kernel.js";
import { bindingStep } from "../dist/verified/generated/lifecycle-kernel.js";
import { storageTransition } from "../dist/verified/generated/storage-kernel.js";

// The retired PoC must not remain reachable through package exports.
await assert.rejects(import("pi-fabric/verified/cell-provider"), { code: "ERR_PACKAGE_PATH_NOT_EXPORTED" });
await assert.rejects(import("pi-fabric/verified/goal-provider"), { code: "ERR_PACKAGE_PATH_NOT_EXPORTED" });
const root = new URL("../", import.meta.url);
for (const file of ["kernel.js", "kernel.d.ts", "provider-kernel.js", "provider-kernel.d.ts", "lifecycle-kernel.js", "lifecycle-kernel.d.ts", "authority-kernel.js", "authority-kernel.d.ts", "storage-kernel.js", "storage-kernel.d.ts", "manifest.json"]) {
  assert.equal(readFileSync(new URL(`src/verified/generated/${file}`, root), "utf8"), readFileSync(new URL(`dist/verified/generated/${file}`, root), "utf8"));
}
assert.equal(kernel.footprintFits(65n, 64n, true), false);
assert.equal(kernel.pointerCurrent(true, false), false);
assert.equal(kernel.headReadable(true, true, false, true, false), false);
assert.equal(kernel.useNormalized(true, true, true, true), false);
assert.deepEqual(kernel.consume(true), { $: "Tuple", fst: true, snd: false });

const list = (values) => values.reduceRight((tail, head) => ({ $: "Con", head, tail }), { $: "Nil" });
const names = (values) => list(values.map(value => list(Array.from({ length: value.length }, (_, i) => BigInt(value.charCodeAt(i))))));
const parent = authority.authorityIssue(names(["exact grant"]));
assert.equal(authority.authorityLive(authority.authorityDerive(parent, names(["different grant"]))), false);
assert.equal(authority.authorityLive(authority.authorityRelease(parent)), false);
const life = { $: "Life", phase: { $: "Retiring" }, owner: false, holds: 0n, calls: 0n, revoked: true };
const closing = bindingStep(life, { $: "Close" });
assert.equal(closing.command.$, "StartClose");
assert.equal(bindingStep(closing.next, { $: "Begin", cleanup: true }).command.$, "Denied");
const revision = value => ({ $: "Revision", high: value / 4294967296n, low: value % 4294967296n });
const request = { $: "Request", key: list([107n]), expected: { $: "At", version: revision(0n) }, change: { $: "Put", value: list([49n]), identity: list([105n]) } };
const storage = storageTransition(request, { $: "Slot", present: false, version: revision(0n), highWater: revision((1n << 48n) - 1n) });
assert.deepEqual(storage, { $: "PutNext", key: request.key, value: request.change.value, identity: request.change.identity, version: revision(1n << 48n), highWater: revision(1n << 48n) });
assert.equal(storageTransition(request, { $: "Slot", present: false, version: revision(1n), highWater: revision(1n) }).$, "Conflict");
assert.equal(storageTransition(request, { $: "Slot", present: false, version: revision(0n), highWater: revision(9007199254740991n) }).$, "Exhausted");
const declaration = [...Array.from({ length: 64 }, (_, i) => `r${i}`), "shared"];
const normalized = kernel.resourceNormalize(kernel.resourceSource(names(declaration)), names(declaration.slice(0, 64)));
assert.deepEqual(normalized, { $: "Unknown" });
assert.equal(kernel.resourceConflict(normalized, kernel.resourceSource(names(["shared"])), true, false), true);

const registry = new ActionRegistry();
let entered;
let release;
const active = new Promise((resolve) => { entered = resolve; });
const gate = new Promise((resolve) => { release = resolve; });
let secondRan = false;
registry.register({
  name: "proofsmoke", description: "Bundled conflict gate smoke",
  async list() { return []; },
  async describe(name) {
    return { name, description: name, inputSchema: { type: "object", additionalProperties: false }, risk: "write",
      effect: { kind: "transactional", ordering: "ordered", resources: name === "hold"
        ? [...Array.from({ length: 64 }, (_, i) => `r${i}`), "shared"] : ["shared"] } };
  },
  async invoke(name) {
    if (name === "hold") { entered(); await gate; } else secondRan = true;
    return name;
  },
});
const invoke = (name) => registry.invoke(`proofsmoke.${name}`, {}, {
  cwd: fileURLToPath(root), signal: undefined, parentToolCallId: "proof-smoke", nestedToolCallId: name,
  extensionContext: {}, update() {}, approve: async () => {}, audits: [], maxResultChars: 1000, effectPolicy: "strict",
});
const held = invoke("hold");
try {
  await active;
  await assert.rejects(invoke("second"), /unknown resource footprint/);
  assert.equal(secondRan, false);
} finally {
  release();
  await held;
  await registry.close();
}

const entries = ["first request", "next request"].map((text, i) => ({
  type: "message", id: `e${i}`, parentId: i === 0 ? null : "e0", timestamp: "2026-01-01T00:00:00Z",
  message: { role: "user", content: text, timestamp: i },
}));
const summary = compileFabricSummary(entries, 100);
assert.ok("compaction" in summary);
assert.ok(Buffer.byteLength(summary.compaction.summary) <= 32_768);
const invocationContext = { cwd: fileURLToPath(root), signal: undefined, parentToolCallId: "provider-smoke", nestedToolCallId: "provider",
  extensionContext: {}, update() {}, approve: async () => {}, audits: [], maxResultChars: 1000, effectPolicy: "strict" };

let disposed = 0;
let closed = 0;
const authorityRegistry = new ActionRegistry();
authorityRegistry.register({
  name: "authoritysmoke", description: "Bundled capability-view check",
  async list() { return []; },
  async describe(name) {
    return ["read", "lease"].includes(name) ? { name, description: name, inputSchema: { type: "object", additionalProperties: false }, risk: "read",
      effect: { kind: name === "lease" ? "scoped" : "none", resources: ["authoritysmoke"], ordering: "ordered" } } : undefined;
  },
  async invoke() { return 42; },
  async acquire() { return { value: 42, dispose() { disposed++; } }; },
  async close() { closed++; },
});
try {
  const lease = await authorityRegistry.acquireCapabilityView(["authoritysmoke.read"], invocationContext);
  const scoped = { ...invocationContext, capabilityView: lease.view };
  assert.equal(await authorityRegistry.invoke("authoritysmoke.read", {}, scoped), 42);
  await lease.release();
  await assert.rejects(authorityRegistry.invoke("authoritysmoke.read", {}, scoped), /released/);
  const owned = await authorityRegistry.acquireScoped("authoritysmoke.lease", {}, invocationContext);
  await authorityRegistry.close();
  assert.equal(disposed, 1);
  assert.equal(closed, 1);
  assert.deepEqual(authorityRegistry.providerStatus(), []);
  await owned.dispose();
  assert.equal(disposed, 1);
} finally { await authorityRegistry.close(); }
console.log("Bundled artifact identity, registry refusal, authority, scoped lifecycle, storage ABI, compaction, and kernel behavior verified.");
