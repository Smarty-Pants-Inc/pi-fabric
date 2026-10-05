#!/usr/bin/env bun
/** Offline mixed-release proof. Build the legacy release normally, then bundle an
 * entry exporting ActorRegistryStore, ActorManager, ActorMeshMonitor, AgentManager,
 * MeshStore and DEFAULT_FABRIC_CONFIG from that release. Pass the bundle's absolute
 * path here. Uses separate processes and private scratch roots; no fleet I/O. */
import assert from "node:assert/strict";
import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { AgentManager } from "../src/agents/manager.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const oldBundle = process.argv[2];
if (!oldBundle || !path.isAbsolute(oldBundle)) throw new Error("Usage: bun scripts/probe-actor-registry-mixed-release.ts /absolute/legacy-bundle.mjs");
const id = "a".repeat(32), instructions = "i".repeat(20_000);
const identity: MeshIdentity = { id: "session:mixed", name: "main", kind: "main", sessionId: "mixed" };
const messages = Array.from({ length: 150 }, (_, i) => ({ id: `m-${i}`, source: "direct", direction: "in", createdAt: i, text: "x".repeat(1_100) }));

if (process.argv[4] === "--old-worker" || process.argv[4] === "--old-resume") {
  const mode = process.argv[4]!;
  const root = process.argv[3]!;
  const old = await import(pathToFileURL(oldBundle).href);
  const actorRoot = path.join(root, "actors"), store = new old.ActorRegistryStore(actorRoot);
  const original = store.records()[0];
  const expected = mode === "--old-resume" ? messages.slice(-100) : [];
  assert.deepEqual(original.messages, expected);
  assert.equal(original.instructions, instructions);
  if (mode === "--old-worker") {
    // The store passes unknown fields through without projecting them.
    await store.withLock(() => store.write(store.records()));
    assert.deepEqual(store.records()[0].messageHistory, original.messageHistory);
  }
  old.ActorMeshMonitor.prototype.start = () => {};
  old.ActorMeshMonitor.prototype.schedule = () => {};
  const mesh = new old.MeshStore(path.join(root, "old-mesh"), 256 * 1024, 100);
  mesh.put = async () => ({ key: "presence", value: {}, version: 1, updatedAt: Date.now(), updatedBy: identity });
  const agents = new old.AgentManager(process.cwd(), old.DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "old-runs") });
  const manager = new old.ActorManager("mixed", identity, mesh, old.DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    actorRoot, persistent: true, rootId: identity.id, claimResidency: "session", reapDeadSessionPresence: false,
  });
  try {
    assert.equal(manager.instructions(id), instructions);
    assert.deepEqual(manager.messages(id, 100), expected);
    if (mode === "--old-worker") await manager.setNice(id, 7); // Old owned-row serializer drops unknown fields.
  } finally { await manager.close(); await agents.close(); }
  if (mode === "--old-worker") {
    const rewritten = store.records()[0];
    assert.equal(rewritten.messageHistory, undefined);
    assert.equal(rewritten.instructionsFile, undefined);
    assert.equal(rewritten.instructions, instructions);
    assert.deepEqual(rewritten.messages, []);
    console.log("PASS old compiled store preserved raw fields; old compiled manager/store then stripped reference fields and saved empty messages.");
  } else {
    console.log("PASS old compiled manager resumed after inline restore with the full active history.");
  }
} else {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-mixed-release-"));
  let manager: ActorManager | undefined, agents: AgentManager | undefined;
  try {
    const actorRoot = path.join(root, "actors"), store = new ActorRegistryStore(actorRoot);
    await store.withLock(() => store.write([{ id, name: "mixed", rootId: identity.id, instructions, messages, createdAt: 1, status: "idle" }]));
    const log = path.join(actorRoot, id, "registry", "messages.jsonl"), archive = fs.readFileSync(log, "utf8");
    assert.equal(JSON.parse(archive.trim()).messages.length, 150);
    const oldOutput = childProcess.execFileSync("nice", ["-n", "19", process.execPath, import.meta.filename, oldBundle, root, "--old-worker"], {
      encoding: "utf8", timeout: 60_000, env: process.env,
    }).trim();
    const fresh = new ActorRegistryStore(actorRoot), row = fresh.records()[0]!;
    assert.equal(row.messageHistory, undefined);
    assert.equal(fresh.instructions(row), instructions);
    assert.deepEqual(fresh.messages(row), messages.slice(-100));
    assert.equal(fs.readFileSync(log, "utf8"), archive);
    // Roll back directly from the old-owned-save shape, then start the actual
    // compiled 6b15d905 manager and verify that it sees the restored ring.
    assert.equal(await fresh.restoreInlineForDowngrade(), 1);
    const restored = fresh.records()[0]!;
    assert.equal(restored.messageHistory, undefined);
    assert.deepEqual(restored.messages, messages.slice(-100));
    assert.equal(fs.readFileSync(log, "utf8"), archive);
    const oldResumeOutput = childProcess.execFileSync("nice", ["-n", "19", process.execPath, import.meta.filename, oldBundle, root, "--old-resume"], {
      encoding: "utf8", timeout: 60_000, env: process.env,
    }).trim();
    ActorMeshMonitor.prototype.start = () => {};
    ActorMeshMonitor.prototype.schedule = () => {};
    const mesh = new MeshStore(path.join(root, "new-mesh"), 256 * 1024, 100);
    mesh.put = async () => ({ key: "presence", value: {}, version: 1, updatedAt: Date.now(), updatedBy: identity });
    agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "new-runs") });
    manager = new ActorManager("mixed", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      actorRoot, persistent: true, rootId: identity.id, claimResidency: "session", reapDeadSessionPresence: false,
    });
    assert.equal(manager.status(id).messages, 100);
    assert.equal(manager.instructions(id), instructions);
    assert.deepEqual(manager.messages(id, 100), messages.slice(-100));
    await manager.setNice(id, 8);
    assert.equal(fs.readFileSync(log, "utf8"), archive);
    await manager.close(); await agents.close();
    console.log(JSON.stringify({ passed: true, oldBundle, oldOutput, oldResumeOutput, archivedMessagesPreserved: 150,
      activeMessagesPreserved: 100, instructionsBytesPreserved: Buffer.byteLength(instructions),
      oldOwnedSaveDroppedUnknownFields: true, downgradeRestoreVerified: true,
      historyArchiveByteIdentical: true, newManagerReloadAndSave: true }, null, 2));
  } finally {
    await manager?.close(); await agents?.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
