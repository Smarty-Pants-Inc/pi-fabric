#!/usr/bin/env bun
/** Offline mixed-release proof for sidecar instructions (smarty-dev#8525).
 * Pass the absolute path of a git worktree checked out at a live release SHA (with
 * node_modules): its OWN src/actors/registry-store.ts and ActorManager run in a child.
 * Proves, for that release: it READS new rows; its owned-row SAVE neither publishes a
 * stub nor loses the text (and keeps a foreign new row verbatim); a row it rewrote reads
 * and re-saves correctly in this checkout. Separate processes, private scratch roots. */
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

const release = process.argv[2];
if (!release || !path.isAbsolute(release)) throw new Error("Usage: bun scripts/probe-actor-registry-mixed-release.ts /absolute/release-worktree");
const owned = "a".repeat(32), foreign = "b".repeat(32);
const instructions = "Review the fleet. ".repeat(1_250), foreignInstructions = "Foreign persona. ".repeat(1_300);
const identity: MeshIdentity = { id: "session:mixed", name: "main", kind: "main", sessionId: "mixed" };
const messages = Array.from({ length: 150 }, (_, i) => ({ id: `m-${i}`, source: "direct", direction: "in", createdAt: i, text: "x".repeat(200) }));
const rows = (file: string) => (JSON.parse(fs.readFileSync(file, "utf8")) as { actors: Record<string, unknown>[] }).actors;
const byId = (file: string, id: string) => rows(file).find(row => row.id === id)!;

const startManager = async (src: { ActorManager: typeof ActorManager; ActorMeshMonitor: typeof ActorMeshMonitor;
  AgentManager: typeof AgentManager; MeshStore: typeof MeshStore; DEFAULT_FABRIC_CONFIG: typeof DEFAULT_FABRIC_CONFIG }, root: string, tag: string) => {
  src.ActorMeshMonitor.prototype.start = () => {};
  src.ActorMeshMonitor.prototype.schedule = () => {};
  const mesh = new src.MeshStore(path.join(root, `${tag}-mesh`), 256 * 1024, 100);
  mesh.put = (async () => ({ key: "presence", value: {}, version: 1, updatedAt: Date.now(), updatedBy: identity })) as typeof mesh.put;
  const agents = new src.AgentManager(process.cwd(), src.DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, `${tag}-runs`) });
  const manager = new src.ActorManager("mixed", identity, mesh, src.DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, rootId: identity.id, claimResidency: "session", reapDeadSessionPresence: false,
  });
  return { manager, close: async () => { await manager.close(); await agents.close(); } };
};

if (process.argv[4] === "--old") {
  const root = process.argv[3]!;
  const load = (file: string) => import(pathToFileURL(path.join(release, "src", file)).href);
  const [{ ActorRegistryStore: OldStore }, oldManager, monitor, agentsModule, meshModule, config] = await Promise.all([
    load("actors/registry-store.ts"), load("actors/manager.ts"), load("actors/mesh-monitor.ts"),
    load("agents/manager.ts"), load("mesh/store.ts"), load("config.ts")]);
  const file = path.join(root, "actors", "actors.json");
  const store = new OldStore(path.join(root, "actors"));
  const foreignBefore = JSON.stringify(byId(file, foreign));
  // READ: the old store follows instructionsFile for both rows.
  for (const [id, text] of [[owned, instructions], [foreign, foreignInstructions]] as const) {
    const row = store.records().find((record: Record<string, unknown>) => record.id === id);
    assert.equal(row.instructions, undefined);
    assert.equal(store.instructions(row), text);
  }
  const old = await startManager({ ActorManager: oldManager.ActorManager, ActorMeshMonitor: monitor.ActorMeshMonitor,
    AgentManager: agentsModule.AgentManager, MeshStore: meshModule.MeshStore, DEFAULT_FABRIC_CONFIG: config.DEFAULT_FABRIC_CONFIG }, root, "old");
  try {
    assert.equal(old.manager.instructions(owned), instructions);
    assert.deepEqual(old.manager.messages(owned, 100), messages.slice(-100));
    // SAVE: the old manager's owned-row serializer (known fields only).
    await old.manager.setNice(owned, 7);
  } finally { await old.close(); }
  const saved = byId(file, owned);
  assert.equal(saved.nice, 7);
  assert.equal(saved.instructions, instructions, "old owned save must carry the full text, not a stub");
  assert.equal(saved.instructionsFile, undefined);
  assert.equal(JSON.stringify(byId(file, foreign)), foreignBefore, "old save must keep a foreign new row verbatim");
  assert.equal(store.instructions(byId(file, foreign)), foreignInstructions);
  // The old store's raw write path hydrates every sidecar row inline: still no stub, no loss.
  await store.withLock(() => store.write(store.records()));
  assert.equal(byId(file, foreign).instructions, foreignInstructions);
  assert.equal(byId(file, owned).instructions, instructions);
  console.log(JSON.stringify({ read: true, ownedSaveFullText: true, foreignRowVerbatim: true, rawStoreWriteFullText: true }));
} else {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-mixed-release-"));
  try {
    const actorRoot = path.join(root, "actors"), file = path.join(actorRoot, "actors.json");
    const store = new ActorRegistryStore(actorRoot);
    await store.withLock(() => store.write([
      { id: owned, name: "mixed", rootId: identity.id, residency: "session", instructions, messages, createdAt: 1, status: "idle" },
      { id: foreign, name: "foreign", rootId: "session:other", residency: "session", instructions: foreignInstructions, messages: [], createdAt: 1, status: "idle" },
    ]));
    for (const id of [owned, foreign]) {
      const row = byId(file, id);
      assert.equal("instructions" in row, false);
      assert.match(String(row.instructionsFile), /^[a-f0-9]{64}$/);
    }
    const newBytes = fs.statSync(file).size;
    const sha = childProcess.execFileSync("git", ["-C", release, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
    const old = JSON.parse(childProcess.execFileSync("nice", ["-n", "19", process.execPath, import.meta.filename, release, root, "--old"], {
      encoding: "utf8", timeout: 120_000, env: process.env, cwd: release,
    }).trim().split("\n").at(-1)!);
    // NEW reads the rows the old release rewrote, then re-addresses them on its own save.
    const fresh = new ActorRegistryStore(actorRoot);
    assert.equal(fresh.instructions(byId(file, owned)), instructions);
    assert.equal(fresh.instructions(byId(file, foreign)), foreignInstructions);
    assert.deepEqual(fresh.messages(byId(file, owned)), messages.slice(-100));
    const next = await startManager({ ActorManager, ActorMeshMonitor, AgentManager, MeshStore, DEFAULT_FABRIC_CONFIG }, root, "new");
    try {
      assert.equal(next.manager.instructions(owned), instructions);
      assert.deepEqual(next.manager.messages(owned, 100), messages.slice(-100));
      await next.manager.setNice(owned, 8);
    } finally { await next.close(); }
    const resaved = byId(file, owned);
    assert.equal("instructions" in resaved, false);
    assert.equal(resaved.nice, 8);
    assert.equal(new ActorRegistryStore(actorRoot).instructions(resaved), instructions);
    console.log(JSON.stringify({ passed: true, release: sha, newRegistryBytes: newBytes, old,
      newReadsOldRewrite: true, newResaveIsSidecar: true }));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
