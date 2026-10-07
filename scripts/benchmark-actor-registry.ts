#!/usr/bin/env bun
/** Isolated real-filesystem benchmark. Before reproduces main's pre-change writer;
 * after drives the manager's native worker status callbacks at one pulse/second.
 * No model, child process, credentials, fleet root or network is used. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AtomicFileWriter } from "../src/core/atomic-write.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-registry-bench-"));
const beforeRoot = path.join(root, "before"), afterRoot = path.join(root, "after");
const identity: MeshIdentity = { id: "session:bench", name: "main", kind: "main", sessionId: "bench" };
const records = Array.from({ length: 50 }, (_, i) => {
  const id = i.toString(16).padStart(32, "0"), name = `actor-${i}`;
  const messages = Array.from({ length: 100 }, (_, j) => ({ id: `message-${i}-${j}`, actorId: id,
    actorName: name, source: "direct", direction: "in", createdAt: 1 + j, text: "m".repeat(900) }));
  messages[99]!.text += "m".repeat(117_000 - Buffer.byteLength(JSON.stringify(messages)));
  assert.equal(Buffer.byteLength(JSON.stringify(messages)), 117_000);
  return { id, name, rootId: identity.id, instructions: "i".repeat(20_000), messages, createdAt: 1, updatedAt: 1,
    status: "idle", events: [], topics: [], residency: "session", runner: "pi", kernel: "typescript", pythonRuntime: "monty",
    delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: true, requirements: [] };
});

let phase = "idle", insideFileWrite = false;
const totals: Record<string, { bytes: number; registryBytes: number; registryWrites: number; lockWrites: number }> = {};
const descriptors = new Map<number, string>();
const original = { open: fs.openSync, close: fs.closeSync, writeFile: fs.writeFileSync, write: fs.writeSync };
const account = (file: string, bytes: number) => {
  if (phase === "idle" || !file.startsWith(root + path.sep)) return;
  const value = totals[phase] ??= { bytes: 0, registryBytes: 0, registryWrites: 0, lockWrites: 0 };
  value.bytes += bytes;
  if (path.basename(file).startsWith("actors.json.") && file.endsWith(".tmp")) { value.registryBytes += bytes; value.registryWrites++; }
  if (file.endsWith(path.join("actors.json.lock", "owner"))) value.lockWrites++;
};
fs.openSync = ((file, flags, mode) => { const fd = original.open(file, flags, mode); descriptors.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.closeSync = ((fd) => { descriptors.delete(fd); return original.close(fd); }) as typeof fs.closeSync;
fs.writeFileSync = ((file, data, options) => {
  const name = typeof file === "number" ? descriptors.get(file) ?? "" : String(file);
  const previous = insideFileWrite; insideFileWrite = true;
  try { original.writeFile(file, data, options); }
  finally { insideFileWrite = previous; }
  account(name, typeof data === "string" ? Buffer.byteLength(data) : data.byteLength);
}) as typeof fs.writeFileSync;
fs.writeSync = ((...args: Parameters<typeof fs.writeSync>) => {
  const bytes = (original.write as (...args: unknown[]) => number)(...args);
  if (!insideFileWrite) account(descriptors.get(args[0]) ?? "", bytes);
  return bytes;
}) as typeof fs.writeSync;
const io = (): number | undefined => {
  try { return Number(/^write_bytes: (\d+)$/m.exec(fs.readFileSync("/proc/self/io", "utf8"))?.[1]); }
  catch { return undefined; }
};
const settle = () => new Promise<void>(resolve => setImmediate(resolve));
let manager: ActorManager | undefined, agents: AgentManager | undefined;
try {
  fs.mkdirSync(beforeRoot, { recursive: true });
  const beforeFile = path.join(beforeRoot, "actors.json"), oldWriter = new AtomicFileWriter(beforeFile);
  oldWriter.write(JSON.stringify({ format: 1, actors: records }, null, 2));
  const beforeRegistrySize = fs.statSync(beforeFile).size;
  const beforeLock = new ActorRegistryStore(beforeRoot);
  const beforeIo = io();
  phase = "before";
  for (let second = 1; second <= 60; second++) {
    records[0]!.status = second % 2 ? "waiting" : "running";
    records[0]!.updatedAt = second * 1_000;
    // Exact pre-change soft-status path: lock + atomic format-1 replacement.
    await beforeLock.withLock(() => oldWriter.write(JSON.stringify({ format: 1, actors: records }, null, 2)));
  }
  phase = "idle";
  const beforeWriteBytes = io()! - beforeIo!;
  records[0]!.status = "idle";
  const store = new ActorRegistryStore(afterRoot);
  phase = "migration";
  store.write(records);
  phase = "idle";
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  mesh.put = async () => ({ key: "presence", value: {}, version: 1, updatedAt: Date.now(), updatedBy: identity });
  ActorMeshMonitor.prototype.start = () => {};
  ActorMeshMonitor.prototype.schedule = () => {};
  agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  let running: (() => void) | undefined, waiting: (() => void) | undefined;
  agents.status = () => ({ id: "bench-run", queuePosition: 1 } as ReturnType<AgentManager["status"]>);
  agents.run = (_request, signal, onSpawned, _authorize, _downgrade, onQueued) => {
    const handle = { id: "bench-run" } as Parameters<NonNullable<typeof onSpawned>>[0];
    running = () => onSpawned?.(handle); waiting = () => onQueued?.(handle);
    running();
    return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new Error("benchmark ended")), { once: true }));
  };
  manager = new ActorManager("bench", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    actorRoot: afterRoot, persistent: true, rootId: identity.id, claimResidency: "session", reapDeadSessionPresence: false,
  });
  assert.equal(manager.list().length, 50);
  manager.tell(records[0]!.id, "one activation for status benchmark");
  for (let i = 0; i < 50; i++) await settle();
  assert.ok(running && waiting);
  const afterIo = io();
  phase = "after";
  const start = performance.now();
  for (let second = 1; second <= 60; second++) {
    await new Promise(resolve => setTimeout(resolve, Math.max(0, start + second * 1_000 - performance.now())));
    (second % 2 ? waiting : running)();
    for (let i = 0; i < 10; i++) await settle();
  }
  const elapsedMs = performance.now() - start;
  phase = "idle";
  const afterWriteBytes = io()! - afterIo!;
  const afterRegistrySize = fs.statSync(path.join(afterRoot, "actors.json")).size;
  phase = "shutdown";
  await manager.close();
  await agents.close();
  phase = "idle";
  const result = { actors: 50, messagesBytesPerActor: 117_000, instructionsBytesPerActor: 20_000, statusChanges: 60,
    afterElapsedMs: Math.round(elapsedMs), before: { ...totals.before, actorsJsonBytes: beforeRegistrySize, procWriteBytes: beforeWriteBytes },
    after: { ...totals.after, actorsJsonBytes: afterRegistrySize, procWriteBytes: afterWriteBytes },
    reduction: Number((totals.before!.bytes / totals.after!.bytes).toFixed(2)),
    migration: totals.migration, shutdown: totals.shutdown,
    accounting: "Logical synchronous write payload bytes incl. registry, locks and queue; migration/setup excluded from steady-state minute. procWriteBytes is Linux /proc/self/io write_bytes, not block-device/journal telemetry. Before is 60 pre-change soft writes; after is 60 real one-second native worker-status pulses." };
  assert.ok(result.reduction >= 20, `Expected 20x reduction, got ${result.reduction}`);
  console.log(JSON.stringify(result, null, 2));
} finally {
  phase = "idle";
  await manager?.close();
  await agents?.close();
  fs.openSync = original.open; fs.closeSync = original.close; fs.writeFileSync = original.writeFile; fs.writeSync = original.write;
  fs.rmSync(root, { recursive: true, force: true });
}
