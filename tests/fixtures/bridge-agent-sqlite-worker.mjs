import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { parentPort, threadId, workerData } from "node:worker_threads";

const root = path.resolve(workerData.root);
const mode = workerData.mode ?? "normal";
const armPath = workerData.armPath;
const enteredPath = workerData.enteredPath;
const completedPath = workerData.completedPath;
const delayMs = workerData.delayMs ?? 0;
const waitSynchronously = () => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);

if (mode === "commit") {
  const originalExec = DatabaseSync.prototype.exec;
  const writeStarts = new WeakMap();
  const changes = database => Number(database.prepare("SELECT total_changes() AS n").get().n);
  let injected = false;
  DatabaseSync.prototype.exec = function (sql, ...args) {
    const statement = typeof sql === "string" ? sql.trim().toUpperCase() : "";
    if (statement === "BEGIN IMMEDIATE") {
      const result = originalExec.call(this, sql, ...args);
      writeStarts.set(this, changes(this));
      return result;
    }
    // Snapshot reads also COMMIT. Only stall a write transaction that actually
    // changed rows, so a mirror preflight/read-only recovery cannot fake this gate.
    const writeChanges = statement === "COMMIT" && writeStarts.has(this) ? changes(this) - writeStarts.get(this) : 0;
    if (!injected && statement === "COMMIT" && writeChanges > 0 && fs.existsSync(armPath)) {
      injected = true;
      const startedAt = Date.now();
      fs.writeFileSync(enteredPath, JSON.stringify({ startedAt, threadId, writeChanges }));
      waitSynchronously();
      fs.writeFileSync(completedPath, JSON.stringify({ blockedMs: Date.now() - startedAt }));
    }
    const result = originalExec.call(this, sql, ...args);
    if (statement === "COMMIT" || statement === "ROLLBACK") writeStarts.delete(this);
    return result;
  };
}

if (mode === "scan") {
  const originalReadDirSync = fs.readdirSync;
  const participants = path.join(root, "participants");
  let injected = false;
  fs.readdirSync = function (directory, ...args) {
    if (!injected && path.resolve(String(directory)) === participants && fs.existsSync(armPath)) {
      injected = true;
      const startedAt = Date.now();
      fs.writeFileSync(enteredPath, JSON.stringify({ startedAt, threadId }));
      waitSynchronously();
      fs.writeFileSync(completedPath, JSON.stringify({ blockedMs: Date.now() - startedAt }));
    }
    return originalReadDirSync.call(this, directory, ...args);
  };
}

const [{ MeshStore }, { StoreBridgeSide }, { serveBridgeAgentOnPort }] = await Promise.all([
  import("../../src/mesh/store.js"),
  import("../../src/mesh/bridge.js"),
  import("../../src/mesh/bridge-agent.js"),
]);

const store = new MeshStore(root, 64 * 1024, workerData.maxReadEvents ?? 100, {
  stateBackend: "sqlite",
});
const side = new StoreBridgeSide(store, workerData.peer ?? "hub", Date.now, true);
if (mode === "commit") {
  // Ordinary fleet.work appends do not commit SQLite state. This test-only adapter
  // deliberately puts real keyed state BEFORE publication so the COMMIT hook above
  // stalls the worker inside an actual transaction, not an unrelated sleep.
  const publish = side.publish.bind(side);
  let ordinal = 0;
  side.publish = async (...args) => {
    await store.put({ key: "test/bridge-agent-before-publish", identity: args[0].from,
      value: { ordinal: ++ordinal, text: args[0].text } });
    return publish(...args);
  };
}
await store.prepareLock();
await serveBridgeAgentOnPort(side, parentPort);
