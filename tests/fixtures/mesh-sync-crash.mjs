// Child for tests/mesh-store-sync.test.ts (smarty-dev#9313): a producer that publishes events and
// writes mesh state, awaits MeshStore.sync(), and only then fsyncs its own "cursor" (the ack file).
// It runs until the parent SIGKILLs it, so the kill can land anywhere, including mid-publish or
// mid-sync. On Linux it records which files the barrier fsynced (readlink of each fsynced fd).
import { createJiti } from "jiti";
import fs from "node:fs";
import path from "node:path";

// As tests/fleet-isolation-setup.ts does in-process: a fresh test root may create its state.db.
globalThis[Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures")] = "create";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, backend, ackPath, copyDir] = process.argv.slice(2);

const fsynced = [];
const realFsync = fs.fsyncSync;
fs.fsyncSync = (fd) => {
  realFsync(fd);
  if (process.platform === "linux") try { fsynced.push(fs.readlinkSync(`/proc/self/fd/${fd}`)); } catch { /* unnamed fd */ }
};

const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
if (store.stateBackend !== backend) throw new Error(`backend fell back to ${store.stateBackend}`);
const identity = { id: "sync-crash", name: "sync-crash", kind: "agent" };
const ack = fs.openSync(ackPath, "a");
let n = 0;
for (let round = 0; ; round += 1) {
  const published = [];
  for (let i = 0; i < 3; i += 1, n += 1) {
    // A plain publish and a plain put: neither syncs on its own.
    const event = await store.publish({ topic: "ops.sync", kind: "probe", text: `event-${n}`, from: identity, data: { n } });
    const key = `forwarder/failed/${n}`;
    await store.put({ key, value: { sequence: event.sequence }, identity });
    published.push({ id: event.id, sequence: event.sequence, key });
  }
  fsynced.length = 0;
  await store.sync();
  const barrier = [...fsynced];
  // The database file alone, right after the barrier: a PASSIVE checkpoint that confirmed the
  // whole WAL copied every frame into it (and fsynced it), so it needs no WAL to hold the rows.
  if (backend === "sqlite" && round < 3) fs.copyFileSync(path.join(root, "state.db"), path.join(copyDir, `state-${round}.db`));
  // The producer's own cursor, advanced only after the barrier.
  fs.writeSync(ack, JSON.stringify({ round, published, fsynced: barrier }) + "\n");
  realFsync(ack);
}
