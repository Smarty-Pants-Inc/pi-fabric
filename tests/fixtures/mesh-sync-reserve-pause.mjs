// Child for tests/mesh-store-sync.test.ts (pi-fabric#894): publisher "A" of the reservation race.
// With PI_FABRIC_TEST_PAUSE_AFTER_RESERVE set by the parent, its publish reserves the next sequence
// and then blocks under .lock, before the append, until the parent releases it. It prints the event.
import { createJiti } from "jiti";

const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root] = process.argv.slice(2);
const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
try {
  const event = await store.publish({ topic: "ops.sync", kind: "probe", text: "reserved-then-paused",
    from: { id: "sync-race-a", name: "sync-race-a", kind: "agent" } });
  process.stdout.write(JSON.stringify(event));
} finally { store.closeState(); }
