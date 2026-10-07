// Runs ONE mesh operation in a separate process, synchronously from a parent test
// that is frozen at a chosen point of its own publish (spawnSync).
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, mode, arg] = process.argv.slice(2);
const store = new MeshStore(root, 1024, 100, JSON.parse(process.env.MESH_RACE_OPTIONS ?? "{}"));
try {
  if (mode === "publish") console.log(JSON.stringify({ ok: true, event: await store.publish(JSON.parse(arg)) }));
  else if (mode === "compact") { await store.settleCompaction(); console.log(JSON.stringify({ ok: true })); }
  else throw new Error(`unknown mode ${mode}`);
} catch (error) {
  console.log(JSON.stringify({ ok: false, name: error?.name, message: String(error?.message ?? error) }));
}
