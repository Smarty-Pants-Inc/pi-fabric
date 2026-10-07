// Tight unkeyed publish loop in a separate process until the stop file appears.
import fs from "node:fs";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, stop] = process.argv.slice(2);
const store = new MeshStore(root, 1024, 100, JSON.parse(process.env.MESH_LOOP_OPTIONS ?? "{}"));
const from = { id: "session:loop", name: "loop", kind: "main" };
let count = 0;
try {
  while (!fs.existsSync(stop)) { await store.publish({ topic: "mesh.fsync", from, text: `loop ${count}` }); count++; }
  console.log(JSON.stringify({ ok: true, count }));
} catch (error) {
  console.log(JSON.stringify({ ok: false, count, message: String(error?.message ?? error) }));
}
