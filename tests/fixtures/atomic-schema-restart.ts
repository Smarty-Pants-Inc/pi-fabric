import fs from "node:fs";
import path from "node:path";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";
import { SchemaController } from "../../src/schema/controller.js";

const [cwd, transactionId] = process.argv.slice(2);
const mesh = new MeshStore(path.join(cwd!, ".pi", "fabric", "mesh"), 256 * 1024, 500);
new SchemaController(cwd!, { ...DEFAULT_FABRIC_CONFIG.schema, mode: "enforce" }, mesh,
  { id: "session:round5-restart", name: "main", kind: "main" });
console.log(JSON.stringify({
  file: fs.readFileSync(path.join(cwd!, "a.txt"), "utf8"),
  workspace: mesh.get("schema/workspace", { fresh: true })?.value,
  journal: JSON.parse(fs.readFileSync(path.join(mesh.root, "schema-transactions", `${transactionId}.json`), "utf8")),
}));
