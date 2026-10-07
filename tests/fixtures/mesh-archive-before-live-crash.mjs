import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
// Arms the store's test-only crash fences (inert in any service, which never sets it).
globalThis[Symbol.for("pi-fabric.mesh.test-crash-hooks")] = true;
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, packet] = process.argv.slice(2);
const store = new MeshStore(root, 64 * 1024, 100);
await store.publish(JSON.parse(packet));
throw new Error("Publication crash fence did not terminate the publisher");
