import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, packet] = process.argv.slice(2);
const store = new MeshStore(root, 64 * 1024, 100);
await store.publish(JSON.parse(packet));
throw new Error("Publication crash fence did not terminate the publisher");
