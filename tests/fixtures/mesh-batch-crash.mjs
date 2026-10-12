import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const [root, packets] = process.argv.slice(2);
// Deterministic batch extent: on a slow disk the 50 ms work bound would split the batch.
performance.now = () => 0;
const store = new MeshStore(root, 64 * 1024, 100);
await store.publishBatch(JSON.parse(packets));
throw new Error("Batch crash fence did not terminate the publisher");
