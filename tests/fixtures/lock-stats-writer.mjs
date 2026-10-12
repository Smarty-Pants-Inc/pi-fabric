// A real child writer that records one acquisition, mutates its environment and exits: the
// file it leaves must carry the writer facts captured at that first acquisition (smarty-dev#8305).
import { buildSync } from "esbuild";
const [modulePath, root] = process.argv.slice(2);
const { outputFiles } = buildSync({
  entryPoints: [modulePath], bundle: true, platform: "node", format: "esm", write: false,
});
const source = Buffer.from(outputFiles[0].contents).toString("base64");
const { createLockStats } = await import(`data:text/javascript;base64,${source}`);
const stats = createLockStats("1");
stats.acquired(root, "publish", 1, 2);
process.env.PI_FABRIC_AGENT_NAME = "mutated-after-capture";
process.env.SMARTY_ROLE = "mutated-after-capture";
// No explicit flush: the exit hook writes the file.
