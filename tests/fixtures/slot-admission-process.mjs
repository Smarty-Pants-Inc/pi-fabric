// Safe Node-JS-only real-store fixture. Never imports/runs a Fabric worker or starts Bun/TS children.
// The bundle is compiled by the parent from the core modules, with installer policy supplied ONLY
// by a private fixture module. No production trust check or test isolation configuration is edited.
import fs from "node:fs";
import { pathToFileURL } from "node:url";
const [bundle, root, inputFile, mode, receipt] = process.argv.slice(2);
const deadline = setTimeout(() => { process.stderr.write("slot fixture finite deadline\n"); process.exit(70); }, 20_000);
deadline.unref();
const { MeshStore, slotAdmissionFor } = await import(pathToFileURL(bundle).href);
const mesh = new MeshStore(root, 64 * 1024, 10_000, { stateBackend: "sqlite" });
const coordinator = slotAdmissionFor(mesh);
const input = JSON.parse(fs.readFileSync(inputFile, "utf8"));
await new Promise(resolve => {
  process.once("message", message => { if (message === "go") resolve(); else process.exit(71); });
  process.send({ phase: "ready" });
});
if (mode === "exit-before-commit") input.beforeCommit = actorId => {
  fs.writeFileSync(receipt, JSON.stringify({ actorId, accepted: true }));
  process.exit(42); // OS rollback of uncommitted SQLite transaction; no signal and no cleanup child.
};
if (mode === "exit-before-sync") mesh.stateBackendHandle.sync = async () => {
  fs.writeFileSync(receipt, JSON.stringify({ actorId: input.actorId, committed: true }));
  process.exit(43); // Deliberate process-crash window after real COMMIT and before durability barrier.
};
let result;
try { result = { ok: true, value: await coordinator.admit(input) }; }
catch (error) { result = { ok: false, code: error.code, reason: error.reason, message: error.message }; }
mesh.closeState();
process.send({ phase: "result", result }, () => { clearTimeout(deadline); process.disconnect(); });
