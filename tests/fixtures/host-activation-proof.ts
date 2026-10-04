import path from "node:path";
import fs from "node:fs";
import { runHostActivationProof } from "../helpers/host-activation-proof.js";
import { isolateTestFleetEnvironment } from "../../scripts/test-temp.js";
const output = process.env.TASK_OUT;
const scratch = process.env.TMPDIR;
if (!output || !scratch) throw new Error("TASK_OUT and TMPDIR required");
isolateTestFleetEnvironment(); // Never inherit production fleet/profile selectors.
const root = fs.mkdtempSync(path.join(scratch, "three-main-proof-"));
try {
  const proof = await runHostActivationProof(root, 4, true);
  fs.cpSync(root, path.join(output, "three-main-proof"), { recursive: true });
  if (proof.activations !== 12 || proof.completed !== 12 || proof.maxConcurrent !== 4 || proof.hostQueueStatuses !== 12) throw new Error("Proof acceptance failed");
  console.log(JSON.stringify({ ...proof, events: undefined }, null, 2));
} finally { fs.rmSync(root, { recursive: true, force: true }); }
