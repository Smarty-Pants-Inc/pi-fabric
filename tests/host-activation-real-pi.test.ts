import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it } from "vitest";
import { runRealPiHostProof } from "./helpers/host-activation-real-pi.js";

it.skipIf(process.platform !== "linux")("real Pi cap-one actor ask across roots and all-scope task run/spawn-join complete", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-real-pi-nested-"));
  try {
    const proof = await runRealPiHostProof(root, 1, 0, true);
    expect(proof).toMatchObject({ roots: 3, limit: 1, completed: 3, activations: 6, maxHeldSlots: 1, inferenceAlwaysAdmitted: true, errors: [] });
    expect(proof.nativeToolCalls).toBeGreaterThanOrEqual(5);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}, 120_000);

it.skipIf(process.platform !== "linux")("real Pi cap-one/all-scope deferred handoff releases the slot for its executor and both complete", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-real-pi-handoff-"));
  try {
    const proof = await runRealPiHostProof(root, 1, 0, true, undefined, true);
    expect(proof).toMatchObject({ roots: 3, limit: 1, completed: 1, activations: 2, maxHeldSlots: 1,
      handoff: true, inferenceAlwaysAdmitted: true, errors: [], shutdownCodes: [0, 0, 0] });
    expect(proof.completedHandoffs).toBeGreaterThanOrEqual(1);
    expect(proof.nativeToolCalls).toBeGreaterThanOrEqual(3);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}, 120_000);
