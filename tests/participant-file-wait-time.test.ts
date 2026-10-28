// Keep: #126 diagnostic loss at the real participant-namespace custody boundary.
// Inject only clocks/timer scheduling; acquire the real namespace receipt twice.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as clock } from "node:perf_hooks";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantFileCustodyTimeoutError, withParticipantFileDirectoryCustody } from "../src/topology/participant-files.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
it("measures namespace acquisition wait, not the largest inter-attempt gap or time in custody", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "participant-wait-time-"));
  const store = new MeshStore(root, 65_536, 100, { stateBackend: "file" });
  let release!: () => void, entered!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const ready = new Promise<void>(resolve => { entered = resolve; });
  const holder = withParticipantFileDirectoryCustody(store, async () => { entered(); await held; });
  await ready;
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  const epoch = Date.now();
  vi.spyOn(clock, "now").mockImplementation(() => Date.now() - epoch);
  try {
    const refused = withParticipantFileDirectoryCustody(store, () => { throw new Error("must not enter custody"); })
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(5_005);
    const error = await refused;
    expect(error).toBeInstanceOf(ParticipantFileCustodyTimeoutError);
    expect(error).toMatchObject({ lockWaitMs: 5_000, maxGapMs: 5 });
    expect((error as Error).message).toContain("lockWaitMs=5000");
    expect((error as ParticipantFileCustodyTimeoutError).attempts).toBeGreaterThan(1);
  } finally { release(); await holder; store.closeState(); }
});
