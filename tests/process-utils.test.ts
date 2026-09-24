import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("spawnDetached", () => {
  // dev-lead review D7 on #26: after the worker exited, its numeric id may name an
  // unrelated process (group); stop and liveness must not act on it.
  it("neither signals nor reports alive a worker id after the worker exited, even if the id is reused", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-spawn-"));
    roots.push(root);
    const worker = path.join(root, "exit.mjs");
    fs.writeFileSync(worker, "process.exit(0);\n");
    const handle = await spawnDetached(worker, [], root);
    await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false), { timeout: 10_000, interval: 20 });
    // The id is now reused by an unrelated process: every numeric probe or signal succeeds.
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    expect(await handle.isAlive()).toBe(false);
    await handle.stop();
    expect(kill).not.toHaveBeenCalled();
  });

  it("stops a worker that is still running", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-spawn-"));
    roots.push(root);
    const worker = path.join(root, "hang.mjs");
    fs.writeFileSync(worker, "setInterval(() => {}, 1_000);\n");
    const handle = await spawnDetached(worker, [], root);
    expect(await handle.isAlive()).toBe(true);
    await handle.stop();
    await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false), { timeout: 10_000, interval: 20 });
  });
});
