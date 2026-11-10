import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { configureFabricDiagnostics, fabricDiagnosticsFile, flushFabricDiagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";
import * as wake from "../src/residency/wake.js";

// Keep: failed background wake retries and committed-publication isolation are data-safety paths.
const roots: string[] = [];
afterEach(async () => {
  await flushFabricDiagnostics();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const from = { id: "publisher", name: "publisher", kind: "main" as const };
const fixture = (session: string, canWakeResidents = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-log-only-"));
  roots.push(root);
  vi.stubEnv("SMARTY_ROLE", "worktree-agent@test");
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  configureFabricDiagnostics(session);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100, { canWakeResidents: () => canWakeResidents });
  const residencyRoot = residentRoot(mesh.root, "session:wake-log-only");
  const config = { rootId: "session:wake-log-only", residencyRoot, cwd: root };
  fs.mkdirSync(residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(residencyRoot, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(residencyRoot, "wake-routes.json"), JSON.stringify({ format: 1,
    rootId: config.rootId, hostId: "resident", configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "listener", name: "listener", topics: ["wake.log-only"] }] }));
  return { mesh, residencyRoot };
};
const messages = async (session: string): Promise<string[]> => {
  await flushFabricDiagnostics();
  return fs.readFileSync(fabricDiagnosticsFile(session), "utf8").trim().split("\n")
    .map(line => JSON.parse(line).message as string).filter(line => line.includes("resident wake"));
};
const failureAt = (root: string) => JSON.parse(fs.readFileSync(path.join(root, "wake-failure.json"), "utf8")) as {
  failures: number; nextRetryAt: number; error: string;
};

it("logs one per-root outage across publication and changing maintenance failures, then recovery and a new outage", async () => {
  const session = "resident-wake-outages";
  const { mesh, residencyRoot } = fixture(session);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
  let failing = true;
  let attempts = 0;
  const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {
    attempts++;
    if (failing) throw new Error(`launch retry ${attempts}`);
  });
  try {
    const event = await mesh.publish({ topic: "wake.log-only", from });
    await wake.wakeResidentActors(mesh, [event], launch);
    const first = failureAt(residencyRoot);
    expect(first.failures).toBe(1);
    expect(residentUnacknowledgedDeliveries(mesh.root).has(residencyRoot)).toBe(true);
    await wake.retryResidentWakes(mesh, launch, first.nextRetryAt);
    const second = failureAt(residencyRoot);
    expect(second.failures).toBe(2);
    expect(second.nextRetryAt).toBeGreaterThan(first.nextRetryAt);
    expect(second.error).toContain("launch retry 2");
    await wake.retryResidentWakes(mesh, launch, second.nextRetryAt);
    const third = failureAt(residencyRoot);
    expect(third.failures).toBe(3);
    expect(third.nextRetryAt).toBeGreaterThan(second.nextRetryAt);
    expect(await messages(session)).toEqual([`[pi-fabric] resident wake deferred for ${residencyRoot}: Error: launch retry 1`]);
    failing = false;
    await wake.retryResidentWakes(mesh, launch, third.nextRetryAt);
    expect(attempts).toBe(4);
    expect(residentUnacknowledgedDeliveries(mesh.root).has(residencyRoot)).toBe(false);
    expect(fs.existsSync(path.join(residencyRoot, "wake-failure.json"))).toBe(false);
    await wake.retryResidentWakes(mesh, launch, third.nextRetryAt + 60_000);
    expect(attempts).toBe(4); // No extra launch or recovery for already served work.
    failing = true;
    const next = await mesh.publish({ topic: "wake.log-only", from });
    await wake.wakeResidentActors(mesh, [next], launch);
    expect(await messages(session)).toEqual([
      `[pi-fabric] resident wake deferred for ${residencyRoot}: Error: launch retry 1`,
      `[pi-fabric] resident wake recovered for ${residencyRoot}`,
      `[pi-fabric] resident wake deferred for ${residencyRoot}: Error: launch retry 5`,
    ]);
    const actual = new Error("explicit launch failure");
    await expect(wake.requestResidentWake(residencyRoot, { id: "direct" }, async () => { throw actual; }, undefined,
      { allowed: () => true, exclusive: mesh.exclusive.bind(mesh) })).rejects.toBe(actual);
    expect(warn).not.toHaveBeenCalled();
    expect(stderr).not.toHaveBeenCalled();
  } finally { mesh.closeState(); }
});

it("keeps the store outer fallback log-only once per outage without turning committed events into publish failures", async () => {
  const session = "resident-wake-outer-outages";
  const { mesh } = fixture(session, true);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
  const deferred = vi.spyOn(wake, "wakeResidentActors")
    .mockRejectedValueOnce(new Error("scan retry 1"))
    .mockRejectedValueOnce(new Error("scan retry 2"))
    .mockResolvedValueOnce(undefined)
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error("scan retry 3"));
  try {
    for (let n = 0; n < 5; n++) await expect(mesh.publish({ topic: "wake.log-only", from })).resolves.toMatchObject({ topic: "wake.log-only" });
    expect(deferred).toHaveBeenCalledTimes(5);
    expect(mesh.read({ topic: "wake.log-only", limit: 10 })).toHaveLength(5);
    expect(await messages(session)).toEqual([
      "[pi-fabric] resident wake deferred: Error: scan retry 1",
      "[pi-fabric] resident wake recovered",
      "[pi-fabric] resident wake deferred: Error: scan retry 3",
    ]);
    expect(warn).not.toHaveBeenCalled();
    expect(stderr).not.toHaveBeenCalled();
  } finally { mesh.closeState(); }
});
