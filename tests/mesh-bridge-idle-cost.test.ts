import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "session:idle0000", name: "idle", kind: "main" };
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-idle-cost-"));
  roots.push(root);
  const hubRoot = path.join(root, "hub");
  fs.mkdirSync(hubRoot);
  // Fleet-sized canonical state, not the tiny state that hid the deployed idle CPU cost.
  const entries: Record<string, MeshStateEntry> = {};
  for (let index = 0; index < 5_000; index++) {
    const key = `control/seen/k${String(index).padStart(5, "0")}`;
    entries[key] = { key, value: "x".repeat(305), version: index + 1, updatedAt: 1, updatedBy: identity };
  }
  const statePath = path.join(hubRoot, "state.json");
  fs.writeFileSync(statePath, JSON.stringify({ readGeneration: randomUUID(), format: 1, revisionFormat: 2, highWater: 5_000, entries }));
  expect(fs.statSync(statePath).size).toBeGreaterThan(2_300_000);
  const hub = new MeshStore(hubRoot, 256 * 1024, 500);
  const writer = new MeshStore(hubRoot, 256 * 1024, 500);
  const far = new MeshStore(path.join(root, "remote"), 256 * 1024, 500);
  const local = new StoreBridgeSide(hub, "quiet");
  const bridge = new MeshBridge({
    localName: "dev1", remoteName: "quiet", local, remote: new StoreBridgeSide(far, "dev1"),
    cursorPath: path.join(root, "cursor.json"),
  });
  await bridge.start();
  await bridge.syncPresence();
  const reads = vi.spyOn(fs, "readFileSync");
  const fullReads = () => reads.mock.calls.filter(([file]) => String(file) === statePath).length;
  return { bridge, hub, writer, local, fullReads };
};

describe("warm mesh bridge idle state cost (smarty-dev#2854)", () => {
  it.each([false, true])("bounds full-state reads over ten idle seconds (3 Hz commits and unrelated log traffic: %s)", async (traffic) => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const { bridge, writer, local, fullReads } = await fixture();
    const presence = vi.spyOn(local, "presence");
    const owned = vi.spyOn(local, "owned");
    let bridgeReads = 0, commits = 0;
    for (let tick = 1; tick <= 40; tick++) {
      now += 250;
      // Exactly three canonical commits per second from a distinct writer/store.
      if (traffic && tick % 4 !== 0) {
        await writer.put({ key: "control/seen/beat", value: ++commits, identity });
      }
      if (traffic && tick % 4 === 0) {
        await writer.publish({ topic: "probe.unrelated", kind: "tick", from: identity });
      }
      const before = fullReads();
      expect(await bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
      const count = fullReads() - before; // excludes writer's own canonical reads
      bridgeReads += count;
      if (tick % 20 !== 0) expect(count).toBe(0);
    }
    // Only the two due presence passes read state: six canonical reads each, not eight
    // full-state parses per tick (plus another eight when filtered pages move the cursor).
    expect(bridgeReads / 10).toBeLessThanOrEqual(1.2);
    expect(presence).toHaveBeenCalledTimes(4); // observation + mirror preflight, every five seconds
    expect(owned).not.toHaveBeenCalled();
    if (traffic) expect(commits).toBe(30);
  }, 20_000);

  it("still reads canonical authority for a nonempty page after idle polls", async () => {
    const { bridge, writer, local, fullReads } = await fixture();
    for (let tick = 0; tick < 4; tick++) await bridge.step();
    const owned = vi.spyOn(local, "owned");
    // Unknown remote recipient is refused, but only after the normal fresh authority read.
    await writer.publish({ topic: "fabric.control.command", kind: "followUp", from: identity,
      to: "session:absent0000", data: { version: 1 } });
    const before = fullReads();
    expect(await bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    expect(owned).toHaveBeenCalledTimes(1);
    expect(fullReads() - before).toBe(5); // presence + owned (four), then canonical mirrored lookup
  });
});
