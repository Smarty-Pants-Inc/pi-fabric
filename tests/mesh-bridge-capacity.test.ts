import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { main, openBridgeStore, runBridge } from "../src/mesh-bridge.js";

const roots: string[] = [];
const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-capacity-"));
  roots.push(root);
  return root;
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("mesh-bridge state capacity", () => {
  it("keeps the default 32 MiB barrier and reads a larger mesh only with an explicit override", () => {
    const root = scratch();
    // Legal state larger than the stock CLI ceiling; padding does not weaken entry limits.
    fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({
      format: 1, revisionFormat: 2, entries: {}, highWater: 0,
    }) + " ".repeat(33 * 1024 * 1024));
    expect(() => openBridgeStore(root, new Map()).listAll("topology/")).toThrow("state exceeds 33554432 bytes");
    expect(openBridgeStore(root, new Map([["max-state-bytes", String(64 * 1024 * 1024)]])).listAll("topology/")).toEqual([]);
    expect(openBridgeStore(root, new Map([["lock-protocol", "2"], ["max-state-bytes", "67108864"]])).lockProtocol).toBe(2);
  });

  it.each(["0", "-1", "524287", "NaN", "Infinity", "1.5", "9007199254740992"])("rejects invalid capacity %s in both modes before transport starts", async (value) => {
    const root = scratch();
    await expect(main(["agent", "--mesh", root, "--peer", "hub", "--max-state-bytes", value])).rejects.toThrow("--max-state-bytes");
    const flags = new Map([["mesh", root], ["name", "hub"], ["remote", "peer"], ["cursor", path.join(root, "cursor.json")], ["max-state-bytes", value]]);
    let spawned = false;
    await expect(runBridge(flags, ["nonexistent-capacity-test-transport"], new AbortController().signal, () => { spawned = true; })).rejects.toThrow("--max-state-bytes");
    expect(spawned).toBe(false);
  });

  it("accepts the store minimum without silently clamping a requested limit", () => {
    expect(openBridgeStore(scratch(), new Map([["max-state-bytes", "524288"]])).listAll("topology/")).toEqual([]);
  });
});
