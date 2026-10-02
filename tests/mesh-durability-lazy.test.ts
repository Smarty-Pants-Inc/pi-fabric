import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
vi.mock("../src/mesh/state-durability.js", async original => {
  loads();
  return original<typeof import("../src/mesh/state-durability.js")>();
});
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });

describe("mesh checkpoint engine first-use boundary", () => {
  it("does not load on cold import, construction, idle reads or volatile publication, then loads once on state use", async () => {
    vi.resetModules(); loads.mockClear();
    const { MeshStore } = await import("../src/mesh/store.js");
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lazy-")); roots.push(directory);
    const mesh = new MeshStore(directory, 65536, 100);
    const identity = { id: "session:lazy", name: "main", kind: "main" as const };
    mesh.get("test/absent"); mesh.listAll();
    await mesh.publish({ topic: "test", from: identity, text: "volatile" });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(loads).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(directory, "state.durable.json"))).toBe(false);
    await mesh.put({ key: "test/key", value: 1, identity });
    expect(loads).toHaveBeenCalledTimes(1);
    await mesh.put({ key: "test/key", value: 2, identity });
    expect(loads).toHaveBeenCalledTimes(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "state.durable.json"), "utf8")).highWater).toBe(2);
  });
});
