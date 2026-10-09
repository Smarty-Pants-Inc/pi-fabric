import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomicWrite from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-write-byte-reuse-"));
  roots.push(root);
  return { root, mesh: new MeshStore(root, 64 * 1024, 100), identity: { id: "writer", name: "writer", kind: "main" as const } };
};

describe("canonical write byte reuse", () => {
  it("freshly reads bytes but shares only frozen unchanged entries between private transaction maps", async () => {
    const { root, mesh, identity } = fixture();
    await mesh.put({ key: "retained/value", value: { nested: { text: "retained" } }, identity });
    const snapshot = mesh.stateToken({ fresh: true });
    const retained = mesh.listAllShared("retained/", { snapshot })[0]!;
    const read = vi.spyOn(atomicWrite, "readFileRetrying");
    await mesh.put({ key: "changed/value", value: 1, identity });
    expect(read.mock.calls.filter(([file]) => file === path.join(root, "state.json"))).toHaveLength(1);
    expect(mesh.listAllShared("retained/", { fresh: true })[0]).toBe(retained);
    expect(Object.isFrozen(retained)).toBe(true);
    expect(Object.isFrozen((retained.value as { nested: object }).nested)).toBe(true);
    expect(() => { (retained.value as { nested: { text: string } }).nested.text = "corrupt"; }).toThrow();
    await mesh.delete({ key: "retained/value", ifVersion: retained.version });
    expect(mesh.get("retained/value", { snapshot })).toEqual(retained);
    expect(mesh.get("retained/value", { fresh: true })).toBeUndefined();
    const tombstone = JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8")).versions["retained/value"];
    await expect(mesh.put({ key: "retained/value", value: "new", identity, ifVersion: retained.version })).rejects.toThrow(/compare-and-swap/);
    await mesh.put({ key: "retained/value", value: "new", identity, ifVersion: tombstone });
    expect(mesh.get("retained/value", { fresh: true })?.version).toBeGreaterThan(tombstone);
  });

  it("rejects parse reuse after foreign byte changes even with copied generation and version labels", async () => {
    const { root, mesh, identity } = fixture();
    await mesh.put({ key: "retained/value", value: { text: "before" }, identity });
    const file = path.join(root, "state.json");
    const state = JSON.parse(fs.readFileSync(file, "utf8"));
    state.entries["retained/value"].value.text = "foreign";
    fs.writeFileSync(file, JSON.stringify(state));
    await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "other/value", value: "ours" }] });
    expect(mesh.get("retained/value", { fresh: true })?.value).toEqual({ text: "foreign" });
    expect(mesh.get("other/value", { fresh: true })?.value).toBe("ours");
    expect(mesh.get("retained/value", { fresh: true })?.version).toBe(state.entries["retained/value"].version);
  });
});
