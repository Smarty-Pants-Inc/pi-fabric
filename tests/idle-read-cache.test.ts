import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { clearSessionTextCache, readSessionText } from "../src/memory/session-file-cache.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  clearSessionTextCache();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const temp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-read-"));
  roots.push(root);
  return root;
};

const stateReadCount = (spy: { mock: { calls: unknown[][] } }): number =>
  spy.mock.calls.filter((args) => typeof args[0] === "string" && args[0].endsWith("/state.json")).length;

describe("idle whole-file read caches", () => {
  it("does not reread an unchanged session and follows appends", () => {
    const root = temp();
    const file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, '{"type":"session","id":"s"}\n{"id":"one","parentId":null}\n');
    const spy = vi.spyOn(fs, "readFileSync");
    expect(readSessionText(file)).toContain('"one"');
    expect(readSessionText(file)).toContain('"one"');
    expect(spy).toHaveBeenCalledTimes(1);
    fs.appendFileSync(file, '{"id":"two","parentId":"one"}\n');
    expect(readSessionText(file)).toContain('"two"');
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("invalidates on inode replacement and truncation", () => {
    const root = temp();
    const file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, "old\n");
    const spy = vi.spyOn(fs, "readFileSync");
    expect(readSessionText(file)).toBe("old\n");
    const replacement = path.join(root, "replacement.jsonl");
    fs.writeFileSync(replacement, "new\n");
    fs.renameSync(replacement, file);
    expect(readSessionText(file)).toBe("new\n");
    fs.writeFileSync(file, "x\n");
    expect(readSessionText(file)).toBe("x\n");
    expect(spy).toHaveBeenCalledTimes(3);
  });

  it("reuses an unchanged actor registry and reloads its replacement", () => {
    const root = temp();
    const file = path.join(root, "actors.json");
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ id: "a" }] }));
    const store = new ActorRegistryStore(root);
    const spy = vi.spyOn(fs, "readFileSync");
    expect(store.records()).toHaveLength(1);
    expect(store.records()).toHaveLength(1);
    expect(spy).toHaveBeenCalledTimes(1);
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ id: "b" }] }));
    expect(store.records()[0]?.id).toBe("b");
    expect(spy).toHaveBeenCalledTimes(2);
  });

  it("selects all namespaces from one fresh canonical state", () => {
    const root = temp();
    const meshRoot = path.join(root, "mesh");
    fs.mkdirSync(meshRoot);
    fs.writeFileSync(path.join(meshRoot, "state.json"), JSON.stringify({
      format: 1, revisionFormat: 2, highWater: 4,
      entries: {
        "topology/hosts/h": { key: "topology/hosts/h", version: 1, updatedAt: 1, updatedBy: { id: "h", name: "h", kind: "main" }, value: {} },
        "topology/participants/p": { key: "topology/participants/p", version: 2, updatedAt: 1, updatedBy: { id: "p", name: "p", kind: "main" }, value: {} },
        "topology/legacy/s": { key: "topology/legacy/s", version: 3, updatedAt: 1, updatedBy: { id: "s", name: "s", kind: "main" }, value: {} },
        "topology/actors/a": { key: "topology/actors/a", version: 4, updatedAt: 1, updatedBy: { id: "a", name: "a", kind: "main" }, value: {} },
      },
    }));
    const mesh = new MeshStore(meshRoot, 64 * 1024, 0);
    const spy = vi.spyOn(fs, "readFileSync");
    const snapshot = mesh.stateToken({ fresh: true });
    for (const prefix of ["topology/hosts/", "topology/participants/", "topology/legacy/", "topology/actors/"]) {
      mesh.listAllShared(prefix, { snapshot });
    }
    expect(stateReadCount(spy)).toBe(1);
  });
});
