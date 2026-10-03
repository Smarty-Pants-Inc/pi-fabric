import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { clearSessionTextCache, readSessionDerived } from "../src/memory/session-file-cache.js";
import { spawnSync } from "node:child_process";
const readIds = (file: string) => readSessionDerived(file, "ids", () => [] as string[], (ids, record) => {
  const id = (record as { id?: string }).id; if (id) ids.push(id);
});
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
    const file = path.join(temp(), "session.jsonl");
    fs.writeFileSync(file, '{"id":"one"}\n');
    expect(readIds(file)).toEqual(["one"]);
    const spy = vi.spyOn(fs, "readSync");
    expect(readIds(file)).toEqual(["one"]);
    expect(spy).not.toHaveBeenCalled();
    fs.appendFileSync(file, '{"id":"two"}\n');
    expect(readIds(file)).toEqual(["one", "two"]);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("invalidates on inode replacement and truncation", () => {
    const root = temp(), file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, '{"id":"old"}\n');
    expect(readIds(file)).toEqual(["old"]);
    const replacement = path.join(root, "replacement.jsonl");
    fs.writeFileSync(replacement, '{"id":"new"}\n');
    fs.renameSync(replacement, file);
    expect(readIds(file)).toEqual(["new"]);
    fs.writeFileSync(file, '{"id":"x"}\n');
    expect(readIds(file)).toEqual(["x"]);
  });

  it("reuses an unchanged actor registry and reloads its replacement", () => {
    const root = temp();
    const file = path.join(root, "actors.json");
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ id: "a" }] }));
    const store = new ActorRegistryStore(root);
    const beforeStamp = store.fingerprint();
    const beforeInode = fs.statSync(file).ino;
    const spy = vi.spyOn(fs, "readFileSync");
    expect(store.records()).toHaveLength(1);
    expect(store.records()).toHaveLength(1);
    expect(spy).toHaveBeenCalledTimes(1);
    const replacement = path.join(root, "replacement.json");
    const result = spawnSync(process.execPath, ["-e", `const fs=require('node:fs'); fs.writeFileSync(process.argv[1],JSON.stringify({format:1,actors:[{id:'b'}]})); fs.renameSync(process.argv[1],process.argv[2]);`, replacement, file]);
    expect(result.status).toBe(0);
    expect(fs.statSync(file).ino).not.toBe(beforeInode);
    expect(store.fingerprint()).not.toBe(beforeStamp);
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
