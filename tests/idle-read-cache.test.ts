import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomicWrite from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const temp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-read-"));
  roots.push(root);
  return root;
};

describe("participant canonical snapshot", () => {
  it("uses one canonical snapshot in ParticipantDirectory and sees the next replacement", () => {
    const root = temp();
    const file = path.join(root, "state.json");
    const identity = { id: "h", name: "main", kind: "main" as const };
    const key = "topology/participants/" + createHash("sha256").update("p").digest("hex");
    const state = (name: string, version: number) => ({
      format: 1, revisionFormat: 2, highWater: version,
      entries: { [key]: { key, version, updatedAt: version, updatedBy: identity, value: {
        format: 1, id: "p", name, kind: "root", rootId: "p", ownerHostId: "h",
        ownerIdentityId: "h", status: "idle", runner: "pi", transport: "host",
        capabilities: ["fabric"], startedAt: 1, updatedAt: version, controlProtocol: "v1",
      } } },
    });
    fs.writeFileSync(file, JSON.stringify(state("before", 1)));
    const mesh = new MeshStore(root, 64 * 1024, 0);
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: "h", rootId: "root", identity, heartbeatMs: 60_000, leaseMs: 120_000,
    });
    // A started directory has a local self record. Isolate the four-namespace scan
    // from cold self()'s separate fallback scan, without starting heartbeat writers.
    vi.spyOn(directory, "self").mockReturnValue(directory.self());
    const select = mesh.listAllShared.bind(mesh);
    let replaced = false;
    const selections = vi.spyOn(mesh, "listAllShared").mockImplementation((prefix, options) => {
      const result = select(prefix, options);
      if (!replaced) {
        replaced = true;
        const replacement = path.join(root, "replacement.json");
        fs.writeFileSync(replacement, JSON.stringify(state("after", 2)));
        fs.renameSync(replacement, file);
      }
      return result;
    });
    const reads = vi.spyOn(atomicWrite, "readFileRetrying");
    expect(directory.list({ fresh: true, includeStale: true }).find((p) => p.id === "p")?.name).toBe("before");
    // self() already captured the unchanged canonical bytes. Freshness validates physical
    // identity and reuses that snapshot, without another parse for the directory scan.
    expect(reads).toHaveBeenCalledTimes(0);
    expect(selections).toHaveBeenCalledTimes(4);
    const snapshot = selections.mock.calls[0]![1]!.snapshot;
    expect(snapshot).toBeDefined();
    expect(selections.mock.calls.every(([, options]) => options?.snapshot === snapshot)).toBe(true);
    expect(directory.list({ fresh: true, includeStale: true }).find((p) => p.id === "p")?.name).toBe("after");
    expect(reads).toHaveBeenCalledTimes(1); // The legacy replacement has no journal: one new parse.
    expect(selections.mock.calls[4]![1]!.snapshot).not.toBe(snapshot);
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
    const spy = vi.spyOn(atomicWrite, "readFileRetrying");
    const snapshot = mesh.stateToken({ fresh: true });
    for (const prefix of ["topology/hosts/", "topology/participants/", "topology/legacy/", "topology/actors/"]) {
      mesh.listAllShared(prefix, { snapshot });
    }
    expect(spy).toHaveBeenCalledTimes(1);
  });
});
