import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";
import { retryResidentWakes } from "../src/residency/wake.js";

// Keep: P2 security-review failure path. Round-robin maintenance of >32 meshes
// must not evict unfinished directory cursors and revisit pinned page one forever.
describe("resident wake fairness across active meshes", () => {
  it("advances every mesh past a pinned first page and closes native cursors with its store", async () => {
    const meshCount = 33;
    const rootsPerMesh = 129; // The smallest overflow backlog crossing the 128-entry page.
    const now = 1_700_000_000_000;
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-multi-mesh-"));
    const meshes: MeshStore[] = [];
    const visited = new Map<MeshStore, Set<string>>();
    const residentRoots = new Set<string>();
    const retainedFiles = new Map<string, string>();
    const nativeDirectories = new Set<fs.Dir>();
    const discoveryEntries: number[] = [];
    const discoveryReads: number[] = [];
    const claimRoots: number[] = [];
    const claimReads: number[] = [];
    let custody: { mesh: MeshStore; discovery: boolean; entries: number; reads: number; roots: Set<string> } | undefined;
    let activeMesh: MeshStore | undefined;
    let routeReadsInsideCustody = 0;
    let routeReadsOutsideCustody = 0;
    let custodyCalls = 0;
    let openedDirectories = 0;
    let closedDirectories = 0;
    let openBeforeCleanup = 0;
    let firstRound: number[] = [];
    let secondRound: number[] = [];
    const launch = vi.fn(async () => {});
    try {
      for (let m = 0; m < meshCount; m++) {
        const mesh = new MeshStore(path.join(fixture, `mesh-${m}`), 65_536, 100, { stateBackend: "file" });
        meshes.push(mesh);
        visited.set(mesh, new Set());
        const overflow = path.join(mesh.root, "wake-overflow");
        fs.mkdirSync(overflow);
        for (let n = 0; n < rootsPerMesh; n++) {
          const rootId = `session:fair-${m}-${n}`;
          const residencyRoot = path.join(mesh.root, "residency", `root-${n}`);
          const config = { rootId, residencyRoot, cwd: fixture };
          const delivery = { id: `pending-${m}-${n}`, sequence: n + 1 };
          const routes = JSON.stringify({ format: 1, rootId, hostId: `host:${m}:${n}`,
            configJson: canonicalResidentWakeConfig(config),
            actors: [{ id: `actor-${m}-${n}`, name: `listener-${n}`, topics: ["stuck.delivery"] }] });
          const pending = JSON.stringify({ format: 1, op: "pending", root: residencyRoot, delivery });
          const failure = JSON.stringify({ format: 1, delivery, nextRetryAt: now + 86_400_000 });
          fs.mkdirSync(residencyRoot, { recursive: true });
          fs.writeFileSync(path.join(residencyRoot, "config.json"), JSON.stringify(config));
          for (const [file, contents] of [
            [path.join(residencyRoot, "wake-routes.json"), routes],
            [path.join(residencyRoot, "wake-failure.json"), failure],
            [path.join(overflow, `root-${n}.json`), pending],
          ]) {
            fs.writeFileSync(file!, contents!);
            retainedFiles.set(file!, contents!);
          }
          residentRoots.add(residencyRoot);
        }
        const exclusive = mesh.exclusive.bind(mesh);
        vi.spyOn(mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
          const discovery = custodyCalls++ === 0;
          return exclusive(() => {
            const transaction = { mesh, discovery, entries: 0, reads: 0, roots: new Set<string>() };
            custody = transaction;
            try { return operation(); }
            finally {
              if (discovery) {
                discoveryEntries.push(transaction.entries);
                discoveryReads.push(transaction.reads);
              } else {
                claimRoots.push(transaction.roots.size);
                claimReads.push(transaction.reads);
              }
              custody = undefined;
            }
          }, timeout);
        });
      }

      // Observe actual native opens and real locked transactions, not candidate
      // mocks. Directory order is immaterial: every overflow entry must be visited.
      const open = fs.openSync;
      vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        const fd = (open as (...args: unknown[]) => number)(file, ...args);
        const at = String(file);
        const parent = path.dirname(at);
        if (activeMesh && residentRoots.has(parent) && path.dirname(parent) === path.join(activeMesh.root, "residency") &&
            path.basename(at) === "wake-routes.json") {
          visited.get(activeMesh)!.add(parent);
          if (custody) routeReadsInsideCustody++;
          else routeReadsOutsideCustody++;
        }
        if (custody) {
          if (parent === path.join(custody.mesh.root, "wake-overflow")) {
            custody.reads++;
            custody.roots.add(path.join(custody.mesh.root, "residency", path.basename(at, ".json")));
          } else if (residentRoots.has(parent)) {
            custody.roots.add(parent);
          }
        }
        return fd;
      }) as typeof fs.openSync);
      const opendir = fs.opendirSync;
      vi.spyOn(fs, "opendirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        const directory = (opendir as (...args: unknown[]) => fs.Dir)(file, ...args);
        nativeDirectories.add(directory);
        openedDirectories++;
        const read = directory.readSync.bind(directory);
        vi.spyOn(directory, "readSync").mockImplementation(() => {
          const entry = read();
          if (entry && custody) custody.entries++;
          return entry;
        });
        const close = directory.closeSync.bind(directory);
        vi.spyOn(directory, "closeSync").mockImplementation(() => {
          close();
          nativeDirectories.delete(directory);
          closedDirectories++;
        });
        return directory;
      }) as typeof fs.opendirSync);
      const retryForMesh = async (mesh: MeshStore): Promise<void> => {
        activeMesh = mesh;
        try { await retryResidentWakes(mesh, launch, now); }
        finally { activeMesh = undefined; }
      };

      // No draining/deleting page-one fixtures to manufacture progress. Every
      // receipt remains in backoff while the same 33 meshes alternate custody.
      for (let round = 0; round < 2; round++) {
        for (const mesh of meshes) {
          custodyCalls = 0;
          await retryForMesh(mesh);
        }
        const counts = meshes.map(mesh => visited.get(mesh)!.size);
        if (round === 0) firstRound = counts;
        else secondRound = counts;
      }
      // A further receipt-discovery page leaves native cursors unfinished, so
      // closeState must release them rather than relying on EOF or process exit.
      for (const mesh of meshes) {
        custodyCalls = 0;
        await retryForMesh(mesh);
      }
      openBeforeCleanup = nativeDirectories.size;
      expect(firstRound).toEqual(Array(meshCount).fill(128));
      expect(discoveryEntries).toHaveLength(meshCount * 3);
      expect(Math.max(...discoveryEntries)).toBeLessThanOrEqual(128);
      expect(Math.max(...discoveryReads)).toBeLessThanOrEqual(128);
      expect(claimRoots.length).toBeGreaterThan(0);
      expect(Math.max(...claimRoots)).toBeLessThanOrEqual(1);
      expect(Math.max(...claimReads)).toBeLessThanOrEqual(1);
      expect(routeReadsOutsideCustody, "retained route files are opened outside global mesh custody").toBeGreaterThan(0);
      expect(routeReadsInsideCustody, "retained route files are not opened under global mesh custody").toBe(0);
      expect(launch).not.toHaveBeenCalled();
      for (const [file, contents] of retainedFiles) expect(fs.readFileSync(file, "utf8")).toBe(contents);
    } finally {
      // Keep observation armed through the real store lifecycle cleanup.
      try { for (const mesh of meshes) mesh.closeState(); }
      finally {
        vi.restoreAllMocks();
        fs.rmSync(fixture, { recursive: true, force: true });
      }
    }
    console.info(JSON.stringify({ probe: "multi-mesh-wake-fairness", meshCount, rootsPerMesh,
      minVisitedAfterTwoRounds: Math.min(...secondRound), maxVisitedAfterTwoRounds: Math.max(...secondRound),
      maxDiscoveryEntries: Math.max(...discoveryEntries), maxDiscoveryReads: Math.max(...discoveryReads),
      maxClaimRoots: Math.max(...claimRoots), maxClaimReads: Math.max(...claimReads),
      openedDirectories, closedDirectories, openBeforeCleanup, openAfterCleanup: nativeDirectories.size }));
    expect(openBeforeCleanup, "exercise closeState with unfinished native directory cursors").toBeGreaterThan(0);
    expect(nativeDirectories.size, "mesh.closeState releases every observed native directory handle").toBe(0);
    expect(closedDirectories).toBe(openedDirectories);
    expect(secondRound, "every one of 33 meshes must reach its 129th pinned delivery in two rounds").toEqual(
      Array(meshCount).fill(rootsPerMesh));
  // Minimum fixture crossing both thresholds; Windows filesystem setup takes ~15-30 s on hosted runners, so budget 4x that.
  }, 120_000);
});
