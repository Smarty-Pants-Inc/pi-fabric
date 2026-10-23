import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";
import { retryResidentWakes } from "../src/residency/wake.js";

type NativeReaderSample = {
  kind: "discovery" | "observation" | "claim";
  meshRoot: string;
  root?: string;
  entries: number;
  files: string[];
};
const nativeReaderProbe = vi.hoisted(() => ({
  phase: undefined as NativeReaderSample | undefined,
  custodyDepth: 0,
  discovery: [] as NativeReaderSample[],
  observations: [] as NativeReaderSample[],
  claims: [] as NativeReaderSample[],
}));

// Instrument the real synchronous reader boundaries. Each wrapper delegates to
// the production implementation and records its native filesystem work.
vi.mock("../src/residency/wake-index.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/residency/wake-index.js")>();
  return {
    ...actual,
    residentWakeRetryCandidates: (meshRoot: string): string[] => {
      const previous = nativeReaderProbe.phase;
      const sample: NativeReaderSample = { kind: "discovery", meshRoot, entries: 0, files: [] };
      nativeReaderProbe.phase = sample;
      try { return actual.residentWakeRetryCandidates(meshRoot); }
      finally {
        nativeReaderProbe.discovery.push(sample);
        nativeReaderProbe.phase = previous;
      }
    },
    residentUnacknowledgedDelivery: (meshRoot: string, root: string) => {
      const kind = nativeReaderProbe.custodyDepth > 0 ? "claim" : "observation";
      const previous = nativeReaderProbe.phase;
      const sample: NativeReaderSample = { kind, meshRoot, root, entries: 0, files: [] };
      nativeReaderProbe.phase = sample;
      try { return actual.residentUnacknowledgedDelivery(meshRoot, root); }
      finally {
        (kind === "claim" ? nativeReaderProbe.claims : nativeReaderProbe.observations).push(sample);
        nativeReaderProbe.phase = previous;
      }
    },
  };
});

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
    const claimRoots: number[] = [];
    const claimReads: number[] = [];
    let custody: { mesh: MeshStore; reads: number; roots: Set<string> } | undefined;
    let activeMesh: MeshStore | undefined;
    let routeReadsInsideCustody = 0;
    let routeReadsOutsideCustody = 0;
    let openedDirectories = 0;
    let closedDirectories = 0;
    let openBeforeCleanup = 0;
    let firstRound: number[] = [];
    let secondRound: number[] = [];
    const launch = vi.fn(async () => {});
    nativeReaderProbe.phase = undefined;
    nativeReaderProbe.custodyDepth = 0;
    nativeReaderProbe.discovery.length = 0;
    nativeReaderProbe.observations.length = 0;
    nativeReaderProbe.claims.length = 0;
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
          return exclusive(() => {
            const previous = custody;
            const transaction = { mesh, reads: 0, roots: new Set<string>() };
            custody = transaction;
            nativeReaderProbe.custodyDepth++;
            try { return operation(); }
            finally {
              claimRoots.push(transaction.roots.size);
              claimReads.push(transaction.reads);
              nativeReaderProbe.custodyDepth--;
              custody = previous;
            }
          }, timeout);
        });
      }

      // Count actual native overflow reads and directory entries only while the
      // delegated production discovery/keyed-reader boundary is executing.
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
        const phase = nativeReaderProbe.phase;
        if (phase && parent === path.join(phase.meshRoot, "wake-overflow")) {
          phase.files.push(path.basename(at));
        }
        if (custody && parent === path.join(custody.mesh.root, "wake-overflow")) {
          custody.reads++;
          custody.roots.add(path.join(custody.mesh.root, "residency", path.basename(at, ".json")));
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
          if (entry && nativeReaderProbe.phase?.kind === "discovery") nativeReaderProbe.phase.entries++;
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
        for (const mesh of meshes) await retryForMesh(mesh);
        const counts = meshes.map(mesh => visited.get(mesh)!.size);
        if (round === 0) firstRound = counts;
        else secondRound = counts;
      }
      // A further receipt-discovery page leaves native cursors unfinished, so
      // closeState must release them rather than relying on EOF or process exit.
      for (const mesh of meshes) await retryForMesh(mesh);
      openBeforeCleanup = nativeDirectories.size;

      const discoveryEntries = nativeReaderProbe.discovery.map(sample => sample.entries);
      const discoveryReads = nativeReaderProbe.discovery.map(sample => sample.files.length);
      const observationReads = nativeReaderProbe.observations.map(sample => sample.files.length);
      const claimBoundaryReads = nativeReaderProbe.claims.map(sample => sample.files.length);
      const maxOrZero = (values: number[]): number => values.length ? Math.max(...values) : 0;
      expect(firstRound).toEqual(Array(meshCount).fill(128));
      expect(secondRound).toEqual(Array(meshCount).fill(rootsPerMesh));
      expect(nativeReaderProbe.discovery).toHaveLength(meshCount * 3);
      expect(Math.max(...discoveryEntries)).toBeLessThanOrEqual(128);
      expect(Math.max(...discoveryReads)).toBeLessThanOrEqual(128);
      expect(discoveryEntries.some(entries => entries > 0), "native bounded discovery reads directory entries").toBe(true);
      expect(discoveryReads.some(reads => reads > 0), "native bounded discovery reads overflow files").toBe(true);
      expect(nativeReaderProbe.observations.length, "keyed observation boundaries execute real reads").toBeGreaterThan(0);
      expect(observationReads.reduce((sum, reads) => sum + reads, 0), "keyed observations open native overflow files").toBeGreaterThan(0);
      for (const sample of [...nativeReaderProbe.observations, ...nativeReaderProbe.claims]) {
        expect(sample.root).toBeDefined();
        expect(sample.files.length, `${sample.kind} reads at most one keyed overflow file`).toBeLessThanOrEqual(1);
        expect(sample.files.every(file => file === `${path.basename(sample.root!)}.json`),
          `${sample.kind} reads only its requested root`).toBe(true);
      }
      expect(claimBoundaryReads.every(reads => reads <= 1), "each actual keyed claim reads at most one overflow file").toBe(true);
      expect(maxOrZero(claimRoots)).toBeLessThanOrEqual(1);
      expect(maxOrZero(claimReads)).toBeLessThanOrEqual(1);
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
    const discoveryEntries = nativeReaderProbe.discovery.map(sample => sample.entries);
    const discoveryReads = nativeReaderProbe.discovery.map(sample => sample.files.length);
    const observationReads = nativeReaderProbe.observations.map(sample => sample.files.length);
    const claimBoundaryReads = nativeReaderProbe.claims.map(sample => sample.files.length);
    const maxOrZero = (values: number[]): number => values.length ? Math.max(...values) : 0;
    console.info(JSON.stringify({ probe: "multi-mesh-wake-fairness", meshCount, rootsPerMesh,
      minVisitedAfterTwoRounds: Math.min(...secondRound), maxVisitedAfterTwoRounds: Math.max(...secondRound),
      nativeDiscoveryCalls: nativeReaderProbe.discovery.length,
      maxDiscoveryEntries: maxOrZero(discoveryEntries), maxDiscoveryReads: maxOrZero(discoveryReads),
      nativeKeyedObservationCalls: nativeReaderProbe.observations.length,
      nativeKeyedObservationReads: observationReads.reduce((sum, reads) => sum + reads, 0),
      nativeKeyedClaimCalls: nativeReaderProbe.claims.length, maxClaimBoundaryReads: maxOrZero(claimBoundaryReads),
      maxClaimRootsPerExclusive: maxOrZero(claimRoots), maxClaimReadsPerExclusive: maxOrZero(claimReads),
      openedDirectories, closedDirectories, openBeforeCleanup, openAfterCleanup: nativeDirectories.size }));
    expect(openBeforeCleanup, "exercise closeState with unfinished native directory cursors").toBeGreaterThan(0);
    expect(openedDirectories, "one recovery and one residency cursor per mesh").toBe(meshCount * 2);
    expect(nativeDirectories.size, "mesh.closeState releases every observed native directory handle").toBe(0);
    expect(closedDirectories, "all 66 native cursors close").toBe(meshCount * 2);
    expect(closedDirectories).toBe(openedDirectories);
    expect(secondRound, "every one of 33 meshes must reach its 129th pinned delivery in two rounds").toEqual(
      Array(meshCount).fill(rootsPerMesh));
  // Minimum fixture crossing both thresholds; Windows filesystem setup takes ~15-30 s on hosted runners, so budget 4x that.
  }, 120_000);
});
