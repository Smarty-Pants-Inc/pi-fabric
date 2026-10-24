import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries, residentWakeIndexPath } from "../src/residency/wake-index.js";
import * as terminal from "../src/residency/wake-terminal.js";
import * as wake from "../src/residency/wake.js";
import { hostLeasePath, readHostLeaseCurrent } from "../src/topology/host-leases.js";
import { mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const HOUR_MS = 60 * 60_000;
const ROOT_LEASE_TTL_MS = 15_000;
const BACKOFF_STEP_MS = 5 * 60_000;
const publisher = { id: "publisher:wake-provenance", name: "publisher", kind: "main" as const };
const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});

async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-mirror-provenance-"));
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const meshRoot = path.join(root, "mesh");
  let allowed = false;
  const mesh = new MeshStore(meshRoot, 65_536, 100, { stateBackend: "sqlite", canWakeResidents: () => allowed });
  const rootId = `session:wake-provenance-${randomUUID()}`;
  const sessionId = rootId.slice("session:".length);
  const main = { id: rootId, name: "Main", kind: "main" as const, sessionId };
  const config: ResidentHostConfig = { format: 1, rootId, sessionId, cwd: root,
    projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
  const directory = config.residencyRoot;
  const configFile = path.join(directory, "config.json");
  const routesFile = path.join(directory, "wake-routes.json");
  const failureFile = path.join(directory, "wake-failure.json");
  const terminalFile = path.join(directory, "wake-terminal.json");
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(configFile, JSON.stringify(config));
  fs.writeFileSync(routesFile, JSON.stringify({ format: 1, rootId, hostId: "host:wake-provenance",
    configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "actor:wake-provenance", name: "listener", topics: ["wake.provenance"] }] }));
  await wake.ensureResidentWakeArchive(mesh);
  await mesh.put({ key: "wake-provenance/sqlite-proof", identity: main, value: { backend: "sqlite" } });
  const event = await mesh.publish({ topic: "wake.provenance", from: publisher });
  const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {});
  // Keep the real retry implementation and MeshStore entry point; inject only the final launch boundary.
  const retry = wake.retryResidentWakes;
  vi.spyOn(wake, "retryResidentWakes").mockImplementation((store, _launch, at, canWake) =>
    retry(store, launch, at, canWake));
  cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  return { root, mesh, meshRoot, rootId, config, directory, configFile, failureFile, terminalFile,
    main, event, launch,
    get now() { return now; },
    advanceBackoff() { now += BACKOFF_STEP_MS; },
    pastGrace(expiresAt: number) { now = expiresAt + 5 * HOUR_MS; },
    pending: () => residentUnacknowledgedDeliveries(meshRoot).get(directory),
    async sweep() { allowed = true; await mesh.retryResidentWakes(); },
    drift() { fs.writeFileSync(configFile, JSON.stringify({ ...config, cwd: path.join(root, "changed") })); },
  };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

/** A real Main writer creates and commits the lease and presence before tests advance time. */
async function nativeMain(f: Fixture) {
  const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.rootId, rootId: f.rootId,
    identity: f.main, reapDeadHosts: false });
  cleanups.push(() => directory.close());
  directory.registerSource(() => [{ format: 1, id: f.rootId, rootId: f.rootId,
    ownerHostId: f.rootId, ownerIdentityId: f.rootId, ownerIncarnation: directory.ownerIncarnation,
    kind: "root", name: "Main", status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"],
    controlProtocol: "v1", sessionId: f.config.sessionId, cwd: f.root, startedAt: f.now - 1_000, updatedAt: f.now }]);
  await directory.start();
  directory.suspend();
  await directory.refresh();
  const lease = readHostLeaseCurrent(f.meshRoot, f.rootId);
  if (!lease) throw new Error("Native Main did not commit its host lease");
  return { directory, lease };
}

function injectRawMirrorLease(f: Fixture, value: string | number | boolean | null): void {
  const file = hostLeasePath(f.meshRoot, f.rootId);
  const raw = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
  // Change only the raw provenance discriminator; all lease identity and timing remain native.
  fs.writeFileSync(file, JSON.stringify({ ...raw, mirrorLease: value }));
}

function exactAckCount(f: Fixture): number {
  const file = residentWakeIndexPath(f.meshRoot);
  if (!fs.existsSync(file)) return 0;
  return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as {
    op?: string; root?: string; delivery?: { id?: string; sequence?: number };
  }).filter(row => row.op === "ack" && row.root === f.directory &&
    row.delivery?.id === f.event.id && row.delivery?.sequence === f.event.sequence).length;
}

function expectNativeSqlitePublication(f: Fixture): void {
  expect(f.mesh.stateBackend).toBe("sqlite");
  expect(fs.existsSync(path.join(f.meshRoot, "state.db"))).toBe(true);
  const archive = MeshArchive.fromRoot(f.meshRoot);
  expect(archive?.lookupEntry(f.event.sequence)).toMatchObject({ committed: true });
  expect(f.pending()).toEqual({ id: f.event.id, sequence: f.event.sequence });
}

const malformedMarkers = [
  ["string-1", "1"],
  ["number-2", 2],
  ["false", false],
  ["null", null],
] as const;

describe("resident wake native lease provenance", () => {
  it.each(malformedMarkers)("retains retry ownership for malformed raw mirrorLease %s", async (_label, marker) => {
    const f = await fixture();
    expectNativeSqlitePublication(f);
    const main = await nativeMain(f);
    const leaseFile = hostLeasePath(f.meshRoot, f.rootId);
    const ttl = main.lease.expiresAt - main.lease.updatedAt;
    expect(ttl).toBe(ROOT_LEASE_TTL_MS);
    f.pastGrace(main.lease.expiresAt);
    f.drift();
    injectRawMirrorLease(f, marker);

    const raw = JSON.parse(fs.readFileSync(leaseFile, "utf8")) as Record<string, unknown>;
    expect(raw.mirrorLease).toBe(marker);
    const parsed = readHostLeaseCurrent(f.meshRoot, f.rootId);
    expect(parsed).toMatchObject({ id: f.rootId, rootId: f.rootId, identityId: f.rootId,
      updatedAt: main.lease.updatedAt, expiresAt: main.lease.expiresAt });
    expect(parsed).not.toHaveProperty("mirrorLease"); // The shared lease reader deliberately sanitizes these raw values.
    expect(parsed).not.toHaveProperty("mirror");
    expect(f.now - main.lease.expiresAt).toBe(5 * HOUR_MS);

    const failures: number[] = [];
    for (let sweep = 0; sweep < 4; sweep++) {
      await f.sweep();
      failures.push(wake.readWakeJson<{ failures?: number }>(f.failureFile)?.failures ?? 0);
      if (sweep < 3) f.advanceBackoff();
    }
    const delivery = { id: f.event.id, sequence: f.event.sequence };
    expect(failures).toEqual([1, 2, 3, 4]);
    expect(f.pending()).toEqual(delivery); // Retry receipt still owns the exact committed nudge.
    expect(wake.readWakeJson<{ delivery?: unknown; failures?: number; error?: string }>(f.failureFile))
      .toMatchObject({ delivery, failures: 4, error: expect.stringContaining("ResidentWakeConfigMismatch") });
    expect(fs.existsSync(f.terminalFile)).toBe(false);
    expect(exactAckCount(f)).toBe(0);
    expect(f.launch).not.toHaveBeenCalled();
  });

  it("retains the valid mirrorLease: 1 projection as non-terminal", async () => {
    const f = await fixture();
    expectNativeSqlitePublication(f);
    const main = await nativeMain(f);
    expect(main.lease.expiresAt - main.lease.updatedAt).toBe(ROOT_LEASE_TTL_MS);
    f.pastGrace(main.lease.expiresAt);
    f.drift();
    injectRawMirrorLease(f, 1);
    expect(readHostLeaseCurrent(f.meshRoot, f.rootId)).toHaveProperty("mirrorLease", 1);

    const failures: number[] = [];
    for (let sweep = 0; sweep < 4; sweep++) {
      await f.sweep();
      failures.push(wake.readWakeJson<{ failures?: number }>(f.failureFile)?.failures ?? 0);
      if (sweep < 3) f.advanceBackoff();
    }
    const delivery = { id: f.event.id, sequence: f.event.sequence };
    expect(failures).toEqual([1, 2, 3, 4]);
    expect(f.pending()).toEqual(delivery);
    expect(wake.readWakeJson<{ delivery?: unknown; failures?: number; error?: string }>(f.failureFile))
      .toMatchObject({ delivery, failures: 4, error: expect.stringContaining("ResidentWakeConfigMismatch") });
    expect(fs.existsSync(f.terminalFile)).toBe(false);
    expect(exactAckCount(f)).toBe(0);
    expect(f.launch).not.toHaveBeenCalled();
  });

  it("terminalizes once for a genuinely native marker-absent lease and remains idle", async () => {
    const f = await fixture();
    expectNativeSqlitePublication(f);
    const main = await nativeMain(f);
    expect(main.lease.expiresAt - main.lease.updatedAt).toBe(ROOT_LEASE_TTL_MS);
    const raw = JSON.parse(fs.readFileSync(hostLeasePath(f.meshRoot, f.rootId), "utf8")) as Record<string, unknown>;
    expect(raw).not.toHaveProperty("mirrorLease");
    f.pastGrace(main.lease.expiresAt);
    f.drift();

    await f.sweep();
    const terminalBytes = fs.readFileSync(f.terminalFile, "utf8");
    expect(wake.readWakeJson<{ format?: number; rootId?: string; delivery?: unknown; code?: string }>(f.terminalFile))
      .toMatchObject({ format: 1, rootId: f.rootId,
        delivery: { id: f.event.id, sequence: f.event.sequence }, code: "RESIDENT_WAKE_CONFIG_MISMATCH" });
    expect(f.pending()).toBeUndefined();
    expect(fs.existsSync(f.failureFile)).toBe(false);
    expect(exactAckCount(f)).toBe(1);
    expect(f.launch).not.toHaveBeenCalled();

    for (let sweep = 0; sweep < 3; sweep++) {
      f.advanceBackoff();
      await f.sweep();
    }
    expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
    expect(f.pending()).toBeUndefined();
    expect(fs.existsSync(f.failureFile)).toBe(false);
    expect(exactAckCount(f)).toBe(1);
    expect(f.launch).not.toHaveBeenCalled();
  });

  it("rechecks malformed raw provenance under both native fences after a valid prefilter", async () => {
    const f = await fixture();
    expectNativeSqlitePublication(f);
    const main = await nativeMain(f);
    expect(main.lease.expiresAt - main.lease.updatedAt).toBe(ROOT_LEASE_TTL_MS);
    f.pastGrace(main.lease.expiresAt);
    f.drift();
    expect(terminal.residentWakeTerminalRoot(f.mesh, f.directory)).toBe(f.rootId);

    const leaseFile = hostLeasePath(f.meshRoot, f.rootId);
    const originalCustody = f.mesh.leaseCustody.bind(f.mesh);
    let injected = false;
    let checkedInsideNativeLeaseCustody = false;
    let publicationFenceHeldAtRecheck = false;
    let rawMarkerAtRecheck: unknown;
    vi.spyOn(f.mesh, "leaseCustody").mockImplementation(async <T>(file: string, operation: () => T | Promise<T>,
      timeout?: number, options?: { ownIncarnation?: string | undefined }): Promise<T> => {
      if (file === leaseFile && !injected) {
        // The normal prefilter just accepted the native, marker-absent lease; change only
        // its raw discriminator before the actual native lease-custody callback begins.
        expect(mainPublicationFenced(f.meshRoot, f.rootId)).toBe(true);
        injectRawMirrorLease(f, 2);
        injected = true;
      }
      return originalCustody(file, async () => {
        if (file === leaseFile) {
          checkedInsideNativeLeaseCustody = true; // This callback is executing under the real host lease gate.
          publicationFenceHeldAtRecheck = mainPublicationFenced(f.meshRoot, f.rootId);
          rawMarkerAtRecheck = (JSON.parse(fs.readFileSync(leaseFile, "utf8")) as Record<string, unknown>).mirrorLease;
        }
        return await operation();
      }, timeout, options);
    });

    await f.sweep();
    const delivery = { id: f.event.id, sequence: f.event.sequence };
    expect(injected).toBe(true);
    expect(checkedInsideNativeLeaseCustody).toBe(true);
    expect(publicationFenceHeldAtRecheck).toBe(true);
    expect(rawMarkerAtRecheck).toBe(2);
    expect(f.pending()).toEqual(delivery);
    expect(wake.readWakeJson<{ delivery?: unknown; failures?: number; error?: string }>(f.failureFile))
      .toMatchObject({ delivery, failures: 1, error: expect.stringContaining("ResidentWakeConfigMismatch") });
    expect(fs.existsSync(f.terminalFile)).toBe(false);
    expect(exactAckCount(f)).toBe(0);
    expect(f.launch).not.toHaveBeenCalled();
    expect(mainPublicationFenced(f.meshRoot, f.rootId)).toBe(false);
  });
});
