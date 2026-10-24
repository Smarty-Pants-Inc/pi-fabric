import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";
import * as terminal from "../src/residency/wake-terminal.js";
import * as wake from "../src/residency/wake.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { LEGACY_ROOT_LEASE_MS } from "../src/topology/legacy-root-liveness.js";
import { mainPublicationFenced, mainPublicationFencePath } from "../src/topology/main-publication-fence.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// Keep: durable-delivery safety and the observed live-Main renewal-lapse incident
// Real append/archive/retry, Main lease CAS and publication;
// only the final resident-launch boundary and wall clock are injected.
// The frozen baseline has no grace export. This namespace-only fallback lets it
// run the SAME behavioral checks instead of failing module loading on an import.
const MIN_GRACE_MS = (terminal as { RESIDENT_WAKE_TERMINAL_MIN_GRACE_MS?: number })
  .RESIDENT_WAKE_TERMINAL_MIN_GRACE_MS ?? 60_000;
const graceFor = (ttl: number) => Math.max(MIN_GRACE_MS, 3 * ttl);
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});

async function fixture() {
  // Allocation and every product/fixture cleanup stay inside this run's TMPDIR.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-grace-"));
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 65_536, 100, { canWakeResidents: () => false });
  cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  const rootId = "session:wake-grace";
  const main = { id: rootId, name: "Main", kind: "main" as const, sessionId: "wake-grace" };
  const config: ResidentHostConfig = { format: 1, rootId, sessionId: main.sessionId, cwd: root,
    projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
  const directory = config.residencyRoot;
  const configFile = path.join(directory, "config.json");
  const terminalFile = path.join(directory, "wake-terminal.json"), failureFile = path.join(directory, "wake-failure.json");
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(configFile, JSON.stringify(config));
  fs.writeFileSync(path.join(directory, "wake-routes.json"), JSON.stringify({ format: 1, rootId, hostId: "host:resident",
    configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "actor:grace-listener", name: "listener", topics: ["grace.delivery"] }] }));
  await wake.ensureResidentWakeArchive(mesh);
  const observer = new ParticipantDirectory(mesh, { enabled: true, hostId: "host:resident", rootId: "host:resident",
    identity: { id: "host:resident", name: "observer", kind: "agent" }, reapDeadHosts: false });
  const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {});
  return { root, mesh, meshRoot, config, directory, configFile, terminalFile, failureFile, main, launch,
    get now() { return now; }, at(value: number) { now = value; },
    pending: () => residentUnacknowledgedDeliveries(meshRoot).get(directory),
    participant: () => observer.get(rootId, now, { fresh: true }),
    publish: () => mesh.publish({ topic: "grace.delivery", from: { id: "publisher", name: "publisher", kind: "main" } }),
    drift() { fs.writeFileSync(configFile, JSON.stringify({ ...config, cwd: path.join(root, "changed") })); },
    repair() { fs.writeFileSync(configFile, JSON.stringify(config)); },
    sweep: () => wake.retryResidentWakes(mesh, launch, now),
  };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

/** Copied minimally from the read-only nativeMain fixture. refresh() is the
 * directory's explicit flush API: it awaits real lease CAS AND presence commit.
 * No fabricated lease JSON stands in for native activation/delayed recovery. */
async function nativeMain(f: Fixture, leaseMs?: number) {
  const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.config.rootId,
    rootId: f.config.rootId, identity: f.main, reapDeadHosts: false, ...(leaseMs !== undefined ? { leaseMs } : {}) });
  cleanups.push(() => directory.close());
  directory.registerSource(() => [{ format: 1, id: f.config.rootId, rootId: f.config.rootId,
    ownerHostId: f.config.rootId, ownerIdentityId: f.config.rootId, ownerIncarnation: directory.ownerIncarnation,
    kind: "root", name: "Main", status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"],
    controlProtocol: "v1", sessionId: f.config.sessionId, cwd: f.root, startedAt: f.now - 1_000, updatedAt: f.now }]);
  await directory.start();
  directory.suspend();
  await directory.refresh(); // Drain/flush startup before advancing the deterministic clock.
  expect(directory.heartbeatArmed).toBe(false);
  const lease = readHostLeaseCurrent(f.meshRoot, f.config.rootId)!;
  expect(lease.incarnationToken).toEqual(expect.any(String));
  expect(lease.expiresAt - lease.updatedAt).toBe(leaseMs ?? LEGACY_ROOT_LEASE_MS);
  expect(f.participant()).toMatchObject({ id: f.config.rootId, kind: "root", stale: false });
  return { directory, lease, flush: () => directory.refresh() };
}

async function recover(f: Fixture, main: Awaited<ReturnType<typeof nativeMain>>, delivery: { id: string; sequence: number }) {
  const failure = wake.readWakeJson<{ nextRetryAt?: number }>(f.failureFile);
  f.at(Math.max(f.now + 1_001, failure?.nextRetryAt ?? 0));
  await main.flush(); // A lapsed Main retains the same UUID and can genuinely renew/publish.
  const renewed = readHostLeaseCurrent(f.meshRoot, f.config.rootId)!;
  expect.soft(renewed.incarnationToken).toBe(main.lease.incarnationToken);
  expect.soft(renewed.updatedAt).toBeGreaterThan(main.lease.updatedAt);
  expect.soft(renewed.expiresAt).toBeGreaterThan(f.now);
  expect.soft(f.participant()).toMatchObject({ id: f.config.rootId, kind: "root", stale: false });
  f.repair();
  // The bounded retry cursor can yield an empty EOF sweep, not one claim per call.
  for (let n = 0; n < 4; n++) await f.sweep();
  expect.soft(f.launch).toHaveBeenCalledOnce();
  expect.soft(wake.readWakeJson(wake.residentWakeRequestPath(f.directory))).toMatchObject(delivery);
  expect.soft(f.pending()).toBeUndefined();
  expect.soft(fs.existsSync(f.failureFile)).toBe(false);
  expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
}

describe("resident terminal disposition requires several native lease TTLs of grace", () => {
  it.each([undefined, 40_000])("keeps a native Main retryable after >TTL renewal delay with leaseMs=%s, then recovers", async leaseMs => {
    const f = await fixture();
    const event = await f.publish();
    const delivery = { id: event.id, sequence: event.sequence };
    const main = await nativeMain(f, leaseMs);
    const ttl = main.lease.expiresAt - main.lease.updatedAt;
    f.at(main.lease.expiresAt + ttl + 1); // More than TWO TTLs since its last renewal, still within grace.
    expect(f.now - main.lease.expiresAt).toBeLessThan(graceFor(ttl));
    expect(f.participant()).toBeUndefined(); // Routing lapse is real, but it is NOT terminal death.
    f.drift();
    const custody = vi.spyOn(f.mesh, "leaseCustody");
    await f.sweep();
    console.info(JSON.stringify({ probe: "native-main-pregrace", ttl, minGraceMs: MIN_GRACE_MS,
      lapseMs: f.now - main.lease.expiresAt, graceMs: graceFor(ttl), pending: f.pending(),
      terminal: fs.existsSync(f.terminalFile) }));
    expect.soft(custody, "pregrace refusal must not fence the delayed native writer").not.toHaveBeenCalled();
    expect.soft(fs.existsSync(mainPublicationFencePath(f.meshRoot, f.config.rootId))).toBe(false);
    expect.soft(f.pending()).toEqual(delivery);
    expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
    expect.soft(wake.readWakeJson(f.failureFile)).toMatchObject({ delivery, failures: 1,
      error: expect.stringContaining("ResidentWakeConfigMismatch") });
    expect.soft(f.launch).not.toHaveBeenCalled();
    await recover(f, main, delivery);
  });

  it.each([undefined, 40_000])("terminalizes a native Main only PAST expiry+max(60s,3*TTL), leaseMs=%s", async leaseMs => {
    const f = await fixture();
    const event = await f.publish();
    const main = await nativeMain(f, leaseMs);
    const ttl = main.lease.expiresAt - main.lease.updatedAt;
    f.drift();
    f.at(main.lease.expiresAt + graceFor(ttl));
    expect.soft(terminal.residentWakeTerminalRoot(f.mesh, f.directory), "the exact grace boundary still retains work").toBeUndefined();
    f.at(f.now + 1);
    await f.sweep();
    console.info(JSON.stringify({ probe: "native-main-past-grace", ttl, graceMs: graceFor(ttl),
      lapseMs: f.now - main.lease.expiresAt, terminal: fs.existsSync(f.terminalFile) }));
    expect.soft(wake.readWakeJson(f.terminalFile)).toMatchObject({ format: 1, rootId: f.config.rootId,
      delivery: { id: event.id, sequence: event.sequence }, code: "RESIDENT_WAKE_CONFIG_MISMATCH", failedAt: f.now });
    expect.soft(f.pending()).toBeUndefined();
    expect.soft(fs.existsSync(f.failureFile)).toBe(false);
    expect.soft(f.launch).not.toHaveBeenCalled();
    expect.soft(mainPublicationFenced(f.meshRoot, f.config.rootId)).toBe(false);
  });

  it("rechecks grace under BOTH native fences when renewal wins and then lapses again before custody", async () => {
    const f = await fixture();
    const event = await f.publish();
    const delivery = { id: event.id, sequence: event.sequence };
    const main = await nativeMain(f);
    const ttl = main.lease.expiresAt - main.lease.updatedAt;
    f.at(main.lease.expiresAt + graceFor(ttl) + 1);
    f.drift();
    expect(terminal.residentWakeTerminalRoot(f.mesh, f.directory)).toBe(f.config.rootId);
    const leaseCustody = f.mesh.leaseCustody.bind(f.mesh);
    let raced = false;
    vi.spyOn(f.mesh, "leaseCustody").mockImplementation(async <T>(file: string, operation: () => T | Promise<T>, timeout?: number,
      options?: { ownIncarnation?: string | undefined }): Promise<T> => {
      if (!raced && file === hostLeasePath(f.meshRoot, f.config.rootId)) {
        raced = true;
        expect(mainPublicationFenced(f.meshRoot, f.config.rootId)).toBe(true);
        // Actual native CAS in the SAME lease gate before terminal custody. The
        // prefilter's old expiry must not survive this new renewal version.
        await renewHostLease(f.mesh, { ...main.lease, updatedAt: f.now, expiresAt: f.now + ttl });
        f.at(f.now + ttl + 1); // New lease is expired again, but late by only ONE ms.
        expect(f.participant()).toBeUndefined();
      }
      return leaseCustody(file, operation, timeout, options);
    });
    await f.sweep();
    expect.soft(raced).toBe(true);
    expect.soft(f.pending()).toEqual(delivery);
    expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
    expect.soft(mainPublicationFenced(f.meshRoot, f.config.rootId)).toBe(false);
    expect.soft(wake.readWakeJson(f.failureFile)).toMatchObject({ delivery, failures: 1 });
    await recover(f, main, delivery);
  });

  it.each(["never-written", "native-clean-close"] as const)("retains a missing lease without expiry evidence: %s", async mode => {
    const f = await fixture();
    const event = await f.publish();
    if (mode === "native-clean-close") {
      const main = await nativeMain(f);
      await main.directory.close();
    }
    expect(fs.existsSync(hostLeasePath(f.meshRoot, f.config.rootId))).toBe(false);
    f.at(f.now + 60 * 60_000);
    f.drift();
    await f.sweep();
    expect.soft(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
    expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
    expect.soft(f.launch).not.toHaveBeenCalled();
  });

  it.each(["zero-ttl", "negative-ttl", "negative-update", "fractional-update", "fractional-expiry", "overflow-deadline"] as const)(
    "cannot use invalid native timing to authorize terminalization: %s", async mode => {
      const f = await fixture();
      const event = await f.publish();
      const main = await nativeMain(f);
      f.at(main.lease.expiresAt + 60 * 60_000);
      const invalid = { ...main.lease };
      if (mode === "zero-ttl") invalid.updatedAt = invalid.expiresAt;
      else if (mode === "negative-ttl") invalid.updatedAt = invalid.expiresAt + 1;
      else if (mode === "negative-update") invalid.updatedAt = -1;
      else if (mode === "fractional-update") invalid.updatedAt += 0.5;
      else if (mode === "fractional-expiry") invalid.expiresAt += 0.5;
      else {
        f.at(Number.MAX_SAFE_INTEGER);
        invalid.expiresAt = Number.MAX_SAFE_INTEGER - 1_000;
        invalid.updatedAt = invalid.expiresAt - LEGACY_ROOT_LEASE_MS;
      }
      // Fault injection ONLY: primary and control tests above use the native writer.
      writeHostLease(f.meshRoot, invalid);
      f.drift();
      await f.sweep();
      expect.soft(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
      expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
      expect.soft(f.launch).not.toHaveBeenCalled();
    });
});
