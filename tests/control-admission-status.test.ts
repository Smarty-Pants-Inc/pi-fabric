import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentManager } from "../src/agents/manager.js";
import type { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { diagnostics } from "../src/core/diagnostics.js";
import type { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import { ParticipantDirectory, type ParticipantDirectoryOptions } from "../src/topology/participant-directory.js";
import { kvWorld } from "./helpers/bridge-nats-kv.js";
import { takeMainPublicationFence } from "../src/topology/main-publication-fence.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: smarty-dev#17018's recorded silent control-inbox stall and the
// admission trust boundary. Use real directory failure/renewal and public status,
// not a canned canConsumeMesh result. No worker, result authentication bypass,
// production fleet root, new polling interval, or result-authentication bypass.
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const ROOT = "session:4ddfc58b-d782-480d-88b7-51a7cbbd61ec";
const fixture = (options: Partial<ParticipantDirectoryOptions> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-control-admission-"));
  const identity: MeshIdentity = { id: ROOT, name: "Main", kind: "main" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity,
    heartbeatMs: 60_000, leaseMs: 180_000, reapDeadHosts: false, random: () => 0.5, ...options,
  });
  directories.push(directory);
  let failure: Error | undefined;
  directory.registerSource((): FabricParticipantRecord[] => {
    if (failure) throw failure;
    return [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main",
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
      startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }];
  });
  const pi = { getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, identity.id, true, root);
  const extensionContext = { isIdle: () => true, hasPendingMessages: () => false } as ExtensionContext;
  // Unused manager methods intentionally absent: Main diagnostics must not read
  // or authenticate any task result, even when its source is the refresh failure.
  const provider = new AgentsProvider({ cwd: root } as AgentManager,
    { identity } as ActorManager, new GlobalActorRegistry(root, 64 * 1024),
    main, directory, undefined, {} as LifecycleBroker);
  const invocation = { extensionContext, signal: new AbortController().signal,
    cwd: root, callId: "control-admission-status", audits: [], update: () => {} } as unknown as FabricInvocationContext;
  const status = (id = "main") => provider.invoke("status", { id }, invocation);
  const alarms = () => mesh.read({ topic: "ops.owner", limit: 100 })
    .filter(event => event.kind === "fabric.control-admission.alarm");
  return { directory, mesh, identity, status, alarms, fail: (error?: Error) => { failure = error; } };
};

describe("Main agents.status exposes effective control admission (#17018)", () => {
  it("names the unconfirmed gate even while Main looks idle", async () => {
    const s = fixture();
    expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.status()).resolves.toMatchObject({ kind: "main", status: "idle",
      controlAdmission: "gated", controlAdmissionReason: expect.stringMatching(/confirm|publish|initial/i) });
  });

  it("exposes the actual refresh failure by alias and exact Main id despite a live host lease", async () => {
    const s = fixture();
    await s.directory.refresh();
    expect(s.directory.canConsumeMesh()).toBe(true);
    const failure = new Error("FABRIC_MISSING_RESULT: completed run has no saved terminal result");
    s.fail(failure);
    await expect(s.directory.refresh()).rejects.toBe(failure);
    expect(readHostLeases(s.mesh.root).get(s.identity.id)?.expiresAt).toBeGreaterThan(Date.now());
    expect(s.directory.canConsumeMesh()).toBe(false);
    for (const id of ["main", s.identity.id]) {
      await expect(s.status(id)).resolves.toMatchObject({ status: "idle", controlAdmission: "gated",
        controlAdmissionReason: expect.stringContaining(failure.message) });
    }
  });

  it("does not turn routing-read success into admission, and clears the gate only on a real refresh", async () => {
    const s = fixture();
    await s.directory.refresh();
    s.fail(new Error("snapshot unavailable"));
    await expect(s.directory.refresh()).rejects.toThrow("snapshot unavailable");
    const confirmed = s.directory.confirmedAt();
    await s.directory.refreshRoutingView();
    expect(s.directory.routingUnavailable()).toBeUndefined();
    expect(s.directory.confirmedAt()).toBe(confirmed);
    expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "gated",
      controlAdmissionReason: expect.stringContaining("snapshot unavailable") });
    s.fail();
    await s.directory.refresh();
    expect(s.directory.canConsumeMesh()).toBe(true);
    const recovered = await s.status();
    expect(recovered).not.toHaveProperty("controlAdmission", "gated");
    expect(recovered).not.toHaveProperty("controlAdmissionReason");
  });

  it("reports overdue and closed admission without disguising either as a source error", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const s = fixture();
    await s.directory.refresh();
    vi.setSystemTime(s.directory.confirmedAt() + 120_000);
    expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "gated",
      controlAdmissionReason: expect.stringMatching(/overdue|expired/i) });
    await s.directory.close();
    expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "gated",
      controlAdmissionReason: expect.stringMatching(/closed/i) });
  });

  it("does not report a healthy idle Main as gated", async () => {
    const s = fixture();
    await s.directory.refresh();
    expect(s.directory.canConsumeMesh()).toBe(true);
    const status = await s.status();
    expect(status).toMatchObject({ kind: "main", status: "idle" });
    expect(status).not.toHaveProperty("controlAdmission", "gated");
    expect(status).not.toHaveProperty("controlAdmissionReason");
  });
});

// Capture and invoke the real heartbeat timeout, just as participant-directory's
// outage tests do. Wall time is controlled; all mesh reads/writes and source
// failures remain real. The production heartbeat is still the sole alarm driver.
const heartbeatFixture = async (options: Partial<ParticipantDirectoryOptions> = {}) => {
  vi.useFakeTimers({ toFake: ["Date"] });
  const s = fixture(options);
  const timeouts = vi.spyOn(globalThis, "setTimeout");
  await s.directory.start();
  const index = timeouts.mock.calls.findIndex(([, delay]) => delay === 60_000);
  expect(index).toBeGreaterThanOrEqual(0);
  const heartbeat = timeouts.mock.calls[index]![0] as () => void;
  clearTimeout(timeouts.mock.results[index]!.value);
  const runs = vi.spyOn(MeshBackgroundRetry.prototype, "run");
  const tick = async (now: number) => {
    vi.setSystemTime(now);
    const before = timeouts.mock.calls.length;
    const runBefore = runs.mock.results.length;
    heartbeat();
    const next = timeouts.mock.calls.findIndex(([, delay], i) => i >= before && delay === 60_000);
    if (next >= 0) clearTimeout(timeouts.mock.results[next]!.value);
    await Promise.all(runs.mock.results.slice(runBefore).map(result => result.value));
  };
  return { ...s, tick };
};

describe("host control-admission alarm on the existing heartbeat (#17018)", () => {
  it("alarms only after >60s gated, once per episode, and rearms after genuine recovery", async () => {
    const s = await heartbeatFixture();
    const since = Date.now();
    s.fail(new Error("snapshot source failed"));
    await expect(s.directory.refresh()).rejects.toThrow("snapshot source failed");
    await s.tick(since + 60_000);
    expect(s.alarms()).toHaveLength(0); // Strict boundary: >60s, not >=60s.
    await s.tick(since + 60_001);
    await vi.waitFor(() => expect(s.alarms()).toHaveLength(1), { timeout: 1_000, interval: 10 });
    const alarm = s.alarms()[0]!;
    expect(alarm).toMatchObject({ topic: "ops.owner", kind: "fabric.control-admission.alarm",
      to: s.identity.id, from: { id: s.identity.id }, data: {
        rootId: s.identity.id, hostId: s.identity.id, controlAdmission: "gated", since,
        reason: expect.stringMatching(/refresh|snapshot/i),
      } });
    expect(alarm.dedupeKey).toEqual(expect.any(String));
    expect(readHostLeases(s.mesh.root).get(s.identity.id)?.expiresAt).toBeGreaterThan(Date.now());
    await s.directory.refreshRoutingView(); // A routing read is not admission recovery.
    await s.tick(since + 120_001);
    expect(s.alarms()).toHaveLength(1);
    s.fail();
    await s.directory.refresh();
    expect(s.directory.canConsumeMesh()).toBe(true);
    const secondSince = since + 120_002;
    vi.setSystemTime(secondSince);
    s.fail(new Error("second snapshot outage"));
    await expect(s.directory.refresh()).rejects.toThrow("second snapshot outage");
    await s.tick(secondSince + 60_001);
    await vi.waitFor(() => expect(s.alarms()).toHaveLength(2), { timeout: 1_000, interval: 10 });
    expect(s.alarms()[1]!.dedupeKey).not.toBe(alarm.dedupeKey);
    expect(s.alarms()[1]!.data).toMatchObject({ since: secondSince });
  });

  it("uses an additional host gate even while the directory stays healthy, without changing canConsumeMesh", async () => {
    let hostReason: string | undefined;
    const s = await heartbeatFixture({ controlAdmissionReason: () => hostReason });
    const since = Date.now();
    hostReason = "host mesh activation is not ready";
    expect(s.directory.canConsumeMesh()).toBe(true);
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "gated", controlAdmissionReason: hostReason });
    await s.tick(since + 60_000);
    expect(s.alarms()).toHaveLength(0);
    await s.tick(since + 60_001);
    await vi.waitFor(() => expect(s.alarms()).toHaveLength(1));
    await s.tick(since + 120_001); // Healthy commits alone must not rearm the host gate.
    expect(s.directory.canConsumeMesh()).toBe(true);
    expect(s.alarms()).toHaveLength(1);
    expect(s.alarms()[0]!.data).toMatchObject({ since, reason: "additional host control admission gate" });
    hostReason = undefined;
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "admitted" });
    const again = since + 120_002;
    vi.setSystemTime(again);
    hostReason = "host binding is not fresh";
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "gated", controlAdmissionReason: hostReason });
    await s.tick(again + 60_001);
    await vi.waitFor(() => expect(s.alarms()).toHaveLength(2));
    expect(s.alarms()[1]!.dedupeKey).not.toBe(s.alarms()[0]!.dedupeKey);
  });

  it("keeps raw refresh failure detail local even when the mesh alarm is available", async () => {
    const s = await heartbeatFixture();
    const since = Date.now();
    const secretDetail = "task payload MUST_NOT_REACH_OWNER_EVENT";
    s.fail(new Error(secretDetail));
    await expect(s.directory.refresh()).rejects.toThrow(secretDetail);
    await expect(s.status()).resolves.toMatchObject({ controlAdmissionReason: expect.stringContaining(secretDetail) });
    const warning = vi.spyOn(diagnostics, "warn");
    await s.tick(since + 60_001);
    await vi.waitFor(() => expect(s.alarms()).toHaveLength(1));
    expect(JSON.stringify(s.alarms())).not.toContain(secretDetail);
    expect(warning.mock.calls.some(args => args.some(value => String(value).includes(secretDetail)))).toBe(true);
  });

  it("does not alarm on a healthy idle Main or a recovered sub-minute gate", async () => {
    const s = await heartbeatFixture();
    const start = Date.now();
    await s.tick(start + 60_001);
    expect(s.alarms()).toHaveLength(0);
    s.fail(new Error("brief snapshot outage"));
    await expect(s.directory.refresh()).rejects.toThrow("brief snapshot outage");
    vi.setSystemTime(start + 90_001);
    s.fail();
    await s.directory.refresh();
    await s.tick(start + 180_002);
    expect(s.directory.canConsumeMesh()).toBe(true);
    expect(s.alarms()).toHaveLength(0);
  });
});

const registerIsolated = (directory: ParticipantDirectory, source: () => FabricParticipantRecord[]) =>
  directory.registerSource(source, { isolateErrors: true });

describe("explicit operational snapshot isolation, never root/publication isolation (#17018 requirement 3)", () => {
  it("keeps a cold healthy root admitted when a separate opted-in source throws, and surfaces its failure", async () => {
    const s = fixture();
    const warning = vi.spyOn(diagnostics, "warn");
    const failure = new Error("unrelated operational source failed");
    // It is deliberately registered before the root source in the peer-label
    // prepass: use an independent directory to test both selection loops.
    const mesh = new MeshStore(path.join(s.mesh.root, "cold-separate-mesh"), 64 * 1024, 1_000);
    const directory = new ParticipantDirectory(mesh, { ...s.directory.options });
    directories.push(directory);
    registerIsolated(directory, () => { throw failure; });
    directory.registerSource(() => [{ format: 1, id: s.identity.id, kind: "root", rootId: s.identity.id,
      ownerHostId: s.identity.id, ownerIdentityId: s.identity.id, name: "Main", status: "idle",
      runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
      startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }]);
    await expect(directory.refresh()).resolves.toBeUndefined();
    expect(directory.canConsumeMesh()).toBe(true);
    expect(directory.get(s.identity.id)).toMatchObject({ kind: "root", stale: false });
    expect(warning.mock.calls.some(args => args.some(value => String(value).includes(failure.message)))).toBe(true);
  });

  it("omits failed opted-in source records instead of trusting the previous snapshot", async () => {
    const s = fixture();
    let broken = false;
    const warning = vi.spyOn(diagnostics, "warn");
    registerIsolated(s.directory, () => {
      if (broken) throw new Error("operational snapshot integrity failure");
      return [{ format: 1, id: "agent:unrelated", kind: "agent", rootId: s.identity.id,
        ownerHostId: s.identity.id, ownerIdentityId: s.identity.id, parentId: s.identity.id,
        name: "Unrelated", status: "completed", runner: "pi", transport: "process",
        capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }];
    });
    await s.directory.refresh();
    expect(s.directory.get("agent:unrelated")).toBeDefined();
    broken = true;
    await expect(s.directory.refresh()).resolves.toBeUndefined();
    expect(s.directory.canConsumeMesh()).toBe(true);
    expect(s.directory.get(s.identity.id)).toMatchObject({ kind: "root", stale: false });
    expect(s.directory.get("agent:unrelated")).toBeUndefined();
    expect(warning.mock.calls.some(args => args.some(value => String(value).includes("operational snapshot integrity failure")))).toBe(true);
    const status = await s.status();
    expect(status).not.toHaveProperty("controlAdmission", "gated");
  });

  it("deduplicates isolated source diagnostics and rearms them after source recovery", async () => {
    const s = fixture();
    const warning = vi.spyOn(diagnostics, "warn");
    let broken = true;
    const remove = registerIsolated(s.directory, () => {
      if (broken) throw new Error("isolated diagnostic episode");
      return [];
    });
    const count = () => warning.mock.calls.filter(args => args.some(value => String(value).includes("isolated diagnostic episode"))).length;
    await s.directory.refresh();
    await s.directory.refresh();
    expect(count()).toBe(1);
    broken = false;
    await s.directory.refresh();
    broken = true;
    await s.directory.refresh();
    expect(count()).toBe(2);
    remove();
    await s.directory.refresh();
    expect(count()).toBe(2);
  });

  it("keeps a non-root source fatal unless explicitly isolated", async () => {
    const s = fixture();
    s.directory.registerSource(() => { throw new Error("default operational source failed"); });
    await expect(s.directory.refresh()).rejects.toThrow("default operational source failed");
    expect(s.directory.canConsumeMesh()).toBe(false);
  });

  it("never catches actor renewal authorization outside an isolated source callback", async () => {
    const failure = new Error("actor renewal authorization failed");
    const s = fixture({ actorRenewalAllowed: () => { throw failure; } });
    registerIsolated(s.directory, () => [{ format: 1, id: "actor:renewal", kind: "actor", rootId: ROOT,
      ownerHostId: ROOT, ownerIdentityId: ROOT, name: "Actor", status: "running", runner: "pi",
      transport: "host", capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }]);
    await expect(s.directory.refresh()).rejects.toBe(failure);
    expect(s.directory.canConsumeMesh()).toBe(false);
  });

  it("keeps a default root source exception fatal and admission closed", async () => {
    const s = fixture();
    await s.directory.refresh();
    const failure = new Error("default root snapshot is unavailable");
    s.fail(failure);
    await expect(s.directory.refresh()).rejects.toBe(failure);
    expect(s.directory.canConsumeMesh()).toBe(false);
  });

  it.skipIf(!["linux", "darwin", "win32"].includes(process.platform))(
    "does not catch an actual root-publication authorization fence as an isolated source failure", async () => {
      const s = fixture();
      registerIsolated(s.directory, () => []);
      const release = await takeMainPublicationFence(s.mesh.root, s.identity.id);
      try {
        await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MAIN_PUBLICATION_FENCED" });
        expect(s.directory.canConsumeMesh()).toBe(false);
        expect(s.directory.get(s.identity.id)).toBeUndefined();
      } finally { release(); }
    },
  );

  it("does not catch shared publication failure even with opted-in operational sources", async () => {
    const s = fixture();
    registerIsolated(s.directory, () => []);
    await s.directory.refresh();
    const confirmed = s.directory.confirmedAt();
    const failure = Object.assign(new Error("shared publication authorization refused"), { code: "EACCES" });
    vi.spyOn(s.mesh, "confirmWritable").mockRejectedValueOnce(failure);
    await expect(s.directory.refresh()).rejects.toBe(failure);
    expect(s.directory.confirmedAt()).toBe(confirmed);
    expect(s.directory.canConsumeMesh()).toBe(false);
  });
});

// Keep: carrier-specific trust/failure paths. The real NATS directory/lease/view
// run against the existing KV/CAS/watch fixture; no broker or native worker is launched.
const natsFixture = async () => {
  const world = kvWorld();
  const s = fixture({ transport: world.transport("diagnostic-machine", ROOT),
    transportStateDir: fs.mkdtempSync(path.join(os.tmpdir(), "control-admission-local-")) });
  await s.directory.start();
  return { ...s, world };
};

describe.skipIf(process.platform !== "linux")("NATS control admission preserves native gate semantics (#17018)", () => {
  it("isolates only opted-in callbacks, removes their old records, and leaves a default refresh error non-gating", async () => {
    const s = await natsFixture();
    let broken = false;
    registerIsolated(s.directory, () => {
      if (broken) throw new Error("NATS isolated operational source failed");
      return [{ format: 1, id: "agent:nats-operational", kind: "agent", rootId: ROOT,
        ownerHostId: ROOT, ownerIdentityId: ROOT, name: "NATS operational", status: "running",
        runner: "pi", transport: "process", capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }];
    });
    await s.directory.refresh();
    expect(s.directory.participantView?.resolve("agent:nats-operational")?.live).toBe(true);
    broken = true;
    await expect(s.directory.refresh()).resolves.toBeUndefined();
    expect(s.directory.participantView?.resolve("agent:nats-operational")).toBeUndefined();
    expect(s.directory.controlAdmission()).toEqual({ controlAdmission: "admitted" });
    s.fail(new Error("default NATS snapshot source failed"));
    await expect(s.directory.refresh()).rejects.toThrow("default NATS snapshot source failed");
    // NATS admission is lease/view/recovery based, not refreshError based.
    expect(s.directory.canConsumeMesh()).toBe(true);
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "admitted" });
  });

  it("alarms once after terminal transport loss disarms the heartbeat", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const epoch = Date.now();
    let offset = 0;
    vi.spyOn(performance, "now").mockImplementation(() => Date.now() - epoch + offset);
    const s = await natsFixture();
    const since = Date.now();
    offset = -1; // Real TransportHostLease rejects a backwards monotonic clock terminally.
    expect(s.directory.canConsumeMesh()).toBe(false);
    expect(s.directory.transportLeaseSignal?.aborted).toBe(true);
    expect(s.directory.heartbeatArmed).toBe(false);
    await expect(s.status()).resolves.toMatchObject({ controlAdmission: "gated",
      controlAdmissionReason: expect.stringMatching(/transport.*lease.*lost/i) });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(s.alarms()).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    await vi.waitFor(() => expect(s.alarms()).toHaveLength(1));
    expect(s.alarms()[0]!.data).toMatchObject({ since, reason: "transport host lease was lost" });
    await vi.advanceTimersByTimeAsync(120_000);
    expect(s.alarms()).toHaveLength(1);
    expect(s.directory.heartbeatArmed).toBe(false);
  });
});

it.each(["close", "suspend", "retire", "quiesce"] as const)("cancels a pending threshold alarm on owner %s", async stop => {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  let live = true;
  const s = fixture({ live: () => live });
  await s.directory.refresh();
  s.fail(new Error("pending source outage"));
  await expect(s.directory.refresh()).rejects.toThrow("pending source outage");
  if (stop === "close") await s.directory.close();
  else if (stop === "suspend") s.directory.suspend();
  else if (stop === "quiesce") await expect(s.directory.quiesce()).rejects.toThrow("pending source outage");
  else live = false;
  await vi.advanceTimersByTimeAsync(60_001);
  expect(s.alarms()).toHaveLength(0);
});
