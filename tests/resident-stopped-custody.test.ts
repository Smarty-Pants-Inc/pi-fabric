import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ACTOR_BLOCK_REALARM_PUBLISH_TIMEOUT_MS, ACTOR_STOPPED_ALARM_AFTER_MS,
  FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmDedupeKey, actorAlarmKey,
} from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MainAgentController } from "../src/main-agent.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { readResidentDrainReceipt, type ResidentDrainIntent, type ResidentDrainReceipt } from "../src/residency/drain.js";
import { mainGenerationPath, residentLaunchSpec, type ResidentMainGeneration } from "../src/residency/handover.js";
import { readResidentIdleExit } from "../src/residency/idle-exit.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentCommand, type ResidentCommandResponse,
  type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Keep reason: the publish/outbox/Main-journal crash boundary cannot be exercised by a callback-only unit test.
// No fake timers: mesh locks, native watches and worker custody continue to use their real scheduling.
const MINUTE = 60_000;
const RETENTION_MS = 15 * MINUTE;
const STOP_NOTICE = "Fabric host notice: actor-stopped;";
const cleanup: Array<() => Promise<void>> = [];
beforeEach(() => installInProcessResidentFence());
afterEach(async () => {
  try { for (const close of cleanup.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
}, 30_000);

const waitFor = async (predicate: () => boolean, description: string): Promise<void> => {
  const deadline = performance.now() + 5_000;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error(`Timed out: ${description}`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

type RegistryRow = { id: string; stoppedAt?: number; stoppedAlarmSince?: number; [key: string]: unknown };
type JournalItem = { id: string; deliveryId?: string; message: string; handed?: boolean };

const fixture = async (options: { idleExitMs?: number; onIdle?: ConstructorParameters<typeof ResidentHost>[1]; drain?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stop-custody-"));
  const realNow = Date.now.bind(Date);
  const clock = { skew: 0 };
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
  const intervals = vi.spyOn(globalThis, "setInterval");
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:stop-custody", sessionId: "stop-custody",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5_000, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "project", actorPollMs: 20, residentIdleExitMs: options.idleExitMs ?? 0 },
    retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  // Only the negotiated-drain cases need retained A/B artifacts and a birth-bound launcher.
  // The copied fake worker remains the real process actor fixture, not a mocked producer.
  let launch: ConstructorParameters<typeof ResidentHost>[3];
  let target: ReturnType<typeof residentLaunchSpec> | undefined;
  let mainParticipants: ParticipantDirectory | undefined;
  if (options.drain) {
    const a = path.join(root, "A"), b = path.join(root, "B");
    for (const release of [a, b]) {
      fs.mkdirSync(path.join(release, "dist/residency"), { recursive: true });
      fs.writeFileSync(path.join(release, "package.json"), '{"type":"module"}');
      fs.copyFileSync(config.workerPath, path.join(release, "dist/worker.js"));
      for (const file of ["index.js", "residency/pi-entry.js"]) {
        fs.writeFileSync(path.join(release, "dist", file), "// retained fixture artifact; never executed\n");
      }
    }
    const previous = residentLaunchSpec({ ...config, workerPath: path.join(a, "dist/worker.js"),
      fabricExtensionPath: path.join(a, "dist/index.js"), piBinary: process.execPath }, path.join(a, "dist/residency/pi-entry.js"));
    Object.assign(config, previous.config);
    target = residentLaunchSpec({ ...config, workerPath: path.join(b, "dist/worker.js"),
      fabricExtensionPath: path.join(b, "dist/index.js") }, path.join(b, "dist/residency/pi-entry.js"));
    launch = { spec: previous, launcher: { pid: process.pid, processStartTime: processStartTime(process.pid)!,
      token: "stop-custody-launcher", entry: previous.entry, runtime: previous.runtime } };
    fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config), { mode: 0o600 });
  }
  const sendMessage = vi.fn();
  const pi = { hostCapabilities: { turnProvenance: 1 }, sendMessage, on: () => () => {} } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, config.rootId, true, config.cwd, config.sessionId);
  const context = { isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getEntries: () => [], getSessionFile: () => undefined },
  } as unknown as ExtensionContext;
  const journal = path.join(root, "main-followups.json");
  main.attachFollowUpDrain(context, 0, journal);
  const hosts: ResidentHost[] = [];
  const clients: ResidencyClient[] = [];
  let host: ResidentHost;
  let client: ResidencyClient;
  let maintenance: Array<() => void> = [];
  const registry = path.join(config.actorRoot, "actors.json");
  const outbox = path.join(config.residencyRoot, "delivery-outbox");
  const prefix = residentDeliveryPrefix(config.rootId);
  cleanup.push(async () => {
    try {
      for (const item of clients.reverse()) await item.close();
      main.closeFollowUpDrain();
      await mainParticipants?.close();
      for (const item of hosts.reverse()) await item.close().catch(() => undefined);
    } finally {
      // The only cleanup target is this fixture's own mkdtemp tree, never global traces or shared state.
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  });
  const startHost = async () => {
    const firstInterval = intervals.mock.calls.length;
    host = new ResidentHost(config, options.onIdle, undefined, launch);
    hosts.push(host);
    await host.start();
    maintenance = intervals.mock.calls.slice(firstInterval)
      .filter(([, ms]) => ms === RETENTION_MS).map(([callback]) => callback as () => void);
    expect(maintenance).toHaveLength(2); // The directory's project and session ActorManagers, not a product-only hook.
    client = new ResidencyClient({ config, mesh: host.mesh, participants: host.participants, mainAgent: main });
    clients.push(client);
  };
  const tick = async () => {
    for (const callback of maintenance) callback();
    // The existing sweep yields with setImmediate before its claim/publish/ack work.
    await new Promise<void>(resolve => setImmediate(resolve));
    await new Promise(resolve => setTimeout(resolve, 40));
  };
  const tickUntil = async (predicate: () => boolean, description: string) => {
    const deadline = performance.now() + 5_000;
    do {
      await tick();
      if (predicate()) return;
    } while (performance.now() < deadline);
    throw new Error(`Timed out after maintenance: ${description}`);
  };
  const rows = (): RegistryRow[] => JSON.parse(fs.readFileSync(registry, "utf8")).actors;
  const row = (id: string) => rows().find(item => item.id === id)!;
  const notices = (): JournalItem[] => fs.existsSync(journal)
    ? (JSON.parse(fs.readFileSync(journal, "utf8")) as { items: JournalItem[] }).items.filter(item => item.message.startsWith(STOP_NOTICE))
    : [];
  const sentNotices = () => sendMessage.mock.calls.filter(([message]) => message.content.includes(STOP_NOTICE));
  const alarms = (id: string) => host.mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
    .filter(event => event.kind === "actor-stopped" && (event.data as { actorId?: string }).actorId === id);
  const deliveries = () => host.mesh.listAll(prefix, { fresh: true });
  const outboxFiles = () => fs.existsSync(outbox) ? fs.readdirSync(outbox).filter(file => file.endsWith(".json")) : [];
  const version = async (key: string) => {
    let result = 0;
    await host.mesh.writeBatch({ identity: host.identity, ops: [], prepare: view => { result = view.version(key); return []; } });
    return result;
  };
  const restart = async (afterClose?: (error: unknown) => void) => {
    await client.close();
    let closeError: unknown;
    try { await host.close(); } catch (error) { closeError = error; }
    if (afterClose) afterClose(closeError);
    else if (closeError) throw closeError;
    await startHost();
  };
  const advance = async (ms: number) => {
    clock.skew += ms;
    // Native 50 ms request polls own dead-root idle sampling, as in residency-idle-exit.
    await new Promise(resolve => setTimeout(resolve, 150));
  };
  const drain = async (): Promise<ResidentDrainReceipt> => {
    if (!launch || !target) throw new Error("Negotiated drain fixture was not enabled");
    const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    mainParticipants = new ParticipantDirectory(host.mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    mainParticipants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
      mainProcess: { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! } }]);
    await mainParticipants.start();
    const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner;
    const generation: ResidentMainGeneration = { pid: process.pid, processStartTime: processStartTime(process.pid)!,
      nonce: randomUUID(), rootId: config.rootId, sessionId: config.sessionId, releaseRoot: launch.spec.releaseRoot };
    fs.writeFileSync(mainGenerationPath(config.residencyRoot), JSON.stringify(generation), { mode: 0o600 });
    const intent: ResidentDrainIntent = { caller: { identity, hostId: identity.id }, main: generation, target,
      expected: { releaseRoot: launch.spec.releaseRoot, configDigest: owner.configDigest!, launcher: launch.launcher,
        owner: { pid: owner.pid, processStartTime: owner.processStartTime!, token: owner.token, hostId: owner.hostId } }, timeoutMs: 2_000 };
    const requestId = randomUUID();
    const command: ResidentCommand = { ...intent, format: 2, operation: "releaseDrain", requestId,
      rootId: config.rootId, createdAt: Date.now() };
    const responsePath = path.join(config.residencyRoot, "responses", `${requestId}.json`);
    fs.writeFileSync(path.join(config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify(command), { mode: 0o600 });
    await waitFor(() => fs.existsSync(responsePath), "real releaseDrain response");
    const response = JSON.parse(fs.readFileSync(responsePath, "utf8")) as ResidentCommandResponse;
    expect(response).toMatchObject({ ok: true, drain: { phase: "held", admissionHeld: true, terminal: false } });
    let receipt: ResidentDrainReceipt | undefined;
    await waitFor(() => {
      receipt = readResidentDrainReceipt(config.residencyRoot, response.drain!.id, response.drain);
      return receipt?.terminal === true;
    }, "real releaseDrain terminal checkpoint receipt");
    return receipt!;
  };
  await startHost();
  return { root, config, clock, registry, outbox, prefix, get host() { return host; }, get client() { return client; },
    tick, tickUntil, row, notices, sentNotices, alarms, deliveries, outboxFiles, version, restart, advance, drain };
};

const stoppedActor = async (s: Awaited<ReturnType<typeof fixture>>) => {
  const actor = await s.host.actors.create({ name: "stopped resident", instructions: "Advise.", residency: "durable",
    transport: "process", responseMode: "directive", delivery: "mailbox", triggerTurn: false });
  // Exercise the same real fake-worker setup as resident-alarm-provenance, without unrelated Main output.
  expect(await s.host.actors.ask(actor.id, "healthy output")).toMatchObject({ action: "message", text: "fake actor advice" });
  await s.host.actors.stop(actor.id);
  await waitFor(() => typeof s.row(actor.id).stoppedAt === "number", "durable stop episode");
  s.clock.skew += ACTOR_STOPPED_ALARM_AFTER_MS + 1_000;
  return actor;
};

// A confirmation probe is uncertainty, not a failed producer. Inject at the exact locked inspect,
// with Main absent so the pre/post-flush crash images and missing marker are directly observable.
const recoverConfirmationProbeTimeout = async (s: Awaited<ReturnType<typeof fixture>>, phase: "before" | "after") => {
  const actor = await stoppedActor(s);
  const since = s.row(actor.id).stoppedAt!;
  const claimKey = actorAlarmKey(actor.id, "actor-stopped");
  const writeBatch = s.host.mesh.writeBatch.bind(s.host.mesh);
  const failAt = phase === "before" ? 1 : 2;
  let probes = 0, failures = 0;
  const fault = vi.spyOn(s.host.mesh, "writeBatch").mockImplementation(async input => {
    if (input.lockTiming?.operation === "resident-delivery-confirmation" && ++probes === failAt) {
      failures++;
      throw new MeshLockTimeoutError(" stopped notice confirmation probe fixture", 1, 0);
    }
    return writeBatch(input);
  });
  try {
    await s.tickUntil(() => failures === 1 && (s.host.mesh.get(claimKey)?.value as { state?: string })?.state === "published",
      `${phase}-flush probe failure settles without a stop marker`);
    expect(probes).toBe(failAt);
    expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
    expect(s.notices()).toHaveLength(0);
    expect(s.sentNotices()).toHaveLength(0);
    expect(s.outboxFiles()).toHaveLength(0);
    // Unknown pre-stage custody must NOT stage anything; post-flush uncertainty retains its single record.
    expect(s.deliveries()).toHaveLength(phase === "before" ? 0 : 1);
    const pending = s.deliveries()[0];
    fault.mockRestore();
    s.client.start();
    await s.tick(); // Existing ActorManager maintenance retries the same episode, not a new retry API.
    await waitFor(() => s.notices().length === 1 && s.deliveries().length === 0, "next maintenance retry reaches Main once");
    await s.tickUntil(() => s.row(actor.id).stoppedAlarmSince === since, "recovered Main confirmation checkpoints the stop marker");
    const notice = s.notices()[0]!;
    expect(notice.deliveryId).toMatch(/^resident:/);
    if (pending) expect(notice.deliveryId).toBe(`resident:${s.config.rootId}:${(pending.value as { id: string }).id}`);
    for (let n = 0; n < 3; n++) await s.tick();
    expect(s.notices().map(item => item.deliveryId)).toEqual([notice.deliveryId]);
    expect(s.sentNotices()).toHaveLength(1);
    expect(s.alarms(actor.id)).toHaveLength(1);
    expect(s.outboxFiles()).toHaveLength(0);
    expect(s.deliveries()).toHaveLength(0);
    return actor;
  } finally { fault.mockRestore(); }
};

// Bounded cases, one actor/root each. These assert durable journal admission as well as native Pi sends.
describe("resident stopped alarm custody", () => {
  it.each(["before", "after"] as const)("confirmation probe timeout %s flush recovers once and permits actual idle exit", async phase => {
    const onIdle = vi.fn();
    const idleExitMs = 60_000;
    const s = await fixture({ onIdle, idleExitMs });
    const actor = await recoverConfirmationProbeTimeout(s, phase);
    expect(onIdle).not.toHaveBeenCalled();
    await s.host.actors.remove(actor.id);
    expect(s.host.actors.hasOwnedActors()).toBe(false);
    await s.advance(1_000); // First dead-root/no-actor request sample starts the continuous window.
    expect(onIdle).not.toHaveBeenCalled();
    await s.advance(idleExitMs);
    await waitFor(() => onIdle.mock.calls.length === 1, "recovered host actually retires on native idle polls");
    expect(onIdle).toHaveBeenCalledExactlyOnceWith("idle-exit");
    const retirement = readResidentIdleExit(s.config.residencyRoot, s.config.rootId);
    expect(retirement).toMatchObject({ reason: "root-dead-idle", pid: process.pid });
    expect(retirement!.idleMs).toBeGreaterThanOrEqual(idleExitMs);
    expect(s.notices()).toHaveLength(1);
    expect(s.sentNotices()).toHaveLength(1);
  }, 30_000);

  it.skipIf(process.platform !== "linux").each(["before", "after"] as const)(
    "confirmation probe timeout %s flush recovers once and permits actual cooperative drain", async phase => {
      const s = await fixture({ drain: true });
      await recoverConfirmationProbeTimeout(s, phase);
      // This endpoint safely resumes A: no launch attestation/containment is invented and no B is launched.
      // Reaching checkpoint/readback proves the real sticky-publication drain guard was passed.
      expect(await s.drain()).toMatchObject({ phase: "checkpointed", outcome: "containment-unavailable",
        code: "CONTAINMENT_UNAVAILABLE", checkpointReadback: true, terminal: true, admissionHeld: false,
        checkpoint: { actors: { project: expect.anything(), session: expect.anything() },
          agents: expect.anything(), control: expect.anything(), lifecycle: expect.anything() },
        quiet: { publications: 0, producerOutbox: 0 } });
      expect(s.notices()).toHaveLength(1);
      expect(s.sentNotices()).toHaveLength(1);
    }, 30_000);

  it("takes over a crash image after alarm publication but before onDeliver and admits ONE Main notice", async () => {
    const s = await fixture();
    const actor = await stoppedActor(s);
    const since = s.row(actor.id).stoppedAt!;
    const claimKey = actorAlarmKey(actor.id, "actor-stopped");
    const dedupeKey = actorAlarmDedupeKey("actor-stopped", actor.id, since, 0);
    // Exact durable crash image from F1: pending claim + actual event/receipt, no onDeliver/outbox.
    // A rejected publish is NOT a crash: the manager would release its pending claim.
    await s.host.mesh.put({ key: claimKey, identity: s.host.identity,
      value: { since, slot: 0, at: Date.now(), owner: s.host.identity.id, state: "pending" } });
    const event = await s.host.mesh.publish({ id: randomUUID(), topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC,
      kind: "actor-stopped", from: s.host.identity, dedupeKey,
      data: { actorId: actor.id, actorName: actor.name, ownerRoot: s.config.rootId, reason: "stop",
        stopReason: "explicit", since, stoppedForMs: Date.now() - since, residency: "durable" } });
    expect(s.alarms(actor.id)).toHaveLength(1);
    expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
    expect(s.outboxFiles()).toHaveLength(0);
    expect(s.deliveries()).toHaveLength(0);
    expect(s.notices()).toHaveLength(0);
    await s.restart();
    expect(s.host.mesh.get(claimKey)?.value).toMatchObject({ state: "pending", since });
    s.clock.skew += ACTOR_BLOCK_REALARM_PUBLISH_TIMEOUT_MS + 1_000;
    s.client.start();
    await s.tickUntil(() => s.notices().length === 1 && s.row(actor.id).stoppedAlarmSince === since, "receipt replay reaches Main");
    await waitFor(() => s.deliveries().length === 0, "Main consumes resident record");
    const notice = s.notices()[0]!;
    expect(notice.deliveryId).toMatch(/^resident:/);
    expect(s.sentNotices()).toHaveLength(1);
    expect(s.alarms(actor.id).map(item => item.id)).toEqual([event.id]);
    for (let n = 0; n < 3; n++) await s.tick();
    expect(s.notices().map(item => item.deliveryId)).toEqual([notice.deliveryId]);
    expect(s.sentNotices()).toHaveLength(1);
  }, 30_000);

  it("reuses consumed delivery custody after Main confirms but the actor marker checkpoint is lost", async () => {
    const s = await fixture();
    const actor = await stoppedActor(s);
    const since = s.row(actor.id).stoppedAt!;
    const claimKey = actorAlarmKey(actor.id, "actor-stopped");
    let refusedMarkers = 0;
    const rename = fs.renameSync;
    const markerFault = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === s.registry && fs.readFileSync(from, "utf8").includes('"stoppedAlarmSince"')) {
        refusedMarkers++;
        throw new Error("crash before stopped marker checkpoint");
      }
      return rename(from, to);
    });
    // Also interrupt the final claim checkpoint: restart must take over a pending claim, not shortcut a published one.
    const put = s.host.mesh.put.bind(s.host.mesh);
    let refusedPublished = 0;
    const claimFault = vi.spyOn(s.host.mesh, "put").mockImplementation(async input => {
      if (input.key === claimKey && (input.value as { state?: string }).state === "published") {
        refusedPublished++;
        throw new Error("crash before published claim checkpoint");
      }
      return put(input);
    });
    try {
      await s.tickUntil(() => s.deliveries().length === 1, "resident handoff before Main starts");
      const record = s.deliveries()[0]!;
      const deliveryId = `resident:${s.config.rootId}:${(record.value as { id: string }).id}`;
      s.client.start();
      await waitFor(() => s.notices().length === 1 && s.deliveries().length === 0, "durable Main admission and tombstone");
      // The interrupted claim checkpoint deliberately left the first attempt pending.
      s.clock.skew += ACTOR_BLOCK_REALARM_PUBLISH_TIMEOUT_MS + 1_000;
      await s.tickUntil(() => refusedMarkers > 0, "confirmed delivery attempts marker checkpoint");
      expect(refusedPublished).toBeGreaterThan(0);
      expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
      expect(s.notices().map(item => item.deliveryId)).toEqual([deliveryId]);
      const consumedVersion = await s.version(record.key);
      expect(consumedVersion).toBeGreaterThan(record.version);
      expect(s.host.mesh.get(claimKey)?.value).toMatchObject({ state: "pending", since });
      await s.restart(error => {
        // Even graceful teardown cannot accidentally commit the missing crash-image marker.
        expect(String(error)).toContain("crash before stopped marker checkpoint");
        expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
        markerFault.mockRestore();
        claimFault.mockRestore();
      });
      s.clock.skew += ACTOR_BLOCK_REALARM_PUBLISH_TIMEOUT_MS + 1_000;
      s.client.start();
      await s.tickUntil(() => s.row(actor.id).stoppedAlarmSince === since, "tombstone confirms replay without another record");
      for (let n = 0; n < 3; n++) await s.tick();
      expect(await s.version(record.key)).toBe(consumedVersion);
      expect(s.notices().map(item => item.deliveryId)).toEqual([deliveryId]);
      expect(s.sentNotices()).toHaveLength(1);
      expect(s.deliveries()).toHaveLength(0);
      expect(s.outboxFiles()).toHaveLength(0);
      expect(s.alarms(actor.id)).toHaveLength(1);
    } finally { markerFault.mockRestore(); claimFault.mockRestore(); }
  }, 30_000);

  it("does not checkpoint an absent Main, then checkpoints admission once and admits a changed stop episode", async () => {
    const s = await fixture();
    const actor = await stoppedActor(s);
    const since = s.row(actor.id).stoppedAt!;
    // Keep the existing disk outbox visible while Main is absent; no onDeliver adapter is replaced.
    const put = s.host.mesh.put.bind(s.host.mesh);
    const handoffFault = vi.spyOn(s.host.mesh, "put").mockImplementation(async input => {
      if (input.key.startsWith(s.prefix)) throw new Error("resident mesh handoff temporarily unavailable");
      return put(input);
    });
    try {
      await s.tickUntil(() => s.alarms(actor.id).length === 1 && s.outboxFiles().length === 1, "alarm and durable resident outbox");
      expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
      expect(s.notices()).toHaveLength(0);
      handoffFault.mockRestore();
      await s.tickUntil(() => s.deliveries().length === 1, "outbox hands off to resident mesh mailbox");
      const record = s.deliveries()[0]!;
      for (let n = 0; n < 3; n++) await s.tick();
      expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
      expect(s.deliveries().map(item => item.key)).toEqual([record.key]);
      s.client.start();
      await waitFor(() => s.notices().length === 1 && s.deliveries().length === 0, "Main journal admits existing resident id");
      expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
      await s.tickUntil(() => s.row(actor.id).stoppedAlarmSince === since, "next maintenance stores confirmed marker");
      const firstId = s.notices()[0]!.deliveryId;
      s.clock.skew += 7 * 60 * MINUTE;
      await s.restart();
      s.client.start();
      for (let n = 0; n < 3; n++) await s.tick();
      expect(s.notices().map(item => item.deliveryId)).toEqual([firstId]);
      expect(s.sentNotices()).toHaveLength(1);
      expect(s.alarms(actor.id)).toHaveLength(1);
      // Public stop is terminal. Model a subsequent active episode in this isolated registry,
      // then use the public API to produce its genuinely new stoppedAt (same actor identity).
      await s.restart(() => {
        const saved = JSON.parse(fs.readFileSync(s.registry, "utf8")) as { actors: RegistryRow[] };
        const row = saved.actors.find(item => item.id === actor.id)!;
        row.status = "idle";
        delete row.stoppedAt;
        delete row.stoppedAlarmSince;
        delete row.stopReason;
        row.updatedAt = Date.now();
        fs.writeFileSync(s.registry, JSON.stringify(saved));
      });
      s.client.start();
      await s.host.actors.stop(actor.id);
      await waitFor(() => s.row(actor.id).stoppedAt! > since, "new persisted stop episode");
      const secondSince = s.row(actor.id).stoppedAt!;
      s.clock.skew += ACTOR_STOPPED_ALARM_AFTER_MS + 1_000;
      await s.tickUntil(() => s.notices().length === 2 && s.row(actor.id).stoppedAlarmSince === secondSince, "second episode admitted");
      expect(new Set(s.notices().map(item => item.deliveryId)).size).toBe(2);
      expect(s.sentNotices()).toHaveLength(2);
      expect(s.alarms(actor.id).map(item => (item.data as { since: number }).since)).toEqual([since, secondSince]);
    } finally { handoffFault.mockRestore(); }
  }, 30_000);

  it("leaves the marker absent on asynchronous outbox persistence rejection and retries the same episode but still blocks drain", async () => {
    const s = await fixture({ drain: process.platform === "linux" });
    const actor = await stoppedActor(s);
    const since = s.row(actor.id).stoppedAt!;
    const rename = fs.renameSync;
    let refusedOutbox = 0;
    const fault = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (path.dirname(String(to)) === s.outbox && String(to).endsWith(".json")) {
        refusedOutbox++;
        throw new Error("injected durable outbox persistence failure");
      }
      return rename(from, to);
    });
    try {
      s.client.start();
      // #queueDelivery is async: a synchronous filesystem fault rejects its Promise after onDeliver returns it.
      await s.tickUntil(() => refusedOutbox > 0 && s.alarms(actor.id).length === 1, "resident outbox promise rejects");
      for (let n = 0; n < 2; n++) await s.tick();
      expect(s.row(actor.id).stoppedAlarmSince).toBeUndefined();
      expect(s.notices()).toHaveLength(0);
      expect(s.sentNotices()).toHaveLength(0);
      expect(s.deliveries()).toHaveLength(0);
      fault.mockRestore();
      await s.tickUntil(() => s.notices().length === 1 && s.row(actor.id).stoppedAlarmSince === since, "failed custody retries through Main journal");
      await waitFor(() => s.deliveries().length === 0, "consumed successful retry");
      for (let n = 0; n < 3; n++) await s.tick();
      expect(s.notices()).toHaveLength(1);
      expect(s.notices()[0]!.deliveryId).toMatch(/^resident:/);
      expect(s.sentNotices()).toHaveLength(1);
      expect(s.alarms(actor.id)).toHaveLength(1);
      expect(s.outboxFiles()).toHaveLength(0);
      if (process.platform === "linux") {
        // A genuine persist rejection MUST remain tracked even after its notice later reaches Main.
        // Catching the entire stable-delivery branch or globally clearing failures would fail this proof.
        const receipt = await s.drain();
        expect(receipt).toMatchObject({ outcome: "resumed", code: "DRAIN_UNCERTAIN", terminal: true,
          admissionHeld: false, reason: expect.stringContaining("unconfirmed producer/publication obligations") });
        expect(receipt.checkpointReadback).not.toBe(true);
      }
    } finally { fault.mockRestore(); }
  }, 30_000);
});
