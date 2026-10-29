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
import { MainAgentController } from "../src/main-agent.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentHostConfig } from "../src/residency/protocol.js";
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

const fixture = async () => {
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
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "project", actorPollMs: 20, residentIdleExitMs: 0 },
    retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
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
      for (const item of hosts.reverse()) await item.close().catch(() => undefined);
    } finally {
      // The only cleanup target is this fixture's own mkdtemp tree, never global traces or shared state.
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  });
  const startHost = async () => {
    const firstInterval = intervals.mock.calls.length;
    host = new ResidentHost(config);
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
  await startHost();
  return { root, config, clock, registry, outbox, prefix, get host() { return host; }, get client() { return client; },
    tick, tickUntil, row, notices, sentNotices, alarms, deliveries, outboxFiles, version, restart };
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

// Four bounded cases, one actor/root each. These assert durable journal admission as well as native Pi sends.
describe("resident stopped alarm custody", () => {
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

  it("leaves the marker absent on asynchronous outbox persistence rejection and retries the same episode", async () => {
    const s = await fixture();
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
    } finally { fault.mockRestore(); }
  }, 30_000);
});
