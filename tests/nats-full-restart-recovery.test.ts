// Keep: a full broker restart crosses JetStream leadership, participant lease expiry,
// the KV watch snapshots, and each routed control request/ACK on the real transport.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { PATH_B_OPERATOR_MACHINE } from "../src/mesh/nats-auth/permissions.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportStream, INBOX_DUPLICATE_WINDOW_MS } from "../src/transport/nats/provision.js";
import { transportDurableName, transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import type { FabricTransport, TransportEnvelope } from "../src/transport/types.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const brokerSuite = describe.skipIf(!binary || process.platform !== "linux");
const MACHINE = "machine-a";
const PARTICIPANTS = 22;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

describe("NATS inbox duplicate window", () => {
  it("matches the 24-hour command deduplication horizon", () => {
    expect(INBOX_DUPLICATE_WINDOW_MS).toBe(24 * 60 * 60_000);
  });
});

interface Owner {
  readonly id: string;
  readonly identity: MeshIdentity;
  readonly transport: FabricTransport;
  readonly mesh: MeshStore;
  readonly directory: ParticipantDirectory;
  verifiedReconnects: number;
  verifiedReconnectAt: number;
  stopReconnectWatch: (() => void) | undefined;
  control: FabricControlPlane | undefined;
}

interface CommandSendEvidence {
  attempts: number;
  errors: number;
  duplicates: number;
  sequences: number[];
  envelope?: TransportEnvelope;
}

interface SettledSend {
  readonly senderId: string;
  readonly targetId: string;
  readonly idempotencyKey: string;
  readonly commandId?: string;
  readonly startedAt: number;
  readonly outcome: "ack" | "error";
  readonly error?: { readonly name: string; readonly code?: string; readonly message: string };
}

let harness: PathBHarness | undefined;
let inboxManager: Awaited<ReturnType<typeof jetstreamManager>> | undefined;
let inboxBaseline = 0;
const initialPresenceRevisions = new Map<string, number>();
const owners: Owner[] = [];
const received = new Map<string, number>();
const executions = new Map<string, number>();
const commandSends = new Map<string, CommandSendEvidence>();

const waitUntil = async (predicate: () => boolean, label: string, deadline: number): Promise<void> => {
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(25);
  }
  throw new Error(`timed out: ${label}`);
};

const presenceKeyFor = (id: string): string => `${MACHINE}.p.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;

const observeCommandSends = (owner: Owner): void => {
  const send = owner.transport.send.bind(owner.transport);
  owner.transport.send = (async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal) => {
    if (envelope.kind !== "command") return send(envelope, signal);
    const evidence = commandSends.get(envelope.id) ?? { attempts: 0, errors: 0, duplicates: 0, sequences: [] };
    evidence.attempts += 1;
    commandSends.set(envelope.id, evidence);
    try {
      const result = await send(envelope, signal);
      if (result.duplicate) evidence.duplicates += 1;
      evidence.sequences.push(result.sequence);
      evidence.envelope = result.envelope;
      return result;
    } catch (error) {
      evidence.errors += 1;
      throw error;
    }
  }) as FabricTransport["send"];
};

const stressCommandRecords = async (afterSequence: number, expectedMessages: ReadonlySet<string>) => {
  const manager = inboxManager;
  if (!manager) throw new Error("JetStream inbox inspector is unavailable");
  const stream = transportInboxStream(MACHINE);
  const lastSequence = (await manager.streams.info(stream)).state.last_seq;
  const records: Array<{ sequence: number; envelopeId: string; commandId: string; message: string }> = [];
  for (let sequence = afterSequence + 1; sequence <= lastSequence; sequence += 1) {
    const stored = await manager.streams.getMessage(stream, { seq: sequence });
    if (!stored) continue;
    const wire = stored.json<{ envelope?: { id?: unknown; kind?: unknown; body?: { commandId?: unknown; message?: unknown } } }>();
    const envelope = wire.envelope;
    if (envelope?.kind !== "command" || typeof envelope.id !== "string" || typeof envelope.body?.commandId !== "string" ||
        typeof envelope.body.message !== "string" || !expectedMessages.has(envelope.body.message)) continue;
    expect(stored.header.get("Nats-Msg-Id")).toBe(envelope.id);
    records.push({ sequence, envelopeId: envelope.id, commandId: envelope.body.commandId, message: envelope.body.message });
  }
  return records;
};

const waitForAcknowledgedStressRecords = async (afterSequence: number, expectedMessages: ReadonlySet<string>,
  acknowledgedMessages: ReadonlySet<string>, deadline: number) => {
  let records = await stressCommandRecords(afterSequence, expectedMessages);
  while (Date.now() < deadline) {
    const storedMessages = new Set(records.map(record => record.message));
    const missing = [...acknowledgedMessages].filter(message => !storedMessages.has(message));
    if (missing.length === 0) return records;
    await sleep(50);
    records = await stressCommandRecords(afterSequence, expectedMessages);
  }
  const storedMessages = new Set(records.map(record => record.message));
  const missing = [...acknowledgedMessages].filter(message => !storedMessages.has(message));
  throw new Error(`timed out waiting for ACKed stress records; stored=${records.length}, missing=${missing.slice(0, 8).join(",")}`);
};

const allViewsReady = (): boolean => owners.length === PARTICIPANTS && owners.every((owner) => {
  if (!owner.directory.canConsumeMesh()) return false;
  const view = owner.directory.participantView;
  if (!view) return false;
  return owners.every((peer) => view.resolve(peer.id)?.live === true);
});

const sendRing = async (prefix: string, deadline: number): Promise<void> => {
  const before = new Map(received);
  const acknowledgements = await Promise.all(owners.map((sender, index) => {
    const target = owners[(index + 1) % owners.length]!;
    const timeoutMs = Math.max(1, Math.min(10_000, deadline - Date.now()));
    const control = sender.control;
    if (!control) throw new Error(`control plane is unavailable for ${sender.id}`);
    return control.request(target.id, target.id, "followUp", {
      message: `${prefix}:${index}`,
      ownerIncarnation: target.directory.ownerIncarnation,
    }, target.identity.id, { idempotencyKey: `${prefix}-${index}`, timeoutMs });
  }));
  expect(acknowledgements).toHaveLength(PARTICIPANTS);
  expect(acknowledgements.every((result) => result.queued && result.acknowledged === true && result.routed === "mesh")).toBe(true);
  expect(owners.every((owner) => (received.get(owner.id) ?? 0) > (before.get(owner.id) ?? 0))).toBe(true);
};

brokerSuite("real NATS full-stop recovery across participant views and routed ACKs", () => {
  beforeAll(async () => {
    harness = await createPathBHarness({ binary, tls: {} });
    expect(harness.serverVersion).toBe("2.14.7");
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const manager = await jetstreamManager(bootstrap);
    const js = jetstream(bootstrap);
    await openNatsKvForMachine(js, "leases", MACHINE);
    await openNatsKvForMachine(js, "presence", MACHINE);
    for (const machine of ["machine-a", "machine-b"] as const) {
      await ensureTransportStream(manager, {
        name: transportInboxStream(machine),
        subjects: [transportInboxStreamSubject(machine)],
        maxBytes: 1_073_741_824,
        maxAgeMs: 0,
        maxMessageBytes: 1_048_576,
        duplicateWindowMs: INBOX_DUPLICATE_WINDOW_MS,
        replicas: 1,
      });
      expect((await manager.streams.info(transportInboxStream(machine))).config.duplicate_window)
        .toBe(INBOX_DUPLICATE_WINDOW_MS * 1_000_000);
    }
    inboxManager = manager;

    for (let index = 0; index < PARTICIPANTS; index++) {
      const sessionId = randomUUID();
      const id = `session:${sessionId}`;
      const identity: MeshIdentity = { id, name: `full-restart-${index}`, kind: "main", sessionId };
      const ownerRoot = path.join(harness.root, `owner-${index}`);
      const meshRoot = path.join(ownerRoot, "mesh");
      const transportStateDir = path.join(ownerRoot, "transport-state");
      fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
      fs.mkdirSync(transportStateDir, { recursive: true, mode: 0o700 });
      const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const transport = await openNatsTransport({
        ...harness.runtimeOptions(harness.users.machineA), hostId: id, name: `full-restart-${index}`,
        ...(index === 0 ? { ephemeralHost: true, ephemeralInactiveSeconds: 300 } : {}),
      });
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, transport, transportStateDir, hostId: id, rootId: id, identity,
      });
      directory.registerSource((): FabricParticipantRecord[] => {
        const now = Date.now();
        return [{
          format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
          ownerIncarnation: directory.ownerIncarnation, name: identity.name, status: "running", runner: "pi",
          transport: "host", capabilities: ["followUp", "steer", "fabric"], sessionId,
          startedAt: now, updatedAt: now, controlProtocol: "v1",
        }];
      });
      const owner: Owner = { id, identity, transport, mesh, directory, verifiedReconnects: 0, verifiedReconnectAt: 0,
        stopReconnectWatch: undefined, control: undefined };
      owner.stopReconnectWatch = transport.onVerifiedReconnect?.(() => {
        owner.verifiedReconnects += 1;
        owner.verifiedReconnectAt = Date.now();
      });
      owners.push(owner);
      received.set(id, 0);
    }

    await Promise.all(owners.map((owner) => owner.directory.start()));
    for (const owner of owners) {
      const view = owner.directory.participantView;
      if (!view) throw new Error("participant NATS view is unavailable");
      const control = new FabricControlPlane(owner.mesh, owner.identity, {
        enabled: true,
        hostId: owner.id,
        transport: owner.transport,
        participants: view,
        ownerIncarnation: owner.directory.ownerIncarnation,
        controlIncarnationFence: "enforce",
        expectedNatsInboxStream: transportInboxStream(MACHINE),
        relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
        canConsumeMesh: () => owner.directory.canConsumeMesh(),
        pollMs: 20,
        acknowledgementTimeoutMs: 10_000,
      });
      owner.control = control;
      control.start((command, from) => {
        executions.set(command.commandId, (executions.get(command.commandId) ?? 0) + 1);
        if (command.operation !== "followUp" || command.targetId !== owner.id) {
          return { accepted: false, error: "unexpected full-restart test command" };
        }
        if (!view.resolve(from.id)?.live) return { accepted: false, error: "sender route is not live" };
        received.set(owner.id, (received.get(owner.id) ?? 0) + 1);
        return { accepted: true, messageId: command.commandId };
      });
    }
    await waitUntil(allViewsReady, "initial 22-participant views", Date.now() + 30_000);
    await sendRing("initial", Date.now() + 30_000);
    for (const owner of owners) {
      const presence = await owner.transport.kv("presence").get<FabricParticipantRecord>(presenceKeyFor(owner.id));
      if (!presence) throw new Error(`initial participant presence is missing for ${owner.id}`);
      initialPresenceRevisions.set(owner.id, presence.revision);
    }
    inboxBaseline = (await manager.streams.info(transportInboxStream(MACHINE))).state.last_seq;
  }, 60_000);

  afterAll(async () => {
    for (const owner of owners) owner.stopReconnectWatch?.();
    await Promise.allSettled(owners.flatMap((owner) => owner.control ? [owner.control.close()] : []));
    await Promise.allSettled(owners.map((owner) => owner.directory.close()));
    await Promise.allSettled(owners.map((owner) => owner.transport.close()));
    for (const owner of owners) owner.mesh.closeState();
    owners.length = 0;
    await harness?.stop();
    harness = undefined;
    inboxManager = undefined;
  });

  it("settles continuous sends across a full restart and dedupes stable inbox IDs", async () => {
    if (!harness) throw new Error("NATS fixture unavailable");
    const expectedMessages = new Set<string>();
    const outcomes: SettledSend[] = [];
    let afterReconnect: SettledSend[] = [];
    const settlementDeadlines: string[] = [];
    const settleSend = async (sender: Owner, key: string, targetIndex: number): Promise<SettledSend> => {
      const target = owners[targetIndex]!;
      const control = sender.control;
      if (!control) throw new Error(`control plane is unavailable for ${sender.id}`);
      const message = `restart-stress:${key}`;
      const startedAt = Date.now();
      expectedMessages.add(message);
      const controller = new AbortController();
      const abortTimer = setTimeout(() => controller.abort(new Error("stress send settlement bound reached")), 5_000);
      let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
      const deadline = new Promise<never>((_, reject) => {
        deadlineTimer = setTimeout(() => {
          controller.abort(new Error("stress send did not settle before its hard deadline"));
          reject(Object.assign(new Error("stress send did not settle before its hard deadline"), { code: "TEST_SEND_SETTLEMENT_DEADLINE" }));
        }, 7_000);
      });
      let settled: SettledSend;
      try {
        const receipt = await Promise.race([control.request(target.id, target.id, "followUp", {
          message,
          ownerIncarnation: target.directory.ownerIncarnation,
        }, target.identity.id, { idempotencyKey: key, timeoutMs: 2_500, signal: controller.signal }), deadline]);
        settled = { senderId: sender.id, targetId: target.id, idempotencyKey: key, commandId: receipt.messageId, startedAt, outcome: "ack" };
      } catch (error) {
        const candidate = error && typeof error === "object" && "code" in error && typeof error.code === "string" ? error.code : undefined;
        const detail = error instanceof Error ? error : new Error(String(error));
        if (candidate === "TEST_SEND_SETTLEMENT_DEADLINE") settlementDeadlines.push(key);
        const errorMessage = detail.message || detail.name || "Unknown send failure";
        settled = { senderId: sender.id, targetId: target.id, idempotencyKey: key, startedAt, outcome: "error",
          error: { name: detail.name, ...(candidate ? { code: candidate } : {}), message: errorMessage.slice(0, 240) } };
      } finally {
        clearTimeout(abortTimer);
        clearTimeout(deadlineTimer);
      }
      outcomes.push(settled);
      return settled;
    };
    for (const owner of owners) observeCommandSends(owner);

    let stopTraffic = false;
    let workerFailure: unknown;
    const workers = owners.map(async (sender, index) => {
      try {
        for (let sequence = 0; sequence < 120 && !stopTraffic; sequence += 1) {
          await settleSend(sender, `restart-load:${index}:${sequence}`, (index + 1) % PARTICIPANTS);
          if (!stopTraffic) await sleep(500);
        }
      } catch (error) { workerFailure ??= error; }
    });
    try {
      await waitUntil(() => owners.every(owner => outcomes.some(outcome => outcome.senderId === owner.id)),
        "each participant's continuous sender started", Date.now() + 15_000);
      const server = harness.serverProcess;
      if (!server) throw new Error("NATS server process is unavailable");
      if (server.exitCode === null && server.signalCode === null) {
        await new Promise<void>((resolve, reject) => {
          const onExit = () => { clearTimeout(timer); resolve(); };
          const timer = setTimeout(() => {
            server.off("exit", onExit);
            reject(new Error("timed out waiting for full NATS stop"));
          }, 5_000);
          server.once("exit", onExit);
          server.kill("SIGTERM");
        });
      }
      const stoppedAt = Date.now();
      await sleep(17_000);
      const restartedAt = Date.now();
      await harness.start();
      // Path B admin connections disable reconnect, so the pre-stop JetStream manager is stale.
      inboxManager = await jetstreamManager(await harness.connect(harness.users.bootstrap));
      const recoveryDeadline = Date.now() + 35_000;
      await waitUntil(() => owners.every(owner => owner.verifiedReconnects > 0 && owner.directory.canConsumeMesh()),
        "every participant's verified reconnect and live lease after restart", recoveryDeadline);
      // Join the confirmed post-reconnect refresh before allowing any routed send. The own-key
      // KV revision proves the force write reached JetStream, not merely that a cached route exists.
      await Promise.all(owners.map(async owner => {
        const before = initialPresenceRevisions.get(owner.id);
        if (before === undefined) throw new Error(`initial presence revision is unavailable for ${owner.id}`);
        await owner.directory.refresh();
        const presence = await owner.transport.kv("presence").get<FabricParticipantRecord>(presenceKeyFor(owner.id), AbortSignal.timeout(5_000));
        if (!presence) throw new Error(`confirmed participant presence is missing after reconnect for ${owner.id}`);
        expect(presence.revision).toBeGreaterThan(before);
        expect(presence.owner).toBe(`${owner.id}#${owner.directory.ownerIncarnation}`);
        expect(owner.directory.confirmedAt()).toBeGreaterThanOrEqual(owner.verifiedReconnectAt);
      }));
      await waitUntil(allViewsReady, "all peer views are live after confirmed post-reconnect writes", recoveryDeadline);

      // These are ordinary routed sends, each issued only after its own transport's
      // verified reconnect, confirmed forced presence write, and full 22-peer view.
      afterReconnect = await Promise.all(owners.map((sender, index) =>
        settleSend(sender, `after-verified-reconnect:${index}`, (index + 1) % PARTICIPANTS)));
      expect(afterReconnect.every(outcome => outcome.outcome === "ack")).toBe(true);
      await sleep(1_000); // Keep all 22 background senders running briefly after reconnection.

      stopTraffic = true;
      await Promise.all(workers);
      expect(workerFailure).toBeUndefined();
      expect(outcomes.length).toBeGreaterThanOrEqual(PARTICIPANTS * 2);
      expect(outcomes.every(outcome => outcome.outcome === "ack" || outcome.outcome === "error")).toBe(true);
      expect(outcomes.filter(outcome => outcome.outcome === "error").every(outcome => Boolean(outcome.error?.message))).toBe(true);
      expect(settlementDeadlines).toEqual([]);
      expect(new Set(outcomes.map(outcome => outcome.idempotencyKey)).size).toBe(outcomes.length);
      expect(owners.every(owner => outcomes.some(outcome => outcome.senderId === owner.id && outcome.startedAt < stoppedAt))).toBe(true);
      expect(owners.every(owner => outcomes.some(outcome => outcome.senderId === owner.id && outcome.startedAt >= stoppedAt && outcome.startedAt < restartedAt))).toBe(true);
      expect(afterReconnect.map(outcome => outcome.senderId).sort()).toEqual(owners.map(owner => owner.id).sort());

      const replay = afterReconnect[0]!;
      const replayEvidence = [...commandSends.entries()].find(([, evidence]) => {
        const body = evidence.envelope?.body;
        return body !== null && body !== undefined && typeof body === "object" && "message" in body && body.message === `restart-stress:${replay.idempotencyKey}`;
      });
      const replayEnvelope = replayEvidence?.[1].envelope;
      if (!replayEvidence || !replayEnvelope) throw new Error("post-reconnect command has no captured durable envelope");
      const [replayEnvelopeId, evidence] = replayEvidence;
      const replaySequence = evidence.sequences[0];
      const replayOwner = owners.find(owner => owner.id === replay.senderId);
      if (!replayOwner) throw new Error(`replay sender disappeared: ${replay.senderId}`);
      const replayCommandId = replay.commandId;
      if (!replayCommandId) throw new Error("post-reconnect ACK omitted its existing Fabric command id");
      const executionsBeforeReplay = executions.get(replayCommandId) ?? 0;
      const duplicate = await replayOwner.transport.send(replayEnvelope);
      expect(replaySequence).toBeTypeOf("number");
      expect(duplicate).toMatchObject({ id: replayEnvelopeId, duplicate: true, sequence: replaySequence });
      expect(evidence.duplicates).toBeGreaterThan(0);
      expect(executions.get(replayCommandId) ?? 0).toBe(executionsBeforeReplay);

    } finally {
      stopTraffic = true;
      await Promise.all(workers);
    }

    expect(workerFailure).toBeUndefined();
    const acknowledgedMessages = new Set(outcomes.filter(outcome => outcome.outcome === "ack")
      .map(outcome => `restart-stress:${outcome.idempotencyKey}`));
    const stored = await waitForAcknowledgedStressRecords(inboxBaseline, expectedMessages, acknowledgedMessages, Date.now() + 15_000);
    const storedCountByEnvelopeId = new Map<string, number>();
    const storedCommandIds = new Set<string>();
    for (const record of stored) {
      storedCountByEnvelopeId.set(record.envelopeId, (storedCountByEnvelopeId.get(record.envelopeId) ?? 0) + 1);
      storedCommandIds.add(record.commandId);
      expect(executions.get(record.commandId) ?? 0).toBeLessThanOrEqual(1);
    }
    for (const count of storedCountByEnvelopeId.values()) expect(count).toBe(1);
    const acknowledgedCommandIds = outcomes.filter(outcome => outcome.outcome === "ack").map(outcome => outcome.commandId);
    expect(new Set(acknowledgedCommandIds).size).toBe(acknowledgedCommandIds.length);
    for (const outcome of outcomes) {
      if (outcome.outcome === "ack") {
        if (!outcome.commandId) throw new Error(`ACK omitted the Fabric command id for ${outcome.idempotencyKey}`);
        expect(stored.filter(record => record.commandId === outcome.commandId)).toHaveLength(1);
        expect(executions.get(outcome.commandId)).toBe(1);
      }
    }
    for (const [envelopeId, evidence] of commandSends) {
      const body = evidence.envelope?.body;
      if (body === null || body === undefined || typeof body !== "object" || !("message" in body) ||
          typeof body.message !== "string" || !expectedMessages.has(body.message)) continue;
      if (evidence.duplicates > 0 || (evidence.attempts > 1 && evidence.sequences.length > 0)) {
        expect(storedCountByEnvelopeId.get(envelopeId)).toBe(1);
      }
    }
    expect(storedCommandIds.size).toBeGreaterThanOrEqual(afterReconnect.length);

    // Preserve B3's close-after-restart coverage: the ephemeral durable must survive restart,
    // then be removed when its owner closes, without masking a stale consumer as success.
    const verifier = await harness.connect(harness.users.bootstrap);
    try {
      const manager = await jetstreamManager(verifier);
      const ephemeral = owners[0]!;
      const stream = transportInboxStream(MACHINE);
      const durable = transportDurableName(ephemeral.id);
      expect((await manager.consumers.info(stream, durable)).config.inactive_threshold).toBe(300_000_000_000);

      const closeResults = await Promise.all(owners.map(async owner => {
        const failures: Array<{ phase: string; error: string }> = [];
        const summarize = (error: unknown): string => error instanceof AggregateError
          ? `AggregateError: ${error.errors.map(summarize).join("; ")}`
          : error instanceof Error ? `${error.name}: ${error.message}` : String(error);
        for (const [phase, close] of [
          ["control", () => owner.control!.close()],
          ["directory", () => owner.directory.close()],
          ["transport", () => owner.transport.close()],
        ] as const) {
          try { await close(); } catch (error) { failures.push({ phase, error: summarize(error) }); }
        }
        try { owner.mesh.closeState(); } catch (error) { failures.push({ phase: "mesh", error: summarize(error) }); }
        return { id: owner.id, failures };
      }));
      expect(closeResults.filter(result => result.failures.length > 0)).toEqual([]);
      await expect(manager.consumers.info(stream, durable)).rejects.toMatchObject({ code: 10014, status: 404 });
    } finally {
      await verifier.close();
    }
  }, 120_000);
});
