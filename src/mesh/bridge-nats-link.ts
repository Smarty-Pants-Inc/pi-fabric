import path from "node:path";
import type { FabricParticipantRecord } from "../topology/types.js";
import { assertNatsLinkPlatform } from "./bridge-nats-config.js";
import { BRIDGE_STALL_ALARM_TOPIC, type BridgeStallAlarm } from "./bridge.js";
import type {
  NatsCarrierAuthorize, NatsCarrierDecision, NatsCarrierLedger, NatsCarrierRecord,
} from "./bridge-nats-carrier.js";
import { createNatsFileLink, type NatsCarrierSide } from "./bridge-nats-file.js";
import {
  createBridgeNatsIdentityGate, loadBridgeNatsSeatPins, parseBridgeNatsAllowSection, type BridgeNatsSeatPins,
} from "./bridge-nats-identity.js";
import { bridgeNatsClaimAction, openBridgeNatsLedger, type FileBridgeNatsLedger } from "./bridge-nats-ledger.js";
import { openNatsBridgeSide, type NatsBridgeSideConfig } from "./bridge-nats-port.js";
import type { NatsTopicAuthorize } from "./bridge-nats-topics.js";
import {
  bridgeNatsLinkIdentity, type BridgeNatsAllowDecision, type BridgeNatsAllowListConfig, type BridgeNatsAllowSection,
  type BridgeNatsCommandRecord, type BridgeNatsIdentityGate, type BridgeNatsLedger, type BridgeNatsPort,
  type BridgeNatsPresenceView,
} from "./bridge-nats-types.js";
import { MeshStore, meshAlarmRecipient } from "./store.js";
import { systemdWatchdogNotify } from "./systemd-watchdog.js";

/**
 * The A' link (smarty-dev#15310, lane L2): the hub's file mesh <-> the org's NATS broker, as the
 * existing MeshBridge with the file store as its local StoreBridgeSide and NatsCarrierSide (over
 * lane L1's BridgeNatsPort) as its remote. L5's `nats-link start` calls startNatsLink.
 *
 * Every crossing passes three independent checks: MeshBridge's own (sender from presence,
 * addressed recipient, held mirrors at commit), the carrier's (verified presence on the source
 * side, the KV source machine, ledger-bound ACKs), and L3's allow gate (static list, destination
 * class, principal carry or strip). There is no relay hook: spoke relays and NATS -> factory
 * topics are PR 2, and MeshBridge fails a cross-spoke command closed without one.
 */

const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;
const STOP_MS = 5_000;

/** What startNatsLink reads from the link config (L5's parsed NatsLinkConfig is assignable to it). */
export interface NatsLinkStartConfig {
  /** This hub's bridge name (MeshBridge localName). */
  readonly name: string;
  /** The link name (MeshBridge remoteName; the remoteHost of NATS mirrors), e.g. `v2`. */
  readonly link: string;
  readonly file: {
    readonly mesh: string;
    readonly lockProtocol: 1 | 2 | 3;
    readonly stateBackend?: "file" | "shadow" | "sqlite";
    readonly maxStateBytes?: number;
  };
  readonly stateDir: string;
  /** The bridge machine's NATS settings (L1's openNatsBridgeSide reads them). */
  readonly nats: NatsBridgeSideConfig["nats"];
  /** L5 hands the allow-list already parsed by L3; a raw `allow` section is parsed here the same way. */
  readonly allow: BridgeNatsAllowListConfig | BridgeNatsAllowSection;
  /** The org.json whose principal seats pin the allow-list; absent = L3's default candidates. */
  readonly orgJson?: string;
}

const parsedAllow = (value: BridgeNatsAllowListConfig | BridgeNatsAllowSection): value is BridgeNatsAllowListConfig =>
  "entries" in value && Array.isArray((value as BridgeNatsAllowListConfig).entries);

export interface NatsLinkPortOptions {
  readonly signal: AbortSignal;
  readonly log: (line: string) => void;
  /** The hub store the link opened: the port's native file-actor listing (N1) reads it. */
  readonly fileStore: MeshStore;
}

/** The factories the link composes (L1's and L3's by default); tests inject fakes. */
export interface NatsLinkFactories {
  /** L1: the raw NATS port for this config (connection, KV, inbox), closed by the link on stop; default openNatsBridgeSide. */
  openPort?(config: NatsBridgeSideConfig, options: NatsLinkPortOptions): Promise<BridgeNatsPort>;
  /** L3: the crash-durable ledger in `directory`; default openBridgeNatsLedger. */
  openLedger?(directory: string): Promise<BridgeNatsLedger>;
  /** org.json seat pins; default: L3's loadBridgeNatsSeatPins (smarty-send's strict reader). */
  seatPins?(): BridgeNatsSeatPins;
}

export interface NatsLinkStartOptions {
  readonly signal: AbortSignal;
  readonly log: (line: string) => void;
  readonly factories?: NatsLinkFactories;
}

export interface NatsLinkHandle {
  /** Settles when the link has stopped: resolves after stop() or an abort, rejects when the link failed. */
  readonly closed: Promise<void>;
  stop(): Promise<void>;
}

const decision = (allowed: BridgeNatsAllowDecision): NatsCarrierDecision =>
  allowed.ok ? { ok: true, principal: allowed.principal } : { ok: false, refuse: allowed.refuse, reason: allowed.reason };

/**
 * L3's gate as the carrier's and N3's authorize hooks. Both identities are verified again here, from
 * the presence views of their own sides (the NATS sender on its delivery's source machine), before
 * allow() decides; the carrier's own checks stay in force as well.
 */
export const natsLinkAuthorizers = (gate: BridgeNatsIdentityGate, views: {
  readonly file: () => BridgeNatsPresenceView; readonly nats: () => BridgeNatsPresenceView;
}): { readonly authorize: NatsCarrierAuthorize; readonly authorizeTopic: NatsTopicAuthorize } => ({
  authorize: request => {
    const source = request.direction === "toNats" ? "file" : "nats";
    const destination = request.direction === "toNats" ? "nats" : "file";
    const sourceView = views[source](), destinationView = views[destination]();
    const sender = gate.verifySender(source, request.senderId, sourceView, destinationView, request.sourceMachine);
    if (!sender.ok) return { ok: false, refuse: sender.refuse, reason: sender.reason };
    const target = gate.verifyTarget(destination, request.targetId, destinationView, sourceView);
    if (!target.ok) return { ok: false, refuse: target.refuse, reason: target.reason };
    return decision(gate.allow({ direction: request.direction, sender: sender.identity, target: target.identity,
      subject: { type: "control", operation: request.operation }, principal: request.principal }));
  },
  authorizeTopic: request => {
    const natsView = views.nats(), fileView = views.file();
    const target = gate.verifyTarget("nats", request.targetId, natsView, fileView);
    if (!target.ok) return { ok: false, refuse: target.refuse, reason: target.reason };
    return decision(gate.allow({ direction: "toNats", sender: { side: "file", native: true, fromId: request.fromId },
      target: target.identity, subject: { type: "topic", topic: request.topic, kind: request.kind } }));
  },
});

/** Only file identities on the allow-list are mirrored into KV (roots by id; actors by root + name, and id when pinned). */
export const natsLinkMirrorAllowed = (allow: BridgeNatsAllowListConfig) =>
  (participant: Pick<FabricParticipantRecord, "id" | "kind" | "rootId" | "name">): boolean =>
    allow.entries.some(entry => entry.side === "file" && (entry.kind === "root"
      ? participant.kind === "root" && participant.id === entry.id
      : participant.kind === "actor" && participant.rootId === entry.rootId && participant.name === entry.name &&
        (entry.id === undefined || entry.id === participant.id)));

/**
 * Whether an inbound command's target is named on allow.file, so a moment's KV lag may be waited for:
 * a root by id, a pinned actor by id, an unpinned actor by the native file record answering to the id.
 */
export const natsLinkFileTargetAllowed = (allow: BridgeNatsAllowListConfig, fileView: () => BridgeNatsPresenceView) =>
  (targetId: string): boolean => allow.entries.some(entry => entry.side === "file" && (entry.kind === "root"
    ? entry.id === targetId
    : entry.id !== undefined ? entry.id === targetId
      : fileView().records(targetId).some(record => record.kind === "actor" && record.mirroredBy === undefined &&
        record.rootId === entry.rootId && record.name === entry.name)));

/** L3's durable ledger as the carrier's ledger port. */
export const natsCarrierLedger = (ledger: BridgeNatsLedger, link: string): NatsCarrierLedger => {
  const linkId = bridgeNatsLinkIdentity(link).id;
  const record = (stored: BridgeNatsCommandRecord): NatsCarrierRecord => ({
    v: 1, direction: stored.direction, origin: stored.origin ?? (stored.requesterId === linkId ? "topic" : "control"), link,
    operation: stored.operation, sourceCommandId: stored.originalCommandId, destinationCommandId: stored.commandId,
    // Toward the file mesh the destination id is the source envelope id (markSent's file event id is bookkeeping).
    sourceEventId: stored.sourceEventId,
    destinationEventId: stored.direction === "toNats" ? stored.envelopeId ?? stored.sourceEventId : stored.sourceEventId,
    targetId: stored.targetId, ownerHostId: stored.ownerHostId, ownerIdentityId: stored.ownerIdentityId,
    ...(stored.targetMachine !== undefined ? { ownerMachine: stored.targetMachine } : {}),
    requesterId: stored.requesterId, from: stored.from, ...(stored.principal !== undefined ? { principal: stored.principal } : {}),
    sourceMachine: stored.sourceMachine,
    ...(stored.originalCancelCommandId !== undefined ? { sourceCancelCommandId: stored.originalCancelCommandId } : {}),
    ...(stored.cancelCommandId !== undefined ? { cancelCommandId: stored.cancelCommandId } : {}),
    replyTo: stored.replyTo, bodyDigest: stored.bodyDigest,
    deadlineAt: stored.deadlineAt, expiresAt: stored.expiresAt,
    ...(stored.ownerIncarnation !== undefined ? { ownerIncarnation: stored.ownerIncarnation } : {}),
    ...(stored.resolvedBody !== undefined ? { body: stored.resolvedBody } : {}),
    ...(stored.ack !== undefined ? { outcome: { accepted: stored.ack.accepted, at: stored.ack.receivedAt, fromId: stored.ack.fromId,
      ackEventId: stored.ack.ackEventId, data: stored.ack.data } } : {}),
  });
  return {
    async claim(input) {
      const result = await ledger.claim({
        direction: input.direction, origin: input.origin, originalCommandId: input.sourceCommandId, commandId: input.destinationCommandId,
        sourceEventId: input.sourceEventId, ...(input.direction === "toNats" ? { envelopeId: input.destinationEventId } : {}),
        ...(input.cancelCommandId !== undefined ? { cancelCommandId: input.cancelCommandId } : {}),
        ...(input.sourceCancelCommandId !== undefined ? { originalCancelCommandId: input.sourceCancelCommandId } : {}),
        from: input.from, ...(input.principal !== undefined ? { principal: input.principal } : {}),
        bodyDigest: input.bodyDigest, sourceMachine: input.sourceMachine, requesterId: input.requesterId, replyTo: input.replyTo,
        targetId: input.targetId, operation: input.operation, ownerHostId: input.ownerHostId, ownerIdentityId: input.ownerIdentityId,
        ...(input.ownerIncarnation !== undefined ? { ownerIncarnation: input.ownerIncarnation } : {}),
        ...(input.ownerMachine !== undefined ? { targetMachine: input.ownerMachine } : {}),
        deadlineAt: input.deadlineAt, ...(input.body !== undefined ? { resolvedBody: input.body } : {}),
      });
      // L3's one never-blocking decision per claim, verbatim.
      const decided = bridgeNatsClaimAction(result);
      return { created: result.created, record: record(result.record), action: decided.action,
        ...(decided.action === "refuse" ? { refuse: decided.refuse } : {}) };
    },
    async lookup(direction, id) {
      const stored = ledger.lookup(direction, id);
      return stored ? record(stored) : undefined;
    },
    async projectNativeIngress(input) {
      // The actual startNatsLink owns this ledger and pins `link` in this closure. Neither paths
      // nor pins come from an event body. Legacy/injected ledgers have no receipt-minting power.
      const projection = ledger as BridgeNatsLedger & Partial<Pick<FileBridgeNatsLedger, "projectNativeIngress">>;
      await projection.projectNativeIngress?.(input.destinationCommandId, link);
    },
    async settle(direction, id, outcome) {
      const stored = ledger.lookup(direction, id);
      if (!stored) return;
      await ledger.settle(id, { v: 1, commandId: id, accepted: outcome.accepted, fromId: outcome.fromId ?? stored.ownerIdentityId,
        ackEventId: outcome.ackEventId ?? `settled:${id}`, data: { ...(outcome.data ?? { accepted: outcome.accepted }) },
        receivedAt: outcome.at });
    },
    async sent(direction, id, receipt) {
      // Toward the file mesh markSent needs the appended event id, which MeshBridge keeps: bookkeeping toward NATS only.
      if (direction === "toNats" && ledger.lookup(direction, id)) {
        await ledger.markSent(id, receipt.sequence !== undefined ? { sequence: receipt.sequence } : {});
      }
    },
    async forwarded(direction, id) {
      if (ledger.lookup(direction, id)?.ack !== undefined) await ledger.markAckForwarded(id);
    },
  };
};

/**
 * Start the link: open the file store, the gate (allow-list plus org.json seat pins), L1's port and
 * L3's ledger, then MeshBridge. Resolves once the first presence pass has linked both sides.
 */
export const startNatsLink = async (config: NatsLinkStartConfig, options: NatsLinkStartOptions): Promise<NatsLinkHandle> => {
  // The hub runs Linux or macOS; the ledger, lock and config checks rely on POSIX file semantics.
  assertNatsLinkPlatform();
  const { signal, log } = options;
  const factories = options.factories ?? {};
  signal.throwIfAborted();
  // The allow-list and the seat pins are checked before anything is opened: a bad list never links.
  const allow = parsedAllow(config.allow) ? config.allow
    : parseBridgeNatsAllowSection(config.allow, { link: config.link, natsMachine: config.nats.machine });
  if (allow.link !== config.link || allow.natsMachine !== config.nats.machine) {
    throw new Error("startNatsLink: the allow-list was parsed for another link or NATS machine");
  }
  const gate = createBridgeNatsIdentityGate(allow, { pins: factories.seatPins?.() ?? loadBridgeNatsSeatPins(config.orgJson) });
  // L1's port admits presence from the allow section's two lists (the side is the list).
  const portConfig: NatsBridgeSideConfig = { link: config.link, nats: config.nats, allow: {
    file: allow.entries.filter(entry => entry.side === "file"), nats: allow.entries.filter(entry => entry.side === "nats") } };
  const store = new MeshStore(config.file.mesh, MAX_EVENT_BYTES, MAX_READ_EVENTS, {
    lockProtocol: config.file.lockProtocol,
    ...(config.file.stateBackend !== undefined ? { stateBackend: config.file.stateBackend } : {}),
    ...(config.file.maxStateBytes !== undefined ? { maxStateBytes: config.file.maxStateBytes } : {}),
  });
  let port: BridgeNatsPort | undefined;
  let opened: BridgeNatsLedger | undefined;
  try {
    await store.prepareLock();
    signal.throwIfAborted();
    port = factories.openPort
      ? await factories.openPort(portConfig, { signal, log, fileStore: store })
      : await openNatsBridgeSide(portConfig, { signal, log, fileStore: store, onError: error =>
        log(`NATS port: ${error instanceof Error ? error.message : String(error)}`) });
    const ledgerDir = path.join(config.stateDir, "ledger");
    opened = await (factories.openLedger ? factories.openLedger(ledgerDir) : openBridgeNatsLedger({ dir: ledgerDir }));
    const ledger = natsCarrierLedger(opened, config.link);
    let remote: NatsCarrierSide | undefined;
    const openedPort = port;
    const { authorize, authorizeTopic } = natsLinkAuthorizers(gate, {
      file: () => remote!.filePresence.view(), nats: () => openedPort.presenceView(),
    });
    const identity = { id: `bridge:${config.name}`, name: "mesh-bridge", kind: "main" as const };
    const link = createNatsFileLink({
      store, localName: config.name, link: config.link, cursorPath: path.join(config.stateDir, "cursor.json"),
      port, ledger, authorize, authorizeTopic, mirrorAllowed: natsLinkMirrorAllowed(allow), log,
      // L1's port lists native file actors itself (N1): the mirror set carries roots only.
      mirrorActors: false,
      waitForTarget: natsLinkFileTargetAllowed(allow, () => remote!.filePresence.view()),
      bridge: {
        stopMs: STOP_MS, notifyWatchdog: systemdWatchdogNotify,
        publishStallAlarm: async (alarm: BridgeStallAlarm) => {
          const to = meshAlarmRecipient();
          if (!to) { log("stall alarm has no configured mesh alarm session recipient"); return; }
          await store.publish({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall", from: identity, to, text: alarm.reason, data: alarm,
            dedupeKey: `bridge-stall:${config.name}:${config.link}:${alarm.stallId}` });
        },
      },
    });
    remote = link.remote;
    const { bridge } = link;
    await bridge.startWithBackoff();
    signal.throwIfAborted();
    log(`linked ${config.name} <-> ${config.link} (NATS machine ${port.machine})`);
    let stopping: Promise<void> | undefined;
    const openedLedger = opened;
    const stop = (): Promise<void> => stopping ??= (async () => {
      await bridge.stop().catch(() => undefined);
      await openedPort.close().catch(() => undefined);
      await openedLedger.close().catch(() => undefined);
      store.closeState();
    })();
    const onAbort = (): void => { void stop(); };
    signal.addEventListener("abort", onAbort, { once: true });
    const closed = bridge.run().then(
      async () => { signal.removeEventListener("abort", onAbort); await stop(); },
      async (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        await stop();
        if (!signal.aborted) throw error;
      });
    void closed.catch(() => undefined);
    return { closed, stop };
  } catch (error) {
    await port?.close().catch(() => undefined);
    await opened?.close().catch(() => undefined);
    store.closeState();
    throw error;
  }
};
