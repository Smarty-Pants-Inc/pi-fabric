import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { FabricParticipantRecord } from "../topology/types.js";
import { assertNatsLinkPlatform } from "./bridge-nats-config.js";
import { openNatsLinkCounters, type NatsLinkCounters } from "./bridge-nats-counters.js";
import { BRIDGE_STALL_ALARM_TOPIC, type BridgePublish, type BridgeStallAlarm } from "./bridge.js";
import { BridgeRelayUnavailableError, startBridgeRelay } from "./bridge-relay.js";
import type {
  NatsCarrierAuthorize, NatsCarrierDecision, NatsCarrierLedger, NatsCarrierRecord,
} from "./bridge-nats-carrier.js";
import { createNatsFileLink, type NatsCarrierSide } from "./bridge-nats-file.js";
import {
  bridgeNatsTopicMatches, createBridgeNatsIdentityGate, loadBridgeNatsSeatPins, parseBridgeNatsAllowSection, type BridgeNatsSeatPins,
} from "./bridge-nats-identity.js";
import { bridgeNatsClaimAction, openBridgeNatsLedger } from "./bridge-nats-ledger.js";
import { openNatsBridgeSide, type NatsBridgeSideConfig } from "./bridge-nats-port.js";
import { startNatsRepublishLink, type NatsRepublishHandle } from "./bridge-nats-republish.js";
import type { NatsTopicAuthorize } from "./bridge-nats-topics.js";
import {
  BRIDGE_NATS_CLASS_RECEIVE, BRIDGE_NATS_REFUSAL, bridgeNatsDestinationClass, bridgeNatsLinkIdentity, type BridgeNatsAllowDecision, type BridgeNatsAllowListConfig, type BridgeNatsAllowSection,
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
 * class, principal carry or strip).
 *
 * PR 2: N4 republishes v2 work topics into the file mesh (nats.events), and the link joins the hub's
 * relay between bridges (relay.spokes), so a work-host lane on a spoke reaches a v2 root and back.
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
  /** The bridge machine's NATS settings (L1's openNatsBridgeSide reads them); `events` turns on N4 (PR 2). */
  readonly nats: NatsBridgeSideConfig["nats"] & { readonly events?: boolean };
  /** L5 hands the allow-list already parsed by L3; a raw `allow` section is parsed here the same way. */
  readonly allow: BridgeNatsAllowListConfig | BridgeNatsAllowSection;
  /** The org.json whose principal seats pin the allow-list; absent = L3's default candidates. */
  readonly orgJson?: string;
  /**
   * PR 2 relay: the hub's spoke links (their `mesh-bridge run --remote` names). A viaSpoke allow entry must
   * name one, and with any configured the link joins the hub relay (bin/nats-link start, Linux).
   */
  readonly relay?: { readonly spokes: readonly string[] };
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
  /** The hub relay registration (L3's peer-authenticated UDS relay); default startBridgeRelay. */
  startRelay?(options: Parameters<typeof startBridgeRelay>[0]): ReturnType<typeof startBridgeRelay>;
}

export interface NatsLinkStartOptions {
  readonly signal: AbortSignal;
  readonly log: (line: string) => void;
  readonly factories?: NatsLinkFactories;
  /**
   * In-process TEST seam only (integrator note 18), the shape of MeshBridge's own `relay`: send one event to
   * another hub link. Given, the link registers NO UDS relay, and its handle offers deliverRelay for the
   * inbound half. Neither the config file nor the CLI can set it: production always uses the UDS relay.
   */
  readonly relay?: (destinationRemote: string, event: BridgePublish) => Promise<void>;
}

export interface NatsLinkHandle {
  /** Settles when the link has stopped: resolves after stop() or an abort, rejects when the link failed. */
  readonly closed: Promise<void>;
  stop(): Promise<void>;
  /** Only with options.relay (the test seam): deliver one event another hub link relayed (MeshBridge.deliverRelay). */
  deliverRelay?(sourceRemote: string, event: BridgePublish): Promise<void>;
}

/** This release's `bin/nats-link`: the only entry the hub relay admits as this link (src/mesh, dist/mesh or dist/chunks). */
export const natsLinkBinPath = (): string => fileURLToPath(new URL("../../bin/nats-link", import.meta.url));

const decision = (allowed: BridgeNatsAllowDecision): NatsCarrierDecision =>
  allowed.ok ? { ok: true, principal: allowed.principal } : { ok: false, refuse: allowed.refuse, reason: allowed.reason };

/**
 * L3's gate as the carrier's and N3's authorize hooks. Both identities are verified again here, from
 * the presence views of their own sides (the NATS sender on its delivery's source machine), before
 * allow() decides; the carrier's own checks stay in force as well. PR 2 relay: a relayed sender must
 * verify as a spoke root of exactly the spoke link it came through, an unrelayed one as no spoke root;
 * a spoke root target is handed back with its spoke, so the command is relayed there.
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
    if (sender.identity.viaSpoke !== request.viaSpoke) {
      return { ok: false, refuse: BRIDGE_NATS_REFUSAL.senderUnverified, reason: request.viaSpoke === undefined
        ? `${request.senderId} is a spoke root; it sends only through its spoke link's relay`
        : `${request.senderId} is not a root that spoke link ${request.viaSpoke} relays` };
    }
    const target = gate.verifyTarget(destination, request.targetId, destinationView, sourceView);
    if (!target.ok) return { ok: false, refuse: target.refuse, reason: target.reason };
    const allowed = decision(gate.allow({ direction: request.direction, sender: sender.identity, target: target.identity,
      subject: { type: "control", operation: request.operation }, principal: request.principal }));
    const spoke = request.direction === "toFile" ? target.identity.viaSpoke : undefined;
    return allowed.ok && spoke !== undefined ? { ...allowed, viaSpoke: spoke } : allowed;
  },
  authorizeTopic: request => {
    const natsView = views.nats(), fileView = views.file();
    const target = gate.verifyTarget("nats", request.targetId, natsView, fileView);
    if (!target.ok) return { ok: false, refuse: target.refuse, reason: target.reason };
    return decision(gate.allow({ direction: "toNats", sender: { side: "file", native: true, fromId: request.fromId },
      target: target.identity, subject: { type: "topic", topic: request.topic, kind: request.kind } }));
  },
});

/**
 * Only file identities on the allow-list are mirrored into KV (roots by id; actors by root + name, and id when
 * pinned). A root's stored origin must match its entry: native for a hub root, its own spoke link for a spoke
 * root (PR 2 relay), so no other link's mirror is ever held under an allow-listed id.
 */
export const natsLinkMirrorAllowed = (allow: BridgeNatsAllowListConfig) =>
  (participant: Pick<FabricParticipantRecord, "id" | "kind" | "rootId" | "name" | "remoteHost">): boolean =>
    allow.entries.some(entry => entry.side === "file" && (entry.kind === "root"
      ? participant.kind === "root" && participant.id === entry.id && participant.remoteHost === entry.viaSpoke
      : participant.kind === "actor" && participant.remoteHost === undefined && participant.rootId === entry.rootId &&
        participant.name === entry.name && (entry.id === undefined || entry.id === participant.id)));

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

/**
 * N4 (security pass P3): whether a republished v2 event may keep `to` naming this hub recipient. Recipients
 * opt in: only an allow.file hub root whose receiveTopics list the topic, and that receives followUp within its
 * destination class (a topic event lands in a root's inbox as a followUp does, so a principal seat or any root
 * with receive [] gets none). Never a spoke root (the hub never forwards a bridged event) or an actor (an
 * actor entry lists no topics). The hub factory is decided in the republish leg (isAddressedHubFactoryEvent).
 */
export const natsLinkRepublishRecipient = (allow: BridgeNatsAllowListConfig) => (to: string, topic: string): boolean =>
  allow.entries.some(entry => entry.side === "file" && entry.kind === "root" && entry.id === to && entry.viaSpoke === undefined &&
    entry.receive.includes("followUp") && BRIDGE_NATS_CLASS_RECEIVE[bridgeNatsDestinationClass(entry)].includes("followUp") &&
    (entry.receiveTopics ?? []).some(pattern => bridgeNatsTopicMatches(pattern, topic)));

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
 * L3's ledger, then MeshBridge, then (PR 2, with relay.spokes) the hub relay. Resolves once the first
 * presence pass has linked both sides.
 */
export const startNatsLink = async (config: NatsLinkStartConfig, options: NatsLinkStartOptions): Promise<NatsLinkHandle> => {
  // The hub runs Linux or macOS; the ledger, lock and config checks rely on POSIX file semantics.
  assertNatsLinkPlatform();
  const { signal, log } = options;
  const factories = options.factories ?? {};
  signal.throwIfAborted();
  // The allow-list and the seat pins are checked before anything is opened: a bad list never links.
  // PR 2: a viaSpoke entry must name one of relay.spokes (the same parse as L5's config).
  const spokes = config.relay?.spokes ?? [];
  const allow = parsedAllow(config.allow) ? config.allow
    : parseBridgeNatsAllowSection(config.allow, { link: config.link, natsMachine: config.nats.machine, spokes });
  if (allow.link !== config.link || allow.natsMachine !== config.nats.machine) {
    throw new Error("startNatsLink: the allow-list was parsed for another link or NATS machine");
  }
  if (allow.entries.some(entry => entry.kind === "root" && entry.viaSpoke !== undefined && !spokes.includes(entry.viaSpoke))) {
    throw new Error("startNatsLink: a viaSpoke allow entry names no configured relay.spokes link");
  }
  // N4 reads republishTopics from the org's event stream: without nats.events they would silently do nothing.
  if (config.nats.events !== true &&
      allow.entries.some(entry => entry.side === "nats" && entry.kind === "root" && (entry.republishTopics?.length ?? 0) > 0)) {
    throw new Error("startNatsLink: an allow.nats entry lists republishTopics, which need nats.events");
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
  let republish: NatsRepublishHandle | undefined;
  let counters: NatsLinkCounters | undefined;
  let udsRelay: Awaited<ReturnType<typeof startBridgeRelay>> | undefined;
  try {
    await store.prepareLock();
    signal.throwIfAborted();
    // L5's crossing counters (`nats-link status`), one snapshot in the state dir, closed on stop.
    fs.mkdirSync(config.stateDir, { recursive: true, mode: 0o700 });
    counters = openNatsLinkCounters(config.stateDir, config.link);
    const openedCounters = counters;
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
    // PR 2 relay: MeshBridge's delivery-only hub-link IPC. The test seam replaces it; otherwise the UDS relay
    // registered below carries it, and until then (or without one) a cross-link command fails closed.
    const relay = options.relay ?? (async (destinationRemote: string, event: BridgePublish): Promise<void> => {
      if (!udsRelay || signal.aborted) throw new BridgeRelayUnavailableError();
      await udsRelay.send(destinationRemote, event);
    });
    const link = createNatsFileLink({
      store, localName: config.name, link: config.link, cursorPath: path.join(config.stateDir, "cursor.json"),
      port, ledger, authorize, authorizeTopic, mirrorAllowed: natsLinkMirrorAllowed(allow), log, relay, counters: openedCounters,
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
    // N4 (PR 2, lane L1): with nats.events, v2 work topics are republished into this file mesh, decided by
    // L3's gate (verifyEventSender + allowRepublish) on both sides' presence. A missing consume grant
    // refuses the start here, before MeshBridge links anything.
    // An addressed v2 event keeps its `to` only for the hub factory or an allow.file root that opted in to its topic.
    republish = await startNatsRepublishLink(config, { store, port, gate, ledger: opened, signal, log, counters: openedCounters,
      recipientAllowed: natsLinkRepublishRecipient(allow),
      views: { refresh: () => remote!.filePresence.refresh(), nats: () => openedPort.presenceView(),
        file: () => remote!.filePresence.view() } });
    await bridge.startWithBackoff();
    signal.throwIfAborted();
    // PR 2 relay: join the hub's relay between bridges once linked, as `mesh-bridge run` does. Only with spokes
    // configured, never beside the test seam. A refusal (another release, a config that is not private, the
    // slot held, not Linux) costs only the relay: hub-local traffic goes on, cross-link commands fail closed.
    if (!options.relay && spokes.length > 0) {
      try {
        udsRelay = await (factories.startRelay ?? startBridgeRelay)({ localName: config.name, remoteName: config.link,
          meshRoot: store.root, bridgeScriptPath: fs.realpathSync(natsLinkBinPath()),
          deliver: (sourceRemote, event) => bridge.deliverRelay(sourceRemote, event) });
        log(`relay: joined the hub relay as ${config.link} (spokes ${spokes.join(", ")})`);
      } catch {
        // Never log the refusal's details (bridge-relay's rule); the operator sees the relay is down.
        log(`relay unavailable; hub-local link remains active; commands between ${spokes.join(", ")} and ${config.link} fail closed`);
      }
      signal.throwIfAborted();
    }
    log(`linked ${config.name} <-> ${config.link} (NATS machine ${port.machine})`);
    let stopping: Promise<void> | undefined;
    const openedLedger = opened;
    const openedRepublish = republish;
    const stop = (): Promise<void> => stopping ??= (async () => {
      const joined = udsRelay;
      udsRelay = undefined;
      await joined?.close().catch(() => undefined);
      await openedRepublish?.stop().catch(() => undefined);
      await bridge.stop().catch(() => undefined);
      await openedPort.close().catch(() => undefined);
      await openedLedger.close().catch(() => undefined);
      try { openedCounters.close(); } catch (error) { log(`counters: ${error instanceof Error ? error.message : String(error)}`); }
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
    return { closed, stop,
      ...(options.relay ? { deliverRelay: (sourceRemote: string, event: BridgePublish) => bridge.deliverRelay(sourceRemote, event) } : {}) };
  } catch (error) {
    await udsRelay?.close().catch(() => undefined);
    await republish?.stop().catch(() => undefined);
    await port?.close().catch(() => undefined);
    await opened?.close().catch(() => undefined);
    try { counters?.close(); } catch { /* the start error is the one to report */ }
    store.closeState();
    throw error;
  }
};
