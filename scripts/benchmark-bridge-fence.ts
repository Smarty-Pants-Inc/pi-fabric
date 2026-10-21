import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { monitorEventLoopDelay } from "node:perf_hooks";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshArchive } from "../src/mesh/archive.js";
import type { BridgeFenceObservation } from "../src/mesh/bridge-fence-stats.js";
import { MeshStore, type MeshEvent, type MeshIdentity, type MeshBatchOperation } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { distribution, observeFences, summarizeSnapshotExhaustions,
  type SnapshotExhaustion } from "./bridge-fence-metrics.js";
import { configurePrivateFeed, inventory, sizes } from "./bridge-fence-inventory.js";
import { seedProductionExtras, seedHistoricalMetadata, seedProductionFiles } from "./bridge-fence-profile.js";
import { backupManifest, backupSourceHash, captureBackup, backupDelta, installBackup, seedBackupDrivers, seedBackupFiles, cpuWorker, pressureSample, pressureWindow, harnessAffinity, ownedSchedule, ownedScheduleWindow,
  type BackupManifest, type BackupCapture } from "./bridge-fence-backup.js";

/**
 * LOCAL ONLY, Node >=24, all stores in one newly-created private mktemp directory.
 * Run ONLY with smarty-reap; never accepts a caller-provided mesh/database root.
 *
 * smarty-reap run /absolute/evidence/reap.json -- node scripts/bridge-fence-runner.mjs \
 *   --local-only --out /absolute/evidence --label baseline --reap-record /absolute/evidence/reap.json
 *
 * Fresh evidence directory per run. summary.json/terminal.json are completion receipts;
 * reap's launch PID is not completion. Always `smarty-reap stop RECORD` after reading the receipt.
 * PRIMARY --profile production-b: 46,995 real retained events, native external archive ON,
 * production aggregate KV/prefix and retained filesystem populations; six real bridges
 * and 100 native-root session stores. --seed-ms 300000 bounds durable history bootstrap
 * only: offered load remains 30 s, quiet drain <=90 s, busy-idle 20 s.
 * Legacy default scale-a is secondary topology-heavy/archive-off 100k-event sizing.
 * Default feed4096; --feed-retained 50000/200000 are NONDEFAULT sensitivities.
 * No artificial busy sleep, fake SQLite event table, or changed archive durability.
 *
 * Reader pacing is the workload, not production polling. MeshBridge.run uses its REAL
 * filesystem notifications and bounded safety-net timers. One outstanding 32-event source
 * batch per link bounds backlog (live --bidirectional: 16 each way, still 32 TOTAL).
 * The producer waits for real destination publish receipts.
 * This is a saturated bounded-window workload, NOT a claim that 100k new deliveries fit 30 s.
 */
const RUNNER = fileURLToPath(new URL("./bridge-fence-runner.mjs", import.meta.url));
const TOPIC = "fleet.work.fence15310";
let HISTORY = 100_000;
const WRITERS = 6, DEFAULT_SESSIONS = 100;
let HOSTS = 800, PARTICIPANTS = 1500, CONTROL = PARTICIPANTS * 4;
let LEASES = 200, FILES = 170;
let productionB = false, liveBackup = false;
let fixture: BackupManifest | undefined;
function configureProfile(profile: string, fixtureSeed = "") {
  liveBackup = profile === "live-backup-b";
  productionB = profile === "production-b" || liveBackup;
  if (productionB) { HISTORY = 46995; HOSTS = 500; PARTICIPANTS = 1000; CONTROL = 77; LEASES = 683; FILES = 388; }
  if (liveBackup) { fixture = backupManifest(fixtureSeed); HUB_ID = fixture.hubId; }
}
const historyId = (n: number) => `${fixture?.historyPrefix ?? "fence-history-"}${n}`;
type Direction = "toLocal" | "toRemote";
export type BridgeDiagnosticKind = "dropped" | "refused" | "skipped";
export type BridgeDiagnosticAttribution = "cohort" | "unrelated" | "unattributable";
export type BridgeDiagnosticClassification = { direction: Direction; kind: BridgeDiagnosticKind; attribution: BridgeDiagnosticAttribution };
export type BridgeDiagnosticAccounting = {
  total: number;
  byDirection: Record<Direction, Record<BridgeDiagnosticKind, Record<BridgeDiagnosticAttribution, number>>>;
};
const DIAGNOSTIC_KINDS: BridgeDiagnosticKind[] = ["dropped", "refused", "skipped"];
const DIAGNOSTIC_ATTRIBUTIONS: BridgeDiagnosticAttribution[] = ["cohort", "unrelated", "unattributable"];
const diagnosticBucket = () => ({ cohort: 0, unrelated: 0, unattributable: 0 });
const diagnosticDirection = () => ({ dropped: diagnosticBucket(), refused: diagnosticBucket(), skipped: diagnosticBucket() });
export function createBridgeDiagnosticAccounting(): BridgeDiagnosticAccounting {
  return { total: 0, byDirection: { toLocal: diagnosticDirection(), toRemote: diagnosticDirection() } };
}
export function classifyBridgeDiagnostic(line: string, expectedIds: ReadonlyMap<string, Direction>): BridgeDiagnosticClassification | undefined {
  const header = /^(toLocal|toRemote): (dropped|refused|skipped)\b/.exec(line);
  if (!header) return undefined;
  const direction = header[1] as Direction, kind = header[2] as BridgeDiagnosticKind;
  // Parse only complete native line shapes; the captured ID is compared as a whole token.
  const eventId = kind === "dropped"
    ? /^to(?:Local|Remote): dropped idempotency conflict sequence \d+ event (\S+) key \S+$/.exec(line)?.[1] ??
      /^to(?:Local|Remote): dropped \S+ (\S+) from \S+:/.exec(line)?.[1]
    : kind === "refused"
      ? /^to(?:Local|Remote): refused \S+ (\S+):/.exec(line)?.[1]
      : /^to(?:Local|Remote): skipped \S+ (\S+) \(sequence \d+\):/.exec(line)?.[1];
  const attribution: BridgeDiagnosticAttribution = eventId === undefined ? "unattributable"
    : expectedIds.has(eventId) ? "cohort" : "unrelated";
  return { direction, kind, attribution };
}
export function recordBridgeDiagnostic(accounting: BridgeDiagnosticAccounting, line: string,
  expectedIds: ReadonlyMap<string, Direction>): BridgeDiagnosticClassification | undefined {
  const classification = classifyBridgeDiagnostic(line, expectedIds);
  if (!classification) return undefined;
  accounting.total++;
  accounting.byDirection[classification.direction][classification.kind][classification.attribution]++;
  return classification;
}
export function countsAsBridgeDiagnosticDrop(classification: BridgeDiagnosticClassification, bidirectional: boolean): boolean {
  // Preserve the legacy toLocal-any-diagnostic rule; bidirectional runs count only exact cohort IDs.
  return bidirectional ? classification.attribution === "cohort" : classification.direction === "toLocal";
}
export function aggregateBridgeDiagnosticAccounting(values: BridgeDiagnosticAccounting[]): BridgeDiagnosticAccounting {
  const aggregate = createBridgeDiagnosticAccounting();
  for (const value of values) {
    aggregate.total += value.total;
    for (const direction of ["toLocal", "toRemote"] as const) for (const kind of DIAGNOSTIC_KINDS)
      for (const attribution of DIAGNOSTIC_ATTRIBUTIONS)
        aggregate.byDirection[direction][kind][attribution] += value.byDirection[direction][kind][attribution];
  }
  return aggregate;
}
const deliveryId = (writer: number, ordinal: number, direction: Direction = "toLocal") =>
  `${fixture?.deliveryPrefix ?? "fence-delivery-"}${writer}-${ordinal}${direction === "toRemote" ? "-toRemote" : ""}`;
const sourceRoot = (writer: number) => fixture?.sources[writer] ?? `session:fence-spoke-${String(writer).padStart(8, "0")}`;
const recipient = (writer: number, direction: Direction) => direction === "toLocal" ? HUB_ID : sourceRoot(writer);
const peerName = (n: number) => fixture?.peers[n] ?? `spoke${n}`;
const hubName = () => fixture?.localName ?? "hub";
// Inventory's legacy key-derived labels are forbidden on an imported live copy.
const fixtureInventory = (root: string, checkpoint = false) => liveBackup
  ? inventory(root, checkpoint, { privacy: "aggregate" }) : inventory(root, checkpoint);
const deliveryTopic = (ordinal: number) => productionB && ordinal % 2 === 1 ? "fabric.control.command" : TOPIC;
const isDeliveryTopic = (topic: string) => topic === TOPIC || topic === "fabric.control.command";
const MAX_EVENT = 65536, MAX_READ = 1000;
let HUB_ID = "session:fence-repro-hub-00000000";
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const keyFor = (prefix: string, id: string) => `${prefix}/${hash(id)}`;
const identity = (id: string, name = id): MeshIdentity => ({ id, name, kind: "main" });
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const writeJson = (file: string, value: unknown) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
const send = (value: unknown) => process.send?.(value);
const errorText = (error: unknown) => liveBackup
  ? `${error instanceof Error ? error.name : "Error"}: live-backup error detail suppressed to protect imported identities`
  : error instanceof Error ? error.stack ?? error.message : String(error);
const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);

type Config = { scratch: string; out: string; hub: string; label: string; loadMs: number; idleMs: number; batch: number; token: string;
  sessions: number; readEveryMs: number; controlEveryMs: number; heartbeatMs: number; feedRetained: number; targetDbBytes: number;
  profile: string; seedMs: number; stateBackup: string; backupSha256: string; fixtureSeed: string;
  cpuWorkers: number; deliveriesPerLink: number; cpuSet: string; cpuCount: number; bidirectional: boolean };
type Message = { kind: string; until?: number };
const mail: Message[] = [];
let receive: ((value: Message) => void) | undefined;
process.on("message", (message: unknown) => {
  if (!object(message) || typeof message.kind !== "string") return;
  const value = message as unknown as Message;
  if (receive) { const next = receive; receive = undefined; next(value); }
  else mail.push(value);
});
const nextMessage = async (): Promise<Message> => mail.shift() ?? await new Promise(resolve => { receive = resolve; });

function openStore(root: string): MeshStore {
  return new MeshStore(root, MAX_EVENT, MAX_READ, { stateBackend: "sqlite", lockProtocol: 3,
    lockTimeoutMs: 15000, staleLockMs: 60000, maxEventLogBytes: 512 * 1024 * 1024,
    retainedEventLogBytes: 512 * 1024 * 1024, lockHeldEventMs: 0 });
}
function createStore(root: string): MeshStore {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  SqliteStateStore.openSync(root, MAX_EVENT, MAX_READ, { initialize: "create" }).close();
  return openStore(root);
}
function hostValue(id: string, who: MeshIdentity, now: number, live = true) {
  return { format: 1, id, rootId: id, identity: who, startedAt: now - 60000,
    updatedAt: now, expiresAt: live ? now + 600000 : now - 60000 };
}
function participantValue(id: string, owner: string, now: number, root = true) {
  return { format: 1, id, kind: root ? "root" : "agent", rootId: owner, ownerHostId: owner, ownerIdentityId: owner,
    ownerIncarnation: `fence-repro:${owner}`, name: id, label: `FENCE-${hash(id).slice(0, 8)}`, status: "idle",
    runner: "pi", transport: root ? "host" : "process", capabilities: ["steer", "followUp"],
    startedAt: now - 60000, updatedAt: now, controlProtocol: "v1", cwd: `/scratch/lanes/${hash(id).slice(0, 12)}`,
    model: "sol-6.1", thinking: "xhigh" };
}
async function native(store: MeshStore, id: string) {
  const who = identity(id), now = Date.now();
  await store.put({ key: keyFor("topology/hosts", id), value: hostValue(id, who, now), identity: who });
  await store.put({ key: keyFor("topology/participants", id), value: participantValue(id, id, now), identity: who });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now - 60000, updatedAt: now, expiresAt: now + 600000 });
  return who;
}
function payload(writer: number, ordinal: number, direction: Direction = "toLocal") {
  const bytes = productionB ? (ordinal % 100 === 99 ? 3600 : 850 + (ordinal * 17 + (writer + 1) * 43) % 501)
    : 850 + ((ordinal * 1543 + (writer + 1) * 317) % 2751);
  const command = productionB && writer >= 0 && ordinal % 2 === 1 ? {
    version: 1, commandId: `${fixture?.commandPrefix ?? "fence-command-"}${writer}-${ordinal}${direction === "toRemote" ? "-toRemote" : ""}`,
    targetId: recipient(writer, direction), operation: "followUp", replyTo: HUB_ID,
  } : {};
  const data = { ...command, writer, ordinal, message: `local-only command ${writer}:${ordinal}`, body: "" };
  data.body = "x".repeat(bytes - Buffer.byteLength(JSON.stringify(data)));
  return data;
}
function controlOp(n: number, now: number, sequence = n + 1): MeshBatchOperation {
  const owner = `session:fence-fleet-${String(n % HOSTS).padStart(8, "0")}`, commandId = `fence-seed-command-${n}`;
  return { kind: "put", key: keyFor("topology/control-seen", commandId), identity: identity(owner),
    value: { format: 1, hostId: owner, commandId, targetId: owner, expiresAt: now + 86400000,
      ownerIncarnation: `fence-repro:${owner}`, sequence,
      acceptance: { accepted: true, messageId: `fence-seed-accepted-${n}`, triggered: true } } };
}
async function seed(store: MeshStore, config: Config) {
  const out = config.out;
  const now = Date.now(), ops: MeshBatchOperation[] = [];
  const root = (n: number) => `session:fence-fleet-${String(n).padStart(8, "0")}`;
  for (let n = 0; n < PARTICIPANTS; n++) {
    const owner = root(n % HOSTS), id = n < HOSTS ? owner : `agent:fence-task-${n}`;
    const who = identity(owner);
    if (n < HOSTS) ops.push({ kind: "put", key: keyFor("topology/hosts", id), identity: who,
      value: hostValue(id, who, now, n < LEASES) });
    ops.push({ kind: "put", key: keyFor("topology/participants", id), identity: who,
      value: participantValue(id, owner, now, n < HOSTS) });
  }
  // Four retained accepted outcomes per participant: topology/control-plane.ts FabricControlSeenRecord.
  // A declared reproducible sizing assumption, not a census of any live host.
  for (let n = 0; n < CONTROL; n++) ops.push(controlOp(n, now));
  for (let at = 0; at < ops.length; at += 200) await store.writeBatch({ identity: identity(HUB_ID), ops: ops.slice(at, at + 200) });
  let controlRows = CONTROL;
  while (!productionB && inventory(store.root).logicalDbBytes < config.targetDbBytes) {
    if (controlRows >= 60000) throw new Error("state-size calibration exceeded 60k bounded control rows");
    const more = Array.from({ length: 200 }, (_, n) => controlOp(controlRows + n, now));
    await store.writeBatch({ identity: identity(HUB_ID), ops: more }); controlRows += more.length;
  }
  let productionAssumptions: unknown;
  if (productionB) productionAssumptions = await seedProductionExtras(store, config.targetDbBytes);
  for (let n = 0; n < LEASES; n++) {
    const id = root(n);
    writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now - 60000, updatedAt: now, expiresAt: now + 600000 });
  }
  for (let n = 0; n < FILES; n++) {
    const id = root(n);
    writeParticipantFile(store.root, { key: keyFor("topology/participants", id), version: 1,
      updatedAt: now, updatedBy: identity(id), value: participantValue(id, id, now) });
  }
  const { started, archiveVerification } = await seedHistory(store, config);
  const retainedFiles = productionB ? seedProductionFiles(store.root, HISTORY) : undefined;
  const historicalMetadata = productionB ? seedHistoricalMetadata(store.root) : undefined;
  writeJson(path.join(out, "seed.json"), { historicalEvents: HISTORY, profile: config.profile,
    productionAssumptions, retainedFiles, historicalMetadata, archiveVerification, eventLogBytes: fs.statSync(path.join(store.root, "events.jsonl")).size,
    stateRows: store.listAll("", { fresh: true }).length, hostRows: HOSTS + 1, participantRows: PARTICIPANTS + 1,
    controlSeenRows: controlRows, hostLeaseFiles: LEASES + 1, participantFiles: FILES,
    targetDbBytes: config.targetDbBytes, stateCalibration: inventory(store.root, true),
    payloadBytes: { min: 850, max: 3600, unit: "JSON-encoded data bytes, before the bridge stamp" },
    sizingSources: ["tests/mesh-bridge-lock-hold.test.ts: HOSTS/PARTICIPANTS/LEASES/FILES",
      "src/topology/types.ts: FabricHostRecord/FabricParticipantRecord",
      "src/topology/control-plane.ts: FabricControlSeenRecord"],
    sizingAssumption: "Schema-shaped retained accepted control outcomes added in 200-row batches until the default-4096-feed DB reaches the requested logical size; not a known production prefix census. 200 live fleet roots; sessions reuse first 100 roots. No opaque padding rows.",
    seedMs: performance.now() - started });
}

// Shared REAL publication/archive path. Never seeds or resequences imported KV/meta/feed.
async function seedHistory(store: MeshStore, config: Config) {
  const out = config.out, from = identity(HUB_ID);
  const started = performance.now();
  const initialHistory = productionB ? HISTORY - 1 : HISTORY;
  for (let at = 0; at < initialHistory;) {
    const inputs = Array.from({ length: Math.min(32, initialHistory - at) }, (_, i) => ({ id: historyId(at + i),
      topic: "fence.history", kind: "record", from, data: payload(-1, at + i) }));
    const written = await store.publishBatch(inputs);
    if (!written.length) throw new Error("seed publish made no progress");
    at += written.length;
    if (at % 10000 === 0) writeJson(path.join(out, "progress.json"), { stage: "seed", events: at, elapsedMs: performance.now() - started });
    if (performance.now() - started > config.seedMs) throw new Error("seed exceeded bounded preparation lifetime");
  }
  let archiveVerification: unknown;
  if (productionB) {
    const archiveDir = path.join(config.scratch, "archive-hub"); fs.mkdirSync(archiveDir, { mode: 0o700 });
    writeJson(path.join(store.root, "event-archive.json"), { version: 1, dir: archiveDir });
    // Approved bootstrap only: last REAL publish triggers native durable full-live catch-up.
    // All later measured publications keep normal native archive begin/commit durability.
    await store.publish({ id: historyId(HISTORY - 1), topic: "fence.history", kind: "record",
      from: identity(HUB_ID), data: payload(-1, HISTORY - 1) });
    const archive = MeshArchive.fromRoot(store.root)!;
    const head = archive.head();
    if (head?.sequence !== HISTORY || archive.firstSequence() !== 1 || head.id !== historyId(HISTORY - 1))
      throw new Error("native archive catch-up head/first mismatch");
    const samples = [1, Math.floor(HISTORY / 2), HISTORY].map(sequence => {
      const event = archive.readAfter(sequence - 1, HISTORY, event => event.sequence === sequence, 1, "fence.history")[0];
      if (!event || event.id !== historyId(sequence - 1) || JSON.stringify(event.data) !== JSON.stringify(payload(-1, sequence - 1)))
        throw new Error(`native archive readback mismatch at ${sequence}`);
      return { sequence, id: event.id, dataBytes: Buffer.byteLength(JSON.stringify(event.data)) };
    });
    archiveVerification = { method: "HISTORY-1 real unarchived publishes, enable archive, one real final publish invokes native durable catchUp",
      head, firstSequence: archive.firstSequence(), samples,
      limitation: "Native unkeyed catchUp omits historical per-event lookup sidecars. No production index census match or historical receipt crash-recovery proof claimed; measured appends use unchanged native begin/commit durability." };
    writeJson(path.join(out, "archive-verification.json"), archiveVerification);
  }
  return { started, archiveVerification };
}

async function growFeed(store: MeshStore, config: Config) {
  if (config.feedRetained === 4096) return;
  const now = Date.now(), started = performance.now();
  for (let at = 0; at < config.feedRetained; at += 200) {
    await store.writeBatch({ identity: identity(HUB_ID), ops: Array.from({ length: 200 }, (_, n) =>
      controlOp((at + n) % CONTROL, now, HISTORY + at + n)) });
    if (performance.now() - started > 60000) throw new Error("nondefault feed seeding exceeded 60 seconds");
  }
}

async function writer(config: Config, index: number) {
  const metrics = observeFences(path.join(config.hub, "state.db"));
  const hub = openStore(config.hub); metrics.attach(hub);
  const far = createStore(path.join(config.scratch, `spoke-${index}`));
  const from = await native(far, sourceRoot(index));
  const directions: Direction[] = config.bidirectional ? ["toLocal", "toRemote"] : ["toLocal"];
  const perDirection = config.deliveriesPerLink / directions.length;
  const directionCounts = { toLocal: { sent: 0, delivered: 0, deliveredDuringLoad: 0 },
    toRemote: { sent: 0, delivered: 0, deliveredDuringLoad: 0 } };
  let delivered = 0, sent = 0, logDrops = 0, deliveredDuringLoad = 0, loadUntil = Infinity;
  const diagnosticAccounting = createBridgeDiagnosticAccounting();
  const expectedDiagnosticIds = new Map<string, Direction>();
  let bridgePhase = "startup";
  const sourceObservations: Array<BridgeFenceObservation & { phase: string }> = [];
  const sourceEvents = new Map<string, { direction: Direction; createdAt: number; publicationReturnedAt: number }>();
  const destinationEvents = new Map<string, { direction: Direction; createdAt: number; receiptObservedAt: number; phase: string }>();
  const progress: Array<{ at: number; phase: string; sent: number; delivered: number }> = [];
  const recordProgress = () => { if (liveBackup) progress.push({ at: Date.now(), phase: bridgePhase, sent, delivered }); };
  let deliveryWake: (() => void) | undefined;
  let runError: unknown;
  const note = (direction: Direction, events: MeshEvent[]) => {
    const matching = events.filter(event => {
      if (!isDeliveryTopic(event.topic) || !object(event.data) || event.data.writer !== index) return false;
      if (!config.bidirectional) return true;
      const data = event.data;
      return event.verification === "bridge" && Number.isInteger(data.ordinal) && object(data.bridge) &&
        data.bridge.from === (direction === "toLocal" ? peerName(index) : hubName()) &&
        data.bridge.id === deliveryId(index, Number(data.ordinal), direction);
    });
    if (liveBackup) for (const event of matching) {
      const data = event.data as Record<string, unknown>;
      if (object(data.bridge) && typeof data.bridge.id === "string") destinationEvents.set(data.bridge.id,
        { direction, createdAt: event.createdAt, receiptObservedAt: Date.now(), phase: Date.now() <= loadUntil ? "load" : "drain" });
    }
    const count = matching.length;
    delivered += count; directionCounts[direction].delivered += count;
    if (Date.now() <= loadUntil) { deliveredDuringLoad += count; directionCounts[direction].deliveredDuringLoad += count; }
    recordProgress(); deliveryWake?.();
  };
  for (const direction of directions) {
    const destination = direction === "toLocal" ? hub : far;
    const batch = destination.publishBatch.bind(destination), single = destination.publish.bind(destination);
    destination.publishBatch = async inputs => { const events = await batch(inputs); note(direction, events); return events; };
    destination.publish = async input => { const event = await single(input); note(direction, [event]); return event; };
  }
  const local = new StoreBridgeSide(hub, peerName(index), Date.now, productionB, {
    observeWriteFence: observation => { metrics.observe(observation); sourceObservations.push({ ...observation, phase: bridgePhase }); },
    ...(liveBackup ? { observeMirrorPhase: metrics.observeMirrorPhase } : {}),
  });
  const remote = new StoreBridgeSide(far, hubName(), Date.now, productionB);
  if (liveBackup) { metrics.observeMirror(local, "local"); metrics.observeMirror(remote, "remote"); }
  const bridge = new MeshBridge({ localName: hubName(), remoteName: peerName(index), local, remote,
    cursorPath: path.join(config.scratch, `cursor-${index}.json`),
    log: line => {
      const diagnostic = recordBridgeDiagnostic(diagnosticAccounting, line, expectedDiagnosticIds);
      if (diagnostic && countsAsBridgeDiagnosticDrop(diagnostic, config.bidirectional)) logDrops++;
      // Native metric lines contain only fixed labels + our synthetic peer; keep those unchanged.
      // Other diagnostics may contain imported identity values, so retain only an aggregate-safe hash.
      const safeLine = !liveBackup || /^bridge (write-fence|delivery-age) v1 /.test(line) ? line : `native diagnostic redacted sha256=${hash(line)}`;
      fs.appendFileSync(path.join(config.out, `writer-${index}.bridge.log`), `${safeLine}\n`, { mode: 0o600 }); } });
  let running: Promise<void> | undefined;
  let endLoad: ReturnType<typeof setTimeout> | undefined;
  const partial = (reason: string) => writeJson(path.join(config.out, `writer-${index}.partial.json`), {
    ok: false, partial: true, reason, pid: process.pid, index, sent, delivered, deliveredDuringLoad, logDrops, phase: bridgePhase,
    diagnosticAccounting, sourceObservations, directionCounts, progress, metrics: metrics.result() });
  const interrupted = () => { partial("owned SIGTERM"); process.exit(143); };
  process.once("SIGTERM", interrupted);
  try {
    await bridge.start();
    send({ kind: "ready", role: "writer", index, pid: process.pid });
    const go = await nextMessage();
    if (go.kind !== "load" || !go.until) throw new Error("writer expected load barrier");
    metrics.phase("load"); bridgePhase = "load"; loadUntil = go.until;
    recordProgress();
    endLoad = setTimeout(() => { metrics.phase("drain"); bridgePhase = "drain"; recordProgress(); }, Math.max(0, loadUntil - Date.now()));
    running = bridge.run().catch(error => { runError = error; deliveryWake?.(); });
    while (Date.now() < go.until && (!config.deliveriesPerLink || sent < config.deliveriesPerLink)) {
      // Offer both bounded source batches before waiting for either direction. The
      // native bridge alone schedules forwarding, admission, retries and backoff.
      for (const direction of directions) {
        const counts = directionCounts[direction];
        const count = config.deliveriesPerLink ? Math.min(config.batch, perDirection - counts.sent) : config.batch;
        const inputs = Array.from({ length: count }, (_, n) => {
          const id = deliveryId(index, counts.sent + n, direction);
          expectedDiagnosticIds.set(id, direction);
          return { id, topic: deliveryTopic(counts.sent + n),
            kind: productionB && (counts.sent + n) % 2 === 1 ? "followUp" : "ask",
            from: direction === "toLocal" ? from : identity(HUB_ID), to: recipient(index, direction),
            data: payload(index, counts.sent + n, direction) };
        });
        const source = direction === "toLocal" ? far : hub;
        let committed = 0;
        while (committed < inputs.length) {
          const written = await source.publishBatch(inputs.slice(committed));
          if (liveBackup && !written.length) throw new Error("source publication made no progress");
          if (liveBackup) for (const event of written) sourceEvents.set(event.id,
            { direction, createdAt: event.createdAt, publicationReturnedAt: Date.now() });
          committed += written.length;
        }
        counts.sent += committed; sent += committed; recordProgress();
      }
      const bound = go.until + 90000;
      const pending = () => directions.some(direction => directionCounts[direction].delivered < directionCounts[direction].sent);
      while (pending()) {
        if (runError) throw runError;
        if (Date.now() >= bound) throw new Error(`writer ${index}: delivery timeout (${delivered}/${sent})`);
        await new Promise<void>(resolve => {
          const timer = setTimeout(() => { deliveryWake = undefined; resolve(); }, Math.min(20000, bound - Date.now()));
          deliveryWake = () => { clearTimeout(timer); deliveryWake = undefined; resolve(); };
          if (!pending() || runError) deliveryWake();
        });
      }
    }
    // A finite source burst does NOT end the common load window or stop native bridge.run.
    if (liveBackup) {
      await sleep(Math.max(0, loadUntil - Date.now()));
      if (runError) throw runError;
      recordProgress();
    }
    send({ kind: "drained", role: "writer", index, sent, delivered, deliveredDuringLoad, at: Date.now() });
    if (liveBackup) { metrics.phase("barrier"); bridgePhase = "barrier"; }
    const idle = await nextMessage();
    if (idle.kind !== "idlebusy" || !idle.until) throw new Error("writer expected busy-idle barrier");
    metrics.phase("idlebusy"); bridgePhase = "idlebusy"; recordProgress();
    await sleep(Math.max(0, idle.until - Date.now()));
    if (liveBackup) { metrics.phase("barrier"); bridgePhase = "barrier"; }
    const stop = await nextMessage();
    if (stop.kind !== "stop") throw new Error("writer expected stop barrier");
    metrics.phase("stop"); bridgePhase = "stop";
    await bridge.stop(); await running;
    if (runError) throw runError;
    const ageSamples: Array<{ link: number; direction: Direction; phase: string; deliveryMs: number; sourceAppendToReceiptObservationMs: number }> = [];
    let missingAgeJoins = 0;
    if (liveBackup) for (const direction of directions) for (let ordinal = 0; ordinal < perDirection; ordinal++) {
      const id = deliveryId(index, ordinal, direction), source = sourceEvents.get(id), destination = destinationEvents.get(id);
      if (!source || !destination || source.direction !== direction || destination.direction !== direction ||
        ![source.createdAt, destination.createdAt, destination.receiptObservedAt].every(Number.isFinite)) { missingAgeJoins++; continue; }
      ageSamples.push({ link: index, direction, phase: destination.phase, deliveryMs: Math.max(0, destination.createdAt - source.createdAt),
        sourceAppendToReceiptObservationMs: Math.max(0, destination.receiptObservedAt - source.createdAt) });
    }
    if (liveBackup) writeJson(path.join(config.out, `writer-${index}.age-evidence.json`), {
      source: [...sourceEvents].map(([id, value]) => ({ id, ...value })),
      destination: [...destinationEvents].map(([sourceId, value]) => ({ sourceId, ...value })),
      clock: "actual source append createdAt joined to destination data.bridge.id and destination append createdAt; publication return and receipt observation are separate wall-clock events" });
    recordProgress();
    writeJson(path.join(config.out, `writer-${index}.json`), { ok: sent === delivered && logDrops === 0 &&
        (!liveBackup || (sent === 32 && ageSamples.length === 32 && missingAgeJoins === 0 &&
          directions.every(direction => directionCounts[direction].sent === perDirection && directionCounts[direction].delivered === perDirection))),
      pid: process.pid, role: "writer", index, sent, delivered, deliveredDuringLoad, logDrops, diagnosticAccounting, sourceObservations, directionCounts,
      ...(liveBackup ? { ageSamples, missingAgeJoins, progress } : {}), metrics: metrics.result() });
  } catch (error) { partial(errorText(error)); throw error; }
  finally {
    if (endLoad) clearTimeout(endLoad); process.removeListener("SIGTERM", interrupted);
    await bridge.stop(); await running;
    hub.closeState(); far.closeState(); metrics.restore();
  }
}

async function reader(config: Config, index: number) {
  const metrics = observeFences(path.join(config.hub, "state.db"));
  const hub = openStore(config.hub); metrics.attach(hub);
  // Reuse 100 of the seeded native fleet roots, not 100 extra fake topology rows.
  const who = identity(fixture?.drivers[index] ?? `session:fence-fleet-${String(index).padStart(8, "0")}`);
  const host = hub.get(keyFor("topology/hosts", who.id), { fresh: true })!;
  const participant = hub.get(keyFor("topology/participants", who.id), { fresh: true })!;
  const productionStateKey = liveBackup ? fixture!.workKeys[index] : !productionB ? undefined : index < 44 ? keyFor("topology/control-seen", `fence-seed-command-${index}`)
    : index < 75 ? `actors/fence-synthetic-${String(index).padStart(8, "0")}` : `mesh/fence-synthetic-${String(index).padStart(8, "0")}`;
  const productionState = productionStateKey ? hub.get(productionStateKey, { fresh: true })! : undefined;
  if (liveBackup && (!host || !participant || !productionState)) throw new Error("missing isolated synthetic driver rows");
  let reads = 0, observedEvents = 0, writes = 0, cursor = hub.latestOffset(), leaseRenewals = 0;
  const putMs: number[] = [], phaseRates: Record<string, unknown> = {};
  // Warm this real backend's read projection before the measured barrier.
  hub.list("topology/participants/", 100, { fresh: true });
  const partial = (reason: string) => writeJson(path.join(config.out, `reader-${index}.partial.json`), {
    ok: false, partial: true, reason, pid: process.pid, index, reads, writes, leaseRenewals, phaseRates, metrics: metrics.result() });
  const interrupted = () => { partial("owned SIGTERM"); process.exit(143); };
  process.once("SIGTERM", interrupted);
  send({ kind: "ready", role: "reader", index, pid: process.pid });
  try {
    for (let phase = await nextMessage(); phase.kind !== "stop"; phase = await nextMessage()) {
      if (!phase.until || !["load", "idlebusy"].includes(phase.kind)) throw new Error("session expected phase barrier");
      metrics.phase(phase.kind);
      const began = Date.now(), beforeWrites = writes, beforeRenewals = leaseRenewals, beforeReads = reads;
      const initial = (period: number) => began + Math.floor(index * period / config.sessions);
      let readAt = initial(config.readEveryMs), controlAt = initial(config.controlEveryMs), renewAt = initial(config.heartbeatMs);
      let missedReads = 0, missedControls = 0, missedRenewals = 0, maxWalBytes = 0, maxDbBytes = 0;
      const advance = (at: number, every: number): [number, number] => {
        const missed = Math.max(0, Math.floor((Date.now() - at) / every));
        return [at + (missed + 1) * every, missed];
      };
      while (Date.now() < phase.until) {
        if (Date.now() >= renewAt) {
          const now = Date.now(), previous = host.value as Record<string, unknown>;
          // Current fleet directories renew files every 5 s; idle file-capable heartbeats
          // do not periodically rewrite host state (host-leases.ts and participant-directory.ts).
          writeHostLease(hub.root, { id: who.id, rootId: who.id, identityId: who.id,
            startedAt: Number(previous.startedAt), updatedAt: now, expiresAt: now + 15000 });
          writeParticipantFile(hub.root, { key: participant.key, version: participant.version, updatedAt: now,
            updatedBy: who, value: { ...(participant.value as Record<string, unknown>), updatedAt: now } });
          leaseRenewals++; const [next, missed] = advance(renewAt, config.heartbeatMs); renewAt = next; missedRenewals += missed;
        }
        if (Date.now() >= controlAt) {
          const start = performance.now(), commandId = `reader-${index}-command-${writes % 64}`;
          if (productionState) {
            await hub.put({ key: productionState.key, identity: who,
              value: { ...(productionState.value as object), updatedAt: Date.now(), sequence: writes } });
          } else await hub.put({ key: keyFor("topology/control-seen", commandId), identity: who,
            value: { format: 1, hostId: who.id, commandId, targetId: HUB_ID, expiresAt: Date.now() + 600000,
              sequence: writes, acceptance: { accepted: true, messageId: `reader-${index}-${writes}` } } });
          putMs.push(performance.now() - start); writes++;
          const [next, missed] = advance(controlAt, config.controlEveryMs); controlAt = next; missedControls += missed;
        }
        if (Date.now() >= readAt) {
          hub.list(index % 2 === 0 ? "topology/participants/" : "topology/control-seen/", 100, { fresh: true });
          hub.get(host.key, { fresh: true });
          if (liveBackup && !hub.get(productionStateKey!, { fresh: true })) throw new Error("synthetic driver work row missing");
          const page = hub.tail(cursor, 256); cursor = page.nextOffset; observedEvents += page.events.length; reads++;
          const current = sizes(config.hub); maxWalBytes = Math.max(maxWalBytes, current.walBytes); maxDbBytes = Math.max(maxDbBytes, current.dbBytes);
          const [next, missed] = advance(readAt, config.readEveryMs); readAt = next; missedReads += missed;
        }
        await sleep(Math.max(0, Math.min(readAt, controlAt, renewAt, phase.until) - Date.now()));
      }
      const elapsedMs = Date.now() - began;
      phaseRates[phase.kind] = { elapsedMs, reads: reads - beforeReads, controlWrites: writes - beforeWrites,
        controlWritesPerSecond: (writes - beforeWrites) / elapsedMs * 1000, leaseAndParticipantRenewals: leaseRenewals - beforeRenewals,
        missedReads, missedControls, missedRenewals, maxWalBytes, maxDbBytes,
        requestedReadHz: 1000 / config.readEveryMs, requestedControlWriteHz: 1000 / config.controlEveryMs,
        requestedNativeLeaseAndParticipantRenewalHz: 1000 / config.heartbeatMs };
      send({ kind: "phaseDone", role: "reader", index, phase: phase.kind, reads, writes });
    }
    writeJson(path.join(config.out, `reader-${index}.json`), { ok: true, pid: process.pid, role: "reader", index,
      reads, observedEvents, writes, leaseRenewals, phaseRates, putLatency: distribution(putMs), metrics: metrics.result() });
  } catch (error) { partial(errorText(error)); throw error; }
  finally { process.removeListener("SIGTERM", interrupted); hub.closeState(); metrics.restore(); }
}

type Child = { child: ChildProcess; closed: Promise<void>; messages: Array<Record<string, unknown>>;
  waits: Array<{ kind: string; resolve: (message: Record<string, unknown>) => void; reject: (error: Error) => void }>;
  error?: Error };
function launch(configFile: string, role: string, index: number, out: string): Child {
  const log = fs.openSync(path.join(out, `${role}-${index}.stderr.log`), "wx", 0o600);
  const child = spawn(process.execPath, [RUNNER, "--child", configFile, role, String(index)], {
    stdio: ["ignore", log, log, "ipc"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" } });
  fs.closeSync(log);
  const value: Child = { child, closed: Promise.resolve(), messages: [], waits: [] };
  const failed = (error: Error) => { value.error = error; for (const wait of value.waits.splice(0)) wait.reject(error); };
  value.closed = new Promise<void>(resolve => {
    child.once("error", error => { failed(error); resolve(); });
    child.once("close", (code, signal) => {
      if (code !== 0 || signal !== null) failed(new Error(`${role}-${index} exited ${code}/${signal}`));
      else if (value.waits.length) failed(new Error(`${role}-${index} closed before expected barrier`));
      resolve();
    });
  });
  child.on("message", (message: unknown) => {
    if (!object(message)) return;
    const at = value.waits.findIndex(wait => wait.kind === message.kind);
    if (at >= 0) value.waits.splice(at, 1)[0]!.resolve(message);
    else value.messages.push(message);
  });
  return value;
}
function wait(child: Child, kind: string): Promise<Record<string, unknown>> {
  if (child.error) return Promise.reject(child.error);
  const at = child.messages.findIndex(message => message.kind === kind);
  if (at >= 0) return Promise.resolve(child.messages.splice(at, 1)[0]!);
  return new Promise((resolve, reject) => child.waits.push({ kind, resolve, reject }));
}
function broadcast(children: Child[], message: Message) {
  for (const { child, error } of children) {
    if (error) throw error;
    if (!child.connected) throw new Error("child IPC disconnected");
    child.send(message);
  }
}
async function bounded<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms} ms`)), ms);
  })]); } finally { if (timer) clearTimeout(timer); }
}
function pins() {
  const names = ["src/mesh/bridge.ts", "src/mesh/store.ts", "src/mesh/state-backend.ts", "src/mesh/state-sqlite.ts",
    "src/mesh/event-log.ts", "src/mesh/archive.ts", "src/mesh/bridge-fence-stats.ts", "src/mesh/backend-fence.ts",
    "src/mesh/commit-outbox.ts", "src/mesh/bridge-delivery-age.ts", "src/topology/host-leases.ts", "src/topology/participant-files.ts", "scripts/benchmark-bridge-fence.ts", "scripts/bridge-fence-metrics.ts", "scripts/bridge-fence-inventory.ts", "scripts/bridge-fence-profile.ts", "scripts/bridge-fence-backup.ts", "scripts/bridge-fence-analyze.mjs", "scripts/bridge-fence-runner.mjs"]; 
  return Object.fromEntries(names.map(name => [name, hash(fs.readFileSync(name, "utf8"))]));
}
function validateLog(hub: MeshStore, sent: number[], bidirectional = false, direction: Direction = "toLocal", link?: number) {
  const expected = sent.map(() => 0), seen = new Set<string>();
  let history = 0, historicalNext = 0, delivered = 0, duplicates = 0, outOfOrder = 0, invalid = 0;
  let historicalMinPayloadBytes = Infinity, historicalMaxPayloadBytes = 0;
  let cursor = 0, previousSequence = 0;
  for (;;) {
    const page = hub.tail(cursor, MAX_READ);
    for (const event of page.events) {
      if (event.sequence <= previousSequence) outOfOrder++;
      previousSequence = event.sequence;
      if (event.topic === "fence.history") {
        if (event.id !== historyId(historicalNext)) outOfOrder++;
        const want = payload(-1, historicalNext++);
        const encoded = JSON.stringify(event.data);
        const bytes = encoded === undefined ? 0 : Buffer.byteLength(encoded);
        historicalMinPayloadBytes = Math.min(historicalMinPayloadBytes, bytes);
        historicalMaxPayloadBytes = Math.max(historicalMaxPayloadBytes, bytes);
        if (!object(event.data) || event.data.writer !== -1 || event.data.ordinal !== want.ordinal ||
          event.data.body !== want.body || event.data.message !== want.message || bytes < 850 || bytes > 3600) invalid++;
        history++;
      }
      if (!isDeliveryTopic(event.topic)) continue;
      const data = event.data;
      // A bidirectional store contains its native source appends too. Exclude only
      // the exact synthetic opposite-direction source, never a bridged receipt.
      if (bidirectional && event.verification === "mesh" && object(data) && Number.isInteger(data.writer) && Number.isInteger(data.ordinal)) {
        const writer = Number(data.writer), ordinal = Number(data.ordinal), opposite = direction === "toLocal" ? "toRemote" : "toLocal";
        if (writer >= 0 && writer < WRITERS && event.id === deliveryId(writer, ordinal, opposite) &&
          event.from.id === (opposite === "toLocal" ? sourceRoot(writer) : HUB_ID)) continue;
      }
      delivered++;
      if (!object(data) || !Number.isInteger(data.writer) || !Number.isInteger(data.ordinal)) { invalid++; continue; }
      const writer = Number(data.writer), ordinal = Number(data.ordinal), marker = `${writer}:${ordinal}`;
      if (seen.has(marker)) duplicates++;
      seen.add(marker);
      if (writer < 0 || writer >= WRITERS || ordinal !== expected[writer]) outOfOrder++;
      if (writer >= 0 && writer < WRITERS) expected[writer] = expected[writer]! + 1;
      const want = payload(writer, ordinal, direction);
      const { bridge: _bridge, ...actualPayload } = data;
      const sender = direction === "toLocal" ? sourceRoot(writer) : HUB_ID;
      if (event.verification !== "bridge" || event.to !== recipient(writer, direction) || !object(data.bridge) ||
        data.bridge.from !== (direction === "toLocal" ? peerName(writer) : hubName()) || data.bridge.id !== deliveryId(writer, ordinal, direction) ||
        event.from.id !== sender || event.from.name !== sender || event.from.kind !== "main" ||
        event.topic !== deliveryTopic(ordinal) || event.kind !== (productionB && ordinal % 2 === 1 ? "followUp" : "ask") ||
        JSON.stringify(actualPayload) !== JSON.stringify(want) || (link !== undefined && writer !== link)) invalid++;
    }
    if (page.nextOffset === cursor || page.events.length === 0) break;
    cursor = page.nextOffset;
  }
  const drops = sent.reduce((sum, value) => sum + value, 0) - seen.size;
  const expectedHistory = direction === "toLocal" ? HISTORY : 0;
  return { ok: history === expectedHistory && historicalNext === expectedHistory && drops === 0 && duplicates === 0 && outOfOrder === 0 && invalid === 0 &&
      (!liveBackup || (sent.length === 6 && sent.every((count, index) => count ===
        (link === undefined || link === index ? (bidirectional ? 16 : 32) : 0)) && delivered === sent.reduce((sum, count) => sum + count, 0))) &&
      expected.every((count, i) => count === sent[i]),
    historicalEvents: history, historicalPayloadBytes: { min: historicalMinPayloadBytes, max: historicalMaxPayloadBytes },
    sent: sent.reduce((sum, value) => sum + value, 0), delivered, drops, duplicates, outOfOrder, invalid,
    sentByWriter: sent, deliveredByWriter: expected,
    checked: "full real MeshStore.tail traversal; logical source bridge-stamp uniqueness, per-source order, exact payload/topic/kind/sender/recipient/verification. Destination-local event ID uniqueness is NOT asserted" };
}
function summarize(reports: Array<Record<string, any>>, windows: Record<string, { from: number; to: number }>) {
  const all = reports.flatMap(report => report.metrics.holds as Array<{ site: string; phase: string; began: number; ended: number;
    ms: number; scans: number; path?: string; prepareMs?: number; afterCommitMs?: number }>);
  const groups: Record<string, number[]> = {};
  for (const hold of all) (groups[`${hold.phase}|${hold.site}`] ??= []).push(hold.ms);
  const byCallsite = Object.fromEntries(Object.entries(groups).map(([key, values]) => [key, distribution(values)]));
  const paths: Record<string, typeof all> = {};
  for (const hold of all) (paths[`${hold.phase}|${hold.site}|${hold.path ?? "unattributed"}`] ??= []).push(hold);
  const byTransactionPath = Object.fromEntries(Object.entries(paths).map(([key, values]) => [key, {
    actualTransaction: distribution(values.map(value => value.ms)),
    mirrorPrepareCallback: distribution(values.filter(value => (value.prepareMs ?? 0) > 0).map(value => value.prepareMs!)),
    mirrorAfterCommitCallback: distribution(values.filter(value => (value.afterCommitMs ?? 0) > 0).map(value => value.afterCommitMs!)),
    transactionOutsidePrepare: distribution(values.map(value => Math.max(0, value.ms - (value.prepareMs ?? 0)))),
    transactionOutsideBothCallbacks: distribution(values.map(value => Math.max(0, value.ms - (value.prepareMs ?? 0) - (value.afterCommitMs ?? 0)))) }]));
  const held = Object.fromEntries(Object.entries(windows).map(([phase, window]) => {
    let end = window.from, heldMs = 0;
    for (const hold of all.filter(hold => hold.phase === phase).sort((a, b) => a.began - b.began)) {
      const from = Math.max(window.from, end, hold.began), to = Math.min(window.to, hold.ended);
      heldMs += Math.max(0, to - from); end = Math.max(end, to);
    }
    return [phase, { ...window, durationMs: window.to - window.from, heldMs, heldPct: heldMs / (window.to - window.from) * 100 }];
  }));
  const sourceGroups: Record<string, { samples: number[]; fallbackCount: number; optimizedCount: number; retryCount: number }> = {};
  for (const observation of reports.flatMap(report => report.sourceObservations ?? []) as Array<BridgeFenceObservation & { phase: string }>) {
    const key = `${observation.phase}|${observation.site}|${observation.scope}`;
    const entry = sourceGroups[key] ??= { samples: [], fallbackCount: 0, optimizedCount: 0, retryCount: 0 };
    entry.samples.push(observation.holdMs);
    entry.fallbackCount += observation.fallback ? 1 : 0;
    entry.optimizedCount += observation.optimized ? 1 : 0;
    entry.retryCount += observation.retry ? 1 : 0;
  }
  const sourceByCallsite = Object.fromEntries(Object.entries(sourceGroups).map(([key, entry]) => [key, {
    ...distribution(entry.samples), fallbackCount: entry.fallbackCount, optimizedCount: entry.optimizedCount, retryCount: entry.retryCount }]));
  return { byCallsite, byTransactionPath, sourceByCallsite, held, fullScanFallbackCallbacks: reports.reduce((sum, report) => sum + report.metrics.fullScanFallbackCallbacks, 0),
    listAllInsideFence: reports.reduce((sum, report) => sum + report.metrics.listAllInside, 0),
    listAllOutsideFence: reports.reduce((sum, report) => sum + report.metrics.listAllOutside, 0),
    topologySqlScansInsideWriteLock: reports.reduce((sum, report) => sum + report.metrics.topologyScansInside, 0),
    kvFullSqlScansInsideWriteLock: reports.reduce((sum, report) => sum + report.metrics.kvFullScansInside, 0),
    busyBeginAttempts: reports.reduce((sum, report) => sum + report.metrics.busyBegins, 0) };
}

async function controller() {
  const flags = process.argv.slice(2);
  if (!flags.includes("--local-only")) throw new Error("required --local-only; this benchmark never opens a live mesh");
  const option = (name: string, fallback?: string) => { const at = flags.indexOf(name); return at >= 0 ? flags[at + 1] : fallback; };
  const outFlag = option("--out"), recordFlag = option("--reap-record");
  if (!outFlag || !recordFlag || !path.isAbsolute(outFlag) || !path.isAbsolute(recordFlag)) throw new Error("absolute --out and --reap-record required");
  const out = path.resolve(outFlag);
  fs.mkdirSync(out, { recursive: true, mode: 0o700 });
  if (fs.existsSync(path.join(out, "config.json"))) throw new Error("fresh evidence directory required");
  // Linux benchmark: verify this process really belongs to the owned smarty-reap process group.
  const proc = fs.readFileSync("/proc/self/stat", "utf8");
  const pgrp = Number(proc.slice(proc.lastIndexOf(")") + 2).split(/\s+/)[2]);
  const record = JSON.parse(fs.readFileSync(recordFlag, "utf8"));
  if (record.pid !== pgrp) throw new Error("not in the --reap-record owned process group");
  writeJson(path.join(out, "reap-launch.json"), record);
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-fence-repro-")); fs.chmodSync(scratch, 0o700);
  const profile = option("--profile", "scale-a")!;
  if (!["scale-a", "production-b", "live-backup-b"].includes(profile)) throw new Error("unknown bounded profile");
  const fixtureSeed = option("--fixture-seed", "")!;
  configureProfile(profile, fixtureSeed);
  const loadMs = Number(option("--load-ms", liveBackup ? "60000" : "30000")), idleMs = Number(option("--idle-ms", "20000"));
  const batch = Number(option("--batch", "32"));
  if ((!liveBackup && ![loadMs, idleMs].every(ms => Number.isInteger(ms) && ms >= 20000 && ms <= 40000)) ||
    (liveBackup && (loadMs !== 60000 || idleMs !== 20000)) || !Number.isInteger(batch) || batch < 1 || batch > 32) {
    throw new Error("legacy phases 20–40s; live-backup load 60s / idle 20s; batch 1–32");
  }
  const sessions = Number(option("--sessions", String(DEFAULT_SESSIONS))), readEveryMs = Number(option("--read-every-ms", "1000"));
  const controlEveryMs = Number(option("--control-every-ms", "5000")), heartbeatMs = Number(option("--heartbeat-ms", "5000"));
  const feedRetained = Number(option("--feed-retained", "4096")), targetDbBytes = Number(option("--target-db-bytes", "14300000"));
  if (!Number.isInteger(sessions) || sessions < 1 || sessions > 100 || ![readEveryMs, controlEveryMs, heartbeatMs].every(ms =>
    Number.isInteger(ms) && ms >= 500 && ms <= 30000) || ![4096, 50000, 200000].includes(feedRetained) ||
    !Number.isSafeInteger(targetDbBytes) || targetDbBytes < 6000000 || targetDbBytes > 30000000) throw new Error("invalid bounded calibration options");
  const stateBackup = option("--state-backup", "")!, backupSha256 = option("--backup-sha256", "")!;
  const cpuWorkers = Number(option("--cpu-workers", "0")), deliveriesPerLink = Number(option("--deliveries-per-link", "0"));
  // Opt-in only: deliveries-per-link stays TOTAL across directions (16+16, not 32+32).
  const bidirectional = flags.includes("--bidirectional");
  const affinity = liveBackup ? harnessAffinity() : { cpuSet: "", cpus: [] as number[] };
  const ownDomain = liveBackup ? pressureSample() : undefined;
  if (!Number.isInteger(cpuWorkers) || cpuWorkers < 0 || cpuWorkers > 12 ||
    (liveBackup && (affinity.cpus.length < 4 || affinity.cpus.length > 6 || !ownDomain?.ownScopeAttested ||
      ownDomain.cgroupPressure.cpu?.some == null)) ||
    (liveBackup && (!path.isAbsolute(stateBackup) || !/^[0-9a-f]{64}$/.test(backupSha256) || sessions !== 100 ||
      batch !== 32 || deliveriesPerLink !== 32 || feedRetained !== 4096 || flags.includes("--target-db-bytes"))) ||
    (!liveBackup && (cpuWorkers !== 0 || deliveriesPerLink !== 0 || stateBackup || backupSha256 || fixtureSeed || bidirectional)))
    throw new Error("invalid live-backup options; exact 100 sessions / 6x32 / default feed required, CPU workers 0..12 only in live profile, entire harness must inherit taskset affinity of 4..6 CPUs in a fresh fence-b-*.scope with readable dedicated CPU PSI");
  const seedMs = Number(option("--seed-ms", productionB ? "300000" : "90000"));
  if (!Number.isInteger(seedMs) || seedMs < 90000 || seedMs > 600000) throw new Error("invalid bounded preparation budget");
  const config: Config = { scratch, out, hub: path.join(scratch, "hub"), label: option("--label", "baseline")!, loadMs, idleMs, batch,
    sessions, readEveryMs, controlEveryMs, heartbeatMs, feedRetained, targetDbBytes, profile, seedMs,
    token: hash(`${process.pid}:${Date.now()}:${scratch}`), stateBackup, backupSha256, fixtureSeed, cpuWorkers, deliveriesPerLink,
    cpuSet: affinity.cpuSet, cpuCount: affinity.cpus.length, bidirectional }; 
  const configFile = path.join(out, "config.json"); writeJson(configFile, config);
  writeJson(path.join(scratch, "owner.json"), { token: config.token, pid: process.pid, out });
  const startPins = pins(); writeJson(path.join(out, "source-pins.json"), startPins);
  const children: Child[] = [], cpuChildren: Child[] = [];
  const allChildren = () => [...children, ...cpuChildren];
  let initialBackup: BackupCapture | undefined;
  let backupInstall: { sha256: string; copyHash: string } | undefined;
  const backupBarriers: Record<string, unknown> = {};
  const phasePressure: Record<string, ReturnType<typeof pressureWindow>> = {};
  const harnessScheduling: Record<string, ReturnType<typeof ownedScheduleWindow>> = {};
  let hub: MeshStore | undefined;
  let restoreFeed = () => undefined as void;
  let result: Record<string, unknown> = { ok: false, label: config.label, scratch, out };
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  let responseProbe: ReturnType<typeof monitorEventLoopDelay> | undefined;
  const startedAt = Date.now();
  try {
    watchdog = setTimeout(() => { for (const value of allChildren()) value.child.kill("SIGKILL");
      writeJson(path.join(out, "terminal.json"), { ok: false, error: `${config.seedMs + 300000}ms hard lifetime exceeded`, scratch }); process.exit(124); }, config.seedMs + 300000);
    if (liveBackup) {
      // No MeshStore exists until the native bootstrap handle is CLOSED, its DB/sidecars
      // preserved separately, pinned copy installed, and generated marker epoch corrected.
      fs.mkdirSync(config.hub, { mode: 0o700 });
      SqliteStateStore.openSync(config.hub, MAX_EVENT, MAX_READ, { initialize: "create" }).close();
      initialBackup = installBackup(config.hub, config.stateBackup, config.backupSha256, fixture!, out);
      backupInstall = JSON.parse(fs.readFileSync(path.join(out, "backup-install.json"), "utf8"));
      hub = openStore(config.hub); await hub.prepareLock();
      const seedStarted = performance.now();
      const drivers = await seedBackupDrivers(hub, fixture!);
      const afterAdd = captureBackup(hub.root), afterAddDelta = backupDelta(initialBackup, afterAdd);
      const nativeAfterAdd = { actualKvRows: afterAdd.summary.counts.kv,
        expectedKvRows: initialBackup.summary.counts.kv! + drivers.initialAdditiveKvRows - afterAddDelta.removed,
        removedImportedRows: afterAddDelta.removed, changedImportedRows: afterAddDelta.changed,
        addedRows: afterAddDelta.added, snapshot: afterAdd.summary };
      const { archiveVerification } = await seedHistory(hub, config);
      const retainedFiles = seedBackupFiles(hub.root, fixture!, HISTORY);
      writeJson(path.join(out, "seed.json"), { profile, historicalEvents: HISTORY, drivers, nativeAfterAdd, retainedFiles, archiveVerification,
        backup: { sha256: backupInstall!.sha256, copySha256: backupInstall!.copyHash, initial: initialBackup.summary },
        eventLogBytes: fs.statSync(path.join(hub.root, "events.jsonl")).size, stateCalibration: fixtureInventory(hub.root, true),
        seedMs: performance.now() - seedStarted });
    } else {
      hub = createStore(config.hub);
      await native(hub, HUB_ID); await seed(hub, config);
    }
    if (config.feedRetained !== 4096) {
      hub.closeState(); restoreFeed = configurePrivateFeed(config.hub, config.feedRetained); hub = openStore(config.hub);
      await growFeed(hub, config);
    }
    const inventories: Record<string, ReturnType<typeof fixtureInventory>> = { seeded: fixtureInventory(config.hub, true) };
    writeJson(path.join(out, "calibration.json"), { config, inventory: inventories.seeded,
      ...(liveBackup ? { backupInput: initialBackup!.summary,
        liveLimitations: "Genuine snapshot KV/meta/feed; only synthetic drivers renewed. Native expiry, retention, handoff cleanup permitted and observed. File/history fixtures synthetic; no imported actor/session adopted. No cross-host transport, consumer runtime or CPU-equivalence claim." } : {}),
      productionAggregateReference: { dbBytes: 14290000, walBytes: 9790000, kvRows: 4750, feedRows: 4048,
        eventLogLines: 46995, eventReceiptFiles: 33669, archiveEnabled: true, reportedHoldMs: [9000, 13000] },
      assumptions: liveBackup ? "Live-backup input is the pinned snapshot, not the old aggregate reference; only 101 native driver roots +100 work rows added. Normal native retention/expiry/cleanup permitted, not a preservation guard. Files/history synthetic; initial counters/feed unmodified. CPU equivalence requires parent calibration." : "Production-b uses 22:47Z aggregate prefix counts/bytes, external native archive, retained receipt/directory populations; native host/participant submix, external app row schemas, historical allocation and exact ancillary leaf layouts are synthetic assumptions, NOT live production values. Scale-a is secondary topology-heavy sizing. Default feed4096; larger feeds NONDEFAULT. 5s file renewals from ParticipantDirectory; read/control offer rates are declared assumptions." });
    if (flags.includes("--prepare-only")) { result = { ok: true, prepareOnly: true, scratch, out, inventories }; return; }
    for (let i = 0; i < WRITERS; i++) children.push(launch(configFile, "writer", i, out));
    for (let i = 0; i < config.sessions; i++) { children.push(launch(configFile, "reader", i, out)); await sleep(25); }
    for (let i = 0; i < config.cpuWorkers; i++) cpuChildren.push(launch(configFile, "cpu", i, out));
    await bounded(Promise.all(allChildren().map(child => wait(child, "ready"))), 90000, "child readiness");
    inventories.beforeLoad = fixtureInventory(config.hub);
    if (liveBackup) { const capture = captureBackup(config.hub); backupBarriers.beforeLoad = { snapshot: capture.summary, delta: backupDelta(initialBackup!, capture) }; }
    const windows: Record<string, { from: number; to: number }> = {};
    // Native bounded diagnostic sampling, not a store poller: no SQL/FS or fence callback.
    if (liveBackup) { responseProbe = monitorEventLoopDelay({ resolution: 20 }); responseProbe.enable(); }
    const scheduleGroups = { controller: [process.pid], writers: children.slice(0, WRITERS).map(value => value.child.pid!),
      readers: children.slice(WRITERS).map(value => value.child.pid!), cpuWorkers: cpuChildren.map(value => value.child.pid!) };
    const loadScheduleStart = liveBackup ? ownedSchedule(scheduleGroups, config.cpuSet) : undefined;
    const loadPressureStart = liveBackup ? pressureSample() : undefined;
    const loadStart = Date.now(), loadUntil = loadStart + config.loadMs;
    broadcast(allChildren(), { kind: "load", until: loadUntil });
    // Deadline observation is outside ALL store fences. No early finite-burst drain barrier.
    const loadPressureEnd = liveBackup ? sleep(Math.max(0, loadUntil - Date.now())).then(() => ({
      pressure: pressureSample(), scheduling: ownedSchedule(scheduleGroups, config.cpuSet) })) : undefined;
    writeJson(path.join(out, "progress.json"), { stage: "load", from: loadStart, until: loadUntil, pids: children.map(value => value.child.pid) });
    const drained = await bounded(Promise.all(children.slice(0, WRITERS).map(child => wait(child, "drained"))), config.loadMs + 95000, "load and bounded final batch drain");
    await bounded(Promise.all(children.slice(WRITERS).map(child => wait(child, "phaseDone"))), 10000, "load readers");
    await bounded(Promise.all(cpuChildren.map(child => wait(child, "phaseDone"))), 10000, "load CPU deadline");
    windows.load = { from: loadStart, to: loadUntil };
    windows.drain = { from: loadUntil, to: Date.now() };
    if (liveBackup) {
      const loadEnd = await loadPressureEnd!;
      phasePressure.load = pressureWindow(loadPressureStart!, loadEnd.pressure, config.cpuCount);
      phasePressure.drain = pressureWindow(loadEnd.pressure, pressureSample(), config.cpuCount);
      harnessScheduling.load = ownedScheduleWindow(loadScheduleStart!, loadEnd.scheduling, config.cpuCount);
      harnessScheduling.drain = ownedScheduleWindow(loadEnd.scheduling, ownedSchedule(scheduleGroups, config.cpuSet), config.cpuCount);
    }
    inventories.afterLoad = fixtureInventory(config.hub);
    if (liveBackup) { const capture = captureBackup(config.hub); backupBarriers.afterLoad = { snapshot: capture.summary, delta: backupDelta(initialBackup!, capture) }; }
    const idleScheduleStart = liveBackup ? ownedSchedule(scheduleGroups, config.cpuSet) : undefined;
    const idlePressureStart = liveBackup ? pressureSample() : undefined;
    const idleStart = Date.now(), idleUntil = idleStart + config.idleMs;
    broadcast(allChildren(), { kind: "idlebusy", until: idleUntil });
    writeJson(path.join(out, "progress.json"), { stage: "idlebusy", from: idleStart, until: idleUntil, drained });
    await bounded(Promise.all(children.slice(WRITERS).map(child => wait(child, "phaseDone"))), config.idleMs + 10000, "busy-idle readers");
    await bounded(Promise.all(cpuChildren.map(child => wait(child, "phaseDone"))), 10000, "busy-idle CPU deadline");
    windows.idlebusy = { from: idleStart, to: Date.now() };
    if (liveBackup) {
      phasePressure.idlebusy = pressureWindow(idlePressureStart!, pressureSample(), config.cpuCount);
      harnessScheduling.idlebusy = ownedScheduleWindow(idleScheduleStart!, ownedSchedule(scheduleGroups, config.cpuSet), config.cpuCount);
    }
    responseProbe?.disable();
    const harnessResponse = responseProbe ? { samples: Number(responseProbe.count),
      maxLagMs: Math.max(0, responseProbe.max / 1e6 - 20), p99LagMs: Math.max(0, responseProbe.percentile(99) / 1e6 - 20),
      method: "node:perf_hooks monitorEventLoopDelay at 20ms resolution, excess above 20ms; controller only, load+drain+idlebusy; no fence instrumentation or store polling" } : undefined;
    inventories.afterIdle = fixtureInventory(config.hub);
    broadcast(allChildren(), { kind: "stop" });
    await bounded(Promise.all(allChildren().map(value => value.closed)), 15000, "child stop");
    for (const child of allChildren()) if (child.error) throw child.error;
    const reports: Array<Record<string, any>> = [];
    for (let i = 0; i < WRITERS; i++) reports.push(JSON.parse(fs.readFileSync(path.join(out, `writer-${i}.json`), "utf8")));
    for (let i = 0; i < config.sessions; i++) reports.push(JSON.parse(fs.readFileSync(path.join(out, `reader-${i}.json`), "utf8")));
    const cpuReports = (await bounded(Promise.all(cpuChildren.map(child => wait(child, "cpuReport"))), 5000, "CPU terminal reports")).map((message, index) => {
      writeJson(path.join(out, `cpu-${index}.json`), message.report); return message.report as Record<string, any>;
    });
    const writerReports = reports.slice(0, WRITERS);
    let correctness = validateLog(hub, writerReports.map(report => Number(config.bidirectional ? report.directionCounts.toLocal.sent : report.sent)), config.bidirectional);
    if (config.bidirectional) {
      const toLocal = correctness;
      const remoteChecks = writerReports.map((_report, index) => {
        const far = openStore(path.join(config.scratch, `spoke-${index}`));
        try { return validateLog(far, writerReports.map((value, link) => link === index ? Number(value.directionCounts.toRemote.sent) : 0),
          true, "toRemote", index); }
        finally { far.closeState(); }
      });
      const sumRemote = (key: "sent" | "delivered" | "drops" | "duplicates" | "outOfOrder" | "invalid") =>
        remoteChecks.reduce((sum, check) => sum + check[key], 0);
      const toRemote = { ok: remoteChecks.every(check => check.ok),
        sent: sumRemote("sent"), delivered: sumRemote("delivered"), drops: sumRemote("drops"),
        duplicates: sumRemote("duplicates"), outOfOrder: sumRemote("outOfOrder"), invalid: sumRemote("invalid"),
        sentByWriter: remoteChecks.map((check, index) => check.sentByWriter[index]),
        deliveredByWriter: remoteChecks.map((check, index) => check.deliveredByWriter[index]), links: remoteChecks };
      correctness = { ...toLocal, ok: toLocal.ok && toRemote.ok,
        sent: toLocal.sent + toRemote.sent, delivered: toLocal.delivered + toRemote.delivered,
        drops: toLocal.drops + toRemote.drops, duplicates: toLocal.duplicates + toRemote.duplicates,
        outOfOrder: toLocal.outOfOrder + toRemote.outOfOrder, invalid: toLocal.invalid + toRemote.invalid,
        sentByWriter: writerReports.map(report => Number(report.sent)),
        deliveredByWriter: writerReports.map((_, index) => toLocal.deliveredByWriter[index]! + toRemote.deliveredByWriter[index]!) };
      // Keep per-direction traversal receipts as well as the legacy 192-total fields.
      Object.assign(correctness, { byDirection: { toLocal, toRemote } });
    }
    const ageSamples = writerReports.flatMap(report => report.ageSamples ?? []) as Array<{
      link: number; direction: Direction; phase: string; deliveryMs: number; sourceAppendToReceiptObservationMs: number }>;
    const missingAgeJoins = reports.slice(0, WRITERS).reduce((sum, report) => sum + Number(report.missingAgeJoins ?? 0), 0);
    let backup: Record<string, unknown> | undefined;
    if (liveBackup) {
      const end = captureBackup(config.hub), sourceEnd = backupSourceHash(config.stateBackup, config.backupSha256);
      backup = { sha256: config.backupSha256, copySha256: backupInstall!.copyHash, sourceStartSha256: backupInstall!.sha256,
        sourceEndSha256: sourceEnd.sha256, fixtureSeed: config.fixtureSeed, namespace: fixture!.namespace,
        initial: initialBackup!.summary, end: end.summary, delta: backupDelta(initialBackup!, end),
        sourceUnchanged: sourceEnd.sha256 === backupInstall!.sha256, barriers: backupBarriers };
    }
    const endPins = pins();
    const sourceUnchanged = JSON.stringify(startPins) === JSON.stringify(endPins);
    const summary = summarize(reports, windows);
    const diagnosticAccounting = aggregateBridgeDiagnosticAccounting(writerReports.map(report =>
      report.diagnosticAccounting as BridgeDiagnosticAccounting));
    const snapshotExhaustions = writerReports.flatMap(report => report.metrics.snapshotExhaustions ?? []) as SnapshotExhaustion[];
    result = { ok: correctness.ok && reports.every(report => report.ok) && cpuReports.every(report => report.ok) && sourceUnchanged &&
        (!liveBackup || (ageSamples.length === 192 && missingAgeJoins === 0 && backup?.sourceUnchanged === true &&
          Object.values(harnessScheduling).every(window => window.valid) && Object.values(phasePressure).every(window => window.ownScopeAttested))), label: config.label, scratch, out,
      node: process.version, writers: WRITERS, readerProcesses: config.sessions, batch: config.batch,
      startedAt, completedAt: Date.now(), windows, inventories,
      feedRetention: { requested: config.feedRetained, isProductionDefault: config.feedRetained === 4096,
        mechanism: config.feedRetained === 4096 ? "unmodified production default" : "explicit private-root SqliteStateStore.openSync options wrapper; actual engine retention, no fabricated rows" },
      readerRates: reports.slice(WRITERS).map(report => ({ index: report.index, phases: report.phaseRates })),
      profile: config.profile, archiveEnabled: productionB, keyedControlFraction: productionB ? 0.5 : 0,
      trafficModel: liveBackup ? `Finite exact six-link x32 TOTAL burst (${config.bidirectional ? "16 toLocal + 16 toRemote per link" : "32 toLocal per link, legacy default"}); all bridges + 100 isolated native-root session stores remain active for common 60s; driver-only work writes, NOT legacy 44/31/25 imported row mix; normal <=90s quiet drain and 20s busy-idle; CPU workers only in load/idlebusy; native admission/expiry/history/retention unchanged; no forced stale/fallback injection` : `${productionB ? "50% ordinary fleet.work, 50% keyed fabric.control.command" : "ordinary fleet.work"}; saturated producer with <=32 outstanding events/link; ${config.sessions} independent session MeshStores; ${productionB ? "state updates split 44% topology control-seen /31% actors /25% mesh to retain aggregate feed mix; exact live submix unknown; " : ""}each offers reads every ${config.readEveryMs}ms, control outcome every ${config.controlEveryMs}ms, native lease/participant files every ${config.heartbeatMs}ms; deterministic phase staggering, missed ticks counted, no catch-up burst; no forced stale/fallback injection`,
      deliveredDuringLoad: reports.slice(0, WRITERS).reduce((sum, report) => sum + Number(report.deliveredDuringLoad), 0),
      deliveredDuringLoadByWriter: reports.slice(0, WRITERS).map(report => report.deliveredDuringLoad),
      deliveriesPerSecond: reports.slice(0, WRITERS).reduce((sum, report) => sum + Number(report.deliveredDuringLoad), 0) / config.loadMs * 1000,
      deliveriesPerSecondIncludingDrain: correctness.delivered / (windows.drain.to - windows.load.from) * 1000,
      correctness, ...summary,
      diagnosticAccounting: { aggregate: diagnosticAccounting,
        byWriter: writerReports.map(report => ({ index: report.index,
          logDrops: report.logDrops, accounting: report.diagnosticAccounting })) },
      sourceUnchanged, sourcePins: startPins, endSourcePins: endPins,
      ...(liveBackup ? { backup, bidirectional: config.bidirectional,
        snapshotExhaustions: { method: "typed thrown exhaustion from each actual StoreBridgeSide.mirror call, not traffic-loop/streak logs; local.mirror=toLocal, remote.mirror=toRemote",
          events: snapshotExhaustions, byDirection: summarizeSnapshotExhaustions(snapshotExhaustions, windows.load!) },
        ageSamples, deliveryAge: { samples: ageSamples.length, missingJoins: missingAgeJoins,
        byDirection: Object.fromEntries((["toLocal", "toRemote"] as const).map(direction => {
          const values = ageSamples.filter(sample => sample.direction === direction);
          return [direction, { samples: values.length, deliveryMs: distribution(values.map(value => value.deliveryMs)),
            sourceAppendToReceiptObservationMs: distribution(values.map(value => value.sourceAppendToReceiptObservationMs)) }];
        })),
        deliveryMs: distribution(ageSamples.map(value => value.deliveryMs)),
        sourceAppendToReceiptObservationMs: distribution(ageSamples.map(value => value.sourceAppendToReceiptObservationMs)),
        clock: "deliveryMs=max(0,destination native append createdAt-source native append createdAt); observationMs=max(0,successful destination publication receipt observed wall time-source createdAt). All 192 synthetic event IDs joined via data.bridge.id, no payload timestamps. Native control-only age logs remain unchanged." },
        writerProgress: reports.slice(0, WRITERS).map(report => ({ index: report.index, progress: report.progress, sent: report.sent,
          delivered: report.delivered, deliveredDuringLoad: report.deliveredDuringLoad, directionCounts: report.directionCounts })), cpuWorkers: config.cpuWorkers, cpuReports, phasePressure, harnessScheduling, harnessResponse,
        ownedCpuChildren: cpuChildren.map(value => ({ pid: value.child.pid, exitCode: value.child.exitCode, signalCode: value.child.signalCode })),
        cpuChildren: cpuChildren.map(value => ({ pid: value.child.pid, exitCode: value.child.exitCode, signalCode: value.child.signalCode })) } : {}),
      ownedChildren: children.map(value => ({ pid: value.child.pid, exitCode: value.child.exitCode, signalCode: value.child.signalCode })),
      instrumentScope: "hub state.db only; actual node:sqlite BEGIN IMMEDIATE success through COMMIT/ROLLBACK; callback intervals exclude acquisition; SQL timing and listAll are observation-only" };
    writeJson(path.join(out, "summary.json"), result);
    process.stdout.write(`${JSON.stringify(result)}\n`);
    if (!result.ok) throw new Error("benchmark invariant failed; see summary.json");
  } catch (error) {
    result = { ...result, ok: false, error: errorText(error) };
    writeJson(path.join(out, "failure.json"), result);
    throw error;
  } finally {
    for (const value of allChildren()) if (value.child.exitCode === null && value.child.signalCode === null) value.child.kill("SIGTERM");
    try { await bounded(Promise.all(allChildren().map(value => value.closed)), 5000, "failure cleanup"); }
    catch { for (const value of allChildren()) if (value.child.exitCode === null && value.child.signalCode === null) value.child.kill("SIGKILL");
      await Promise.all(allChildren().map(value => value.closed)); }
    responseProbe?.disable(); hub?.closeState(); restoreFeed(); if (watchdog) clearTimeout(watchdog);
    writeJson(path.join(out, "terminal.json"), { ...result, terminalAt: Date.now(),
      childrenClosed: children.every(value => value.child.exitCode !== null || value.child.signalCode !== null),
      cpuChildrenClosed: cpuChildren.every(value => value.child.exitCode !== null || value.child.signalCode !== null) });
  }
}

async function main() {
  if (process.release.name !== "node" || process.platform !== "linux") throw new Error("local Linux Node reproduction only");
  if (process.argv[2] !== "--child") return controller();
  const file = process.argv[3], role = process.argv[4], index = Number(process.argv[5]);
  if (!file || !Number.isSafeInteger(index)) throw new Error("bad child arguments");
  const config = JSON.parse(fs.readFileSync(file, "utf8")) as Config;
  const owner = JSON.parse(fs.readFileSync(path.join(config.scratch, "owner.json"), "utf8"));
  if (!process.connected || owner.pid !== process.ppid || owner.token !== config.token ||
    path.dirname(config.hub) !== config.scratch || (fs.statSync(config.scratch).mode & 0o077) !== 0) {
    throw new Error("invalid private scratch ownership");
  }
  configureProfile(config.profile, config.fixtureSeed);
  // CPU workers never install state/FS observers or touch a database. Their readiness is IPC-only.
  if (role === "cpu" && liveBackup && index >= 0 && index < config.cpuWorkers) {
    const report = await cpuWorker(index, nextMessage, send);
    send({ kind: "cpuReport", report });
    process.disconnect?.(); return;
  }
  const disconnected = () => process.exit(1);
  if (liveBackup) process.once("disconnect", disconnected);
  const restoreFeed = liveBackup ? () => undefined : configurePrivateFeed(config.hub, config.feedRetained);
  try {
    if (role === "writer" && index >= 0 && index < WRITERS) await writer(config, index);
    else if (role === "reader" && index >= 0 && index < config.sessions) await reader(config, index);
    else throw new Error("bad child role/index");
  } finally { restoreFeed(); process.removeListener("disconnect", disconnected); }
  process.disconnect?.();
}
if (process.argv[1] && [fileURLToPath(import.meta.url), RUNNER].includes(path.resolve(process.argv[1]))) {
  try { await main(); }
  catch (error) { process.stderr.write(`${errorText(error)}\n`); process.exitCode = 1; process.disconnect?.(); }
}
