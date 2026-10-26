// N12 / R2-5: data-safety + cross-version journal boundary. Exact-ca4 SOURCE-reader leg ONLY.
// Actual carrier/ledger/projection/StoreBridgeSide append and NatsCarrierSide ACK/outcome paths;
// synthetic KV/delivery + synthetic owner ACKs, NO broker, native callback, SDK, app or installed
// ca4 artifact. Corruption refusal proves safety, NOT restored traffic. Parent owns execution.
// Platform gap: native receipt production needs POSIX modes/no-follow/directory fsync; Windows
// skips this coverage, never counts as rollback proof. Missing pinned old history/archive FAIL loudly.
// R4 additions below are PREPARED/UNRUN; historical 11-case SOURCE receipt does not cover them.
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { NatsCarrierSide } from "../src/mesh/bridge-nats-file.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
import { NATIVE_INGRESS_DIRECTORY, readNativeIngressReceipt } from "../src/mesh/bridge-nats-ingress.js";
import { BRIDGE_NATS_LEDGER_FILE, BRIDGE_NATS_LEDGER_LOCK, openBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger, natsLinkAuthorizers } from "../src/mesh/bridge-nats-link.js";
import { BRIDGE_NATS_HORIZON_MS, bridgeNatsMappedCommandId,
  type BridgeNatsClaimInput, type BridgeNatsCommandRecord, type BridgeNatsLedger,
  type BridgeNatsPresenceRecord } from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { residentHostId } from "../src/residency/protocol.js";
import { BRIDGE_MACHINE, FakePort, LINK, POD_MACHINE, fileLead, natsLead, route, settle } from "./helpers/bridge-nats-l2-fakes.js";
import { FROZEN_CA4_LEDGER, loadFrozenCa4Ledger, type FrozenCa4Ledger } from "./helpers/frozen-ca4-ledger.js";

const POSIX = process.platform !== "win32" && !!fs.constants.O_NOFOLLOW && !!fs.constants.O_DIRECTORY && !!process.geteuid;
const ACTOR = "a".repeat(32); // Test-only; never the production actor pin.
const OWNER = residentHostId(fileLead.id), INCARNATION = "synthetic-ledger-owner";
const principal = { id: "herdr:synthetic-ledger", binding: "herdr-client" as const };
const body = (event: MeshEvent) => event.data as Record<string, unknown>;
const json = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const hash = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const mapped = (source: string) => bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, source);
const publication = (event: MeshEvent, from: string): BridgePublish => ({
  topic: event.topic, kind: event.kind, from: event.from, to: event.to!,
  ...(event.principal ? { principal: event.principal } : {}),
  // MeshBridge's fixed hop stamp, not a fabricated native receipt/context.
  data: { ...body(event), bridge: { from, id: event.id } },
});
const claimInput = (record: BridgeNatsCommandRecord): BridgeNatsClaimInput => {
  const { v: _v, state: _s, claimedAt: _c, expiresAt: _e, destinationSequence: _q, ack: _a, ...input } = record;
  if (input.direction === "toFile") { const { envelopeId: _destination, ...claim } = input; return claim; }
  return input;
};

/** Generate positive bytes through the candidate, NOT a hand-authored JSONL encoder/parser. */
async function candidateJournal(root: string) {
  const now = Date.now(), dir = path.join(root, "candidate-ledger");
  const ledger = await openBridgeNatsLedger({ dir, clock: () => now, minCompactLines: 10_000 });
  const store = new MeshStore(path.join(root, "mesh"), 256 * 1024, 1_000);
  const port = new FakePort(), file = new StoreBridgeSide(store, LINK, () => now);
  let side: NatsCarrierSide | undefined;
  const seen: Array<{ direction: "toFile" | "toNats"; eventId: string }> = [];
  const identities: MeshIdentity[] = [fileLead, { id: OWNER, name: "synthetic-resident", kind: "agent" }];
  try {
    await store.prepareLock();
    for (const identity of identities) await store.put({ key: key("topology/hosts/", identity.id), identity,
      value: { format: 1, id: identity.id, rootId: fileLead.id, identity, startedAt: now - 1_000,
        updatedAt: now, expiresAt: now + 3_600_000 } });
    const actor: MeshIdentity = { id: ACTOR, name: "synthetic-native-ledger", kind: "actor" };
    for (const [identity, machine, incarnation, extra] of [
      [natsLead, POD_MACHINE, "pod-owner", {}], [fileLead, BRIDGE_MACHINE, "root-owner", {}],
      [actor, BRIDGE_MACHINE, INCARNATION, { ownerHostId: OWNER, ownerIdentityId: OWNER }],
    ] as const) port.routes.set(identity.id, route(identity, machine, incarnation, extra));
    const records: BridgeNatsPresenceRecord[] = [
      { id: fileLead.id, name: fileLead.name, kind: "main", rootId: fileLead.id, ownerHostId: fileLead.id,
        ownerIdentityId: fileLead.id, live: true, observedAt: now },
      { id: ACTOR, name: actor.name, kind: "actor", rootId: fileLead.id, ownerHostId: OWNER,
        ownerIdentityId: OWNER, ownerIncarnation: INCARNATION, live: true, observedAt: now },
    ];
    const allow = parseBridgeNatsAllowSection({
      file: [{ kind: "root", id: fileLead.id, send: ["ask"], receive: ["ask", "cancel"], principals: [principal.id] },
        { kind: "actor", rootId: fileLead.id, name: actor.name, id: ACTOR, send: [], receive: ["ask", "cancel"] }],
      nats: [{ kind: "root", id: natsLead.id, machine: POD_MACHINE, send: ["ask", "cancel"], receive: ["ask"], principals: [principal.id] }],
    }, { link: LINK, natsMachine: BRIDGE_MACHINE });
    const gate = createBridgeNatsIdentityGate(allow, { pins: { seats: new Map(), bound: new Map() }, clock: () => now });
    const { authorize, authorizeTopic } = natsLinkAuthorizers(gate, {
      file: () => ({ side: "file", records: id => records.filter(record => record.id === id) }),
      nats: () => port.presenceView(),
    });
    side = new NatsCarrierSide({ port, file, link: LINK, ledger: natsCarrierLedger(ledger, LINK), authorize, authorizeTopic,
      now: () => now });
    await side.latestSequence();
    let through = 0, stream = 0;
    const persist = async (event: MeshEvent): Promise<MeshEvent> => {
      const receipt = await file.publish(publication(event, LINK));
      const stored = store.read({ after: 0, limit: 1_000 }).find(row => row.sequence === receipt.sequence)!;
      expect(stored).toBeDefined();
      // Exercise actual event-level journal dedupe as well as command claims.
      await ledger.markSeen("toFile", event.id, { destinationId: stored.id, destinationSequence: stored.sequence });
      seen.push({ direction: "toFile", eventId: event.id });
      return stored;
    };
    const receive = async (source: string, operation: "ask" | "cancel" = "ask", append = true, targetId = ACTOR) => {
      const native = targetId === ACTOR;
      const envelope = { id: `synthetic-envelope-${source}`, kind: "command", from: natsLead, principal,
        to: { machine: BRIDGE_MACHINE, hostId: native ? OWNER : fileLead.id }, targetId, operation,
        body: { version: 1, commandId: source, targetId, operation, replyTo: natsLead.id,
          ownerIncarnation: native ? INCARNATION : "root-owner", message: native ? "smarty-inbox.rpc.v1" : "ordinary non-native ask",
          data: { v: 1, requestId: source, operation: "add", opKey: "synthetic-only", args: { title: "journal fixture", nested: [1, null, true] } },
          ...(operation === "cancel" ? { cancelCommandId: native ? "completed-ask" : "ordinary-ask" } : {}),
          requestCreatedAt: now, requestedAt: now, deadlineAt: now + 60_000 } };
      const delivery = port.deliver(envelope, POD_MACHINE, ++stream);
      await settle();
      const read = await side!.read(through);
      expect(read.events).toHaveLength(1);
      expect(delivery.acked).toBe(0);
      const record = ledger.lookup("toFile", mapped(source))!;
      expect(record.resolvedBody).toEqual(body(read.events[0]!));
      expect(readNativeIngressReceipt(dir, record.commandId)).toMatchObject({ commandId: record.commandId,
        sourceMachine: POD_MACHINE, sender: { id: natsLead.id, sessionId: natsLead.id.slice(8), kind: "main" } });
      if (append) {
        await persist(read.events[0]!);
        through = read.through;
        await side!.read(through); // Actual source ACK-after-durable-append path.
        expect(delivery.acked).toBe(1);
        expect(delivery.termed).toEqual([]);
      }
      return record.commandId;
    };
    const answer = async (id: string, result: Record<string, unknown>) => {
      // Synthetic owner outcome: no native handler/app is entered by this journal test.
      const record = ledger.lookup("toFile", id)!;
      const event = await store.publish({ topic: "fabric.control.ack", kind: "accepted",
        from: record.targetId === ACTOR ? identities[1]! : fileLead, to: natsLead.id,
        data: { version: 1, commandId: id, targetId: record.targetId, ownerIncarnation: record.ownerIncarnation,
          accepted: true, result } });
      await side!.publish(publication(event, "synthetic-hub")); // Real mapping/send/settle/forwarded path.
      expect(ledger.lookup("toFile", id)?.state).toBe("ackForwarded");
      expect(port.sent.at(-1)?.body).toMatchObject({ accepted: true, result });
    };
    const complete = await receive("completed-ask");
    await answer(complete, { data: { requestId: "completed-ask", ok: true, itemId: "synthetic-item" } });
    const cancel = await receive("completed-cancel", "cancel");
    await answer(cancel, { halted: true });
    // F2: the very same ingress projection also covers canonical-Main ordinary traffic.
    // Preserve its real claims/receipts on downgrade, not just those naming the native actor.
    const ordinaryAsk = await receive("ordinary-ask", "ask", true, fileLead.id);
    await answer(ordinaryAsk, { ordinary: "answer" });
    const ordinaryCancel = await receive("ordinary-cancel", "cancel", true, fileLead.id);
    await answer(ordinaryCancel, { halted: true });
    expect(readNativeIngressReceipt(dir, ordinaryAsk)).toMatchObject({ targetId: fileLead.id, operation: "ask" });
    expect(readNativeIngressReceipt(dir, ordinaryCancel)).toMatchObject({ targetId: fileLead.id, operation: "cancel",
      cancelCommandId: ordinaryAsk, sourceCancelCommandId: "ordinary-ask" });
    // An admitted claim whose destination outcome is unknown remains unknown on downgrade.
    // It is NEVER sent/re-executed by the old-reader leg below.
    const unknown = await receive("unknown-ask", "ask", false);
    await ledger.markUncertain(unknown);

    // Ordinary reverse direction too: real resolved outbound body and markSent; actual inbound
    // owner ACK saves an outcome BEFORE file append. Leave its source ACK unforwarded deliberately.
    const outbound = await store.publish({ topic: "fabric.control.command", kind: "ask", from: fileLead, to: natsLead.id, principal,
      data: { version: 1, commandId: "reverse-ask", targetId: natsLead.id, operation: "ask", replyTo: fileLead.id,
        ownerIncarnation: "pod-owner", message: "synthetic reverse ask", requestedAt: now, deadlineAt: now + 60_000 } });
    await side.publish(publication(outbound, "synthetic-hub"));
    const sent = port.sent.at(-1)!;
    const reverse = bridgeNatsMappedCommandId(LINK, "toNats", fileLead.id, "reverse-ask");
    expect(ledger.lookup("toNats", reverse)?.state).toBe("sent");
    // The earlier unknown command stays in hand; don't advance past/replay it as a recovery action.
    // Read the ACK directly through the actual carrier and persist its outcome through the same port.
    const plan = await side.carrier.inbound({ sourceMachine: POD_MACHINE, sequence: ++stream, envelope: {
      id: `ack:${sent.id}`, kind: "ack", from: natsLead, to: { machine: BRIDGE_MACHINE, hostId: fileLead.id },
      targetId: natsLead.id, operation: "accepted", body: { version: 1, commandId: reverse, targetId: natsLead.id,
        accepted: true, bodyDigest: ledger.lookup("toNats", reverse)!.bodyDigest, ownerIncarnation: "pod-owner",
        result: { data: { synthetic: "reverse-result" } } },
    } });
    if (plan.kind !== "event" || !plan.settle) throw new Error(`Expected real reverse ACK plan, got ${plan.kind}`);
    await side.carrier.settle(plan.settle); // No forward receipt: retain pendingAcks on purpose.
    await persist(plan.event);
    expect(ledger.lookup("toNats", reverse)?.state).toBe("acked");
    const commands = [complete, cancel, ordinaryAsk, ordinaryCancel, unknown, reverse]
      .map(id => ledger.lookup(id === reverse ? "toNats" : "toFile", id)!);
    expect(commands.every(Boolean)).toBe(true);
    expect(commands[1]).toMatchObject({ cancelCommandId: complete, originalCancelCommandId: "completed-ask", principal });
    fs.writeFileSync(path.join(dir, NATIVE_INGRESS_DIRECTORY, "preserve-unknown-evidence.bin"), Buffer.from([0, 255, 10, 123]), { mode: 0o600 });
    return { now, dir, commands: json(commands), seen, expected: snapshot(ledger, commands, seen) };
  } finally {
    side?.close();
    await port.close();
    await ledger.close();
    store.closeState();
  }
}

function snapshot(ledger: BridgeNatsLedger, commands: readonly BridgeNatsCommandRecord[], seen: readonly { direction: "toFile" | "toNats"; eventId: string }[]) {
  return json({ records: commands.map(record => ledger.lookup(record.direction, record.commandId)),
    originals: commands.map(record => ledger.lookupOriginal(record.direction, record.requesterId, record.originalCommandId)),
    pendingResends: ledger.pendingResends(), pendingAcks: ledger.pendingAcks(),
    seen: seen.map(record => ledger.seen(record.direction, record.eventId)) });
}
function evidence(dir: string) {
  const native = path.join(dir, NATIVE_INGRESS_DIRECTORY);
  return { ledgerSha256: hash(fs.readFileSync(path.join(dir, BRIDGE_NATS_LEDGER_FILE))),
    native: Object.fromEntries(fs.readdirSync(native).sort().map(name => [name, hash(fs.readFileSync(path.join(native, name)))])) };
}

/** A fresh ledger-only claim: never publish it, enter a callback, or replay prior unknown work. */
function appendInput(candidate: Awaited<ReturnType<typeof candidateJournal>>, original: string): BridgeNatsClaimInput {
  const first = claimInput(candidate.commands[0]!);
  const commandId = mapped(original);
  const resolvedBody = { ...first.resolvedBody, commandId, message: "append boundary: héllo 🧷",
    data: { fixture: original, nested: [1, null, true] } };
  return { ...first, commandId, originalCommandId: original, sourceEventId: `append-envelope-${original}`, resolvedBody,
    bodyDigest: controlBodyFingerprint({ ...resolvedBody, commandId: original }, first.principal) };
}

type AppendObservation = {
  stage: string; mode: "normal" | "crash"; pid: number; candidateSourceSha256: string;
  writeCalls: number; fsyncCalls: number; truncateCalls: number; requestedBytes: number; persistedBytes: number;
  visibleAtWrite: boolean; visibleAtFsync?: boolean;
};
/** Native close/error events, NOT fixture stdout, establish terminal process state. A deadline
 * abort is a failure even if it kills the child with the same signal as the intended crash. */
async function candidateAppendChild(root: string, dir: string, now: number, input: BridgeNatsClaimInput, mode: "normal" | "crash") {
  const artifacts = path.join(root, `append-child-${mode}`);
  fs.mkdirSync(artifacts, { mode: 0o700 });
  const child = spawn(process.execPath, [fileURLToPath(new URL("./fixtures/native-ledger-crash-append.mjs", import.meta.url)),
    mode, dir, String(now), JSON.stringify(input), artifacts], {
    cwd: fileURLToPath(new URL("..", import.meta.url)), stdio: ["ignore", "pipe", "pipe"],
    signal: AbortSignal.timeout(30_000), killSignal: "SIGKILL",
  });
  let stdout = "", stderr = "", failure: Error | undefined;
  child.stdout.on("data", chunk => { stdout += String(chunk); });
  child.stderr.on("data", chunk => { stderr += String(chunk); });
  child.once("error", error => { failure = error; });
  const terminal = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  // Retain the real child's output even on failure; no descendants/detached workers are launched.
  fs.writeFileSync(path.join(artifacts, "stdout.log"), stdout, { mode: 0o600 });
  fs.writeFileSync(path.join(artifacts, "stderr.log"), stderr, { mode: 0o600 });
  if (failure) throw new Error(`N12 actual candidate child failed: ${failure.message}; ${stderr}`, { cause: failure });
  expect(terminal, stderr).toEqual(mode === "crash" ? { code: null, signal: "SIGKILL" } : { code: 0, signal: null });
  const observation = JSON.parse(fs.readFileSync(path.join(artifacts, "observation.json"), "utf8")) as AppendObservation;
  expect(observation.pid).toBe(child.pid);
  expect(child.exitCode).toBe(terminal.code); expect(child.signalCode).toBe(terminal.signal);
  expect(observation.candidateSourceSha256).toBe(hash(fs.readFileSync(fileURLToPath(new URL("../src/mesh/bridge-nats-ledger.ts", import.meta.url)))));
  return { terminal, artifacts, observation, line: fs.readFileSync(path.join(artifacts, "candidate-append-line.bin")) };
}

describe.skipIf(!POSIX)("N12 exact-ca4 SOURCE-reader (installed compiled-artifact gate remains external)", () => {
  let root: string, source: FrozenCa4Ledger, candidate: Awaited<ReturnType<typeof candidateJournal>>;
  let sealed: string, pristine: ReturnType<typeof evidence>;
  const ownCopy = (name: string, from = sealed): string => {
    const to = path.join(root, name);
    fs.cpSync(from, to, { recursive: true, force: false, errorOnExist: true });
    return to;
  };
  const noLock = (dir: string) => expect(fs.existsSync(path.join(dir, BRIDGE_NATS_LEDGER_LOCK))).toBe(false);
  const unchangedOriginals = () => {
    expect(evidence(candidate.dir)).toEqual(pristine);
    expect(evidence(sealed)).toEqual(pristine);
    noLock(candidate.dir); noLock(sealed);
  };
  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "n12-ca4-source-ledger-"));
    const oldRoot = path.join(root, "exact-ca4-source"); fs.mkdirSync(oldRoot, { mode: 0o700 });
    source = await loadFrozenCa4Ledger(oldRoot);
    expect(source.openBridgeNatsLedger).not.toBe(openBridgeNatsLedger);
    expect(source.provenance).toMatchObject({ ...FROZEN_CA4_LEDGER, leg: "exact-ca4 SOURCE-reader",
      sharedNodeModules: null, installedCompiledArtifactVerified: false });
    candidate = await candidateJournal(root);
    noLock(candidate.dir);
    // Preserve pristine, independent bytes BEFORE the first old ledger load/truncate/compact.
    sealed = ownCopy("sealed-candidate-original", candidate.dir);
    pristine = evidence(sealed);
    expect(evidence(candidate.dir)).toEqual(pristine);
  }, 60_000);
  afterAll(() => {
    // Keep pristine/damaged copies, actual child output and intended-line evidence for the
    // Parent-owned rollback gate, including after a failed assertion. No raw trace cleanup.
    if (root) console.info(`N12 retained SOURCE-reader/crash evidence: ${root}`);
  });

  it("keeps optional toFile bodies, identity/principal/cancel/outcomes and dedupe through old append/compact/reopen", async () => {
    const working = ownCopy("valid-working"), clock = { now: candidate.now };
    let old = await source.openBridgeNatsLedger({ dir: working, clock: () => clock.now, minCompactLines: 1 });
    try {
      expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
      const bytes = fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_FILE));
      for (const record of candidate.commands) {
        const duplicate = await old.claim(claimInput(record));
        expect(duplicate).toEqual({ status: "duplicate", created: false, record });
        expect(source.bridgeNatsClaimAction(duplicate).action).toBe(record.ack ? "replayAck" : "resend");
      }
      const first = candidate.commands[0]!;
      expect((await old.claim({ ...claimInput(first), bodyDigest: "f".repeat(64) })).status).toBe("conflict");
      expect((await old.claim({ ...claimInput(first), replyTo: "synthetic-other-return" })).status).toBe("mismatch");
      expect(fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_FILE))).toEqual(bytes); // Dedupe never appends.
      const holder = fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_LOCK, "owner"));
      await expect(source.openBridgeNatsLedger({ dir: working, lockTimeoutMs: 0 })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_LOCKED" });
      expect(fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_LOCK, "owner"))).toEqual(holder);
      await old.markSeen("toNats", "old-live-append", { destinationId: "synthetic-old-destination", destinationSequence: 81 });
      // An actual old mutation expired at the normal reopen clock forces the old maintenance
      // compaction path, without expiring any original claim/outcome or editing journal records.
      clock.now = candidate.now - BRIDGE_NATS_HORIZON_MS - 1;
      await old.markSeen("toFile", "old-expired-append");
      clock.now = candidate.now;
      expect(fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_FILE)).length).toBeGreaterThan(bytes.length);
    } finally { await old.close(); }
    noLock(working);
    old = await source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now, minCompactLines: 1 });
    try {
      expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
      expect(old.seen("toNats", "old-live-append")).toMatchObject({ destinationId: "synthetic-old-destination", destinationSequence: 81 });
      expect(old.seen("toFile", "old-expired-append")).toBeUndefined();
      const lines = fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_FILE), "utf8").trimEnd().split("\n");
      expect(lines).toHaveLength(candidate.commands.length + candidate.seen.length + 1); // Actual compaction, not just open.
      expect(evidence(working).native).toEqual(pristine.native); // Old reader ignores/preserves EVERY native-ingress file.
    } finally { await old.close(); }
    noLock(working);
    old = await source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now });
    try { expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected); }
    finally { await old.close(); }
    noLock(working); unchangedOriginals();
  });

  it("uses one full-line write durably before visibility; a real mid-append crash remains ca4 SOURCE-readable", async () => {
    const input = appendInput(candidate, "actual-append-cut");
    const valid = fs.readFileSync(path.join(sealed, BRIDGE_NATS_LEDGER_FILE));
    const normal = ownCopy("normal-child-working"), crashed = ownCopy("crash-child-working");
    // Normal counterexample: identical actual candidate claim, but no persistence fault/crash.
    const completed = await candidateAppendChild(root, normal, candidate.now, input, "normal");
    expect(completed.observation).toMatchObject({ stage: "claim-resolved-after-fsync", mode: "normal",
      writeCalls: 1, fsyncCalls: 1, truncateCalls: 0, visibleAtWrite: false, visibleAtFsync: false,
      requestedBytes: completed.line.length, persistedBytes: completed.line.length });
    expect(completed.line.at(-1)).toBe(0x0a);
    expect(completed.line.indexOf(0x0a)).toBe(completed.line.length - 1);
    expect(completed.line.length).toBeGreaterThan(completed.line.toString("utf8").length); // Check BYTES, not UTF-16 length.
    expect(fs.readFileSync(path.join(normal, BRIDGE_NATS_LEDGER_FILE))).toEqual(Buffer.concat([valid, completed.line]));
    noLock(normal);
    const sealedNormal = ownCopy("sealed-normal-child", normal); // Before ANY old open.

    // A controlled fault persists a prefix of the buffer from the REAL candidate #append,
    // then the owned child actually dies without returning from writeSync/closing its ledger.
    const killed = await candidateAppendChild(root, crashed, candidate.now, input, "crash");
    expect(killed.observation).toMatchObject({ stage: "prefix-persisted-before-write-return", mode: "crash",
      writeCalls: 1, fsyncCalls: 0, truncateCalls: 0, visibleAtWrite: false, requestedBytes: killed.line.length });
    expect(killed.line).toEqual(completed.line); // Exact candidate encoder bytes, not a handwritten damaged string.
    const prefix = killed.line.subarray(0, killed.observation.persistedBytes);
    expect(prefix.length).toBeGreaterThan(0); expect(prefix.length).toBeLessThan(killed.line.length);
    expect(prefix.includes(0x0a)).toBe(false);
    const damaged = Buffer.concat([valid, prefix]);
    expect(fs.readFileSync(path.join(crashed, BRIDGE_NATS_LEDGER_FILE))).toEqual(damaged);
    const deadOwner = fs.readFileSync(path.join(crashed, BRIDGE_NATS_LEDGER_LOCK, "owner"));
    expect(Number(deadOwner.toString().split("\n")[1])).toBe(killed.observation.pid);
    // Native close/SIGKILL was awaited above BEFORE copying evidence or letting the old
    // reader judge the real dead custody lock. No manual lock removal or synthetic owner.
    const sealedCrash = ownCopy("sealed-actual-crash", crashed), crashEvidence = evidence(sealedCrash);
    expect(crashEvidence.native).toEqual(pristine.native);

    let old = await source.openBridgeNatsLedger({ dir: normal, clock: () => candidate.now, minCompactLines: 10_000 });
    try {
      const normalState = snapshot(old, candidate.commands, candidate.seen);
      expect({ ...normalState, pendingResends: normalState.pendingResends.filter(record => record.commandId !== input.commandId) })
        .toEqual(candidate.expected);
      const appended = old.lookup(input.direction, input.commandId)!;
      expect(appended).toMatchObject({ ...input, state: "claimed", claimedAt: candidate.now });
      expect(old.lookupOriginal(input.direction, input.requesterId, input.originalCommandId)).toEqual(appended);
      expect((await old.claim(input)).created).toBe(false);
      for (const record of candidate.commands) expect((await old.claim(claimInput(record))).created).toBe(false);
      expect(evidence(normal)).toEqual(evidence(sealedNormal));
    } finally { await old.close(); }
    noLock(normal);
    old = await source.openBridgeNatsLedger({ dir: crashed, clock: () => candidate.now, minCompactLines: 10_000 });
    try {
      expect(fs.readFileSync(path.join(crashed, BRIDGE_NATS_LEDGER_FILE))).toEqual(valid);
      expect(old.lookup(input.direction, input.commandId)).toBeUndefined(); // Unresolved/unpublished new claim only.
      expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
      for (const record of candidate.commands) expect((await old.claim(claimInput(record))).created).toBe(false);
      expect(old.pendingResends()).toEqual(candidate.expected.pendingResends); // Inspect only; NEVER dispatch unknown work.
      expect(evidence(crashed).native).toEqual(pristine.native);
    } finally { await old.close(); }
    noLock(crashed);
    old = await source.openBridgeNatsLedger({ dir: crashed, clock: () => candidate.now, minCompactLines: 10_000 });
    try {
      expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
      expect(evidence(crashed)).toEqual(pristine); // Truncated prefix remains gone across a second old open.
    } finally { await old.close(); }
    noLock(crashed);
    expect(evidence(sealedCrash)).toEqual(crashEvidence);
    expect(fs.readFileSync(path.join(sealedCrash, BRIDGE_NATS_LEDGER_LOCK, "owner"))).toEqual(deadOwner);
    unchangedOriginals();
  }, 90_000);

  it.each(["short", "zero", "write-error", "fsync-error"] as const)(
    "fails stop on %s without retry, visibility or a later queued append; old SOURCE retains the prior ledger", async mode => {
      const working = ownCopy(`failed-${mode}`), file = path.join(working, BRIDGE_NATS_LEDGER_FILE);
      const before = fs.readFileSync(file), input = appendInput(candidate, `failed-${mode}`);
      const ledger = await openBridgeNatsLedger({ dir: working, clock: () => candidate.now, minCompactLines: 10_000 });
      const inode = fs.statSync(file), owner = fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_LOCK, "owner"));
      const isLedger = (fd: number) => { const stat = fs.fstatSync(fd); return stat.dev === inode.dev && stat.ino === inode.ino; };
      const realWrite = fs.writeSync.bind(fs), realSync = fs.fsyncSync.bind(fs), realTruncate = fs.ftruncateSync.bind(fs);
      const ioError = Object.assign(new Error(`injected ${mode}`), { code: "EIO" });
      let writes = 0, syncs = 0;
      const truncations: number[] = [];
      const writeSpy = vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, offset = 0,
        length = buffer.length, position: number | null = null) => {
        if (!isLedger(fd)) return realWrite(fd, buffer, offset, length, position);
        writes++;
        // Bounded negative control: the PRE-FIX zero-write loop throws on a second call,
        // rather than hanging the test runner. A second write is still an assertion failure.
        if (writes > 1) throw new Error("journal append retried after a short/zero write");
        expect(offset).toBe(0); expect(length).toBe(buffer.length);
        expect(buffer.indexOf(0x0a)).toBe(buffer.length - 1);
        expect(ledger.lookup(input.direction, input.commandId)).toBeUndefined();
        if (mode === "zero") return 0;
        if (mode === "fsync-error") return realWrite(fd, buffer, offset, length, position);
        const count = realWrite(fd, buffer, offset, Math.min(17, length), position);
        if (mode === "write-error") throw ioError;
        return count;
      }) as unknown as typeof fs.writeSync);
      const syncSpy = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        if (isLedger(fd)) {
          syncs++;
          expect(ledger.lookup(input.direction, input.commandId)).toBeUndefined();
          if (mode === "fsync-error" && syncs === 1) throw ioError;
        }
        realSync(fd);
      });
      const truncateSpy = vi.spyOn(fs, "ftruncateSync").mockImplementation((fd, length) => {
        if (isLedger(fd)) truncations.push(length ?? 0);
        realTruncate(fd, length);
      });
      try {
        const [failed, queued] = await Promise.allSettled([
          ledger.claim(input), ledger.markSeen("toNats", `queued-after-${mode}`),
        ]);
        expect(failed.status).toBe("rejected"); expect(queued.status).toBe("rejected");
        if (failed.status !== "rejected" || queued.status !== "rejected") throw new Error("fault did not stop queued mutations");
        if (mode === "short" || mode === "zero") expect(failed.reason.message).toMatch(/^ledger append was short /);
        else expect(failed.reason).toBe(ioError);
        expect(queued.reason).toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
        await expect(ledger.markSeen("toFile", `later-after-${mode}`)).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
        expect(writes).toBe(1); expect(syncs).toBe(mode === "fsync-error" ? 2 : 1);
        expect(truncations).toEqual([before.length]);
        expect(ledger.lookup(input.direction, input.commandId)).toBeUndefined();
        expect(ledger.lookupOriginal(input.direction, input.requesterId, input.originalCommandId)).toBeUndefined();
        expect(ledger.seen("toNats", `queued-after-${mode}`)).toBeUndefined();
        expect(ledger.seen("toFile", `later-after-${mode}`)).toBeUndefined();
        expect(snapshot(ledger, candidate.commands, candidate.seen)).toEqual(candidate.expected);
        expect(fs.readFileSync(file)).toEqual(before);
        expect(fs.readFileSync(path.join(working, BRIDGE_NATS_LEDGER_LOCK, "owner"))).toEqual(owner);
      } finally {
        writeSpy.mockRestore(); syncSpy.mockRestore(); truncateSpy.mockRestore();
        await ledger.close();
      }
      noLock(working);
      const sealedFailure = ownCopy(`sealed-failed-${mode}`, working);
      const old = await source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now, minCompactLines: 10_000 });
      try {
        expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
        expect(old.lookup(input.direction, input.commandId)).toBeUndefined();
        expect(evidence(working)).toEqual(evidence(sealedFailure));
        expect(evidence(working).native).toEqual(pristine.native);
      } finally { await old.close(); }
      noLock(working); unchangedOriginals();
    });

  it.each(["", '{"t":"cmd","r":'])(
    "refuses an UNPARSEABLE MIDDLE line before valid candidate records, even with crash-tail suffix %j", async tail => {
      const working = ownCopy(tail ? "middle-and-tail-working" : "middle-working");
      const file = path.join(working, BRIDGE_NATS_LEDGER_FILE), valid = fs.readFileSync(file);
      const cut = valid.indexOf(0x0a) + 1;
      expect(cut).toBeGreaterThan(0); expect(cut).toBeLessThan(valid.length);
      const corrupt = Buffer.concat([valid.subarray(0, cut), Buffer.from("UNPARSEABLE MIDDLE\n"), valid.subarray(cut), Buffer.from(tail)]);
      fs.writeFileSync(file, corrupt, { mode: 0o600 });
      const quarantine = ownCopy(tail ? "sealed-middle-and-tail" : "sealed-middle", working);
      await expect(source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CORRUPT" });
      noLock(working);
      expect(fs.readFileSync(file)).toEqual(corrupt);
      expect(fs.readFileSync(path.join(quarantine, BRIDGE_NATS_LEDGER_FILE))).toEqual(corrupt);
      expect(evidence(working)).toEqual(evidence(quarantine));
      // No bad-line edit, deletion, backup restoration, replacement ledger, link start or replay.
      // Operating result under DESIGN §8 is STOPPED with evidence, NOT traffic restoration.
      // Only the lead may choose backup/fresh recovery, with disclosed dedupe risk and an opKey
      // dedupe check afterwards. This test never silently repairs or resumes either release.
      unchangedOriginals();
    });

  it.each(["no-newline-tail", "checksum-tail"])(
    "handles only ONE documented %s append, retaining originals and never making known work new", async name => {
      const working = ownCopy(`${name}-working`), file = path.join(working, BRIDGE_NATS_LEDGER_FILE);
      const valid = fs.readFileSync(file);
      const completeLine = valid.toString("utf8").split("\n")[0]!;
      // Negative seed only: copy a GENUINE candidate line, altering just its checksum, not
      // a positive fixture encoder or an edited/salvaged old-source implementation.
      const badChecksum = completeLine.replace(/"h":"([0-9a-f])/, (_match, first: string) => `"h":"${first === "0" ? "1" : "0"}`);
      expect(badChecksum).not.toBe(completeLine);
      const tail = name === "no-newline-tail" ? '{"t":"cmd","r":' : `${badChecksum}\n`;
      fs.appendFileSync(file, tail);
      const sealedTail = ownCopy(`sealed-${name}`, working), damaged = fs.readFileSync(file);
      const old = await source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now, minCompactLines: 10_000 });
      try {
        expect(fs.readFileSync(file)).toEqual(valid); // Old source's documented single-tail truncation only.
        expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
        for (const record of candidate.commands) expect((await old.claim(claimInput(record))).created).toBe(false);
        expect(old.pendingResends()).toEqual(candidate.expected.pendingResends); // Unknown remains unknown; not dispatched.
      } finally { await old.close(); }
      noLock(working);
      expect(fs.readFileSync(path.join(sealedTail, BRIDGE_NATS_LEDGER_FILE))).toEqual(damaged);
      expect(evidence(working).native).toEqual(pristine.native);
      unchangedOriginals();
    });

  it.each(["mode", "uid"] as const)(
    "R4: candidate OPEN refuses receipt-directory %s drift; ca4 SOURCE ignores it without changing evidence", async defect => {
      const working = ownCopy(`directory-open-${defect}`), directory = path.join(working, NATIVE_INGRESS_DIRECTORY);
      const before = evidence(working), realLstat = fs.lstatSync.bind(fs);
      // Mode fault is real on the test-owned directory. UID is injected only at its stat boundary:
      // no privileged chown or claim that this is an installed-filesystem ownership-drift probe.
      if (defect === "mode") fs.chmodSync(directory, 0o750);
      const stat = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
        const result = realLstat(...args);
        if (defect === "uid" && String(args[0]) === directory && result) {
          return new Proxy(result, { get: (target, property) => property === "uid" ? process.geteuid!() + 1 : Reflect.get(target, property) });
        }
        return result;
      }) as typeof fs.lstatSync);
      try {
        await expect(openBridgeNatsLedger({ dir: working, clock: () => candidate.now })).rejects.toThrow(/unsafe receipt directory/);
        noLock(working);
        const old = await source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now });
        try {
          expect(snapshot(old, candidate.commands, candidate.seen)).toEqual(candidate.expected);
          expect(evidence(working)).toEqual(before);
        } finally { await old.close(); }
      } finally { stat.mockRestore(); if (defect === "mode") fs.chmodSync(directory, 0o700); }
      // Explicit fault removal is TEST setup, never an automatic production repair policy.
      const reopened = await openBridgeNatsLedger({ dir: working, clock: () => candidate.now });
      try { expect(snapshot(reopened, candidate.commands, candidate.seen)).toEqual(candidate.expected); }
      finally { await reopened.close(); }
      noLock(working); unchangedOriginals();
    });

  it.each(["mode", "uid"] as const)(
    "R4: receipt-directory %s drift after compaction rename fail-stops later mutations until valid reopen", async defect => {
      const working = ownCopy(`directory-compact-${defect}`), directory = path.join(working, NATIVE_INGRESS_DIRECTORY);
      const clock = { now: candidate.now - BRIDGE_NATS_HORIZON_MS - 1 };
      const ledger = await openBridgeNatsLedger({ dir: working, clock: () => clock.now, minCompactLines: 1 });
      const receipts = evidence(working).native, realLstat = fs.lstatSync.bind(fs);
      const file = path.join(working, BRIDGE_NATS_LEDGER_FILE), inode = fs.statSync(file);
      const lineCount = () => fs.readFileSync(file, "utf8").trimEnd().split("\n").length;
      const originalLines = lineCount(), retainedCount = candidate.commands.length + candidate.seen.length;
      // Bound DISTINCT durable writes by the complete known fixture, including the fresh trigger.
      // At the past bookkeeping clock every pressure record is live, so seeding cannot compact.
      // Advance only forward to candidate.now: all ORIGINAL claims/seen/receipts remain live and
      // unchanged. This component clock is not wall-clock liveness/idle/600s evidence.
      const pressure = Array.from({ length: 4 * (retainedCount + 1) }, (_, index) => `r4-expired-pressure-${index}`);
      try {
        expect(snapshot(ledger, candidate.commands, candidate.seen)).toEqual(candidate.expected);
        for (const id of pressure) expect((await ledger.markSeen("toFile", id)).expiresAt).toBeLessThan(candidate.now);
        expect(lineCount()).toBe(originalLines + pressure.length);
        expect(fs.statSync(file).ino).toBe(inode.ino);
        clock.now = candidate.now;
        expect(snapshot(ledger, candidate.commands, candidate.seen)).toEqual(candidate.expected);
      } catch (error) { await ledger.close(); throw error; }
      if (defect === "mode") fs.chmodSync(directory, 0o750);
      const stat = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
        const result = realLstat(...args);
        if (defect === "uid" && String(args[0]) === directory && result) {
          return new Proxy(result, { get: (target, property) => property === "uid" ? process.geteuid!() + 1 : Reflect.get(target, property) });
        }
        return result;
      }) as typeof fs.lstatSync);
      try {
        // Normal append crosses the STRICT lines > 4*live threshold with expired pressure.
        // A changed real inode + exact retained line count prove actual replacement, not a
        // private compactor call, mocked rename success or a source-text assertion.
        await ledger.markSeen("toFile", "r4-compaction-trigger");
        const compacted = fs.statSync(file);
        expect(compacted.dev).toBe(inode.dev); expect(compacted.ino).not.toBe(inode.ino);
        expect(lineCount()).toBe(retainedCount + 1);
        for (const id of pressure) expect(ledger.seen("toFile", id)).toBeUndefined();
        expect(snapshot(ledger, candidate.commands, candidate.seen)).toEqual(candidate.expected);
        // The rename-triggering mutation may resolve; its post-rename prune fault latches #failed.
        // Probe the NEXT public mutation: an existing-key dedupe must not bypass CLOSED either.
        let stopped = false;
        try { await ledger.markSeen("toFile", "r4-compaction-trigger"); }
        catch (error) { expect(error).toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" }); stopped = true; }
        expect(stopped).toBe(true);
        await expect(ledger.markSeen("toFile", "r4-after-fail-stop")).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
        expect(ledger.seen("toFile", "r4-after-fail-stop")).toBeUndefined();
        expect(evidence(working).native).toEqual(receipts);
      } finally {
        stat.mockRestore(); if (defect === "mode") fs.chmodSync(directory, 0o700);
        await ledger.close();
      }
      noLock(working);
      const reopened = await openBridgeNatsLedger({ dir: working, clock: () => candidate.now });
      try {
        expect(snapshot(reopened, candidate.commands, candidate.seen)).toEqual(candidate.expected);
        expect(reopened.seen("toFile", "r4-compaction-trigger")).toBeDefined();
        expect(reopened.seen("toFile", "r4-after-fail-stop")).toBeUndefined();
        for (const id of pressure) expect(reopened.seen("toFile", id)).toBeUndefined();
        expect(evidence(working).native).toEqual(receipts);
      } finally { await reopened.close(); }
      noLock(working); unchangedOriginals();
    });

  it("refuses TWO damaged suffix appends instead of labelling arbitrary evidence loss a crash tail", async () => {
    const working = ownCopy("two-tail-working"), file = path.join(working, BRIDGE_NATS_LEDGER_FILE);
    fs.appendFileSync(file, 'damaged-complete-line\n{"t":"cmd"');
    const quarantine = ownCopy("sealed-two-tail", working), before = evidence(working);
    await expect(source.openBridgeNatsLedger({ dir: working, clock: () => candidate.now })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CORRUPT" });
    noLock(working);
    expect(evidence(working)).toEqual(before); expect(evidence(quarantine)).toEqual(before);
    unchangedOriginals();
  });
});
