// Keep: trust boundary + data safety. Real carrier, identity/allow gate, durable ledger/projection,
// StoreBridgeSide publication and persisted MeshStore readback. KV/delivery fixtures are NOT a real
// broker, full MeshBridge, control plane or native callback; parent owns that decisive integration.
// Expected platform gap: receipts require POSIX no-follow + namespace durability, not Windows.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { NativeActorBinding } from "../src/actors/native-handler.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { residentHostId } from "../src/residency/protocol.js";
import { issueRootScope, sealRootScope } from "../src/scope.js";
import * as ingressRecipe from "../src/mesh/bridge-nats-ingress.js";
import { StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { NatsBridgeCarrier, type NatsCarrierLedger, type NatsInbound } from "../src/mesh/bridge-nats-carrier.js";
import { NatsCarrierSide } from "../src/mesh/bridge-nats-file.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
import {
  claimNativeAskAdmission, consumeNativeAskAdmission, createNativeActorIngress, encodeNativeIngressReceipt, NATIVE_INGRESS_DIRECTORY,
  NATIVE_INGRESS_MAX_BYTES, readNativeIngressReceipt, type NativeControlAdmission,
} from "../src/mesh/bridge-nats-ingress.js";
import { BRIDGE_NATS_LEDGER_FILE, openBridgeNatsLedger, type FileBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger, natsLinkAuthorizers, startNatsLink, type NatsLinkHandle, type NatsLinkStartConfig } from "../src/mesh/bridge-nats-link.js";
import {
  BRIDGE_NATS_HORIZON_MS, bridgeNatsMappedCommandId, type BridgeNatsPresenceRecord, type BridgeNatsPresenceView,
} from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import {
  BRIDGE_MACHINE, FakePort, FakePresence, LINK, POD_MACHINE, fileLead, natsLead, otherNats, route, settle,
} from "./helpers/bridge-nats-l2-fakes.js";

const ACTOR = "34200000000000000000000000000001";
const OWNER = residentHostId(fileLead.id);
const ACTOR_IDENTITY: MeshIdentity = { id: ACTOR, name: "native-inbox", kind: "actor" };
const principal = { id: "herdr:principal-a", binding: "herdr-client" as const };
const roots: string[] = [];
const ledgers: FileBridgeNatsLedger[] = [];
const stores: MeshStore[] = [];
const sides: NatsCarrierSide[] = [];
const links: NatsLinkHandle[] = [];
afterEach(async () => {
  for (const link of links.splice(0)) await link.stop();
  vi.restoreAllMocks();
  for (const side of sides.splice(0)) side.close();
  for (const ledger of ledgers.splice(0)) await ledger.close();
  for (const store of stores.splice(0)) store.closeState();
  // Only directories allocated by THIS test are ever deleted.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const eventOf = (plan: NatsInbound): MeshEvent => {
  if (plan.kind !== "event") throw new Error(`expected event, got ${plan.kind}: ${"reason" in plan ? plan.reason : ""}`);
  return plan.event;
};
const commandId = (source = "source-ask", sender = natsLead.id) => bridgeNatsMappedCommandId(LINK, "toFile", sender, source);
const bodyOf = (event: MeshEvent) => event.data as Record<string, unknown>;
const POSIX = process.platform !== "win32" && !!fs.constants.O_NOFOLLOW && !!fs.constants.O_DIRECTORY && !!process.geteuid;

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "bridge-native-ingress-"));
  roots.push(root);
  const clock = { now: Date.now() };
  const dir = path.join(root, "ledger");
  const ledger = await openBridgeNatsLedger({ dir, clock: () => clock.now, minCompactLines: 1 });
  ledgers.push(ledger);
  const store = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  stores.push(store);
  await store.prepareLock();
  const file = new StoreBridgeSide(store, LINK, () => clock.now);
  const presence = new FakePresence();
  presence.natsEntries.set(ACTOR, route(ACTOR_IDENTITY, BRIDGE_MACHINE, "native-owner-inc", { ownerHostId: OWNER, ownerIdentityId: OWNER }));
  const rootRecord = {
    id: fileLead.id, name: fileLead.name, kind: "main", rootId: fileLead.id, ownerHostId: fileLead.id,
    ownerIdentityId: fileLead.id, live: true as boolean, observedAt: clock.now,
  } satisfies BridgeNatsPresenceRecord;
  const actorRecord: BridgeNatsPresenceRecord = {
    id: ACTOR, name: ACTOR_IDENTITY.name, kind: "actor", rootId: fileLead.id, ownerHostId: OWNER, ownerIdentityId: OWNER,
    ownerIncarnation: "native-owner-inc", live: true, observedAt: clock.now,
  };
  const fileView = (): BridgeNatsPresenceView => ({ side: "file", records: id =>
    [rootRecord, actorRecord].filter(record => record.id === id).map(record => ({ ...record, observedAt: clock.now })) });
  const natsView = (): BridgeNatsPresenceView => ({ side: "nats", records: id => {
    const entry = presence.nats(id);
    return entry ? [{ id, name: entry.identity.name, kind: entry.identity.kind,
      rootId: entry.identity.kind === "main" ? id : fileLead.id,
      ...(entry.identity.sessionId !== undefined ? { sessionId: entry.identity.sessionId } : {}),
      ownerHostId: entry.route.ownerHostId, ownerIdentityId: entry.route.ownerIdentityId,
      ownerIncarnation: entry.route.ownerIncarnation, machine: entry.route.machine, live: entry.route.live, observedAt: clock.now }] : [];
  } });
  const allow = parseBridgeNatsAllowSection({
    file: [{ kind: "root", id: fileLead.id, send: [], receive: [] },
      { kind: "actor", rootId: fileLead.id, name: ACTOR_IDENTITY.name, id: ACTOR, send: [], receive: ["ask", "cancel"] }],
    nats: [natsLead, otherNats].map(sender => ({ kind: "root", id: sender.id, machine: POD_MACHINE,
      send: ["ask", "cancel"], receive: [], principals: [principal.id] })),
  }, { link: LINK, natsMachine: BRIDGE_MACHINE });
  const gate = createBridgeNatsIdentityGate(allow, { pins: { seats: new Map(), bound: new Map() }, clock: () => clock.now });
  const { authorize, authorizeTopic } = natsLinkAuthorizers(gate, { file: fileView, nats: natsView });
  const port = natsCarrierLedger(ledger, LINK);
  const carrierFor = (ledgerPort: NatsCarrierLedger = port) => new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE,
    presence, ledger: ledgerPort, authorize, now: () => clock.now });
  const carrier = carrierFor();
  const binding: NativeActorBinding = { actorId: ACTOR, rootId: fileLead.id, scope: "session", name: ACTOR_IDENTITY.name,
    handlerKey: "smarty-inbox.rpc.v1", residentModule: path.join(root, "not-imported.mjs"), ingress: { link: LINK, ledgerDir: dir } };
  const verifier = (bindings: readonly NativeActorBinding[] = [binding]) => createNativeActorIngress({ bindings, now: () => clock.now });
  const envelope = (body: Record<string, unknown> = {}, extra: Record<string, unknown> = {}) => ({
    id: "source-envelope", kind: "command", to: { machine: BRIDGE_MACHINE, hostId: OWNER }, targetId: ACTOR,
    operation: body.operation ?? "ask", from: natsLead, principal,
    body: { version: 1, commandId: "source-ask", targetId: ACTOR, operation: "ask", replyTo: natsLead.id,
      ownerIncarnation: "native-owner-inc", message: " smarty-inbox.rpc.v1 ",
      data: { v: 1, requestId: "r1", operation: "add", opKey: "opaque", args: { title: "a", nested: [null, true, 2] } },
      requestCreatedAt: clock.now, requestedAt: clock.now, deadlineAt: clock.now + 60_000, ...body }, ...extra,
  });
  const deliver = (input = envelope(), machine = POD_MACHINE) => carrier.inbound({ envelope: input, sourceMachine: machine, sequence: 1 });
  const persist = async (raw: MeshEvent): Promise<MeshEvent> => {
    // Actual StoreBridgeSide reconstruction. MeshBridge's reviewed fixed toLocal stamp is supplied
    // here, not a context/proof fixture; authority was already persisted by the genuine carrier.
    await file.publish({ topic: raw.topic, kind: raw.kind, from: raw.from, to: raw.to!, principal: raw.principal,
      data: { ...bodyOf(raw), bridge: { from: LINK, id: raw.id } } } as BridgePublish);
    return store.read({ after: 0, limit: 100 }).find(event => bodyOf(event).commandId === bodyOf(raw).commandId)!;
  };
  const request = (event: MeshEvent) => ({ actorId: ACTOR, message: bodyOf(event).message as string, data: clone(bodyOf(event).data) });
  return { root, dir, clock, ledger, store, file, presence, rootRecord, actorRecord, authorize, authorizeTopic,
    binding, verifier, envelope, deliver, persist, request, carrier, carrierFor, port };
};

/** R4 ordinary-traffic boundary: actual startNatsLink, MeshBridge loops, file store and ledger.
 * Only the NATS port/KV presence is fake. No owner reply, native context, ACK or callback is minted.
 * Historical R4 SOURCE/component runs are transcribed in docs/native-actors/r5-final-receipt.md;
 * distinct from the earlier N12 11 + ledger 24 receipt, and never installed-artifact proof.
 */
const ordinaryLinkFixture = async (ledgerClock?: () => number) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "r4-ordinary-link-")); roots.push(root);
  const now = Date.now(), mesh = path.join(root, "mesh"), stateDir = path.join(root, "state");
  const store = new MeshStore(mesh, 256 * 1024, 500); stores.push(store);
  await store.prepareLock();
  const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
  const host = (identity: MeshIdentity): FabricHostRecord => ({ format: 1, id: identity.id, rootId: identity.id, identity,
    startedAt: now - 1_000, updatedAt: now, expiresAt: now + 3_600_000 });
  const participant = (identity: MeshIdentity): FabricParticipantRecord => ({ format: 1, id: identity.id, kind: "root",
    rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id, ownerIncarnation: `inc-${identity.name}`,
    name: identity.name, label: identity.name, status: "idle", runner: "pi", transport: "host" as const,
    capabilities: ["steer", "followUp"], sessionId: identity.id.slice(8), startedAt: now - 1_000, updatedAt: now,
    controlProtocol: "v1" as const });
  await store.put({ key: key("topology/hosts/", fileLead.id), identity: fileLead, value: host(fileLead) });
  await store.put({ key: key("topology/participants/", fileLead.id), identity: fileLead, value: participant(fileLead) });
  const config: NatsLinkStartConfig = { name: "r4-fixture", link: LINK, file: { mesh, lockProtocol: 1 }, stateDir,
    allow: { file: [{ kind: "root", id: fileLead.id, send: ["ask", "cancel"], receive: ["ask", "cancel"] }],
      nats: [{ kind: "root", id: natsLead.id, machine: POD_MACHINE, send: ["ask", "cancel"], receive: ["ask", "cancel"] }] },
    nats: { servers: ["tls://nats.invalid:4222"], machine: BRIDGE_MACHINE, credentials: path.join(root, "unused.creds"),
      caFile: path.join(root, "unused.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
      pathBMachines: [BRIDGE_MACHINE, POD_MACHINE] } };
  const ports: FakePort[] = [];
  const start = async () => {
    const port = new FakePort(); ports.push(port);
    for (const [identity, machine] of [[natsLead, POD_MACHINE], [fileLead, BRIDGE_MACHINE]] as const) {
      port.routes.set(identity.id, route(identity, machine, `inc-${identity.name}`));
    }
    port.natsPresence = { hosts: [{ record: host(natsLead), expiresAt: now + 3_600_000, observedAt: now }],
      participants: [participant(natsLead)], reserved: [natsLead.id] };
    let ledger: FileBridgeNatsLedger | undefined;
    const handle = await startNatsLink(config, { signal: new AbortController().signal, log: () => undefined, factories: {
      openPort: async () => port, seatPins: () => ({ seats: new Map(), bound: new Map() }),
      // Ledger bookkeeping only: carrier/presence/deadlines and real wall timers stay unchanged.
      openLedger: async dir => { ledger = await openBridgeNatsLedger({ dir, minCompactLines: 1,
        ...(ledgerClock ? { clock: ledgerClock } : {}) }); ledgers.push(ledger); return ledger; },
    } });
    links.push(handle);
    return { port, handle, ledger: ledger! };
  };
  const envelope = (source: string, operation: "ask" | "cancel" = "ask") => ({ id: `r4-envelope-${source}`, kind: "command",
    from: natsLead, to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: fileLead.id, operation,
    body: { version: 1, commandId: source, targetId: fileLead.id, operation, ownerIncarnation: `inc-${fileLead.name}`,
      replyTo: natsLead.id, message: "ordinary non-native command", requestedAt: now, deadlineAt: now + 60_000,
      ...(operation === "cancel" ? { cancelCommandId: "ordinary-ask" } : {}) } });
  const accepted = async (port: FakePort, source: string, operation: "ask" | "cancel" = "ask") => {
    let delivery!: ReturnType<FakePort["deliver"]>;
    await new Promise<void>(resolve => { delivery = port.deliver(envelope(source, operation), POD_MACHINE, undefined, resolve); });
    expect(delivery.acked).toBe(1); expect(delivery.termed).toEqual([]);
    const id = commandId(source), receipt = readNativeIngressReceipt(path.join(stateDir, "ledger"), id);
    expect(receipt).toMatchObject({ targetId: fileLead.id, operation, commandId: id });
    expect(store.read({ after: 0, limit: 500 }).filter(event => bodyOf(event)?.commandId === id)).toHaveLength(1);
    return receipt;
  };
  return { root, now, store, ports, start, envelope, accepted, dir: path.join(stateDir, "ledger") };
};

describe.skipIf(!POSIX)("R4 ordinary receipt faults stop the WHOLE real link (fake NATS port only)", () => {
  it.each(["mode", "uid"] as const)("OPEN directory %s drift refuses start for ordinary traffic, then valid custody reopens", async defect => {
    const f = await ordinaryLinkFixture(), first = await f.start();
    await f.accepted(first.port, "ordinary-ask"); await first.handle.stop(); await first.handle.closed;
    const directory = path.join(f.dir, NATIVE_INGRESS_DIRECTORY), realLstat = fs.lstatSync.bind(fs);
    if (defect === "mode") fs.chmodSync(directory, 0o750);
    const stat = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
      const result = realLstat(...args);
      return defect === "uid" && String(args[0]) === directory && result
        ? new Proxy(result, { get: (target, property) => property === "uid" ? process.geteuid!() + 1 : Reflect.get(target, property) }) : result;
    }) as typeof fs.lstatSync);
    try {
      await expect(f.start()).rejects.toThrow(/unsafe receipt directory/);
      expect(f.ports.at(-1)!.closed).toBe(true);
    } finally { stat.mockRestore(); if (defect === "mode") fs.chmodSync(directory, 0o700); }
    const valid = await f.start();
    await f.accepted(valid.port, "ordinary-cancel", "cancel");
    expect(valid.port.sent).toHaveLength(0); // No synthetic owner reply/notRun.
  }, 15_000);

  it.each(["mode", "uid"] as const)("post-rename compaction directory %s drift fail-stops the active WHOLE link", async defect => {
    const clock = { now: Date.now() - BRIDGE_NATS_HORIZON_MS - 1 };
    const f = await ordinaryLinkFixture(() => clock.now), first = await f.start();
    await f.accepted(first.port, "ordinary-ask");
    const directory = path.join(f.dir, NATIVE_INGRESS_DIRECTORY), before = f.store.read({ after: 0, limit: 500 });
    const original = clone(first.ledger.lookup("toFile", commandId("ordinary-ask"))!);
    const receiptFile = path.join(directory, `${original.commandId}.json`), receiptBytes = fs.readFileSync(receiptFile);
    const file = path.join(f.dir, BRIDGE_NATS_LEDGER_FILE), inode = fs.statSync(file);
    const lineCount = () => fs.readFileSync(file, "utf8").trimEnd().split("\n").length;
    const originalLines = lineCount();
    // One retained claim + the fresh trigger will be live. These DISTINCT public writes are
    // live while seeded, then strictly expired after ONE monotonic bookkeeping-clock advance.
    // This is component maintenance coverage, not wall-clock liveness/idle/600s evidence.
    const pressure = Array.from({ length: 4 * (1 + 1) }, (_, index) => `r4-active-expired-${index}`);
    for (const id of pressure) expect((await first.ledger.markSeen("toFile", id)).expiresAt).toBeLessThan(f.now);
    expect(lineCount()).toBe(originalLines + pressure.length);
    expect(fs.statSync(file).ino).toBe(inode.ino); // Seeding itself did not compact.
    clock.now = f.now;
    expect(first.ledger.lookup("toFile", original.commandId)).toEqual(original);
    expect(readNativeIngressReceipt(f.dir, original.commandId).expiresAt).toBeGreaterThan(clock.now);
    const realLstat = fs.lstatSync.bind(fs);
    if (defect === "mode") fs.chmodSync(directory, 0o750);
    const stat = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
      const result = realLstat(...args);
      return defect === "uid" && String(args[0]) === directory && result
        ? new Proxy(result, { get: (target, property) => property === "uid" ? process.geteuid!() + 1 : Reflect.get(target, property) }) : result;
    }) as typeof fs.lstatSync);
    try {
      // Existing lines + expired pressure + this append exceed 4 * two live records.
      // The real normal compactor replaces the inode, then receipt pruning faults.
      await first.ledger.markSeen("toFile", "r4-active-compaction");
      const compacted = fs.statSync(file);
      expect(compacted.dev).toBe(inode.dev); expect(compacted.ino).not.toBe(inode.ino);
      expect(lineCount()).toBe(2); // Original claim + fresh seen record, no expired pressure.
      for (const id of pressure) expect(first.ledger.seen("toFile", id)).toBeUndefined();
      expect(first.ledger.lookup("toFile", original.commandId)).toEqual(original);
      expect(fs.readFileSync(receiptFile)).toEqual(receiptBytes);
      // The triggering mutation can resolve with the post-rename failure latched. A subsequent
      // PUBLIC mutation must be CLOSED, even when its key already exists (dedupe is no bypass).
      let stopped = false;
      try { await first.ledger.markSeen("toFile", "r4-active-compaction"); }
      catch (error) { expect(error).toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" }); stopped = true; }
      expect(stopped).toBe(true);
      // Observe the ACTUAL running composition's next carrier mutation propagate the latch.
      const failure = expect(first.handle.closed).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
      const delivery = first.port.deliver(f.envelope("after-compaction"), POD_MACHINE);
      await failure;
      expect(first.port.closed).toBe(true); expect(first.port.sent).toHaveLength(0);
      expect(delivery.acked).toBe(0); expect(delivery.termed).toEqual([]);
      expect(f.store.read({ after: 0, limit: 500 })).toEqual(before);
    } finally { stat.mockRestore(); if (defect === "mode") fs.chmodSync(directory, 0o700); }
    const valid = await f.start(); await f.accepted(valid.port, "after-compaction");
    expect(valid.ledger.lookup("toFile", commandId("ordinary-ask"))).toBeDefined();
    expect(valid.ledger.lookup("toFile", original.commandId)).toEqual(original);
    expect(fs.readFileSync(receiptFile)).toEqual(receiptBytes);
    expect(valid.ledger.seen("toFile", "r4-active-compaction")).toBeDefined();
    for (const id of pressure) expect(valid.ledger.seen("toFile", id)).toBeUndefined();
    await valid.handle.stop(); await valid.handle.closed;
  }, 15_000);

  describe.each(["ask", "cancel"] as const)("duplicate ordinary %s", operation => {
    it.each(["recipe", "issued-scope", "fsync", "namespace"] as const)(
      "%s fault rejects link.closed without ACK/exposure; stable recipe/scope permits retained replay", async defect => {
        // Fresh-process scope issuance is simulated only by resetting this test's process-wide holder,
        // as tests/scope.test.ts does. Actual issueRootScope/seal/processSender run; no sender mock.
        const symbol = Symbol.for("pi-fabric:scope:v1"), saved = Object.getOwnPropertyDescriptor(globalThis, symbol);
        const issue = (id: string) => {
          Reflect.deleteProperty(globalThis, symbol);
          issueRootScope({ principal: { id }, grants: [{ resource: "mesh:*", actions: ["read", "write"] }] });
          sealRootScope({});
        };
        let restoreFault: () => void = () => undefined;
        try {
          issue("test:r4-original-link");
          const f = await ordinaryLinkFixture(), first = await f.start();
          await f.accepted(first.port, "ordinary-ask");
          if (operation === "cancel") await f.accepted(first.port, "ordinary-cancel", "cancel");
          const source = `ordinary-${operation}`, file = path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId(source)}.json`);
          const before = fs.readFileSync(file), events = f.store.read({ after: 0, limit: 500 });
          await first.handle.stop(); await first.handle.closed;
          if (defect === "issued-scope") issue("test:r4-changed-link");
          if (defect === "recipe") {
            const recipe = ingressRecipe.nativeIngressReceiptForClaim;
            const spy = vi.spyOn(ingressRecipe, "nativeIngressReceiptForClaim").mockImplementation((record, link) => {
              const receipt = recipe(record, link);
              // Counterexample at the actual producer recipe seam, never a positive forged receipt.
              return { ...receipt, eventDigest: (receipt.eventDigest[0] === "0" ? "1" : "0") + receipt.eventDigest.slice(1) };
            });
            restoreFault = () => spy.mockRestore();
          }
          if (defect === "fsync" || defect === "namespace") {
            const inode = fs.statSync(defect === "fsync" ? file : path.dirname(file)), fsync = fs.fsyncSync.bind(fs);
            const spy = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
              const stat = fs.fstatSync(fd);
              if (stat.dev === inode.dev && stat.ino === inode.ino) throw Object.assign(new Error(`R4 ${defect} fault`), { code: "EIO" });
              fsync(fd);
            });
            restoreFault = () => spy.mockRestore();
          }
          // In-horizon resend still fails after a restart: no per-delivery exception or automatic repair.
          for (let attempt = 0; attempt < 2; attempt++) {
            const current = await f.start();
            const failure = expect(current.handle.closed).rejects.toThrow(defect === "recipe" || defect === "issued-scope" ? /conflicts/ : /R4 .* fault/);
            const delivery = current.port.deliver(f.envelope(source, operation), POD_MACHINE);
            await failure;
            expect(delivery.acked).toBe(0); expect(delivery.termed).toEqual([]);
            expect(current.port.closed).toBe(true); expect(current.port.sent).toHaveLength(0);
            await expect(current.ledger.markSeen("toFile", "after-link-stop")).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
            expect(f.store.read({ after: 0, limit: 500 })).toEqual(events);
            expect(fs.readFileSync(file)).toEqual(before);
          }
          restoreFault(); restoreFault = () => undefined; issue("test:r4-original-link");
          const valid = await f.start();
          await f.accepted(valid.port, source, operation);
          expect(fs.readFileSync(file)).toEqual(before);
          expect(valid.port.sent).toHaveLength(0);
          expect(f.store.read({ after: 0, limit: 500 })).toEqual(events); // Actual file dedupe, not another command/effect.
          await valid.handle.stop(); await valid.handle.closed;
        } finally {
          restoreFault(); Reflect.deleteProperty(globalThis, symbol);
          if (saved) Object.defineProperty(globalThis, symbol, saved);
        }
      }, 20_000);
  });
});

describe.skipIf(!POSIX)("native ingress: genuine durable claims, not public event authority", () => {
  it("admits a genuine persisted event after verifier reconstruction, absent optional old-pod sid, with exact opaque bytes", async () => {
    const f = await fixture();
    const raw = eventOf(await f.deliver(f.envelope({ data: { seat: "forged-seat", sender: "not-authority", anything: [1, { x: "y" }] } },
      { from: { ...natsLead, kind: "actor", name: "untrusted", sessionId: "forged" }, sourceMachine: "forged" })));
    const event = await f.persist(raw);
    const receipt = readNativeIngressReceipt(f.dir, commandId());
    expect(receipt).toMatchObject({ sourceMachine: POD_MACHINE, sender: { id: natsLead.id, kind: "main", sessionId: natsLead.id.slice(8) },
      commandId: commandId(), sourceCommandId: "source-ask", sourceEventId: raw.id, ownerHostId: OWNER, ownerIdentityId: OWNER });
    expect(receipt.expiresAt).toBeGreaterThanOrEqual(f.ledger.lookup("toFile", commandId())!.expiresAt);
    expect(event.sender).toEqual({ authority: "host" });
    expect(fs.statSync(path.join(f.dir, NATIVE_INGRESS_DIRECTORY)).mode & 0o777).toBe(0o700);
    expect(fs.statSync(path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId()}.json`)).mode & 0o777).toBe(0o600);
    const admission = f.verifier().admitNativeControl(event);
    const request = f.request(event);
    claimNativeAskAdmission(admission, request);
    const signal = new AbortController().signal;
    const context = consumeNativeAskAdmission(admission, request, signal);
    expect(context).toEqual({ actorId: ACTOR, source: "nats-bridge", sender: { id: natsLead.id, kind: "main", sessionId: natsLead.id.slice(8) },
      sourceMachine: POD_MACHINE, link: LINK, commandId: commandId(), sourceCommandId: "source-ask", sourceEventId: raw.id,
      deadlineAt: bodyOf(event).deadlineAt, signal });
    expect(Object.isFrozen(context)).toBe(true);
    expect(Object.isFrozen(context.sender)).toBe(true);
    expect(request.data).toEqual({ seat: "forged-seat", sender: "not-authority", anything: [1, { x: "y" }] });
    expect(request.message).toBe(" smarty-inbox.rpc.v1 "); // No transport/application schema or trimming here.
    await f.ledger.close();
    const reopened = await openBridgeNatsLedger({ dir: f.dir, clock: () => f.clock.now });
    ledgers.push(reopened);
    expect(f.verifier().admitNativeControl(event)).toBeDefined();
  });

  it("canonicalizes poisoned optional PRESENCE sid from its verified Main ID, never from the wire or data", async () => {
    const f = await fixture();
    const sender = f.presence.natsEntries.get(natsLead.id)!;
    f.presence.natsEntries.set(natsLead.id, { ...sender, identity: { ...sender.identity, sessionId: "attacker-session" } });
    const raw = eventOf(await f.deliver());
    expect(raw.from.sessionId).toBe(natsLead.id.slice(8));
    const event = await f.persist(raw);
    const handle = f.verifier().admitNativeControl(event), request = f.request(event);
    claimNativeAskAdmission(handle, request);
    expect(consumeNativeAskAdmission(handle, request, new AbortController().signal).sender.sessionId).toBe(natsLead.id.slice(8));
    const poison = clone(event); poison.from.sessionId = "attacker-session";
    expect(() => f.verifier().admitNativeControl(poison)).toThrow();
  });

  it("uses the existing long effective deadline clamp; keeps the original claim horizon", async () => {
    const f = await fixture();
    const requestedAt = f.clock.now, maximum = 24 * 60 * 60_000 + 60_000;
    const raw = eventOf(await f.deliver(f.envelope({ deadlineAt: requestedAt + maximum * 10 })));
    const event = await f.persist(raw);
    const receipt = readNativeIngressReceipt(f.dir, commandId());
    expect(receipt.deadlineAt).toBe(requestedAt + maximum);
    expect(receipt.expiresAt).toBe(requestedAt + maximum * 10 + BRIDGE_NATS_HORIZON_MS);
    const request = f.request(event), handle = f.verifier().admitNativeControl(event);
    claimNativeAskAdmission(handle, request);
    f.clock.now = requestedAt + maximum + 1;
    expect(() => consumeNativeAskAdmission(handle, request, new AbortController().signal)).toThrow(/expired/);
    expect(() => f.verifier().admitNativeControl(event)).toThrow(/expired/);
  });

  it("preserves generic data-only ask forwarding without minting native admission", async () => {
    const f = await fixture();
    const raw = eventOf(await f.deliver(f.envelope({ message: undefined })));
    expect(bodyOf(raw).message).toBeUndefined();
    expect(fs.existsSync(path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId()}.json`))).toBe(false);
    expect(() => f.verifier().admitNativeControl(raw)).toThrow();
  });

  it("requires live source machine + actual allow gate + live owner Main before creating ANY receipt", async () => {
    const f = await fixture();
    expect(await f.deliver(f.envelope(), "wrong-machine")).toMatchObject({ kind: "refused" });
    expect(await f.deliver(f.envelope({ replyTo: otherNats.id }))).toMatchObject({ kind: "refused" });
    expect(await f.deliver(f.envelope({}, { from: { id: "session:unknown" } }))).toMatchObject({ kind: "refused" });
    f.rootRecord.live = false;
    expect(await f.deliver()).toMatchObject({ kind: "reply", envelope: { body: { notRun: true } } });
    expect(f.ledger.lookup("toFile", commandId())).toBeUndefined();
    expect(fs.existsSync(path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId()}.json`))).toBe(false);
  });

  it("refuses every altered retained semantic field, including fields omitted by the generic wire digest", async () => {
    const f = await fixture(), event = await f.persist(eventOf(await f.deliver()));
    const mutations: Array<(event: MeshEvent) => void> = [
      e => { bodyOf(e).message = "different"; }, e => { bodyOf(e).data = { changed: true }; },
      e => { bodyOf(e).commandId = "f".repeat(64); }, e => { bodyOf(e).targetId = "f".repeat(32); },
      e => { bodyOf(e).replyTo = otherNats.id; }, e => { bodyOf(e).deadlineAt = f.clock.now + 3; },
      e => { bodyOf(e).requestedAt = f.clock.now - 1; }, e => { bodyOf(e).requestCreatedAt = 1; },
      e => { bodyOf(e).ownerIncarnation = "another-owner"; }, e => { e.to = "another-host"; },
      e => { e.from = { ...e.from, id: otherNats.id, sessionId: otherNats.id.slice(8) }; },
      e => { e.from.kind = "actor"; }, e => { e.principal = { id: "herdr:principal-b", binding: "herdr-client" }; },
      e => { (bodyOf(e).bridge as Record<string, unknown>).id = "another-source-envelope"; },
      e => { (bodyOf(e).bridge as Record<string, unknown>).from = "stdio-peer"; },
      e => { bodyOf(e).binding = { model: "not-a-model-route" }; }, e => { bodyOf(e).wakeCause = { changed: true }; },
      e => { bodyOf(e).triggerTurn = true; }, e => { bodyOf(e).nativeContext = { sourceMachine: POD_MACHINE }; },
      e => { (e as unknown as Record<string, unknown>).nativeAdmission = {}; },
      e => { e.sender = { id: natsLead.id, kind: "main" } as never; },
      e => { e.sender = { authority: "scope", principalId: "another", digest: "f".repeat(64) }; },
      e => { delete e.sender; }, e => { e.text = "extra delivery semantics"; },
    ];
    for (const mutate of mutations) {
      const altered = clone(event); mutate(altered);
      altered.dedupeFingerprint = controlBodyFingerprint(altered.data, altered.principal);
      expect(() => f.verifier().admitNativeControl(altered)).toThrow();
    }
    const diagnostics = clone(event);
    diagnostics.id = "file-local-id"; diagnostics.sequence += 100; diagnostics.createdAt += 100; diagnostics.from.name = "display only";
    expect(f.verifier().admitNativeControl(diagnostics)).toBeDefined();
  });

  it("public MeshStore.publish bridge/context labels cannot mint or alter a genuine receipt", async () => {
    const f = await fixture(), original = await f.persist(eventOf(await f.deliver()));
    const publicStore = new MeshStore(path.join(f.root, "public-publisher"), 256 * 1024, 100); stores.push(publicStore);
    const altered = clone(original); bodyOf(altered).replyTo = otherNats.id;
    await publicStore.publish({ topic: altered.topic, kind: altered.kind, from: { ...altered.from, verified: "bridge" },
      ...(altered.to !== undefined ? { to: altered.to } : {}),
      ...(altered.principal !== undefined ? { principal: altered.principal } : {}), data: altered.data,
      ...(altered.dedupeKey !== undefined ? { dedupeKey: altered.dedupeKey } : {}),
      dedupeFingerprint: controlBodyFingerprint(altered.data, altered.principal) });
    expect(() => f.verifier().admitNativeControl(publicStore.read({ after: 0, limit: 100 })[0]!)).toThrow(/differs/);
    const missing = clone(original); bodyOf(missing).commandId = commandId("not-carried");
    await publicStore.publish({ topic: missing.topic, kind: missing.kind, from: { ...missing.from, verified: "bridge" },
      ...(missing.to !== undefined ? { to: missing.to } : {}),
      ...(missing.principal !== undefined ? { principal: missing.principal } : {}), data: missing.data });
    expect(() => f.verifier().admitNativeControl(publicStore.read({ after: 0, limit: 100 })[1]!)).toThrow();
    expect(readNativeIngressReceipt(f.dir, commandId()).replyTo).toBe(natsLead.id);
  });

  it("fails closed for reserved/quarantined ids, missing binding, wrong link/root/directory pins", async () => {
    const f = await fixture(), event = await f.persist(eventOf(await f.deliver()));
    const missing = createNativeActorIngress({ bindings: [], isNativeActor: id => id === ACTOR, now: () => f.clock.now });
    expect(missing.requiresNativeAdmission(ACTOR)).toBe(true);
    expect(missing.requiresNativeAdmission("ordinary")).toBe(false);
    expect(() => missing.admitNativeControl(event)).toThrow(/binding/);
    for (const binding of [{ ...f.binding, rootId: otherNats.id }, { ...f.binding, ingress: { ...f.binding.ingress, link: "another" } },
      { ...f.binding, ingress: { ...f.binding.ingress, ledgerDir: path.join(f.root, "absent") } }]) {
      expect(() => f.verifier([binding]).admitNativeControl(event)).toThrow();
    }
    const binding = clone(f.binding), verifier = f.verifier([binding]);
    (binding.ingress as { link: string }).link = "mutated-after-seal";
    expect(verifier.requiresNativeAdmission(ACTOR)).toBe(true);
    expect(verifier.admitNativeControl(event)).toBeDefined();
  });

  it.each(["missing", "corrupt", "truncated", "oversized", "symlink", "mode", "directory-mode", "directory-symlink"])(
    "denies %s persisted receipt evidence without reconstructing it from a public event", async defect => {
      const f = await fixture(), event = await f.persist(eventOf(await f.deliver()));
      const directory = path.join(f.dir, NATIVE_INGRESS_DIRECTORY), file = path.join(directory, `${commandId()}.json`);
      if (defect === "missing") fs.unlinkSync(file);
      if (defect === "corrupt") fs.writeFileSync(file, "{}", { mode: 0o600 });
      if (defect === "truncated") fs.writeFileSync(file, "{\"v\":1", { mode: 0o600 });
      if (defect === "oversized") fs.writeFileSync(file, " ".repeat(NATIVE_INGRESS_MAX_BYTES + 1), { mode: 0o600 });
      if (defect === "symlink") { const moved = path.join(f.root, "saved-receipt"); fs.renameSync(file, moved); fs.symlinkSync(moved, file); }
      if (defect === "mode") fs.chmodSync(file, 0o644);
      if (defect === "directory-mode") fs.chmodSync(directory, 0o755);
      if (defect === "directory-symlink") {
        const moved = path.join(f.root, "saved-projection"); fs.renameSync(directory, moved); fs.symlinkSync(moved, directory, "dir");
      }
      expect(() => f.verifier().admitNativeControl(event)).toThrow();
    });

  it("checks proof/deadline again at enqueue/callback, binds once and never accepts copied JSON handles", async () => {
    const f = await fixture(), event = await f.persist(eventOf(await f.deliver()));
    const saved = clone(event), ingress = f.verifier(), handle = ingress.admitNativeControl(event), request = f.request(event);
    expect(() => claimNativeAskAdmission({} as NativeControlAdmission, request)).toThrow();
    expect(() => claimNativeAskAdmission(clone(handle), request)).toThrow();
    expect(() => consumeNativeAskAdmission(handle, request, new AbortController().signal)).toThrow();
    expect(() => claimNativeAskAdmission(handle, { ...request, message: request.message.trim() })).toThrow();
    expect(() => claimNativeAskAdmission(handle, { ...request, actorId: "a".repeat(32) })).toThrow();
    claimNativeAskAdmission(handle, request);
    expect(() => claimNativeAskAdmission(handle, request)).toThrow();
    const changed = clone(request); changed.data = { different: true };
    expect(() => consumeNativeAskAdmission(handle, changed, new AbortController().signal)).toThrow();
    // Mutation of the input event cannot mutate the issued proof or original snapshot.
    bodyOf(event).message = "changed after admission"; bodyOf(event).data = { changed: true };
    expect(consumeNativeAskAdmission(handle, request, new AbortController().signal).commandId).toBe(commandId());
    expect(() => consumeNativeAskAdmission(handle, request, new AbortController().signal)).toThrow();
    const second = ingress.admitNativeControl(saved);
    claimNativeAskAdmission(second, request);
    const aborted = new AbortController(); aborted.abort();
    expect(() => consumeNativeAskAdmission(second, request, aborted.signal)).toThrow(/cancelled/);
    f.clock.now = (bodyOf(saved).deadlineAt as number) + 1;
    expect(() => consumeNativeAskAdmission(second, request, new AbortController().signal)).toThrow(/expired/);
  });

  it("requires its OWN genuine cancel receipt and same requester/machine/target command lineage", async () => {
    const f = await fixture();
    await f.persist(eventOf(await f.deliver()));
    const raw = eventOf(await f.deliver(f.envelope({ operation: "cancel", commandId: "source-cancel", cancelCommandId: "source-ask" },
      { id: "source-cancel-envelope" })));
    const cancel = await f.persist(raw), handle = f.verifier().admitNativeControl(cancel);
    expect(handle).toBeDefined();
    expect(() => claimNativeAskAdmission(handle, { actorId: ACTOR, message: "", data: null })).toThrow();
    const foreign = await f.deliver(f.envelope({ operation: "cancel", commandId: "foreign-cancel", cancelCommandId: "source-ask", replyTo: otherNats.id },
      { id: "foreign-envelope", from: otherNats }));
    expect(foreign).toMatchObject({ kind: "settled" });
    expect(fs.existsSync(path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId("foreign-cancel", otherNats.id)}.json`))).toBe(false);
    fs.unlinkSync(path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId("source-cancel")}.json`));
    expect(() => f.verifier().admitNativeControl(cancel)).toThrow();
    // A forged cancel event cannot borrow the original ask's proof.
    const forged = clone(cancel); bodyOf(forged).commandId = commandId();
    expect(() => f.verifier().admitNativeControl(forged)).toThrow();
  });

  it("retains FIRST exact toFile bytes across reopen; only fresh same-byte redelivery repairs missing projection", async () => {
    const f = await fixture(), envelope = f.envelope();
    const first = eventOf(await f.deliver(envelope)), event = await f.persist(first);
    const file = path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId()}.json`), bytes = fs.readFileSync(file, "utf8");
    const stored = f.ledger.lookup("toFile", commandId())!;
    expect(stored.resolvedBody).toEqual(bodyOf(first));
    expect(Object.isFrozen(stored.resolvedBody)).toBe(true);
    fs.unlinkSync(file);
    expect(() => f.verifier().admitNativeControl(event)).toThrow();
    await f.ledger.close();
    const reopened = await openBridgeNatsLedger({ dir: f.dir, clock: () => f.clock.now }); ledgers.push(reopened);
    const carrier = f.carrierFor(natsCarrierLedger(reopened, LINK));
    const sender = f.presence.natsEntries.get(natsLead.id)!;
    f.presence.natsEntries.set(natsLead.id, { ...sender, identity: { ...sender.identity, sessionId: "poisoned-on-redelivery" } });
    const resent = eventOf(await carrier.inbound({ envelope: { ...envelope, id: "different-source-resend-id" }, sourceMachine: POD_MACHINE, sequence: 2 }));
    expect(resent.id).toBe(first.id); // source stamp stays bound to the first claim, not the latest packet
    expect(resent.data).toEqual(first.data);
    expect(fs.readFileSync(file, "utf8")).toBe(bytes);
    expect(f.verifier().admitNativeControl(event)).toBeDefined();
    // Generic wire digest ignores these fields; complete retained evidence MUST NOT.
    for (const changed of [{ deadlineAt: envelope.body.deadlineAt + 1 }, { requestedAt: envelope.body.requestedAt - 1 },
      { requestCreatedAt: envelope.body.requestCreatedAt - 1 }, { ownerIncarnation: "altered" }]) {
      const plan = await carrier.inbound({ envelope: { ...envelope, body: { ...envelope.body, ...changed } }, sourceMachine: POD_MACHINE, sequence: 3 });
      expect(plan).toMatchObject({ kind: "reply", envelope: { body: { notRun: true } } });
      expect(fs.readFileSync(file, "utf8")).toBe(bytes);
    }
  });

  it("never overwrites an existing-different receipt; failures expose neither event nor source ACK", async () => {
    const f = await fixture(), fake = new FakePort();
    for (const [id, entry] of f.presence.natsEntries) fake.routes.set(id, entry);
    const side = new NatsCarrierSide({ port: fake, file: f.file, link: LINK, ledger: f.port,
      authorize: f.authorize, authorizeTopic: f.authorizeTopic, now: () => f.clock.now }); sides.push(side);
    await side.latestSequence();
    const delivery = fake.deliver(f.envelope(), POD_MACHINE, 1); await settle();
    const link = vi.spyOn(fs, "linkSync").mockImplementation(() => { throw new Error("projection link failure"); });
    await expect(side.read(0)).rejects.toThrow(/projection link failure/);
    expect(delivery.acked).toBe(0);
    expect(f.store.read({ after: 0, limit: 100 })).toHaveLength(0);
    expect(f.ledger.lookup("toFile", commandId())?.resolvedBody).toBeDefined(); // claim was durable first
    link.mockRestore();
    const read = await side.read(0), event = await f.persist(read.events[0]!);
    expect(delivery.acked).toBe(0); // not even after projection; only the subsequent durable-append cursor ACKs
    await side.read(read.through); expect(delivery.acked).toBe(1);
    expect(f.verifier().admitNativeControl(event)).toBeDefined();
    const file = path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId()}.json`);
    // Negative disk-corruption fixture, not a hand-minted positive admission/context. Even a
    // structurally valid differing receipt cannot replace the authentic retained claim's proof.
    const conflict = encodeNativeIngressReceipt({ ...readNativeIngressReceipt(f.dir, commandId()), eventDigest: "e".repeat(64) });
    fs.writeFileSync(file, conflict, { mode: 0o600 });
    await expect(f.deliver()).rejects.toThrow(/conflicts/);
    expect(fs.readFileSync(file, "utf8")).toBe(conflict);
  });

  it("a failed parent namespace barrier withholds the event, then an exact fresh redelivery re-confirms durability", async () => {
    const f = await fixture();
    const directory = path.join(f.dir, NATIVE_INGRESS_DIRECTORY); fs.mkdirSync(directory, { mode: 0o700 });
    const inode = fs.statSync(directory), fsync = fs.fsyncSync.bind(fs);
    const spy = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd);
      if (stat.dev === inode.dev && stat.ino === inode.ino) throw new Error("projection parent barrier failure");
      fsync(fd);
    });
    await expect(f.deliver()).rejects.toThrow(/parent barrier failure/);
    expect(f.ledger.lookup("toFile", commandId())?.resolvedBody).toBeDefined();
    spy.mockRestore();
    expect((await f.deliver()).kind).toBe("event");
  });

  it("legacy claims without historical retained bytes remain fail-closed; receiver never reconstructs", async () => {
    const f = await fixture(), first = eventOf(await f.deliver()), stored = f.ledger.lookup("toFile", commandId())!;
    const dir = path.join(f.root, "legacy-ledger"), legacy = await openBridgeNatsLedger({ dir, clock: () => f.clock.now }); ledgers.push(legacy);
    const { resolvedBody: _missing, ...oldRecord } = stored;
    await legacy.claim(oldRecord); // negative fixture: exact prior record, intentionally without upgrade evidence
    const carrier = f.carrierFor(natsCarrierLedger(legacy, LINK));
    const redelivery = eventOf(await carrier.inbound({ envelope: f.envelope(), sourceMachine: POD_MACHINE, sequence: 2 }));
    expect(redelivery.data).toEqual(first.data);
    expect(legacy.lookup("toFile", commandId())!.resolvedBody).toBeUndefined();
    const event = await f.persist(redelivery);
    expect(() => f.verifier([{ ...f.binding, ingress: { link: LINK, ledgerDir: dir } }]).admitNativeControl(event)).toThrow();
  });

  it("prunes only through existing ledger maintenance after BOTH claim/receipt horizon, never at deadline", async () => {
    const f = await fixture();
    const event = await f.persist(eventOf(await f.deliver())), receipt = readNativeIngressReceipt(f.dir, commandId());
    const file = path.join(f.dir, NATIVE_INGRESS_DIRECTORY, `${commandId()}.json`);
    f.clock.now = receipt.deadlineAt + 1;
    expect(() => f.verifier().admitNativeControl(event)).toThrow(/expired/);
    await f.ledger.close();
    let reopened = await openBridgeNatsLedger({ dir: f.dir, clock: () => f.clock.now }); ledgers.push(reopened);
    expect(fs.existsSync(file)).toBe(true);
    await reopened.close(); f.clock.now = receipt.expiresAt + 1;
    reopened = await openBridgeNatsLedger({ dir: f.dir, clock: () => f.clock.now }); ledgers.push(reopened);
    expect(fs.existsSync(file)).toBe(false);
  });
});
