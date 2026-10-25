// L1 (smarty-dev#15310, PR 2 N4): v2 work topics into the hub file mesh. Only an allow-listed live v2
// root's work topic is republished, authored as the link with the verified sender in its stamp, once per
// NATS message id across redeliveries and a crash, within the rate bound, and nothing is ever published
// into FABRIC_EV; a credential without the consume grant refuses the start and says so for `status`.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { JetStreamApiError } from "@nats-io/jetstream";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  NATS_REPUBLISH_RATE,
  natsRepublishStatePath,
  runNatsRepublish,
  startNatsRepublishLink,
  type NatsRepublishRate,
  type NatsRepublishViews,
} from "../src/mesh/bridge-nats-republish.js";
import { NATS_LINK_COUNTER_NAMES } from "../src/mesh/bridge-nats-counters.js";
import type { NatsBridgeEventMessage, NatsBridgeEventSource } from "../src/mesh/bridge-nats-events.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
import type { BridgeNatsPortResolved, BridgeNatsPresenceRecord } from "../src/mesh/bridge-nats-types.js";
import type { MeshEvent, MeshPublishInput } from "../src/mesh/event-log.js";
import type { NatsConnectionHandle } from "../src/transport/nats/connection.js";

const ORG = "example-org";
const BRIDGE = "machine-a";
const V2 = "machine-b";
const LINK = "v2";
const LEAD = "session:22222222-2222-4222-8222-222222222222";
const OTHER = "session:33333333-3333-4333-8333-333333333333";
const temps: string[] = [];
afterEach(() => { for (const dir of temps.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

interface FakeMessage extends NatsBridgeEventMessage { calls: string[] }

class ScriptedEvents implements NatsBridgeEventSource {
  constructor(readonly messages: FakeMessage[]) {}
  isVerified(): boolean { return true; }
  async *consume(signal: AbortSignal): AsyncIterable<NatsBridgeEventMessage> {
    for (const message of this.messages) { if (signal.aborted) return; yield message; }
  }
}

function eventMessage(topic: string, body: Record<string, unknown> = {}, options: { msgId?: string; sequence?: number } = {}): FakeMessage {
  const event = { id: options.msgId ?? "evt-1", topic, kind: "eta", from: { id: LEAD, name: "claimed", kind: "main" }, to: "factory",
    text: "ETA 2h", data: { issue: 15310 }, principal: { id: "someone", binding: "org-agent" }, createdAt: 5, ...body };
  const calls: string[] = [];
  return {
    calls, subject: `fabric.ev.${ORG}.${topic}`, data: Buffer.from(JSON.stringify(event)),
    headers: { get: (name: string) => name === "Nats-Msg-Id" ? options.msgId ?? "evt-1" : "" },
    info: { streamSequence: options.sequence ?? 10, deliveryCount: 1 },
    async ackAck() { calls.push("ack"); return true; },
    nak(delay?: number) { calls.push(`nak:${delay ?? ""}`); },
    term(reason?: string) { calls.push(`term:${reason ?? ""}`); },
  };
}

const resolved = (id: string, machine: string): BridgeNatsPortResolved => ({
  route: { machine, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "inc", capabilities: ["followUp"], live: true },
  identity: { id, name: "v2-lead", kind: "main", sessionId: id.slice(8) },
});

/** A file store with the keyed-append contract: one event per dedupeKey, a replay returns the first. */
class FakeStore {
  readonly appended: MeshEvent[] = [];
  readonly inputs: MeshPublishInput[] = [];
  fail: Error | undefined;
  async publish(input: MeshPublishInput): Promise<MeshEvent> {
    this.inputs.push(input);
    if (this.fail) throw this.fail;
    const prior = this.appended.find(event => event.dedupeKey === input.dedupeKey);
    if (prior) return prior;
    const event = { id: `file-${this.appended.length + 1}`, sequence: this.appended.length + 1, createdAt: 1, topic: input.topic,
      kind: input.kind ?? "message", from: input.from, ...(input.to ? { to: input.to } : {}), ...(input.text ? { text: input.text } : {}),
      data: input.data, ...(input.dedupeKey ? { dedupeKey: input.dedupeKey } : {}) } as MeshEvent;
    this.appended.push(event);
    return event;
  }
}

const ledger = () => {
  const seen = new Map<string, unknown>();
  return { seen: (_direction: "toFile", id: string) => seen.get(id), map: seen,
    markSeen: async (_direction: "toFile", id: string, receipt?: unknown) => { seen.set(id, receipt ?? true); } };
};

// L3's real gate: the only sender and topic decision. LEAD may republish fleet.work.*; OTHER may not.
const CLOCK = 1_000_000;
const gate = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({ nats: [
  { kind: "root", id: LEAD, machine: V2, republishTopics: ["fleet.work.*"] },
  { kind: "root", id: OTHER, machine: V2 },
] }, { link: LINK, natsMachine: BRIDGE }), { pins: { seats: new Map(), bound: new Map() }, clock: () => CLOCK });
const natsRecord = (id: string, machine: string): BridgeNatsPresenceRecord => ({ id, name: "v2-lead", kind: "main", rootId: id,
  sessionId: id.slice(8), ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "inc", machine, live: true, observedAt: CLOCK });
/** Presence as the port and the hub would read it now: the NATS root on `machineOf(id)`, a file native optionally. */
const viewsFor = (machineOf: (id: string) => string | undefined, fileNative: string[] = []): NatsRepublishViews => ({
  nats: () => ({ side: "nats", records: (id: string) => { const machine = machineOf(id); return machine ? [natsRecord(id, machine)] : []; } }),
  file: () => ({ side: "file", records: (id: string) => fileNative.includes(id) ? [{ ...natsRecord(id, "x"), machine: undefined }] as never : [] }),
});
const onV2 = (id: string): string | undefined => id === LEAD || id === OTHER ? V2 : undefined;

async function run(messages: FakeMessage[], options: { store?: FakeStore; ledger?: ReturnType<typeof ledger>;
  rate?: NatsRepublishRate; topics?: string[]; machineOf?: (id: string) => string | undefined; fileNative?: string[] } = {}) {
  const store = options.store ?? new FakeStore();
  const counted: string[] = [];
  const machineOf = options.machineOf ?? onV2;
  const leg = runNatsRepublish({
    gate, views: viewsFor(machineOf, options.fileNative),
    counters: { add: name => {
      // L5's contract: an unknown counter name is a programming error.
      if (!(NATS_LINK_COUNTER_NAMES as readonly string[]).includes(name)) throw new Error(`unknown counter ${name}`);
      counted.push(name);
    } },
    link: LINK, org: ORG, machine: BRIDGE, store, source: new ScriptedEvents(messages),
    resolve: id => { const machine = machineOf(id); return machine ? resolved(id, machine) : undefined; },
    ...(options.ledger ? { ledger: options.ledger } : {}),
    ...(options.rate ? { rate: options.rate } : {}), ...(options.topics ? { topics: options.topics } : {}), now: () => 1_000,
  }, new AbortController().signal);
  await leg.done;
  return { store, counters: leg.counters(), counted };
}

describe("N4 republish: v2 work topics into the hub file mesh", () => {
  it("republishes an allow-listed live root's work topic once, as the link, with the verified sender stamped", async () => {
    const message = eventMessage("fleet.work.pi-fabric.15310", {}, { msgId: "evt-1" });
    const book = ledger();
    const { store, counters } = await run([message], { ledger: book });
    expect(store.inputs).toEqual([{
      topic: "fleet.work.pi-fabric.15310", kind: "eta", from: { id: "bridge:v2", name: "mesh-bridge", kind: "main" }, to: "factory",
      text: "ETA 2h", data: { issue: 15310, bridge: { from: LINK, id: "evt-1",
        sender: { id: LEAD, name: "v2-lead", kind: "main", sessionId: LEAD.slice(8), machine: V2 } } },
      dedupeKey: "bridge-nats-republish:v2:evt-1", durable: true,
    }]);
    expect(store.inputs[0]).not.toHaveProperty("principal");
    expect(message.calls).toEqual(["ack"]);
    expect(book.map.get("republish:v2:evt-1")).toEqual({ destinationId: "file-1", destinationSequence: 1 });
    expect(counters).toMatchObject({ republished: 1, duplicates: 0, rateDeferred: 0, refused: {} });
  });

  it("counts with L5's names: accepted after the append, duplicate on a known id, refused on a gate refusal", async () => {
    const book = ledger();
    const ok = eventMessage("fleet.work.a.1", {}, { msgId: "c-1" });
    const again = eventMessage("fleet.work.a.1", {}, { msgId: "c-1" });
    const no = eventMessage("fleet.work.a.2", { from: { id: OTHER, name: "x", kind: "main" } }, { msgId: "c-2" });
    const { counted } = await run([ok, again, no], { ledger: book });
    expect(counted).toEqual(["republish.accepted", "republish.duplicate", "republish.refused"]);
  });

  it("appends once across a redelivery (ledger) and across a crash before the ledger write (keyed append)", async () => {
    const book = ledger();
    const store = new FakeStore();
    const first = eventMessage("fleet.work.tasks", {}, { msgId: "evt-7" });
    const again = eventMessage("fleet.work.tasks", {}, { msgId: "evt-7" });
    const { counters } = await run([first, again], { ledger: book, store });
    expect(store.appended).toHaveLength(1);
    expect(store.inputs).toHaveLength(1);
    expect(counters).toMatchObject({ republished: 1, duplicates: 1 });
    expect(again.calls).toEqual(["ack"]);
    // The ledger lost the record (crash after the append): the keyed append appends nothing new.
    book.map.clear();
    const replay = eventMessage("fleet.work.tasks", {}, { msgId: "evt-7" });
    await run([replay], { ledger: book, store });
    expect(store.inputs).toHaveLength(2);
    expect(store.appended).toHaveLength(1);
    expect(replay.calls).toEqual(["ack"]);
  });

  it("refuses a topic the sender may not republish, a sender off its pinned machine, and a non-work topic", async () => {
    const unlisted = eventMessage("fleet.work.a.1", { from: { id: OTHER, name: "x", kind: "main" } }, { msgId: "o-1" });
    const offMachine = eventMessage("fleet.work.a.2", {}, { msgId: "m-1" });
    const owner = eventMessage("ops.owner", { kind: "pr.wake" }, { msgId: "w-1" });
    const first = await run([unlisted], {});
    const second = await run([offMachine], { machineOf: id => id === LEAD ? "machine-c" : undefined });
    const third = await run([owner], { topics: ["ops.owner"] });
    const both = eventMessage("fleet.work.a.3", {}, { msgId: "n-1" });
    const fourth = await run([both], { fileNative: [LEAD] });
    for (const [message, result] of [[unlisted, first], [offMachine, second], [owner, third], [both, fourth]] as const) {
      expect(result.store.inputs).toEqual([]);
      expect(message.calls).toEqual([expect.stringMatching(/^term:/)]);
    }
    // L3's gate codes: not in republishTopics; not live on its pinned machine; native on both sides.
    expect(first.counters.refused).toEqual({ BRIDGE_NATS_TOPIC_NOT_ALLOWED: 1 });
    expect(second.counters.refused).toEqual({ BRIDGE_NATS_SENDER_UNVERIFIED: 1 });
    expect(third.counters.refused).toEqual({ BRIDGE_NATS_NOT_A_WORK_TOPIC: 1 });
    expect(fourth.counters.refused).toEqual({ BRIDGE_NATS_AMBIGUOUS: 1 });
  });

  it("retries (never refuses) while a side's presence is unknown", async () => {
    const message = eventMessage("fleet.work.a.1", {}, { msgId: "u-1" });
    const store = new FakeStore();
    const leg = runNatsRepublish({ link: LINK, org: ORG, machine: BRIDGE, store, source: new ScriptedEvents([message]), gate,
      resolve: id => resolved(id, V2), views: { ...viewsFor(onV2), nats: () => ({ side: "nats", records: () => {
        throw Object.assign(new Error("not ready"), { code: "FABRIC_DIRECTORY_NOT_READY" });
      } }) }, now: () => 1_000 }, new AbortController().signal);
    await leg.done;
    expect(store.inputs).toEqual([]);
    expect(message.calls).toEqual([expect.stringMatching(/^nak:/)]);
  });

  it("never republishes what already crossed a bridge, or what claims one of this link's mirrors", async () => {
    const bridged = eventMessage("fleet.work.a.1", { data: { bridge: { from: "spoke", id: "x" } } }, { msgId: "b-1" });
    const mirror = eventMessage("fleet.work.a.2", {}, { msgId: "b-2" });
    const one = await run([bridged]);
    const two = await run([mirror], { machineOf: id => id === LEAD ? BRIDGE : undefined });
    expect(one.store.inputs).toEqual([]);
    expect(two.store.inputs).toEqual([]);
    expect(one.counters.refused).toEqual({ BRIDGE_NATS_BRIDGED_AGAIN: 1 });
    expect(two.counters.refused).toEqual({ BRIDGE_NATS_SENDER_IS_BRIDGE: 1 });
  });

  it("defers (NAK) over the rate bound and on a busy file mesh, never drops", async () => {
    const rate = { ...NATS_REPUBLISH_RATE, burst: 1 };
    const a = eventMessage("fleet.work.a.1", {}, { msgId: "r-1" }), b = eventMessage("fleet.work.a.2", {}, { msgId: "r-2" });
    const { counters, store } = await run([a, b], { rate });
    expect(store.inputs).toHaveLength(1);
    expect(a.calls).toEqual(["ack"]);
    expect(b.calls).toEqual([expect.stringMatching(/^nak:/)]);
    expect(counters.rateDeferred).toBe(1);
    const busy = new FakeStore();
    busy.fail = Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    const c = eventMessage("fleet.work.a.3", {}, { msgId: "r-3" });
    await run([c], { store: busy });
    expect(c.calls).toEqual([expect.stringMatching(/^nak:/)]);
  });

});

describe("N4 start: the consume grant, and nothing published into FABRIC_EV", () => {
  const permission = (): Error => Object.assign(new Error("Permissions Violation for Publish to \"$JS.API.CONSUMER.INFO...\""),
    { name: "PermissionViolationError" });
  function fakeConnection(options: { refuse?: boolean } = {}) {
    const published: string[] = [];
    const queue: unknown[] = [];
    let wake: (() => void) | undefined;
    let ended = false;
    const consumers = new Map<string, Record<string, unknown>>();
    const connection = {
      isVerified: () => true,
      async close() { ended = true; wake?.(); },
      nc: { publish: (subject: string) => { published.push(subject); } },
      manager: { consumers: {
        async info(_stream: string, durable: string) {
          if (options.refuse) throw permission();
          const config = consumers.get(durable);
          if (!config) throw new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
          return { config };
        },
        async add(_stream: string, config: Record<string, unknown>) { consumers.set(String(config.durable_name), config); return { config }; },
      } },
      js: {
        publish: async (subject: string) => { published.push(subject); throw new Error("must never publish"); },
        consumers: { async get() { return { async consume() {
          return {
            async close() { ended = true; wake?.(); }, closed: async () => undefined,
            async *[Symbol.asyncIterator]() {
              for (;;) {
                const next = queue.shift();
                if (next) { yield next; continue; }
                if (ended) return;
                await new Promise<void>(resolve => { wake = resolve; });
              }
            },
          };
        } }; } },
      },
    };
    return { connection: connection as unknown as NatsConnectionHandle, published, consumers,
      push(message: unknown) { queue.push(message); wake?.(); } };
  }
  const config = (stateDir: string, events = true) => ({
    link: LINK, stateDir, nats: { servers: ["tls://localhost:4222"], machine: BRIDGE, credentials: "/x/user.creds", caFile: "/x/ca.pem",
      custodyStream: "C", livenessStream: "L", pathBMachines: [BRIDGE, V2], org: ORG, events },
  });
  const stateDir = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-republish-")); temps.push(dir); return dir; };

  it("is off unless nats.events is set", async () => {
    const fake = fakeConnection();
    await expect(startNatsRepublishLink(config(stateDir(), false), { store: new FakeStore(), port: { machine: BRIDGE, resolve: () => undefined },
      gate, views: viewsFor(onV2), signal: new AbortController().signal, log: () => undefined, connect: async () => fake.connection }))
      .resolves.toBeUndefined();
  });

  it("consumes its own durable and republishes into the file mesh only; FABRIC_EV is never published to", async () => {
    const fake = fakeConnection();
    const store = new FakeStore();
    const dir = stateDir();
    const handle = await startNatsRepublishLink(config(dir), { store, port: { machine: BRIDGE, resolve: id => id === LEAD ? resolved(LEAD, V2) : undefined },
      gate, views: viewsFor(onV2), signal: new AbortController().signal, log: () => undefined, connect: async () => fake.connection });
    expect([...fake.consumers.keys()]).toEqual(["bev_machine-a"]);
    const message = eventMessage("fleet.work.pi-fabric.15310", {}, { msgId: "s-1" });
    fake.push(message);
    await vi.waitFor(() => expect(store.appended).toHaveLength(1));
    await vi.waitFor(() => expect(message.calls).toEqual(["ack"]));
    await handle!.stop();
    expect(fake.published).toEqual([]);
    expect(JSON.parse(fs.readFileSync(natsRepublishStatePath(dir), "utf8"))).toMatchObject({ v: 1, link: LINK, state: "stopped",
      durable: "bev_machine-a" });
    // POSIX only: Windows has no owner-only file modes (nats-link runs only on Linux and macOS).
    if (process.platform !== "win32") expect(fs.statSync(natsRepublishStatePath(dir)).mode & 0o777).toBe(0o600);
  });

  it("refuses the start without the consume grant, and records why for nats-link status", async () => {
    const fake = fakeConnection({ refuse: true });
    const dir = stateDir();
    const opened: Record<string, unknown>[] = [];
    const start = startNatsRepublishLink(config(dir), { store: new FakeStore(), port: { machine: BRIDGE, resolve: () => undefined },
      gate, views: viewsFor(onV2), signal: new AbortController().signal, log: () => undefined,
      connect: async input => { opened.push({ ...input }); return fake.connection; } });
    await expect(start).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING" });
    // The message names the code, the stream and the durable (never a USER.INFO machine tag mismatch).
    await expect(start).rejects.toThrow("FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING: nats.events is on, but the bridge credential lacks " +
      "the FABRIC_EV_EXAMPLE_ORG consume grant (durable bev_machine-a: ");
    // Verified with the standard agent policy: no `events` (which would demand fabric.ev publish) and no `org`.
    expect(opened).toHaveLength(1);
    expect(opened[0]).not.toHaveProperty("events");
    expect(opened[0]).not.toHaveProperty("org");
    expect(JSON.parse(fs.readFileSync(natsRepublishStatePath(dir), "utf8"))).toMatchObject({ state: "refused",
      code: "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING", durable: "bev_machine-a" });
    expect(fake.published).toEqual([]);
  });

  it("needs the org to name the stream", async () => {
    const { org: _org, ...nats } = config(stateDir()).nats;
    await expect(startNatsRepublishLink({ link: LINK, stateDir: stateDir(), nats }, { store: new FakeStore(),
      port: { machine: BRIDGE, resolve: () => undefined }, gate, views: viewsFor(onV2), signal: new AbortController().signal, log: () => undefined }))
      .rejects.toThrow(/nats.org/);
  });
});
