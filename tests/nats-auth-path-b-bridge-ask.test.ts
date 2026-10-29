// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' T3/T5 on a real broker: a moved v2 lead asks a Ryzen 1 actor (review-astra
// stand-in) owned by the file Main, gets its answer back through the link, and cancels a second ask mid-run:
// the cancel's cancelCommandId is mapped like the command it cancels and stops exactly that run. The actor's
// worker is Fabric's fixture worker (no model call); the actor manager, control planes and broker are real.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import { readNativeIngressReceipt, NATIVE_INGRESS_DIRECTORY } from "../src/mesh/bridge-nats-ingress.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import type { FabricActorMessage } from "../src/actors/types.js";
import type { MeshEvent } from "../src/mesh/store.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, E2E_WAIT_MS, linkTail, NATS_MAIN_MACHINE, natsLinkPresent, waitForValue, type MixedHarness, type WireEvent,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
if (!linked) console.warn("A' bridge ask E2E skipped: src/mesh/bridge-nats-link.ts (lane L2) is not in this checkout");

const CONTROL = "fabric.control.command";
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
const dataOf = (event: MeshEvent) => (event.data ?? {}) as Record<string, unknown>;

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link: v2 Main asks a Ryzen 1 actor on a real broker", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = await createMixedHarness({ binary: binary!, fileActor: "review-astra-fixture" }); }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  const actorMessages = (): FabricActorMessage[] => h.fileMain.runtime.actors.messages(h.actor!.id);

  it("the actor is mirrored for v2 with its ask capability, owned by the file root on the bridge machine", async () => {
    const row = await waitForValue(() => h.natsMain.runtime.participantInfos({ fresh: true }).find((entry) => entry.id === h.actor!.id && !entry.stale),
      "actor routable from v2", E2E_WAIT_MS);
    expect(row).toMatchObject({ kind: "actor", rootId: h.fileMain.id, ownerHostId: h.fileMain.id, machine: BRIDGE_MACHINE });
    expect(row.capabilities).toContain("ask");
  }, E2E_WAIT_MS + 30_000);

  it("ask + answer: same-command retimed attempts replay the first actual result, never another activation or receipt deadline", async () => {
    const token = `BRIDGE_ASK_${randomUUID()}`;
    const mark = h.wire.mark();
    const before = actorMessages().length;
    const result = await h.natsMain.invoke("agents.ask", { id: h.actor!.id, message: token })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); }) as FabricActorMessage;
    expect(result).toMatchObject({ actorId: h.actor!.id, direction: "out", text: "fake worker complete" });
    const added = actorMessages().slice(before);
    const inbound = added.filter((message) => message.direction === "in");
    expect(inbound).toHaveLength(1);
    // An actor records an inbound ask as its payload (actors/manager.ts: data, no text), the same as a native ask.
    expect(inbound[0]!.data).toMatchObject({ message: token });
    expect(added.filter((message) => message.direction === "out")).toEqual([result]);
    // Wire: v2 sent one ask to the actor's owner host on the bridge machine; the link appended one command
    // from v2's verified identity with the mapped commandId; the answer came back as v2's ACK result.
    const sent = h.wire.since(mark).filter((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && event.envelope.operation === "ask");
    expect(sent).toHaveLength(1);
    expect(sent[0]!.envelope).toMatchObject({ targetId: h.actor!.id, to: { machine: BRIDGE_MACHINE, hostId: h.fileMain.id } });
    const appended = h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter((event) => dataOf(event).operation === "ask" &&
      String(dataOf(event).message).includes(token));
    expect(appended).toHaveLength(1);
    expect(appended[0]).toMatchObject({ from: { id: h.natsMain.id }, data: { targetId: h.actor!.id,
      commandId: bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(bodyOf(sent[0]!).commandId)) } });
    const ack = h.wire.since(mark).find((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "ack" && bodyOf(event).commandId === bodyOf(sent[0]!).commandId);
    expect(ack).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { from: { id: h.fileMain.id }, body: { accepted: true, result } } });
    // Public agents.ask currently chooses its own key. Re-submit its actual keyed commandId,
    // not a second agents.ask invocation (which would intentionally generate a DIFFERENT key).
    const original = sent[0]!.envelope, originalBody = bodyOf(sent[0]!);
    const mapped = bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(originalBody.commandId));
    const dir = path.join(h.linkConfig.stateDir, "ledger"), file = path.join(dir, NATIVE_INGRESS_DIRECTORY, `${mapped}.json`);
    const receipt = readNativeIngressReceipt(dir, mapped), receiptBytes = fs.readFileSync(file);
    expect(receipt).toMatchObject({ sourceEventId: original.id, deadlineAt: originalBody.deadlineAt });
    const raw = await h.openPeer(NATS_MAIN_MACHINE, `ask-replay-${randomUUID()}`);
    const ackFor = (id: string) => h.wire.since(mark).find(event => event.phase === "deliver" &&
      event.hostId === h.natsMain.id && event.envelope.kind === "ack" && event.envelope.id === `ack:${id}`);
    const response = async (envelope: TransportEnvelope): Promise<Record<string, unknown>> => {
      await raw.send(envelope);
      await h.wire.until(() => !!ackFor(envelope.id), `ask replay response ${envelope.id}`, E2E_WAIT_MS);
      const delivered = ackFor(envelope.id)!;
      // Replay/refusal is emitted by the bridge for the mirrored target; the first owner ACK above is root-authored.
      expect(delivered).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { from: { id: h.actor!.id } } });
      return bodyOf(delivered);
    };
    const retry: TransportEnvelope = { ...original, id: randomUUID(), body: { ...originalBody,
      requestCreatedAt: Number(originalBody.requestCreatedAt) + 1, requestedAt: Number(originalBody.requestedAt) + 1,
      deadlineAt: Number(originalBody.deadlineAt) + 1 } };
    expect(controlBodyFingerprint(retry.body, retry.principal)).toBe(controlBodyFingerprint(original.body, original.principal));
    const replay = await response(retry);
    expect(replay.accepted).toBe(true);
    expect(replay.result).toEqual(result); // Actual broker-delivered cached reply, including the FIRST message id.
    expect(replay.messageId).toBe(result.id);
    expect(readNativeIngressReceipt(dir, mapped)).toEqual(receipt);
    expect(fs.readFileSync(file)).toEqual(receiptBytes);
    // Even an authorized live sender must not get its cached success for changed input/principal/epoch.
    for (const [envelope, code] of [
      [{ ...retry, id: randomUUID(), body: { ...originalBody, message: `${token}_CHANGED` } }, "FABRIC_CONTROL_ID_CONFLICT"],
      [{ ...retry, id: randomUUID(), principal: { id: "test:other-principal", binding: "herdr-client" } }, "FABRIC_CONTROL_ID_CONFLICT"],
      [{ ...retry, id: randomUUID(), body: { ...originalBody, ownerIncarnation: "different-owner-epoch" } }, "BRIDGE_NATS_CLAIM_MISMATCH"],
    ] as const) {
      const refused = await response(envelope);
      expect(refused).toMatchObject({ accepted: false, notRun: true, errorCode: code });
      expect(refused).not.toHaveProperty("result");
      expect(fs.readFileSync(file)).toEqual(receiptBytes);
    }
    const afterReplay = actorMessages().slice(before);
    expect(afterReplay.filter(message => message.direction === "in")).toHaveLength(1);
    expect(afterReplay.filter(message => message.direction === "out")).toEqual([result]);
    expect(h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter(event => dataOf(event).operation === "ask" &&
      String(dataOf(event).message).includes(token))).toEqual(appended);
  }, E2E_WAIT_MS * 2);

  it("cancel: aborting a v2 ask mid-run sends a mapped cancel that stops exactly that file-side run", async () => {
    const token = `HANG BRIDGE_ASK_CANCEL_${randomUUID()}`;
    const mark = h.wire.mark();
    const before = actorMessages().length;
    const abort = new AbortController();
    const asking = h.natsMain.invoke("agents.ask", { id: h.actor!.id, message: token }, abort.signal).then(() => "answered", () => "aborted");
    // THIS ask is admitted on the file side (its own inbound record) and its run is live: never an earlier run's state.
    const run = await waitForValue(() => {
      const admitted = actorMessages().slice(before).some((message) => message.direction === "in" &&
        (message.data as { message?: string } | undefined)?.message === token);
      return admitted ? h.fileMain.runtime.actors.status(h.actor!.id).inFlightRun : undefined;
    }, () => `the asked run is live on the file side\n${linkTail(h)}`, E2E_WAIT_MS);
    // The ask's own PubAck receipt is on record before the abort, so the abort meets a published command.
    const isAsk = (event: WireEvent) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && event.envelope.operation === "ask";
    await h.wire.until(() => h.wire.since(mark).some(isAsk), "v2's ask PubAck recorded", E2E_WAIT_MS);
    abort.abort();
    expect(await asking).toBe("aborted");
    // The cancel v2 published names the ask's NATS commandId; the link maps it exactly like the command.
    // Bounded by load, not by a fixed budget: the cancel leaves v2's background queue after its abort.
    const ask = h.wire.since(mark).find(isAsk)!;
    await h.wire.until(() => h.wire.since(mark).some((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.operation === "cancel"), "v2 published the cancel", E2E_WAIT_MS);
    const cancel = h.wire.since(mark).find((event) => event.phase === "send" && event.hostId === h.natsMain.id && event.envelope.operation === "cancel")!;
    expect(bodyOf(cancel)).toMatchObject({ cancelCommandId: bodyOf(ask).commandId, targetId: h.actor!.id });
    const crossed = await waitForValue(() => h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).find((event) => dataOf(event).operation === "cancel" &&
      dataOf(event).cancelCommandId === bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(bodyOf(ask).commandId))),
      () => `mapped cancel appended to the file mesh\n${linkTail(h)}`, E2E_WAIT_MS);
    expect(crossed).toMatchObject({ from: { id: h.natsMain.id } });
    // That run ends without an answer; the actor is idle again and nothing else ran.
    await waitForValue(() => h.fileMain.runtime.actors.status(h.actor!.id).inFlightRun ? undefined : true, "cancelled run ended", E2E_WAIT_MS);
    const added = actorMessages().slice(before);
    expect(added.filter((message) => message.direction === "in")).toHaveLength(1);
    expect(added.filter((message) => message.direction === "out" && message.runId === run.id && !message.error && message.text === "fake worker complete")).toEqual([]);
    // The actor still answers the next ask: the cancel stopped one run, not the actor or the link.
    await expect(h.natsMain.invoke("agents.ask", { id: h.actor!.id, message: `BRIDGE_ASK_AFTER_CANCEL_${randomUUID()}` }))
      .resolves.toMatchObject({ direction: "out", text: "fake worker complete" });
  }, E2E_WAIT_MS * 6);
});
