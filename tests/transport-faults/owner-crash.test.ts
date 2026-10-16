// Keep reason: data safety; real process death between control custody/outcome/ACK boundaries.
// Scope: FILE legacy FabricControlPlane + real MeshStore + real Main native journal, B2 ONLY.
// expected-skip: win32: requires POSIX SIGKILL and the real file namespace durability barrier.
// Real authenticated NATS claim/custody/ACK-PubAck OS death lives in nats-owner-crash.test.ts.
// No A1 transportKind prerequisite: the legacy FILE regression floor MUST run at the port-only base.
// B2c is a FILE ACK append + explicit MeshStore.sync, NOT JetStream PubAck/double-ack proof.
// No B3 Pi/model consumption, power-cut, remote-authentication, HA or NATS/I-LOCK claim.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../../src/mesh/store.js";
import { FabricControlPlane, type FabricControlAcceptance, type FabricControlCommand, type FabricControlResult } from "../../src/topology/control-plane.js";
import { OwnedOwner } from "./control-fixtures/owned-owner.js";
import { OWNER_HOST_ID, SENDER_HOST_ID, TARGET_ID, bounded, fixturePaths, seenKey, type CrashBoundary } from "./control-fixtures/paths.js";

/** CONTRACT-v2 §3.1 T-B2a/b/c; E1 separates persistence from wake/receipt failures; E4 leaves
 * this Main-control path in scope (completion B4/B5 is same-host and owned by another suite).
 * All claims/outcomes below are created by the REAL CP. All custody is written by the REAL
 * MainAgentController. Wrappers only report/hold public operation returns; they do not dispatch,
 * forge ACKs, seed state, replace claim logic, advance cursors or emulate a transport.
 *
 * The original sender stays alive with its public pending request. Its public pause() holds
 * ACK consumption until AFTER the exact owned child was SIGKILLed and its actual close joined.
 * A NEW process, same mesh/root and ownerHostId, new incarnation, replays the retained command.
 * A subsequent public sender.request with the SAME key appends a fresh FILE event (E1), but
 * owner-local control-seen prevents a second handler admission. A distinct key is still allowed.
 */
const repository = fileURLToPath(new URL("../../", import.meta.url));
const indeterminate = "Fabric control outcome is indeterminate after owner restart";
const identity: MeshIdentity = { id: SENDER_HOST_ID, name: "owner-crash-sender", kind: "main", sessionId: "owner-crash-sender" };
type Seen = {
  hostId: string; commandId: string; targetId: string; ownerIncarnation: string;
  sequence?: number; acceptance?: FabricControlAcceptance;
};
type Item = { id: string; deliveryId: string; message: string; handed?: true };
type Observation = { type: string; commandId: string; pid: number };
type Settlement = { ok: true; value: FabricControlResult } | { ok: false; error: unknown };
function track(request: Promise<FabricControlResult>) {
  const observation = { settlements: 0 };
  const settled = request.then((value): Settlement => {
    observation.settlements++; return { ok: true, value };
  }, (error: unknown): Settlement => {
    observation.settlements++; return { ok: false, error };
  });
  const observed = bounded(settled, "public sender request");
  // A failed earlier IPC assertion may defer joining this until cleanup; never leak an
  // unhandled deadline rejection while preserving the original rejection for its awaiter.
  void observed.catch(() => undefined);
  return { observation, settled: observed };
}
function successful(result: Settlement): FabricControlResult {
  if (!result.ok) throw result.error;
  expect(result.value).toMatchObject({ queued: true, routed: "mesh", acknowledged: true, triggered: false });
  return result.value;
}
function uncertain(result: Settlement): void {
  expect(result.ok).toBe(false);
  if (result.ok) throw new Error("interrupted claim must not report accepted custody");
  expect(result.error).toBeInstanceOf(Error);
  expect((result.error as Error).message).toBe(indeterminate);
  expect(result.error).not.toMatchObject({ notRun: true });
  expect(result.error).not.toMatchObject({ notDelivered: true });
}

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-owner-crash-"));
  const paths = fixturePaths(root);
  const mesh = new MeshStore(paths.mesh, 64 * 1024, 1_000, { stateBackend: "file", canWakeResidents: () => false });
  const seen = new MeshStore(paths.seen, 64 * 1024, 1_000, { stateBackend: "file" });
  const sender = new FabricControlPlane(mesh, identity, {
    enabled: true, hostId: SENDER_HOST_ID, pollMs: 20, acknowledgementTimeoutMs: 60_000,
  });
  sender.start(() => ({ accepted: false, error: "sender is not a recipient" }));
  sender.pause(); // Requests still use the real public send path; only intake is held.
  const children: OwnedOwner[] = [];
  function launch(boundary?: CrashBoundary) {
    const child = new OwnedOwner(repository, root, children.length, boundary);
    children.push(child);
    return child;
  }
  const commands = () => mesh.read({ topic: "fabric.control.command", limit: 1_000 }).filter(event => event.kind === "followUp");
  const acknowledgements = (id: string) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 })
    .filter(event => (event.data as { commandId?: string }).commandId === id);
  const record = (id: string): Seen | undefined => seen.get(seenKey(id), { fresh: true })?.value as Seen | undefined;
  const journal = (): Item[] => {
    if (!fs.existsSync(paths.journal)) return [];
    const stored = JSON.parse(fs.readFileSync(paths.journal, "utf8")) as { version: number; items: Item[] };
    expect(stored.version).toBe(1);
    // Replay may normalize diagnostic provenance. Compare native custody identity/payload,
    // not incidental serialization order or unrelated provenance schema details.
    return stored.items.map(item => ({ id: item.id, deliveryId: item.deliveryId, message: item.message,
      ...(item.handed === true ? { handed: true as const } : {}) }));
  };
  const observations = (): Observation[] => fs.existsSync(paths.observations)
    ? fs.readFileSync(paths.observations, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Observation)
    : [];
  return {
    root, paths, mesh, sender, launch, commands, acknowledgements, record, journal, observations,
    request: (key: string, incarnation: string) => sender.request(OWNER_HOST_ID, TARGET_ID, "followUp",
      { ownerIncarnation: incarnation, message: "retain this real Main followUp" }, OWNER_HOST_ID, { idempotencyKey: key }),
    close: async () => {
      // No broad cleanup and no graceful close of a faulted child. Join every actual child close
      // before removing its mesh/custody/launch proof. On failure, retain that evidence directory.
      const stopped = await Promise.allSettled(children.map(child => child.killAndJoin()));
      sender.pause();
      const senderClosed = await Promise.allSettled([bounded(sender.close(), "sender close", 10_000)]);
      mesh.closeState(); seen.closeState();
      const failures = [...stopped, ...senderClosed].flatMap(result => result.status === "rejected" ? [result.reason] : []);
      if (failures.length) throw new AggregateError(failures, `Owner-crash cleanup not quiescent; retained ${root}`);
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
}
function data(event: MeshEvent): FabricControlCommand { return event.data as FabricControlCommand; }
function assertNoNativeConsumption(f: ReturnType<typeof fixture>) {
  expect(f.observations().filter(event => event.type === "unexpected-injection")).toEqual([]);
  expect(f.journal().every(item => item.handed !== true)).toBe(true);
  expect(fs.existsSync(`${f.paths.journal}.delivered`)).toBe(false);
}
async function checkpoint(child: OwnedOwner, through: number, bareClaim = false) {
  const message = await child.checkpoint();
  if (bareClaim) {
    // Expected B2a/b safety disposition: no outcome is fabricated to make release pass.
    expect(message.type).toBe("checkpoint-refused");
    expect(message.error).toBe("Control disk readback has a claim without a confirmed outcome");
    return;
  }
  expect(message.type).toBe("checkpoint");
  const receipt = message.receipt as { admissionHeld: boolean; state: { consumedSequence: number }; receiptScope: string };
  expect(receipt.admissionHeld).toBe(true);
  expect(receipt.state.consumedSequence).toBeGreaterThanOrEqual(through);
  expect(receipt.receiptScope).toContain("persisted claims/outcomes and correlated ACK log");
}

// The file's only platform prerequisite; missing A1/B integration never suppresses these tests.
describe.skipIf(process.platform === "win32")("owner SIGKILL / actual legacy FILE control", () => {
  it("zero-fault floor: public sender reaches actual CP/Main custody and correlated stored outcome/ACK", async () => {
    const f = fixture();
    try {
      const child = f.launch();
      const ready = await child.receipt(message => message.type === "ready");
      expect(ready).toMatchObject({ pid: child.child.pid, ownerHostId: OWNER_HOST_ID, meshRoot: f.paths.mesh,
        journal: f.paths.journal, carrier: "file", nativeConsumption: false });
      expect(child.ownership().pid).toBe(ready.pid);
      f.sender.resume();
      const request = track(f.request("healthy-floor", String(ready.incarnation)));
      const result = successful(await request.settled);
      expect(request.observation.settlements).toBe(1);
      expect(f.commands()).toHaveLength(1);
      const event = f.commands()[0]!;
      const id = data(event).commandId;
      await child.receipt(message => message.type === "ack" && message.commandId === id);
      expect(f.record(id)).toMatchObject({ commandId: id, hostId: OWNER_HOST_ID, targetId: TARGET_ID,
        sequence: event.sequence, acceptance: { accepted: true, messageId: result.messageId } });
      expect(f.journal()).toMatchObject([{ deliveryId: id, id: result.messageId }]);
      expect(f.observations().filter(entry => entry.type === "handler").map(entry => entry.commandId)).toEqual([id]);
      expect(f.acknowledgements(id)).toHaveLength(1);
      await checkpoint(child, event.sequence);
      assertNoNativeConsumption(f);
      expect(child.diagnostics()).toBe("");
    } finally { await f.close(); }
  }, 150_000);

  it.each([
    ["claim", "T-B2a: after real owner-local create-only claim, before handler"],
    ["custody", "T-B2b: after real Main durable journal return, before CP outcome"],
    ["ack", "T-B2c: after durable FILE ACK append return, before owner cursor (not JetStream PubAck)"],
  ] as const)("%s / %s", async (boundary, _description) => {
    const f = fixture();
    try {
      const first = f.launch(boundary);
      const ready = await first.receipt(message => message.type === "ready");
      const incarnation = String(ready.incarnation);
      const original = track(f.request(`crash-${boundary}`, incarnation));
      const fence = await first.receipt(message => message.type === "boundary" && message.point === boundary);
      expect(f.commands()).toHaveLength(1);
      const command = f.commands()[0]!;
      const id = data(command).commandId;
      expect(fence).toMatchObject({ pid: first.child.pid, commandId: id, carrier: "file" });
      expect(data(command)).toMatchObject({ ownerIncarnation: incarnation, targetId: TARGET_ID, operation: "followUp" });
      const stored = f.record(id);
      expect(stored).toMatchObject({ hostId: OWNER_HOST_ID, commandId: id, targetId: TARGET_ID, ownerIncarnation: incarnation });
      // A real shared legacy claim exists too; it is NOT the owner-local outcome authority.
      expect(f.mesh.get(seenKey(id), { fresh: true })?.value).toMatchObject({ commandId: id, ownerIncarnation: incarnation });
      const handlerBefore = f.observations().filter(entry => entry.type === "handler" && entry.commandId === id);
      expect(handlerBefore).toHaveLength(boundary === "claim" ? 0 : 1);
      const held = f.journal().filter(item => item.deliveryId === id);
      expect(held).toHaveLength(boundary === "claim" ? 0 : 1);
      if (boundary !== "ack") {
        expect(stored).not.toHaveProperty("acceptance");
        expect(stored).not.toHaveProperty("sequence");
        expect(f.acknowledgements(id)).toEqual([]);
      } else {
        expect(stored).toMatchObject({ sequence: command.sequence,
          acceptance: { accepted: true, messageId: held[0]!.id } });
        expect(f.acknowledgements(id)).toHaveLength(1);
        expect(fence.persistence).toContain("NOT JetStream PubAck");
        expect(Number(fence.through)).toBeGreaterThanOrEqual(Number(fence.ackSequence));
      }
      expect(original.observation.settlements).toBe(0); // Sender's intake is explicitly held, not timed.
      assertNoNativeConsumption(f);
      const birth = first.ownership();
      expect(birth.started).not.toBe("");
      expect(birth.pid).toBe(fence.pid);
      // THE fault: no gate release and no CP/Main close. Exact preload-owned PID, actual OS kill.
      expect(await first.killAndJoin()).toEqual({ code: null, signal: "SIGKILL" });
      expect(first.diagnostics()).toBe("");
      // Read AFTER process death/join, not merely after an in-memory spy callback.
      expect(f.record(id)).toEqual(stored);
      expect(f.journal().filter(item => item.deliveryId === id)).toEqual(held);

      const restarted = f.launch();
      const resumed = await restarted.receipt(message => message.type === "ready");
      expect(resumed).toMatchObject({ ownerHostId: OWNER_HOST_ID, meshRoot: f.paths.mesh, journal: f.paths.journal });
      expect(resumed.incarnation).not.toBe(incarnation);
      restarted.ownership();
      const replay = await restarted.receipt(message => message.type === "ack" && message.commandId === id);
      expect(replay.data).toMatchObject(boundary === "ack"
        ? { accepted: true, messageId: held[0]!.id, ownerIncarnation: incarnation }
        : { accepted: false, error: indeterminate, ownerIncarnation: incarnation });
      expect(replay.data).not.toMatchObject({ notRun: true });
      f.sender.resume();
      const originalResult = await original.settled;
      if (boundary === "ack") expect(successful(originalResult).messageId).toBe(held[0]!.id);
      else uncertain(originalResult);
      expect(original.observation.settlements).toBe(1); // Original + replay ACK cannot settle twice.

      // PUBLIC same-key retry, NOT a forged raw command replay or a new logical id.
      const after = restarted.receipts.length;
      const retry = track(f.request(`crash-${boundary}`, incarnation));
      const retried = await retry.settled;
      await restarted.receipt(message => message.type === "ack" && message.commandId === id, after);
      if (boundary === "ack") {
        expect(successful(retried)).toEqual(successful(originalResult));
      } else uncertain(retried);
      expect(retry.observation.settlements).toBe(1);
      expect(original.observation.settlements).toBe(1);
      expect(f.commands()).toHaveLength(2); // FILE allows fresh event ids (E1), not carrier dedupe.
      expect(f.commands().map(event => data(event).commandId)).toEqual([id, id]);
      expect(f.commands()[1]!.id).not.toBe(command.id);
      expect(f.record(id)).toEqual(stored); // b retains native custody, but never promotes/re-admits a bare claim.
      expect(f.journal().filter(item => item.deliveryId === id)).toEqual(held);
      expect(f.observations().filter(entry => entry.type === "handler" && entry.commandId === id)).toHaveLength(boundary === "claim" ? 0 : 1);
      for (const ack of f.acknowledgements(id)) {
        expect(ack.data).toMatchObject(boundary === "ack" ? stored!.acceptance! : { accepted: false, error: indeterminate });
        expect(ack.data).not.toMatchObject({ notRun: true });
      }
      expect(f.acknowledgements(id)).toHaveLength(boundary === "ack" ? 3 : 2);

      // A new logical id and the NEW activation's address may still be admitted normally.
      const fresh = track(f.request(`crash-${boundary}-distinct`, String(resumed.incarnation)));
      const next = successful(await fresh.settled);
      expect(fresh.observation.settlements).toBe(1);
      expect(f.commands()).toHaveLength(3);
      const freshCommand = f.commands().at(-1)!;
      const freshId = data(freshCommand).commandId;
      expect(freshId).not.toBe(id);
      await restarted.receipt(message => message.type === "ack" && message.commandId === freshId);
      expect(f.record(freshId)).toMatchObject({ ownerIncarnation: resumed.incarnation,
        acceptance: { accepted: true, messageId: next.messageId } });
      expect(f.journal().filter(item => item.deliveryId === freshId)).toMatchObject([{ id: next.messageId }]);
      expect(f.journal()).toHaveLength(boundary === "claim" ? 1 : 2);
      expect(f.observations().filter(entry => entry.type === "handler").map(entry => entry.commandId))
        .toEqual(boundary === "claim" ? [freshId] : [id, freshId]);
      await checkpoint(restarted, freshCommand.sequence, boundary !== "ack");
      assertNoNativeConsumption(f);
      expect(restarted.diagnostics()).toBe("");
    } finally { await f.close(); }
  }, 150_000);
});
