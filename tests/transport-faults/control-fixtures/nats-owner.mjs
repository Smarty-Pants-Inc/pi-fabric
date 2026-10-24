// Actual Node CP/NATS/Main composition. Manual known routing is NOT a production roots/lease proof.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createJiti } from "jiti";

const [repository, root, boundary = "none"] = process.argv.slice(2);
if (!path.isAbsolute(repository ?? "") || !path.isAbsolute(root ?? "") || !process.send) throw new Error("Absolute repository/root and IPC required");
if (process.release.name !== "node" || "bun" in process.versions) throw new Error("Actual Node child required");
// Finite abandonment/deadlock bound; normal events are IPC and broker delivery, never polling.
const deadline = setTimeout(() => { process.stderr.write("NATS owner exceeded 180 s deadline\n"); process.exit(124); }, 180_000);
function send(message) {
  return new Promise((resolve, reject) => process.send({ ...message, pid: process.pid }, error => error ? reject(error) : resolve()));
}
function fatal(error) {
  process.stderr.write(String(error?.stack ?? error) + "\n");
  void send({ type: "fatal", error: String(error?.stack ?? error) }).finally(() => process.exit(1));
}
process.on("unhandledRejection", fatal);
process.on("uncaughtException", fatal);
process.on("disconnect", () => { clearTimeout(deadline); process.exit(125); });
// Reuse owner.mjs's official jiti source-loading path, with filesystem cache disabled.
const jiti = createJiti(pathToFileURL(path.join(repository, "index.js")).href, { fsCache: false });
const load = relative => jiti.import(path.join(repository, relative));
const { MeshStore } = await load("src/mesh/store.ts");
const { FabricControlPlane } = await load("src/topology/control-plane.ts");
const { MainAgentController } = await load("src/main-agent.ts");
const { DEFAULT_FABRIC_CONFIG } = await load("src/config.ts");
const { openFabricTransport } = await load("src/transport/index.ts");
const { knownParticipants } = await load("tests/transport-faults/control-fixtures/nats-owner.ts");
const { OWNER_HOST_ID, TARGET_ID, CONTROL_SEEN_PREFIX, fixturePaths, bounded } = await load("tests/transport-faults/control-fixtures/paths.ts");
const paths = fixturePaths(root);
const identity = { id: OWNER_HOST_ID, name: "owner-crash", kind: "main", sessionId: "owner-crash" };
function observe(type, commandId, detail = {}) {
  fs.appendFileSync(paths.observations, JSON.stringify({ type, commandId, pid: process.pid, ...detail }) + "\n");
}
let selected = false;
async function stopAt(point, detail) {
  if (selected || boundary !== point) return;
  selected = true;
  await send({ type: "boundary", point, carrier: "nats", ...detail });
  // Never released: parent must SIGKILL this exact birth and join native close. A missed
  // boundary FAILS at the finite IPC/process deadline, rather than NAKing as a fake death.
  await bounded(new Promise(() => {}), `SIGKILL boundary ${point}`, 90_000);
  throw new Error("Unreachable boundary return");
}

// Observe the actual A1 operation, not put(): NATS claims and outcomes use writeBatch.
const realBatch = MeshStore.prototype.writeBatch;
MeshStore.prototype.writeBatch = async function (input) {
  const result = await realBatch.call(this, input);
  if (path.resolve(this.root) === path.resolve(paths.seen)) {
    const claims = input.ops.filter(op => op.kind === "put" && op.ifVersion === 0 && op.key.startsWith(CONTROL_SEEN_PREFIX));
    if (claims.length) {
      await this.sync(); // Explicit real owner-local state/namespace durability barrier.
      for (const op of claims) {
        const record = this.get(op.key, { fresh: true })?.value;
        if (!record || record.carrier !== "nats" || record.acceptance !== undefined) throw new Error("Not a real bare NATS claim");
        await stopAt("claim", { commandId: record.commandId, key: op.key, record,
          persistence: "actual owner-local create-only writeBatch and MeshStore.sync returned" });
      }
    }
  }
  return result;
};
const mesh = new MeshStore(paths.mesh, 1_048_576, 1_000, { stateBackend: "file", canWakeResidents: () => false });
const runtimeOptions = JSON.parse(process.env.FABRIC_OWNER_OPTIONS ?? "null");
if (!runtimeOptions || runtimeOptions.machine !== "machine-b") throw new Error("NATS owner configuration is incomplete");
const config = { ...DEFAULT_FABRIC_CONFIG.mesh, root: paths.mesh, transport: "nats",
  nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, ...runtimeOptions, operatorMachine: "fabric-ops" } };
const real = await openFabricTransport({ config, mesh, hostId: OWNER_HOST_ID });
if (real.kind !== "nats" || real.self.machine !== "machine-b") throw new Error("Wrong actual carrier");
// Transparent public-operation observers only: actual send resolves after js.publish PubAck;
// held return prevents CP's final delivery.ack. No alternative admission/ACK engine.
const transport = {
  kind: real.kind, self: real.self,
  send: async (envelope, signal) => {
    const sent = await real.send(envelope, signal);
    if (envelope.kind === "ack") {
      await stopAt("ack", { commandId: sent.envelope.body.commandId, sent,
        persistence: "real ACK-envelope transport.send PubAck returned; CP has not called delivery.ack" });
      await send({ type: "puback", commandId: sent.envelope.body.commandId, sent });
    }
    return sent;
  },
  inbox: async function* (signal) {
    for await (const delivery of real.inbox(signal)) {
      const body = delivery.payload.body;
      await send({ type: "delivery", commandId: body.commandId, payload: delivery.payload,
        sequence: delivery.sequence, attempt: delivery.attempt, sourceMachine: delivery.sourceMachine, verification: delivery.verification });
      yield { ...delivery,
        ack: async () => {
          observe("final-ack-call", body.commandId);
          await delivery.ack();
          await send({ type: "final-ack", commandId: body.commandId, sequence: delivery.sequence, drain: await real.drainState() });
        },
        nak: delay => { observe("unexpected-nak", body.commandId); delivery.nak(delay); },
        term: async reason => { observe("unexpected-term", body.commandId); await delivery.term(reason); },
      };
    }
  },
  kv: bucket => real.kv(bucket), drainState: signal => real.drainState(signal), close: () => real.close(),
};
const pi = {
  on: () => () => {}, getThinkingLevel: () => "high",
  sendMessage: () => { observe("unexpected-injection", ""); throw new Error("B2 must not inject into Pi"); },
  sendUserMessage: () => { observe("unexpected-injection", ""); throw new Error("B2 must not inject a user message"); },
};
const context = { hasUI: false, isIdle: () => false, isPromptPending: () => false, hasPendingMessages: () => false,
  sessionManager: { getSessionId: () => "owner-crash", getEntries: () => [], isPersisted: () => false } };
const main = new MainAgentController(pi, TARGET_ID, true, root, "owner-crash", true, undefined, {}, undefined, paths.mesh);
main.attachFollowUpDrain(context, 86_400_000, paths.journal);
// A new activation, same exact host/root. The known view is explicit test routing, not lease proof.
const incarnation = `nats-owner-${process.pid}`;
const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: OWNER_HOST_ID,
  ownerIncarnation: incarnation, controlIncarnationFence: "enforce", transport,
  participants: knownParticipants(incarnation), relayOperatorMachine: "fabric-ops", acknowledgementTimeoutMs: 120_000 });
control.start(async (command, from, _signal, verification) => {
  if (command.targetId !== TARGET_ID || command.operation !== "followUp") throw new Error("Unexpected native route");
  observe("handler", command.commandId);
  const result = main.deliverAgent({ from, verification, message: command.message, delivery: "followUp",
    deliveryId: command.commandId, principal: command.principal, data: command.data,
    inboxEventId: command.eventId, inboxTopic: command.inboxTopic, inboxReceipts: command.inboxReceipts });
  observe("custody-return", command.commandId);
  await stopAt("custody", { commandId: command.commandId, messageId: result.messageId,
    persistence: "actual MainAgentController.deliverAgent durable journal returned; before CP outcome" });
  return { accepted: true, messageId: result.messageId, triggered: result.triggered,
    pendingFollowUps: result.pendingFollowUps, oldestAgeS: result.oldestAgeS };
});
await send({ type: "ready", incarnation, ownerHostId: OWNER_HOST_ID, meshRoot: paths.mesh, journal: paths.journal,
  carrier: "nats", machine: real.self.machine, nativeConsumption: false, routing: "manual known ParticipantView" });
