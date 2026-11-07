// A real OS process, not Vitest's module registry or a second control dispatcher.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createJiti } from "jiti";

const [repository, root, boundary = "none"] = process.argv.slice(2);
if (!path.isAbsolute(repository ?? "") || !path.isAbsolute(root ?? "") || !process.send) {
  throw new Error("owner.mjs requires absolute repository/root paths and an IPC channel");
}
// Finite lifetime even if a test abandons the IPC channel. This is not a polling timer.
const deadline = setTimeout(() => {
  process.stderr.write("owner fixture exceeded its 120 s process deadline\n");
  process.exit(124);
}, 120_000);
const jiti = createJiti(pathToFileURL(path.join(repository, "index.js")).href, { fsCache: false });
const { MeshStore } = await jiti.import(path.join(repository, "src/mesh/store.ts"));
const { FabricControlPlane } = await jiti.import(path.join(repository, "src/topology/control-plane.ts"));
const { MainAgentController } = await jiti.import(path.join(repository, "src/main-agent.ts"));
const { OWNER_HOST_ID, TARGET_ID, CONTROL_SEEN_PREFIX, fixturePaths, bounded } = await jiti.import(
  path.join(repository, "tests/transport-faults/control-fixtures/paths.ts"),
);
const paths = fixturePaths(root);
const identity = { id: OWNER_HOST_ID, name: "owner-crash", kind: "main", sessionId: "owner-crash" };
function send(message) {
  return new Promise((resolve, reject) => process.send({ ...message, pid: process.pid }, error => error ? reject(error) : resolve()));
}
function observe(type, commandId) {
  // Test observations only. These never decide admission or stand in for custody/outcomes.
  fs.appendFileSync(paths.observations, JSON.stringify({ type, commandId, pid: process.pid }) + "\n");
}
const gates = new Map();
let selected = false;
async function stopAt(point, detail) {
  if (selected || boundary !== point) return;
  selected = true;
  const token = `${process.pid}:${point}`;
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  gates.set(token, release); // Install before the receipt can reach the parent.
  try {
    await send({ type: "boundary", point, token, ...detail });
    await bounded(gate, `IPC release ${point}`, 60_000);
  } finally { gates.delete(token); }
}

// Wrap ONLY public real operations, after their real commit/return. No synthetic claims,
// outcomes, ACKs, cancellation or dispatch. No admission fence option: legacy #putFenced
// therefore uses this real MeshStore.put path (not a guessed writeBatch interception).
const realPut = MeshStore.prototype.put;
MeshStore.prototype.put = async function (input) {
  const result = await realPut.call(this, input);
  if (path.resolve(this.root) === path.resolve(paths.seen) && input.key.startsWith(CONTROL_SEEN_PREFIX)) {
    // Explicit real file barrier; process-death proof does not assume put implies power-loss safety.
    await this.sync();
    if (input.ifVersion === 0 && input.value.acceptance === undefined) {
      await stopAt("claim", { commandId: input.value.commandId, key: input.key, version: result.version,
        carrier: "file", persistence: "owner-local create-only MeshStore.put returned; MeshStore.sync returned" });
    }
  }
  return result;
};
const realPublish = MeshStore.prototype.publish;
MeshStore.prototype.publish = async function (input) {
  const event = await realPublish.call(this, input);
  if (path.resolve(this.root) === path.resolve(paths.mesh) && input.topic === "fabric.control.ack") {
    // Legacy CP's unkeyed publish does NOT request durable:true. Make the real public
    // barrier explicit here, rather than falsely calling an append return a JetStream PubAck.
    // The wrapper has not returned to CP: outcome/ACK retirement and its final cursor step
    // are still blocked. No persistent file cursor or broker ack is invented for legacy CP.
    const synced = await this.sync();
    await stopAt("ack", { commandId: event.data.commandId, ackSequence: event.sequence,
      through: synced.through, carrier: "file",
      persistence: "real ACK file append returned and MeshStore.sync returned; NOT JetStream PubAck" });
    await send({ type: "ack", commandId: event.data.commandId, sequence: event.sequence, data: event.data });
  }
  return event;
};

// No native model invocation. A genuinely busy context holds followUps in the actual durable
// Main journal; a long flush age is secondary protection, not a sleep or an injection timer.
const pi = {
  on: () => () => {},
  getThinkingLevel: () => "high",
  sendMessage: () => { observe("unexpected-injection", ""); throw new Error("B2 fixture must not inject into Pi"); },
  sendUserMessage: () => { observe("unexpected-injection", ""); throw new Error("B2 fixture must not inject a user message"); },
};
const context = {
  hasUI: false,
  isIdle: () => false,
  isPromptPending: () => false,
  hasPendingMessages: () => false,
  sessionManager: { getSessionId: () => "owner-crash", getEntries: () => [], isPersisted: () => false },
};
const main = new MainAgentController(pi, TARGET_ID, true, root, "owner-crash", true,
  undefined, {}, undefined, paths.mesh);
main.attachFollowUpDrain(context, 24 * 60 * 60 * 1_000, paths.journal);
const mesh = new MeshStore(paths.mesh, 64 * 1024, 1_000, { stateBackend: "file", canWakeResidents: () => false });
const control = new FabricControlPlane(mesh, identity, {
  enabled: true, hostId: OWNER_HOST_ID, pollMs: 20, acknowledgementTimeoutMs: 60_000,
  controlIncarnationFence: "enforce",
});
control.start(async (command, from, _signal, verification) => {
  if (command.targetId !== TARGET_ID || command.operation !== "followUp") throw new Error("unexpected owner fixture route");
  observe("handler", command.commandId);
  const result = main.deliverAgent({ from, verification, message: command.message, delivery: "followUp",
    deliveryId: command.commandId, principal: command.principal, data: command.data,
    inboxEventId: command.eventId, inboxTopic: command.inboxTopic, inboxReceipts: command.inboxReceipts,
  });
  observe("custody-return", command.commandId);
  await stopAt("custody", { commandId: command.commandId, messageId: result.messageId,
    carrier: "file", persistence: "real MainAgentController.deliverAgent durable journal return, before CP outcome" });
  return { accepted: true, messageId: result.messageId, triggered: result.triggered,
    pendingFollowUps: result.pendingFollowUps, oldestAgeS: result.oldestAgeS };
});
process.on("message", message => {
  if (message?.type === "release") gates.get(message.token)?.();
  if (message?.type === "checkpoint") {
    void (async () => {
      control.pause();
      try {
        await control.checkpointForRelease(); // Public join, not a quiet-time guess.
        await send({ type: "checkpoint", token: message.token, receipt: control.releaseCheckpointReceipt() });
      } catch (error) {
        // B2a/b intentionally retain a bare claim: the REAL release check must refuse it.
        // Do not invent a successful release/cursor receipt for an indeterminate command.
        await send({ type: "checkpoint-refused", token: message.token, error: String(error?.message ?? error) });
      }
    })().catch(fatal);
  }
});
function fatal(error) {
  process.stderr.write(String(error?.stack ?? error) + "\n");
  void send({ type: "fatal", error: String(error?.stack ?? error) }).finally(() => process.exit(1));
}
process.on("unhandledRejection", fatal);
process.on("uncaughtException", fatal);
// Disconnect is parent abandonment, not a graceful shutdown standing in for crash injection.
process.on("disconnect", () => { clearTimeout(deadline); process.exit(125); });
await send({ type: "ready", incarnation: control.incarnation, ownerHostId: OWNER_HOST_ID,
  meshRoot: paths.mesh, journal: paths.journal, carrier: "file", nativeConsumption: false });
