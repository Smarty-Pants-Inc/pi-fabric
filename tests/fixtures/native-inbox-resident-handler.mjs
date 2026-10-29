// TEST ONLY: actual resident-loader adapter around the committed SQLite fixture.
// This is NOT smarty-inbox's production handler, HOME/config, policy or client.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { createNativeInboxHandler } from "./native-inbox-handler.mjs";

const pinsFile = process.env.FABRIC_NATIVE_OWNER_PINS;
assert.ok(pinsFile && path.isAbsolute(pinsFile), "N8 resident requires host-owned owner pins");
const root = fs.realpathSync(path.dirname(pinsFile));
assert.equal(path.join(root, "pins.json"), pinsFile);
const pins = JSON.parse(fs.readFileSync(pinsFile, "utf8"));
assert.ok(process.env.PI_FABRIC_RESIDENT_CONFIG, "N8 fixture must load in the actual detached resident, not Main");
const residentConfig = JSON.parse(fs.readFileSync(process.env.PI_FABRIC_RESIDENT_CONFIG, "utf8"));
assert.equal(residentConfig.rootId, pins.binding.rootId);
assert.equal(residentConfig.sessionId, pins.binding.rootId.slice(8));
const configFile = path.join(root, "native-inbox-fixture.json");
const stat = fs.lstatSync(configFile);
assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.uid === process.geteuid() && (stat.mode & 0o777) === 0o600);
const fixture = JSON.parse(fs.readFileSync(configFile, "utf8"));
assert.deepEqual(Object.keys(fixture).sort(), ["seats", "v"]);
assert.equal(fixture.v, 1);
// Neither payload nor context can select a DB path, seat map, module or HOME.
const databasePath = path.join(root, "native-inbox.sqlite");
const rpc = createNativeInboxHandler({ databasePath, seats: fixture.seats });
const moduleLoadId = randomUUID(), databaseOpenId = randomUUID();
const observe = value => fs.appendFileSync(pins.observations, JSON.stringify({ at: Date.now(), pid: process.pid, ...value }) + "\n", { mode: 0o600 });
observe({ event: "module-loaded", resident: true, fixture: "transactional-test-only", moduleLoadId,
  module: import.meta.url, rootId: residentConfig.rootId, sessionId: residentConfig.sessionId,
  residentIdleExitMs: residentConfig.mesh.residentIdleExitMs });
const databaseStat = fs.lstatSync(databasePath);
observe({ event: "database-opened", databaseOpenId, moduleLoadId, databasePath,
  dev: databaseStat.dev, ino: databaseStat.ino, readback: rpc.readback() });
// No restart substitution: the real process must die to close/reconstruct this
// module and SQLite connection. This adapter has no fault/clock/context switch.
// Owner helper's IMMUTABLE binding key. Business request.message stays exactly
// smarty-inbox.rpc.v1; the key is not substituted into the request or wire.
export const handlerKey = "native-residency.test.v1";
assert.equal(pins.binding.handlerKey, handlerKey);
export const handle = async (request, ctx) => {
  assert.equal(ctx.actorId, pins.binding.actorId);
  assert.equal(ctx.link, pins.binding.ingress.link);
  const enteredAt = Date.now();
  const reply = await rpc(request, ctx);
  // The transaction COMMIT (PRAGMA synchronous=FULL) is complete before these
  // observations and the actual returned application data. No throw/loss shim.
  const record = { pid: process.pid, moduleLoadId, databaseOpenId, enteredAt, settledAt: Date.now(), request,
    context: { actorId: ctx.actorId, source: ctx.source, sender: ctx.sender, sourceMachine: ctx.sourceMachine,
      link: ctx.link, commandId: ctx.commandId, sourceCommandId: ctx.sourceCommandId, sourceEventId: ctx.sourceEventId,
      deadlineAt: ctx.deadlineAt, signalAborted: ctx.signal.aborted }, data: reply.data, readback: rpc.readback() };
  fs.appendFileSync(pins.callbacks, JSON.stringify(record) + "\n", { mode: 0o600 });
  return reply; // Actual public reply.data is checked by the separate old caller.
};
