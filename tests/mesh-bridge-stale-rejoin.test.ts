// Keep: a retained notification forward must not crash the process on a stale mirror rejoin.
// Real stores, canonical installers and run(); control only notification delivery, mirror
// completion/failure and the public busy-tail contract. Never replace syncPresence's result.
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import { describe, expect, it } from "vitest";

const repository = fileURLToPath(new URL("../", import.meta.url));

// Bundle repository-relative imports, just as the other native bridge probes do. The
// separate Node process has no Vitest rejection handler and explicitly uses strict policy.
const probe = String.raw`
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  BridgeMirrorDeferredError, BridgeStaleIncarnationError, MeshBridge, StoreBridgeSide,
} from "./src/mesh/bridge.ts";
import { MeshStore } from "./src/mesh/store.ts";
import { writeHostLease } from "./src/topology/host-leases.ts";
import { writeParticipantFile } from "./src/topology/participant-files.ts";

const mode = process.argv[2], root = process.argv[3];
let lastStage = "boot";
const stage = (name, detail = {}) => {
  lastStage = name;
  fs.writeSync(1, JSON.stringify({ probe: "P2_REJOIN", mode, stage: name, ...detail }) + "\n");
};
// Observation only: do NOT handle uncaughtException/unhandledRejection or suppress the
// native exit. A baseline crash must identify the exact reached rejoin, not just exit 1.
process.on("uncaughtExceptionMonitor", (error, origin) => {
  fs.writeSync(2, JSON.stringify({ probe: "P2_REJOIN", mode, stage: "native-uncaught",
    lastStage, origin, code: error?.code, name: error?.name }) + "\n");
});
assert.equal(process.release.name, "node", "the probe requires native Node, not a compatibility runtime");
assert.ok(Number(process.versions.node.split(".")[0]) >= 24, "the probe requires Node 24 or newer");
assert.equal(process.listenerCount("uncaughtException"), 0, "no handler may suppress native strict exit");
assert.equal(process.listenerCount("unhandledRejection"), 0, "no framework may own the probe's rejections");
stage("native-ready", { node: process.versions.node, unhandled: "strict" });

const gate = () => {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
};
const bounded = async (pending, label, ms = 10_000) => {
  let timer;
  try {
    return await Promise.race([pending, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("Timed out at " + label + "; last stage=" + lastStage)), ms);
    })]);
  } finally { clearTimeout(timer); }
};
const turn = () => new Promise(resolve => setImmediate(resolve));
const observe = pending => pending.then(value => ({ ok: true, value }), error => ({ ok: false, error }));
const key = (kind, id) => "topology/" + kind + "/" + createHash("sha256").update(id).digest("hex");
const identity = name => ({ id: "session:rejoin-" + name, name: "main", kind: "main", sessionId: name });
const native = async (store, who) => {
  const now = Date.now(), id = who.id;
  await store.put({ key: key("hosts", id), identity: who, value: {
    format: 1, id, rootId: id, identity: who, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: who, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "fixture:" + id,
    name: who.sessionId, label: who.sessionId, status: "idle", runner: "pi", transport: "host",
    capabilities: ["followUp"], sessionId: who.sessionId, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
};

const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100, { stateBackend: "file" });
const far = new MeshStore(path.join(root, "far"), 64 * 1024, 100, { stateBackend: "file" });
const local = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
const lead = identity("lead"), missing = identity("missing");
const failure = mode === "stale" ? new BridgeStaleIncarnationError(missing.id, "fixture-old-host")
  : mode === "retryable" ? new BridgeMirrorDeferredError(false, 30_000)
  : new Error("fixture permanent inbound mirror refusal");
const cursorPath = path.join(root, "cursor.json");
const logs = [], writes = [], bufferedNotices = [];
const inboundGate = gate(), outboundGate = gate(), inboundEntered = gate(), outboundEntered = gate();
const retained = gate(), busyParked = gate(), coordinatorJoined = gate(), rejoined = gate();
const laterFailed = gate(), retryPageRead = gate(), initialLocalRead = gate(), initialRemoteRead = gate();
let notice, gated = false, healthy = false, tearingDown = false;
let inboundCalls = 0, outboundCalls = 0, inboundFailures = 0;
let parkReads = false, busyReads = 0, sourceReadsWithEvent = 0, watchRetryPage = false;
let explicitCall = false, awaitingCoordinator = false, retryArmed = false;
let rejoinInfo, runSettled = false, shared, sharedOutcome, strictOutcome, event, originalBody;

// Keep real filesystem subscriptions installed but gate their delivery from startup,
// issuing named callbacks explicitly. Even delayed startup-mirror notices must not
// decide whether the coordinator has entered another aggregate before the gates arm.
for (const side of [local, remote]) {
  const subscribe = side.subscribeChanges.bind(side);
  side.subscribeChanges = (changed, failed) => {
    if (side === remote) notice = changed;
    return subscribe(kind => { bufferedNotices.push(kind); }, failed);
  };
}
const realInbound = local.mirror.bind(local), realOutbound = remote.mirror.bind(remote);
local.mirror = (...args) => {
  const pending = (async () => {
    if (gated && ++inboundCalls === 1) {
      inboundEntered.resolve();
      await inboundGate.promise;
      if (!tearingDown) { inboundFailures++; throw failure; }
    } else if (gated && mode === "retryable" && !healthy && !tearingDown) {
      // No successful negative sample may sneak in before canonical recovery.
      inboundFailures++;
      laterFailed.resolve();
      throw failure;
    }
    return realInbound(...args);
  })();
  writes.push(observe(pending));
  return pending;
};
remote.mirror = (...args) => {
  const pending = (async () => {
    if (gated && ++outboundCalls === 1) {
      outboundEntered.resolve();
      await outboundGate.promise;
    }
    return realOutbound(...args);
  })();
  writes.push(observe(pending));
  return pending;
};
const localTail = local.tail.bind(local), remoteTail = remote.tail.bind(remote);
local.tail = (...args) => { initialLocalRead.resolve(); return localTail(...args); };
remote.tail = async (after, offset) => {
  initialRemoteRead.resolve();
  if (parkReads) {
    busyReads++;
    stage("busy-before-authority", { ordinal: busyReads, after, offset: offset ?? null });
    busyParked.resolve();
    // This exact boundary-preserving response is the production busy-tail contract.
    return { events: [], busy: true, through: after, offset };
  }
  const page = await remoteTail(after, offset);
  if (event && page.events.some(item => item.id === event.id)) {
    sourceReadsWithEvent++;
    if (watchRetryPage) retryPageRead.resolve();
  }
  return page;
};
const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
  membershipRetryMs: 30_000, stopMs: 5_000,
  log: line => logs.push(line), debug: line => {
    logs.push(line);
    if (line.includes("toLocal: bridge membership pending")) {
      stage("retained-incomplete");
      parkReads = true;
      retained.resolve();
    }
  },
});
const realSync = bridge.syncPresence.bind(bridge);
bridge.syncPresence = () => {
  const pending = realSync();
  if (!explicitCall && awaitingCoordinator && pending === shared) {
    coordinatorJoined.resolve();
  }
  if (!explicitCall && retryArmed) {
    rejoinInfo = { shared: pending === shared, outboundCalls, inboundCalls, runSettled };
    stage("retained-rejoin", rejoinInfo);
    retryArmed = false;
    rejoined.resolve();
  }
  // Critically, no .then/.catch/await here: the production caller alone must observe
  // an early rejected rejoin. Even an observational catch would hide the regression.
  return pending;
};
const publicSync = () => {
  explicitCall = true;
  try { return bridge.syncPresence(); }
  finally { explicitCall = false; }
};
const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8"));
const assertRetained = () => {
  assert.ok(cursor().toLocal.after < event.sequence, "the durable cursor must stay before the held event");
  assert.deepEqual(hub.read({ after: 0, limit: 100 }), [], "no body may be published without canonical authority");
  assert.deepEqual(far.read({ after: event.sequence - 1, limit: 1 })[0], originalBody,
    "the retained source body must remain intact");
  assert.equal(logs.some(line => line.includes(event.id) && line.includes(": dropped ")), false);
};

let running;
try {
  await native(hub, lead);
  assert.equal(local.holds(missing.id), false);
  assert.equal((await remote.presence()).hosts.some(host => host.record.id === missing.id), false);
  running = observe(bridge.run()).then(result => { runSettled = true; return result; });
  await bounded(Promise.all([initialLocalRead.promise, initialRemoteRead.promise]), "initial notification drains");
  await turn();
  assert.equal(runSettled, false);
  gated = true;
  shared = publicSync();
  sharedOutcome = observe(shared);
  await bounded(Promise.all([inboundEntered.promise, outboundEntered.promise]), "both real mirror halves entered");
  awaitingCoordinator = true;
  notice("presence");
  await bounded(coordinatorJoined.promise, "coordinator joined the held aggregate");
  awaitingCoordinator = false;
  await turn();
  stage("coordinator-awaiting-shared", { inboundCalls, outboundCalls });

  event = await far.publish({ topic: "fabric.control.command", kind: "followUp", from: missing, to: lead.id,
    data: { version: 1, commandId: "rejoin-retained-command", targetId: lead.id, replyTo: missing.id,
      operation: "followUp", destinationRemoteHost: "hub", ownerIncarnation: "fixture:" + lead.id,
      message: "retained rejoin body" },
  });
  originalBody = far.read({ after: event.sequence - 1, limit: 1 })[0];
  assert.deepEqual(originalBody, event);
  notice("events");
  await bounded(retained.promise, "the real absent-sender refresh hit its one-second cap");
  assertRetained();
  // The next retained refresh also caps, then parks on busy without authority().
  notice("lease");
  await bounded(busyParked.promise, "retained retry parked before canonical authority");
  await turn();
  assertRetained();
  assert.equal(inboundCalls, 1);
  assert.equal(outboundCalls, 1);
  assert.equal(runSettled, false, "the coordinator must still own the pending aggregate");

  inboundGate.resolve();
  await turn(); // real per-mirror rejection policy runs while the sibling remains held
  assert.equal(inboundFailures, 1);
  assert.equal(runSettled, false);
  if (mode === "stale") {
    const strict = await observe(publicSync());
    assert.equal(strict.ok, false);
    assert.equal(strict.error, failure, "strict public sync must retain the original stale error");
  } else {
    const strict = publicSync();
    assert.equal(strict, shared, "non-stale rejoin must keep sharing the owned aggregate");
    strictOutcome = observe(strict);
  }
  stage("half-failure-observed", { error: failure.name, siblingPending: true });
  retryArmed = true;
  stage("lease-wake");
  notice("lease");
  await bounded(rejoined.promise, "the actual retained forward rejoined after half-failure");
  assert.equal(rejoinInfo.shared, mode !== "stale");
  assert.equal(rejoinInfo.runSettled, false);
  assert.equal(rejoinInfo.inboundCalls, 1);
  assert.equal(rejoinInfo.outboundCalls, 1);
  await turn(); // strict native unhandled-rejection processing must get a full turn
  stage("post-rejoin-turn");
  assertRetained();
  assert.equal(runSettled, false);

  if (mode === "stale") {
    const strict = await observe(publicSync());
    assert.equal(strict.ok, false);
    assert.equal(strict.error, failure);
    stage("strict-public-rejection", { original: true });
  }
  const readsBeforeRelease = sourceReadsWithEvent;
  watchRetryPage = mode === "retryable";
  parkReads = false;
  outboundGate.resolve();
  const settled = await bounded(sharedOutcome, "the original aggregate settled both mirror writes");
  assert.equal(settled.ok, false);
  assert.equal(settled.error, failure);
  if (strictOutcome) {
    const strict = await bounded(strictOutcome, "strict shared public rejection");
    assert.equal(strict.ok, false);
    assert.equal(strict.error, failure);
    stage("strict-public-rejection", { original: true });
  }
  assertRetained();

  if (mode === "retryable") {
    // Read the real retained page again with subsequent aggregates still failing.
    // Busy-tail gating is OFF: a failed sample itself must not establish absence.
    notice("events");
    await bounded(Promise.all([laterFailed.promise, retryPageRead.promise]),
      "the retained real page was reread while later aggregates also failed retryably");
    await turn();
    assert.ok(sourceReadsWithEvent > readsBeforeRelease, "the retained real page was reread, not hidden by busy");
    assertRetained();
    assert.equal(runSettled, false, "retryable mirror uncertainty must not be fatal");
    stage("failed-sample-not-absence", { inboundFailures, sourceReadsWithEvent });
    // Install current canonical source authority while every aggregate is still
    // failing. Observe publication before enabling recovery: a real watch-independent
    // retry may restore authority without waiting for the driver's final notice.
    const delivered = gate();
    const publish = local.publishWithReceipt.bind(local);
    local.publishWithReceipt = async (...args) => {
      const receipt = await publish(...args);
      delivered.resolve();
      return receipt;
    };
    await native(far, missing);
    // Settle any pass that sampled before the installation. No old absent snapshot
    // may become a successful aggregate merely because the test enables recovery.
    const recoveryFence = await bounded(observe(publicSync()), "pre-recovery failed aggregate fence");
    assert.equal(recoveryFence.ok, false);
    assert.equal(recoveryFence.error, failure);
    healthy = true;
    await realInbound(await remote.presence(), Date.now());
    assert.equal(local.holds(missing.id), true);
    notice("presence");
    await bounded(delivered.promise, "canonical recovery delivered the retained body");
    await turn();
    const events = hub.read({ after: 0, limit: 100 });
    assert.equal(events.length, 1);
    assert.equal(events[0].data.bridge.id, event.id);
    assert.equal(events[0].data.message, "retained rejoin body");
    assert.ok(cursor().toLocal.after >= event.sequence);
    assert.deepEqual(far.read({ after: event.sequence - 1, limit: 1 })[0], originalBody);
    assert.equal(logs.some(line => line.includes(event.id) && line.includes(": dropped ")), false);
    assert.equal(runSettled, false);
    stage("canonical-recovery-delivered", { originalBody: true, published: events.length });
  } else {
    const result = await bounded(running, "fatal run policy after the sibling settled");
    assert.equal(result.ok, false);
    assert.equal(result.error, failure, "the catch must not swallow fatal notification policy");
    assertRetained();
    stage("fatal-run", { original: true, stale: mode === "stale" });
  }
} finally {
  // Release every fixture-owned gate on assertion failure too. Never close a store
  // while a held installer can still write to it. A native fatal exit never enters
  // this cleanup; its abandoned fixture belongs only to this test-created directory.
  tearingDown = true;
  gated = false;
  parkReads = false;
  inboundGate.resolve();
  outboundGate.resolve();
  if (sharedOutcome) await bounded(sharedOutcome, "teardown shared pass");
  if (strictOutcome) await bounded(strictOutcome, "teardown strict caller");
  await bridge.stop();
  if (running) await bounded(running, "teardown run");
  let observed = 0;
  while (observed < writes.length) {
    const pending = writes.slice(observed);
    observed = writes.length;
    await bounded(Promise.all(pending), "all fixture-owned mirror writes settled");
  }
  hub.closeState();
  far.closeState();
  stage("closed-after-owned-writes", { writes: writes.length, bufferedNotices: bufferedNotices.length });
}
stage("complete");
`;

describe("native retained membership rejoin rejection ownership", () => {
  it.each(["stale", "retryable", "permanent"] as const)(
    "observes the real %s half-failure without losing strict rejection or recovery policy",
    mode => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-stale-rejoin-"));
      try {
        fs.symlinkSync(path.join(repository, "node_modules"), path.join(root, "node_modules"),
          process.platform === "win32" ? "junction" : "dir");
        const program = path.join(root, "probe.mjs");
        buildSync({ stdin: { contents: probe, resolveDir: repository }, outfile: program,
          bundle: true, platform: "node", format: "esm", target: "node24", packages: "external" });
        const child = spawnSync(process.execPath, ["--unhandled-rejections=strict", program, mode, root], {
          encoding: "utf8", timeout: 25_000,
        });
        const diagnostic = JSON.stringify({ mode, error: child.error?.message, status: child.status,
          signal: child.signal, stdout: child.stdout, stderr: child.stderr }, null, 2);
        // Preserve all stage output in the assertion on RED, including a native fatal exit.
        expect(child.error, diagnostic).toBeUndefined();
        expect(child.status, diagnostic).toBe(0);
        expect(child.stderr, diagnostic).not.toContain('"stage":"native-uncaught"');
        const stages = child.stdout.trim().split("\n").map(line => JSON.parse(line) as {
          probe: string; mode: string; stage: string; shared?: boolean;
        });
        expect(stages.every(item => item.probe === "P2_REJOIN" && item.mode === mode), diagnostic).toBe(true);
        const names = stages.map(item => item.stage);
        for (const name of ["native-ready", "coordinator-awaiting-shared", "retained-incomplete",
          "busy-before-authority", "half-failure-observed", "lease-wake", "retained-rejoin",
          "post-rejoin-turn", "strict-public-rejection", "closed-after-owned-writes", "complete"]) {
          expect(names, diagnostic).toContain(name);
        }
        expect(names.indexOf("busy-before-authority"), diagnostic).toBeLessThan(names.indexOf("half-failure-observed"));
        expect(names.indexOf("half-failure-observed"), diagnostic).toBeLessThan(names.indexOf("retained-rejoin"));
        expect(stages.find(item => item.stage === "retained-rejoin")?.shared, diagnostic).toBe(mode !== "stale");
        if (mode === "retryable") {
          expect(names, diagnostic).toContain("failed-sample-not-absence");
          expect(names, diagnostic).toContain("canonical-recovery-delivered");
          expect(names, diagnostic).not.toContain("fatal-run");
        } else {
          expect(names, diagnostic).toContain("fatal-run");
          expect(names, diagnostic).not.toContain("canonical-recovery-delivered");
        }
      } finally {
        // Only this invocation's mkdtemp root; never repository/session evidence.
        fs.rmSync(root, { recursive: true, force: true });
      }
    }, 40_000,
  );
});
