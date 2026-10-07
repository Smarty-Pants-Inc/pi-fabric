// One round of the release-candidate canary, checks 3-5, under the full load:
//   3 mesh publish + read back (and arrival on the spoke through the bridges, informational);
//   4 a durable actor round trip through the resident host (setTools, then actorStatus);
//   5 a forced participant preparation failure that recovers: the actor registry changes during
//     publication preparation (a real ActorRegistryStore write between the fence's preparation and
//     its validation under custody) until the whole refresh fails, then the next 5 s heartbeat
//     publishes; a short forced burst must recover within the same refresh.
//   node canary.mjs --release DIR --hub ROOT --spoke ROOT --plan FILE --scratch DIR --round N --out FILE
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { argMap, delay, loadCandidate, openReader, openStore, readJson, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const candidate = await loadCandidate(args.release, { directory: true, registry: true, actorClient: true });
const plan = readJson(args.plan);
const round = Number(args.round ?? 0);
const result = { round, startedAt: Date.now(), checks: {} };
// The resident client polls with unref'd timers: keep this round alive until it has an answer.
const keepAlive = setInterval(() => undefined, 1_000);
const timed = async (name, run) => {
  const started = performance.now();
  try { result.checks[name] = { pass: true, ...await run(), ms: Math.round(performance.now() - started) }; }
  catch (error) { result.checks[name] = { pass: false, error: String(error?.stack ?? error).slice(0, 600), ms: Math.round(performance.now() - started) }; }
};
const waitFor = async (what, deadlineMs, probe) => {
  const deadline = Date.now() + deadlineMs;
  for (;;) {
    const value = probe();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`${what}: not within ${deadlineMs} ms`);
    await delay(50);
  }
};

await timed('3-mesh-publish-read', async () => {
  const nonce = randomUUID();
  // A hub Main's identity on an allow-listed topic, so the bridges also carry it to the spoke.
  const main = plan.mains[0];
  const from = { id: main.id, name: main.name, kind: 'main', sessionId: main.sessionId };
  const started = Date.now();
  const event = await openStore(candidate, args.hub).publish({ topic: 'fleet.work.shadow-canary', from, to: plan.spokeMains[0].id, data: { nonce, round } });
  const reader = openStore(candidate, args.hub);
  await waitFor('hub read', 10_000, () => reader.read({ topic: 'fleet.work.shadow-canary', limit: 50 }).find(item => item.data?.nonce === nonce));
  const readMs = Date.now() - started;
  let spokeMs = null;
  try {
    const spoke = openStore(candidate, args.spoke);
    await waitFor('spoke read', 10_000, () => spoke.read({ topic: 'fleet.work.shadow-canary', limit: 50 }).find(item => item.data?.nonce === nonce));
    spokeMs = Date.now() - started;
  } catch { /* informational: bridges may filter topics */ }
  return { seq: event.seq ?? event.id, readMs, spokeMs };
});

await timed('4-durable-actor-round-trip', async () => {
  const host = plan.hosts[round % plan.hosts.length];
  const client = new candidate.ResidentActorClient(args.hub, host.rootId);
  const caller = { identity: { id: host.rootId, name: host.mainName, kind: 'main', sessionId: host.sessionId }, hostId: host.rootId };
  const before = await client.actorStatus(host.canaryActor);
  const tools = JSON.stringify(before.tools ?? []) === JSON.stringify(['read', 'ls']) ? ['read', 'find'] : ['read', 'ls'];
  const set = await client.setActor({ operation: 'setTools', id: host.canaryActor, tools }, undefined, caller);
  const after = await client.actorStatus(host.canaryActor);
  if (JSON.stringify(set.tools) !== JSON.stringify(tools) || JSON.stringify(after.tools) !== JSON.stringify(tools)) {
    throw new Error(`tools did not round-trip: set ${JSON.stringify(set.tools)}, read ${JSON.stringify(after.tools)}, want ${JSON.stringify(tools)}`);
  }
  return { host: host.mainName, actor: host.canaryActor, status: after.status, tools };
});

await timed('5-forced-preparation-failure-recovers', async () => {
  const scratch = fs.mkdtempSync(path.join(args.scratch, `canary-${round}-`));
  const registry = new candidate.ActorRegistryStore(path.join(scratch, 'actors'));
  registry.write([]);
  const sessionId = randomUUID();
  const id = `session:${sessionId}`;
  const identity = { id, name: `shadow-canary-${round}`, kind: 'main', sessionId };
  let forced = 0, mutations = 0, flip = false;
  const directory = new candidate.ParticipantDirectory(openStore(candidate, args.hub, { backgroundReadCacheMs: 5_000 }), {
    enabled: true, hostId: id, rootId: id, identity, reapDeadHosts: false,
    // The resident host's fence: registry generations captured before selection, checked under custody.
    preparePublicationFence: () => {
      const generation = registry.fingerprint();
      return () => registry.fingerprint() === generation;
    },
  });
  const startedAt = Date.now();
  directory.registerSource(() => {
    if (forced > 0) {
      forced--;
      mutations++;
      registry.write([{ id: `canary-${mutations}`, rootId: id, residency: 'durable', status: 'idle', name: `c${mutations}` }]);
    }
    flip = !flip;
    return [directory.root({ id, cwd: args.scratch, sessionId, status: flip ? 'running' : 'idle', startedAt, updatedAt: Date.now(), pendingMessages: 0 },
      true, identity.name, { role: undefined })];
  });
  const reader = openReader(candidate, args.hub, `shadow-canary-reader:${process.pid}`);
  const listed = () => reader.list({ scope: 'project', fresh: true }).find(info => info.id === id && !info.stale);
  try {
    await directory.resumeLineage();
    // (a) More forced changes than the directory's preparation retries: the first publication fails.
    forced = 8;
    let startError;
    try { await directory.start(); } catch (error) { startError = error; }
    if (!/changed during participant preparation/.test(String(startError?.message))) {
      throw new Error(`forced failure not observed: ${startError ? startError.message : 'start() succeeded'} (forced left ${forced}, mutations ${mutations})`);
    }
    const failedAt = Date.now();
    if (listed()) throw new Error('record published despite a failed preparation');
    forced = 0;
    // The heartbeat timer, not this canary, must recover it.
    await waitFor('recovery by heartbeat', 15_000, listed);
    const recoveredMs = Date.now() - failedAt;
    // (b) Fewer forced changes than the retries: the same refresh recovers.
    const before = mutations;
    forced = 2;
    await directory.refresh();
    if (mutations - before !== 2 || forced !== 0) throw new Error(`short burst not consumed: ${mutations - before} mutations`);
    if (!listed()) throw new Error('record missing after an in-refresh recovery');
    return { failure: String(startError.message).slice(0, 120), forcedMutations: mutations, recoveredByHeartbeatMs: recoveredMs, inRefreshRecovery: true };
  } finally {
    await directory.close().catch(() => undefined);
  }
});

result.pass = Object.values(result.checks).every(check => check.pass);
result.finishedAt = Date.now();
writeJson(args.out, result);
clearInterval(keepAlive);
process.exit(0);
