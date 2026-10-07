// Load driver: ~3 events/s on the hub (and a trickle on the spoke) through the candidate's
// MeshStore.publish, and durable actor registry saves on every resident host through the
// candidate's ResidentActorClient, as each host's Main would send them.
//   node driver.mjs --release DIR --hub ROOT --spoke ROOT --plan FILE --rate 3 --spoke-rate 0.5 --actor-save-s 30 --out FILE
import { argMap, isLockTimeout, loadCandidate, openStore, readJson, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const candidate = await loadCandidate(args.release, { actorClient: true });
const plan = readJson(args.plan);
const identity = { id: `shadow-driver-${process.pid}`, name: 'shadow-driver', kind: 'agent' };
const stats = { publish: { hub: newCounter(), spoke: newCounter() }, saves: newCounter(), errors: [] };
function newCounter() { return { ok: 0, failed: 0, lockTimeouts: 0, skipped: 0, latencies: [] }; }
const record = (counter, started, error) => {
  if (error) {
    counter.failed++;
    if (isLockTimeout(error)) counter.lockTimeouts++;
    if (stats.errors.length < 30) stats.errors.push({ at: Date.now(), message: String(error?.message ?? error).slice(0, 300) });
  } else counter.ok++;
  counter.latencies.push(Math.round(performance.now() - started));
};

// fleet.work.* is on the bridge allow-list (src/mesh/bridge.ts isBridgedTopic), so these events
// also load the bridges, as fleet work events do. Senders are real participants of their mesh.
const publisher = (name, root, rate, from, to, toEvery = 1) => {
  if (!(rate > 0)) return;
  const store = openStore(candidate, root);
  let seq = 0;
  let inFlight = false;
  setInterval(() => {
    if (inFlight) { stats.publish[name].skipped++; return; }
    inFlight = true;
    const started = performance.now();
    const addressed = to && ++seq % toEvery === 0;
    store.publish({ topic: 'fleet.work.shadow-load', from, ...(addressed ? { to } : {}), data: { seq, at: Date.now(), mesh: name } })
      .then(() => record(stats.publish[name], started), error => record(stats.publish[name], started, error))
      .finally(() => { inFlight = false; });
  }, Math.round(1_000 / rate));
};
const mainIdentity = main => main ? { id: main.id, name: main.name, kind: 'main', sessionId: main.sessionId } : identity;
const hubSender = plan.mains.at(-1), spokeSender = plan.spokeMains[0];
// Every third hub event is addressed to a spoke Main, so it crosses the bridges.
publisher('hub', args.hub, Number(args.rate ?? 3), mainIdentity(hubSender), spokeSender?.id, 3);
publisher('spoke', args.spoke, Number(args['spoke-rate'] ?? 0.5), mainIdentity(spokeSender), hubSender?.id);

// Registry saves: round-robin over every host's load actors (the canary actor is left alone), so
// each actor saves once per --actor-save-s on average. One request in flight per host.
const targets = [];
for (const host of plan.hosts) {
  const client = new candidate.ResidentActorClient(args.hub, host.rootId);
  const caller = { identity: { id: host.rootId, name: host.mainName, kind: 'main', sessionId: host.sessionId }, hostId: host.rootId };
  const state = { client, caller, busy: false };
  for (const id of host.loadActors) targets.push({ id, state, flip: false });
}
if (targets.length) {
  let next = 0;
  setInterval(() => {
    const target = targets[next++ % targets.length];
    if (target.state.busy) { stats.saves.skipped++; return; }
    target.state.busy = true;
    target.flip = !target.flip;
    const started = performance.now();
    target.state.client.setActor({ operation: 'setTools', id: target.id, tools: target.flip ? ['read', 'grep'] : ['read'] }, undefined, target.state.caller)
      .then(() => record(stats.saves, started), error => record(stats.saves, started, error))
      .finally(() => { target.state.busy = false; });
  }, Math.max(20, Math.round(Number(args['actor-save-s'] ?? 30) * 1_000 / targets.length)));
}

const summary = counter => {
  const sorted = [...counter.latencies].sort((a, b) => a - b);
  const at = q => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : null;
  return { ok: counter.ok, failed: counter.failed, lockTimeouts: counter.lockTimeouts, skipped: counter.skipped,
    p50Ms: at(0.5), p99Ms: at(0.99), maxMs: sorted.at(-1) ?? null };
};
const startedAt = Date.now();
const save = () => writeJson(args.out, { pid: process.pid, startedAt, savedAt: Date.now(),
  publish: { hub: summary(stats.publish.hub), spoke: summary(stats.publish.spoke) }, saves: summary(stats.saves), errors: stats.errors });
setInterval(save, 10_000);
const stop = () => { save(); process.exit(0); };
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
