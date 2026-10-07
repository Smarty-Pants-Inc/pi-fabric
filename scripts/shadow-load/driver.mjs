// Load driver: plain events on the hub and every spoke, a github-factory-like forwarder (bursty,
// ~60 events/min on the hub), review-actor-style wakes, and durable actor registry saves on every
// resident host, all through the candidate's own MeshStore and ResidentActorClient, sent as the
// hub's and spokes' Mains would send them.
//   node driver.mjs --release DIR --hub ROOT --spokes ROOT,ROOT,... --plan FILE --rate 1 --spoke-rate 0.5
//     --forward-per-min 60 --forward-burst 6 --wake-s 5 --actor-save-s 30 --out FILE
import { argMap, isLockTimeout, loadCandidate, openStore, readJson, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const candidate = await loadCandidate(args.release, { actorClient: true });
const plan = readJson(args.plan);
const identity = { id: `shadow-driver-${process.pid}`, name: 'shadow-driver', kind: 'agent' };
const spokes = String(args.spokes ?? '').split(',').filter(Boolean);
const stats = { publish: { hub: newCounter(), spoke: newCounter() }, forward: { ...newCounter(), bursts: 0, maxBurst: 0 }, wakes: newCounter(), saves: newCounter(), errors: [] };
function newCounter() { return { ok: 0, failed: 0, lockTimeouts: 0, skipped: 0, latencies: [] }; }
const record = (counter, started, error) => {
  if (error) {
    counter.failed++;
    if (isLockTimeout(error)) counter.lockTimeouts++;
    if (stats.errors.length < 30) stats.errors.push({ at: Date.now(), message: String(error?.message ?? error).slice(0, 300) });
  } else counter.ok++;
  if (counter.latencies.length < 200_000) counter.latencies.push(Math.round(performance.now() - started));
};
const mainIdentity = main => main ? { id: main.id, name: main.name, kind: 'main', sessionId: main.sessionId } : identity;
const spokeLists = spokes.map((_, index) => plan[`spoke${index + 1}Mains`] ?? []);

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
const hubSender = plan.mains.at(-1);
// Every third hub event is addressed to a spoke Main, so it crosses the bridges.
publisher('hub', args.hub, Number(args.rate ?? 1), mainIdentity(hubSender), plan.spokeMains?.[0]?.id, 3);
spokes.forEach((root, index) => publisher('spoke', root, Number(args['spoke-rate'] ?? 0.5), mainIdentity(spokeLists[index][0]), hubSender?.id));

// The github-factory-like forwarder: webhook deliveries arrive in bursts (a push fans out into
// check runs, statuses and review requests), each forwarded as its own publish, ~60/min on average.
const forwardPerMin = Number(args['forward-per-min'] ?? 60);
const forwardBurst = Math.max(1, Number(args['forward-burst'] ?? 6));
if (forwardPerMin > 0) {
  const store = openStore(candidate, args.hub);
  const forwarder = mainIdentity(plan.mains[1] ?? plan.mains[0]);
  const kinds = ['pull_request', 'check_run', 'check_suite', 'issue_comment', 'pull_request_review', 'push', 'workflow_run', 'status'];
  const body = 'x'.repeat(1_200);
  let seq = 0;
  let busy = false;
  setInterval(() => {
    if (Math.random() >= forwardPerMin / 60 / forwardBurst) return;
    if (busy) { stats.forward.skipped++; return; }
    busy = true;
    const size = 1 + Math.floor(Math.random() * (2 * forwardBurst - 1));
    stats.forward.bursts++;
    stats.forward.maxBurst = Math.max(stats.forward.maxBurst, size);
    void (async () => {
      for (let i = 0; i < size; i++) {
        const kind = kinds[seq % kinds.length];
        const spokeList = spokeLists[seq % Math.max(1, spokeLists.length)] ?? [];
        const to = seq % 3 === 0 ? spokeList[seq % Math.max(1, spokeList.length)]?.id : undefined;
        seq++;
        const started = performance.now();
        await store.publish({ topic: `fleet.work.github.${kind}`, from: forwarder, ...(to ? { to } : {}),
          data: { delivery: `${Date.now()}-${seq}`, repo: `Smarty-Pants-Inc/repo-${seq % 9}`, number: 400 + (seq % 300), action: 'synchronize', body } })
          .then(() => record(stats.forward, started), error => record(stats.forward, started, error));
      }
    })().finally(() => { busy = false; });
  }, 1_000);
}

// Registry saves and review-actor-style wakes go to each host's load actors (the canary actor is
// left alone). One request in flight per host.
const targets = [];
const hostStates = [];
for (const host of plan.hosts) {
  const client = new candidate.ResidentActorClient(args.hub, host.rootId);
  const caller = { identity: { id: host.rootId, name: host.mainName, kind: 'main', sessionId: host.sessionId }, hostId: host.rootId };
  const state = { client, caller, busy: false, host };
  hostStates.push(state);
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

  // A review wake: the owning Main publishes a review request addressed to the actor on the hub,
  // then polls the actor's status through its resident host, as the review flow does.
  const wakeS = Number(args['wake-s'] ?? 5);
  if (wakeS > 0) {
    const wakeStore = openStore(candidate, args.hub);
    let wakeNext = 0;
    setInterval(() => {
      const target = targets[(wakeNext++ * 7) % targets.length];
      const started = performance.now();
      wakeStore.publish({ topic: 'fleet.work.review.requested', from: target.state.caller.identity, to: target.id,
        data: { pr: 500 + (wakeNext % 200), repo: 'Smarty-Pants-Inc/pi-fabric', reason: 'review_requested' } })
        .then(() => target.state.client.actorStatus(target.id))
        .then(() => record(stats.wakes, started), error => record(stats.wakes, started, error));
    }, Math.round(wakeS * 1_000));
  }
}

const summary = counter => {
  const sorted = [...counter.latencies].sort((a, b) => a - b);
  const at = q => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : null;
  const { latencies, ...rest } = counter;
  return { ...rest, p50Ms: at(0.5), p99Ms: at(0.99), maxMs: sorted.at(-1) ?? null };
};
const startedAt = Date.now();
const save = () => writeJson(args.out, { pid: process.pid, startedAt, savedAt: Date.now(),
  publish: { hub: summary(stats.publish.hub), spoke: summary(stats.publish.spoke) }, forward: summary(stats.forward), wakes: summary(stats.wakes),
  saves: summary(stats.saves), errors: stats.errors });
setInterval(save, 10_000);
const stop = () => { save(); process.exit(0); };
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
