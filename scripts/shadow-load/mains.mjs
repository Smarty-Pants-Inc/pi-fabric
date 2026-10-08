// N real Main participant directories in one process (the candidate's own ParticipantDirectory,
// 5 s heartbeat, host lease files and confirmWritable), each on its own MeshStore. A Main with
// autoReload follows a pin change onto the pinned release the way a Main's self-reload does
// (smarty-dev#2160): at its next turn end (a random jitter), the old runtime quiesces its
// directory for "reload" and closes it, the same process loads the pinned release's code, and the
// new runtime resumes the lineage, starts its directory and publishes ops.fabric.reloaded.
//   node mains.mjs --release DIR --mesh ROOT --plan FILE --list mains|spokeNMains --from I --count N --cwd DIR --out FILE
//     [--slot baseline|candidate --pin FILE --pin-release DIR --reload-jitter-ms 10000]
import fs from 'node:fs';
import { argMap, delay, loadCandidate, readJson, startMain, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const candidate = await loadCandidate(args.release, { directory: true });
const plan = readJson(args.plan);
const from = Number(args.from ?? 0);
const slot = args.slot ?? 'candidate';
const mains = plan[args.list ?? 'mains'].slice(from, from + Number(args.count ?? 1));
const running = [];
const health = {};
let startErrors = 0;
const startWithRetry = async (code, main) => {
  for (let attempt = 0; ; attempt++) {
    try {
      const started = await startMain(code, args.mesh, main, args.cwd);
      // A Main whose first publication hits a busy lock keeps its heartbeat and joins later: its
      // directory's timer is already armed, so it is kept, never replaced by a second directory.
      if (started.startError) {
        startErrors++;
        process.stderr.write(`start ${main.name}: initial publish failed, heartbeat joins later: ${started.startError?.message ?? started.startError}\n`);
      }
      const { startError, ...rest } = started;
      return { ...rest, startError: startError ? String(startError?.message ?? startError).slice(0, 200) : undefined };
    }
    catch (error) {
      // A start that failed before the timer existed (resumeLineage) is retried as a restarted Pi would.
      process.stderr.write(`start ${main.name} attempt ${attempt}: ${error?.message ?? error}\n`);
      if (attempt >= 5) throw error;
      await delay(1_000);
    }
  }
};
for (const main of mains) {
  const { startError, ...started } = await startWithRetry(candidate, main);
  running.push({ main, slot, ...started });
  health[main.id] = { name: main.name, slot, autoReload: Boolean(main.autoReload), maxConfirmAgeMs: 0, stalledSamples: 0, startedAt: Date.now(), reloads: [] };
}
const save = () => writeJson(args.out, { pid: process.pid, ids: mains.map(main => main.id), readyAt, health, startErrors, savedAt: Date.now() });
const readyAt = Date.now();
save();
// The directory's own view of its last committed heartbeat: a Main whose shared write stalls
// (lock outage) shows a growing confirm age here before peers see it lapse.
setInterval(() => {
  const now = Date.now();
  for (const { main, directory, reloading } of running) {
    if (reloading) continue;
    const row = health[main.id];
    const age = now - directory.confirmedAt();
    row.maxConfirmAgeMs = Math.max(row.maxConfirmAgeMs, age);
    if (directory.writeStalled(now)) row.stalledSamples++;
  }
}, 1_000).unref();

// Pin change: every autoReload Main on another slot follows it, at its own next turn end.
let pinnedAt = 0;
let target;
const reload = async (row, pin) => {
  const started = Date.now();
  const entry = { at: started, from: row.slot, to: pin.slot };
  health[row.main.id].reloads.push(entry);
  try {
    // The old runtime's shutdown("reload"): the directory publishes the root as reloading, then closes.
    // As FabricRuntimeState.shutdown("reload") does, a failed quiesce or close does not stop the reload.
    await row.directory.quiesce?.('reload').catch(error => { entry.quiesceError = String(error?.message ?? error).slice(0, 200); });
    await row.directory.close().catch(error => { entry.closeError = String(error?.message ?? error).slice(0, 200); });
    target ??= loadCandidate(args['pin-release'] ?? pin.release, { directory: true });
    const code = await target;
    globalThis.__shadowWrapMeshStore?.(code.MeshStore);
    const { startError, ...next } = await startWithRetry(code, row.main);
    if (startError) entry.startError = startError;
    Object.assign(row, next, { slot: pin.slot });
    entry.ms = Date.now() - started;
    await next.store.publish({ topic: 'ops.fabric.reloaded', from: next.identity,
      data: { sessionId: row.main.sessionId, from: entry.from, to: pin.slot, ms: entry.ms } })
      .catch(error => { entry.publishError = String(error?.message ?? error).slice(0, 200); });
  } catch (error) {
    entry.error = String(error?.message ?? error).slice(0, 300);
    process.stderr.write(`reload ${row.main.name}: ${error?.stack ?? error}\n`);
  } finally {
    row.reloading = false;
    save();
  }
};
if (args.pin) {
  setInterval(() => {
    const pin = readJson(args.pin);
    if (!pin || pin.at === pinnedAt) return;
    pinnedAt = pin.at;
    for (const row of running) {
      if (!row.main.autoReload || row.slot === pin.slot || row.reloading) continue;
      row.reloading = true;
      setTimeout(() => void reload(row, pin), Math.floor(Math.random() * Number(args['reload-jitter-ms'] ?? 10_000)));
    }
  }, 1_000);
}
const timer = setInterval(save, 10_000);
let stopping = false;
const stop = async () => {
  if (stopping) return;
  stopping = true;
  clearInterval(timer);
  save();
  await Promise.allSettled(running.map(({ directory }) => directory.close()));
  process.exit(0);
};
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
fs.writeFileSync(`${args.out}.ready`, String(process.pid));
