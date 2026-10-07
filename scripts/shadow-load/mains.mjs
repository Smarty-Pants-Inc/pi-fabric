// N real Main participant directories in one process (the candidate's own ParticipantDirectory,
// 5 s heartbeat, host lease files and confirmWritable), each on its own MeshStore.
//   node mains.mjs --release DIR --mesh ROOT --plan FILE --list mains|spokeMains --from I --count N --cwd DIR --out FILE
import fs from 'node:fs';
import { argMap, delay, loadCandidate, readJson, startMain, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const candidate = await loadCandidate(args.release, { directory: true });
const plan = readJson(args.plan);
const from = Number(args.from ?? 0);
const mains = plan[args.list ?? 'mains'].slice(from, from + Number(args.count ?? 1));
const running = [];
const health = {};
for (const main of mains) {
  for (let attempt = 0; ; attempt++) {
    try { running.push({ main, ...await startMain(candidate, args.mesh, main, args.cwd) }); break; }
    catch (error) {
      // A Main whose first publication hits a busy lock keeps its heartbeat and joins later; a
      // start that failed before the timer existed is retried as a restarted Pi would.
      process.stderr.write(`start ${main.name} attempt ${attempt}: ${error?.message ?? error}\n`);
      if (attempt >= 5) throw error;
      await delay(1_000);
    }
  }
  health[main.id] = { name: main.name, maxConfirmAgeMs: 0, stalledSamples: 0, startedAt: Date.now() };
}
const save = () => writeJson(args.out, { pid: process.pid, ids: mains.map(main => main.id), readyAt, health, savedAt: Date.now() });
const readyAt = Date.now();
save();
// The directory's own view of its last committed heartbeat: a Main whose shared write stalls
// (lock outage) shows a growing confirm age here before peers see it lapse.
setInterval(() => {
  const now = Date.now();
  for (const { main, directory } of running) {
    const row = health[main.id];
    const age = now - directory.confirmedAt();
    row.maxConfirmAgeMs = Math.max(row.maxConfirmAgeMs, age);
    if (directory.writeStalled(now)) row.stalledSamples++;
  }
}, 1_000).unref();
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
