// A separate process for tests/helpers/process-harness.ts (smarty-dev#8082). It runs the real src code through
// jiti and reports on stdout, one JSON line per event. A test pauses it at a chosen point and then sends it a real
// signal; the pause only stops the process, it does not replace any behavior of the code under test.
//
//   main <meshRoot> <rootId>         a root Main: stdin "publish" runs its ParticipantDirectory publish step
//   remover <pauseAt> <cli args...>  the real `fabric-actors` CLI (src/actors-cli.ts main) with one pause point:
//     fence-temp       the fence record's temp file is written, the link to the fence path is not made (sync)
//     fence-link       the fence file is linked, its flock is not taken yet (sync)
//     beforeTreeMove   inside the fenced check-plus-delete, after the final check (stdin "continue" resumes)
//     none             no pause
//   host <residencyRoot> <pauseAt>   a real ResidentHost on <residencyRoot>/config.json, the live remover
//     (smarty-dev#8090); pauseAt is fence-temp, fence-link, none, or:
//     afterBindings    the live cleanup deleted the bindings, before the tree delete and its fence (stdin "continue")
//     beforeDelete     under the fence, after the final check (stdin "continue" resumes)
// A synchronous pause blocks the thread (Atomics.wait), so only a signal ends it.
import fs from "node:fs";
import os from "node:os";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";
import { createJiti } from "jiti";

const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const emit = event => fs.writeSync(1, JSON.stringify({ pid: process.pid, ...event }) + "\n");
const lines = createInterface({ input: process.stdin });
const queue = [];
let waiter;
lines.on("line", line => { if (waiter) { const resolve = waiter; waiter = undefined; resolve(line); } else queue.push(line); });
const nextLine = () => queue.length ? Promise.resolve(queue.shift()) : new Promise(resolve => { waiter = resolve; });
const blockForever = () => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0); };
/** fence-temp / fence-link: block the thread in fs.linkSync of the Main publication fence file (any remover). */
const pauseFence = pauseAt => {
  if (pauseAt !== "fence-temp" && pauseAt !== "fence-link") return false;
  const link = fs.linkSync;
  fs.linkSync = (from, to) => {
    const fence = String(to).includes("/main-publication-fences/") && !String(to).endsWith(".tmp");
    if (fence && pauseAt === "fence-temp") { emit({ event: "paused", at: pauseAt, temp: String(from), fence: String(to) }); blockForever(); }
    const result = link(from, to);
    if (fence && pauseAt === "fence-link") { emit({ event: "paused", at: pauseAt, fence: String(to) }); blockForever(); }
    return result;
  };
  return true;
};

const [role, ...args] = process.argv.slice(2);
if (role === "main") {
  const [meshRoot, rootId] = args;
  const { mainDirectory } = await jiti.import("./tests/helpers/dead-root-fixture.ts");
  const { processStartTime } = await jiti.import("./src/residency/process-identity.ts");
  const directory = mainDirectory(meshRoot, rootId, { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid) });
  emit({ event: "ready" });
  for (;;) {
    const command = await nextLine();
    if (command === "publish") {
      try { await directory.refresh(); emit({ event: "publish", ok: true }); }
      catch (error) { emit({ event: "publish", ok: false, code: error?.code, message: String(error?.message ?? error) }); }
    } else if (command === "close" || command === undefined) {
      await directory.close();
      emit({ event: "closed" });
      process.exit(0);
    }
  }
} else if (role === "host") {
  const [residencyRoot, pauseAt] = args;
  const { treeDeleteHooks } = await jiti.import("./src/topology/main-publication-fence.ts");
  const { ResidentHost } = await jiti.import("./src/residency/host.ts");
  if (!pauseFence(pauseAt)) {
    if (pauseAt === "afterBindings") {
      // The step right before the tree delete in ActorManager#finishCleanup, on every release (no hook needed).
      const { ActorBindingStore } = await jiti.import("./src/actors/binding-store.ts");
      const remove = ActorBindingStore.prototype.delete;
      ActorBindingStore.prototype.delete = async function (...values) {
        const result = await remove.apply(this, values);
        emit({ event: "paused", at: pauseAt }); await nextLine();
        return result;
      };
    } else if (pauseAt === "beforeDelete") {
      treeDeleteHooks.beforeDelete = async () => { emit({ event: "paused", at: pauseAt }); await nextLine(); };
    } else if (pauseAt !== "none") throw new Error(`Unknown pause point: ${pauseAt}`);
  }
  const host = new ResidentHost(JSON.parse(fs.readFileSync(`${residencyRoot}/config.json`, "utf8")), () => {});
  await host.start();
  emit({ event: "ready" });
  for (;;) {
    const command = await nextLine();
    if (command === "close" || command === undefined) { await host.close(); emit({ event: "closed" }); process.exit(0); }
  }
} else if (role === "remover") {
  const [pauseAt, ...cli] = args;
  const { offlineRemovalHooks } = await jiti.import("./src/actors/remove-offline.ts");
  const { main } = await jiti.import("./src/actors-cli.ts");
  if (pauseFence(pauseAt)) { /* paused in fs.linkSync */ } else if (pauseAt === "beforeTreeMove") {
    offlineRemovalHooks.beforeTreeMove = async () => { emit({ event: "paused", at: pauseAt }); await nextLine(); };
  } else if (pauseAt !== "none") throw new Error(`Unknown pause point: ${pauseAt}`);
  let out = "", err = "";
  const code = await main(cli, { out: text => { out += text; }, err: text => { err += text; } });
  emit({ event: "exit", code, out, err });
  process.exit(0);
} else throw new Error(`Unknown role: ${role}`);
