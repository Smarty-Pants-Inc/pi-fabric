// A separate process for tests/helpers/process-harness.ts (smarty-dev#8082). It runs the real src code through
// jiti and reports on stdout, one JSON line per event. A test pauses it at a chosen point and then sends it a real
// signal; the pause only stops the process, it does not replace any behavior of the code under test.
//
//   main <meshRoot> <rootId>         a root Main: stdin "publish" runs its ParticipantDirectory publish step;
//                                    "removeActor <id>" sends the ordinary same-root removeActor request
//   resident <residencyRoot> <pauseAt>  the real ResidentHost of the root (its config.json); stdin "arm" enables
//     the one pause, "continue" resumes it, "report" lists the observed calls, "close" closes the host:
//     beforeRemove     after the operator request's acceptance and stop, before ActorManager.remove (no fence yet)
//     beforeCheck      actorTreeDeletionHooks.beforeCheck: fence held, final check not run
//     beforeDelete     actorTreeDeletionHooks.beforeDelete: fence held, final check passed, tree not removed
//     fence-temp, fence-link, none   as for the remover
//     Hooks, the live remove's verifiedRemove of a whole actor tree (Linux) and actor-tree
//     fs.rmSync calls (fallback, whole-root close) record fence state; verifiedRemove also asserts the held fence.
//     Optional `record-tree-deletes` mode skips only whole actor-tree removal for local probes; the default
//     delegates to the real verifier/rmSync, so hosted CI deletes for real.
//   resident <residencyRoot> <pauseAt> record-tree-deletes   the same, except that a whole actor-tree removal (the
//     actor root itself, or an actor-id directory /^[a-f0-9]{32}$/ directly under it) is recorded and NOT performed
//     (#4664: a local recorded run never deletes actor history). Every other fs.rmSync (registry and binding lock
//     directories, markers, request files) runs unchanged. Without the flag the deletes are real (hosted tests).
//   remover <pauseAt> <cli args...>  the real `fabric-actors` CLI (src/actors-cli.ts main) with one pause point:
//     fence-temp       the fence record's temp file is written, the link to the fence path is not made (sync)
//     fence-link       the fence file is linked, its flock is not taken yet (sync)
//     beforeTreeMove   inside the fenced check-plus-delete, after the final check (stdin "continue" resumes)
//     none             no pause
//   generation-exit <root> <generation> before-link|after-link   a host's exit record, paused around its link
//   launcher-error <config> <generation>                        real launcher/child; pauses before its error link
// Synchronous pauses block the thread (Atomics.wait); launcher-error uses an externally created resume file.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
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
const pauseAtLink = (pauseAt, armed = () => true) => {
  const link = fs.linkSync;
  fs.linkSync = (from, to) => {
    const fence = String(to).includes("/main-publication-fences/") && !String(to).endsWith(".tmp") && armed();
    if (fence && pauseAt === "fence-temp") { emit({ event: "paused", at: pauseAt, temp: String(from), fence: String(to) }); blockForever(); }
    const result = link(from, to);
    if (fence && pauseAt === "fence-link") { emit({ event: "paused", at: pauseAt, fence: String(to) }); blockForever(); }
    return result;
  };
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
    } else if (command?.startsWith("removeActor ")) {
      const { ResidentActorClient } = await jiti.import("./src/residency/actor-client.ts");
      try { emit({ event: "removeActor", ok: true, result: await new ResidentActorClient(meshRoot, rootId).removeActor(command.slice(12)) }); }
      catch (error) { emit({ event: "removeActor", ok: false, message: String(error?.message ?? error) }); }
    } else if (command === "close" || command === undefined) {
      await directory.close();
      emit({ event: "closed" });
      process.exit(0);
    }
  }
} else if (role === "remover") {
  const [pauseAt, ...cli] = args;
  const { offlineRemovalHooks } = await jiti.import("./src/actors/remove-offline.ts");
  const { main } = await jiti.import("./src/actors-cli.ts");
  if (pauseAt === "fence-temp" || pauseAt === "fence-link") pauseAtLink(pauseAt);
  else if (pauseAt === "beforeTreeMove") {
    offlineRemovalHooks.beforeTreeMove = async () => { emit({ event: "paused", at: pauseAt }); await nextLine(); };
  } else if (pauseAt !== "none") throw new Error(`Unknown pause point: ${pauseAt}`);
  let out = "", err = "";
  const code = await main(cli, { out: text => { out += text; }, err: text => { err += text; } });
  emit({ event: "exit", code, out, err });
  process.exit(0);
} else if (role === "resident") {
  const [residencyRoot, pauseAt, mode] = args;
  if (mode !== undefined && mode !== "record-tree-deletes") throw new Error(`Unknown resident mode: ${mode}`);
  const recordTreeDeletes = mode === "record-tree-deletes";
  if (!["beforeRemove", "beforeCheck", "beforeDelete", "fence-temp", "fence-link", "none"].includes(pauseAt)) throw new Error(`Unknown pause point: ${pauseAt}`);
  const config = JSON.parse(fs.readFileSync(path.join(residencyRoot, "config.json"), "utf8"));
  const { ResidentHost } = await jiti.import("./src/residency/host.ts");
  const { actorTreeDeletionHooks, mainPublicationFenced } = await jiti.import("./src/topology/main-publication-fence.ts");
  const fenced = () => mainPublicationFenced(config.meshRoot, config.rootId);
  const calls = [];
  let armed = false, resume;
  const take = at => { if (!armed || pauseAt !== at) return false; armed = false; return true; };
  const pause = async (at, detail = {}) => { emit({ event: "paused", at, ...detail }); await new Promise(resolve => { resume = resolve; }); };
  for (const name of ["beforeCheck", "beforeDelete"]) {
    actorTreeDeletionHooks[name] = async () => { calls.push({ kind: "hook", name, fenced: fenced() }); if (take(name)) await pause(name); };
  }
  // Observe tree mutations; optional record mode skips only the actor root or an exact actor-id directory.
  const actorRoot = path.resolve(config.actorRoot);
  const isTree = file => { const resolved = path.resolve(String(file)); return resolved === actorRoot || path.dirname(resolved) === actorRoot; };
  const isWholeTree = file => { const resolved = path.resolve(String(file));
    return resolved === actorRoot || (path.dirname(resolved) === actorRoot && /^[a-f0-9]{32}$/.test(path.basename(resolved))); };
  // smarty-dev#16444: a whole actor tree now leaves by ONE archive rename (performed: it deletes nothing), recorded
  // with the fence state. The source may be the held-parent path (/proc/self/fd/<fd>/./<name>).
  const rename = fs.renameSync;
  fs.renameSync = (source, destination) => {
    let resolved = path.resolve(String(source));
    try {
      resolved = path.join(fs.realpathSync(path.dirname(String(source))), path.basename(String(source)));
      const realRoot = fs.realpathSync(actorRoot); // reported by the lexical actor root, as the other events are
      if (resolved === realRoot || resolved.startsWith(realRoot + path.sep)) resolved = path.join(actorRoot, path.relative(realRoot, resolved));
    } catch { /* as given */ }
    if (isWholeTree(resolved)) calls.push({ kind: "archive", path: resolved, fenced: fenced() });
    return rename(source, destination);
  };
  const rm = fs.rmSync;
  fs.rmSync = (file, ...rest) => {
    if (isTree(file) && fs.existsSync(String(file)) && fs.statSync(String(file)).isDirectory()) {
      calls.push({ kind: "rm", path: path.resolve(String(file)), fenced: fenced() });
      if (recordTreeDeletes && isWholeTree(file)) return undefined; // recorded, not performed
    }
    return rm(file, ...rest);
  };
  // The live remove lazy-imports verifiedRemove; the cached jiti module is shared, so this export override is what it
  // calls. Linux only: off Linux the verifier runs the fallback fs.rmSync above, which already records (no double).
  const verifier = await jiti.import("./src/storage/verified-remove.ts");
  const verifiedRemove = verifier.verifiedRemove;
  verifier.verifiedRemove = (target, judged, options) => {
    if (process.platform !== "linux" || !isWholeTree(target)) return verifiedRemove(target, judged, options);
    const held = fenced();
    calls.push({ kind: "rm", path: path.resolve(String(target)), fenced: held });
    if (!held) throw new Error(`verifiedRemove(${target}) without the held Main publication fence`);
    if (recordTreeDeletes) return { removed: true, bytes: 0 }; // recorded, not performed
    return verifiedRemove(target, judged, options);
  };
  if (pauseAt === "fence-temp" || pauseAt === "fence-link") pauseAtLink(pauseAt, () => take(pauseAt));
  const host = new ResidentHost(config, () => {});
  await host.start();
  const remove = host.actors.remove.bind(host.actors);
  host.actors.remove = async (id, options) => {
    calls.push({ kind: "remove", id, options: options ?? {} });
    if (take("beforeRemove")) await pause("beforeRemove", { id, options: options ?? {} });
    return remove(id, options);
  };
  emit({ event: "ready" });
  for (;;) {
    const command = await nextLine();
    if (command === "arm") { armed = true; emit({ event: "armed" }); }
    else if (command === "continue") { const wake = resume; resume = undefined; wake?.(); }
    else if (command === "report") emit({ event: "report", calls });
    else if (command === "close" || command === undefined) {
      await host.close();
      emit({ event: "closed" });
      process.exit(0);
    }
  }
} else if (role === "launcher-error") {
  const [configPath, generation] = args;
  const link = fs.linkSync;
  fs.linkSync = (from, to) => {
    if (String(to).endsWith(`${generation}.error.json`)) {
      emit({ event: "paused", at: "error-link", generation });
      const resumeFile = path.join(path.dirname(configPath), `resume-${generation}`);
      while (!fs.existsSync(resumeFile)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
    }
    return link(from, to);
  };
  const { supervise } = await jiti.import("./src/residency/launcher.ts");
  process.argv.push("--launch-token", generation);
  await supervise(configPath);
  emit({ event: "done" });
} else if (role === "generation-exit") {
  // A host writing its generation exit record (src/residency/generation-exit.ts), paused around the publishing
  // link: before-link (the complete temp file is written, not linked) or after-link (published).
  const [root, generation, pauseAt] = args;
  const { recordResidentGenerationExit } = await jiti.import("./src/residency/generation-exit.ts");
  const link = fs.linkSync;
  fs.linkSync = (from, to) => {
    const record = String(to).endsWith(".exit.json");
    if (record && pauseAt === "before-link") { emit({ event: "paused", at: pauseAt }); blockForever(); }
    const result = link(from, to);
    if (record && pauseAt === "after-link") { emit({ event: "paused", at: pauseAt }); blockForever(); }
    return result;
  };
  emit({ event: "exit", result: recordResidentGenerationExit(root, { format: 1, generation, writer: "host", reason: "idle-exit", pid: process.pid, at: Date.now() }) });
  process.exit(0);
} else throw new Error(`Unknown role: ${role}`);
