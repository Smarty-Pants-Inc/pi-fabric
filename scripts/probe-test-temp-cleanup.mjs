#!/usr/bin/env node
// Ordinary `bun run test` lifecycle proof. No mocks or caller cleanup before assertions.
import { spawn } from "node:child_process";
import fs from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const caller = fs.realpathSync(process.env.TMPDIR ?? "");
const stat = fs.statSync(caller);
if (!caller.startsWith("/srv/scratch/paul/") || stat.uid !== process.getuid() || (stat.mode & 0o077) !== 0) {
  throw new Error("Set TMPDIR to a private caller-owned directory under /srv/scratch/paul/");
}
process.umask(0o022);
const evidence = fs.mkdtempSync(join(caller, "temp-proof-"));
const tmpNames = () => fs.readdirSync("/tmp").filter(name => name.startsWith("pi-fabric-vitest-"));
const beforeTmp = new Set(tmpNames());
function census(root) {
  if (!fs.existsSync(root)) return { exists: false, entries: 0, inodes: 0 };
  const inodes = new Set();
  let entries = 0;
  function walk(path) {
    for (const name of fs.readdirSync(path)) {
      const child = join(path, name);
      const info = fs.lstatSync(child);
      entries++;
      inodes.add(`${info.dev}:${info.ino}`);
      if (info.isDirectory()) walk(child);
    }
  }
  walk(root);
  return { exists: true, entries, inodes: inodes.size };
}
// One native observation after child.close; zombies cannot write and are not active workers.
function activeGroup(pgid) {
  const active = [];
  for (const name of fs.readdirSync("/proc").filter(name => /^\d+$/.test(name))) {
    let text;
    try { text = fs.readFileSync(`/proc/${name}/stat`, "utf8"); }
    catch (error) { if (error.code === "ENOENT" || error.code === "ESRCH") continue; throw error; }
    const fields = text.slice(text.lastIndexOf(")") + 1).trim().split(/\s+/);
    if (Number(fields[2]) === pgid && !["Z", "X", "x"].includes(fields[0])) active.push(Number(name));
  }
  return active;
}
const cases = [];
const runs = [];
const caseRoots = [];
function signalGroup(pgid, signal) {
  try { process.kill(-pgid, signal); }
  catch (error) { if (error.code !== "ESRCH") throw error; }
}
function groupOf(pid) {
  const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  return Number(text.slice(text.lastIndexOf(")") + 1).trim().split(/\s+/)[2]);
}
async function launch(name, mode, caseRoot) {
  const output = join(evidence, name);
  fs.mkdirSync(output);
  const config = join(output, "vitest.config.mts");
  fs.writeFileSync(config, `import base from ${JSON.stringify(join(repo, "vitest.config.cts"))};\nexport default {...base,root:${JSON.stringify(repo)},test:{...base.test,projects:undefined,include:[${JSON.stringify(join(repo, "tests/fixtures/test-temp-lifecycle.ts"))}]}};\n`);
  const result = { name, mode, caseRoot, command: ["bun", "run", "test", "--config", config] };
  const logs = [];
  let readyResolve;
  const ready = new Promise(resolveReady => { readyResolve = resolveReady; });
  const watcher = fs.watch(output, (_event, filename) => {
    if (String(filename) !== "ready.json") return;
    try { readyResolve(JSON.parse(fs.readFileSync(join(output, "ready.json"), "utf8"))); }
    catch { /* Readiness is emitted again once the fixture completes the write. */ }
  });
  const env = { ...process.env, TMPDIR: caseRoot, TMP: caseRoot, TEMP: caseRoot, CI: "false",
    TEMPFIX_PROBE_MODE: mode, TEMPFIX_PROBE_EVIDENCE: output };
  // Each command starts from the caller, not this controller's inherited launcher root.
  delete env.PI_TEST_TEMP_RUN_DIR;
  delete env.XDG_RUNTIME_DIR;
  delete env.DBUS_SESSION_BUS_ADDRESS;
  const child = spawn("bun", ["run", "test", "--config", config],
    { cwd: repo, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  child.stdout.on("data", chunk => {
    logs.push(chunk);
    stdout += chunk.toString();
    const match = stdout.match(/TEMPFIX_READY (\{[^\n]+\})\r?\n/);
    if (match) readyResolve(JSON.parse(match[1]));
  });
  child.stderr.on("data", chunk => logs.push(chunk));
  const closed = new Promise((resolveClosed, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => {
      const signals = { SIGINT: 2, SIGTERM: 15, SIGKILL: 9 };
      resolveClosed({ code, signal, status: code ?? 128 + signals[signal] });
    });
  });
  const run = { child, closed, result, output, logs, watcher, terminal: false, pgid: undefined, timer: undefined };
  runs.push(run);
  const deadline = new Promise((_, reject) => {
    run.timer = setTimeout(() => {
      if (run.pgid) signalGroup(run.pgid, "SIGKILL");
      signalGroup(child.pid, "SIGKILL");
      reject(new Error(`Lifecycle proof deadline: ${name}`));
    }, 45_000);
  });
  run.deadline = deadline;
  const marker = await Promise.race([ready, closed.then(exit => { throw new Error(`Exited before readiness: ${JSON.stringify(exit)}`); }), deadline]);
  watcher.close();
  run.pgid = groupOf(marker.pid);
  if (run.pgid === groupOf(process.pid)) throw new Error("Fixture unexpectedly joined the controller's process group");
  result.root = marker.runRoot;
  result.live = census(marker.runRoot);
  if (dirname(marker.runRoot) !== caseRoot || dirname(marker.fixtureDir) !== marker.runRoot) throw new Error("Fixture escaped its caller-owned run root");
  if (result.live.entries < 10) throw new Error("Fixture did not create the real temporary tree");
  return run;
}
async function finish(run, action, expectedStatus, rootMustRemain = false) {
  if (action === "release") fs.writeFileSync(join(run.output, "release.json"), "{}\n");
  else {
    signalGroup(run.child.pid, action); // The ordinary command's group, including its launcher.
    if (action === "SIGKILL") signalGroup(run.pgid, action); // No launcher survives to forward SIGKILL.
  }
  run.result.exit = await Promise.race([run.closed, run.deadline]);
  run.terminal = true;
  clearTimeout(run.timer);
  run.result.activeWorkers = [...new Set([...activeGroup(run.child.pid), ...activeGroup(run.pgid)])];
  if (run.result.activeWorkers.length) throw new Error(`Active writers remain: ${run.result.activeWorkers}`);
  run.result.rootBeforeCallerCleanup = census(run.result.root);
  if (run.result.exit.status !== expectedStatus) throw new Error(`${run.result.name}: expected ${expectedStatus}, got ${JSON.stringify(run.result.exit)}`);
  if (run.result.rootBeforeCallerCleanup.exists !== rootMustRemain) throw new Error(`${run.result.name}: unexpected root lifetime before caller cleanup`);
  run.result.ok = true;
  cases.push(run.result);
  fs.writeFileSync(join(run.output, "vitest.log"), Buffer.concat(run.logs));
}
function newCase(name) {
  const root = fs.mkdtempSync(join(caller, `case-${name}-`));
  caseRoots.push(root);
  return root;
}
let summary;
try {
  for (const [name, mode, action, status] of [
    ["normal", "normal", "release", 0], ["fail", "fail", "release", 1],
    ["sigterm", "normal", "SIGTERM", 143], ["sigint", "normal", "SIGINT", 130],
  ]) {
    await finish(await launch(name, mode, newCase(name)), action, status);
  }
  const shared = newCase("kill-and-restart");
  const markerless = join(shared, "pi-fabric-vitest-markerless");
  fs.mkdirSync(markerless);
  fs.writeFileSync(join(markerless, "sentinel"), "not owned by this harness");
  const killed = await launch("sigkill", "normal", shared);
  await finish(killed, "SIGKILL", 137, true);
  // A second NORMAL ordinary command must sweep the killed run at START, while held at readiness.
  const keeper = await launch("restart-sweeps-stale", "normal", shared);
  const staleAfterRestart = census(killed.result.root);
  if (staleAfterRestart.exists) throw new Error("Second ordinary run did not sweep the SIGKILL stale root");
  const recovered = await launch("restart-preserves-live", "normal", shared);
  const liveDuringRestart = census(keeper.result.root);
  if (!liveDuringRestart.exists || fs.readFileSync(join(markerless, "sentinel"), "utf8") !== "not owned by this harness") {
    throw new Error("Startup sweep touched a live-owner or markerless root");
  }
  await finish(recovered, "release", 0);
  if (!fs.existsSync(keeper.result.root)) throw new Error("Another run's cleanup touched the live owner");
  await finish(keeper, "release", 0);
  const newTmp = tmpNames().filter(name => !beforeTmp.has(name));
  summary = { ok: newTmp.length === 0, evidence, ordinaryCommand: "bun run test --config <small-fixture-config>",
    newTmp, staleAfterRestart, liveDuringRestart, markerlessUntouched: fs.existsSync(markerless), cases };
} finally {
  for (const run of runs) {
    if (!run.terminal) {
      if (run.pgid) signalGroup(run.pgid, "SIGKILL");
      signalGroup(run.child.pid, "SIGKILL");
      await run.closed;
    }
    clearTimeout(run.timer);
    run.watcher.close();
    fs.writeFileSync(join(run.output, "vitest.log"), Buffer.concat(run.logs));
  }
  // Archive evidence first; remove caller-owned case roots ONLY after all native writers are terminal.
  if (summary && runs.every(run => run.terminal && activeGroup(run.child.pid).length === 0 && activeGroup(run.pgid).length === 0)) {
    fs.writeFileSync(join(evidence, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
    for (const root of caseRoots) fs.rmSync(root, { recursive: true, force: true });
  }
}
console.log(JSON.stringify(summary));
process.exitCode = summary?.ok ? 0 : 1;
