#!/usr/bin/env node
// Real Vitest pass/fail/SIGTERM proof. No mocks; only controller-owned case roots are removed.
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
for (const mode of ["normal", "fail", "signal"]) {
  const output = join(evidence, mode);
  fs.mkdirSync(output);
  const caseRoot = fs.mkdtempSync(join(caller, `case-${mode}-`));
  const config = join(output, "vitest.config.mts");
  fs.writeFileSync(config, `import base from ${JSON.stringify(join(repo, "vitest.config.ts"))};\nexport default {...base,root:${JSON.stringify(repo)},test:{...base.test,projects:undefined,include:[${JSON.stringify(join(repo, "tests/fixtures/test-temp-lifecycle.ts"))}]}};\n`);
  const result = { mode, caseRoot, before: census(caseRoot) };
  let watcher;
  let timer;
  let child;
  let terminal = false;
  const logs = [];
  try {
    let readyResolve;
    const ready = new Promise(resolveReady => { readyResolve = resolveReady; });
    watcher = fs.watch(output, (_event, name) => {
      if (String(name) !== "ready.json") return;
      try { readyResolve(JSON.parse(fs.readFileSync(join(output, "ready.json"), "utf8"))); }
      catch { /* Another file event follows the completed write. */ }
    });
    const env = { ...process.env, TMPDIR: caseRoot, TMP: caseRoot, TEMP: caseRoot, CI: "false",
      TEMPFIX_PROBE_MODE: mode, TEMPFIX_PROBE_EVIDENCE: output };
    delete env.XDG_RUNTIME_DIR;
    delete env.DBUS_SESSION_BUS_ADDRESS;
    child = spawn(process.execPath, [join(repo, "node_modules/vitest/vitest.mjs"), "run", "--config", config],
      { cwd: repo, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    child.stdout.on("data", chunk => logs.push(chunk));
    child.stderr.on("data", chunk => logs.push(chunk));
    const closed = new Promise((resolveClosed, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolveClosed({ code, signal }));
    });
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) {
          try { process.kill(-child.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
        }
        reject(new Error(`Lifecycle proof deadline: ${mode}`));
      }, 45_000);
    });
    const marker = await Promise.race([ready, closed.then(exit => { throw new Error(`Exited before readiness: ${JSON.stringify(exit)}`); }), deadline]);
    result.root = marker.runRoot;
    result.live = census(marker.runRoot);
    if (dirname(marker.runRoot) !== caseRoot || dirname(marker.fixtureDir) !== marker.runRoot) throw new Error("Fixture escaped its caller-owned run root");
    if (mode === "signal") process.kill(-child.pid, "SIGTERM");
    else fs.writeFileSync(join(output, "release.json"), "{}\n");
    result.exit = await Promise.race([closed, deadline]);
    terminal = true;
    result.activeWorkers = activeGroup(child.pid);
    if (result.activeWorkers.length) throw new Error(`Active writers remain: ${result.activeWorkers}`);
    result.rootBeforeCallerCleanup = census(marker.runRoot);
    result.caseBeforeCallerCleanup = census(caseRoot);
    if (mode !== "signal" && result.rootBeforeCallerCleanup.exists) throw new Error("Normal/failing run skipped cleanup");
    if (mode === "normal" && result.exit.code !== 0) throw new Error("Normal suite failed");
    if (mode === "fail" && result.exit.code !== 1) throw new Error("Intentional failing suite did not fail");
    if (mode === "signal" && result.exit.code !== 143 && result.exit.signal !== "SIGTERM") throw new Error("Run was not terminated by SIGTERM");
    result.ok = result.live.entries >= 10;
  } finally {
    clearTimeout(timer);
    watcher?.close();
    fs.writeFileSync(join(output, "vitest.log"), Buffer.concat(logs));
    // This is the single caller cleanup place, after native writers have stopped.
    if (terminal && activeGroup(child.pid).length === 0) fs.rmSync(caseRoot, { recursive: true, force: true });
  }
  result.after = census(caseRoot);
  result.ok &&= !result.after.exists && result.after.entries === 0 && result.after.inodes === 0;
  cases.push(result);
}
const newTmp = tmpNames().filter(name => !beforeTmp.has(name));
const summary = { ok: cases.every(entry => entry.ok) && newTmp.length === 0, evidence, newTmp, cases };
fs.writeFileSync(join(evidence, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary));
process.exitCode = summary.ok ? 0 : 1;
