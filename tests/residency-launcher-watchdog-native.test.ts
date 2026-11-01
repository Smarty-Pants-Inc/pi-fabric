import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { launchLog, same } from "./helpers/owned-processes.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const live = (pid: number) => {
  try { const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8"); return text.slice(text.lastIndexOf(")") + 2).split(" ")[0] !== "Z"; }
  catch { return false; }
};
const until = async (condition: () => boolean, ms = 15_000) => {
  const end = Date.now() + ms;
  while (!condition()) { if (Date.now() > end) throw new Error("Compiled watchdog probe deadline"); await sleep(20); }
};
const fixture = (mode: "tree" | "renew" | "evidence" | "orphan-stale" | "orphan-healthy" | "orphan-owner-loss", reparented = false) => {
  const orphan = mode.startsWith("orphan-");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-launcher-wedge-"));
  const owned = launchLog(root);
  const hostPath = path.join(root, "host.mjs");
  const config = { cwd: root, rootId: orphan ? "session:aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" : "fixture-root",
    sessionId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", meshRoot: path.join(root, "mesh"), piBinary: hostPath, actorRoot: path.join(root, "actors"),
    residencyRoot: root, watchdog: { enabled: mode !== "orphan-owner-loss", intervalMs: 50, stallMs: 100, coldStartMs: 0 } };
  const configPath = path.join(root, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  if (orphan) {
    fs.mkdirSync(config.actorRoot);
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ format: 1,
      actors: [{ id: "fixture-durable-actor", rootId: config.rootId, residency: "durable", status: "running" }] }));
  }
  fs.mkdirSync(path.join(root, "runs")); fs.mkdirSync(path.join(root, "runs", "one"));
  fs.writeFileSync(path.join(root, "helper.mjs"), `import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(path.join(root, "helper.json"))},JSON.stringify({pid:process.pid}));
let ticks=0;setInterval(()=>{fs.writeFileSync(${JSON.stringify(path.join(root, "helper-heartbeat.tmp"))},String(++ticks));fs.renameSync(${JSON.stringify(path.join(root, "helper-heartbeat.tmp"))},${JSON.stringify(path.join(root, "helper-heartbeat"))});if(fs.existsSync(${JSON.stringify(path.join(root, "release-helper"))}))process.exit(0);},20);
process.on('SIGTERM',()=>{fs.writeFileSync(${JSON.stringify(path.join(root, "helper-stopped.json"))},JSON.stringify({pid:process.pid,signal:'SIGTERM'}));process.exit(0);});`);
  fs.writeFileSync(path.join(root, "intermediate.mjs"), `import {spawn} from 'node:child_process';import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(path.join(root, "intermediate.json"))},JSON.stringify({pid:process.pid}));
const child=spawn(process.execPath,[${JSON.stringify(path.join(root, "helper.mjs"))}],{stdio:'ignore'});child.unref();`);
  fs.writeFileSync(hostPath, `import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {spawn} from 'node:child_process';
const config=JSON.parse(fs.readFileSync(process.env.PI_FABRIC_RESIDENT_CONFIG,'utf8'));
const root=config.residencyRoot;
fs.appendFileSync(path.join(root,'host-launches.jsonl'),JSON.stringify({pid:process.pid,config,args:process.argv.slice(2)})+'\\n');
const stat=fs.readFileSync('/proc/'+process.pid+'/stat','utf8');
const token=process.env.PI_FABRIC_RESIDENT_GENERATION;
fs.writeFileSync(path.join(root,'owner.json'),JSON.stringify({format:1,hostId:'fixture-host',token,pid:process.pid,startedAt:Date.now(),readyAt:Date.now(),maintenanceReady:1,processStartTime:stat.slice(stat.lastIndexOf(')')+2).split(' ')[19]}));
const dir=path.join(config.meshRoot,'host-leases');fs.mkdirSync(dir,{recursive:true});
const file=path.join(dir,createHash('sha256').update('fixture-host').digest('hex').slice(0,32)+'.json');
const renew=()=>{const now=Date.now();fs.writeFileSync(file+'.tmp',JSON.stringify({format:1,id:'fixture-host',rootId:config.rootId,identityId:'fixture-host',updatedAt:now,expiresAt:now+1000}));fs.renameSync(file+'.tmp',file);};
renew();let timer=setInterval(renew,20);${mode === "orphan-healthy" || mode === "orphan-owner-loss" ? "" : "setTimeout(()=>clearInterval(timer),75);"}
${mode === "tree" || reparented ? `const intermediate=spawn(process.execPath,[${JSON.stringify(path.join(root, "intermediate.mjs"))}],{stdio:'ignore'});intermediate.unref();` : orphan ? `spawn(process.execPath,[${JSON.stringify(path.join(root, "helper.mjs"))}],{stdio:'ignore'});` : ""}
${mode === "renew" ? "process.on('SIGUSR2',()=>{renew();timer=setInterval(renew,20);});" : ""}
process.on('SIGTERM',()=>{fs.appendFileSync(path.join(root,'terms.jsonl'),JSON.stringify({pid:process.pid})+'\\n');process.exit(0);});
${mode === "orphan-owner-loss" ? "process.stdin.on('end',()=>fs.writeFileSync(path.join(root,'ignored-eof.json'),JSON.stringify({pid:process.pid})));process.stdin.resume();" : ""}
fs.writeFileSync(path.join(root,'maintenance-ready.json'),JSON.stringify({token,readyAt:Date.now()}));
// The actual host sends readiness over its owned IPC channel after lease startup;
// watched owner-file writes alone deliberately do not settle launcher startup.
if(process.connected&&process.send)process.send({event:'resident-ready',root,token},()=>{});
console.error('fixture child started '+process.pid);setInterval(()=>{},1000);`);
  const mainPath = path.join(root, "main.mjs");
  if (orphan) fs.writeFileSync(mainPath, `import fs from 'node:fs';import {spawn} from 'node:child_process';
const record=event=>fs.appendFileSync(${JSON.stringify(path.join(root, "main-events.jsonl"))},JSON.stringify(event)+'\\n');
const fd=fs.openSync(${JSON.stringify(path.join(root, "launcher-stderr.log"))},'a');
const launcher=spawn(process.execPath,[${JSON.stringify(launcherPath)},'--config',${JSON.stringify(configPath)}],{detached:true,stdio:['ignore','ignore',fd,'ipc']});fs.closeSync(fd);
record({event:'main-started',pid:process.pid,launcherPid:launcher.pid});
launcher.on('error',error=>{record({event:'launcher-error',error:String(error)});process.exit(2);});
launcher.on('message',message=>{record(message);if(message?.event==='resident-ready'&&message.root===${JSON.stringify(root)}){launcher.disconnect();launcher.unref();process.exit(0);}});
setTimeout(()=>{record({event:'main-readiness-timeout'});process.exit(3);},10000);`);
  // Real CLI in both cases. In orphan modes the direct child is the originating
  // Main, whose native exit is joined before checking its detached resident chain.
  const launcher = spawn(process.execPath, orphan ? [mainPath] : [launcherPath, "--config", configPath], {
    env: { ...process.env, ...owned.env, ...(orphan ? { PI_FABRIC_MESH_BACKGROUND_GIVEUP_MS: "200" } : {}) }, stdio: "ignore",
  });
  const lifetime = watchResidentChild(launcher); launcher.on("error", () => {});
  const events = () => {
    try { return fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line)); }
    catch { return []; }
  };
  const launches = () => {
    try { return fs.readFileSync(path.join(root, "host-launches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)); }
    catch { return []; }
  };
  const close = async () => {
    // Preserve synthetic startup/owner/IPC evidence before removing the fixture,
    // including when the originating Main never receives readiness.
    if (orphan) {
      const files = fs.readdirSync(root).filter(name => /\.(?:log|json|jsonl)$/.test(name));
      const generations = path.join(root, "generations");
      if (fs.existsSync(generations)) files.push(...fs.readdirSync(generations).map(name => path.join("generations", name)));
      const leases = path.join(root, "mesh", "host-leases");
      if (fs.existsSync(leases)) files.push(...fs.readdirSync(leases).filter(name => name.endsWith(".json")).map(name => path.join("mesh", "host-leases", name)));
      console.error("native detached fixture", JSON.stringify({ mode, root,
        files: Object.fromEntries(files.map(name => [name, fs.readFileSync(path.join(root, name), "utf8").slice(-12_000)])),
        processes: owned.owned().map(record => ({ ...record, live: same(record) && live(record.pid) })),
      }));
    }
    fs.chmodSync(path.join(root, "wedges"), 0o700);
    if (!lifetime.exited) launcher.kill("SIGTERM");
    // Kill only fixture-recorded, birth-validated PIDs. No group/pattern kills.
    for (const record of owned.owned()) if (same(record) && live(record.pid)) {
      try { process.kill(record.pid, "SIGKILL"); } catch { /* fixture exited */ }
    }
    await lifetime.exit;
    await until(() => owned.owned().every(record => !same(record) || !live(record.pid)));
    console.error("native fixture cleanup", JSON.stringify({ mode, root,
      processes: owned.owned().map(record => ({ ...record, live: same(record) && live(record.pid) })),
    }));
    // Retain the native session traces, including RED evidence, after owned cleanup.
  };
  return { root, launcher, lifetime, events, launches, close };
};

describe.skipIf(process.platform !== "linux")("compiled residency launcher CLI watchdog", () => {
  // Keep: #18131 is a native detached-lifetime failure; Main/PPID loss alone
  // must not kill durable work. Fixtures publish the real owner/lease contract,
  // and an actual actor subprocess proves continued work or owned cleanup.
  it("exits the stale resident, actor and leftover launcher after originating Main's native exit", async () => {
    expect(fs.existsSync(launcherPath), "fresh build required").toBe(true);
    const f = fixture("orphan-stale");
    try {
      expect(await f.lifetime.exit).toEqual({ code: 0, signal: null }); // Native Main receipt, not PPID inference.
      const launcherPid = f.events().find(row => row.event === "launcher-started").pid;
      await until(() => fs.existsSync(path.join(f.root, "helper.json")));
      const actor = JSON.parse(fs.readFileSync(path.join(f.root, "helper.json"), "utf8"));
      const owner = JSON.parse(fs.readFileSync(path.join(f.root, "owner.json"), "utf8"));
      await until(() => !live(launcherPid));
      expect(live(owner.pid)).toBe(false); expect(live(actor.pid)).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "helper-stopped.json"), "utf8")))
        .toEqual({ pid: actor.pid, signal: "SIGTERM" });
      expect(f.events().filter(row => row.event === "launcher-exit")).toEqual([expect.objectContaining({
        reason: "resident-watchdog-giveup", nativeExit: { code: 0, signal: null }, custody: "retained",
      })]);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "generations", `${owner.token}.exit.json`), "utf8")))
        .toMatchObject({ generation: owner.token, writer: "launcher", pid: owner.pid, code: 0, signal: null });
      expect(fs.existsSync(path.join(f.root, "watchdog-custody.json"))).toBe(true);
      expect(f.launches()).toHaveLength(1);
    } finally { await f.close(); }
  }, 25_000);

  it("keeps the detached durable actor working and resident lease renewing after Main natively exits", async () => {
    expect(fs.existsSync(launcherPath), "fresh build required").toBe(true);
    const f = fixture("orphan-healthy");
    try {
      expect(await f.lifetime.exit).toEqual({ code: 0, signal: null });
      const launcherPid = f.events().find(row => row.event === "launcher-started").pid;
      await until(() => fs.existsSync(path.join(f.root, "helper-heartbeat")));
      const actor = JSON.parse(fs.readFileSync(path.join(f.root, "helper.json"), "utf8"));
      const ticks = Number(fs.readFileSync(path.join(f.root, "helper-heartbeat"), "utf8"));
      const leaseDir = path.join(f.root, "mesh", "host-leases");
      const leaseFile = path.join(leaseDir, fs.readdirSync(leaseDir).find(name => name.endsWith(".json"))!);
      const lease = () => JSON.parse(fs.readFileSync(leaseFile, "utf8"));
      const before = lease().updatedAt;
      await until(() => lease().updatedAt >= before + 1000); // Five shortened give-up budgets AFTER Main exit.
      expect(Number(fs.readFileSync(path.join(f.root, "helper-heartbeat"), "utf8"))).toBeGreaterThan(ticks);
      expect(live(launcherPid)).toBe(true); expect(live(f.launches()[0].pid)).toBe(true); expect(live(actor.pid)).toBe(true);
      expect(fs.existsSync(path.join(f.root, "watchdog-custody.json"))).toBe(false);
      expect(f.events().some(row => row.event === "launcher-exit" || row.event === "watchdog-stopping")).toBe(false);
    } finally { await f.close(); }
  }, 20_000);

  it("bounds a duplicate child ignoring EOF without a watchdog, joining native exit and leaving successor files untouched", async () => {
    const f = fixture("orphan-owner-loss");
    try {
      expect(await f.lifetime.exit).toEqual({ code: 0, signal: null });
      const launcherPid = f.events().find(row => row.event === "launcher-started").pid;
      await until(() => fs.existsSync(path.join(f.root, "helper.json")));
      const actor = JSON.parse(fs.readFileSync(path.join(f.root, "helper.json"), "utf8"));
      const ownerFile = path.join(f.root, "owner.json");
      const old = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
      const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
      const successor = JSON.stringify({ ...old, pid: process.pid, token: "live-successor",
        processStartTime: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] });
      fs.writeFileSync(ownerFile, successor);
      fs.writeFileSync(path.join(f.root, "error.json"), "successor diagnostic");
      const beforeLog = fs.readFileSync(path.join(f.root, "launcher.log"), "utf8");
      await until(() => !live(launcherPid));
      expect(live(old.pid)).toBe(false); expect(live(actor.pid)).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "ignored-eof.json"), "utf8"))).toEqual({ pid: old.pid });
      const failure = JSON.parse(fs.readFileSync(path.join(f.root, "generations", `${old.token}.error.json`), "utf8"));
      expect(JSON.parse(failure.error)).toMatchObject({ event: "launcher-exit", reason: "resident-owner-giveup",
        pid: old.pid, nativeExit: { code: 0, signal: null }, giveupMs: 200 });
      expect(fs.readFileSync(path.join(f.root, "launcher-stderr.log"), "utf8")).toContain('"reason":"resident-owner-giveup"');
      expect(fs.readFileSync(ownerFile, "utf8")).toBe(successor);
      expect(fs.readFileSync(path.join(f.root, "error.json"), "utf8")).toBe("successor diagnostic");
      expect(fs.readFileSync(path.join(f.root, "launcher.log"), "utf8")).toBe(beforeLog);
      // A successful direct-child join is not whole-attempt containment proof.
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "watchdog-custody.json"), "utf8")))
        .toMatchObject({ format: 1, launcher: { pid: launcherPid }, owner: { pid: old.pid, token: old.token } });
      expect(f.launches()).toHaveLength(1);
    } finally { await f.close(); }
  }, 20_000);

  it("F1 owner-loss after helper reparenting stops the old native session, retains unresolved custody and never touches the live successor", async () => {
    expect(fs.existsSync(launcherPath), "fresh build required").toBe(true);
    const f = fixture("orphan-owner-loss", true);
    try {
      expect(await f.lifetime.exit).toEqual({ code: 0, signal: null }); // Real originating Main readiness + native exit.
      const launcherPid = f.events().find(row => row.event === "launcher-started").pid;
      await until(() => fs.existsSync(path.join(f.root, "helper-heartbeat")) && fs.existsSync(path.join(f.root, "intermediate.json")));
      const helper = JSON.parse(fs.readFileSync(path.join(f.root, "helper.json"), "utf8"));
      const intermediate = JSON.parse(fs.readFileSync(path.join(f.root, "intermediate.json"), "utf8"));
      const ownerFile = path.join(f.root, "owner.json");
      const old = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
      await until(() => {
        const stat = fs.readFileSync(`/proc/${helper.pid}/stat`, "utf8");
        const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        // A subreaper is allowed: prove loss of both ancestry edges, not PPID=1.
        return !live(intermediate.pid) && Number(fields[1]) !== intermediate.pid && Number(fields[1]) !== old.pid;
      });
      const helperStat = fs.readFileSync(`/proc/${helper.pid}/stat`, "utf8");
      const helperFields = helperStat.slice(helperStat.lastIndexOf(")") + 2).split(" ");
      expect(Number(helperFields[3])).toBe(old.pid); // Same old session, not arbitrary setsid containment.
      expect(live(old.pid)).toBe(true); expect(live(launcherPid)).toBe(true); expect(live(helper.pid)).toBe(true);
      console.error("native owner-loss pre-successor", JSON.stringify({ root: f.root, launcherPid, old,
        intermediatePid: intermediate.pid, helperPid: helper.pid, helperStat, intermediateGone: !live(intermediate.pid) }));

      // Only NOW replace owner authority with a valid, birth-pinned live process.
      const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
      const successor = Buffer.from(JSON.stringify({ ...old, pid: process.pid, token: "live-successor",
        processStartTime: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] }));
      const diagnostic = Buffer.from("successor diagnostic");
      fs.writeFileSync(ownerFile, successor);
      fs.writeFileSync(path.join(f.root, "error.json"), diagnostic);
      const beforeLog = fs.readFileSync(path.join(f.root, "launcher.log"));
      await until(() => !live(launcherPid));
      expect(live(old.pid)).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "ignored-eof.json"), "utf8"))).toEqual({ pid: old.pid });
      // Neither PID absence nor PPID change substitutes for the child's native join.
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "generations", `${old.token}.exit.json`), "utf8")))
        .toMatchObject({ generation: old.token, writer: "launcher", pid: old.pid, code: 0, signal: null });
      const failure = JSON.parse(fs.readFileSync(path.join(f.root, "generations", `${old.token}.error.json`), "utf8"));
      expect(JSON.parse(failure.error)).toMatchObject({ event: "launcher-exit", reason: "resident-owner-giveup",
        pid: old.pid, nativeExit: { code: 0, signal: null }, giveupMs: 200 });
      const unchanged = () => {
        expect(fs.readFileSync(ownerFile)).toEqual(successor);
        expect(fs.readFileSync(path.join(f.root, "error.json"))).toEqual(diagnostic);
        expect(fs.readFileSync(path.join(f.root, "launcher.log"))).toEqual(beforeLog);
      };
      unchanged();
      console.error("native owner-loss post-exit", JSON.stringify({ root: f.root, helperPid: helper.pid,
        helperLive: live(helper.pid), custody: fs.existsSync(path.join(f.root, "watchdog-custody.json")),
        launches: f.launches().length, successorBytesUnchanged: true }));
      expect(live(helper.pid), "F1: reparented old-session helper must stop before launcher exits").toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "helper-stopped.json"), "utf8")))
        .toEqual({ pid: helper.pid, signal: "SIGTERM" });
      // Emptying the observed session still cannot prove absence of escaped workers.
      const custodyFile = path.join(f.root, "watchdog-custody.json");
      const custody = fs.readFileSync(custodyFile);
      expect(JSON.parse(custody.toString())).toMatchObject({ format: 1,
        launcher: { pid: launcherPid }, owner: { pid: old.pid, token: old.token } });
      await sleep(300); // More than another shortened owner-loss budget: no replacement.
      expect(fs.readFileSync(custodyFile)).toEqual(custody);
      expect(f.launches()).toHaveLength(1);
      unchanged();
    } finally { await f.close(); }
  }, 20_000);

  it("F1 a double-forked, reparented grandchild keeps the old session live: no respawn before or after its exit without whole-attempt containment", async () => {
    expect(fs.existsSync(launcherPath), "fresh build required").toBe(true);
    const f = fixture("tree");
    try {
      await until(() => fs.existsSync(path.join(f.root, "helper.json")));
      const helper = JSON.parse(fs.readFileSync(path.join(f.root, "helper.json"), "utf8"));
      await until(() => {
        const stat = fs.readFileSync(`/proc/${helper.pid}/stat`, "utf8");
        const intermediate = JSON.parse(fs.readFileSync(path.join(f.root, "intermediate.json"), "utf8"));
        const ppid = Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[1]);
        // The executor may be a subreaper, so PPid need not be init (1).
        return !live(intermediate.pid) && ppid !== intermediate.pid && ppid !== f.launches()[0].pid;
      });
      await until(() => f.events().some(row => row.event === "watchdog-deferred" && row.members?.includes(helper.pid)));
      expect(live(f.launches()[0].pid)).toBe(false); expect(live(helper.pid)).toBe(true);
      expect(f.launches()).toHaveLength(1);
      expect(f.lifetime.exited).toBe(false);
      const stopping = f.events().find(row => row.event === "watchdog-stopping");
      const dir = stopping.evidenceDir;
      for (const name of ["status", "stat", "io", "wchan", "threads.stat", "fd-count", "runs-entry-count", "child-log-tail", "lease.json"]) expect(fs.existsSync(path.join(dir, name))).toBe(true);
      const reports = fs.readdirSync(dir).filter(name => /^report\..*\.json$/.test(name));
      expect(reports).toHaveLength(1);
      const report = JSON.parse(fs.readFileSync(path.join(dir, reports[0]!), "utf8"));
      expect(report.javascriptStack).toBeDefined(); expect(report.environmentVariables).toBeUndefined();
      await sleep(1100); expect(f.launches()).toHaveLength(1);
      fs.writeFileSync(path.join(f.root, "release-helper"), "release");
      await until(() => !live(helper.pid)); await sleep(1100);
      // Even an empty session would not fence detached workers/panes/scopes.
      expect(f.launches()).toHaveLength(1); expect(f.lifetime.exited).toBe(false);
    } finally { await f.close(); }
  }, 20_000);
  it("F2 the real child renews during the five-second report allowance and receives no TERM/KILL or replacement", async () => {
    const f = fixture("renew");
    try {
      await until(() => f.events().some(row => row.event === "watchdog-aborted" && row.signal === "SIGTERM"));
      await sleep(250);
      expect(live(f.launches()[0].pid)).toBe(true); expect(f.launches()).toHaveLength(1);
      expect(fs.existsSync(path.join(f.root, "terms.jsonl"))).toBe(false);
      expect(f.events().some(row => row.event === "child-exit")).toBe(false);
    } finally { await f.close(); }
  }, 15_000);
  it.each(["restore", "shutdown"])("F3 an unwritable evidence directory retains supervision and owned %s", async mode => {
    const f = fixture("evidence");
    try {
      await until(() => f.launches().length === 1);
      fs.chmodSync(path.join(f.root, "wedges"), 0o500);
      await until(() => f.events().filter(row => row.event === "watchdog-evidence-error").length >= 2);
      expect(live(f.launches()[0].pid)).toBe(true); expect(f.lifetime.exited).toBe(false);
      if (mode === "restore") {
        fs.chmodSync(path.join(f.root, "wedges"), 0o700);
        await until(() => f.events().some(row => row.event === "watchdog-deferred" && row.proofCheck));
        expect(live(f.launches()[0].pid)).toBe(false); expect(f.launches()).toHaveLength(1);
      } else {
        f.launcher.kill("SIGTERM"); await f.lifetime.exit;
        expect(live(f.launches()[0].pid)).toBe(false);
        expect(fs.existsSync(path.join(f.root, "terms.jsonl"))).toBe(true);
      }
    } finally { await f.close(); }
  }, 15_000);
});
