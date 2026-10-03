import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { launchLog, same, type Owned } from "./helpers/owned-processes.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const hostPath = path.resolve("dist/residency/host.js");
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, ms = 12_000) {
  const deadline = Date.now() + ms;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error("Native recovery deadline"); await sleep(20); }
}
const traces = (root: string): Array<Record<string, unknown>> => {
  try { return fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line)); }
  catch { return []; }
};

describe.skipIf(process.platform !== "linux" || !fs.existsSync(launcherPath))("native resident watchdog (#3864)", () => {
  it.each(["stale-lease", "missing-owner"] as const)("alarms once, escalates a hung TERM and blocks recovery of its exact %s child", { timeout: 35_000 }, async fault => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-watchdog-"));
    const fake = path.join(root, "host.mjs");
    fs.writeFileSync(fake, `import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';
const root=${JSON.stringify(root)},hostId='resident:watchdog',rootId='session:watchdog';
const countFile=path.join(root,'starts');let count=0;try{count=Number(fs.readFileSync(countFile,'utf8'));}catch{}count++;
const birth=fs.readFileSync('/proc/'+process.pid+'/stat','utf8').split(') ').at(-1).trim().split(/\\s+/)[19];
const readyAt=Date.now(),fault=${JSON.stringify(fault)};if(count>1||fault==='stale-lease')fs.writeFileSync(path.join(root,'owner.json'),JSON.stringify({format:1,pid:process.pid,processStartTime:birth,hostId,token:String(process.pid),readyAt}));
const leases=path.join(root,'mesh','host-leases');fs.mkdirSync(leases,{recursive:true});
const name=createHash('sha256').update(hostId).digest('hex').slice(0,32)+'.json';
const renew=()=>{if(count===1&&fault==='missing-owner')return;const updatedAt=count===1?readyAt-20000:Date.now();fs.writeFileSync(path.join(leases,name),JSON.stringify({format:1,id:hostId,rootId,identityId:hostId,updatedAt,expiresAt:updatedAt+60000}));};
renew();setInterval(renew,200);process.on('SIGTERM',()=>{fs.appendFileSync(path.join(root,'terms'),String(process.pid)+'\\n');if(count>1)process.exit(0);});
fs.writeFileSync(countFile,String(count));`);
    const config = path.join(root, "config.json");
    fs.writeFileSync(config, JSON.stringify({ cwd: root, piBinary: fake, meshRoot: path.join(root, "mesh"), rootId: "session:watchdog" }));
    const child = spawn(process.execPath, [launcherPath, "--config", config], { stdio: "ignore" });
    const life = watchResidentChild(child);
    try {
      await until(() => life.exited, 28_000);
      expect(await life.exit).toEqual({ code: 1, signal: null });
      const rows = traces(root), first = rows.find(row => row.event === "child-spawned")!;
      expect(rows.filter(row => row.event === "watchdog-alarm")).toEqual([expect.objectContaining({ pid: first.pid, reason: "stale-lease" })]);
      expect(rows.find(row => row.event === "child-exit" && row.pid === first.pid)).toMatchObject({ signal: "SIGKILL" });
      expect(fs.readFileSync(path.join(root, "terms"), "utf8").trim()).toBe(String(first.pid));
      await sleep(1_200); // No alarm storm or extra start after native child exit.
      expect(traces(root).filter(row => row.event === "watchdog-alarm")).toHaveLength(1);
      expect(traces(root).filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(traces(root).filter(row => row.event === "watchdog-restart-blocked")).toHaveLength(1);
      expect(fs.readFileSync(path.join(root, "starts"), "utf8")).toBe("1");
      expect(JSON.parse(fs.readFileSync(path.join(root, "watchdog-alarm.json"), "utf8"))).toMatchObject({ reason: "stale-lease", pid: first.pid });
      if (fault === "stale-lease") {
        const owner = JSON.parse(fs.readFileSync(path.join(root, "owner.json"), "utf8"));
        expect(processStartTime(owner.pid)).not.toBe(owner.processStartTime);
      }
    } finally {
      if (!life.exited) await life.stop();
      await life.exit;
      // The controller must finish cleanup before the fixture disappears.
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["normal", "alarm-write-failure"] as const)("refuses successor admission with a live setsid/reparented helper, even after launcher re-entry (%s)", { timeout: 25_000 }, async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-escaped-helper-"));
    const ownership = launchLog(root);
    const preload = path.join(root, "alarm-preload.mjs");
    fs.writeFileSync(preload, `import fs from 'node:fs';
if(${JSON.stringify(mode)}==='alarm-write-failure'&&process.argv[1]===${JSON.stringify(launcherPath)}) {
 const write=fs.writeFileSync;
 fs.writeFileSync=function(file,...args){
  if(typeof file==='number'&&fs.readlinkSync('/proc/self/fd/'+file)===${JSON.stringify(path.join(root, "watchdog-alarm.json"))}) throw Object.assign(new Error('injected alarm write failure'),{code:'EIO'});
  return write.call(this,file,...args);
 };
}`);
    const fake = path.join(root, "host.mjs"), helperReady = path.join(root, "helper-ready"), effects = path.join(root, "effects");
    const alive = (owned: Owned): boolean => {
      if (!same(owned)) return false;
      try { return fs.readFileSync(`/proc/${owned.pid}/stat`, "utf8").split(") ").at(-1)?.startsWith("Z ") === false; }
      catch { return false; }
    };
    const helperCode = `import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(helperReady)},String(process.pid));setInterval(()=>fs.appendFileSync(${JSON.stringify(effects)},'effect\\n'),20);`;
    const brokerCode = `import {spawn} from 'node:child_process';const helper=spawn(process.execPath,['--input-type=module','-e',${JSON.stringify(helperCode)}],{detached:true,stdio:'ignore'});helper.unref();`;
    fs.writeFileSync(fake, `import fs from 'node:fs';import path from 'node:path';import {spawn} from 'node:child_process';import {createHash} from 'node:crypto';
const root=${JSON.stringify(root)},hostId='resident:escaped',rootId='session:escaped';
const birth=fs.readFileSync('/proc/'+process.pid+'/stat','utf8').split(') ').at(-1).trim().split(/\\s+/)[19];
fs.appendFileSync(path.join(root,'starts'),String(process.pid)+'\\n');
fs.writeFileSync(path.join(root,'owner.json'),JSON.stringify({format:1,pid:process.pid,processStartTime:birth,hostId,token:String(process.pid),readyAt:Date.now()}));
const leases=path.join(root,'mesh','host-leases');fs.mkdirSync(leases,{recursive:true});
const lease=path.join(leases,createHash('sha256').update(hostId).digest('hex').slice(0,32)+'.json');
const broker=spawn(process.execPath,['--input-type=module','-e',${JSON.stringify(brokerCode)}],{stdio:'ignore'});
broker.once('exit',()=>{const timer=setInterval(()=>{if(!fs.existsSync(${JSON.stringify(helperReady)}))return;clearInterval(timer);const updatedAt=Date.now()-20000;fs.writeFileSync(lease,JSON.stringify({format:1,id:hostId,rootId,identityId:hostId,updatedAt,expiresAt:updatedAt+60000}));},20);});
process.on('SIGTERM',()=>process.exit(0));setInterval(()=>{},1000);`);
    const config = path.join(root, "config.json");
    fs.writeFileSync(config, JSON.stringify({ cwd: root, piBinary: fake, meshRoot: path.join(root, "mesh"), rootId: "session:escaped" }));
    const controllers: Array<{ child: ReturnType<typeof spawn>; life: ReturnType<typeof watchResidentChild> }> = [];
    const launch = () => {
      const child = spawn(process.execPath, [launcherPath, "--config", config], { env: { ...process.env, ...ownership.env, NODE_OPTIONS: `${ownership.env.NODE_OPTIONS} --import=${pathToFileURL(preload).href}` }, stdio: "ignore" });
      const controller = { child, life: watchResidentChild(child) }; controllers.push(controller); return controller;
    };
    try {
      const first = launch();
      await until(() => fs.existsSync(helperReady) && fs.existsSync(effects));
      const helperPid = Number(fs.readFileSync(helperReady, "utf8"));
      const helper = ownership.owned().find(row => row.pid === helperPid)!;
      expect(helper).toBeDefined();
      const stat = fs.readFileSync(`/proc/${helperPid}/stat`, "utf8").split(") ").at(-1)!.trim().split(/\s+/);
      expect(Number(stat[2])).toBe(helperPid); // setsid: different group from resident host
      expect(Number(stat[1])).not.toBe(Number(fs.readFileSync(path.join(root, "starts"), "utf8").trim())); // reparented before alarm
      await until(() => traces(root).some(row => row.event === "watchdog-alarm"));
      await until(() => first.life.exited || traces(root).filter(row => row.event === "child-spawned").length > 1);
      expect(traces(root).filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(await first.life.exit).toEqual({ code: 1, signal: null });
      expect(traces(root).filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(traces(root).filter(row => row.event === "watchdog-restart")).toHaveLength(0);
      expect(traces(root).filter(row => row.event === "watchdog-restart-blocked")).toHaveLength(1);
      expect(alive(helper)).toBe(true);
      const alarm = fs.readFileSync(path.join(root, "watchdog-alarm.json"), "utf8");
      if (mode === "alarm-write-failure") expect(alarm).toBe(""); // negative latch survives failed diagnostics
      else expect(JSON.parse(alarm)).toMatchObject({ reason: "stale-lease" });
      const leases = path.join(root, "mesh", "host-leases");
      const leasePath = path.join(leases, fs.readdirSync(leases)[0]!);
      const leaseBefore = fs.readFileSync(leasePath, "utf8"), effectsBefore = fs.statSync(effects).size;
      const second = launch();
      await until(() => second.life.exited);
      expect(await second.life.exit).toEqual({ code: 1, signal: null });
      await sleep(100);
      expect(fs.statSync(effects).size).toBeGreaterThan(effectsBefore); // old helper can still perform effects
      expect(alive(helper)).toBe(true);
      expect(fs.readFileSync(path.join(root, "starts"), "utf8").trim().split("\n")).toHaveLength(1);
      expect(fs.readFileSync(leasePath, "utf8")).toBe(leaseBefore); // no successor re-takes lease
      expect(fs.readFileSync(path.join(root, "watchdog-alarm.json"), "utf8")).toBe(alarm);
    } finally {
      // Fixture launch-time birth receipts, never ancestry/argv, own cleanup.
      for (const row of ownership.owned().filter(alive)) if (same(row)) {
        try { process.kill(row.pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
      }
      for (const { child, life } of controllers) { if (!life.exited) child.kill("SIGKILL"); await life.exit; }
      await until(() => ownership.owned().filter(alive).length === 0);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("stops a real non-reaping host without a successor even while its lease keeps renewing", { timeout: 35_000 }, async ctx => {
    try { execFileSync("cc", ["--version"], { stdio: "ignore" }); }
    catch { ctx.skip(); return; }
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-zombie-"));
    const fake = path.join(root, "host"), leases = path.join(root, "mesh", "host-leases");
    fs.mkdirSync(leases, { recursive: true });
    const lease = path.join(leases, createHash("sha256").update("resident:zombie").digest("hex").slice(0, 32) + ".json");
    let child: ReturnType<typeof spawn> | undefined;
    let life: ReturnType<typeof watchResidentChild> | undefined;
    try {
      execFileSync("cc", [path.resolve("tests/fixtures/unreaped-resident-host.c"), "-o", fake], { stdio: "pipe" });
      const config = path.join(root, "config.json");
      fs.writeFileSync(config, JSON.stringify({ cwd: root, piBinary: fake, meshRoot: path.join(root, "mesh"), rootId: "session:watchdog" }));
      child = spawn(process.execPath, [launcherPath, "--config", config], {
        env: { ...process.env, WATCHDOG_ROOT: root, WATCHDOG_LEASE: lease }, stdio: "ignore",
      });
      life = watchResidentChild(child);
      await until(() => fs.existsSync(path.join(root, "zombie-pid")));
      const zombie = Number(fs.readFileSync(path.join(root, "zombie-pid"), "utf8"));
      await until(() => fs.readFileSync(`/proc/${zombie}/stat`, "utf8").split(") ").at(-1)?.startsWith("Z ") === true);
      await until(() => life!.exited, 23_000);
      expect(await life.exit).toEqual({ code: 1, signal: null });
      expect(traces(root).filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(traces(root).filter(row => row.event === "watchdog-restart-blocked")).toHaveLength(1);
      expect(JSON.parse(fs.readFileSync(path.join(root, "watchdog-alarm.json"), "utf8"))).toMatchObject({ reason: "unreaped-child" });
      expect(traces(root).filter(row => row.event === "watchdog-alarm")).toEqual([expect.objectContaining({ reason: "unreaped-child" })]);
      expect(JSON.parse(fs.readFileSync(lease, "utf8")).updatedAt).toBeGreaterThan(Date.now() - 3_000);
    } finally {
      if (life && !life.exited) await life.stop();
      await life?.exit;
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["close", "startup"] as const)("exits on its own TERM deadline with hung %s, keeping its fence until exit", { timeout: 15_000 }, async phase => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-host-term-"));
    const config = path.join(root, "config.json"), ready = path.join(root, "ready");
    fs.writeFileSync(config, JSON.stringify({
      format: 1, rootId: "session:term", sessionId: "term", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: root,
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "worker.js", fabricExtensionPath: "index.js",
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    }));
    const script = path.join(root, "hung-host.mjs");
    fs.writeFileSync(script, `import fs from 'node:fs';import {ResidentHost,runResidentHostFromConfigPath} from ${JSON.stringify(pathToFileURL(hostPath).href)};
${phase === "close" ? `const start=ResidentHost.prototype.start;ResidentHost.prototype.start=async function(){await start.call(this);fs.writeFileSync(${JSON.stringify(ready)},'ready');};ResidentHost.prototype.close=()=>new Promise(()=>{});` : `ResidentHost.prototype.start=()=>{fs.writeFileSync(${JSON.stringify(ready)},'ready');return new Promise(()=>{});};`}
setInterval(()=>{},1000);await runResidentHostFromConfigPath(${JSON.stringify(config)});`);
    const child = spawn(process.execPath, [script], { env: { ...process.env, PI_FABRIC_TEST_RESIDENT_SHUTDOWN_MS: "200" }, stdio: ["ignore", "pipe", "pipe"] });
    const life = watchResidentChild(child);
    let stderr = ""; child.stderr?.on("data", chunk => { stderr += chunk; });
    try {
      await until(() => fs.existsSync(ready));
      const started = Date.now();
      child.kill("SIGTERM"); await sleep(50); child.kill("SIGTERM");
      await until(() => life.exited, 1_500);
      const exit = await life.exit;
      expect(exit).toEqual({ code: 1, signal: null });
      expect(stderr).toContain("shutdown deadline exceeded");
      expect(Date.now() - started).toBeLessThan(1_500);
      if (phase === "close") expect(fs.existsSync(path.join(root, "owner.json"))).toBe(true);
    } finally { if (!life.exited) child.kill("SIGKILL"); await life.exit; fs.rmSync(root, { recursive: true, force: true }); }
  });
});
