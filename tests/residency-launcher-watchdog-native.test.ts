import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { hostLeasePath } from "../src/topology/host-leases.js";
import { launchLog, same } from "./helpers/owned-processes.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const live = (pid: number) => {
  try {
    const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return text.slice(text.lastIndexOf(")") + 2).split(" ")[0] !== "Z";
  } catch { return false; }
};
const until = async (condition: () => boolean) => {
  const end = Date.now() + 10_000;
  while (!condition()) {
    if (Date.now() > end) throw new Error("Compiled watchdog probe deadline");
    await sleep(20);
  }
};

describe.skipIf(process.platform !== "linux" || !fs.existsSync(launcherPath))("compiled launcher wedge recovery", () => {
  it("captures native proc/thread evidence and a Node report before TERM/KILL, then renews with the same launch config", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-launcher-wedge-"));
    const owned = launchLog(root);
    const hostPath = path.join(root, "host.mjs");
    const driverPath = path.join(root, "driver.mjs");
    const config = { cwd: root, rootId: "fixture-root", meshRoot: path.join(root, "mesh"), piBinary: hostPath,
      residencyRoot: root, watchdog: { intervalMs: 50, stallMs: 100, coldStartMs: 0 } };
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(config));
    fs.mkdirSync(path.join(root, "runs")); fs.mkdirSync(path.join(root, "runs", "one"));
    fs.writeFileSync(driverPath, `import {supervise} from ${JSON.stringify(pathToFileURL(launcherPath).href)};
await supervise(${JSON.stringify(configPath)}, {reportWaitMs: 50,termMs: 100,killWaitMs: 1000});`);
    fs.writeFileSync(hostPath, `import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';
const config=JSON.parse(fs.readFileSync(process.env.PI_FABRIC_RESIDENT_CONFIG,'utf8'));
const root=config.residencyRoot, log=path.join(root,'host-launches.jsonl');
const attempt=fs.existsSync(log)?fs.readFileSync(log,'utf8').trim().split('\\n').length:0;
fs.appendFileSync(log,JSON.stringify({pid:process.pid,attempt,configPath:process.env.PI_FABRIC_RESIDENT_CONFIG,config,args:process.argv.slice(2)})+'\\n');
const stat=fs.readFileSync('/proc/'+process.pid+'/stat','utf8');
fs.writeFileSync(path.join(root,'owner.json'),JSON.stringify({hostId:'fixture-host',pid:process.pid,processStartTime:stat.slice(stat.lastIndexOf(')')+2).split(' ')[19]}));
const dir=path.join(config.meshRoot,'host-leases');fs.mkdirSync(dir,{recursive:true});
const file=path.join(dir,createHash('sha256').update('fixture-host').digest('hex').slice(0,32)+'.json');
const renew=()=>{const now=Date.now();fs.writeFileSync(file+'.tmp',JSON.stringify({format:1,id:'fixture-host',rootId:config.rootId,identityId:'fixture-host',updatedAt:now,expiresAt:now+1000}));fs.renameSync(file+'.tmp',file);};
renew();let timer=setInterval(renew,20);if(attempt===0)setTimeout(()=>clearInterval(timer),75);
process.on('SIGTERM',()=>{fs.appendFileSync(path.join(root,'terms.jsonl'),JSON.stringify({pid:process.pid,evidence:fs.readdirSync(path.join(root,'wedges')).filter(name=>/^20/.test(name))})+'\\n');if(attempt>0)process.exit(0);});
console.error('fixture child started '+process.pid);setInterval(()=>{},1000);`);
    const driver = spawn(process.execPath, [driverPath], { env: { ...process.env, ...owned.env }, stdio: "ignore" });
    const lifetime = watchResidentChild(driver);
    driver.on("error", () => {});
    try {
      const markerFile = path.join(root, "wedges", "latest.json");
      await until(() => fs.existsSync(markerFile));
      const marker = JSON.parse(fs.readFileSync(markerFile, "utf8"));
      expect(marker).toMatchObject({ topic: "fleet.residency.fixture-host", kind: "wedge-recovered", restartCount: 1 });
      const dir = marker.evidenceDir;
      for (const name of ["status", "stat", "io", "wchan", "threads.stat", "fd-count", "runs-entry-count", "child-log-tail", "lease.json"]) expect(fs.existsSync(path.join(dir, name))).toBe(true);
      expect(Number(fs.readFileSync(path.join(dir, "fd-count"), "utf8"))).toBeGreaterThan(0);
      expect(fs.readFileSync(path.join(dir, "threads.stat"), "utf8").trim().split("\n").length).toBeGreaterThan(0);
      expect(fs.readFileSync(path.join(dir, "runs-entry-count"), "utf8")).toBe("1");
      expect(fs.readFileSync(path.join(dir, "child-log-tail"), "utf8")).toContain("fixture child started");
      const reports = fs.readdirSync(dir).filter(name => /^report\..*\.json$/.test(name));
      expect(reports).toHaveLength(1);
      const report = JSON.parse(fs.readFileSync(path.join(dir, reports[0]!), "utf8"));
      expect(report.javascriptStack).toBeDefined();
      expect(report.environmentVariables).toBeUndefined();
      const launches = fs.readFileSync(path.join(root, "host-launches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(launches).toHaveLength(2); expect(launches[0].config).toEqual(launches[1].config);
      expect(launches[0].args).toEqual(launches[1].args);
      const traces = fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(traces.filter(row => row.event === "watchdog-restart")).toHaveLength(1);
      expect(traces.find(row => row.event === "watchdog-restart").cpu).toMatchObject({ userTicks: expect.any(Number), systemTicks: expect.any(Number) });
      expect(traces.find(row => row.event === "child-exit" && row.pid === launches[0].pid)?.signal).toBe("SIGKILL");
      expect(live(launches[0].pid)).toBe(false); expect(live(launches[1].pid)).toBe(true);
      const terms = fs.readFileSync(path.join(root, "terms.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(terms[0].pid).toBe(launches[0].pid); expect(terms[0].evidence).toHaveLength(1);
      const leaseFile = hostLeasePath(config.meshRoot, "fixture-host");
      const before = JSON.parse(fs.readFileSync(leaseFile, "utf8")).updatedAt;
      await sleep(100);
      expect(JSON.parse(fs.readFileSync(leaseFile, "utf8")).updatedAt).toBeGreaterThan(before);
      driver.kill("SIGTERM"); await lifetime.exit;
    } finally {
      for (const record of owned.owned()) {
        if (same(record) && live(record.pid)) { try { process.kill(record.pid, "SIGKILL"); } catch { /* fixture exited */ } }
      }
      if (!lifetime.exited) driver.kill("SIGKILL");
      await lifetime.exit;
      await until(() => owned.owned().every(record => !same(record) || !live(record.pid)));
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);
});
