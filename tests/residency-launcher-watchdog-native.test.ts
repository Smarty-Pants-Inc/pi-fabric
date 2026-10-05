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
const fixture = (mode: "tree" | "renew" | "evidence") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-launcher-wedge-"));
  const owned = launchLog(root);
  const hostPath = path.join(root, "host.mjs");
  const config = { cwd: root, rootId: "fixture-root", meshRoot: path.join(root, "mesh"), piBinary: hostPath,
    residencyRoot: root, watchdog: { intervalMs: 50, stallMs: 100, coldStartMs: 0 } };
  const configPath = path.join(root, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  fs.mkdirSync(path.join(root, "runs")); fs.mkdirSync(path.join(root, "runs", "one"));
  fs.writeFileSync(path.join(root, "helper.mjs"), `import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(path.join(root, "helper.json"))},JSON.stringify({pid:process.pid}));
setInterval(()=>{if(fs.existsSync(${JSON.stringify(path.join(root, "release-helper"))}))process.exit(0);},20);`);
  fs.writeFileSync(path.join(root, "intermediate.mjs"), `import {spawn} from 'node:child_process';import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(path.join(root, "intermediate.json"))},JSON.stringify({pid:process.pid}));
const child=spawn(process.execPath,[${JSON.stringify(path.join(root, "helper.mjs"))}],{stdio:'ignore'});child.unref();`);
  fs.writeFileSync(hostPath, `import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {spawn} from 'node:child_process';
const config=JSON.parse(fs.readFileSync(process.env.PI_FABRIC_RESIDENT_CONFIG,'utf8'));
const root=config.residencyRoot;
fs.appendFileSync(path.join(root,'host-launches.jsonl'),JSON.stringify({pid:process.pid,config,args:process.argv.slice(2)})+'\\n');
const stat=fs.readFileSync('/proc/'+process.pid+'/stat','utf8');
fs.writeFileSync(path.join(root,'owner.json'),JSON.stringify({hostId:'fixture-host',token:'native-owner',pid:process.pid,startedAt:Date.now(),processStartTime:stat.slice(stat.lastIndexOf(')')+2).split(' ')[19]}));
const dir=path.join(config.meshRoot,'host-leases');fs.mkdirSync(dir,{recursive:true});
const file=path.join(dir,createHash('sha256').update('fixture-host').digest('hex').slice(0,32)+'.json');
const renew=()=>{const now=Date.now();fs.writeFileSync(file+'.tmp',JSON.stringify({format:1,id:'fixture-host',rootId:config.rootId,identityId:'fixture-host',updatedAt:now,expiresAt:now+1000}));fs.renameSync(file+'.tmp',file);};
renew();let timer=setInterval(renew,20);setTimeout(()=>clearInterval(timer),75);
${mode === "tree" ? `const intermediate=spawn(process.execPath,[${JSON.stringify(path.join(root, "intermediate.mjs"))}],{stdio:'ignore'});intermediate.unref();` : ""}
${mode === "renew" ? "process.on('SIGUSR2',()=>{renew();timer=setInterval(renew,20);});" : ""}
process.on('SIGTERM',()=>{fs.appendFileSync(path.join(root,'terms.jsonl'),JSON.stringify({pid:process.pid})+'\\n');process.exit(0);});
console.error('fixture child started '+process.pid);setInterval(()=>{},1000);`);
  // Exercise the real residency launcher CLI, not an imported supervise wrapper.
  const launcher = spawn(process.execPath, [launcherPath, "--config", configPath], { env: { ...process.env, ...owned.env }, stdio: "ignore" });
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
    fs.chmodSync(path.join(root, "wedges"), 0o700);
    if (!lifetime.exited) launcher.kill("SIGTERM");
    // Kill only fixture-recorded, birth-validated PIDs. No group/pattern kills.
    for (const record of owned.owned()) if (same(record) && live(record.pid)) {
      try { process.kill(record.pid, "SIGKILL"); } catch { /* fixture exited */ }
    }
    await lifetime.exit;
    await until(() => owned.owned().every(record => !same(record) || !live(record.pid)));
    fs.rmSync(root, { recursive: true, force: true });
  };
  return { root, launcher, lifetime, events, launches, close };
};

describe.skipIf(process.platform !== "linux")("compiled residency launcher CLI watchdog", () => {
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
