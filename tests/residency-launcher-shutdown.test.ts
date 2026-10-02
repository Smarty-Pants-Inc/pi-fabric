import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { launchLog, same, type Owned } from "./helpers/owned-processes.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, ms = 12_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Compiled launcher shutdown deadline");
    await sleep(20);
  }
}
const live = (owned: Owned): boolean => {
  if (!same(owned)) return false;
  try {
    const stat = fs.readFileSync(`/proc/${owned.pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[0] !== "Z";
  } catch { return false; }
};

// Native Windows child receipt tests live in residency-child-lifetime.test.ts.
// This launcher fault injection specifically exercises Linux /proc uncertainty.
describe.skipIf(process.platform !== "linux" || !fs.existsSync(launcherPath))("compiled launcher native shutdown", () => {
  it.each(["unreadable-birth", "observed-helper"] as const)("joins one shutdown pass for %s, even after repeated TERM", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-launcher-stop-"));
    const ownership = launchLog(root);
    const ready = path.join(root, "host-ready.json"), helperReady = path.join(root, "helper-ready.json");
    const signals = path.join(root, "native-signals.jsonl"), denied = path.join(root, "birth-denied");
    const hostPath = path.join(root, "host.mjs"), preload = path.join(root, "launcher-preload.mjs");
    fs.writeFileSync(preload, `import fs from 'node:fs';import {ChildProcess} from 'node:child_process';
if(process.argv[1]===${JSON.stringify(launcherPath)}) {
 const kill=ChildProcess.prototype.kill;
 ChildProcess.prototype.kill=function(signal){fs.appendFileSync(${JSON.stringify(signals)},JSON.stringify({pid:this.pid,signal})+'\\n');return kill.call(this,signal);};
 ${mode === "unreadable-birth" ? `const read=fs.readFileSync;
 fs.readFileSync=function(file,...args){if(/^\\/proc\\/\\d+\\/stat$/.test(String(file))&&String(file)!=='/proc/'+process.pid+'/stat') {
 fs.writeFileSync(${JSON.stringify(denied)},'EACCES');throw Object.assign(new Error('injected unreadable child birth'),{code:'EACCES'});
 }return read.call(this,file,...args);};` : ""}
}`);
    const helperCode = `import fs from 'node:fs';
process.on('SIGTERM',()=>{});fs.writeFileSync(${JSON.stringify(helperReady)},JSON.stringify({pid:process.pid}));setInterval(()=>{},1000);`;
    fs.writeFileSync(hostPath, `import fs from 'node:fs';import {spawn} from 'node:child_process';
${mode === "observed-helper" ? `const helper=spawn(process.execPath,['--input-type=module','-e',${JSON.stringify(helperCode)}],{detached:true,stdio:'ignore',env:process.env});helper.unref();
process.on('SIGTERM',()=>process.exit(0));` : "process.on('SIGTERM',()=>{});"}
fs.writeFileSync(${JSON.stringify(ready)},JSON.stringify({pid:process.pid}));setInterval(()=>{},1000);`);
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostPath }));
    const launcher = spawn(process.execPath, [launcherPath, "--config", configPath], {
      env: { ...process.env, ...ownership.env, NODE_OPTIONS: `${ownership.env.NODE_OPTIONS} --import=${pathToFileURL(preload).href}` }, stdio: "ignore",
    });
    const lifetime = watchResidentChild(launcher);
    launcher.on("error", () => {});
    try {
      await until(() => fs.existsSync(ready) && (mode !== "observed-helper" || fs.existsSync(helperReady)));
      if (mode === "unreadable-birth") expect(fs.existsSync(denied)).toBe(true);
      const hostPid = JSON.parse(fs.readFileSync(ready, "utf8")).pid as number;
      const host = ownership.owned().find(record => record.pid === hostPid)!;
      expect(host).toBeDefined();
      expect(live(host)).toBe(true);
      launcher.kill("SIGTERM");
      await sleep(200);
      launcher.kill("SIGTERM"); // Must not take the default abrupt-exit path.
      expect(lifetime.exited).toBe(false);
      if (mode === "observed-helper") {
        await until(() => !live(host));
        const helperPid = JSON.parse(fs.readFileSync(helperReady, "utf8")).pid as number;
        expect(live(ownership.owned().find(record => record.pid === helperPid)!)).toBe(true);
        expect(lifetime.exited).toBe(false); // Finalizer joins cleanup after native host exit.
      }
      await until(() => lifetime.exited);
      await lifetime.exit;
      expect(ownership.owned().filter(live)).toEqual([]);
      const directSignals = fs.readFileSync(signals, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(directSignals).toEqual(mode === "unreadable-birth"
        ? [{ pid: hostPid, signal: "SIGTERM" }, { pid: hostPid, signal: "SIGKILL" }]
        : [{ pid: hostPid, signal: "SIGTERM" }]);
      const traces = fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(traces.filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(traces.filter(row => row.event === "child-spawned" && (row.kind === "target" || row.kind === "fallback"))).toHaveLength(0);
    } finally {
      // Only this fixture's launch-time identities may be signaled, rechecked
      // immediately before each signal. Reap the controller we directly own.
      for (const record of ownership.owned().filter(live)) {
        if (same(record)) { try { process.kill(record.pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; } }
      }
      if (!lifetime.exited) launcher.kill("SIGKILL");
      await lifetime.exit;
      await until(() => ownership.owned().filter(live).length === 0);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);
});
