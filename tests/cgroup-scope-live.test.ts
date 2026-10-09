import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody, executionCgroups } from "../src/process-cgroup.js";
import { executionGroup } from "../src/worker/execution-group.js";
import { spawnScopedExecution, releaseScopedChild } from "../src/worker/scope-spawn.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const available = process.platform === "linux" && fs.existsSync("/sys/fs/cgroup/cgroup.controllers") &&
  spawnSync("systemd-run", ["--user", "--scope", "--quiet", "--collect", "true"], { timeout: 5_000 }).status === 0;
afterEach(() => vi.restoreAllMocks());
const closedWithin = async (closed: Promise<void>) => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { await Promise.race([closed, new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("owned scope native close timed out")), 5_000);
  })]); } finally { clearTimeout(timer); }
};
const executing = (pid: number): boolean => {
  try { const fields = fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ").at(-1)!.split(" "); return !["Z", "X"].includes(fields[0]!); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
};

describe.skipIf(!available)("real Linux cgroup scope custody", () => {
  it("scrubs target user-bus discovery and nested systemd-run --user --scope fails", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-bus-"));
    const marker = path.join(root, "bus.json");
    const child = await spawnScopedExecution((binary, args, options) => spawn(binary, [...args], options),
      process.execPath, ["-e", `const fs=require('node:fs'),{spawnSync}=require('node:child_process');
        const probe=spawnSync('systemd-run',['--user','--scope','true'],{timeout:2000,encoding:'utf8'});
        fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify({status:probe.status,stderr:probe.stderr,dbus:process.env.DBUS_SESSION_BUS_ADDRESS,xdg:process.env.XDG_RUNTIME_DIR}));`],
      { stdio: "ignore", detached: true });
    const close = new Promise<void>(resolve => child.once("close", () => resolve()));
    const receipt = executionCgroups.get(child);
    try {
      expect(receipt).toBeDefined(); releaseScopedChild(child); await closedWithin(close);
      const result = JSON.parse(fs.readFileSync(marker, "utf8"));
      expect(result.dbus).toBeUndefined(); expect(result.xdg).toBeUndefined();
      expect(result.status).not.toBe(0); expect(result.stderr).toContain("Failed to connect");
      expect(await receipt!.waitForExit(2_000)).toBe(true);
    } finally {
      await receipt?.signal("SIGKILL"); releaseScopedChild(child); await closedWithin(close);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("nested Fabric custody launchers still obtain scopes with a scrubbed ambient environment", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-nested-"));
    const worker = path.join(root, "worker.mjs"); fs.writeFileSync(worker, "setInterval(()=>{},1000)");
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined); vi.stubEnv("XDG_RUNTIME_DIR", undefined);
    let child: ChildProcess | undefined, close: Promise<void> | undefined;
    let handle: Awaited<ReturnType<ProcessTransport["launch"]>> | undefined;
    try {
      child = await spawnScopedExecution((binary, args, options) => spawn(binary, [...args], options), process.execPath, [worker], { detached: true, stdio: "ignore" });
      close = new Promise(resolve => child!.once("close", () => resolve()));
      expect(executionCgroups.get(child)).toBeDefined(); releaseScopedChild(child);
      handle = await new ProcessTransport().launch({ id: "nested", name: "nested", cwd: root, workerPath: worker, workerArguments: [] });
      expect(handle.treeClosed).toBeDefined(); expect(await handle.isAlive()).toBe(true);
    } finally {
      if (child) { await executionCgroups.get(child)?.signal("SIGKILL"); releaseScopedChild(child); if (close) await closedWithin(close); }
      if (handle) { await handle.stop(); await handle.waitForClose?.(); }
      vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it("catches a sibling systemd scope escaper with the recorded session, even after leader exit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-escape-"));
    const marker = path.join(root, "escape.json"), leaf = path.join(root, "leaf.mjs"), worker = path.join(root, "worker.mjs");
    const bus = { DBUS_SESSION_BUS_ADDRESS: process.env.DBUS_SESSION_BUS_ADDRESS, XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR };
    fs.writeFileSync(leaf, `import fs from 'node:fs'; process.on('SIGTERM',()=>{});
      const fields=fs.readFileSync('/proc/self/stat','utf8').split(') ').at(-1).split(' ');
      fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify({pid:process.pid,session:Number(fields[3]),scope:'/sys/fs/cgroup'+fs.readFileSync('/proc/self/cgroup','utf8').trim().split('::')[1]}));setInterval(()=>{},1000);`);
    fs.writeFileSync(worker, `import fs from 'node:fs'; import {spawn} from 'node:child_process'; import {randomUUID} from 'node:crypto';
      // Deliberately restore known bus discovery: scrub is hardening, not a same-UID boundary.
      const escape=spawn('/usr/bin/systemd-run',['--user','--scope','--quiet','--collect','--unit=fabric-test-escape-'+randomUUID()+'.scope','--',process.execPath,${JSON.stringify(leaf)}],{env:{...process.env,...${JSON.stringify(bus)}},stdio:'ignore'});escape.unref();
      const ready=setInterval(()=>{if(fs.existsSync(${JSON.stringify(marker)})){clearInterval(ready);process.exit(0)}},10);`);
    const child = await spawnScopedExecution((binary, args, options) => spawn(binary, [...args], options), process.execPath, [worker], { detached: true, stdio: "ignore" });
    const close = new Promise<void>(resolve => child.once("close", () => resolve()));
    const receipt = executionCgroups.get(child);
    let escaped: { pid: number; session: number; scope: string } | undefined;
    try {
      expect(receipt).toBeDefined(); releaseScopedChild(child);
      await vi.waitFor(() => expect(fs.existsSync(marker)).toBe(true), { timeout: 5_000 });
      escaped = JSON.parse(fs.readFileSync(marker, "utf8"));
      expect(escaped!.scope).not.toBe(receipt!.directory);
      expect(escaped!.session).toBe(receipt!.execution!.session);
      await closedWithin(close); expect(executing(escaped!.pid)).toBe(true);
      const started = Date.now(); await receipt!.signal("SIGKILL");
      expect(await receipt!.waitForExit(3_000)).toBe(true);
      await vi.waitFor(() => expect(executing(escaped!.pid)).toBe(false), { timeout: 3_000 });
      expect(Date.now() - started).toBeLessThan(4_000);
    } finally {
      await receipt?.signal("SIGKILL"); releaseScopedChild(child); await closedWithin(close);
      if (!escaped && fs.existsSync(marker)) escaped = JSON.parse(fs.readFileSync(marker, "utf8"));
      if (escaped) { const cleanup = cgroupCustody(escaped.scope); await cleanup.signal("SIGKILL"); expect(await cleanup.waitForExit(3_000)).toBe(true); }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);

  it.each(["setsid", "double-fork"])("lists and KILLs an unsampled %s escaper after scope leader exit (#7248)", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-live-"));
    const marker = path.join(root, "orphan.pid");
    const leaf = path.join(root, "leaf.mjs");
    fs.writeFileSync(leaf, `import fs from 'node:fs'; process.on('SIGTERM',()=>{}); fs.writeFileSync(${JSON.stringify(marker)},String(process.pid)); setInterval(()=>{},1000);`);
    const fork = path.join(root, "fork.mjs");
    fs.writeFileSync(fork, `import {spawn} from 'node:child_process'; const leaf=spawn(process.execPath,[${JSON.stringify(leaf)}],{detached:true,stdio:'ignore'}); leaf.unref();`);
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from 'node:fs'; import {spawn} from 'node:child_process';
      const child=spawn(${mode === "setsid" ? JSON.stringify("/usr/bin/setsid") : "process.execPath"},${mode === "setsid" ? `[process.execPath,${JSON.stringify(leaf)}]` : `[${JSON.stringify(fork)}]`},{stdio:'ignore'});
      child.unref(); const ready=setInterval(()=>{if(fs.existsSync(${JSON.stringify(marker)})){clearInterval(ready);process.exit(0)}},10);`);
    let child: ChildProcess | undefined, close: Promise<void> | undefined;
    try {
      child = await spawnScopedExecution((binary, args, options) => spawn(binary, [...args], options), process.execPath, [worker], { cwd: root, detached: true, stdio: "ignore" });
      close = new Promise(resolve => child!.once("close", () => resolve()));
      const receipt = executionCgroups.get(child);
      expect(receipt, "must run actual cgroup path, not silently prove legacy fallback").toBeDefined();
      const group = executionGroup(child);
      releaseScopedChild(child);
      await vi.waitFor(() => expect(fs.existsSync(marker)).toBe(true), { timeout: 5_000 });
      await closedWithin(close);
      const pid = Number(fs.readFileSync(marker, "utf8"));
      expect(executing(pid)).toBe(true);
      expect(receipt!.members()).toContain(pid);
      const scan = vi.spyOn(fs, "readdirSync");
      group.observe(); expect(group.exited()).toBe(false);
      expect(scan.mock.calls.filter(call => String(call[0]) === "/proc")).toEqual([]);
      await group.signal("SIGKILL");
      expect(await receipt!.waitForExit(3_000)).toBe(true);
      await vi.waitFor(() => expect(executing(pid)).toBe(false), { timeout: 3_000, interval: 10 });
      expect(scan.mock.calls.filter(call => String(call[0]) === "/proc")).toHaveLength(1);
      expect(group.exited()).toBe(true);
    } finally {
      vi.restoreAllMocks();
      if (child) {
        await executionCgroups.get(child)?.signal("SIGKILL");
        releaseScopedChild(child);
        if (close) await closedWithin(close);
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);

  it("never execs an unadmitted target when its owner's gate pipe closes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-gate-"));
    const marker = path.join(root, "target-ran");
    const child = await spawnScopedExecution((binary, args, options) => spawn(binary, [...args], options),
      process.execPath, ["-e", `require('node:fs').writeFileSync(${JSON.stringify(marker)},'executed')`], { stdio: "ignore", detached: true });
    const close = new Promise<void>(resolve => child.once("close", () => resolve()));
    const receipt = executionCgroups.get(child);
    try {
      expect(receipt).toBeDefined();
      // Closing this owned fd models a crashed worker, without a polling gate.
      child.stdio[3]!.destroy();
      await closedWithin(close);
      expect(await receipt!.waitForExit(2_000)).toBe(true);
      expect(fs.existsSync(marker)).toBe(false);
    } finally {
      await receipt?.signal("SIGKILL"); releaseScopedChild(child); await closedWithin(close);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("preserves literal argv through systemd without environment expansion", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-argv-"));
    const marker = path.join(root, "argv.json");
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify(process.argv.slice(2)));`);
    const argv = ["$HOME", "$$", "a b", "single'quote", "$1", "${NOT_AN_ENV}"];
    const child = await spawnScopedExecution((binary, args, options) => spawn(binary, [...args], options),
      process.execPath, [worker, ...argv], { stdio: "ignore", detached: true });
    const close = new Promise<void>(resolve => child.once("close", () => resolve()));
    const receipt = executionCgroups.get(child);
    try {
      expect(receipt).toBeDefined(); releaseScopedChild(child); await closedWithin(close);
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toEqual(argv);
      expect(await receipt!.waitForExit(2_000)).toBe(true);
    } finally {
      await receipt?.signal("SIGKILL"); releaseScopedChild(child); await closedWithin(close);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["sibling", "inherited"] as const)("ProcessTransport retains %s execution scope after worker crash", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-parent-"));
    const marker = path.join(root, "execution.json"), crash = path.join(root, "crash");
    const leaf = path.join(root, "leaf.mjs");
    fs.writeFileSync(leaf, `import fs from 'node:fs'; process.on('SIGTERM',()=>process.exit(0));
      const fields=fs.readFileSync('/proc/'+process.pid+'/stat','utf8').split(') ').at(-1).split(' ');
      const cgroup='/sys/fs/cgroup'+fs.readFileSync('/proc/'+process.pid+'/cgroup','utf8').trim().split('::')[1];
      fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify({pid:process.pid,started:fields[19],cgroup}));setInterval(()=>{},1000);`);
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from 'node:fs'; import {spawn} from 'node:child_process'; import {randomUUID} from 'node:crypto';
      process.send({type:'fabric-execution-custody'});
      process.once('message',()=>{
        const child=${mode === "sibling"
          ? `spawn('/usr/bin/systemd-run',['--user','--scope','--unit=fabric-execution-'+randomUUID()+'.scope','--quiet','--collect','--','/usr/bin/setsid',process.execPath,${JSON.stringify(leaf)}],{detached:true,stdio:'ignore'})`
          : `spawn(process.execPath,[${JSON.stringify(leaf)}],{detached:true,stdio:'ignore'})`};child.unref();
        const ready=setInterval(()=>{if(!fs.existsSync(${JSON.stringify(marker)}))return;clearInterval(ready);
          const receipt=JSON.parse(fs.readFileSync(${JSON.stringify(marker)},'utf8'));
          process.send({type:'fabric-execution-started',...receipt});
          setInterval(()=>{if(fs.existsSync(${JSON.stringify(crash)}))process.exit(9)},10);
        },10);
      });`);
    const handle = await new ProcessTransport().launch({ id: "live", name: "live", cwd: root, workerPath: worker, workerArguments: [] });
    try {
      expect(handle.treeClosed, "default must be a cgroup, not the legacy path").toBeDefined();
      expect(handle.livenessPollIntervalMs).toBe(1_000);
      await vi.waitFor(() => expect(fs.existsSync(marker)).toBe(true), { timeout: 5_000 });
      const pid = JSON.parse(fs.readFileSync(marker, "utf8")).pid as number;
      fs.writeFileSync(crash, "crash"); await closedWithin(handle.closed!);
      const scan = vi.spyOn(fs, "readdirSync");
      expect(await handle.isAlive(), "dead custodian cannot release live execution").toBe(true);
      expect(scan.mock.calls.filter(call => String(call[0]) === "/proc")).toEqual([]);
      await handle.stop();
      expect(await handle.isAlive()).toBe(false);
      await vi.waitFor(() => expect(executing(pid)).toBe(false), { timeout: 3_000, interval: 10 });
      expect(scan.mock.calls.filter(call => String(call[0]) === "/proc").length).toBeLessThanOrEqual(2);
      expect(handle.lostContact?.()).toBeUndefined();
    } finally {
      vi.restoreAllMocks(); await handle.stop(); await handle.waitForClose?.();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);
});
