import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, JOINED_SCRATCH_FILE, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";
import { removeEmptyProcessScratchScope } from "../src/storage/process-scratch-scope.js";
import { scratchEvidence } from "./scratch-evidence.js";

const nativeBunScope = (() => {
  if (process.platform !== "linux") return false;
  try {
    execFileSync("bun", ["--version"], { stdio: "ignore" });
    const membership = fs.readFileSync("/proc/self/cgroup", "utf8").trim().match(/^0::(\/.*)$/)![1]!;
    fs.accessSync(path.join("/sys/fs/cgroup", membership), fs.constants.W_OK);
    return true;
  } catch { return false; }
})();

// Observe the fixture's birth identity / zombie state, never signal an orphan
// by numeric PID: worker exit does not give ownership of its detached helper.
const identity = (pid: number): string | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    return fields[0] === "Z" ? undefined : fields[19];
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return undefined;
    throw error;
  }
};

describe("Bun configuration scratch custody (#369 F4)", () => {
  it.skipIf(!nativeBunScope)("does not start Bun or its config when the attachment gate fails", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bun-gate-refusal-"));
    const allocation = allocateRunTmpDirectory(root);
    let handle: Awaited<ReturnType<typeof spawnDetached>> | undefined;
    try {
      expect(allocation.scope).toBeDefined();
      fs.writeFileSync(path.join(root, "bunfig.toml"), 'preload = ["./preload.ts"]\n');
      fs.writeFileSync(path.join(root, "preload.ts"), 'import fs from "node:fs"; fs.writeFileSync("preload-started", "unsafe");');
      const worker = path.join(root, "worker.ts");
      fs.writeFileSync(worker, 'import fs from "node:fs"; fs.writeFileSync("worker-started", "unsafe");');
      handle = await spawnDetached(worker, [], root, undefined, { ...process.env, TMPDIR: allocation.directory }, { ...allocation.scope!, directory: path.join(root, "missing-scope") });
      await handle.waitForClose();
      expect(handle.lostContact()).toBeUndefined();
      expect(fs.existsSync(path.join(root, "preload-started"))).toBe(false);
      expect(fs.existsSync(path.join(root, "worker-started"))).toBe(false);
      expect(fs.existsSync(path.join(root, JOINED_SCRATCH_FILE))).toBe(false);
      expect(disposeRunTmpDirectory(root)).toBe(false);
      expect(fs.existsSync(path.join(root, UNRESOLVED_SCRATCH_FILE))).toBe(true);
      expect(fs.existsSync(allocation.directory)).toBe(true);
    } finally {
      await handle?.stop();
      if (allocation.scope) removeEmptyProcessScratchScope(allocation.scope);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(!nativeBunScope)("contains a bunfig preload's redirected surviving helper before runtime startup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bun-preload-"));
    const run = path.join(root, "run"); fs.mkdirSync(run, { mode: 0o700 });
    const release = path.join(root, "release"), ready = path.join(root, "ready.json"), spawned = path.join(root, "spawned");
    const worker = path.join(root, "worker.ts"), writer = path.join(root, "writer.mjs");
    const allocation = allocateRunTmpDirectory(run);
    let handle: Awaited<ReturnType<typeof spawnDetached>> | undefined;
    try {
      // The ambient guards are all absent; the execution hook is project config.
      for (const key of ["NODE_OPTIONS", "BUN_OPTIONS", "LD_PRELOAD", "LD_AUDIT"]) expect(process.env[key]?.trim()).toBeFalsy();
      expect(allocation.scope).toBeDefined();
      fs.writeFileSync(path.join(root, "bunfig.toml"), 'preload = ["./preload.ts"]\n');
      fs.writeFileSync(writer, `import fs from "node:fs";
fs.writeFileSync(process.env.TMPDIR + "/live-writer", "ongoing preload work");
fs.writeFileSync(${JSON.stringify(ready)}, JSON.stringify({pid:process.pid,cgroup:fs.readFileSync("/proc/self/cgroup","utf8").trim(),tmpdir:process.env.TMPDIR}));
const timer = setInterval(() => {
  if (fs.existsSync(${JSON.stringify(release)})) { clearInterval(timer); process.exit(0); }
}, 10);
setTimeout(() => { clearInterval(timer); process.exit(1); }, 15000).unref();
`);
      fs.writeFileSync(path.join(root, "preload.ts"), `import fs from "node:fs";
import { spawn } from "node:child_process";
const child = spawn(${JSON.stringify(process.execPath)}, [${JSON.stringify(writer)}], {detached:true,stdio:"ignore",env:process.env});
fs.writeFileSync(${JSON.stringify(spawned)}, String(child.pid));
child.unref();
`);
      fs.writeFileSync(worker, `import fs from "node:fs";
const deadline = Date.now() + 10000;
while (!fs.existsSync(${JSON.stringify(ready)})) {
  if (Date.now() > deadline) throw new Error("Preload helper did not start");
  await new Promise(resolve => setTimeout(resolve,10));
}
fs.writeFileSync(${JSON.stringify(path.join(root, "worker.json"))}, JSON.stringify({cgroup:fs.readFileSync("/proc/self/cgroup","utf8").trim(),tmpdir:process.env.TMPDIR,argv:process.argv.slice(2)}));
`);
      handle = await spawnDetached(worker, ["argument with spaces", "$(not-shell-code)"], root, undefined, { ...process.env, TMPDIR: allocation.directory }, allocation.scope);
      await handle.waitForClose();
      expect(handle.lostContact()).toBeUndefined();
      const helper = JSON.parse(fs.readFileSync(ready, "utf8"));
      const report = JSON.parse(fs.readFileSync(path.join(root, "worker.json"), "utf8"));
      const helperBirth = identity(helper.pid);
      expect(helperBirth).toBeDefined();
      expect(fs.existsSync(path.join(run, JOINED_SCRATCH_FILE))).toBe(true);
      expect(report.cgroup).toBe("0::" + allocation.scope!.directory.slice("/sys/fs/cgroup".length));
      expect(report.argv).toEqual(["argument with spaces", "$(not-shell-code)"]);
      // On 90d613ba this returns true: the worker scope is empty, but the
      // pre-attachment helper is still alive with this private scratch path.
      expect(disposeRunTmpDirectory(run), "preload helper still owns scratch after worker exit").toBe(false);
      expect(helper.cgroup).toBe(report.cgroup);
      expect(helper.tmpdir).toBe(allocation.directory);
      expect(fs.readFileSync(path.join(allocation.directory, "live-writer"), "utf8")).toBe("ongoing preload work");
      expect(fs.existsSync(path.join(run, UNRESOLVED_SCRATCH_FILE))).toBe(true);
      fs.writeFileSync(release, "finish");
      await vi.waitFor(() => expect(disposeRunTmpDirectory(run)).toBe(true), { timeout: 3000, interval: 20 });
      expect(fs.existsSync(allocation.directory)).toBe(false);
      scratchEvidence("f4-bun-preload", { scope: allocation.scope, workerPid: handle.pid,
        helper, worker: report, helperBirthAtLiveCheck: helperBirth, helperBirthAfterExit: identity(helper.pid) ?? null,
        configuredHook: "bunfig.toml preload", workerJoined: true,
        scratchPreservedWhileHelperLive: true, helperExitConfirmed: true,
        scratchRemovedAfterHelperExit: true, scopeRemoved: !fs.existsSync(allocation.scope!.directory) });
    } finally {
      // Also drain the real orphan on red / assertion failures before removing
      // fixture files. The bounded helper self-exit is a second safety net.
      await handle?.stop();
      fs.writeFileSync(release, "finish");
      if (fs.existsSync(spawned)) {
        const pid = Number(fs.readFileSync(spawned, "utf8")), birth = identity(pid);
        if (birth !== undefined) await vi.waitFor(() => expect(identity(pid)).not.toBe(birth), { timeout: 17000, interval: 20 });
      }
      if (allocation.scope) await vi.waitFor(() => expect(fs.existsSync(allocation.scope!.directory) ? fs.readFileSync(path.join(allocation.scope!.directory, "cgroup.events"), "utf8") : "populated 0").toMatch(/^populated 0$/m), { timeout: 2000, interval: 20 });
      if (allocation.scope) removeEmptyProcessScratchScope(allocation.scope);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 25000);
});
