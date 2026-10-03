import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { allocateRunTmpDirectory, disposeRunTmpDirectory } from "../src/storage/run-scratch.js";
import { removeEmptyProcessScratchScope } from "../src/storage/process-scratch-scope.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { launchLog } from "./helpers/owned-processes.js";

const delegated = (() => {
  if (process.platform !== "linux") return false;
  try { const membership = fs.readFileSync("/proc/self/cgroup", "utf8").match(/^0::(\/.*)$/m)![1]!; fs.accessSync(path.join("/sys/fs/cgroup", membership), fs.constants.W_OK); return true; }
  catch { return false; }
})();

it.skipIf(!delegated)("records a scoped F4 worker's logical argv while retaining its native launch birth receipt", async () => {
  const project = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-owned-scratch-gate-")), run = path.join(project, "run"), worker = path.join(project, "worker.mjs"), report = path.join(project, "argv.json");
  fs.mkdirSync(run, { mode: 0o700 });
  fs.writeFileSync(worker, `import fs from "node:fs";fs.writeFileSync(${JSON.stringify(report)},JSON.stringify(process.argv.slice(1)));`);
  const log = launchLog(project);
  for (const [key, value] of Object.entries(log.env)) vi.stubEnv(key, value);
  const allocation = allocateRunTmpDirectory(run);
  let handle: Awaited<ReturnType<typeof spawnDetached>> | undefined;
  try {
    expect(allocation.scope).toBeDefined();
    handle = await spawnDetached(worker, ["spaces and quotes \"", "--flag"], project, undefined, { ...process.env, TMPDIR: allocation.directory }, allocation.scope);
    await handle.waitForClose(); expect(handle.lostContact()).toBeUndefined();
    const native = log.owned().find(entry => entry.pid === handle!.pid)!;
    expect(native).toBeDefined(); expect(native.started).toMatch(/^\d+$/);
    expect(native.argv).toEqual(JSON.parse(fs.readFileSync(report, "utf8")));
    expect(native.argv).toEqual([worker, "spaces and quotes \"", "--flag"]);
    await vi.waitFor(() => expect(disposeRunTmpDirectory(run)).toBe(true), { timeout: 2000 });
  } finally {
    await handle?.stop(); vi.unstubAllEnvs();
    if (allocation.scope) removeEmptyProcessScratchScope(allocation.scope);
    fs.rmSync(project, { recursive: true, force: true });
  }
});
