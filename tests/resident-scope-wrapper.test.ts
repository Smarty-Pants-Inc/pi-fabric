import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { taskScopeWrapperSource } from "../src/agents/transports/task-scope-wrapper.js";

const ownCgroup = (() => {
  if (process.platform !== "linux") return undefined;
  try {
    const line = fs.readFileSync("/proc/self/cgroup", "utf8").trim();
    return line.startsWith("0::/") && !line.includes("\n") ? line.slice(3) : undefined;
  } catch { return undefined; }
})();
const CAP = 24 * 1024 ** 3;
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = (residentShared: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-shared-wrapper-")); roots.push(root);
  const real = path.join(root, "real-systemd-run");
  const record = path.join(root, "real-args");
  fs.writeFileSync(real, `#!/bin/sh\nprintf '%s\\n' "$@" > '${record}'\nexit 42\n`, { mode: 0o700 });
  const wrapper = path.join(root, "wrapper");
  // The fourth argument opts into a shared resident outer scope. This fixture only executes requests whose
  // cap is unchanged; the lower-cap case must be refused before any cgroup write or real systemd-run dispatch.
  fs.writeFileSync(wrapper, taskScopeWrapperSource(real, ownCgroup!, CAP, residentShared), { mode: 0o700 });
  const run = (args: string[]) => spawnSync(wrapper, args, { encoding: "utf8", timeout: 5_000,
    env: { PATH: "/usr/bin:/bin", FABRIC_TASK_CGROUP: ownCgroup!, FABRIC_TASK_SYSTEMD_RUN_WRAPPER: wrapper } });
  return { run, record };
};

describe.skipIf(!ownCgroup)("task systemd-run wrapper in a resident shared scope", () => {
  it("refuses a lower MemoryMax with a typed resident-cap error and never dispatches", () => {
    const f = fixture(true);
    const result = f.run(["--user", "--scope", "-p", "MemoryMax=20G", "-p", "MemorySwapMax=0", "--", "true"]);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toMatch(/fabric-resident-shared-scope: code=RESIDENT_SHARED_CAP_TIGHTENING requested=\d+ current=\d+/);
    expect(fs.existsSync(f.record)).toBe(false);
  });

  it("executes the exact existing 24G/swap-zero request in-place in the same scope", () => {
    const f = fixture(true);
    const command = ["/bin/sh", "-c", 'printf "%s\\n" "$$"; cat /proc/self/cgroup'];
    const result = f.run(["--user", "--scope", "-p", "MemoryMax=24G", "-p", "MemorySwapMax=0", "--", ...command]);
    expect(result.status, result.stderr).toBe(0);
    const [pid, cgroup] = result.stdout.trim().split("\n");
    expect(Number(pid)).toBe(result.pid);
    expect(cgroup).toBe(`0::${ownCgroup}`);
    expect(fs.existsSync(f.record)).toBe(false);
  });

});
