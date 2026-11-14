// Run: bun scripts/benchmark-process-census.ts
// smarty-dev#20387: measure the actual helper on a host-sized real filesystem
// fixture, not a mocked reader or a second implementation of the census.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { linuxProcesses, type LinuxGroupMember } from "../src/agents/transports/linux-process-census.js";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-benchmark-process-census-"));
try {
  const procRoot = path.join(directory, "proc");
  const cgroupDirectory = path.join(directory, "scope");
  const nested = path.join(cgroupDirectory, "execution", "nested");
  fs.mkdirSync(procRoot);
  fs.mkdirSync(nested, { recursive: true });
  const put = (pid: number, parent: number, group: number): void => {
    const fields = Array<string>(20).fill("0");
    fields[0] = "S"; fields[1] = String(parent); fields[2] = String(group); fields[19] = `${pid}00`;
    const processDirectory = path.join(procRoot, String(pid));
    fs.mkdirSync(processDirectory);
    fs.writeFileSync(path.join(processDirectory, "stat"), `${pid} (benchmark worker) ${fields.join(" ")}\n`);
  };
  const hostCount = 5_000;
  for (let pid = 1_000; pid < 1_000 + hostCount; pid++) put(pid, 1, pid);
  const primary = 100;
  const processTree = [
    [primary, 1, primary], [101, primary, primary],
    [102, primary, 102], [103, 102, 102],
    [104, 103, 104], [105, 104, 104],
  ] as const;
  for (const [pid, parent, group] of processTree) put(pid, parent, group);
  const writeScope = (relative: string, pids: number[]) =>
    fs.writeFileSync(path.join(cgroupDirectory, relative, "cgroup.procs"), `${pids.join("\n")}\n`);
  writeScope("", [100, 101]);
  writeScope("execution", [102, 103, 103]); // legitimate migration duplicate
  writeScope("execution/nested", [104, 105]);
  const ownedPids = new Set<number>(processTree.map(([pid]) => pid));
  const ownedGroups = new Set<number>(processTree.map(([, , group]) => group));
  const related = (members: LinuxGroupMember[]) => members
    .filter(member => ownedPids.has(member.pid) || ownedGroups.has(member.group))
    .sort((left, right) => left.pid - right.pid);
  const full = () => linuxProcesses({ procRoot });
  const scoped = () => linuxProcesses({ procRoot, cgroupDirectory, ownedPids });
  const expected = full();
  assert.equal(expected.length, hostCount + processTree.length);
  assert.deepEqual(related(expected).map(member => member.pid), processTree.map(([pid]) => pid));
  assert.deepEqual(related(scoped()), related(expected));

  // Warm filesystem/code caches. Alternate order to avoid always charging the
  // same route for incidental host load. Timings are observations, not gates.
  full(); scoped();
  const samples = { full: [] as number[], scoped: [] as number[] };
  const counts = { full: 0, scoped: 0 };
  const rounds = 20;
  const measure = (name: keyof typeof samples, census: () => LinuxGroupMember[]) => {
    const start = performance.now();
    const members = census();
    samples[name].push(performance.now() - start);
    counts[name] = members.length;
    // Compare the resulting owned/group membership, not the host inventory:
    // omitting unrelated processes is the intended fast path.
    assert.deepEqual(related(members), related(expected));
  };
  for (let round = 0; round < rounds; round++) {
    if (round % 2 === 0) { measure("full", full); measure("scoped", scoped); }
    else { measure("scoped", scoped); measure("full", full); }
  }
  const summarize = (name: keyof typeof samples) => {
    const sorted = [...samples[name]].sort((a, b) => a - b);
    const mean = sorted.reduce((sum, value) => sum + value, 0) / sorted.length;
    const median = (sorted[rounds / 2 - 1]! + sorted[rounds / 2]!) / 2;
    return { mean, row: {
      route: name === "full" ? "full /proc (previous census)" : "scoped cgroup.procs + owned PIDs",
      calls: rounds, returned: counts[name], related: processTree.length,
      meanMsPerCall: mean.toFixed(3), medianMsPerCall: median.toFixed(3),
      minMsPerCall: sorted[0]!.toFixed(3), maxMsPerCall: sorted.at(-1)!.toFixed(3),
    } };
  };
  const fullResult = summarize("full");
  const scopedResult = summarize("scoped");
  console.log(`Real fake-proc entries: ${hostCount + processTree.length}; scoped tree: ${processTree.length}; nested cgroups: 3`);
  console.table([fullResult.row, scopedResult.row]);
  console.log(`Owned/group membership equality: PASS (${rounds * 2} measured calls, ${processTree.length} related members)`);
  console.log(`Observed mean full/scoped ratio: ${(fullResult.mean / scopedResult.mean).toFixed(2)}x (informational; no timing assertion)`);
} finally {
  // This benchmark allocated this exact mkdtemp root; no host proc/cgroup files
  // or pre-existing directories are removed.
  fs.rmSync(directory, { recursive: true, force: true });
}
