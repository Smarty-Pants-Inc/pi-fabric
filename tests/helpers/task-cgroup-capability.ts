import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { findExecutable } from "../../src/agents/transports/process-utils.js";

/** Real, test-owned scope probe: a bus socket alone cannot prove usable delegation. */
export const taskCgroupUnavailableReason = (): string | undefined => {
  try { if (fs.statfsSync("/sys/fs/cgroup").type !== 0x63677270) return "no cgroup-v2 mount"; }
  catch { return "no cgroup-v2 mount"; }
  if (process.getuid === undefined) return "no POSIX UID ownership";
  const executable = findExecutable("systemd-run");
  if (!executable) return "no systemd-run executable";
  const unit = `fabric-capability-${randomUUID()}.scope`;
  const probe = spawnSync(executable, ["--user", "--scope", "--quiet", "--collect", `--unit=${unit}`,
    "-p", "MemoryMax=1G", "-p", "MemorySwapMax=0", "-p", "Delegate=yes", "--", "/bin/sh", "-c",
    'IFS=: read -r hierarchy controllers group < /proc/self/cgroup; printf "%s\\n" "$group"; cat "/sys/fs/cgroup$group/memory.max" "/sys/fs/cgroup$group/memory.swap.max"; stat -c %u "/sys/fs/cgroup$group"',
  ], { encoding: "utf8", timeout: 10_000 });
  if (probe.error || probe.status !== 0) return `no usable owned user scope (${probe.error?.message ?? (probe.stderr.trim() || probe.status)})`;
  const [group, memory, swap, owner] = probe.stdout.trim().split("\n");
  if (!group?.endsWith(`/${unit}`) || memory !== String(1024 ** 3) || swap !== "0" || owner !== String(process.getuid())) {
    return `scope ownership/cap not delegated (${probe.stdout.trim()})`;
  }
  return undefined;
};
