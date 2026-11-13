import fs from "node:fs";
import path from "node:path";

export type LinuxGroupMember = { pid: number; parent: number; group: number; started: string; state: string };

export const linuxGroupMember = (pid: number, procRoot = "/proc"): LinuxGroupMember | undefined => {
  try {
    const stat = fs.readFileSync(path.join(procRoot, String(pid), "stat"), "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { pid, parent: Number(fields[1]), group: Number(fields[2]), started: fields[19]!, state: fields[0]! };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" || (error as NodeJS.ErrnoException).code === "ESRCH") return undefined;
    throw error; // Unknown identity is not permission to signal or to report exit.
  }
};

const live = (member: LinuxGroupMember | undefined): member is LinuxGroupMember =>
  member !== undefined && member.state !== "Z" && member.state !== "X";

const cgroupPids = (directory: string): Set<number> => {
  const pids = new Set<number>();
  const pending = [directory];
  // cgroup.procs covers only its own cgroup, not nested cgroups, and may repeat PIDs:
  // https://docs.kernel.org/admin-guide/cgroup-v2.html#core-interface-files
  while (pending.length) {
    const current = pending.pop()!;
    const procs = fs.readFileSync(path.join(current, "cgroup.procs"), "utf8").trim();
    for (const entry of procs ? procs.split(/\s+/) : []) {
      const pid = Number(entry);
      if (!/^\d+$/.test(entry) || !Number.isSafeInteger(pid) || pid <= 0) throw new Error("Unreadable cgroup.procs census");
      pids.add(pid);
    }
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      if (entry.isDirectory()) pending.push(path.join(current, entry.name));
    }
  }
  return pids;
};

/** Snapshot only: callers retain their birth, ownership and process-group checks.
 * cgroupDirectory must be an already verified dedicated scope, never the caller's ambient cgroup. */
export const linuxProcesses = (options: {
  cgroupDirectory?: string;
  ownedPids?: Iterable<number>;
  procRoot?: string;
} = {}): LinuxGroupMember[] => {
  const { cgroupDirectory, ownedPids, procRoot = "/proc" } = options;
  const full = (): LinuxGroupMember[] => fs.readdirSync(procRoot).flatMap((entry) => {
    if (!/^\d+$/.test(entry)) return [];
    const member = linuxGroupMember(Number(entry), procRoot);
    return live(member) ? [member] : [];
  });
  if (!cgroupDirectory) return full();
  let scoped: Set<number>;
  try { scoped = cgroupPids(cgroupDirectory); }
  catch { return full(); } // Missing, unreadable or partially walked scopes never prove exit.
  const candidates = new Set(scoped);
  for (const pid of ownedPids ?? []) candidates.add(pid);
  const snapshot: LinuxGroupMember[] = [];
  for (const pid of candidates) {
    const member = linuxGroupMember(pid, procRoot);
    if (!live(member)) continue;
    // A known live escape can fork or create another group outside the scope.
    // Keep the old whole-host discovery until that obligation is gone.
    if (!scoped.has(pid)) return full();
    snapshot.push(member);
  }
  // An empty scoped snapshot is not an exit receipt: check the whole host first.
  return snapshot.length ? snapshot : full();
};
