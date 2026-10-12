import fs from "node:fs";
import cp from "node:child_process";
import os from "node:os";
import { BASH_IDLE_EXIT_CODE, BASH_IDLE_TERM_GRACE_S } from "./guards/bash-idle-policy.js";

// Both modes execute this real file: never put watchdog or custody code in a shell -e operand.
// The detached envelope retains IPC custody while the command handles TERM during grace.
if (process.argv[2] === "--group") {
  process.on("SIGTERM", () => {});
  process.on("disconnect", () => {
    try { process.kill(-process.pid, "SIGKILL"); } catch { process.exit(1); }
  });
  const [sh, command] = process.argv.slice(-2) as [string, string];
  const child = cp.spawn(sh, ["-c", command], { stdio: ["ignore", "inherit", "inherit"] });
  child.on("error", error => { process.stderr.write(String(error) + "\n"); process.exit(127); });
  child.on("exit", (code, signal) => process.exit(code ?? 128 + (os.constants.signals[signal!] || 0)));
} else {
  // A detached group catches reparented jobs; Linux /proc traversal additionally catches
  // setsid children, retaining start times across TERM/reparenting to avoid reused PIDs.
  // A double-forked setsid daemon that leaves before discovery remains outside custody (#7934).
  const [seconds, sh] = process.argv.slice(-2) as [string, string];
  const n = Number(seconds);
  const child = cp.spawn(process.execPath, [process.argv[1]!, "--group", sh, fs.readFileSync(0, "utf8")], {
    detached: true, stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  let timer: ReturnType<typeof setTimeout> | undefined, grace: ReturnType<typeof setTimeout> | undefined;
  let code = 1, exited = false, idle = false, done = false;
  const tracked = new Map<number, string>();
  const finish = () => {
    if (done) return;
    done = true;
    clearTimeout(timer); clearTimeout(grace);
    let pending = 2;
    const flushed = () => { if (--pending === 0) process.exit(idle ? BASH_IDLE_EXIT_CODE : code); };
    process.stdout.write("", flushed); process.stderr.write("", flushed);
  };
  const rows = () => {
    const result = new Map<number, { pp: number; pg: number; start: string }>();
    try {
      for (const name of fs.readdirSync("/proc")) {
        if (!/^\d+$/.test(name)) continue;
        try {
          const stat = fs.readFileSync("/proc/" + name + "/stat", "utf8");
          const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
          result.set(Number(name), { pp: Number(fields[1]), pg: Number(fields[2]), start: fields[19]! });
        } catch { /* process exited */ }
      }
    } catch { /* /proc is unavailable off Linux */ }
    return result;
  };
  const sweep = (signal: NodeJS.Signals) => {
    const all = rows(), owned = new Set([child.pid!]);
    for (const [pid, start] of tracked) if (all.get(pid)?.start === start) owned.add(pid);
    for (let grew = true; grew;) {
      grew = false;
      for (const [pid, row] of all) {
        if ((owned.has(row.pp) || row.pg === child.pid) && !owned.has(pid)) { owned.add(pid); grew = true; }
      }
    }
    for (const pid of owned) { const row = all.get(pid); if (row) tracked.set(pid, row.start); }
    try { process.kill(-child.pid!, signal); } catch { /* group exited */ }
    for (const [pid, start] of tracked) {
      const row = all.get(pid);
      if (row?.start === start && row.pg !== child.pid) try { process.kill(pid, signal); } catch { /* exited */ }
    }
  };
  const expire = () => {
    idle = true;
    process.stderr.write("\n[pi-fabric] bash idle timeout: no output for " + n + " s; killed; rerun with a bounded range or a command that prints progress\n");
    sweep("SIGTERM");
    grace = setTimeout(() => { sweep("SIGKILL"); finish(); }, BASH_IDLE_TERM_GRACE_S * 1000);
  };
  const arm = () => { clearTimeout(timer); if (!exited && !idle) timer = setTimeout(expire, n * 1000); };
  const forward = (stream: NodeJS.WriteStream) => (data: Buffer) => {
    stream.write(data);
    if (idle) return;
    if (exited) { clearTimeout(grace); grace = setTimeout(finish, 100); } else arm();
  };
  child.stdout!.on("data", forward(process.stdout)); child.stderr!.on("data", forward(process.stderr));
  child.on("error", error => { process.stderr.write(String(error) + "\n"); code = 127; if (!idle) finish(); });
  child.on("exit", (status, signal) => {
    exited = true; clearTimeout(timer);
    code = status ?? 128 + (os.constants.signals[signal!] || 0);
    if (!idle) { clearTimeout(grace); grace = setTimeout(finish, 100); }
  });
  child.on("close", () => { if (!idle) finish(); });
  arm();
}
