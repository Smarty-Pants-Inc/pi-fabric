import fs from "node:fs";
import { spawnSync } from "node:child_process";

let fenceAvailable: boolean | undefined;
/** Automated recovery must never use the racy PID-file fallback. Probe at first use only. */
export const kernelFenceAvailable = (): boolean => {
  if (process.platform !== "linux") return false;
  return fenceAvailable ??= spawnSync("setpriv", ["--pdeathsig", "KILL", "flock", "--version"],
    { stdio: "ignore", timeout: 1_000 }).status === 0;
};

export class FileLockBusy extends Error {}

/**
 * waitSeconds=0 is non-blocking; requireParentDeath forbids the records-compatible
 * fallback without setpriv (resident Linux hosts must never weaken their fence).
 * Take an exclusive flock(2) on `file` (created 0600, never followed, owned by this user) and return its fd;
 * closing the fd, or the process's death, releases it.
 */
// ponytail: util-linux flock(1) locks the open file description it inherits as fd 3, which this process keeps
// open after the child exits; no native addon, and the wait does not block the event loop.
// #1720: the wait is bounded (`flock -w`), and on Linux `setpriv --pdeathsig KILL` kills the helper when this
// process dies, so a killed issuer never leaves a waiter behind. Without setpriv only the deadline bounds it.

export const lockFile = async (file: string, waitSeconds = 120, requireParentDeath = false): Promise<number> => {
  const fd = fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== process.getuid?.()) throw new Error(`${file} is not a regular file owned by this user`);
    const { spawn } = await import("node:child_process");
    const flock = waitSeconds === 0 ? ["flock", "-x", "-n", "3"] : ["flock", "-x", "-w", String(waitSeconds), "3"];
    const run = (argv: string[]) => new Promise<number | null>((resolve, reject) => {
      const child = spawn(argv[0]!, argv.slice(1), { stdio: ["ignore", "ignore", "inherit", fd] });
      child.on("error", reject);
      child.on("exit", (status) => resolve(status));
    });
    const code = process.platform === "linux"
      ? await run(["setpriv", "--pdeathsig", "KILL", ...flock]).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT" && !requireParentDeath) return run(flock);
        throw error;
      })
      : await run(flock);
    // flock(1) exits 1 when -w expires.
    if (code === 1) throw new FileLockBusy(`timed out after ${waitSeconds}s waiting for lock ${file}`);
    if (code !== 0) throw new Error(`cannot lock ${file} (flock exited ${code})`);
    return fd;
  } catch (error) {
    fs.closeSync(fd);
    throw error;
  }
};
