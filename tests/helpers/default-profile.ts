import os from "node:os";
import path from "node:path";

// pi-fabric#1562 Windows CI: the hosted ACL suite's fixtures belong below the runner's DEFAULT profile. Since
// baaf2cf64 (2026-10-14) isolateTestFleetEnvironment (scripts/test-temp.ts) points USERPROFILE (win32) or HOME
// (POSIX) at the file's fleet root inside TMPDIR, and Node's os.homedir() prefers that variable: "On Windows, it uses
// the USERPROFILE environment variable if defined" (https://nodejs.org/api/os.html#oshomedir). os.userInfo().homedir
// "is provided by the operating system. This differs from the result of os.homedir(), which queries environment
// variables" (https://nodejs.org/api/os.html#osuserinfooptions). On a hosted runner TMPDIR lies under RUNNER_TEMP
// (D:\a\_temp), whose volume ACL can grant Authenticated Users modify: an ancestor the gate rightly refuses.

const inside = (parent: string, child: string): boolean => {
  const relative = path.relative(path.resolve(parent), path.resolve(child));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
};

/** The OS-provided profile directory of the current user, never one inside TMPDIR or RUNNER_TEMP. */
export const defaultProfileDirectory = (): string => {
  const profile = path.resolve(os.userInfo().homedir);
  for (const temp of [os.tmpdir(), process.env.RUNNER_TEMP]) {
    if (temp && inside(temp, profile)) throw new Error(`the default profile ${profile} is inside the temporary directory ${temp}`);
  }
  return profile;
};

/** True when `child` is `parent` or below it (platform path rules). */
export const isWithin = inside;
