import fs from "node:fs";
import path from "node:path";

const isSocket = (file: string): boolean => {
  try { return fs.statSync(file).isSocket(); } catch { return false; }
};

/** Cheap, fail-closed proof of delegated cgroup-v2 and a usable user bus. */
export const delegatedUserCgroupAvailable = (
  environment: NodeJS.ProcessEnv = process.env,
  cgroupRoot = "/sys/fs/cgroup",
): boolean => {
  if (process.platform !== "linux") return false;
  const uid = process.getuid?.();
  if (uid === undefined) return false;
  const runtime = environment.XDG_RUNTIME_DIR;
  const bus = environment.DBUS_SESSION_BUS_ADDRESS?.match(/^unix:path=([^,;]+)/)?.[1];
  const busUsable = (runtime && path.isAbsolute(runtime) && isSocket(path.join(runtime, "bus")))
    || (bus && path.isAbsolute(bus) && isSocket(bus));
  if (!busUsable) return false;
  try {
    if (!fs.statSync(path.join(cgroupRoot, "cgroup.controllers")).isFile()) return false;
    const manager = path.join(cgroupRoot, "user.slice", `user-${uid}.slice`, `user@${uid}.service`);
    return fs.readFileSync(path.join(manager, "cgroup.subtree_control"), "utf8").split(/\s+/).includes("memory");
  } catch { return false; }
};
