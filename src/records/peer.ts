import { spawn } from "node:child_process";
import fs from "node:fs";
import type net from "node:net";

/** A connection's peer, as the kernel reports it (SO_PEERCRED), plus what /proc shows of it. */
export interface PeerInfo { pid: number; uid: number; gid: number; cmdline?: string; cwd?: string }

// ponytail: Node exposes no SO_PEERCRED; a one-line helper reads it from a duplicate of the
// connection's descriptor, once per connection. Without python3 the audit records nothing.
// The descriptor goes to the child as fd 3, and getsockopt(2) is called through ctypes: both
// leave the shared descriptor non-blocking. As fd 0-2, or as a Python socket object, it would be
// switched to blocking mode, and one large response would then block the whole service.
const PEERCRED = "import ctypes,struct; l=ctypes.CDLL(None,use_errno=True); b=ctypes.create_string_buffer(12); n=ctypes.c_uint32(12); "
  + "r=l.getsockopt(3,1,17,b,ctypes.byref(n)); print(*struct.unpack('3i', b.raw)) if r==0 else None";

export const peerCredentials = (socket: net.Socket, timeoutMs = 5_000): Promise<PeerInfo | undefined> =>
  new Promise((resolve) => {
    let out = "";
    let child: ReturnType<typeof spawn>;
    // The raw descriptor, not the stream: handing the stream to spawn would stop this process
    // reading the connection. The child gets a duplicate and only asks the kernel about it.
    const fd = (socket as unknown as { _handle?: { fd?: unknown } })._handle?.fd;
    if (typeof fd !== "number" || fd < 0) {
      resolve(undefined);
      return;
    }
    try {
      child = spawn("python3", ["-c", PEERCRED], { stdio: ["ignore", "pipe", "ignore", fd], timeout: timeoutMs });
    } catch {
      resolve(undefined);
      return;
    }
    child.stdout?.setEncoding("utf8");
    child.stdout?.on("data", (chunk: string) => { out += chunk; });
    child.on("error", () => resolve(undefined));
    child.on("close", () => {
      const [pid, uid, gid] = out.trim().split(/\s+/).map(Number);
      if (!Number.isSafeInteger(pid) || !Number.isSafeInteger(uid) || !Number.isSafeInteger(gid) || !pid) {
        resolve(undefined);
        return;
      }
      resolve({ pid: pid!, uid: uid!, gid: gid!, ...procInfo(pid!) });
    });
  });

/** cmdline is world-readable; cwd is readable only for the service's own uid (ptrace rules). */
const procInfo = (pid: number): { cmdline?: string; cwd?: string } => {
  const info: { cmdline?: string; cwd?: string } = {};
  try { info.cmdline = fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0").filter(Boolean).join(" ").slice(0, 1024); } catch { /* gone or hidden */ }
  try { info.cwd = fs.readlinkSync(`/proc/${pid}/cwd`); } catch { /* another uid: not visible */ }
  return info;
};

/** Whether a process still exists (EPERM means it exists under another uid). */
export const processAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
};
