// PR #864 P1 crash probe. "hold [lockTimeoutMs]": acquire the mesh lock with an unknown PID
// namespace and die holding it (SIGKILL). "acquire <offsetMs>": a new process tries the lock with
// its clock moved forward by offsetMs, and prints ACQUIRED or REFUSED <code>.
import fs from "node:fs";
import path from "node:path";
import { MeshStore } from "../../src/mesh/store.js";

const [root, mode, offsetText] = process.argv.slice(2);
if (!root || !mode) throw new Error("usage: <root> hold [lockTimeoutMs] | acquire [offsetMs]");
const print = (line: string): void => { fs.writeSync(1, `${line}\n`); };

if (mode === "hold") {
  // Identity is computed on first use: make this process's PID namespace unreadable first.
  const readlink = fs.readlinkSync;
  fs.readlinkSync = ((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target) === "/proc/self/ns/pid") throw Object.assign(new Error("denied"), { code: "EACCES" });
    return (readlink as (...args: unknown[]) => unknown)(target, ...rest);
  }) as typeof fs.readlinkSync;
  const store = new MeshStore(root, 64 * 1024, 100, offsetText ? { lockTimeoutMs: Number(offsetText) } : {});
  await store.exclusive(() => {
    print(`HELD ${JSON.stringify(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8"))}`);
    process.kill(process.pid, "SIGKILL");
  });
  print("UNREACHABLE");
} else {
  const offset = Number(offsetText ?? 0);
  const realNow = Date.now;
  Date.now = () => realNow() + offset;
  const store = new MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 500 });
  try {
    print(await store.exclusive(() => "ACQUIRED"));
  } catch (error) {
    print(`REFUSED ${(error as { code?: string }).code ?? String(error)}`);
  }
}
