import fs from "node:fs";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody } from "../src/process-cgroup.js";

afterEach(() => vi.restoreAllMocks());
describe.skipIf(process.platform !== "linux")("documented same-UID escape residual (#7478)", () => {
  it("never scans or adopts same-session/group siblings outside the pinned scope", async () => {
    const own = "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/fabric-execution-owned.scope";
    vi.spyOn(fs, "statSync").mockReturnValue({ dev: 1, ino: 1 } as fs.Stats);
    vi.spyOn(fs, "fstatSync").mockReturnValue({ dev: 1, ino: 1 } as fs.Stats);
    vi.spyOn(fs, "openSync").mockReturnValue(42);
    vi.spyOn(fs, "closeSync").mockImplementation(() => {});
    vi.spyOn(fs, "accessSync").mockImplementation(() => {});
    vi.spyOn(fs, "watch").mockImplementation(() => Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher);
    const scan = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("SID/PGID is not sibling custody"); });
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      if (String(file).endsWith("cgroup.events")) return "populated 1\nfrozen 1\n";
      if (String(file).endsWith("cgroup.procs")) return "100\n";
      if (String(file) === "/proc/100/cgroup") return `0::${own.slice("/sys/fs/cgroup".length)}\n`;
      if (String(file) === "/proc/100/stat") {
        const fields = Array<string>(20).fill("0");
        fields[0] = "S"; fields[2] = "100"; fields[3] = "100"; fields[19] = "1000";
        return `100 (fixture) ${fields.join(" ")}`;
      }
      throw new Error(`Unowned read: ${String(file)}`);
    });
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation(() => {});
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const receipt = cgroupCustody(own, { pid: 100, parent: 1, group: 100, session: 100, started: "1000" });
    try {
      await receipt.signal("SIGTERM"); await receipt.signal("SIGKILL");
      // No out-of-scope PID is discovered or signalled; a matching SID/PGID does
      // not give permission to freeze a sibling scope. Real escape stays alive
      // in cgroup-scope-live.test.ts until explicit fixture-custody cleanup.
      expect(kill).not.toHaveBeenCalled();
      expect(write.mock.calls.map(call => String(call[0]))).toEqual([
        "/proc/self/fd/42/cgroup.kill",
      ]);
      expect(scan).not.toHaveBeenCalled();
      expect(read.mock.calls.every(call => String(call[0]).startsWith("/proc/self/fd/42/") || ["/proc/100/stat", "/proc/100/cgroup"].includes(String(call[0])))).toBe(true);
    } finally { receipt.dispose(); }
  });
});
