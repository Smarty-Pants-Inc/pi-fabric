import fs from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { checkResidentSessionExit, observeProcessTree, stopOwnedSessionDescendants } from "../src/residency/launcher-owner.js";

const stat = (pid: number, ppid: number, session: number, state = "S", birth = "100") =>
  `${pid} (fixture with ) parentheses) ${state} ${ppid} ${session} ${session} ${Array(15).fill("0").join(" ")} ${birth}`;
const fixture = (rows: Record<string, string | Error>, mounts = "proc /proc proc rw 0 0") => {
  const read = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file) === "/proc/mounts") return mounts;
    const match = /^\/proc\/(\d+)\/stat$/.exec(String(file));
    if (match) {
      const row = rows[match[1]!];
      if (row instanceof Error) throw row;
      if (row === undefined) throw Object.assign(new Error("gone"), { code: "ENOENT" });
      return row;
    }
    return (read as Function)(file, ...args);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "readdirSync").mockImplementation((() => Object.keys(rows)) as unknown as typeof fs.readdirSync);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    expect(pid).toBe(-77); expect(signal).toBe(0);
    throw Object.assign(new Error("gone"), { code: "ESRCH" });
  });
};
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe.skipIf(process.platform !== "linux")("checked resident session exit (never whole-attempt authorization)", () => {
  it("checks reparented and zombie members, not a descendant sample", () => {
    fixture({ "10": stat(10, 1, 77), "11": stat(11, 1, 77, "Z"), "12": stat(12, 1, 88) });
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: false, members: [10, 11] });
  });
  it("reports the necessary empty boundary only when every member is gone", () => {
    fixture({ "12": stat(12, 1, 88), "13": Object.assign(new Error("exited during read"), { code: "ENOENT" }) });
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: true, members: [] });
  });
  it("does not treat a member born after enumeration as group exit", () => {
    fixture({}); vi.mocked(process.kill).mockReturnValue(true);
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: false, reason: "owned process group still exists" });
  });
  it.each(["EACCES", "EIO"])("fails closed for a %s member instead of sampling past it", code => {
    fixture({ "12": Object.assign(new Error("unreadable member"), { code }) });
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: false, reason: expect.stringContaining("unproven") });
  });
  it("fails closed for restricted proc visibility and invalid rows", () => {
    fixture({}, "proc /proc proc rw,hidepid=2 0 0");
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: false, reason: "restricted proc visibility" });
    vi.restoreAllMocks(); fixture({ "12": "broken stat" });
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: false, reason: "invalid proc stat" });
  });
  it("fails closed for an enumeration error, group probe error or missing session identity", () => {
    fixture({}); vi.mocked(fs.readdirSync).mockImplementation(() => { throw new Error("enumeration failed"); });
    expect(checkResidentSessionExit(77).empty).toBe(false);
    vi.restoreAllMocks(); fixture({}); vi.mocked(process.kill).mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    expect(checkResidentSessionExit(77).empty).toBe(false);
    expect(checkResidentSessionExit(undefined).empty).toBe(false);
  });
});

// Keep reason: F1's real owner-loss/double-fork incident crosses a PID/birth
// trust boundary. Synthetic proc changes reach reuse/read races cheaply; the
// parent-owned native fixture verifies the actual reparented process shutdown.
describe.skipIf(process.platform !== "linux")("owned OLD session descendant shutdown", () => {
  const setup = (extra: Record<string, string | Error> = {}) => {
    vi.useFakeTimers();
    const rows: Record<string, string | Error> = { "77": stat(77, 9, 77), ...extra };
    fixture(rows);
    const signals: Array<[number, string | number | undefined]> = [];
    vi.mocked(process.kill).mockImplementation((pid, signal) => {
      if (signal === 0) {
        expect(pid).toBe(-77);
        if (Object.values(rows).some(row => typeof row === "string" && row.split(") ").at(-1)!.split(" ")[3] === "77")) return true;
        throw Object.assign(new Error("gone"), { code: "ESRCH" });
      }
      expect(pid).toBeGreaterThan(0); expect(pid).not.toBe(77);
      signals.push([pid, signal]); delete rows[String(pid)]; return true;
    });
    const attempt = observeProcessTree(77);
    return { rows, signals, attempt };
  };
  it("records and stops an already-reparented same-SID member by its exact birth, excluding child and successor", async () => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77, "S", "101"), "88": stat(88, 1, 88, "S", "102") });
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    expect(attempt.processes.get(10)).toMatchObject({ pid: 10, ppid: 1, processStartTime: "101" });
    expect(signals).toEqual([[10, "SIGTERM"]]);
    delete rows["77"]; await vi.runAllTimersAsync(); await stopped;
    expect(rows["88"]).toBeDefined(); expect(attempt.processes.has(88)).toBe(false);
  });
  it("waits for native child shutdown without signaling the direct child", async () => {
    const { rows, signals, attempt } = setup();
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    setTimeout(() => { delete rows["77"]; }, 80);
    await vi.runAllTimersAsync(); await stopped;
    expect(signals).toEqual([]);
  });
  it.each([undefined, 88])("rejects an unanchored requested child %s without signaling", async child => {
    const { signals, attempt } = setup();
    await expect(stopOwnedSessionDescendants(attempt, child)).rejects.toThrow("launch-recorded");
    expect(signals).toEqual([]);
  });
  it.each(["gone", "reused", "different session"])("rejects an initially %s child instead of trusting an arbitrary SID", async kind => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77) });
    if (kind === "gone") rows["77"] = Object.assign(new Error("gone"), { code: "ENOENT" });
    else rows["77"] = stat(77, 9, kind === "different session" ? 88 : 77, "S", kind === "reused" ? "200" : "100");
    await expect(stopOwnedSessionDescendants(attempt, 77)).rejects.toThrow();
    expect(signals).toEqual([]);
  });
  it("reenters after native child exit using a previously birth-validated old SID member", async () => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77, "S", "101"), "88": stat(88, 1, 88, "S", "102") });
    const kill = vi.mocked(process.kill).getMockImplementation()!;
    let denied = true;
    vi.mocked(process.kill).mockImplementation((pid, signal) => {
      if (signal !== 0 && denied) { denied = false; throw Object.assign(new Error("retry cleanup"), { code: "EPERM" }); }
      return kill(pid, signal);
    });
    const first = stopOwnedSessionDescendants(attempt, 77); // Starts before native stop.
    delete rows["77"]; // Caller has independently stopped/joined the native child.
    await expect(first).rejects.toThrow("retry cleanup");
    expect(attempt.processes.get(10)?.processStartTime).toBe("101");
    const retried = stopOwnedSessionDescendants(attempt, 77);
    await vi.runAllTimersAsync(); await retried;
    expect(signals).toEqual([[10, "SIGTERM"]]); expect(rows["88"]).toBeDefined();
  });
  it.each(["empty", "new member", "reused member", "moved member", "reused child"])("rejects unsafe reentry with %s instead of borrowing a stale numeric SID", async kind => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77, "S", "101") });
    const kill = vi.mocked(process.kill).getMockImplementation()!;
    vi.mocked(process.kill).mockImplementation((pid, signal) => {
      if (signal !== 0) throw Object.assign(new Error("first cleanup denied"), { code: "EPERM" });
      return kill(pid, signal);
    });
    const first = stopOwnedSessionDescendants(attempt, 77);
    delete rows["77"];
    await expect(first).rejects.toThrow("first cleanup denied");
    delete rows["10"];
    if (kind === "new member") rows["11"] = stat(11, 1, 77, "S", "102");
    if (kind === "reused member") rows["10"] = stat(10, 1, 77, "S", "999");
    if (kind === "moved member") rows["10"] = stat(10, 1, 88, "S", "101");
    if (kind === "reused child") { rows["77"] = stat(77, 9, 77, "S", "200"); rows["10"] = stat(10, 1, 77, "S", "101"); }
    vi.mocked(process.kill).mockImplementation(kill);
    await expect(stopOwnedSessionDescendants(attempt, 77)).rejects.toThrow();
    expect(signals).toEqual([]);
  });
  it("rejects child PID reuse during the wait and never signals successor members", async () => {
    const { rows, signals, attempt } = setup();
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    const rejected = expect(stopped).rejects.toThrow("reused");
    rows["77"] = stat(77, 9, 77, "S", "200"); rows["10"] = stat(10, 77, 77, "S", "201");
    await vi.runAllTimersAsync(); await rejected; expect(signals).toEqual([]);
  });
  it("recaptures new reparented members born during TERM and KILL waits", async () => {
    const { rows, signals, attempt } = setup();
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    setTimeout(() => { rows["10"] = stat(10, 1, 77, "S", "101"); }, 20);
    setTimeout(() => { rows["11"] = stat(11, 1, 77, "S", "102"); delete rows["77"]; }, 5_020);
    await vi.runAllTimersAsync(); await stopped;
    expect(signals).toEqual([[10, "SIGTERM"], [11, "SIGKILL"]]);
    expect(attempt.processes.get(11)?.processStartTime).toBe("102");
  });
  it("birth-revalidates each member immediately before signaling", async () => {
    const { signals, attempt } = setup({ "10": stat(10, 1, 77, "S", "101") });
    const read = vi.mocked(fs.readFileSync).getMockImplementation()!;
    let memberReads = 0;
    vi.mocked(fs.readFileSync).mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === "/proc/10/stat" && ++memberReads === 3) return stat(10, 1, 77, "S", "999");
      return (read as Function)(file, ...args);
    }) as typeof fs.readFileSync);
    await expect(stopOwnedSessionDescendants(attempt, 77)).rejects.toThrow("identity changed");
    expect(signals).toEqual([]);
  });
  it.each(["mount", "missing mount", "wrong mount", "enumeration", "EACCES", "EIO", "invalid", "hidepid", "probe"])("fails closed for unknown %s evidence", async kind => {
    const { rows, signals, attempt } = setup();
    if (kind === "enumeration") vi.mocked(fs.readdirSync).mockImplementation(() => { throw new Error("unknown proc"); });
    else if (["EACCES", "EIO"].includes(kind)) rows["10"] = Object.assign(new Error("unreadable"), { code: kind });
    else if (kind === "invalid") rows["10"] = "broken stat";
    else if (kind === "probe") vi.mocked(process.kill).mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    else {
      const read = vi.mocked(fs.readFileSync).getMockImplementation()!;
      vi.mocked(fs.readFileSync).mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        if (String(file) === "/proc/mounts") {
          if (kind === "mount") throw new Error("unknown mount");
          if (kind === "missing mount") return "";
          if (kind === "wrong mount") return "tmpfs /proc tmpfs rw 0 0";
          return "proc /proc proc rw,hidepid=2 0 0";
        }
        return (read as Function)(file, ...args);
      }) as typeof fs.readFileSync);
    }
    await expect(stopOwnedSessionDescendants(attempt, 77)).rejects.toThrow(); expect(signals).toEqual([]);
  });
  it("rejects membership becoming unreadable during cleanup", async () => {
    const { rows, attempt } = setup();
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    const rejected = expect(stopped).rejects.toThrow("unknown");
    rows["10"] = Object.assign(new Error("unreadable"), { code: "EIO" });
    await vi.runAllTimersAsync(); await rejected;
  });
  it("joins TERM-resistant members with individual KILL", async () => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77, "S", "101") });
    const kill = vi.mocked(process.kill).getMockImplementation()!;
    vi.mocked(process.kill).mockImplementation((pid, signal) => {
      if (signal === "SIGTERM") { signals.push([pid, signal]); delete rows["77"]; return true; }
      return kill(pid, signal);
    });
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    await vi.runAllTimersAsync(); await stopped;
    expect(signals).toEqual([[10, "SIGTERM"], [10, "SIGKILL"]]);
  });
  it("does not call zombies exited session evidence or signal them", async () => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77, "Z", "101") });
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    const rejected = expect(stopped).rejects.toThrow("unproven");
    delete rows["77"]; await vi.runAllTimersAsync(); await rejected;
    expect(signals).toEqual([]);
  });
  it("rejects a previously observed member PID reused during cleanup", async () => {
    const { rows, signals, attempt } = setup({ "10": stat(10, 1, 77, "Z", "101") });
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    const rejected = expect(stopped).rejects.toThrow("identity or session changed");
    rows["10"] = stat(10, 1, 88, "S", "999");
    await vi.runAllTimersAsync(); await rejected; expect(signals).toEqual([]);
  });
  it("rejects a purported member older than the anchored launch birth", async () => {
    const { signals, attempt } = setup({ "10": stat(10, 1, 77, "S", "99") });
    await expect(stopOwnedSessionDescendants(attempt, 77)).rejects.toThrow("predates");
    expect(signals).toEqual([]);
  });
  it("does not treat ESRCH as proof of exit while membership still lists the member", async () => {
    const { rows, attempt } = setup({ "10": stat(10, 1, 77, "S", "101") });
    const kill = vi.mocked(process.kill).getMockImplementation()!;
    vi.mocked(process.kill).mockImplementation((pid, signal) => {
      if (signal !== 0) throw Object.assign(new Error("gone"), { code: "ESRCH" });
      return kill(pid, signal);
    });
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    const rejected = expect(stopped).rejects.toThrow("unproven");
    delete rows["77"]; await vi.runAllTimersAsync(); await rejected;
  });
  it("tolerates ESRCH only when recapture proves the member disappeared", async () => {
    const { rows, attempt } = setup({ "10": stat(10, 1, 77, "S", "101") });
    const kill = vi.mocked(process.kill).getMockImplementation()!;
    vi.mocked(process.kill).mockImplementation((pid, signal) => {
      if (signal !== 0) { delete rows[String(pid)]; delete rows["77"]; throw Object.assign(new Error("gone"), { code: "ESRCH" }); }
      return kill(pid, signal);
    });
    const stopped = stopOwnedSessionDescendants(attempt, 77);
    await vi.runAllTimersAsync(); await stopped;
    expect(checkResidentSessionExit(77)).toMatchObject({ empty: true, known: true });
  });
});
