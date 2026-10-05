import fs from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { checkResidentSessionExit } from "../src/residency/launcher-owner.js";

const stat = (pid: number, ppid: number, session: number, state = "S") =>
  `${pid} (fixture with ) parentheses) ${state} ${ppid} ${session} ${session} ${Array(16).fill("0").join(" ")}`;
const fixture = (rows: Record<string, string | Error>, mounts = "proc /proc proc rw 0 0") => {
  const read = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file) === "/proc/mounts") return mounts;
    const match = /^\/proc\/(\d+)\/stat$/.exec(String(file));
    if (match) { const row = rows[match[1]!]; if (row instanceof Error) throw row; return row; }
    return (read as Function)(file, ...args);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "readdirSync").mockImplementation((() => Object.keys(rows)) as unknown as typeof fs.readdirSync);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    expect(pid).toBe(-77); expect(signal).toBe(0);
    throw Object.assign(new Error("gone"), { code: "ESRCH" });
  });
};
afterEach(() => vi.restoreAllMocks());
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
