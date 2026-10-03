import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { localScratchVolume, noPotentialScratchHolders, scratchHostEpoch } from "../src/storage/scratch-process-census.js";

const now = Date.now(), directory = path.resolve(os.tmpdir(), "census-run", "tmp");
const date = (value:number) => new Date(value).toUTCString().replace(/^(\w+), (\d+) (\w+) (\d+) (.*) GMT$/, "$1 $3 $2 $5 $4");
const row = (pid:number, birth=now-60000, state="S", uid=process.getuid?.() ?? 0, ppid=1, comm="node") => `${pid} ${ppid} ${uid} ${state} ${date(birth)} ${comm}\n`;
const self = () => row(process.pid);
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe.skipIf(process.platform === "win32")("conservative POSIX scratch holder census", () => {
  it("accepts only two complete absence observations and bounds/sanitizes each query", () => {
    const query = vi.spyOn(childProcess,"execFileSync").mockReturnValue((self()+row(2147483647)) as never);
    expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(true);
    expect(query).toHaveBeenCalledTimes(2);
    expect(query).toHaveBeenCalledWith("/bin/ps",["-axo","pid=,ppid=,uid=,stat=,lstart=,comm="],expect.objectContaining({timeout:1000,maxBuffer:1024*1024,env:expect.objectContaining({LC_ALL:"C",TZ:"UTC",LD_PRELOAD:"",LD_AUDIT:""})}));
  });
  it.each(["new", "same-second", "unknown", "empty", "missing-self", "query-error"])("retains uncertainty/potential holders (%s)", fault => {
    const query = vi.spyOn(childProcess,"execFileSync");
    if(fault==="query-error")query.mockImplementation(()=>{throw new Error("census unavailable");});
    else query.mockReturnValue((fault==="unknown"?self()+"unparseable\n":fault==="empty"?"":fault==="missing-self"?row(2147483647):self()+row(2147483647,fault==="same-second"?now-500:now+1000)) as never);
    expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(false);
  });
  it("counts a surviving ordinary process even without any saved worker identity or open-file/env report", () => {
    vi.spyOn(childProcess,"execFileSync").mockReturnValue((self()+row(2147483647,now+1000,"Ss")) as never);
    expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(false);
  });
  it("ignores confirmed zombies, foreign-user processes and only the fixed ps query child", () => {
    vi.spyOn(childProcess,"execFileSync").mockReturnValue((self()+row(2147483647,now+1000,"Z")+row(2147483646,now+1000,"S",(process.getuid?.()??0)+1)+row(2147483645,now+1000,"R",process.getuid?.()??0,process.pid,"ps")) as never);
    expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(true);
  });
  it("a new holder in the second census vetoes collection", () => {
    vi.spyOn(childProcess,"execFileSync").mockReturnValueOnce(self() as never).mockReturnValueOnce((self()+row(2147483647,now+1000)) as never);
    expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(false);
  });
  it.each(["TMPDIR","TMP","TEMP"])("never excludes a collector that itself has run scratch as %s", key => {
    const query=vi.spyOn(childProcess,"execFileSync");vi.stubEnv(key,directory);
    expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(false);expect(query).not.toHaveBeenCalled();
  });
  it("deadline exhaustion never authorizes deletion or starts a query", () => {
    const query=vi.spyOn(childProcess,"execFileSync");expect(noPotentialScratchHolders(now,directory,()=>true)).toBe(false);expect(query).not.toHaveBeenCalled();
  });
});

describe.skipIf(process.platform !== "linux")("locality and clock guardrails", () => {
  it.each([0x6969,0xff534d42,0x65735546,0x12345678])("does not authorize the local census on shared/unknown filesystem type %s", type => {
    vi.spyOn(fs,"statfsSync").mockReturnValue({type} as fs.StatsFs);
    expect(localScratchVolume(directory)).toBe(false);
  });
  it.each([0xef53,0x01021994,0x9123683e])("accepts checked local filesystem type %s", type => {
    vi.spyOn(fs,"statfsSync").mockReturnValue({type} as fs.StatsFs);
    expect(localScratchVolume(directory)).toBe(true);
  });
  it.each(["drift","backwards","nonfinite"])("clock uncertainty vetoes the calendar census (%s)", fault => {
    const query=vi.spyOn(childProcess,"execFileSync");vi.spyOn(os,"uptime").mockReturnValue(10000);
    expect(noPotentialScratchHolders(new Date().getTime(),directory,()=>false,fault==="drift"?9990:fault==="backwards"?10001:NaN)).toBe(false);
    expect(query).not.toHaveBeenCalled();
  });
});

describe("native epoch proof and Windows query contract", () => {
  it.each(["local", "foreign-type", "remote-root", "ambiguous-root"])("Darwin fallback requires the trusted local root filesystem class (%s)", fault => {
    const original=Object.getOwnPropertyDescriptor(process,"platform")!;
    vi.spyOn(fs,"statfsSync").mockImplementation(((file:fs.PathLike)=>({type:String(file)==="/"?24:fault==="foreign-type"?42:24})) as typeof fs.statfsSync);
    const root=fault==="remote-root"?"server:/disk on / (nfs, read-only)":"/dev/disk1s1 on / (apfs, local, read-only)";
    vi.spyOn(childProcess,"execFileSync").mockReturnValue((root+"\n"+(fault==="ambiguous-root"?root+"\n":"")) as never);
    try {Object.defineProperty(process,"platform",{value:"darwin"});expect(localScratchVolume(directory)).toBe(fault==="local");}
    finally {Object.defineProperty(process,"platform",original);}
  });
  it.each(["darwin","win32"])("uses an actual kernel boot UUID, not calendar boot time (%s)", platform => {
    const original=Object.getOwnPropertyDescriptor(process,"platform")!;
    const boot="00112233-4455-6677-8899-AABBCCDDEEFF";
    const query=vi.spyOn(childProcess,"execFileSync").mockReturnValue((boot+"\n") as never);
    vi.stubEnv("SystemRoot","C:\\Windows");
    try {
      Object.defineProperty(process,"platform",{value:platform});
      expect(scratchHostEpoch()).toEqual({platform,hostname:os.hostname(),boot:boot.toLowerCase()});
      if(platform==="darwin")expect(query).toHaveBeenCalledWith("/usr/sbin/sysctl",["-n","kern.bootsessionuuid"],expect.any(Object));
      else {
        const args=query.mock.calls[0]![1] as string[];
        const script=Buffer.from(args[args.length-1]!,"base64").toString("utf16le");
        expect(script).toContain("NtQuerySystemInformation(90");expect(script).not.toContain("LastBootUpTime");
      }
      query.mockReturnValue("calendar or unavailable boot" as never);
      expect(scratchHostEpoch()).toBeUndefined();
    } finally {Object.defineProperty(process,"platform",original);}
  });
  it.skipIf(process.platform!=="linux")("reads the actual Linux boot identity, with no subprocess/startup work", () => {
    const query=vi.spyOn(childProcess,"execFileSync");expect(scratchHostEpoch()).toEqual({hostname:os.hostname(),platform:"linux",boot:fs.readFileSync("/proc/sys/kernel/random/boot_id","utf8").trim()});expect(query).not.toHaveBeenCalled();
  });
  it.each([false,true])("uses pinned Windows PowerShell, complete session births and fail-closed uncertainty (live=%s)", live => {
    const original=Object.getOwnPropertyDescriptor(process,"platform")!;
    const query=vi.spyOn(childProcess,"execFileSync").mockReturnValue(JSON.stringify([{pid:process.pid,ppid:process.ppid,birth:now-60000,zombie:false,query:false},...(live?[{pid:2147483647,ppid:process.pid,birth:now+1000,zombie:false,query:false}]:[])]) as never);
    vi.stubEnv("SystemRoot","C:\\Windows");vi.stubEnv("PSModulePath","C:\\untrusted");
    try {
      Object.defineProperty(process,"platform",{value:"win32"});
      expect(noPotentialScratchHolders(now,directory,()=>false)).toBe(!live);
      expect(query).toHaveBeenCalledWith("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",expect.arrayContaining(["-NoProfile","-EncodedCommand"]),expect.objectContaining({env:expect.objectContaining({PI_FABRIC_CENSUS_PID:String(process.pid),PSModulePath:"C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules"})}));
    } finally { Object.defineProperty(process,"platform",original); }
  });
});
