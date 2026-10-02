import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";

// Exercise extracted production wiring, never a real SSH host or agent profile.
// Only the 900s proof duration is shortened; the wrapper's TERM/30s KILL grace
// and GNU timeout executable are unchanged. A real detached bridge stand-in has
// a linked child in its group; both ignore TERM, as does the fake cleanup SSH.
const supported = process.platform === "linux" && spawnSync("timeout", ["--version"]).status === 0 &&
  spawnSync("python3", ["-c", "import os,signal; assert hasattr(os,'pidfd_open') and hasattr(signal,'pidfd_send_signal')"]).status === 0;

type Identity = { pid: number; state: string; group: number; session: number; start: string };
const identity = (pid: number): Identity | undefined => {
  try {
    const raw = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
    return { pid, state: fields[0]!, group: Number(fields[2]), session: Number(fields[3]), start: fields[19]! };
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return undefined;
    throw error;
  }
};
const liveGroup = (owner: Identity): Identity[] => fs.readdirSync("/proc").filter((p) => /^\d+$/.test(p))
  .map((p) => identity(Number(p))).filter((p): p is Identity => !!p && p.state !== "Z" &&
    p.group === owner.group && p.session === owner.session);

const source = fs.readFileSync(path.resolve("proof/forge-ssh-bridge.mjs"), "utf8");
const wrapper = fs.readFileSync(path.resolve("proof/forge-ssh-bridge.sh"), "utf8");
const mainAnchor = "\ntry {\n  cfg = JSON.parse";

const runProbe = (mode: "stalled" | "normal" | "responsive") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-proof-cleanup-"));
  const ownersFile = path.join(root, "owners.json");
  let owners: Identity[] = [];
  let probe: unknown;
  try {
    expect(source.indexOf(mainAnchor)).toBeGreaterThan(0);
    fs.writeFileSync(path.join(root, "forge-ssh-bridge.sh"), wrapper);
    // PATH shim changes only the overall proof duration, not cleanup's grace.
    fs.writeFileSync(path.join(root, "timeout"), `#!/bin/bash\nargs=("$@"); for i in "\${!args[@]}"; do [[ \${args[$i]} != 900s ]] || args[$i]=${mode === "responsive" ? "20s" : "2s"}; done\nexec /usr/bin/timeout "\${args[@]}"\n`, { mode: 0o700 });
    fs.writeFileSync(path.join(root, "ssh"), `#!${process.execPath}\nimport fs from 'node:fs';\nconst raw=fs.readFileSync('/proc/self/stat','utf8'); const s=raw.slice(raw.lastIndexOf(')')+2).split(' ');\nfs.appendFileSync(${JSON.stringify(path.join(root, "ssh.jsonl"))},JSON.stringify({pid:process.pid,state:s[0],group:Number(s[2]),session:Number(s[3]),start:s[19]})+'\\n');\n${mode === "responsive" ? "const {spawn}=await import('node:child_process'); const p=spawn('sh',['-c',process.argv.at(-1)],{stdio:'inherit'}); p.once('exit',code=>{process.exitCode=code??1});" : "process.on('SIGTERM',()=>{}); const {spawn}=await import('node:child_process'); spawn(process.execPath,['-e',\"process.on('SIGTERM',()=>{}); setInterval(()=>{},1000)\"],{stdio:['ignore','inherit','inherit']}); setInterval(()=>{},1000);"}\n`, { mode: 0o700 });
    const linkProgram = `process.on('SIGTERM',()=>{}); const c=require('node:child_process').spawn(process.execPath,['-e',"process.on('SIGTERM',()=>{}); console.log('LINKED'); setInterval(()=>{},1000)"],{stdio:['ignore','inherit','inherit']}); setInterval(()=>{},1000);`;
    const harness = `
evidence=${JSON.stringify(root)}; cfg={sshHost:'fake-cleanup',remoteNode:process.execPath};
const p=child(process.execPath,['-e',${JSON.stringify(linkProgram)}],{stdio:['ignore','pipe','ignore']},'linked bridge stand-in');
await new Promise((resolve,reject)=>{ p.stdout.once('data',resolve); p.once('error',reject); });
ownedMembers(p.owned);
fs.writeFileSync(${JSON.stringify(ownersFile)},JSON.stringify([p.owned]));
fs.writeFileSync(${JSON.stringify(path.join(root, "driver.json"))},JSON.stringify(processIdentity(process.pid)));
results.status='PASS';
${mode === "stalled" ? "remoteOwned=p.owned; remoteBridge=p.owned; remoteOwners.set(p.pid,p.owned);" : ""}
${mode === "responsive" ? `const r=spawn(process.execPath,['-e',${JSON.stringify(linkProgram)}],{detached:true,stdio:['ignore','pipe','ignore']});
await new Promise((resolve,reject)=>{r.stdout.once('data',resolve);r.once('error',reject)});
remoteOwned=ownLocal(r.pid); ownedMembers(remoteOwned); remoteOwners.set(r.pid,remoteOwned);
fs.writeFileSync(${JSON.stringify(path.join(root, "remote.json"))},JSON.stringify(remoteOwned));` : "try { await sleep(60000); } catch {}"}
try { await cleanup(); } catch (error) { results.cleanupFailure=String(error); }
results.status=finalStatus(results.status,results.interrupted,results.cleanup?.errors??['missing cleanup']); save();
process.exitCode=results.status==='PASS'?0:1;
`;
    fs.writeFileSync(path.join(root, "forge-ssh-bridge.mjs"), source.slice(0, source.indexOf(mainAnchor)) + harness);
    fs.writeFileSync(path.join(root, "config.json"), "{}");
    const started = Date.now();
    const run = spawnSync("bash", [path.join(root, "forge-ssh-bridge.sh"), path.join(root, "config.json")], {
      env: { ...process.env, PATH: `${root}:${process.env.PATH}` }, encoding: "utf8", timeout: 50_000, killSignal: "SIGKILL",
    });
    owners = JSON.parse(fs.readFileSync(ownersFile, "utf8")) as Identity[];
    const sshFile = path.join(root, "ssh.jsonl");
    if (fs.existsSync(sshFile)) owners.push(...fs.readFileSync(sshFile, "utf8").trim().split("\n").map((line) => JSON.parse(line) as Identity));
    const remoteFile = path.join(root, "remote.json");
    const remoteOwner: Identity | undefined = fs.existsSync(remoteFile) ? JSON.parse(fs.readFileSync(remoteFile, "utf8")) as Identity : undefined;
    const survivors = [...owners, ...(remoteOwner ? [remoteOwner] : [])].flatMap(liveGroup);
    const resultFile = path.join(root, "results.json");
    const result = fs.existsSync(resultFile) ? JSON.parse(fs.readFileSync(resultFile, "utf8")) as {
      status: string; interrupted?: boolean; cleanup?: { receipts: Array<{ remote: boolean; dead: boolean }>; errors: string[] };
    } : undefined;
    probe = { mode, elapsedMs: Date.now() - started, wrapperStatus: run.status, wrapperSignal: run.signal,
      spawnError: run.error?.message, ownedGroups: owners.map((p) => p.group), survivors, result };
    console.log(JSON.stringify(probe));
    expect(run.error).toBeUndefined();
    expect(survivors).toEqual([]);
    expect(run.status).toBe(mode === "responsive" ? 0 : 124);
    expect(run.signal).toBeNull(); // A 30s grace SIGKILL is a cleanup failure.
    expect(result?.status).toBe(mode === "responsive" ? "PASS" : "FAIL");
    expect(result?.interrupted).toBe(mode === "responsive" ? undefined : true);
    const localReceipts = result?.cleanup?.receipts.filter((r) => !r.remote);
    expect(localReceipts).toHaveLength(owners.length);
    expect(localReceipts?.every((r) => r.dead)).toBe(true);
    if (mode === "stalled") {
      expect(owners.length).toBeGreaterThan(1); // Must actually start the stalled SSH.
      expect(result?.cleanup?.errors.join(";")).toMatch(/deadline/);
    } else expect(result?.cleanup?.errors).toEqual([]);
    if (mode === "responsive") expect(result?.cleanup?.receipts.some((r) => r.remote && r.dead)).toBe(true);
  } finally {
    // The before-fix negative control deliberately leaves the stand-in alive.
    // Signal only groups created by this probe, bound to their recorded leader.
    if (!owners.length && fs.existsSync(ownersFile)) owners = JSON.parse(fs.readFileSync(ownersFile, "utf8")) as Identity[];
    const sshFile = path.join(root, "ssh.jsonl");
    if (fs.existsSync(sshFile)) owners.push(...fs.readFileSync(sshFile, "utf8").trim().split("\n").map((line) => JSON.parse(line) as Identity));
    for (const name of ["driver.json", "remote.json"]) {
      const file = path.join(root, name);
      if (fs.existsSync(file)) owners.push(JSON.parse(fs.readFileSync(file, "utf8")) as Identity);
    }
    for (const owner of owners) {
      const current = identity(owner.pid);
      if (current && (current.start !== owner.start || current.group !== owner.group || current.session !== owner.session)) throw new Error("probe leader identity changed");
      for (const member of liveGroup(owner)) {
        // Even a dead SSH leader may leave inherited-pipe children. All these
        // members belong to a new group/session created by this probe only.
        if (BigInt(member.start) < BigInt(owner.start)) throw new Error("pre-existing probe group member");
        execFileSync("python3", ["-c", `import os,signal,sys,json
m=json.loads(sys.argv[1])
try:
    fd=os.pidfd_open(m['pid'])
    s=open('/proc/%d/stat'%m['pid']).read().rsplit(') ',1)[1].split()
    assert s[19]==m['start'] and int(s[2])==m['group'] and int(s[3])==m['session']
    signal.pidfd_send_signal(fd,signal.SIGKILL)
    os.close(fd)
except (ProcessLookupError,FileNotFoundError): pass`, JSON.stringify(member)], { timeout: 2000 });
      }
      // Wait for actual death, not just a successful signal syscall.
      execFileSync("python3", ["-c", `import os,sys,time
until=time.monotonic()+2
while True:
    live=[]
    for p in os.listdir('/proc'):
        if not p.isdigit(): continue
        try:
            s=open('/proc/'+p+'/stat').read().rsplit(') ',1)[1].split()
            if s[0]!='Z' and s[2]==sys.argv[1] and s[3]==sys.argv[2]: live.append(p)
        except FileNotFoundError: pass
    if not live: break
    assert time.monotonic()<until,live
    time.sleep(.02)`, String(owner.group), String(owner.session)], { timeout: 3000 });
    }
    if (process.env.BRIDGE_PROOF_EVIDENCE_DIR) {
      fs.mkdirSync(process.env.BRIDGE_PROOF_EVIDENCE_DIR, { recursive: true });
      fs.writeFileSync(path.join(process.env.BRIDGE_PROOF_EVIDENCE_DIR, `${mode}.json`), `${JSON.stringify(probe, null, 2)}\n`);
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
};

describe.skipIf(!supported)("bridge proof cleanup under its real wrapper grace", () => {
  it("reaps linked local groups even when cleanup SSH stalls", () => runProbe("stalled"), 65_000);
  it("reaps linked local groups without remote cleanup", () => runProbe("normal"), 65_000);
  it("preserves successful serialized remote cleanup", () => runProbe("responsive"), 65_000);
});
