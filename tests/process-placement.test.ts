import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { liveAgentPlacement, probeAgentPlacement } from "../src/agents/placement-config.js";
import type { InheritedSessionPin } from "../src/agents/session-pins.js";

const roots: string[] = [], managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
});
const fake = `import fs from 'node:fs'; import path from 'node:path';
const [mode, root, id, ...argv] = process.argv.slice(2);
const dir = path.join(root, id); fs.mkdirSync(dir, {recursive:true});
if (mode === 'launch') {
 fs.writeFileSync(path.join(dir,'argv.json'), JSON.stringify(argv));
 const task = argv.at(-1);
 if (task === 'reject') { console.error('launcher rejected'); process.exit(3); }
 if (task === 'malformed') { console.log('no accepted receipt'); process.exit(0); }
 fs.writeFileSync(path.join(dir,'result.md'), task === 'pending' ? 'partial' : 'REMOTE: '+task);
 if (task !== 'pending' && task !== 'forge-pending') fs.writeFileSync(path.join(dir,'rc'), task === 'fail' ? '7' : '0');
 console.log('RYZEN2_TASK_ACCEPTED '+id+' on '+(task === 'forge' || task === 'forge-pending' ? 'ryzen3' : 'ryzen2')+' (unit smarty-task-'+id+').');
} else if (mode === 'cancel') {
 fs.writeFileSync(path.join(dir,'cancel-argv.json'), JSON.stringify(argv));
 fs.writeFileSync(path.join(dir,'cancelled'), 'yes');
 fs.writeFileSync(path.join(dir,'rc'),'124');
} else if (mode === 'unconfirmed') { console.log('cancel acknowledged without exit');
} else if (mode === 'poll') {
 fs.writeFileSync(path.join(dir,'poll-argv.json'), JSON.stringify(argv));
 const rc = path.join(dir,'rc');
 console.log(JSON.stringify(fs.existsSync(rc) ? {rc:fs.readFileSync(rc,'utf8').trim(),text:fs.readFileSync(path.join(dir,'result.md'),'utf8')} : {rc:null}));
} else if (mode === 'bad-poll') { console.log('not JSON'); }
`;
const fixture = (pollCommand = false, timeoutMs = 1_000, resolveInheritedSessionPins?: () => InheritedSessionPin[] | undefined, live = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-")); roots.push(root);
  const profile = path.join(root, "profile"); fs.mkdirSync(profile); vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  const launcher = path.join(root, "launcher.mjs"); fs.writeFileSync(launcher, fake);
  const results = path.join(root, "results");
  const raw = {
    default: "remote", command: [process.execPath, launcher, "launch", results, "{id}", "--host", "auto", "--minutes", "{minutes}", "--cwd", "{cwd}", "--model", "{model}", "--thinking", "{thinking}", "--", "{task}"],
    ...(pollCommand ? { resultCommand: [process.execPath, launcher, "poll", results, "{id}"] } : { resultDirectory: path.join(results, "{id}") }),
    // Cancellation and its receipt each launch Node; 300ms is not a portable startup budget.
    cancelCommand: [process.execPath, launcher, "cancel", results, "{id}"], pollIntervalMs: 10, commandTimeoutMs: 5_000,
  };
  const config = { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs, placement: normalizeFabricConfig({agents:{placement:raw}}).agents.placement! };
  const worker = path.join(root, "local.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs'; const args = new Map(); for(let i=2;i<process.argv.length;i+=2)args.set(process.argv[i],process.argv[i+1]); const now=Date.now(); fs.writeFileSync(args.get('--status-file'),JSON.stringify({id:args.get('--id'),name:args.get('--name'),task:'local',status:'completed',runner:'pi',transport:'process',cwd:args.get('--cwd'),startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:0,text:'LOCAL',usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));`);
  const placementConfigPath = path.join(profile, "fabric.json");
  if (live) fs.writeFileSync(placementConfigPath, JSON.stringify({ agents: { placement: raw } }));
  const manager = new AgentManager(root, config, { workerPath: worker, runRoot: path.join(root, "runs"), ...(resolveInheritedSessionPins ? { resolveInheritedSessionPins } : {}), ...(live ? { placementConfigPath } : {}) }); managers.push(manager);
  return { root, profile, results, raw, config, manager, launcher };
};
const launch = (f: ReturnType<typeof fixture>, task: string, timeoutMs = 5_000): AgentTransportLaunch => {
  const dir = path.join(f.root, "direct"); fs.mkdirSync(dir, {recursive:true});
  fs.writeFileSync(path.join(dir,"task.txt"),task);
  return { id: "direct-id", name: "direct", cwd: f.root, workerPath: "unused", workerArguments: ["--task-file",path.join(dir,"task.txt"),"--status-file",path.join(dir,"status.json"),"--log-file",path.join(dir,"events.jsonl"),"--timeout-ms",String(timeoutMs),"--model","test/model","--thinking","high"] };
};

describe("host process task placement", () => {
  it("keeps absent/default placement local and parses strict argv templates", () => {
    expect(normalizeFabricConfig({}).agents.placement).toBeUndefined();
    const f = fixture();
    expect(normalizeFabricConfig({agents:{placement:{...f.raw,default:undefined}}}).agents.placement?.default).toBe("local");
    expect(f.config.placement).toMatchObject({default:"remote",capabilities:[],pollIntervalMs:10});
    for (const change of [{command:"shell string"},{default:"auto"},{command:["{typo}"]},{cancelCommand:[]},{resultDirectory:undefined},{resultDirectory:"relative/{id}"},{pollIntervalMs:0},{capabilities:[1]},{sshAliases:[]},{sshAliases:{ryzen3:"-oProxyCommand=bad"}},{command:["fake","{sshAlias}"]},{resultCommand:["fake","{sshAlias}"],resultDirectory:undefined}]) {
      expect(() => normalizeFabricConfig({agents:{placement:{...f.raw,...change}}})).toThrow();
    }
  });
  it("does not allow a workspace to override host placement", () => {
    const f=fixture(); fs.mkdirSync(path.join(f.root,".pi"));
    fs.writeFileSync(path.join(f.profile,"fabric.json"), JSON.stringify({agents:{placement:f.raw}}));
    fs.writeFileSync(path.join(f.root,".pi/fabric.json"), JSON.stringify({agents:{placement:{...f.raw,default:"local",capabilities:["github-write"]}}}));
    expect(loadFabricConfig({cwd:f.root,agentDir:f.profile,projectTrusted:true}).agents.placement).toEqual(f.config.placement);
  });
  it("uses changed host placement on the next spawn without stopping a running task", async () => {
    const f = fixture(false, 10_000, undefined, true);
    const first = await f.manager.spawn({ task: "pending", transport: "process" });
    const nextResults = path.join(f.root, "new-results");
    const next = {
      ...f.raw,
      command: f.raw.command.map(entry => entry === f.results ? nextResults : entry),
      resultDirectory: path.join(nextResults, "{id}"),
      cancelCommand: f.raw.cancelCommand.map(entry => entry === f.results ? nextResults : entry),
      capabilities: ["compute"],
    };
    // A conflicting workspace edit is still never placement authority.
    fs.mkdirSync(path.join(f.root, ".pi"));
    fs.writeFileSync(path.join(f.root, ".pi/fabric.json"), JSON.stringify({ agents: { placement: { ...next, default: "local" } } }));
    fs.writeFileSync(path.join(f.profile, "fabric.json"), JSON.stringify({ agents: { placement: next } }));
    const second = await f.manager.spawn({ task: "new policy", transport: "process", needs: ["compute"] });
    expect(await f.manager.wait(second.id)).toMatchObject({ status: "completed", text: "REMOTE: new policy" });
    expect(fs.existsSync(path.join(nextResults, second.id, "argv.json"))).toBe(true);
    expect(fs.existsSync(path.join(f.results, second.id))).toBe(false);
    await expect(f.manager.wait(first.id, { timeoutMs: 30 })).rejects.toThrow("still running");
    expect(fs.existsSync(path.join(f.results, first.id, "cancelled"))).toBe(false);
    // Original receipt path remains authoritative for the already-admitted task.
    fs.writeFileSync(path.join(f.results, first.id, "result.md"), "ORIGINAL TASK COMPLETED");
    fs.writeFileSync(path.join(f.results, first.id, "rc"), "0");
    expect(await f.manager.wait(first.id)).toMatchObject({ status: "completed", text: "ORIGINAL TASK COMPLETED" });
    fs.writeFileSync(path.join(f.profile, "fabric.json"), JSON.stringify({ agents: { placement: { ...next, default: "local" } } }));
    expect((await f.manager.run({ task: "local policy", transport: "process" })).text).toBe("LOCAL");
  });
  it("caches unchanged host placement and handles atomic replacement, invalid edits, removal and recreation", () => {
    const f = fixture();
    const file = path.join(f.profile, "fabric.json");
    const read = liveAgentPlacement(file, f.config.placement);
    expect(read()).toBeUndefined();
    fs.writeFileSync(file, JSON.stringify({ agents: { placement: f.raw } }));
    const accepted = read()!;
    expect(accepted.default).toBe("remote");
    const readFile = vi.spyOn(fs, "readFileSync");
    expect(read()).toBe(accepted);
    expect(readFile).not.toHaveBeenCalled();
    // Replacing the inode must invalidate even with a preserved mtime and size.
    const replacement = `${file}.tmp`;
    const stat = fs.statSync(file);
    fs.writeFileSync(replacement, JSON.stringify({ agents: { placement: { ...f.raw, default: "local" } } }) + " ");
    fs.utimesSync(replacement, stat.atime, stat.mtime);
    fs.renameSync(replacement, file);
    expect(read()?.default).toBe("local");
    const local = read();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    for (const invalid of ['{"agents":', JSON.stringify({ agents: { placement: { ...f.raw, command: [] } } })]) {
      fs.writeFileSync(file, invalid);
      expect(read()).toBe(local);
      expect(read()).toBe(local);
      expect(fs.readFileSync(file, "utf8")).toBe(invalid);
    }
    expect(warn).toHaveBeenCalledTimes(2);
    fs.writeFileSync(file, JSON.stringify({ agents: { placement: f.raw } }));
    expect(read()?.default).toBe("remote");
    fs.writeFileSync(file, "{}");
    expect(read()).toBeUndefined();
    fs.unlinkSync(file);
    expect(read()).toBeUndefined();
    fs.writeFileSync(file, JSON.stringify({ agents: { placement: f.raw } }));
    expect(read()?.default).toBe("remote");
  });
  it("registers needs on run/spawn and rejects malformed declarations", () => {
    const defaults={runner:"pi" as const,timeoutMs:1000};
    expect(normalizeAgentRunRequest({task:"x",needs:["corpus"]},defaults).needs).toEqual(["corpus"]);
    for(const needs of ["corpus",[1],[""]]) expect(()=>normalizeAgentRunRequest({task:"x",needs},defaults)).toThrow("needs");
    for (const name of ["run","spawn"]) expect((AGENTS_ACTION_DESCRIPTORS.find(d=>d.name===name)!.inputSchema as {properties:Record<string,unknown>}).properties.needs).toBeDefined();
  });
  it.each(["github-write","corpus","mac","ryzen1-service","unknown"])("keeps unmet %s local with exactly one audit line", async need => {
    const f=fixture(); const h=await f.manager.spawn({task:"x",transport:"process",needs:[need]});
    expect((await f.manager.wait(h.id)).text).toBe("LOCAL");
    const lines=fs.readFileSync(path.join(f.manager.runDirectory(h.id)!,"events.jsonl"),"utf8").trim().split("\n").map(line=>JSON.parse(line)).filter(line=>line.type==="placement.local");
    expect(lines).toEqual([expect.objectContaining({reason:`unmet needs: ${need}`,needs:[need]})]);
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it.each([false,true])("preserves argv and maps spawn/wait results (command polling=%s)", async pollCommand => {
    const f=fixture(pollCommand); f.config.placement.capabilities=["compute"];
    const task="quoted ' task; $(not-a-shell)\nsecond line";
    const h=await f.manager.spawn({task,transport:"process",needs:["compute"],model:"test/model",thinking:"high"});
    const result=await f.manager.wait(h.id);
    expect(result).toMatchObject({status:"completed",text:`REMOTE: ${task}`,exitCode:0});
    expect(JSON.parse(fs.readFileSync(path.join(f.results,h.id,"argv.json"),"utf8"))).toEqual(["--host","auto","--minutes","1","--cwd",f.root,"--model","test/model","--thinking","high","--",task]);
  });
  it("ships a Git source cwd with --src rather than the target-only --cwd flag", async () => {
    const f=fixture(); execFileSync("git",["init","--quiet",f.root]);
    f.config.placement.command=f.config.placement.command.map(entry=>entry==="--cwd"?"--src":entry);
    const result=await f.manager.run({task:"source packet",transport:"process",model:"test/model",thinking:"high"});
    expect(result).toMatchObject({status:"completed",text:"REMOTE: source packet"});
    const argv=JSON.parse(fs.readFileSync(path.join(f.results,result.id,"argv.json"),"utf8"));
    expect(argv).toEqual(["--host","auto","--minutes","1","--src",f.root,"--model","test/model","--thinking","high","--","source packet"]);
  });
  it("resolves Ryzen 3 polling and cancellation through the configured forge alias", async () => {
    const f=fixture(true);
    f.config.placement.sshAliases={ryzen2:"ryzen2-agent",ryzen3:"forge-agent"};
    f.config.placement.resultCommand!.push("{sshAlias}");
    f.config.placement.cancelCommand.push("{sshAlias}");
    const result=await f.manager.run({task:"forge",transport:"process"});
    expect(result).toMatchObject({status:"completed",text:"REMOTE: forge"});
    expect(JSON.parse(fs.readFileSync(path.join(f.results,result.id,"poll-argv.json"),"utf8"))).toEqual(["forge-agent"]);
    const req=launch(f,"forge-pending",5_000); const h=await new ProcessTransport(undefined,f.config.placement).launch(req);
    await h.stop();
    expect(JSON.parse(fs.readFileSync(path.join(f.results,req.id,"cancel-argv.json"),"utf8"))).toEqual(["forge-agent"]);
    expect(h.stopDebt?.()).toBeUndefined();
  });
  it("does not guess an SSH alias or fall back locally after unmapped host acceptance", async () => {
    const f=fixture(true); f.config.placement.sshAliases={ryzen2:"ryzen2-agent"};
    f.config.placement.resultCommand!.push("{sshAlias}");
    const req=launch(f,"forge",5_000);
    await expect(new ProcessTransport(undefined,f.config.placement).launch(req)).rejects.toMatchObject({launchOutcome:"unknown",cleanupPending:true});
    expect(fs.existsSync(path.join(f.results,req.id,"argv.json"))).toBe(true);
    expect(fs.existsSync(path.join(f.results,req.id,"poll-argv.json"))).toBe(false);
  });
  it("maps agents.run failures and never retries the remote launch", async () => {
    const f=fixture(true); const result=await f.manager.run({task:"fail",transport:"process"});
    expect(result).toMatchObject({status:"failed",exitCode:7,text:"REMOTE: fail"});
    expect(result.error).toContain("rc=7");
    expect(fs.readdirSync(f.results)).toEqual([result.id]);
  });
  it("does not treat partial text as completion, and rejects remote controls", async () => {
    const f=fixture(); const h=await f.manager.spawn({task:"pending",transport:"process"});
    expect(()=>f.manager.steer(h.id,"x")).toThrow("Remote placement");
    expect(()=>f.manager.compact(h.id)).toThrow("Remote placement");
    await expect(f.manager.wait(h.id,{timeoutMs:30})).rejects.toThrow("still running");
    fs.writeFileSync(path.join(f.results,h.id,"result.md"),"FINAL"); fs.writeFileSync(path.join(f.results,h.id,"rc"),"0");
    expect((await f.manager.wait(h.id)).text).toBe("FINAL");
  });
  it("cancels at the task deadline and maps a timeout via agents.wait", async () => {
    const f=fixture(true); const h=await f.manager.spawn({task:"pending",transport:"process"});
    expect(await f.manager.wait(h.id)).toMatchObject({status:"timed_out",exitCode:124});
    expect(fs.existsSync(path.join(f.results,h.id,"cancelled"))).toBe(true);
  });
  it("bounds broken polling and reports timeout debt without claiming remote exit", async () => {
    const f=fixture(true); f.config.placement.resultCommand=[process.execPath,f.launcher,"bad-poll",f.results,"{id}"];
    const req=launch(f,"pending"); const h=await new ProcessTransport(undefined,f.config.placement).launch(req);
    const { deadline } = JSON.parse(fs.readFileSync(path.join(f.root,"direct/placement.json"),"utf8"));
    // Advance to the recorded deadline, not a sleep with platform-dependent timer/startup jitter.
    vi.spyOn(Date, "now").mockReturnValue(deadline);
    expect(await h.isAlive()).toBe(false);
    const record=JSON.parse(fs.readFileSync(path.join(f.root,"direct/status.json"),"utf8"));
    expect(record.status).toBe("timed_out"); expect(h.lostContact?.()).toContain("exit unconfirmed");
  });
  it("does not launch or audit remote work when final authority is revoked", async () => {
    const f=fixture(); const req=launch(f,"x"); req.authorize=()=>false;
    await expect(new ProcessTransport(undefined,f.config.placement).launch(req)).rejects.toThrow("authorized");
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it("falls back locally with one audit reason when the executable probe fails", async () => {
    const f=fixture(); f.config.placement.command=[path.join(f.root,"missing"),"{id}"];
    const h=await f.manager.spawn({task:"x",transport:"process"});
    expect((await f.manager.wait(h.id)).text).toBe("LOCAL");
    const events=fs.readFileSync(path.join(f.manager.runDirectory(h.id)!,"events.jsonl"),"utf8").trim().split("\n").map(line=>JSON.parse(line));
    expect(events.filter(line=>line.type==="placement.local")).toEqual([expect.objectContaining({reason:expect.stringContaining("placement-probe-failed:")})]);
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it("does not invent a local retry when an executable disappears after a successful startup probe", async () => {
    const f=fixture(); const launcher=path.join(f.root,process.platform==="win32"?"removed.exe":"removed");
    fs.writeFileSync(launcher,"never execute",{mode:0o700}); f.config.placement.command=[launcher,"{id}"];
    expect(probeAgentPlacement(f.config.placement,f.root).reason).toBeUndefined(); fs.unlinkSync(launcher);
    await expect(f.manager.spawn({task:"x",transport:"process"})).rejects.toThrow("ENOENT");
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it.each(["home","non-git"])("keeps an unshippable %s --src cwd local", async kind => {
    const f=fixture(); f.config.placement.command=f.config.placement.command.map(entry=>entry==="--cwd"?"--src":entry);
    if(kind==="home") vi.stubEnv("HOME",f.root);
    const h=await f.manager.spawn({task:"x",transport:"process"});
    expect((await f.manager.wait(h.id)).text).toBe("LOCAL");
    expect(fs.readFileSync(path.join(f.manager.runDirectory(h.id)!,"events.jsonl"),"utf8")).toContain('"reason":"cwd-not-shippable"');
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it.each([".local", "huge-cache"])("never invokes the launcher or ships an ignored %s source root", async directory => {
    const f = fixture(); execFileSync("git", ["init", "--quiet", f.root]);
    fs.writeFileSync(path.join(f.root, ".gitignore"), `${directory}/\n`);
    const cwd = path.join(f.root, directory); fs.mkdirSync(cwd);
    const privateFile = path.join(cwd, "private-fixture.txt"); fs.writeFileSync(privateFile, "NONSECRET PRIVATE FIXTURE");
    expect(execFileSync("git", ["-C", cwd, "rev-parse", "--is-inside-work-tree"], { encoding: "utf8" }).trim()).toBe("true");
    expect(() => execFileSync("git", ["-C", cwd, "check-ignore", "-q", "--", cwd])).not.toThrow();
    f.config.placement.command = f.config.placement.command.map(entry => entry === "--cwd" ? "--src" : entry);
    const result = await f.manager.run({ task: "must remain local", cwd, transport: "process" });
    expect(result).toMatchObject({ status: "completed", text: "LOCAL", cwd });
    const events = fs.readFileSync(path.join(f.manager.runDirectory(result.id)!, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events.filter(line => line.type.startsWith("placement."))).toEqual([expect.objectContaining({ type: "placement.local", reason: "cwd-not-shippable" })]);
    expect(fs.existsSync(f.results)).toBe(false); // The fake launcher creates this on any invocation.
    expect(fs.readFileSync(privateFile, "utf8")).toBe("NONSECRET PRIVATE FIXTURE");
  });
  it.each([
    { code: 128, stdout: "", stderr: "fatal: unable to check ignore rules" },
    { code: 1, killed: true, stdout: "", stderr: "" },
    { code: 1, signal: "SIGKILL", stdout: "", stderr: "" },
    { code: 1, stdout: "", stderr: "unexpected diagnostic" },
    { code: "ENOENT", stdout: "", stderr: "" },
    {},
  ])("keeps failed or indeterminate ignore checks local (%j)", async failure => {
    const f = fixture();
    f.config.placement.command = f.config.placement.command.map(entry => entry === "--cwd" ? "--src" : entry);
    const query = vi.spyOn(processUtils, "executeFile").mockResolvedValueOnce({ stdout: "true\n", stderr: "" }).mockRejectedValueOnce(Object.assign(new Error("ignore check failed"), failure));
    const result = await f.manager.run({ task: "must remain local", transport: "process" });
    expect(result.text).toBe("LOCAL");
    expect(query).toHaveBeenNthCalledWith(2, "git", ["-C", fs.realpathSync(f.root), "check-ignore", "-q", "--", fs.realpathSync(f.root)], expect.objectContaining({ killSignal: "SIGKILL" }));
    const events = fs.readFileSync(path.join(f.manager.runDirectory(result.id)!, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events.filter(line => line.type.startsWith("placement."))).toEqual([expect.objectContaining({ type: "placement.local", reason: "cwd-not-shippable" })]);
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it.each(["request", "parent"] as const)("keeps %s account pins local and forwards the resolved snapshot", async source => {
    const pins = [{ pool: "anthropic", accountId: "explicit-work", label: "Work" }];
    const resolver = vi.fn(() => source === "parent" ? pins : undefined);
    const f = fixture(false, 1_000, resolver);
    const worker = path.join(f.root, "local.mjs");
    fs.appendFileSync(worker, `fs.writeFileSync(${JSON.stringify(path.join(f.root, "worker-argv.json"))}, JSON.stringify(Object.fromEntries(args)));`);
    const h = await f.manager.spawn({ task: "pinned", transport: "process", ...(source === "request" ? { inheritedSessionPins: pins } : {}) });
    expect((await f.manager.wait(h.id)).text).toBe("LOCAL");
    const argv = JSON.parse(fs.readFileSync(path.join(f.root, "worker-argv.json"), "utf8"));
    expect(JSON.parse(argv["--inherited-session-pins"])).toEqual(pins);
    expect(resolver).toHaveBeenCalledTimes(source === "parent" ? 1 : 0);
    const lines = fs.readFileSync(path.join(f.manager.runDirectory(h.id)!, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)).filter(line => line.type === "placement.local");
    expect(lines).toEqual([expect.objectContaining({ reason: "inherited account pins require the local worker" })]);
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it.each(["configured", "requested"] as const)("keeps agents.run with %s disabled extensions local using the effective setting", async source => {
    const f = fixture();
    if (source === "configured") f.config.extensions = false;
    const worker = path.join(f.root, "local.mjs");
    fs.appendFileSync(worker, `fs.writeFileSync(${JSON.stringify(path.join(f.root, "worker-argv.json"))}, JSON.stringify(Object.fromEntries(args)));`);
    const result = await f.manager.run({ task: "disabled extensions", transport: "process", ...(source === "requested" ? { extensions: false } : {}) });
    expect(result.text).toBe("LOCAL");
    expect(JSON.parse(fs.readFileSync(path.join(f.root, "worker-argv.json"), "utf8"))["--extensions"]).toBe("false");
    const lines = fs.readFileSync(path.join(f.manager.runDirectory(result.id)!, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)).filter(line => line.type === "placement.local");
    expect(lines).toEqual([expect.objectContaining({ reason: "extensions disabled require the local worker" })]);
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it("allows an explicit extension opt-in to override the disabled host default", async () => {
    const f = fixture(); f.config.extensions = false;
    expect(await f.manager.run({ task: "enabled", extensions: true, transport: "process" })).toMatchObject({ status: "completed", text: "REMOTE: enabled" });
  });
  it("keeps actor and durable requests local with a recorded reason", async () => {
    const f=fixture();
    for(const request of [{task:"actor",actorId:"actor-probe"},{task:"durable",residency:"durable" as const}]) {
      const result=await f.manager.run(request); expect(result.text).toBe("LOCAL");
      expect(fs.readFileSync(path.join(f.manager.runDirectory(result.id)!,"events.jsonl"),"utf8")).toContain("placement.local");
    }
    expect(fs.existsSync(f.results)).toBe(false);
  });
  it("retains unknown remote exit debt rather than claiming cancellation proves exit", async () => {
    const f=fixture(); f.config.placement.cancelCommand=[process.execPath,f.launcher,"unconfirmed",f.results,"{id}"];
    const req=launch(f,"pending"); const h=await new ProcessTransport(undefined,f.config.placement).launch(req);
    await h.stop();
    expect(h.lostContact?.()).toContain("exit unconfirmed");
    expect(h.stopDebt?.()).toBe(h.lostContact?.());
    expect(await h.isAlive()).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(f.root,"direct/status.json"),"utf8")).status).toBe("stopped");
  });
  it.each(["reject","malformed"])("reports %s launch uncertainty, no local fallback", async task => {
    const f=fixture(); const result=await f.manager.run({task,transport:"process"});
    expect(result.status).toBe("failed"); expect(result.error).toContain("unknown");
    expect(fs.existsSync(path.join(f.manager.runDirectory(result.id)!,"unresolved-worker.json"))).toBe(true);
    expect(result.text).not.toBe("LOCAL");
  });
  it("audits unsupported one-shot features and configured local default", async () => {
    const f=fixture();
    for (const request of [{task:"tools",tools:["read"]},{task:"effort",thinking:"off" as const},{task:"default"}]) {
      if(request.task==="default") f.config.placement.default="local";
      const result=await f.manager.run(request); expect(result.text).toBe("LOCAL");
      expect(fs.readFileSync(path.join(f.manager.runDirectory(result.id)!,"events.jsonl"),"utf8")).toContain(request.task!=="default" ? "worker features" : "default is local");
    }
  });
});
