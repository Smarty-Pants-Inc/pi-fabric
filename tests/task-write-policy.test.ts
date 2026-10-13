import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { checkWritePolicyRequest, resolveChildWritePolicy } from "../src/agents/child-env.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import type { AgentRunRequest } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";

// All files are allocated below this test's own TMPDIR. No destructive test paths or cleanup calls.
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});
const fixture = (placement = false) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-write-")));
  const cwd = path.join(root, "work");
  fs.mkdirSync(cwd);
  const outside = path.join(root, "outside.txt");
  const worker = path.join(root, "worker.mjs");
  // The launch fixture uses the production guard and records real allowed writes/refused attempts.
  fs.writeFileSync(worker, `import fs from 'node:fs'; import path from 'node:path';
import { readWritePolicy, writePolicyDenial } from ${JSON.stringify(pathToFileURL(path.resolve("src/agents/write-guard.ts")).href)};
const args = new Map(); for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const policy = readWritePolicy(args.get('--write-policy'));
const cwd = args.get('--cwd'), tmpdir = args.get('--tmpdir');
const probes = [];
if (args.get('--runner') === 'pi' && !args.get('--actor-id') && !args.get('--resident-startup-probe')) {
  for (const file of [path.join(cwd,'inside.txt'), ...(tmpdir ? [path.join(tmpdir,'scratch.txt')] : []), ${JSON.stringify(outside)}]) {
    const reason = policy && writePolicyDenial(policy,'write',{path:file,content:'probe'},cwd);
    if (reason) probes.push({file,allowed:false,reason});
    else { fs.writeFileSync(file,'probe'); probes.push({file,allowed:true}); }
  }
}
fs.appendFileSync(args.get('--log-file'),JSON.stringify({type:'agent_start'})+'\\n');
const now=Date.now(); fs.writeFileSync(args.get('--status-file'),JSON.stringify({id:args.get('--id'),name:args.get('--name'),task:'probe',status:'completed',runner:args.get('--runner'),transport:'process',sessionId:String(process.pid),cwd,startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:probes.length,text:JSON.stringify({args:Object.fromEntries(args),policy:policy??null,tmpdir:tmpdir??null,probes}),exitCode:0,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));`);
  let remote;
  if (placement) {
    const launcher = path.join(root, "launcher.mjs");
    const results = path.join(root, "placed");
    fs.writeFileSync(launcher, `import fs from 'node:fs'; import path from 'node:path';
const [mode,id]=process.argv.slice(2); if(mode==='launch'){const dir=path.join(${JSON.stringify(results)},id);fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(path.join(dir,'result.md'),'REMOTE');fs.writeFileSync(path.join(dir,'rc'),'0');console.log('RYZEN2_TASK_ACCEPTED '+id+' on testhost');}`);
    remote = normalizeFabricConfig({ agents: { placement: {
      default: "remote", command: [process.execPath, launcher, "launch", "{id}", "{task}"],
      resultDirectory: path.join(results, "{id}"), cancelCommand: [process.execPath, launcher, "cancel", "{id}"],
      commandTimeoutMs: 5_000, pollIntervalMs: 10,
    } } }).agents.placement;
  }
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, retainRuns: true,
    ...(remote ? { placement: remote } : {}),
  }, { workerPath: worker, runRoot: path.join(root, "runs"), fullCodeMode: false });
  managers.push(manager);
  const events = (id: string) => fs.readFileSync(path.join(manager.runDirectory(id)!, "events.jsonl"), "utf8")
    .trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
  return { root, cwd, outside, manager, events };
};
const report = async (f: ReturnType<typeof fixture>, request: Partial<AgentRunRequest> = {}) => {
  const handle = await f.manager.spawn({ task: "probe", transport: "process", ...request });
  const result = await f.manager.wait(handle.id);
  expect(result.status).toBe("completed");
  return { result, value: JSON.parse(result.text) };
};

describe("local process task write default", () => {
  it("allows real writes in cwd and its private scratch, refuses outside, and logs the shell gap once", async () => {
    const f = fixture();
    const { result, value } = await report(f);
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [f.cwd, value.tmpdir].sort(), shell: "unconfined" });
    expect(value.tmpdir).toBe(path.join(f.manager.runDirectory(result.id)!, "tmp"));
    if (process.platform !== "win32") expect(fs.statSync(value.tmpdir).mode & 0o777).toBe(0o700);
    expect(value.probes.map((probe: { allowed: boolean }) => probe.allowed)).toEqual([true, true, false]);
    expect(fs.readFileSync(path.join(f.cwd, "inside.txt"), "utf8")).toBe("probe");
    expect(fs.readFileSync(path.join(value.tmpdir, "scratch.txt"), "utf8")).toBe("probe");
    expect(fs.existsSync(f.outside)).toBe(false);
    expect(f.events(result.id).filter(event => event.type.startsWith("write_policy.")))
      .toEqual([expect.objectContaining({ type: "write_policy.default", policy: value.policy, warning: expect.stringContaining("Shell writes are unconfined") })]);
  });

  it("enforces the same default for nested pi.write and pi.edit while leaving bash allowed", async () => {
    const f = fixture();
    const { value } = await report(f);
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify(value.policy));
    const provider = new PiToolsProvider(f.cwd, undefined, undefined, { allowUnhookedTools: true, powerShellToolDefinitionFactory: undefined });
    const context = { cwd: f.cwd, extensionContext: { cwd: f.cwd,
      sessionManager: { getSessionId: () => "task-write-test", getSessionFile: () => undefined },
    }, parentToolCallId: "outer", nestedToolCallId: "nested", update() {} } as unknown as FabricInvocationContext;
    try {
      await provider.invoke("write", { path: "nested.txt", content: "one" }, context);
      await provider.invoke("edit", { path: "nested.txt", edits: [{ oldText: "one", newText: "two" }] }, context);
      expect(fs.readFileSync(path.join(f.cwd, "nested.txt"), "utf8")).toBe("two");
      await expect(provider.invoke("write", { path: f.outside, content: "escape" }, context)).rejects.toThrow(/outside writable roots/);
      await expect(provider.invoke("edit", { path: f.outside, edits: [{ oldText: "one", newText: "escape" }] }, context)).rejects.toThrow(/outside writable roots/);
      expect(fs.existsSync(f.outside)).toBe(false);
      const shell = await provider.invoke("bash", { command: "echo nested-shell-allowed", timeout: 5 }, context);
      expect(JSON.stringify(shell)).toContain("nested-shell-allowed");
    } finally { await provider.close(); }
  });

  it("keeps explicit readOnly and wider roots instead of adding the default scratch", async () => {
    const readonly = fixture();
    const ro = await report(readonly, { readOnly: true });
    expect(ro.value.policy).toEqual({ readOnly: true, writableRoots: [], shell: "deny" });
    expect(ro.value.tmpdir).toBeNull();
    expect(ro.value.probes.every((probe: { allowed: boolean }) => !probe.allowed)).toBe(true);
    const wider = fixture();
    const rw = await report(wider, { writableRoots: [wider.root], shell: "unconfined" });
    expect(rw.value.policy.writableRoots).toEqual([wider.root]);
    expect(rw.value.tmpdir).toBeNull();
    expect(fs.readFileSync(wider.outside, "utf8")).toBe("probe");
  });

  it("opts out explicitly and logs unrestricted once", async () => {
    const f = fixture();
    const { result, value } = await report(f, { writePolicy: "unrestricted" });
    expect(value.policy).toBeNull();
    expect(value.tmpdir).toBeNull();
    expect(fs.readFileSync(f.outside, "utf8")).toBe("probe");
    expect(f.events(result.id).filter(event => event.type.startsWith("write_policy.")))
      .toEqual([expect.objectContaining({ type: "write_policy.unrestricted" })]);
  });

  it("keeps actor and resident startup launches unchanged", async () => {
    for (const request of [{ actorId: "actor" }, { residentStartupProbe: true }]) {
      const f = fixture();
      const { result, value } = await report(f, request);
      expect(value.policy).toBeNull();
      expect(value.tmpdir).toBeNull();
      expect(f.events(result.id).filter(event => event.type.startsWith("write_policy."))).toEqual([]);
    }
  });

  it.each(["claude", "veda"])("keeps %s unrestricted and logs the unsupported runner once", async runner => {
    const f = fixture();
    const { result, value } = await report(f, { runner });
    expect(value.policy).toBeNull();
    expect(value.tmpdir).toBeNull();
    expect(f.events(result.id).filter(event => event.type.startsWith("write_policy.")))
      .toEqual([expect.objectContaining({ type: "write_policy.unsupported", runner })]);
  });

  it("preserves default remote placement and logs the missing forwarding rather than claiming confinement", async () => {
    const f = fixture(true);
    const handle = await f.manager.spawn({ task: "remote probe", transport: "process" });
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "completed", text: "REMOTE" });
    expect(f.events(handle.id).filter(event => event.type.startsWith("write_policy.")))
      .toEqual([expect.objectContaining({ type: "write_policy.remote_unconfined" })]);
    expect(f.events(handle.id).some(event => event.type === "placement.remote")).toBe(true);
    expect(fs.existsSync(path.join(f.cwd, "inside.txt"))).toBe(false);
  });

  it("pins an explicit policy local instead of silently dropping it at remote placement", async () => {
    const f = fixture(true);
    const { result, value } = await report(f, { readOnly: true });
    expect(value.policy.readOnly).toBe(true);
    expect(f.events(result.id).filter(event => event.type.startsWith("placement.")))
      .toEqual([expect.objectContaining({ type: "placement.local", reason: expect.stringContaining("explicit or inherited write policy") })]);
    expect(fs.existsSync(path.join(f.root, "placed"))).toBe(false);
  });

  it("inherits a caller policy and refuses an unrestricted escape before launch", async () => {
    const allowed = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-parent-")));
    const policy = { readOnly: true, writableRoots: [], shell: "deny" as const };
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify(policy));
    const f = fixture();
    const inherited = await report(f);
    expect(inherited.value.policy).toEqual(policy);
    expect(inherited.value.tmpdir).toBeNull();
    await expect(f.manager.spawn({ task: "escape", writePolicy: "unrestricted" })).rejects.toThrow(/cannot launch an unrestricted child/);
    expect(resolveChildWritePolicy({}, policy, allowed)).toBe(policy);
    expect(() => resolveChildWritePolicy({ writePolicy: "unrestricted" }, policy, allowed)).toThrow(/unrestricted child/);
  });

  it("validates and exposes the opt-out without accepting a malformed policy or ambiguous combination", () => {
    for (const request of [{ writePolicy: "anything" }, { writePolicy: "unrestricted", readOnly: false }, { writePolicy: "unrestricted", writableRoots: [] }, { writePolicy: "unrestricted", shell: "unconfined" }]) {
      expect(() => checkWritePolicyRequest(request as unknown as AgentRunRequest)).toThrow(/writePolicy/);
    }
    expect(normalizeAgentRunRequest({ task: "x", writePolicy: "unrestricted" }, { runner: "pi", timeoutMs: 1000 }).writePolicy).toBe("unrestricted");
    for (const name of ["spawn", "run"]) {
      const schema = AGENTS_ACTION_DESCRIPTORS.find(action => action.name === name)!.inputSchema as { properties: Record<string, unknown> };
      expect(schema.properties.writePolicy).toMatchObject({ type: "string", enum: ["unrestricted"] });
    }
  });
});
