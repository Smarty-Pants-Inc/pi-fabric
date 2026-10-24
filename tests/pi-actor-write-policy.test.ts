import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { modelExceptionGrant } from "./helpers/model-exceptions.js";
import fabricWriteGuard, { writePolicyDenial } from "../src/agents/write-guard.js";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// Trust-boundary regression: real AgentManager admission/launch and production
// write guard, with a tiny local worker. All files stay in fresh test-owned temp;
// no worktrees, native Pi, unguarded deletion, or inherited fleet state is needed.
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});
const fixture = (executorRuntime?: string) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-actor-write-policy-")));
  const cwd = path.join(root, "work");
  fs.mkdirSync(cwd);
  const runRoot = path.join(cwd, "resident-runs"); // overlap must never disable actor confinement
  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs'; import path from 'node:path';
import {readWritePolicy,writePolicyDenial} from ${JSON.stringify(pathToFileURL(path.resolve("src/agents/write-guard.ts")).href)};
const args=new Map();for(let i=2;i<process.argv.length;i+=2)args.set(process.argv[i],process.argv[i+1]);
const policy=readWritePolicy(args.get('--write-policy')),tmpdir=args.get('--tmpdir'),cwd=args.get('--cwd');
const probes=[];
for(const file of [path.join(cwd,'state.txt'),...(tmpdir?[path.join(tmpdir,'scratch.txt')]:[])]) {
 const reason=policy&&writePolicyDenial(policy,'write',{path:file},cwd);
 if(!reason)fs.writeFileSync(file,'probe');probes.push({file,allowed:!reason});
}
fs.appendFileSync(args.get('--log-file'),JSON.stringify({type:'agent_start'})+'\\n');
const now=Date.now();fs.writeFileSync(args.get('--status-file'),JSON.stringify({id:args.get('--id'),name:args.get('--name'),task:'probe',status:'completed',runner:args.get('--runner'),transport:'process',sessionId:String(process.pid),cwd,startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:probes.length,text:JSON.stringify({policy:policy??null,tmpdir:tmpdir??null,probes}),exitCode:0,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));`);
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, retainRuns: true },
    { workerPath: worker, runRoot, fullCodeMode: false, ...(executorRuntime ? { executorRuntime: () => executorRuntime } : {}) });
  managers.push(manager);
  return { cwd, runRoot, manager };
};
const report = async (f: ReturnType<typeof fixture>, request: Partial<AgentRunRequest>) => {
  const result = await f.manager.run({ task: "probe", transport: "process", ...request });
  expect(result.status, result.error).toBe("completed");
  return { value: JSON.parse(result.text), run: f.manager.runDirectory(result.id)!, temporaryDirectory: result.temporaryDirectory };
};

describe("Pi actor scratch-only write policy", () => {
  it.each([
    { runner: "pi", actorId: "actor" }, { runner: "pi", actorName: "actor" },
    { runner: "pi-durable", actorId: "actor" }, { runner: "pi-durable", actorName: "actor" },
  ])("confines $runner / $actorId / $actorName direct activations despite cwd/run-root overlap", async identity => {
    const f = fixture();
    const { value, run, temporaryDirectory } = await report(f, identity);
    // smarty-dev#15878: a short private TMPDIR when one fits the host's temp path, else the run's own tmp.
    expect(value.tmpdir).toBe(temporaryDirectory ?? path.join(run, "tmp"));
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [value.tmpdir], shell: "unconfined" });
    if (process.platform !== "win32") expect(fs.statSync(value.tmpdir).mode & 0o777).toBe(0o700);
    expect(value.probes.map((probe: { allowed: boolean }) => probe.allowed)).toEqual([false, true]);
    expect(fs.existsSync(path.join(f.cwd, "state.txt"))).toBe(false);
    expect(fs.readFileSync(path.join(value.tmpdir, "scratch.txt"), "utf8")).toBe("probe");
    for (const tool of ["write", "edit"]) {
      for (const file of [path.join(f.cwd, "state.txt"), path.join(run, "status.json"), path.join(run, "events.jsonl"),
        path.join(run, "nested", "child", "task.txt"), path.join(path.dirname(run), "peer", "tmp", "state.txt")]) {
        expect(writePolicyDenial(value.policy, tool, { path: file }, f.cwd)).toContain("outside writable roots");
      }
      expect(writePolicyDenial(value.policy, tool, { path: path.join(value.tmpdir, "ok.txt") }, f.cwd)).toBeUndefined();
    }
  });

  it.each([{ readOnly: true }, { readOnly: false }, { writableRoots: [] }])("preserves explicit policy shell denial: %j", async policy => {
    const { value } = await report(fixture(), { actorName: "explicit", ...policy });
    expect(value.policy).toEqual({ readOnly: policy.readOnly ?? false, writableRoots: policy.readOnly ? [] : [value.tmpdir], shell: "deny" });
    expect(fs.statSync(value.tmpdir).isDirectory()).toBe(true);
  });

  it("preserves explicit shell unconfined with readOnly while leaving writes denied", async () => {
    const { value } = await report(fixture(), { actorId: "reader", readOnly: true, shell: "unconfined" });
    expect(value.policy).toEqual({ readOnly: true, writableRoots: [], shell: "unconfined" });
    expect(value.probes.every((probe: { allowed: boolean }) => !probe.allowed)).toBe(true);
  });

  it("inherits readOnly and shell denial without allowing a writable escape", async () => {
    const policy = { readOnly: true, writableRoots: [], shell: "deny" };
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify(policy));
    const f = fixture();
    const { value } = await report(f, { actorId: "reader" });
    expect(value.policy).toEqual(policy);
    expect(fs.statSync(value.tmpdir).isDirectory()).toBe(true);
    await expect(f.manager.spawn({ task: "escape", actorId: "reader", readOnly: false })).rejects.toThrow(/read-only agent/);
    await expect(f.manager.spawn({ task: "escape", actorName: "reader", shell: "unconfined" })).rejects.toThrow(/shell-confined agent/);
  });

  it("never grants all inherited writable roots", async () => {
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify({ readOnly: false, writableRoots: [fs.realpathSync(os.tmpdir())], shell: "deny" }));
    const { value } = await report(fixture(), { actorName: "narrowed" });
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [value.tmpdir], shell: "deny" });
  });

  it.each([{ actorId: "actor" }, { actorName: "actor" }])("rejects roots and unrestricted opt-outs before launch: %j", async identity => {
    const f = fixture();
    const entries = () => fs.existsSync(f.runRoot) ? fs.readdirSync(f.runRoot) : [];
    const before = entries();
    await expect(f.manager.spawn({ task: "escape", ...identity, writePolicy: "unrestricted" })).rejects.toThrow(/Actors cannot use writePolicy/);
    await expect(f.manager.spawn({ task: "escape", ...identity, writableRoots: [f.cwd] })).rejects.toThrow(/only private run scratch/);
    expect(f.manager.listForUi()).toEqual([]);
    expect(entries()).toEqual(before);
  });

  it.each([{ readOnly: "false" }, { writableRoots: null }, { writableRoots: "cwd" }, { writableRoots: [42] }])("fails closed on malformed direct policies: %j", async policy => {
    const f = fixture();
    await expect(f.manager.spawn({ task: "malformed", actorName: "actor", ...policy } as unknown as AgentRunRequest)).rejects.toThrow(/readOnly|writableRoots/);
    expect(f.manager.listForUi()).toEqual([]);
  });

  it("fails closed on a malformed inherited policy", async () => {
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify({ readOnly: "false", writableRoots: [], shell: "unconfined" }));
    const { value } = await report(fixture(), { actorId: "malformed-parent" });
    expect(value.policy).toEqual({ readOnly: true, writableRoots: [], shell: "deny" });
  });

  it("uses the same shell default for native-executor preflight and launch", async () => {
    const f = fixture("node-process");
    await expect(f.manager.spawn({ task: "denied", actorId: "actor", kernel: "typescript", readOnly: true })).rejects.toThrow(/native Fabric executor/);
    await expect(f.manager.spawn({ task: "denied", actorName: "actor", kernel: "typescript", writableRoots: [] })).rejects.toThrow(/native Fabric executor/);
    const { value } = await report(f, { actorId: "actor", kernel: "typescript" });
    expect(value.policy.shell).toBe("unconfined");
  });

  it("keeps Claude actor defaults and explicit unrestricted historical behavior", async () => {
    const f = fixture();
    const model = "claude-haiku-test";
    const modelReason = "Named Claude actor write-policy exception #11864";
    const grant = modelExceptionGrant({ model, lane: path.basename(f.cwd), role: "actor:claude" });
    // Isolate this write-policy test from the host's CLI and grant file. The
    // model-exception admission and source-trust paths have dedicated tests.
    vi.spyOn(os, "homedir").mockReturnValue(path.dirname(f.cwd));
    vi.spyOn(f.manager, "claudeModels").mockResolvedValue([{
      value: "default", resolvedModel: model, resolvedModelKnown: true,
      displayName: "Default (test)", description: "Known native default",
    }]);
    const admit = vi.spyOn(f.manager, "admitModelException").mockReturnValue(grant);
    try {
      for (const policy of [{}, { writePolicy: "unrestricted" as const }]) {
        const { value } = await report(f, { runner: "claude", actorName: "claude", modelReason, ...policy });
        expect(value.policy).toBeNull();
        expect(value.tmpdir).toBeNull();
      }
      for (const policy of [{ readOnly: false }, { writableRoots: [] }, { shell: "deny" as const }]) {
        await expect(f.manager.spawn({ task: "unsupported", runner: "claude", actorId: "claude", modelReason, ...policy })).rejects.toThrow(/only by the Pi runner/);
      }
      expect(admit.mock.calls.slice(0, 2).map(([resolvedModel, reason, runner, role]) => [resolvedModel, reason, runner, role]))
        .toEqual(Array(2).fill([model, modelReason, "claude", "actor:claude"]));
    } finally {
      vi.restoreAllMocks();
    }
  });

  it("keeps resident startup and ordinary overlapping tasks unchanged", async () => {
    for (const request of [{ residentStartupProbe: true }, {}]) {
      const { value } = await report(fixture(), request);
      expect(value.policy).toBeNull();
      expect(value.tmpdir).toBeNull();
    }
  });

  it("preserves ordinary refusal bytes and appends the actor hint in both direct and tool_call routes", () => {
    const f = fixture();
    const outside = path.join(path.dirname(f.cwd), "factory-state.json");
    const policy = { readOnly: false, writableRoots: [f.cwd], shell: "unconfined" as const };
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify(policy));
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_ACTOR_NAME", undefined);
    let handler!: (event: unknown, context: unknown) => unknown;
    fabricWriteGuard({ on: (_name: string, callback: typeof handler) => { handler = callback; } } as unknown as ExtensionAPI);
    for (const toolName of ["write", "edit"]) {
      const ordinary = `Fabric write policy refuses ${toolName} outside writable roots; refused path: ${JSON.stringify(outside)}; allowed roots: ${JSON.stringify(policy.writableRoots)}`;
      expect(writePolicyDenial(policy, toolName, { path: outside }, f.cwd)).toBe(ordinary);
      for (const key of ["PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME"]) {
        vi.stubEnv(key, "actor");
        const reason = `${ordinary}; write outside actor scratch refused; use the shell or a trusted helper for factory state`;
        expect(writePolicyDenial(policy, toolName, { path: outside }, f.cwd)).toBe(reason);
        expect(handler({ toolName, input: { path: outside } }, { cwd: f.cwd })).toEqual({ block: true, reason });
        expect(writePolicyDenial(policy, toolName, { path: path.join(f.cwd, "ok.txt") }, f.cwd)).toBeUndefined();
        expect(writePolicyDenial(policy, "bash", { command: "echo ok" }, f.cwd)).toBeUndefined();
        vi.stubEnv(key, undefined);
      }
    }
  });
});
