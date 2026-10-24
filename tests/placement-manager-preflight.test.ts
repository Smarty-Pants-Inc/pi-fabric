import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { PlacementAdmissionError, PlacementQueueError } from "../src/agents/placement-admission.js";
import { normalizePlacementDeciderConfig as normalizePlacementDeciderConfigRaw } from "../src/agents/placement-decider-config.js";

const normalizePlacementDeciderConfig = (value: unknown) => {
  const config = normalizePlacementDeciderConfigRaw(value);
  if (!config) throw new Error("placement test config unexpectedly normalized to undefined");
  return config;
};

const roots: string[] = [];
const managers: AgentManager[] = [];
const root = (): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "placement-manager-preflight-"));
  roots.push(directory);
  return directory;
};
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

// Trust-boundary fixture: the observer only sees the published prompt-free
// placement wire object; no provider, remote host, or native authorization runs.
const shadowCommands = (directory: string, holdGeneration?: number) => {
  const context = path.join(directory, "context.cjs");
  const decision = path.join(directory, "decision.cjs");
  const requests = path.join(directory, "requests.jsonl");
  const release = path.join(directory, "release");
  const held = path.join(directory, "held");
  const done = path.join(directory, "done");
  fs.writeFileSync(context, `process.stdout.write(JSON.stringify(${JSON.stringify({
    native_launch_id: "parent-native", launch_spec_id: "parent-spec", generation: 3,
    fence: { token: "fence", state: "active" }, host: "host-a", unit: "main-parent.service",
    boot_id: "boot-a", invocation_id: "invocation-a", main_pid_start: "1234",
    org_id: "org-a", work_id: "work-a", role: "project-agent", profile: "main-profile",
    source_pin: "source-pin", memory_profile: "pi-normal",
    origin: { class: "background-agent", origin_ref: "session:main" },
  })}));\n`);
  fs.writeFileSync(decision, `const fs = require('node:fs'); let input = '';
process.stdin.setEncoding('utf8'); process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
  const request = JSON.parse(input);
  fs.appendFileSync(${JSON.stringify(requests)}, JSON.stringify(request) + '\\n');
  const emit = () => {
    process.stdout.write(JSON.stringify({ schema: 'smarty.placement.shadow.v1', kind: 'SHADOW_OBSERVATION',
      launch_id: request.launch_id, generation: request.generation, pool_status: 'NOT_ENFORCED',
      hypothetical: request.generation === ${holdGeneration ?? -1}
        ? { decision: 'QUEUE', reason: 'FACTS_UNKNOWN', retry_after: 1 }
        : { decision: 'PLACE', host: 'never-route-shadow', model: 'hypothetical/model', thinking: 'max', pool_id: 'pool' },
      actual_baseline_ref: null }));
    fs.writeFileSync(${JSON.stringify(done)} + '-' + request.generation, 'done'); process.exitCode = 78;
  };
  if (request.generation !== ${holdGeneration ?? -1}) { emit(); return; }
  const finish = () => {
    if (!fs.existsSync(${JSON.stringify(release)})) return;
    watcher.close(); clearTimeout(timer); emit();
  };
  const watcher = fs.watch(${JSON.stringify(directory)}, finish);
  const timer = setTimeout(() => { watcher.close(); process.exitCode = 2; }, 1800);
  fs.writeFileSync(${JSON.stringify(held)}, 'held'); finish();
});\n`);
  return { requests, release, held, done, config: normalizePlacementDeciderConfig({
    command: [process.execPath, decision], contextCommand: [process.execPath, context],
    mode: "shadow", wakeSourceMachine: "ryzen1",
  }) };
};

const binding = { role: "task-agent", complexity: "normal" as const, profile: "task-normal", memoryProfileRef: "pi-normal",
  launchId: "stable-semantic-launch", generation: 7 };

describe("AgentManager placement preflight", () => {
  it("observes each replacement with the same launch id and monotonic base+1 generations", async () => {
    const directory = root();
    const observer = shadowCommands(directory);
    const worker = path.join(directory, "replacement.mjs");
    const attempts = path.join(directory, "attempts.jsonl");
    fs.writeFileSync(worker, `import fs from 'node:fs';
const args = new Map(); for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const attempt = fs.existsSync(${JSON.stringify(attempts)}) ? fs.readFileSync(${JSON.stringify(attempts)}, 'utf8').trim().split('\\n').length + 1 : 1;
fs.appendFileSync(${JSON.stringify(attempts)}, JSON.stringify({launchId:args.get('--placement-launch-id'), generation:Number(args.get('--placement-generation'))})+'\\n');
const now=Date.now(); fs.writeFileSync(args.get('--status-file'), JSON.stringify({
 id:args.get('--id'), name:args.get('--name'), task:'private placement replacement prompt', cwd:args.get('--cwd'), runner:'pi', transport:'process',
 status:attempt < 3 ? 'failed':'completed', startedAt:now, updatedAt:now, finishedAt:now, turns:0, toolCalls:0,
 text:attempt < 3 ? '':'replacement completed', ...(attempt < 3 ? {error:'No API key found'}:{}),
 usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}, exitCode:attempt < 3 ? 1:0 }));
process.exit(0);\n`);
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents,
      budgetUsd: 0, deniedModels: [], timeoutMs: 10_000, placementDecider: observer.config,
    }, { workerPath: worker, runRoot: path.join(directory, "runs") });
    managers.push(manager);
    const result = await manager.run({ task: "private placement replacement prompt", transport: "process", placement: binding });
    expect(result.status, result.error).toBe("completed");
    expect(fs.readFileSync(attempts, "utf8").trim().split("\n").map(line => JSON.parse(line))).toEqual([
      { launchId: binding.launchId, generation: 7 }, { launchId: binding.launchId, generation: 8 }, { launchId: binding.launchId, generation: 9 },
    ]);
    await vi.waitFor(() => expect(fs.existsSync(`${observer.done}-9`)).toBe(true));
    const sent = fs.readFileSync(observer.requests, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(sent.map(row => [row.launch_id, row.generation]).sort((a,b) => a[1]-b[1])).toEqual([
      [binding.launchId, 7], [binding.launchId, 8], [binding.launchId, 9],
    ]);
    expect(JSON.stringify(sent)).not.toContain("private placement replacement prompt");
    expect(JSON.stringify(sent)).not.toMatch(/provenance|actorActivation|contextCommand|wakeSourceMachine/u);
  }, 15_000);

  it("validates worker identity/generation on the host and refuses replay, malformed requests and stopped runs", async () => {
    const directory = root();
    const nativeLaunch = ProcessTransport.prototype.launch;
    let callback: AgentTransportLaunch["onPlacementRestart"];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function(this: ProcessTransport, request) {
      callback = request.onPlacementRestart;
      return nativeLaunch.call(this, request);
    });
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs"),
    }); managers.push(manager);
    const handle = await manager.spawn({ task: "HANG private callback prompt", transport: "process", placement: binding });
    const ask = callback!;
    const next = { type: "placement.restart" as const, launchId: binding.launchId, generation: 8 };
    expect(await ask({ ...next, launchId: "foreign" })).toBe("REFUSE");
    expect(await ask({ ...next, generation: 9 })).toBe("REFUSE");
    expect(await ask({ ...next, generation: NaN })).toBe("REFUSE");
    expect(await ask({ ...next, task: "must-not-be-accepted" } as typeof next)).toBe("REFUSE");
    expect(await ask(next)).toBe("BASELINE");
    expect(await ask(next)).toBe("REFUSE");
    expect(await ask({ ...next, generation: 9 })).toBe("BASELINE");
    await manager.stop(handle.id);
    expect(await ask({ ...next, generation: 10 })).toBe("REFUSE");
  });

  it("directly probes the source worker: a held shadow restart observer cannot delay the next fake Pi child and is later recorded", async () => {
    const directory = root();
    const observer = shadowCommands(directory, 8);
    const pi = path.join(directory, "fake-pi.mjs");
    const boots = path.join(directory, "pi-boots.jsonl");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.stubEnv("PI_FABRIC_TEST_RECOVERY_TIME_SCALE", "0.01");
    const profile = path.join(directory, "profile"); fs.mkdirSync(profile); vi.stubEnv("PI_CODING_AGENT_DIR", profile);
    // Opaque fake CLI: no SDK, provider, systemd or remote authorization. The
    // real worker drives admission, drain, same-session resume and parent IPC.
    fs.writeFileSync(pi, `import fs from 'node:fs'; import readline from 'node:readline';
const boots=${JSON.stringify(boots)}; const attempt=fs.existsSync(boots)?fs.readFileSync(boots,'utf8').trim().split('\\n').length+1:1;
fs.appendFileSync(boots,JSON.stringify({attempt,model:'fake/model',argv:process.argv.slice(2)})+'\\n');
const session=process.argv[process.argv.indexOf('--session')+1];
if(!fs.existsSync(session))fs.writeFileSync(session,JSON.stringify({type:'session',version:3,id:'fake-session',timestamp:new Date().toISOString(),cwd:process.cwd()})+'\\n');
const send=e=>process.stdout.write(JSON.stringify(e)+'\\n');
readline.createInterface({input:process.stdin}).on('line',line=>{
 const frame=JSON.parse(line); const reply=data=>send({type:'response',id:frame.id,command:frame.type,success:true,data});
 if(frame.type==='get_state'){reply({model:{provider:'fake',id:'model'},thinkingLevel:'high',isStreaming:false,isCompacting:false});return;}
 if(frame.type==='set_model'||frame.type==='set_thinking_level'){reply();return;}
 if(frame.type!=='prompt')return;
 const message={role:'assistant',provider:'fake',model:'model',content:attempt===1?[]:[{type:'text',text:'fake Pi restarted'}],
 usage:{input:0,output:0,cacheRead:0,cacheWrite:0},stopReason:attempt===1?'error':'stop',...(attempt===1?{errorMessage:'fetch failed'}:{})};
 send({type:'agent_start'});send({type:'message_end',message});send({type:'turn_end',message,toolResults:[]});
 send({type:'agent_end',messages:[message],willRetry:false});
 process.stdout.write(JSON.stringify({type:'agent_settled'})+'\\n',()=>process.exit(0));
});\n`);
    const nativeLaunch = ProcessTransport.prototype.launch;
    const launches: AgentTransportLaunch[] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function(this: ProcessTransport, request) {
      launches.push(request); return nativeLaunch.call(this, request);
    });
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [],
      timeoutMs: 10_000, placementDecider: observer.config,
    }, { workerPath: path.resolve("src/worker.ts"), piBinary: pi, runRoot: path.join(directory, "runs") });
    managers.push(manager);
    const handle = await manager.spawn({ task: "private direct worker restart prompt", transport: "process", extensions: false, placement: binding });
    try {
      await vi.waitFor(() => {
        expect(fs.existsSync(observer.held)).toBe(true);
        expect(fs.readFileSync(boots, "utf8").trim().split("\n")).toHaveLength(2);
      }, { timeout: 5_000 });
      expect(fs.existsSync(`${observer.done}-8`)).toBe(false);
      const result = await manager.wait(handle.id);
      expect(result.status, result.error).toBe("completed");
      expect(result.text).toBe("fake Pi restarted");
      expect(launches).toHaveLength(1); // Worker restart, NOT a replacement worker.
      expect(launches[0]!.placementGrant).toBeUndefined();
      expect(fs.existsSync(`${observer.done}-8`)).toBe(false);
    } finally { fs.writeFileSync(observer.release, "release"); }
    await vi.waitFor(() => expect(warn.mock.calls.some(call => String(call[0]).includes("shadow-queue"))).toBe(true), { timeout: 2_000 });
    const sent = fs.readFileSync(observer.requests, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(sent.map(row => [row.launch_id, row.generation]).sort((a,b) => a[1]-b[1])).toEqual([[binding.launchId, 7], [binding.launchId, 8]]);
    expect(JSON.stringify(sent)).not.toMatch(/private direct|provenance|actorActivation|contextCommand|wakeSourceMachine/u);
  }, 15_000);

  it("keeps enforce unavailable in public host config", () => {
    expect(() => normalizeFabricConfig({ agents: { placementDecider: {
      command: [process.execPath], mode: "enforce", wakeSourceMachine: "ryzen1",
    } } })).toThrow(/enforce is unavailable/u);
  });
  it("observes shadow placement without waiting for the decider or withholding the worker", async () => {
    const directory = root();
    const context = path.join(directory, "context.cjs");
    const decision = path.join(directory, "decision.cjs");
    const requestFile = path.join(directory, "request.json");
    const decisionStarted = path.join(directory, "decision-started");
    const decisionDone = path.join(directory, "decision-done");
    const release = path.join(directory, "release-observer");
    fs.writeFileSync(context, `process.stdout.write(JSON.stringify(${JSON.stringify({
      native_launch_id: "parent-native", launch_spec_id: "parent-spec", generation: 3,
      fence: { token: "fence", state: "active" }, host: "host-a", unit: "main-parent.service",
      boot_id: "boot-a", invocation_id: "invocation-a", main_pid_start: "1234",
      org_id: "org-a", work_id: "work-a", role: "project-agent", profile: "main-profile",
      source_pin: "source-pin", memory_profile: "pi-normal",
      origin: { class: "background-agent", origin_ref: "session:main" },
    })}));\n`);
    fs.writeFileSync(decision, `const fs = require('node:fs'); let input = '';
process.stdin.setEncoding('utf8'); process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
  const request = JSON.parse(input);
  fs.writeFileSync(${JSON.stringify(requestFile)}, JSON.stringify(request));
  fs.writeFileSync(${JSON.stringify(decisionStarted)}, 'started');
  const finish = () => {
    if (!fs.existsSync(${JSON.stringify(release)})) return;
    watcher.close(); clearTimeout(deadline);
    process.stdout.write(JSON.stringify({ schema: 'smarty.placement.shadow.v1', kind: 'SHADOW_OBSERVATION',
      launch_id: request.launch_id, generation: request.generation, pool_status: 'NOT_ENFORCED',
      hypothetical: { decision: 'QUEUE', reason: 'FACTS_UNKNOWN', retry_after: 1 }, actual_baseline_ref: null }));
    fs.writeFileSync(${JSON.stringify(decisionDone)}, 'done'); process.exitCode = 78;
  };
  const watcher = fs.watch(${JSON.stringify(directory)}, finish);
  const deadline = setTimeout(() => { watcher.close(); process.exitCode = 2; }, 1800);
  finish();
});\n`);
    const placementDecider = normalizePlacementDeciderConfig({
      command: [process.execPath, decision], contextCommand: [process.execPath, context],
      mode: "shadow", wakeSourceMachine: "ryzen1",
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", transport: "process", retainRuns: false,
      budgetUsd: 0, placementDecider,
    }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs") });
    managers.push(manager);
    const launching = manager.spawn({ task: "HANG private prompt must never reach placement", transport: "process",
      placement: { role: "task-agent", complexity: "normal", profile: "task-normal", memoryProfileRef: "pi-normal" } });
    let handle: { id: string } | undefined;
    let launchFailure: unknown;
    let timeout: NodeJS.Timeout | undefined;
    try {
      handle = await Promise.race([launching, new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error("shadow observation delayed worker launch")), 800);
      })]);
      await vi.waitFor(() => expect(fs.existsSync(decisionStarted)).toBe(true), { timeout: 1_000 });
      expect(fs.existsSync(decisionDone)).toBe(false);
      expect(manager.status(handle.id).status).toBe("running");
      const sent = JSON.parse(fs.readFileSync(requestFile, "utf8")) as Record<string, unknown>;
      expect(sent).toMatchObject({ operation: "decide", mode: "enforced", role: "task-agent",
        interaction: { class: "background-agent", origin_ref: "session:main" } });
      expect(JSON.stringify(sent)).not.toContain("private prompt");
    } catch (error) { launchFailure = error; }
    finally {
      if (timeout) clearTimeout(timeout);
      fs.writeFileSync(release, "release");
      await vi.waitFor(() => expect(fs.existsSync(decisionDone)).toBe(true), { timeout: 2_000 }).catch(() => undefined);
      handle ??= await launching.catch(() => undefined);
      if (handle) await manager.stop(handle.id);
    }
    if (launchFailure) throw launchFailure;
  });
  it("fails closed on missing trusted context before a Pi worker or decider fallback starts", async () => {
    const directory = root();
    const context = path.join(directory, "context.cjs");
    const decision = path.join(directory, "decision.cjs");
    const workerMarker = path.join(directory, "worker-started");
    const decisionMarker = path.join(directory, "decider-started");
    const worker = path.join(directory, "worker.mjs");
    fs.writeFileSync(context, "process.stdout.write('{}');\n");
    fs.writeFileSync(decision, `require('node:fs').writeFileSync(${JSON.stringify(decisionMarker)}, 'called');\nprocess.stdout.write('{}');\n`);
    fs.writeFileSync(worker, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(workerMarker)}, 'started');\n`);
    const placementDecider = normalizePlacementDeciderConfig({
      command: [process.execPath, decision], contextCommand: [process.execPath, context],
      mode: "enforce", wakeSourceMachine: "ryzen1",
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", transport: "process", retainRuns: false,
      budgetUsd: 0, placementDecider,
    }, { workerPath: worker, runRoot: path.join(directory, "runs") });
    managers.push(manager);

    let failure: unknown;
    try { await manager.spawn({ task: "private prompt must not reach placement", transport: "process" }); }
    catch (error) { failure = error; }
    expect(failure).toBeInstanceOf(PlacementAdmissionError);
    expect(failure).toMatchObject({ reason: "invalid-request" });
    expect(fs.existsSync(workerMarker)).toBe(false);
    expect(fs.existsSync(decisionMarker)).toBe(false);
  });

  it("preserves live-human origin and refuses a QUEUE without launching", async () => {
    const directory = root();
    const context = path.join(directory, "context.cjs");
    const decision = path.join(directory, "decision.cjs");
    const decisionRequests = path.join(directory, "decision-requests.jsonl");
    const workerMarker = path.join(directory, "worker-started");
    const worker = path.join(directory, "worker.mjs");
    fs.writeFileSync(context, `process.stdout.write(JSON.stringify(${JSON.stringify({
      native_launch_id: "parent-native", launch_spec_id: "parent-spec", generation: 3,
      fence: { token: "fence", state: "active" }, host: "host-a", unit: "main-parent.service",
      boot_id: "boot-a", invocation_id: "invocation-a", main_pid_start: "1234",
      org_id: "org-a", work_id: "work-a", role: "project-agent", profile: "main-profile",
      source_pin: "source-pin", memory_profile: "pi-normal",
      origin: { class: "live-human", origin_ref: "session:main" },
    })}));\n`);
    fs.writeFileSync(decision, `const fs = require('node:fs'); let input = '';
process.stdin.setEncoding('utf8'); process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
  const request = JSON.parse(input);
  fs.appendFileSync(${JSON.stringify(decisionRequests)}, JSON.stringify(request) + '\\n');
  process.stdout.write(JSON.stringify({ schema: 'smarty.placement.v1', decision: 'QUEUE', mode: 'enforced',
    launch_id: request.launch_id, generation: request.generation, reason: 'AGENT_CAPACITY_EXHAUSTED',
    retry_after: 1, evidence: [], queue_id: 'queue-1' }));
  process.exitCode = 75;
});\n`);
    fs.writeFileSync(worker, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(workerMarker)}, 'started');\n`);
    const placementDecider = normalizePlacementDeciderConfig({
      command: [process.execPath, decision], contextCommand: [process.execPath, context],
      mode: "enforce", wakeSourceMachine: "ryzen1",
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", transport: "process", retainRuns: false,
      budgetUsd: 0, placementDecider,
    }, { workerPath: worker, runRoot: path.join(directory, "runs") });
    managers.push(manager);

    let failure: unknown;
    try {
      await manager.spawn({ task: "private prompt must not reach placement", transport: "process",
        placement: { role: "task-agent", complexity: "normal", profile: "task-normal", memoryProfileRef: "pi-normal",
          launchId: "semantic-launch", generation: 4 } });
    } catch (error) { failure = error; }
    expect(failure).toBeInstanceOf(PlacementAdmissionError);
    expect(failure).toMatchObject({ reason: "invalid-response" });
    const sent = JSON.parse(fs.readFileSync(decisionRequests, "utf8").trim()) as Record<string, unknown>;
    expect(sent).toMatchObject({ launch_id: "semantic-launch", generation: 4, interaction: { class: "live-human", origin_ref: "session:main" } });
    expect(JSON.stringify(sent)).not.toContain("private prompt");
    expect(fs.existsSync(workerMarker)).toBe(false);
  });

  it("returns a background QUEUE with its exact identity and no worker start", async () => {
    const directory = root();
    const context = path.join(directory, "context.cjs");
    const decision = path.join(directory, "decision.cjs");
    const workerMarker = path.join(directory, "worker-started");
    const worker = path.join(directory, "worker.mjs");
    fs.writeFileSync(context, `process.stdout.write(JSON.stringify(${JSON.stringify({
      native_launch_id: "parent-native", launch_spec_id: "parent-spec", generation: 3,
      fence: { token: "fence", state: "active" }, host: "host-a", unit: "main-parent.service",
      boot_id: "boot-a", invocation_id: "invocation-a", main_pid_start: "1234",
      org_id: "org-a", work_id: "work-a", role: "project-agent", profile: "main-profile",
      source_pin: "source-pin", memory_profile: "pi-normal",
      origin: { class: "background-agent", origin_ref: "session:main" },
    })}));\n`);
    fs.writeFileSync(decision, `let input = ''; process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
  const request = JSON.parse(input);
  process.stdout.write(JSON.stringify({ schema: 'smarty.placement.v1', decision: 'QUEUE', mode: 'enforced',
    launch_id: request.launch_id, generation: request.generation, reason: 'AGENT_CAPACITY_EXHAUSTED',
    retry_after: 1, evidence: [], queue_id: 'queue-1' }));
  process.exitCode = 75;
});\n`);
    fs.writeFileSync(worker, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(workerMarker)}, 'started');\n`);
    const placementDecider = normalizePlacementDeciderConfig({
      command: [process.execPath, decision], contextCommand: [process.execPath, context],
      mode: "enforce", wakeSourceMachine: "ryzen1",
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", transport: "process", retainRuns: false,
      budgetUsd: 0, placementDecider,
    }, { workerPath: worker, runRoot: path.join(directory, "runs") });
    managers.push(manager);

    let failure: unknown;
    try {
      await manager.spawn({ task: "private prompt must not reach placement", transport: "process",
        placement: { role: "task-agent", complexity: "normal", profile: "task-normal", memoryProfileRef: "pi-normal",
          launchId: "semantic-launch", generation: 4 } });
    } catch (error) { failure = error; }
    expect(failure).toBeInstanceOf(PlacementQueueError);
    expect(failure).toMatchObject({
      request: { launch_id: "semantic-launch", generation: 4, interaction: { class: "background-agent" } },
      queue: { queue_id: "queue-1", launch_id: "semantic-launch", generation: 4, reason: "AGENT_CAPACITY_EXHAUSTED" },
    });
    expect(JSON.stringify((failure as PlacementQueueError).request)).not.toContain("private prompt");
    expect(fs.existsSync(workerMarker)).toBe(false);
  });
});
