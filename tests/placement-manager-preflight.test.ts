import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
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
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("AgentManager placement preflight", () => {
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
