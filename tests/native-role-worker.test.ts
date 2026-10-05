import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { snapshotNativeRoleBinding } from "../src/agents/native-role-binding.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const workerPath = path.resolve("dist/worker.js");
const nativeTools = ["read", "grep", "find", "ls", "bash", "write"];
const roles = ["review-agent", "security-agent"] as const;
const roleCases = roles.flatMap(role => [role, `pi-fabric-${role === "review-agent" ? "review" : "security"}-astra`,
  `pi-fabric-shards8-2-${role === "review-agent" ? "review" : "security"}-astra`].map(name => ({ role, name })));
const roots: string[] = [];
const managers: AgentManager[] = [];
const actors: ActorManager[] = [];
afterEach(async () => {
  await Promise.all(actors.splice(0).map(actor => actor.close()));
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const runActor = async (role: typeof roles[number], scenario: string, name: string = role) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-role-process-")); roots.push(root);
  const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ extensions: [path.resolve("tests/fixtures/native-role-probe.ts")], enableInstallTelemetry: false }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: false,
    mesh: { enabled: false }, agents: { model: "role-probe/global", thinking: "medium", defaultTools: ["read"], budgetUsd: 0, timeoutMs: 20000 },
    approvals: { shell: "allow", write: "allow", agent: "allow", network: "deny" } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("NATIVE_ROLE_PROBE_SCENARIO", scenario);
  for (const key of ["PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_TOOL_ALLOWLIST", "PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE"]) vi.stubEnv(key, undefined);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 30000 }, {
    workerPath, fullCodeMode: false, piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), runRoot: path.join(root, "runs"), mainAgentId: "session:role-test",
  }); managers.push(agents);
  const identity = { id: "session:role-test", name: "main", kind: "main" as const, sessionId: "role-test" };
  const makeOwner = () => new ActorManager("role-test", identity, new MeshStore(path.join(root, "mesh"), 65536, 100),
    { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20 }, agents, () => {}, { actorRoot: path.join(root, "actors"), persistent: true });
  let owner = makeOwner(); actors.push(owner);
  const actor = await owner.create({ name, instructions: "Run exactly one child pass and report the outcome.", responseMode: "text",
    model: "role-probe/requested", thinking: "max", tools: nativeTools, extensions: true });
  if (scenario === "recover") {
    await owner.close();
    owner = makeOwner(); actors.push(owner);
  }
  const response = await owner.ask(actor.id, "Exercise native role child binding");
  const result = await agents.wait(response.runId!);
  const log = fs.readFileSync(path.join(root, "runs", result.id, "events.jsonl"), "utf8");
  expect(result, `${result.error}\n${result.stderr}\n${log}`).toMatchObject({ status: "completed", model: "role-probe/requested", thinking: "max" });
  const admitted = log.trim().split("\n").map(line => JSON.parse(line)).find(event => event.type === "fabric_native_role_admitted");
  expect(admitted, "owner activation must install the native binding before inference").toMatchObject({
    model: "role-probe/requested", thinking: "max", participant: { id: actor.id, kind: "actor" } });
  expect([...(admitted.tools as string[])].sort()).toEqual([...nativeTools].sort());
  const reported = JSON.parse(result.text) as { results: Array<{ child?: AgentRunRecord }> };
  const children = reported.results.flatMap(entry => {
    if (!entry.child) return [];
    const record = entry.child;
    const data = record.text ? JSON.parse(record.text) as { attestation?: Record<string, unknown>; beforeFirstInference?: boolean; actorId?: string | null; results?: unknown[] } : {};
    return [{ record, data }];
  });
  if (process.env.NATIVE_ROLE_TEST_EVIDENCE) fs.appendFileSync(process.env.NATIVE_ROLE_TEST_EVIDENCE, JSON.stringify({ role, scenario,
    actor: { id: actor.id, runId: result.id, status: result.status }, children: children.map(({ record, data }) => ({ id: record.id,
      status: record.status, model: record.model, thinking: record.thinking, admittedModel: record.admittedModel, admittedThinking: record.admittedThinking,
      inferenceStarted: record.inferenceStarted, toolCalls: record.toolCalls, error: record.error,
      attestation: data.attestation, beforeFirstInference: data.beforeFirstInference })) }) + "\n");
  return { actor, result, log, root, children, agents };
};

/** Built worker + actual native Pi actor -> agents.spawn -> built worker + actual native Pi child. */
describe.skipIf(!fs.existsSync(workerPath))("real native role actor child admission (offline)", () => {
  it.each(roles)("%s recovered factory actor retains canonical admission and bound child tools", async role => {
    const name = `pi-fabric-shards8-2-${role === "review-agent" ? "review" : "security"}-astra`;
    const { children, log } = await runActor(role, "recover", name);
    expect(log).toContain('"type":"fabric_native_role_admitted"');
    expect(children).toHaveLength(1);
    expect(children[0]!.record).toMatchObject({ status: "completed", admittedModel: "role-probe/requested", admittedThinking: "max" });
    expect(children[0]!.data.attestation).toMatchObject({ model: "role-probe/requested", thinking: "max" });
  }, 45000);
  it.each(roleCases.flatMap(({ role, name }) => ["handoff-model", "handoff-effort", "handoff-tools", "handoff-extensions", "handoff-matching"].map(scenario => ({ role, name, scenario }))))("$name real bound actor refuses $scenario before continuation inference/tools", async ({ role, name, scenario }) => {
    const { result, agents } = await runActor(role, scenario, name);
    expect(result.text).toContain("NATIVE_ROLE_BINDING_MISMATCH");
    expect(result.text).toContain("trajectory handoff is unavailable");
    expect(agents.list()).toHaveLength(1); // Only parent; no continuation run/worker/tools.
    expect(result.text).not.toContain("native-role-bash-delivered");
  }, 45000);
  it("refuses an opaque launcher without native hook readiness before sending even the admission prompt", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-role-no-hook-")); roots.push(root);
    vi.stubEnv("FAKE_MODEL_SCENARIO", undefined);
    const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 5000 }, {
      workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-model.mjs"), runRoot: path.join(root, "runs"),
      fullCodeMode: false, fabricExtensionPath: path.resolve("dist/index.js"),
    }); managers.push(agents);
    const result = await agents.run({ task: "must never dispatch", extensions: true, nativeRoleBinding: snapshotNativeRoleBinding({
      role: "review-agent", model: "cliproxyapi/gpt-6.1-sol", thinking: "max", tools: nativeTools }) });
    expect(result).toMatchObject({ status: "failed", toolCalls: 0 });
    expect(result.error).toContain("native role hook did not acknowledge readiness");
    expect(result.inferenceStarted).not.toBe(true);
    const events = fs.readFileSync(path.join(root, "runs", result.id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events.filter(event => event.type === "fake_received").some(event => event.frame.type === "prompt")).toBe(false);
  });
  it.each(roleCases.flatMap(({ role, name }) => ["inherit", "explicit"].map(scenario => ({ role, name, scenario }))))("$name $scenario delivers bash and canonical self pair without actor identity", async ({ role, name, scenario }) => {
    const { actor, result, log, children } = await runActor(role, scenario, name);
    expect(result.text).toContain("native-role-bash-delivered");
    expect(result.text).toContain("role-probe/requested"); expect(result.text).toContain("max");
    expect(children).toHaveLength(1);
    const child = children[0]!;
    const attestation = child.data.attestation;
    expect(attestation).toMatchObject({ model: "role-probe/requested", thinking: "max",
      participant: { id: child.record.id, kind: "agent", model: "role-probe/requested", thinking: "max" } });
    expect([...(attestation!.tools as string[])].sort()).toEqual([...nativeTools].sort());
    expect(child.record).toMatchObject({ status: "completed", model: "role-probe/requested", thinking: "max",
      admittedModel: "role-probe/requested", admittedThinking: "max", spawner: { id: actor.id, kind: "actor" } });
    expect(child.record.actorId).toBeUndefined();
    expect(child.record.id).not.toBe(actor.id);
    expect(child.data.beforeFirstInference).toBe(true);
    expect(child.data.actorId).toBeNull();
    expect(child.data.results).toContainEqual(expect.objectContaining({ self: expect.objectContaining({ id: child.record.id,
      kind: "agent", model: "role-probe/requested", thinking: "max" }), spawner: expect.objectContaining({ id: actor.id, kind: "actor" }) }));
    expect(result.text).toContain(actor.id); // Immediate spawner, not child identity.
    expect(log).toContain('"type":"fabric_native_role_admitted"');
    expect(log).not.toContain("NATIVE_ROLE_BINDING_MISMATCH");
  }, 45000);
  it.each(roleCases.flatMap(({ role, name }) => ["wrong-model", "wrong-effort", "wrong-tools", "restrictive-ceiling", "prepared-model", "effort-clamp", "missing-bash", "wrong-metadata"].map(scenario => ({ role, name, scenario }))))("$name refuses $scenario rather than running a weakened pass", async ({ role, name, scenario }) => {
    const { result, children } = await runActor(role, scenario, name);
    expect(result.text).toContain("NATIVE_ROLE_BINDING_MISMATCH");
    expect(result.text).not.toContain("native-role-bash-delivered");
    expect(children).toHaveLength(["effort-clamp", "missing-bash", "wrong-metadata"].includes(scenario) ? 1 : 0);
    for (const child of children) {
      expect(child.record).toMatchObject({ status: "failed", toolCalls: 0 });
      expect(child.record.inferenceStarted).not.toBe(true);
      expect(child.record.turns).toBe(0);
    }
  }, 45000);
});
