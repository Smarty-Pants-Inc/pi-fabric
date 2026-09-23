import assert from "node:assert/strict";
import { ActionRegistry } from "../dist/core/action-registry.js";
import { AgentService, createAgentServiceClient, createAgentServiceHandler, createAgentsProvider } from "../dist/agents.js";
import { DEFAULT_JEV_CONFIG, JevProvider, JevObservationHost, createJevAuthProvider } from "../dist/jev.js";

const registry = new ActionRegistry();
// Minimal runtime fixture: no environment-key resolution or external network.
const config = {
  fullCodeMode: true,
  jev: { ...DEFAULT_JEV_CONFIG, credentialCommand: [] },
  executor: { memoryLimitBytes: 64 * 1024 * 1024, maxNestedResultChars: 32768 },
  approvals: { read: "allow", execute: "allow", network: "deny", write: "deny", agent: "allow" },
};
const context = {
  cwd: process.cwd(), signal: undefined, parentToolCallId: "jev-dist-smoke", nestedToolCallId: "jev-dist-smoke",
  extensionContext: { cwd: process.cwd(), hasUI: false, sessionManager: { getSessionId: () => "compiled-observer" } }, update() {},
};
const fixtureAction = { name: "items", description: "Read synthetic items", risk: "read", inputSchema: { type: "object", properties: {}, additionalProperties: false } };
let fixtureCalls = 0;
let fixtureClosed = false;
const fixture = {
  name: "fixture", description: "Connector-neutral offline capability fixture",
  async list() { return [fixtureAction]; },
  async describe(name) { return name === fixtureAction.name ? fixtureAction : undefined; },
  async invoke(name, args) {
    assert.equal(fixtureClosed, false);
    assert.equal(name, "items");
    assert.deepEqual(args, {});
    fixtureCalls++;
    return { items: [{ id: "fixture" }] };
  },
  async close() { fixtureClosed = true; },
};
const advice = [];
const observationHost = new JevObservationHost("compiled-observer", message => advice.push(message));
const provider = new JevProvider({ registry, config, observationHost });
const agentService = new AgentService({rootId: "compiled-smoke", port: {execute: async () => ({status: "completed", text: "fixture"})}});
registry.register(provider);
registry.register(fixture);
try {
  const foreground = await provider.invoke("run", { input: null, program: {
    name: "compiled-capability-probe", inputSchema: {}, outputSchema: { type: "integer", minimum: 1 },
    requires: ["fixture.items"],
    code: `const result = await tools.call({ref: "fixture.items"}) as {items:unknown[]};
      return result.items.length;`,
  } }, context);
  assert.equal(foreground.state, "completed", foreground.error);
  assert.equal(foreground.result, 1);
  assert.equal(fixtureCalls, 1);
  const background = await provider.invoke("spawn", { input: null, program: {
    name: "compiled-loop-probe", inputSchema: {}, outputSchema: {}, requires: [],
    code: "while (true) await program.sleep(10);",
  } }, context);
  assert.equal(background.state, "running");
  const stopped = await provider.invoke("stop", {id: background.id}, context);
  assert.equal(stopped.state, "cancelled");
  assert.equal((await provider.invoke("wait", {id: background.id}, context)).state, "cancelled");
  assert.equal((await provider.invoke("join", {id: background.id}, context)).state, "cancelled");
  const agents = createAgentServiceClient(createAgentServiceHandler(agentService, "compiled-smoke"));
  const child = await agents.spawn({task: "offline fixture"});
  const waited = await agents.wait(child.id);
  assert.equal(waited.status, "completed");
  assert.deepEqual(await agents.join(child.id), waited);
  assert.deepEqual(await agentService.join("compiled-smoke", child.id), waited);
  assert.deepEqual(await createAgentsProvider(agents).invoke("join", {id: child.id}, context), waited);
  const observing = await provider.invoke("spawn", {
    input: null, observe: {events: ["turn_end"], delivery: "steer"},
    program: {name: "compiled-turn-advisor", inputSchema: {}, outputSchema: {}, requires: ["jev.advise"],
      code: "const event = await program.nextEvent(); return await program.advise({eventId:event.id,message:'Check the fixture'});"},
  }, context);
  observationHost.observe("turn_end", {turnIndex: 1}, {sessionId: "compiled-observer"});
  const observed = await provider.invoke("wait", {id: observing.id}, context);
  assert.equal(observed.state, "completed", observed.error);
  assert.deepEqual(observed.result, {delivered: true});
  assert.equal(advice.length, 1);
  assert.equal(advice[0].runId, observing.id);
  assert.equal(advice[0].triggerTurn, false);
  assert.equal(observationHost.size, 0);
  const auth = createJevAuthProvider();
  assert.equal(auth.id, "jev");
  assert.equal(auth.getModels().length, 0);
  assert.equal(typeof auth.auth.apiKey.login, "function");
  const originalFetch = globalThis.fetch;
  const probability = 0.5;
  let classifications = 0;
  try {
    globalThis.fetch = async (url, options) => {
      assert.equal(url, "https://api.typesafe.ai/v1/systemone");
      const body = JSON.parse(options.body);
      assert.equal(body.model, "jev-latest");
      assert.equal(body.questions.safe_to_auto_approve.type, "noul");
      assert.equal(body.questions.touches_secrets.type, "noul");
      assert.equal(body.questions.destructive.type, "noul");
      assert.equal(body.questions.targets_agent_artifacts.type, "noul");
      assert.equal(body.state.action.ref, "jev.spawn");
      classifications++;
      return Response.json({ model: "jev-latest", answers: { safe_to_auto_approve: { type: "noul", noul: probability }, touches_secrets: { type: "noul", noul: 0 }, destructive: { type: "noul", noul: 0 }, targets_agent_artifacts: { type: "noul", noul: 0 } }, usage: { input_tokens: 20, output_tokens: 3 } });
    };
    config.approvals.read = "auto";
    config.approvals.model = "pi-fabric/typesafe/jev-latest";
    context.extensionContext.modelRegistry = { getApiKeyForProvider: async () => "compiled-fixture-key" };
    context.extensionContext.sessionManager.getBranch = () => [{ type: "message", message: { role: "user", content: "Observe completed Main turns for local test verification" } }];
    const request = { input: null, observe: { events: ["turn_end"] }, program: {
      name: "compiled-auto-approval", inputSchema: {}, outputSchema: {}, requires: [], code: "return null;",
    } };
    context.extensionContext.hasUI = true;
    context.extensionContext.mode = "rpc";
    context.extensionContext.ui = { notify() {}, async select(title) { throw new Error(`Unexpected approval prompt: ${title}`); } };
    const approved = await provider.invoke("spawn", request, context);
    assert.equal((await provider.invoke("wait", { id: approved.id }, context)).state, "completed");
    assert.equal(config.jev.autoApprovalThreshold, 0.5);
    config.jev.autoApprovalThreshold = 0.75;
    context.extensionContext.hasUI = false;
    await assert.rejects(provider.invoke("spawn", request, context), /no interactive UI/);
    assert.equal(classifications, 2);
    assert.equal(observationHost.size, 0);
  } finally { globalThis.fetch = originalFetch; }
  console.log("Compiled Jev smoke passed: auth-only provider, typed foreground capability dispatch, background stop/wait, agent/Jev join aliases, event-driven Main advice, and Jev auto-mode approval; no external calls.");
} finally {
  observationHost.close();
  await provider.close();
  await fixture.close();
  assert.equal(fixtureClosed, true);
  await agentService.close();
}
