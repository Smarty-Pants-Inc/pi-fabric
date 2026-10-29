// Supplemental real-SDK contract probe, NOT an alternate N7 Main/runtime.
// It creates no persisted session and never registers a provider or model.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { assertApprovedNativeSdk, isNativeSdkUnconfigured, NATIVE_SDK_RELEASE } from "../helpers/native-sdk-no-model.ts";

const pins = JSON.parse(fs.readFileSync(process.env.FABRIC_NATIVE_OWNER_PINS, "utf8"));
const { sdkDirectory } = pins;
assertApprovedNativeSdk(sdkDirectory);
const sdk = await import(pathToFileURL(path.join(sdkDirectory, "index.js")).href);
const { Agent } = await import(pathToFileURL(path.join(sdkDirectory, "../../pi-agent-core/dist/agent.js")).href);
const sentinel = new Agent().state.model; // SDK-owned default, NOT an invented model.
assert.equal(isNativeSdkUnconfigured(sentinel, sdkDirectory), true);
assert.equal(isNativeSdkUnconfigured(undefined, sdkDirectory), false);
assert.equal(isNativeSdkUnconfigured(sentinel, sdkDirectory.replace(NATIVE_SDK_RELEASE, "unapproved-release")), false);
const clone = () => structuredClone(sentinel);
const mutations = [];
const reject = (label, change) => {
  const value = clone(); change(value);
  assert.equal(isNativeSdkUnconfigured(value, sdkDirectory), false, label);
  mutations.push(label);
};
for (const key of Object.keys(sentinel)) {
  reject(`field:${key}`, value => {
    const old = value[key];
    value[key] = typeof old === "string" ? `${old}changed` : typeof old === "boolean" ? !old : typeof old === "number" ? 1 : null;
  });
  reject(`missing:${key}`, value => { delete value[key]; });
}
for (const key of Object.keys(sentinel.cost)) {
  reject(`field:cost.${key}`, value => { value.cost[key] = 1; });
  reject(`missing:cost.${key}`, value => { delete value.cost[key]; });
  reject(`negative-zero:cost.${key}`, value => { value.cost[key] = -0; });
}
for (const key of ["contextWindow", "maxTokens"]) reject(`negative-zero:${key}`, value => { value[key] = -0; });
reject("field:input.length", value => { value.input.length = 1; });
reject("field:input.0", value => { value.input.push("text"); });
for (const location of ["root", "cost", "input"]) {
  const at = value => location === "root" ? value : value[location];
  reject(`extra:${location}`, value => { at(value).extra = 0; });
  reject(`hidden:${location}`, value => { Object.defineProperty(at(value), "extra", { value: 0 }); });
  reject(`symbol:${location}`, value => { at(value)[Symbol("extra")] = 0; });
  reject(`prototype:${location}`, value => { Object.setPrototypeOf(at(value), null); });
}
let getterCalls = 0;
reject("accessor:id", value => { Object.defineProperty(value, "id", { enumerable: true, configurable: true, get() { getterCalls++; return "unknown"; } }); });
assert.equal(getterCalls, 0);
let proxyCalls = 0;
assert.equal(isNativeSdkUnconfigured(new Proxy(sentinel, { getPrototypeOf() { proxyCalls++; return Object.prototype; } }), sdkDirectory), false);
assert.equal(proxyCalls, 0);

const cwd = process.cwd(), agentDir = process.env.PI_CODING_AGENT_DIR;
const settingsManager = sdk.SettingsManager.create(cwd, agentDir, { projectTrusted: false });
assert.deepEqual(settingsManager.getGlobalSettings().modelPolicy, { allow: [] });
const modelRuntime = await sdk.ModelRuntime.create({ settingsManager, authPath: path.join(agentDir, "auth.json"),
  modelsPath: null, refreshOnCreate: false, allowModelNetwork: false });
const catalogModel = modelRuntime.getModels(undefined, { includeDisallowed: true })[0];
assert.ok(catalogModel, "Actual pinned built-in catalog must contain a real registered model");
assert.ok(modelRuntime.getProvider(catalogModel.provider));
assert.equal(isNativeSdkUnconfigured(catalogModel, sdkDirectory), false);
assert.equal(modelRuntime.isModelAllowed(catalogModel), false);
assert.deepEqual(await modelRuntime.listCredentials(), []);

const counts = { beforeAgentStart: 0, agentStart: 0, providerCalls: 0, beforeProviderRequest: 0, providerStream: 0, toolCalls: 0, toolExecution: 0 };
// Instrument the REAL registered provider instances as a last-resort safety tripwire.
// No replacement model/provider/runtime and no fabricated response: reaching a real
// dispatch is counted and throws, so a regression cannot accidentally use the network.
const restores = [];
for (const provider of modelRuntime.getProviders()) {
  for (const key of ["stream", "streamSimple"]) {
    if (typeof provider[key] !== "function") continue;
    const descriptor = Object.getOwnPropertyDescriptor(provider, key);
    Object.defineProperty(provider, key, { configurable: true, writable: true, value() {
      counts.providerCalls++; throw new Error("FORBIDDEN actual provider dispatch in no-model probe");
    } });
    restores.push(() => { if (descriptor) Object.defineProperty(provider, key, descriptor); else delete provider[key]; });
  }
}
assert.ok(restores.length > 0, "Real registered provider dispatch instrumentation is required");
let observerBound = false, sessionStarts = 0;
const resourceLoader = new sdk.DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true,
  noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
  extensionFactories: [pi => {
    observerBound = true;
    pi.on("session_start", () => { sessionStarts++; });
    pi.on("before_agent_start", () => { counts.beforeAgentStart++; });
    pi.on("before_provider_request", () => { counts.beforeProviderRequest++; });
    pi.on("provider_stream_event", () => { counts.providerStream++; });
    pi.on("tool_call", () => { counts.toolCalls++; });
  }] });
await resourceLoader.reload();
const { session } = await sdk.createAgentSession({ cwd, agentDir, settingsManager, modelRuntime, resourceLoader,
  sessionManager: sdk.SessionManager.inMemory(cwd), tools: [] });
let turnRefusal;
try {
  await session.bindExtensions({ mode: "rpc" });
  assert.equal(observerBound, true); assert.equal(sessionStarts, 1);
  assert.equal(isNativeSdkUnconfigured(session.model, sdkDirectory), true);
  const beforeMessages = structuredClone(session.messages);
  assert.equal(beforeMessages.filter(message => message.role !== "system").length, 0);
  session.subscribe(event => {
    if (event.type === "agent_start") counts.agentStart++;
    if (event.type === "tool_execution_start") counts.toolExecution++;
  });
  // Actual would-be turn through the genuine AgentSession input/preflight path.
  // No input handler consumes this one: the SDK must refuse before turn events.
  await assert.rejects(session.prompt("N7_SENTINEL_MUST_NOT_START_A_TURN"), error => {
    turnRefusal = error.message;
    return error.message.startsWith("No API key found for the selected model.");
  });
  assert.deepEqual(session.messages, beforeMessages);
  assert.equal(session.inputAdmissionCount, 0);
  assert.equal(session.isIdle, true);
  assert.deepEqual(session.getActiveToolNames(), []);
  // Supplemental supported deny-all boundary: policy rejects BOTH sentinel and
  // actual catalog model synchronously, before auth/provider stream dispatch.
  for (const model of [session.model, catalogModel]) {
    assert.throws(() => modelRuntime.streamSimple(model, { messages: [] }), error =>
      error instanceof sdk.ModelPolicyError && error.message === `model ${model.provider}/${model.id} is not allowed by modelPolicy.allow`);
  }
  assert.deepEqual(counts, { beforeAgentStart: 0, agentStart: 0, providerCalls: 0, beforeProviderRequest: 0, providerStream: 0, toolCalls: 0, toolExecution: 0 });
  assert.deepEqual(await modelRuntime.listCredentials(), []);
  process.stdout.write(JSON.stringify({ sdkRelease: NATIVE_SDK_RELEASE, sentinelAccepted: true,
    catalogModelRejected: { provider: catalogModel.provider, id: catalogModel.id }, mutations, turnRefusal,
    observerBound, sessionStarts, instrumentedDispatchMethods: restores.length, counts }) + "\n");
} finally {
  await session.abort(); session.dispose();
  for (const restore of restores.reverse()) restore();
}
