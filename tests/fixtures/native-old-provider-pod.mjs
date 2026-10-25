// TEST ONLY: separate-process ACTUAL f339 public agents.ask + real Pi SDK context.
// No handwritten control envelope, current-provider fallback, fake Main/context,
// provider/model registration, source/global API patch, or model turn.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createJiti } from "jiti";
import { assertApprovedNativeSdk, isNativeSdkUnconfigured } from "../helpers/native-sdk-no-model.ts";
import { nativeOwnerSession, takeNativeOwnerSessionFence } from "./native-owner-session.mjs";

process.umask(0o077);
const pinsFile = process.env.FABRIC_NATIVE_OLD_POD_PINS;
if (!pinsFile || !path.isAbsolute(pinsFile)) throw new Error("N8 BLOCKED: explicit old-pod pins required");
const root = fs.realpathSync(path.dirname(pinsFile));
const pins = JSON.parse(fs.readFileSync(pinsFile, "utf8"));
const inside = file => {
  const relative = path.relative(root, file);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
};
for (const file of [pins.cwd, pins.agentDir, pins.sessionFile, pins.sessionPin, pins.sourceDirectory]) {
  assert.ok(typeof file === "string" && path.isAbsolute(file) && inside(file), "Old-pod state escaped owned root");
}
assert.equal(fs.realpathSync(process.cwd()), pins.cwd);
assert.equal(fs.realpathSync(process.execPath), pins.nodeBinary);
assert.match(pins.actorId, /^[0-9a-f]{32}$/);
const guard = process[Symbol.for("pi-fabric.test-deletion-guard")];
assert.ok(guard?.root && path.isAbsolute(guard.root), "N8 BLOCKED: inherited deletion guard required");
const outsideBefore = guard.count;
const trace = value => fs.appendFileSync(path.join(root, "producer.jsonl"), JSON.stringify({ pid: process.pid, at: Date.now(), ...value }) + "\n", { mode: 0o600 });
process.once("exit", code => trace({ event: "native-exit", code, outsideDeletionCount: guard.count - outsideBefore }));

// FIRST native session touch is behind this actual process's kernel lifetime fence.
// fd stays open until process death, never an early session-disposal unlock.
const fence = takeNativeOwnerSessionFence(pins.sessionFile);
const pin = await nativeOwnerSession("provision", pins.sdkDirectory, pins.sessionFile, pins.cwd, pins.sessionPin, fence);
assertApprovedNativeSdk(pins.sdkDirectory);
const sdk = await import(pathToFileURL(path.join(pins.sdkDirectory, "index.js")).href);
const jiti = createJiti(import.meta.url, { tryNative: false, fsCache: path.join(root, "jiti-cache") });
// This checked-in materializer verifies the exact old commit, source hashes and
// byte-identical lock before sharing ONLY third-party node_modules. Its dynamic
// old-source imports use jiti's .js -> .ts resolver, not a current source alias.
const { loadFrozenF3393715, FROZEN_F3393715_COMMIT } = await jiti.import("../helpers/frozen-f3393715.ts");
const frozen = await loadFrozenF3393715(pins.sourceDirectory, pins.repoRoot);
assert.equal(frozen.provenance.commit, FROZEN_F3393715_COMMIT);
// The signed account inventory MUST also reach the genuine old normalizer and
// verifier. Do not rely on a mutable global two-machine default or ambient env.
assert.equal(process.env.PI_FABRIC_MESH_NATS, undefined);
assert.deepEqual(pins.policyEnv, {
  PI_FABRIC_NATS_CUSTODY_STREAM: pins.nats.custodyStream,
  PI_FABRIC_NATS_LIVENESS_STREAM: pins.nats.livenessStream,
  PI_FABRIC_NATS_MACHINES: pins.nats.pathBMachines.join(","),
});
for (const [key, value] of Object.entries(pins.policyEnv)) assert.equal(process.env[key], value);
const config = frozen.normalizeFabricConfig({ fullCodeMode: false,
  mesh: { enabled: true, transport: "nats", hostTransport: "nats", announce: true, actorScope: "session",
    stateBackend: "file", root: path.join(root, "mesh"), nats: pins.nats },
  agents: { enabled: false, budgetUsd: 0, notifyOnComplete: false, sessionExport: false },
  residency: { enabled: false }, records: { enabled: false }, mcp: { enabled: false },
  memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
});
// Do NOT shorten/re-pin the frozen provider's actual 24h default. Each test
// invocation below has an observation/cancellation bound, NOT a #344 client.
assert.equal(config.agents.timeoutMs, 24 * 60 * 60 * 1000);
assert.equal(config.mesh.nats.machine, pins.nats.machine);
assert.equal(config.mesh.nats.custodyStream, pins.nats.custodyStream);
assert.equal(config.mesh.nats.livenessStream, pins.nats.livenessStream);
assert.deepEqual(config.mesh.nats.pathBMachines, pins.nats.pathBMachines);
const settingsManager = sdk.SettingsManager.create(pins.cwd, pins.agentDir, { projectTrusted: true });
assert.deepEqual(settingsManager.getGlobalSettings().modelPolicy, { allow: [] });
const modelRuntime = await sdk.ModelRuntime.create({ settingsManager, authPath: path.join(pins.agentDir, "auth.json"),
  modelsPath: null, refreshOnCreate: false, allowModelNetwork: false });
assert.deepEqual(await modelRuntime.listCredentials(), []);
const counts = { beforeAgentStart: 0, agentStart: 0, providerRequest: 0, providerStream: 0, providerResponse: 0, toolCall: 0, toolExecution: 0 };
let runtime, context, initializationError;
const resourceLoader = new sdk.DefaultResourceLoader({ cwd: pins.cwd, agentDir: pins.agentDir, settingsManager,
  noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
  extensionFactories: [pi => {
    // Real SDK ExtensionAPI + real SDK context, never makeMain()/a cast object.
    runtime = new frozen.FabricRuntimeState(pi, new frozen.CapturedToolCatalog(), { paths: {
      extension: path.join(frozen.sourceRoot, "src/index.ts"), worker: path.join(frozen.sourceRoot, "src/worker/entry.ts"),
      residentHost: path.join(frozen.sourceRoot, "src/residency/pi-entry.ts"), skills: path.join(frozen.sourceRoot, "config"),
    } });
    pi.on("session_start", async (_event, ctx) => {
      try {
        assert.equal(ctx.isProjectTrusted(), true);
        assert.equal(ctx.sessionManager.getSessionId(), pin.sessionId);
        assert.equal(ctx.sessionManager.getSessionFile(), pin.sessionFile);
        assert.ok(isNativeSdkUnconfigured(ctx.model, pins.sdkDirectory));
        context = ctx;
        await runtime.initialize(ctx, config);
      } catch (error) { initializationError = error; }
    });
    pi.on("input", () => ({ action: "handled" }));
    for (const [event, key] of [["before_agent_start", "beforeAgentStart"], ["agent_start", "agentStart"],
      ["before_provider_request", "providerRequest"], ["provider_stream_event", "providerStream"],
      ["after_provider_response", "providerResponse"], ["tool_call", "toolCall"]]) pi.on(event, () => { counts[key]++; });
  }] });
await resourceLoader.reload();
assert.deepEqual(resourceLoader.getExtensions().errors, []);
const { session } = await sdk.createAgentSession({ cwd: pins.cwd, agentDir: pins.agentDir, settingsManager, modelRuntime, resourceLoader,
  sessionManager: sdk.SessionManager.open(pin.sessionFile, path.dirname(pin.sessionFile), pins.cwd), tools: [] });
session.subscribe(event => { if (event.type === "tool_execution_start") counts.toolExecution++; });
let stopped = false;
const state = async () => {
  assert.equal(runtime.mainAgentInfo(context).id, pin.rootId);
  assert.ok(isNativeSdkUnconfigured(session.model, pins.sdkDirectory));
  assert.deepEqual(await modelRuntime.listCredentials(), []);
  assert.equal(session.messages.filter(message => message.role !== "system").length, 0);
  assert.ok(Object.values(counts).every(value => value === 0), "N8 must not run a model/provider/tool turn");
  assert.equal(guard.count, outsideBefore, "Old producer attempted an outside-root deletion");
  return { pin, counts: { ...counts }, timeoutMs: config.agents.timeoutMs, sdkUnconfigured: true,
    nats: { machine: config.mesh.nats.machine, custodyStream: config.mesh.nats.custodyStream,
      livenessStream: config.mesh.nats.livenessStream, pathBMachines: config.mesh.nats.pathBMachines },
    provenance: frozen.provenance, entry: 'actual frozen registry.invoke("agents.ask") with actual SDK context' };
};
const stop = async () => {
  if (stopped) return;
  stopped = true;
  await runtime?.shutdown("exit");
  await session.abort();
  session.dispose();
};
const send = value => new Promise((resolve, reject) => {
  if (!process.send) { reject(new Error("N8 requires an owned IPC subprocess")); return; }
  process.send(value, error => error ? reject(error) : resolve());
});
const discover = (id, milliseconds = 60_000) => new Promise((resolve, reject) => {
  let watcher, timer, finished = false;
  const finish = (error, value) => {
    if (finished) return;
    finished = true; clearTimeout(timer); watcher?.close();
    if (error) reject(error); else resolve(value);
  };
  const check = () => {
    try {
      const row = runtime.participantInfos({ fresh: true }).find(value => value.id === id && !value.stale);
      if (row) finish(undefined, row);
    } catch (error) { finish(error); }
  };
  // Normal source: the real NATS projection's committed files. No discovery
  // polling/retry loop and no injected participant. Deadline only bounds failure.
  watcher = fs.watch(root, { recursive: true, persistent: false }, check);
  watcher.on("error", error => finish(error));
  timer = setTimeout(() => finish(new Error("N8 BLOCKED: frozen public actor discovery deadline")), milliseconds);
  check();
});
try {
  await session.bindExtensions({ mode: "rpc" });
  if (initializationError) throw initializationError;
  assert.ok(runtime?.initialized && context, "N8 BLOCKED: actual SDK context/runtime not initialized");
  trace({ event: "ready", ...(await state()) });
  await send({ event: "ready", ...(await state()) });
} catch (error) { await stop(); throw error; }
let queue = Promise.resolve();
process.on("message", input => {
  queue = queue.then(async () => {
    if (!input || typeof input.id !== "string" || stopped) return;
    try {
      let result;
      if (input.action === "discover") result = await discover(input.actorId);
      else if (input.action === "state") result = await state();
      else if (input.action === "ask") {
        // PUBLIC provider action. The actual old provider creates its own UUID,
        // routes via its old router/control and returns its actual Fabric reply.
        const signal = AbortSignal.timeout(40_000);
        const publicArgs = { id: input.actorId, message: input.message, data: input.data };
        result = await runtime.registry.invoke("agents.ask", publicArgs, {
          cwd: pins.cwd, signal, parentToolCallId: `n8-old-public-${input.id}`, nestedToolCallId: "agents.ask",
          extensionContext: context, update() {},
          // Standalone-host approval port: the parent's grant permits ONLY this
          // exact synthetic actor/action/input. Not an unconditional authorizer,
          // model/tool grant, fabricated SDK context or bridge receipt authority.
          approve: async (action, approvedArgs) => {
            assert.equal(action.ref, "agents.ask");
            assert.equal(approvedArgs.id, pins.actorId);
            assert.deepEqual(approvedArgs, publicArgs);
          }, audits: [], maxResultChars: 128_000,
        });
      } else if (input.action === "stop") {
        result = await state(); await stop();
      } else throw new Error("Unknown bounded old-pod action");
      trace({ event: "result", id: input.id, action: input.action, result });
      await send({ id: input.id, ok: true, result });
      if (input.action === "stop") process.disconnect();
    } catch (error) {
      trace({ event: "refusal", id: input.id, action: input.action, message: String(error?.message ?? error) });
      await send({ id: input.id, ok: false, error: String(error?.message ?? error) });
    }
  }).catch(error => { trace({ event: "fatal", message: String(error?.message ?? error) }); process.exitCode = 1; process.disconnect(); });
});
process.on("disconnect", () => { void queue.then(stop).catch(error => { trace({ event: "shutdown-error", message: String(error?.message ?? error) }); process.exitCode = 1; }); });
