// Test-only observation and deny-input guard. No provider/model/session/runtime substitutes.
import fs from "node:fs";
import { isNativeSdkUnconfigured } from "../helpers/native-sdk-no-model.ts";
import { registerHooks, syncBuiltinESMExports } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";
const pinsFile = process.env.FABRIC_NATIVE_OWNER_PINS;
if (!pinsFile) throw new Error("Native owner observer pins required");
const pins = JSON.parse(fs.readFileSync(pinsFile, "utf8"));
const role = process.env.PI_FABRIC_RESIDENT_CONFIG ? "resident" : "main";
const record = value => fs.appendFileSync(pins.observations, JSON.stringify({ role, pid: process.pid, ...value }) + "\n", { mode: 0o600 });
// TEST ONLY: observes the unchanged SDK call, then delegates ALL original
// arguments to the real fs implementation. The native short-write interposer
// below it, not a fixture append or SDK replacement, stops the actual write.
if (new URL(import.meta.url).search === "?startup-io") {
  const target = process.env.FABRIC_NATIVE_STARTUP_SESSION;
  const receipt = process.env.FABRIC_NATIVE_STARTUP_RECEIPT;
  if (!target || !receipt || target !== pins.sessionFile) throw new Error("Actual startup I/O pins required");
  const proof = () => {
    const named = fs.lstatSync(`${target}.main.lock`);
    const matches = fs.readdirSync("/proc/self/fd").flatMap(name => {
      try {
        const held = fs.fstatSync(Number(name)), info = fs.readFileSync(`/proc/self/fdinfo/${name}`, "utf8");
        return held.dev === named.dev && held.ino === named.ino && /^lock:\s+\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s/m.test(info)
          ? [{ fd: Number(name), dev: held.dev, ino: held.ino, uid: held.uid, mode: held.mode, nlink: held.nlink, info }] : [];
      } catch (error) { if (error.code === "ENOENT" || error.code === "EBADF") return []; throw error; }
    });
    if (matches.length !== 1 || !named.isFile() || named.nlink !== 1 || (named.mode & 0o7777) !== 0o600 || named.uid !== process.geteuid()) {
      throw new Error("Startup barrier has no actual self-open-description WRITE FLOCK");
    }
    return matches[0];
  };
  const append = fs.appendFileSync;
  fs.appendFileSync = function(file, data, ...rest) {
    if (file === target) {
      fs.writeFileSync(`${receipt}.sdk-call.json`, JSON.stringify({ pid: process.pid, event: "actual-sdk-appendFileSync-invocation",
        stack: new Error().stack, dataBase64: Buffer.from(data).toString("base64"), proof: proof() }) + "\n", { mode: 0o600, flag: "wx" });
    }
    return append.call(this, file, data, ...rest);
  };
  syncBuiltinESMExports(); // SDK captures named fs imports; observe those too, BEFORE SDK evaluation.
  if (process.env.FABRIC_NATIVE_STARTUP_PHASE === "pre-ready") {
    const prefix = pathToFileURL(`${pins.sdkDirectory}${path.sep}`).href;
    let stopped = false;
    registerHooks({ load(url, context, nextLoad) {
      if (!stopped && url.startsWith(prefix)) {
        stopped = true;
        fs.writeFileSync(receipt, JSON.stringify({ pid: process.pid, event: "before-first-sdk-load", url, proof: proof() }) + "\n", { mode: 0o600, flag: "wx" });
        process.kill(process.pid, "SIGSTOP"); // Actual owned Main; parent observes T before SIGKILL.
      }
      return nextLoad(url, context);
    } });
  }
}

export default function(pi) {
  pi.on("input", () => { record({ event: "input-handled" }); return { action: "handled" }; });
  pi.on("before_agent_start", () => { record({ event: "FORBIDDEN-agent-preflight" }); throw new Error("Native fixture may not start a model turn"); });
  pi.on("agent_start", () => { record({ event: "FORBIDDEN-agent-start" }); });
  pi.on("before_provider_request", () => { record({ event: "FORBIDDEN-provider-request" }); throw new Error("Native fixture has no model authority"); });
  pi.on("provider_stream_event", () => { record({ event: "FORBIDDEN-provider-stream" }); });
  pi.on("after_provider_response", () => { record({ event: "FORBIDDEN-provider-response" }); });
  pi.on("tool_call", () => { record({ event: "FORBIDDEN-tool-call" }); return { block: true, reason: "Native owner fixture has no tool turns" }; });
  pi.on("session_start", (_event, ctx) => {
    const unconfigured = isNativeSdkUnconfigured(ctx.model, pins.sdkDirectory);
    const selectedModel = !unconfigured;
    record({ event: "session-start", selectedModel, unconfigured, projectTrusted: ctx.isProjectTrusted(),
      rootId: `session:${ctx.sessionManager.getSessionId()}`,
      sessionFile: ctx.sessionManager.getSessionFile(), inputAdmission: pi.hostCapabilities.inputAdmission,
      model: ctx.model ? { id: ctx.model.id, provider: ctx.model.provider, api: ctx.model.api,
        hasEndpoint: !!ctx.model.baseUrl, contextWindow: ctx.model.contextWindow, maxTokens: ctx.model.maxTokens } : null });
    if (selectedModel || pi.hostCapabilities.inputAdmission !== 1) ctx.shutdown();
  });
}
