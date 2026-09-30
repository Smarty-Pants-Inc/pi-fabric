import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";

const backends = ["quickjs", "node-process", "monty", "cpython"] as const;
const context = { cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "ceiling-boundaries" } } as unknown as ExtensionContext;
const options = { context, signal: undefined, parentToolCallId: "ceiling-boundaries", onPartial() {} };
const busyUntil = (deadline: number): void => { while (Date.now() < deadline) { /* synchronous preparation/effect */ } };
const executeAtBoundary = (
  service: FabricExecutionService,
  code: string,
  setAdmission: (admit: () => void) => void,
  onPartial: Parameters<FabricExecutionService["execute"]>[0]["onPartial"] = options.onPartial,
) => {
  let admitted = false;
  return executeAfterAdmission((signal, startClock) => {
    setAdmission(() => { admitted = true; startClock(); });
    return service.execute({ ...options, code, signal, onPartial });
  }, () => admitted);
};
const codeFor = (backend: typeof backends[number], spend = true): string => backend === "monty" || backend === "cpython"
  ? `return await tools.call(ref="demo.effect", args={"spend": ${spend ? "True" : "False"}})`
  : `return tools.call({ ref: "demo.effect", args: { spend: ${spend} } });`;
const configFor = (backend: typeof backends[number]) => {
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.executor.mainMaxTimeoutMs = 500;
  config.executor.timeoutMs = 5_000;
  config.executor.memoryLimitBytes = 128 * 1024 * 1024;
  config.ui.updateDebounceMs = 0;
  config.approvals.read = "allow";
  config.approvals.write = "allow";
  if (backend === "monty" || backend === "cpython") { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
  else config.executor.runtime = backend;
  return config;
};

describe.each(backends)("absolute Main effect/publication boundaries (%s)", backend => {
  it.each(["preparation", "descriptor observation"] as const)("rejects new effect admission after synchronous %s", async stage => {
    const registry = new ActionRegistry();
    const descriptor = { name: "effect", description: "harmless counter", risk: "write" as const, inputSchema: { type: "object", additionalProperties: true } };
    let spendOnObservation = false;
    let lateCalls = 0;
    let admit = () => {};
    registry.register({ name: "demo", description: "budget probe", async list() { return [descriptor]; },
      async describe(_name, ctx) {
        if (spendOnObservation) { spendOnObservation = false; busyUntil(ctx.mainDeadlineAt! + 20); }
        return descriptor;
      },
      async prepareArguments(_name, args, ctx) {
        admit();
        if (args.spend) {
          if (stage === "preparation") busyUntil(ctx.mainDeadlineAt! + 20);
          else spendOnObservation = true;
        }
        return args;
      },
      async invoke(_name, args) { if (args.spend) lateCalls++; return "counter-only"; },
    });
    const service = new FabricExecutionService(registry, configFor(backend));
    try {
      expect((await executeAtBoundary(service, codeFor(backend, false), start => { admit = start; })).success).toBe(true);
      const result = await executeAtBoundary(service, codeFor(backend), start => { admit = start; });
      expect(result.error).toMatch(/MainExecutionCeilingError/);
      expect(lateCalls, "effect admission must check wall time after preparation and descriptor observation").toBe(0);
    } finally { await registry.close(); }
  });

  it.each(["result", "callbacks", "serialization", "exception"] as const)("rejects late %s before audits, media, previews or partials can publish it", async publication => {
    const registry = new ActionRegistry();
    const descriptor = { name: "effect", description: "harmless marker", risk: "read" as const, inputSchema: { type: "object", additionalProperties: true } };
    const marker = "CEILING_LATE_RESULT_MARKER";
    const partials: unknown[] = [];
    let admit = () => {};
    registry.register({ name: "demo", description: "publication probe", async list() { return [descriptor]; }, async describe() { return descriptor; },
      async invoke(_name, args, ctx) {
        admit();
        if (!args.spend) return "warm";
        if (publication === "serialization") {
          return { toJSON() { busyUntil(ctx.mainDeadlineAt! + 20); return { marker }; } };
        }
        busyUntil(ctx.mainDeadlineAt! + 20);
        if (publication === "exception") throw new Error(marker);
        if (publication === "callbacks") {
          ctx.attachMedia?.([{ type: "image", data: marker, mimeType: "image/png" }], marker);
          ctx.attachPreview?.({ marker });
          ctx.update(marker);
          ctx.activity?.({ type: "progress", message: marker });
          ctx.updateArguments?.({ marker });
        }
        return marker;
      },
    });
    const service = new FabricExecutionService(registry, configFor(backend));
    try {
      expect((await executeAtBoundary(service, codeFor(backend, false), start => { admit = start; })).success).toBe(true);
      const result = await executeAtBoundary(service, codeFor(backend), start => { admit = start; }, update => { partials.push(structuredClone(update)); });
      expect(result.value).toBeUndefined();
      expect(result.error).toMatch(/MainExecutionCeilingError/);
      expect(JSON.stringify([result.audits, result.trace, result.media, partials])).not.toContain(marker);
      expect(result.audits[0]?.success).toBe(false);
      expect(result.audits[0]?.result).toBeUndefined();
    } finally { await registry.close(); }
  });
});
