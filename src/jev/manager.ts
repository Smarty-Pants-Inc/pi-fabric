import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { runAbortable } from "../async-settlement.js";
import type { ActionRegistry, FabricCapabilityViewLease, FabricCallAudit } from "../core/action-registry.js";
import { ApprovalController, FabricSessionApprovals } from "../core/approval-controller.js";
import { FabricAutoApprovalClassifier } from "../core/auto-approval-classifier.js";
import type { FabricConfig } from "../config.js";
import type { FabricInvocationContext } from "../protocol.js";
import type { JevLaunch, JevRunInfo, JevJson, JevResponse } from "./types.js";
import { checkSchema, checkValue, jsonText, object } from "./validation.js";
import { checkObserve, type JevObservationHost, type JevSubscription } from "./observation.js";

interface Run {
  info: JevRunInfo;
  controller: AbortController;
  done: Promise<JevRunInfo>;
  observation: JevSubscription | undefined;
}
export interface JevManagerOptions {
  registry: ActionRegistry;
  config: FabricConfig;
  observationHost?: JevObservationHost | undefined;
  authorize?(ref: string, parentToolCallId: string): Promise<void>;
}
const prelude = `const input = JSON.parse(π.__jevInput);
const program = Object.freeze({
  sleep: (ms: number): Promise<unknown> => tools.call({ ref: "jev.$sleep", args: { ms } }),
  emit: (value: unknown): Promise<unknown> => tools.call({ ref: "jev.$emit", args: { value } }),
  nextEvent: (): Promise<FabricJevHostEvent> => tools.call({ ref: "jev.$nextEvent" }) as Promise<FabricJevHostEvent>,
  advise: (args: {eventId: string; message: string}): Promise<FabricJevAdviceResult> => tools.call({ ref: "jev.advise", args: {...args, id: π.__jevRunId} }) as Promise<FabricJevAdviceResult>,
});\n`;

export class JevProgramManager {
  readonly #runs = new Map<string, Run>();
  readonly #approvals = new FabricSessionApprovals();
  #closed = false;
  #starting = 0;
  constructor(readonly options: JevManagerOptions) {}
  list() {
    return [...this.#runs.values()].map(({ info }) => ({
      id: info.id, name: info.name, state: info.state, background: info.background,
      startedAt: info.startedAt, evaluations: info.evaluations, toolCalls: info.toolCalls,
      ...(info.observation ? { observation: structuredClone(info.observation) } : {}),
    }));
  }
  #get(id: string): Run {
    const run = this.#runs.get(id);
    if (!run) throw new Error("Unknown or expired Jev run ID");
    return run;
  }
  status(id: string, after = 0): JevRunInfo {
    const info = this.#get(id).info;
    return structuredClone({ ...info, events: info.events.filter(e => e.sequence > after) });
  }
  async wait(id: string, signal?: AbortSignal): Promise<JevRunInfo> {
    return structuredClone(await runAbortable(signal, () => this.#get(id).done));
  }
  /** Alias for wait. */
  join(id: string, signal?: AbortSignal): Promise<JevRunInfo> {
    return this.wait(id, signal);
  }
  async stop(id: string): Promise<JevRunInfo> {
    const run = this.#get(id);
    if (run.info.state === "running") run.controller.abort(new Error("Jev run stopped"));
    return structuredClone(await run.done);
  }
  advise(id: string, eventId: string, message: string) {
    if (typeof message !== "string" || !message.trim() || message.length > 2000 || typeof eventId !== "string" || eventId.length > 256)
      throw new Error("Advice requires a current eventId and 1–2000 message characters");
    const run = this.#get(id);
    if (!run.observation) throw new Error("Jev run has no observation subscription");
    return run.observation.advise(eventId, message);
  }
  async launch(request: JevLaunch, context: FabricInvocationContext, background: boolean): Promise<JevRunInfo> {
    const { config, registry } = this.options;
    if (this.#closed) throw new Error("Jev program manager is closed");
    if (this.#starting + [...this.#runs.values()].filter(r => r.info.state === "running").length >= config.jev.maxConcurrentRuns)
      throw new Error("Jev concurrent run limit reached");
    context.signal?.throwIfAborted();
    this.#starting++;
    let lease: FabricCapabilityViewLease | undefined;
    let starting = true;
    try {
      const { program: definition, input, observe } = structuredClone(request);
      const observationHost = this.options.observationHost;
      const observationRevision = observationHost?.revision;
      if (observe !== undefined) {
        checkObserve(observe);
        if (!background) throw new Error("Observation requires jev.spawn; foreground runs cannot wait for their own turn to end");
        if (!observationHost) throw new Error("Main lifecycle observation is unavailable in this host");
        if (context.extensionContext.sessionManager?.getSessionId() !== observationHost.sessionId)
          throw new Error("Jev observation is scoped to its owning Main session");
      }
      checkSchema(definition.inputSchema);
      checkSchema(definition.outputSchema);
      checkValue(definition.inputSchema, input, "Program input");
      const requires = [...new Set(definition.requires)];
      for (const ref of requires) {
        if (!/^[a-z][a-z0-9_-]*\.[a-zA-Z0-9_.$-]+$/.test(ref) || (ref.startsWith("jev.") && ref !== "jev.evaluate" && ref !== "jev.advise"))
          throw new Error("Programs require exact action refs; recursive Jev lifecycle calls are not allowed");
        if (context.capabilityView && !Object.hasOwn(context.capabilityView.bindings, ref))
          throw new Error(`Jev program cannot widen its caller's capabilities: ${ref}`);
        if (!config.fullCodeMode && (ref.startsWith("pi.") || ref.startsWith("extensions.")))
          throw new Error("Full-code tool access is disabled");
      }
      lease = await registry.acquireCapabilityView(requires, context);
      if (!lease.satisfied || !lease.view) throw new Error(`Missing Jev program capabilities: ${lease.missing.join(", ")}`);
      if (context.capabilityView) for (const ref of requires) {
        const before = context.capabilityView.bindings[ref]!;
        const after = lease.view.bindings[ref]!;
        if (before.providerBindingId !== after.providerBindingId || before.descriptorHash !== after.descriptorHash)
          throw new Error("Caller capability generation changed; relaunch with a fresh commitment");
      }
      const { TypeScriptKernelRuntime } = await import("../runtime/typescript-kernel.js");
      const runtime = new TypeScriptKernelRuntime("quickjs");
      const sources = await registry.guestTypeSources({ ...context, capabilityView: lease.view });
      const { code, checked } = runtime.prepare(prelude + definition.code, true, [], sources, [], true);
      if (checked.errors.length) throw new Error(`Jev program typecheck failed: ${checked.errors.map(e => e.message).join("; ").slice(0, 2000)}`);
      const approval = new ApprovalController(config.approvals, context.extensionContext, this.#approvals, new FabricAutoApprovalClassifier(() => config.jev));
      if (observe) await approval.approve({
        ref: "jev.spawn", provider: "jev", name: "spawn",
        description: "Observe future Main lifecycle events and explicitly selected content for a bounded Jev program",
        inputSchema: {}, risk: "read",
      }, { observe });
      if (this.#closed) throw new Error("Jev program manager is closed");
      context.signal?.throwIfAborted();
      if (observe && observationHost!.revision !== observationRevision)
        throw new Error("Main lifecycle changed while preparing the observer; relaunch explicitly with current context");
      const limits = definition.limits ?? {};
      const bounded = (value: number | undefined, fallback: number, ceiling: number) => Math.min(value ?? fallback, ceiling);
      const timeoutMs = bounded(limits.timeoutMs, 60_000, config.jev.maxDurationMs);
      const maxEvaluations = bounded(limits.maxEvaluations, 100, config.jev.maxEvaluations);
      const maxToolCalls = bounded(limits.maxToolCalls, 1000, config.jev.maxToolCalls);
      const maxTokens = bounded(limits.maxTokens, 100_000, config.jev.maxTokens);
      const id = randomUUID();
      const controller = new AbortController();
      const info: JevRunInfo = {
        id, name: definition.name, state: "running", background, startedAt: Date.now(),
        evaluations: 0, toolCalls: 0, usage: { input_tokens: 0, output_tokens: 0 },
        events: [], nextSequence: 1, logs: [],
      };
      let budgetFailure: string | undefined;
      const failBudget = (message: string): never => {
        budgetFailure = message;
        queueMicrotask(() => controller.abort(new Error(message)));
        throw new Error(message);
      };
      const observation = observe ? observationHost!.subscribe(observe, { id, name: definition.name }, controller) : undefined;
      if (observation) info.observation = observation.stats;
      const runLease = lease;
      lease = undefined;
      const abort = () => controller.abort(new Error("Foreground caller cancelled"));
      if (!background) context.signal?.addEventListener("abort", abort, { once: true });
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; controller.abort(new Error("Jev deadline exceeded")); }, timeoutMs);
      let evaluating = false;
      const audits: FabricCallAudit[] = [];
      const emit = (value: unknown) => {
        jsonText(value, 4096, "Program event");
        info.events.push({ sequence: info.nextSequence++, at: Date.now(), value: value as JevJson });
        if (info.events.length > 64) info.events.shift();
      };
      const run: Run = { info, controller, observation, done: undefined! };
      this.#runs.set(id, run);
      this.#starting--;
      starting = false;
      this.#prune();
      // Yield before execution so spawn returns without running a controller tick inline.
      run.done = (async () => {
        try {
          await delay(0, undefined, { signal: controller.signal });
          const result = await runtime.execute(code, async (rawRef, rawArgs, signal) => {
            signal.throwIfAborted();
            controller.signal.throwIfAborted();
            if (budgetFailure) throw new Error(budgetFailure);
            if (++info.toolCalls > maxToolCalls) return failBudget("Jev tool-call budget exhausted");
            const ref = rawRef === "fabric.$call" ? rawArgs.ref : rawRef;
            const args = rawRef === "fabric.$call" ? rawArgs.args ?? {} : rawArgs;
            if (typeof ref !== "string" || !object(args)) throw new Error("Invalid Jev program tool call");
            if (ref === "jev.$sleep") {
              if (typeof args.ms !== "number" || !Number.isFinite(args.ms) || args.ms < 0 || args.ms > timeoutMs) throw new Error("Invalid program.sleep duration");
              await delay(args.ms, undefined, { signal });
              return null;
            }
            if (ref === "jev.$emit") { emit(args.value); return null; }
            if (ref === "jev.$nextEvent") {
              if (!observation) throw new Error("program.nextEvent requires observe on jev.spawn");
              return observation.next();
            }
            if (ref === "jev.advise" && args.id !== id) throw new Error("A Jev program can only advise through its own run");
            if (!Object.hasOwn(runLease.view!.bindings, ref)) throw new Error(`Capability not granted to Jev program: ${ref}`);
            if (ref === "jev.evaluate") {
              if (evaluating) throw new Error("One Jev evaluation may be in flight per program; batch independent questions in one request");
              if (info.evaluations >= maxEvaluations || info.usage.input_tokens + info.usage.output_tokens >= maxTokens)
                return failBudget("Jev inference budget exhausted");
              info.evaluations++;
              evaluating = true;
            }
            try {
              const value = await registry.invoke(ref, args, {
                cwd: context.cwd, extensionContext: context.extensionContext, signal, capabilityView: runLease.view!,
                ...(context.effectPolicy ? { effectPolicy: context.effectPolicy } : {}),
                parentToolCallId: `jev:${id}`, nestedToolCallId: `jev:${id}:${info.toolCalls}`,
                update() {}, activity() {}, attachMedia() {}, attachPreview() {},
                authorize: async action => { await this.options.authorize?.(action.ref, `jev:${id}`); },
                approve: async (action, prepared) => {
                  if (action.ref === "schema.commit") {
                    await approval.approve({ ...action, risk: "write" }, prepared);
                    await approval.approve({ ...action, risk: "execute" }, prepared);
                  } else await approval.approve(action, prepared);
                },
                audits, maxResultChars: ref === "jev.evaluate" ? 1_048_576 : config.executor.maxNestedResultChars,
              });
              if (ref === "jev.evaluate") {
                const response = value as JevResponse;
                info.usage.input_tokens += response.usage.input_tokens;
                info.usage.output_tokens += response.usage.output_tokens;
                if (info.usage.input_tokens + info.usage.output_tokens > maxTokens)
                  return failBudget("Jev reported-token budget exhausted");
              }
              return value;
            } finally {
              if (ref === "jev.evaluate") evaluating = false;
              // Keep audit retention bounded even in a long-lived loop.
              if (audits.length > 64) audits.splice(0, audits.length - 64);
            }
          }, {
            timeoutMs, memoryLimitBytes: Math.min(config.executor.memoryLimitBytes, 64 * 1024 * 1024),
            maxCpuSliceMs: 100, maxPendingTimers: 128, maxLogChars: 4096, strings: { __jevInput: JSON.stringify(input), __jevRunId: id }, signal: controller.signal,
          });
          info.logs = result.logs;
          if (budgetFailure) { info.state = "failed"; info.error = budgetFailure; }
          else if (timedOut || result.terminationReason === "timed_out") { info.state = "timed_out"; info.error = result.error ?? "Jev deadline exceeded"; }
          else if (controller.signal.aborted || result.terminationReason === "aborted") { info.state = "cancelled"; }
          else if (result.terminationReason !== "completed") { info.state = "failed"; info.error = result.error ?? "Jev program failed"; }
          else {
            checkValue(definition.outputSchema, result.value, "Program output");
            info.result = result.value;
            info.state = "completed";
          }
        } catch (error) {
          info.state = budgetFailure ? "failed" : timedOut ? "timed_out" : controller.signal.aborted ? "cancelled" : "failed";
          info.error = (budgetFailure ?? (error instanceof Error ? error.message : "Jev program failed")).slice(0, 2000);
        } finally {
          clearTimeout(timer);
          observation?.close();
          context.signal?.removeEventListener("abort", abort);
          await runLease.release();
          info.endedAt = Date.now();
          this.#prune();
        }
        return info;
      })();
      return background ? this.status(id) : structuredClone(await run.done);
    } finally {
      if (starting) this.#starting--;
      await lease?.release();
    }
  }
  #prune(): void {
    for (const [id, run] of this.#runs) {
      if (this.#runs.size <= this.options.config.jev.maxRetainedRuns) break;
      if (run.info.state !== "running") this.#runs.delete(id);
    }
  }
  /** Runs starting or running: a reload cancels them (smarty-dev#2160). */
  runningCount(): number {
    return this.#starting + [...this.#runs.values()].filter(r => r.info.state === "running").length;
  }
  stopAll(): void {
    for (const run of this.#runs.values()) if (run.info.state === "running") run.controller.abort(new Error("Jev provider closed"));
  }
  async close(): Promise<void> {
    this.#closed = true;
    // Terminal runs are already out of QuickJS and may be releasing the last
    // provider lease right now. Waiting for those here would self-deadlock.
    const running = [...this.#runs.values()].filter(r => r.info.state === "running");
    this.stopAll();
    await Promise.all(running.map(r => r.done));
    this.#runs.clear();
  }
}
