import { Type } from "typebox";
import { validationMessage } from "../core/action-arguments.js";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { JevClient, JevCredentials, type JevCredentialSource, type JevDispatch } from "../jev/client.js";
import type { DurableShellBridge } from "../jev-fabric/bridge.js";
import type { JevFabricServe } from "../jev-fabric/serve.js";
import { JevProgramManager, type JevManagerOptions } from "../jev/manager.js";
import { resolveJevModelRoute, type JevRoute } from "../jev/routes.js";
import type { JevLaunch, JevRequest } from "../jev/types.js";
import { jevObserveSchema } from "../jev/observation.js";
import { decisionRequestSchema } from "../jev/decision-schema.js";
import { JevDecisions, decisionEnvironment } from "../jev/decisions.js";
import type { DecisionRequest } from "../jev/decision-types.js";

const description = Type.Union([Type.String(), Type.Array(Type.Unknown()), Type.Record(Type.String(), Type.Unknown())]);
const question = Type.Union([
  Type.Object({ type: Type.Literal("noul"), instructions: description, criteria: Type.Optional(Type.Object({ true: Type.Optional(description), false: Type.Optional(description) }, { additionalProperties: false })) }, { additionalProperties: false }),
  Type.Object({ type: Type.Literal("choice"), instructions: description, criteria: Type.Record(Type.String(), Type.Union([description, Type.Null()]), { minProperties: 1, maxProperties: 255 }) }, { additionalProperties: false }),
  Type.Object({ type: Type.Literal("score"), instructions: description, criteria: Type.Array(description, { minItems: 2, maxItems: 10 }) }, { additionalProperties: false }),
]);
export const jevRequestSchema = Type.Object({
  state: description,
  questions: Type.Record(Type.String(), question, { minProperties: 1, maxProperties: 128 }),
  model: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
}, { additionalProperties: false });
export const jevLaunchSchema = Type.Object({
  program: Type.Object({
    name: Type.String({ minLength: 1, maxLength: 128 }),
    code: Type.String({ minLength: 1, maxLength: 65_536 }),
    inputSchema: Type.Record(Type.String(), Type.Unknown()),
    outputSchema: Type.Record(Type.String(), Type.Unknown()),
    requires: Type.Array(Type.String({ minLength: 3, maxLength: 256 }), { maxItems: 64, uniqueItems: true }),
    limits: Type.Optional(Type.Object({
      timeoutMs: Type.Optional(Type.Integer({ minimum: 1 })),
      maxEvaluations: Type.Optional(Type.Integer({ minimum: 0 })),
      maxToolCalls: Type.Optional(Type.Integer({ minimum: 1 })),
      maxTokens: Type.Optional(Type.Integer({ minimum: 1 })),
    }, { additionalProperties: false })),
  }, { additionalProperties: false }),
  input: Type.Unknown(),
  observe: Type.Optional(jevObserveSchema),
}, { additionalProperties: false });
const idSchema = Type.Object({ id: Type.String({ minLength: 1, maxLength: 128 }) }, { additionalProperties: false });
const statusSchema = Type.Object({
  id: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  after: Type.Optional(Type.Integer({ minimum: 0 })),
}, { additionalProperties: false });
export const jevAdviceSchema = Type.Object({
  id: Type.String({ minLength: 1, maxLength: 128 }),
  eventId: Type.String({ minLength: 1, maxLength: 256 }),
  message: Type.String({ minLength: 1, maxLength: 2000 }),
}, { additionalProperties: false });
export const JEV_ACTION_DESCRIPTORS: FabricActionDescriptor[] = [
  { name: "decide", description: "Explicit lossless decision inference through native adapters. Returns status (including error), rawJson, provenance, refusals and nullable usage without legacy normalization. Requires decisions and decision-targets; no classify fallback. Endpoint overrides require an exact trusted profile/registered/preset route. Inspect budget.exhausted; calls consume credits.", inputSchema: decisionRequestSchema as unknown as Record<string, unknown>, risk: "network", effect: { kind: "emission", resources: ["decision:inference"], ordering: "ordered" } },
  { name: "resolveDecision", description: "Resolve a per-call decision target offline before credentials. No model requests or key lookup; returns target, unresolved credential env, image support, generated flag and selected profile only.", inputSchema: Type.Object({ request: decisionRequestSchema }, { additionalProperties: false }) as unknown as Record<string, unknown>, risk: "read" },
  { name: "decisionProviders", description: "List native decision adapter presets and capabilities offline. These are not verified live models or credential checks.", inputSchema: Type.Object({}, { additionalProperties: false }) as unknown as Record<string, unknown>, risk: "read" },
  { name: "models", description: "List registered Pi classifier handles intersected with native decision adapter capabilities, image support, generated/logit provenance and unverified credential presence. No inference or credential resolver execution.", inputSchema: Type.Object({}, { additionalProperties: false }) as unknown as Record<string, unknown>, risk: "read" },
  { name: "evaluate", description: "Ask Jev typed Choice, Noul (probability of yes), and Score questions over shared state. No generated text. Batch independent questions. Sends state to TypeSafe and consumes API credits; no automatic retries.", inputSchema: jevRequestSchema as unknown as Record<string, unknown>, risk: "network", effect: { kind: "emission", resources: ["typesafe:inference"], ordering: "unknown" } },
  { name: "run", description: "Run a bounded TypeScript shell orchestrator or typed-decision program in the foreground. Returns a terminal run envelope with schema-validated result or error. Globals: input, jev.evaluate, program.sleep(ms), program.emit(value), and Fabric tools restricted to exact requires. Use granted pi.bash and tasks.wait/watch for shell orchestration; maxEvaluations:0 disables program inference. No automatic judgments, host imports or secrets.", inputSchema: jevLaunchSchema as unknown as Record<string, unknown>, risk: "execute" },
  { name: "spawn", description: "Launch a session-owned background Jev program. Optional observe requires read approval and subscribes to Main lifecycle events; program.nextEvent waits without polling and shares only explicitly selected bounded context. program.advise needs observe.delivery and requires jev.advise. Use status, wait/join, and stop; not restart-durable.", inputSchema: jevLaunchSchema as unknown as Record<string, unknown>, risk: "execute" },
  { name: "status", description: "Without id: credential configuration status (never retrieves secrets) and run summaries. With id: state, usage, bounded events after sequence, logs, result/error. Retains at most 64 events; nextSequence allows detecting gaps.", inputSchema: statusSchema as unknown as Record<string, unknown>, risk: "read" },
  { name: "wait", description: "Wait for a background Jev program's terminal run envelope. Cancelling the wait does not cancel the program.", inputSchema: idSchema as unknown as Record<string, unknown>, risk: "read" },
  { name: "join", description: "Alias for jev.wait: wait for a background Jev program's terminal run envelope without cancelling the program when the wait is cancelled.", inputSchema: idSchema as unknown as Record<string, unknown>, risk: "read" },
  { name: "advise", description: "Deliver bounded advice from an observing Jev run to Main. Requires its current consumed eventId, explicit observe.delivery, freshness, message budget, and the per-input feedback gate. The program helper program.advise supplies its own id. Subject to agent approvals; returns delivered and an optional suppression reason.", inputSchema: jevAdviceSchema as unknown as Record<string, unknown>, risk: "agent", effect: { kind: "emission", resources: ["main:messages"], ordering: "ordered" } },
  { name: "stop", description: "Cancel a Jev program and await sandbox cleanup. Idempotent for retained terminal runs; already-issued external effects cannot be undone.", inputSchema: idSchema as unknown as Record<string, unknown>, risk: "execute" },
];

export class JevProvider implements FabricProvider {
  readonly name = "jev";
  readonly description = "Shell-first TypeScript orchestration with explicit typed Jev decisions";
  readonly manager: JevProgramManager;
  readonly client: JevClient;
  readonly route: JevRoute;
  /** One jev-fabric `serve` connection per program run: one client, one budget, like a Bend program. */
  readonly #runs = new Map<string, Promise<JevFabricServe | undefined>>();
  #transport: "fabric" | "jev-fabric" | undefined;
  readonly #jevFabric: DurableShellBridge | undefined;
  readonly #decisions: JevDecisions;
  readonly #runDecisions = new Map<string, JevDecisions>();
  readonly #decisionEnv: NodeJS.ProcessEnv;
  #closed = false;
  constructor(options: JevManagerOptions & { credentialSource?: JevCredentialSource; jevFabric?: DurableShellBridge | undefined }, client?: JevClient) {
    this.#jevFabric = options.jevFabric;
    this.#decisions = new JevDecisions(options.config.jev);
    this.#decisionEnv = decisionEnvironment(options.config.jev);
    this.manager = new JevProgramManager({ ...options, onRunStarted: id => { this.#runDecisions.set(`jev:${id}`, this.#decisions.fork()); options.onRunStarted?.(id); } });
    this.route = resolveJevModelRoute(options.config.jev.model).route;
    this.client = client ?? new JevClient(options.config.jev, undefined,
      new JevCredentials(options.config.jev.credentialCommand, process.env, options.credentialSource, this.route.envKeys), this.route);
  }
  /** UI integration: future direct calls/new programs only; never resets the session budget. */
  setDecisionProfile(profile: string | null | undefined): void { this.#decisions.setProfile(profile); }
  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return JEV_ACTION_DESCRIPTORS.filter(d => !query || `${d.name} ${d.description}`.toLowerCase().includes(query));
  }
  async describe(name: string): Promise<FabricActionDescriptor | undefined> { return JEV_ACTION_DESCRIPTORS.find(d => d.name === name); }
  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    if (this.#closed) throw new Error("Jev provider is closed");
    const descriptor = await this.describe(name);
    if (!descriptor) throw new Error(`Unknown Jev action: ${name}`);
    const invalid = validationMessage(descriptor.inputSchema, args);
    if (invalid) throw new Error(`Invalid jev.${name} arguments: ${invalid}`);
    switch (name) {
      case "decide":
      case "resolveDecision":
      case "decisionProviders":
      case "models": {
        const scope = context.parentToolCallId.startsWith("jev:") ? context.parentToolCallId : "$decision-session";
        const serve = (await this.#connection(scope, true))!;
        const adapter = this.#runDecisions.get(scope) ?? this.#decisions;
        if (name === "decide") return adapter.decide(serve, args as unknown as DecisionRequest, context);
        if (name === "resolveDecision") return adapter.resolve(serve, args.request as Partial<DecisionRequest>, context);
        if (name === "models") return adapter.models(serve, context);
        return adapter.providers(serve, context.signal);
      }
      case "evaluate": {
        const dispatch = await this.#dispatchFor(context.parentToolCallId);
        this.#transport = dispatch ? "jev-fabric" : "fabric";
        return this.client.evaluate(args as unknown as JevRequest, context.signal ?? new AbortController().signal, dispatch);
      }
      case "run": return this.manager.launch(args as unknown as JevLaunch, context, false);
      case "spawn": return this.manager.launch(args as unknown as JevLaunch, context, true);
      case "status": return typeof args.id === "string" ? this.manager.status(args.id, args.after as number | undefined) : {
        credentials: this.client.credentials.status(), model: this.client.config.model, route: this.route.id, runs: this.manager.list(),
        transport: { configured: this.client.config.transport, ...(this.#transport ? { last: this.#transport } : {}) },
      };
      case "join":
      case "wait": return this.manager.wait(args.id as string, context.signal);
      case "advise": return this.manager.advise(args.id as string, args.eventId as string, args.message as string);
      case "stop": return this.manager.stop(args.id as string);
    }
  }
  /**
   * Program decisions go through the run's own jev-fabric connection when
   * configured and available; direct calls stay in-process. `auto` falls back
   * to in-process only when no suitable binary resolves, never after a request fails.
   */
  async #dispatchFor(scope: string | undefined): Promise<JevDispatch | undefined> {
    // Direct legacy calls are deliberately unchanged and remain in-process.
    if (!scope?.startsWith("jev:")) return undefined;
    const decisions = this.manager.usesDecisions(scope.slice(4));
    if (this.client.config.transport === "fabric" && !decisions) return undefined;
    const serve = await this.#connection(scope, decisions);
    if (!serve) return undefined;
    const provider = this.route.id === "vercel-ai-gateway" ? "vercel" : this.route.id;
    return (request, credential, signal) => serve.request("jev", {
      request, provider, credential, timeoutMs: this.client.config.requestTimeoutMs,
    }, signal);
  }

  async #connection(scope: string, decisions: boolean): Promise<JevFabricServe | undefined> {
    const transport = this.client.config.transport;
    const bridge = this.#jevFabric;
    if (!bridge) {
      if (decisions || transport === "jev-fabric") throw new Error("Jev decisions/jev.transport jev-fabric needs jev-fabric, which runs on macOS and Linux outside managed hosts");
      return undefined;
    }
    let pending = this.#runs.get(scope);
    if (!pending) {
      pending = (async () => {
        let binary: string;
        try {
          binary = (await bridge.resolve(decisions ? "decisions" : "jev")).path;
        } catch (error) {
          if (decisions || transport === "jev-fabric") throw error;
          return undefined;
        }
        const { JevFabricServe } = await import("../jev-fabric/serve.js");
        const limits = scope.startsWith("jev:") ? this.manager.inferenceLimits(scope.slice(4)) : undefined;
        return JevFabricServe.open(binary, {
          home: bridge.home, cwd: bridge.options.cwd, timeoutMs: 86_400_000,
          evaluations: limits?.evaluations ?? this.client.config.maxEvaluations, tokens: limits?.tokens ?? this.client.config.maxTokens,
          env: this.#decisionEnv,
        });
      })();
      this.#runs.set(scope, pending);
    }
    const serve = await pending;
    if (decisions && (!serve || serve.banner.protocol < 2 || !["decisions", "decision-targets"].every(feature => serve.banner.features?.includes(feature)))) {
      await serve?.close();
      throw new Error("Lossless decisions require native decisions and decision-targets features; no legacy fallback is permitted");
    }
    return serve;
  }

  async invocationEnded(parentToolCallId: string): Promise<void> {
    this.#runDecisions.get(parentToolCallId)?.close();
    this.#runDecisions.delete(parentToolCallId);
    const pending = this.#runs.get(parentToolCallId);
    if (!pending) return;
    this.#runs.delete(parentToolCallId);
    await (await pending.catch(() => undefined))?.close();
  }

  async close(): Promise<void> {
    this.#closed = true;
    await this.manager.close();
    await this.client.drainCredentials();
    this.client.close();
    this.#decisions.close();
    for (const adapter of this.#runDecisions.values()) adapter.close();
    this.#runDecisions.clear();
    const connections = [...this.#runs.values()];
    this.#runs.clear();
    await Promise.allSettled(connections.map(async pending => (await pending)?.close()));
  }
}
