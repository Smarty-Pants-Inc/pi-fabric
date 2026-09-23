import { Type } from "typebox";
import { validationMessage } from "../core/action-arguments.js";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { JevClient, JevCredentials, type JevCredentialSource } from "../jev/client.js";
import { JevProgramManager, type JevManagerOptions } from "../jev/manager.js";
import { resolveJevModelRoute, type JevRoute } from "../jev/routes.js";
import type { JevLaunch, JevRequest } from "../jev/types.js";
import { jevObserveSchema } from "../jev/observation.js";

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
      maxEvaluations: Type.Optional(Type.Integer({ minimum: 1 })),
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
  { name: "evaluate", description: "Ask Jev typed Choice, Noul (probability of yes), and Score questions over shared state. No generated text. Batch independent questions. Sends state to TypeSafe and consumes API credits; no automatic retries.", inputSchema: jevRequestSchema as unknown as Record<string, unknown>, risk: "network", effect: { kind: "emission", resources: ["typesafe:inference"], ordering: "unknown" } },
  { name: "run", description: "Run an isolated TypeScript System One program in the foreground. Returns a terminal run envelope with schema-validated result or error. Globals: input, jev.evaluate, program.sleep(ms), program.emit(value), and Fabric tools restricted to exact requires. Loops are supported; no host imports or secrets.", inputSchema: jevLaunchSchema as unknown as Record<string, unknown>, risk: "execute" },
  { name: "spawn", description: "Launch a session-owned background Jev program. Optional observe requires read approval and subscribes to Main lifecycle events; program.nextEvent waits without polling and shares only explicitly selected bounded context. program.advise needs observe.delivery and requires jev.advise. Use status, wait/join, and stop; not restart-durable.", inputSchema: jevLaunchSchema as unknown as Record<string, unknown>, risk: "execute" },
  { name: "status", description: "Without id: credential configuration status (never retrieves secrets) and run summaries. With id: state, usage, bounded events after sequence, logs, result/error. Retains at most 64 events; nextSequence allows detecting gaps.", inputSchema: statusSchema as unknown as Record<string, unknown>, risk: "read" },
  { name: "wait", description: "Wait for a background Jev program's terminal run envelope. Cancelling the wait does not cancel the program.", inputSchema: idSchema as unknown as Record<string, unknown>, risk: "read" },
  { name: "join", description: "Alias for jev.wait: wait for a background Jev program's terminal run envelope without cancelling the program when the wait is cancelled.", inputSchema: idSchema as unknown as Record<string, unknown>, risk: "read" },
  { name: "advise", description: "Deliver bounded advice from an observing Jev run to Main. Requires its current consumed eventId, explicit observe.delivery, freshness, message budget, and the per-input feedback gate. The program helper program.advise supplies its own id. Subject to agent approvals; returns delivered and an optional suppression reason.", inputSchema: jevAdviceSchema as unknown as Record<string, unknown>, risk: "agent", effect: { kind: "emission", resources: ["main:messages"], ordering: "ordered" } },
  { name: "stop", description: "Cancel a Jev program and await sandbox cleanup. Idempotent for retained terminal runs; already-issued external effects cannot be undone.", inputSchema: idSchema as unknown as Record<string, unknown>, risk: "execute" },
];

export class JevProvider implements FabricProvider {
  readonly name = "jev";
  readonly description = "TypeSafe System One judgments and foreground/background reactive TypeScript programs";
  readonly manager: JevProgramManager;
  readonly client: JevClient;
  readonly route: JevRoute;
  constructor(options: JevManagerOptions & { credentialSource?: JevCredentialSource }, client?: JevClient) {
    this.manager = new JevProgramManager(options);
    this.route = resolveJevModelRoute(options.config.jev.model).route;
    this.client = client ?? new JevClient(options.config.jev, undefined,
      new JevCredentials(options.config.jev.credentialCommand, process.env, options.credentialSource, this.route.envKeys), this.route);
  }
  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return JEV_ACTION_DESCRIPTORS.filter(d => !query || `${d.name} ${d.description}`.toLowerCase().includes(query));
  }
  async describe(name: string): Promise<FabricActionDescriptor | undefined> { return JEV_ACTION_DESCRIPTORS.find(d => d.name === name); }
  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = await this.describe(name);
    if (!descriptor) throw new Error(`Unknown Jev action: ${name}`);
    const invalid = validationMessage(descriptor.inputSchema, args);
    if (invalid) throw new Error(`Invalid jev.${name} arguments: ${invalid}`);
    switch (name) {
      case "evaluate": return this.client.evaluate(args as unknown as JevRequest, context.signal ?? new AbortController().signal);
      case "run": return this.manager.launch(args as unknown as JevLaunch, context, false);
      case "spawn": return this.manager.launch(args as unknown as JevLaunch, context, true);
      case "status": return typeof args.id === "string" ? this.manager.status(args.id, args.after as number | undefined) : {
        credentials: this.client.credentials.status(), model: this.client.config.model, route: this.route.id, runs: this.manager.list(),
      };
      case "join":
      case "wait": return this.manager.wait(args.id as string, context.signal);
      case "advise": return this.manager.advise(args.id as string, args.eventId as string, args.message as string);
      case "stop": return this.manager.stop(args.id as string);
    }
  }
  async close(): Promise<void> { await this.manager.close(); this.client.close(); }
}
