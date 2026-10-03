import { validationMessage } from "../core/action-arguments.js";
import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import type { MeshIdentity } from "../mesh/store.js";
import {
  assertDecisionId,
  type DecisionStore,
  MAX_DECISION_BODY_CHARS,
  MAX_DECISION_DEADLINE_MS,
  MAX_DECISION_OPTIONS,
  MAX_DECISION_TEXT_CHARS,
  MAX_DECISION_TITLE_CHARS,
  MAX_ESCALATION_HOP_MS,
  MAX_ESCALATION_HOPS,
  MAX_ESCALATION_REASON_CHARS,
  MIN_ESCALATION_HOP_MS,
  type DecisionRecord,
  type DecisionStatus,
} from "../decisions/store.js";
import { actionArgNormalizer } from "./arg-normalization.js";

// Durable pending decisions: a program raises an approval, question, or
// escalation; a human (TUI `/fabric decisions`, `pi-fabric decisions` CLI) or
// the authorized holder answers it. Programs never answer decisions held by
// "user", and never answer one they raised in the same fabric_exec call.
// Authority follows the current holder: once an escalation moves a decision,
// the previous holder can no longer answer or escalate it.

const resource = ["fabric:decisions"];
const idSchema = { type: "string", minLength: 8, maxLength: 72 };
const holderSchema = { type: "string", maxLength: 139 };
const descriptors: FabricActionDescriptor[] = [
  {
    name: "raise",
    description: "Create a durable open decision (approval, question, or escalation) for a human or holder to answer; returns { id }",
    inputSchema: {
      type: "object",
      required: ["title"],
      additionalProperties: false,
      properties: {
        kind: { type: "string", enum: ["approval", "question", "escalation"] },
        title: { type: "string", minLength: 1, maxLength: MAX_DECISION_TITLE_CHARS },
        body: { type: "string", maxLength: MAX_DECISION_BODY_CHARS },
        options: {
          type: "array",
          minItems: 1,
          maxItems: MAX_DECISION_OPTIONS,
          items: {
            type: "object",
            required: ["id", "label"],
            additionalProperties: false,
            properties: {
              id: { type: "string", minLength: 1, maxLength: 64 },
              label: { type: "string", minLength: 1, maxLength: MAX_DECISION_TITLE_CHARS },
            },
          },
        },
        input: { type: "string", enum: ["text", "confirm", "select", "editor"] },
        holder: {
          type: "string",
          maxLength: 139,
          description: '"user" (default, human only), "root" (root session), or "supervisor:<participantId>"',
        },
        deadline: { type: "number", description: "Epoch ms; at most 30 days ahead" },
        timeoutMs: { type: "integer", minimum: 1_000, maximum: MAX_DECISION_DEADLINE_MS },
        onExpire: { type: "string", enum: ["cancel", "default", "escalate"] },
        defaultOptionId: { type: "string", maxLength: 64 },
        escalation: {
          type: "object",
          additionalProperties: false,
          description: 'With onExpire "escalate": holders in order (default climbs supervisor -> root -> user), per-hop timeout, and what the last expiry does',
          properties: {
            chain: { type: "array", minItems: 1, maxItems: MAX_ESCALATION_HOPS, items: holderSchema },
            hopTimeoutMs: { type: "integer", minimum: MIN_ESCALATION_HOP_MS, maximum: MAX_ESCALATION_HOP_MS },
            onFinal: { type: "string", enum: ["cancel", "default"] },
          },
        },
      },
    },
    risk: "agent",
    namespace: "coordination",
    effect: { kind: "emission", resources: resource, ordering: "ordered" },
  },
  {
    name: "wait",
    description: "Wait until a decision is answered, expires, or is cancelled (or timeoutMs passes) and return its record",
    inputSchema: {
      type: "object",
      required: ["id"],
      additionalProperties: false,
      properties: {
        id: idSchema,
        timeoutMs: { type: "integer", minimum: 0, maximum: MAX_DECISION_DEADLINE_MS },
      },
    },
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "list",
    description: "List decisions newest first, optionally by status and current holder",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        status: { type: "string", enum: ["open", "answered", "expired", "cancelled"] },
        holder: holderSchema,
        limit: { type: "integer", minimum: 1, maximum: 200 },
      },
    },
    risk: "read",
    namespace: "coordination",
    effect: { kind: "none", resources: resource, ordering: "commutative" },
  },
  {
    name: "answer",
    description: "Answer an open decision held by this participant (root or supervisor:<id>); never decisions held by \"user\"",
    inputSchema: {
      type: "object",
      required: ["id"],
      additionalProperties: false,
      properties: {
        id: idSchema,
        optionId: { type: "string", maxLength: 64 },
        text: { type: "string", maxLength: MAX_DECISION_TEXT_CHARS },
      },
    },
    risk: "agent",
    namespace: "coordination",
    effect: { kind: "emission", resources: resource, ordering: "ordered" },
  },
  {
    name: "escalate",
    description: "Pass an open decision this participant holds to the next holder in its escalation chain now",
    inputSchema: {
      type: "object",
      required: ["id"],
      additionalProperties: false,
      properties: {
        id: idSchema,
        reason: { type: "string", maxLength: MAX_ESCALATION_REASON_CHARS },
      },
    },
    risk: "agent",
    namespace: "coordination",
    effect: { kind: "emission", resources: resource, ordering: "ordered" },
  },
  {
    name: "cancel",
    description: "Cancel an open decision this participant raised or holds",
    inputSchema: {
      type: "object",
      required: ["id"],
      additionalProperties: false,
      properties: { id: idSchema },
    },
    risk: "agent",
    namespace: "coordination",
    effect: { kind: "emission", resources: resource, ordering: "ordered" },
  },
];

export const normalizeDecisionsArgs = actionArgNormalizer(() => descriptors);

const MAX_TRACKED_CALLS = 64;

export class DecisionsProvider implements FabricProvider {
  readonly name = "decisions";
  readonly description = "Durable pending decisions answered by a human or an authorized holder";
  // fabric_exec call id -> decisions raised inside that call (self-answer guard).
  readonly #raisedInCall = new Map<string, Set<string>>();

  constructor(
    readonly store: DecisionStore,
    readonly identity: MeshIdentity,
  ) {}

  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return descriptors.filter((action) => !query || `${action.name} ${action.description}`.toLowerCase().includes(query));
  }

  async describe(name: string): Promise<FabricActionDescriptor | undefined> {
    return descriptors.find((action) => action.name === name);
  }

  prepareArguments(name: string, args: Record<string, unknown>): Record<string, unknown> {
    return normalizeDecisionsArgs(name, args);
  }

  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = descriptors.find((action) => action.name === name);
    if (!descriptor) throw new Error(`Unknown decisions action: ${name}`);
    const error = validationMessage(descriptor.inputSchema, args);
    if (error) throw new Error(`Invalid decisions.${name} arguments: ${error}`);
    switch (name) {
      case "raise": {
        const runId = process.env.PI_FABRIC_PARENT_RUN?.trim();
        const record = await this.store.raise(args, runId ? { runId } : {});
        this.#trackRaised(context.parentToolCallId, record.id);
        context.activity?.({ type: "progress", message: `Decision raised: ${record.title}` });
        return { id: record.id };
      }
      case "wait":
        return this.store.wait(assertDecisionId(args.id), {
          ...(typeof args.timeoutMs === "number" ? { timeoutMs: args.timeoutMs } : {}),
          ...(context.signal ? { signal: context.signal } : {}),
        });
      case "list":
        return this.store.list({
          ...(typeof args.status === "string" ? { status: args.status as DecisionStatus } : {}),
          ...(typeof args.holder === "string" ? { holder: args.holder } : {}),
          ...(typeof args.limit === "number" ? { limit: args.limit } : {}),
        });
      case "answer": {
        const id = assertDecisionId(args.id);
        await this.#requireOpen(id);
        this.#assertNotRaisedHere(id, context, "answer");
        return this.store.answer(id, args, { answeredBy: this.identity.id, via: "program" }, (record) => this.#assertHolder(record));
      }
      case "escalate": {
        const id = assertDecisionId(args.id);
        await this.#requireOpen(id);
        this.#assertNotRaisedHere(id, context, "escalate");
        return this.store.escalate(id, args, { escalatedBy: this.identity.id }, (record) => this.#assertHolder(record));
      }
      case "cancel": {
        const id = assertDecisionId(args.id);
        await this.#requireOpen(id);
        return this.store.cancel(id, { answeredBy: this.identity.id, via: "program" }, (record) => {
          if (record.raisedBy.participantId !== this.identity.id) this.#assertHolder(record);
        });
      }
      default:
        throw new Error(`Unknown decisions action: ${name}`);
    }
  }

  async #requireOpen(id: string): Promise<DecisionRecord> {
    const record = await this.store.get(id);
    if (!record) throw new Error(`Unknown decision: ${id}`);
    if (record.status !== "open") throw new Error(`Decision ${id} is ${record.status}, not open`);
    return record;
  }

  #assertNotRaisedHere(id: string, context: FabricInvocationContext, verb: string): void {
    if (this.#raisedInCall.get(context.parentToolCallId)?.has(id)) {
      throw new Error(`Decision ${id} was raised by this same program; it cannot ${verb} it itself`);
    }
  }

  #assertHolder(record: DecisionRecord): void {
    if (record.holder === "user") {
      throw new Error(`Decision ${record.id} is held by the user; answer it with /fabric decisions or the pi-fabric decisions CLI`);
    }
    if (record.holder === "root" && this.identity.kind === "main") return;
    if (record.holder === `supervisor:${this.identity.id}`) return;
    throw new Error(`Decision ${record.id} is held by ${record.holder}, not this participant`);
  }

  #trackRaised(callId: string, id: string): void {
    let raised = this.#raisedInCall.get(callId);
    if (!raised) {
      if (this.#raisedInCall.size >= MAX_TRACKED_CALLS) {
        this.#raisedInCall.delete(this.#raisedInCall.keys().next().value!);
      }
      raised = new Set();
      this.#raisedInCall.set(callId, raised);
    }
    raised.add(id);
  }
}
