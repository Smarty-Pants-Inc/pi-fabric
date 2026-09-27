import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";

const MAIN_AGENT_ALIAS = "main";
export type FabricAgentMessageDelivery = "steer" | "followUp";

export interface FabricMainAgentInfo {
  id: string;
  name: "Main";
  kind: "main";
  status: "idle" | "running" | "remote";
  runner: "pi";
  transport: "host";
  cwd?: string;
  sessionId?: string;
  model?: string;
  thinking?: string;
  startedAt?: number;
  updatedAt: number;
  pendingMessages: boolean;
  local: boolean;
}

export interface FabricMainAgentDeliveryRequest {
  from: MeshIdentity;
  message: string;
  delivery: FabricAgentMessageDelivery;
  triggerTurn?: boolean;
  data?: unknown;
}

/** How far behind a Main is on followUps, so a sender can switch to steer (smarty-dev#1495). */
export interface FabricFollowUpQueueDepth {
  pendingFollowUps: number;
  oldestAgeS: number;
}

export interface FabricAgentMessageResult extends Partial<FabricFollowUpQueueDepth> {
  queued: true;
  messageId: string;
  routed: "local" | "main" | "mesh";
  acknowledged?: boolean;
}

export interface FabricMainModelSwitchResult {
  ok: boolean;
  error?: string;
}

export interface FabricMainAgentTarget {
  readonly id: string;
  readonly local: boolean;
  matches(id: string): boolean;
  info(context?: ExtensionContext): FabricMainAgentInfo;
  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult;
  // Switch Main's live session model in place. Only local hosts hold the pi
  // extension session required for the mutation, so remote targets omit it.
  switchModel?(
    target: { provider: string; id: string },
    context: ExtensionContext,
  ): Promise<FabricMainModelSwitchResult>;
}

export interface FabricIdentityResolution {
  identity: MeshIdentity;
  mainAgentId: string;
}

export const resolveFabricIdentity = (
  sessionId: string,
  environment: NodeJS.ProcessEnv = process.env,
): FabricIdentityResolution => {
  const actorId = environment.PI_FABRIC_ACTOR_ID?.trim();
  const parentAgentId = environment.PI_FABRIC_PARENT_RUN?.trim();
  const identity: MeshIdentity = actorId
    ? {
        id: actorId,
        name: environment.PI_FABRIC_ACTOR_NAME?.trim() || actorId.slice(0, 8),
        kind: "actor",
        sessionId,
      }
    : parentAgentId
      ? {
          id: parentAgentId,
          name: environment.PI_FABRIC_AGENT_NAME?.trim() || parentAgentId.slice(0, 8),
          kind: "agent",
          sessionId,
        }
      : { id: `session:${sessionId}`, name: "main", kind: "main", sessionId };
  const inheritedMainAgentId = environment.PI_FABRIC_MAIN_AGENT_ID?.trim();
  return {
    identity,
    mainAgentId:
      inheritedMainAgentId || (identity.kind === "main" ? identity.id : `session:${sessionId}`),
  };
};

const escapeXmlText = (value: string): string =>
  value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

const serializableData = (value: unknown): unknown => {
  try {
    const serialized = JSON.stringify(value);
    return serialized === undefined ? undefined : JSON.parse(serialized) as unknown;
  } catch {
    return { fabricUnserializable: true };
  }
};

interface HeldAgentMessage {
  id: string;
  from: MeshIdentity;
  message: string;
  sentAt: number;
  data?: unknown;
}

export class MainAgentController implements FabricMainAgentTarget {
  readonly startedAt = Date.now();
  readonly #held: HeldAgentMessage[] = [];
  readonly #unsubscribe: Array<() => void> = [];
  #context: ExtensionContext | undefined;
  #flushMs = 0;
  #suspended = false;
  #closed = false;
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    readonly pi: ExtensionAPI,
    readonly id: string,
    readonly local: boolean,
    readonly cwd: string,
    readonly sessionId?: string,
  ) {}

  matches(id: string): boolean {
    const target = id.trim();
    return target === MAIN_AGENT_ALIAS || target === this.id;
  }

  info(context?: ExtensionContext): FabricMainAgentInfo {
    const model =
      this.local && context?.model
        ? `${context.model.provider}/${context.model.id}`
        : undefined;
    const thinking = this.local ? this.pi.getThinkingLevel() : undefined;
    return {
      id: this.id,
      name: "Main",
      kind: "main",
      status: this.local ? (context?.isIdle() === false ? "running" : "idle") : "remote",
      runner: "pi",
      transport: "host",
      ...(this.local ? { cwd: this.cwd, startedAt: this.startedAt } : {}),
      ...(this.sessionId ? { sessionId: this.sessionId } : {}),
      ...(model ? { model } : {}),
      ...(thinking ? { thinking } : {}),
      updatedAt: Date.now(),
      pendingMessages: this.local ? (context?.hasPendingMessages() ?? false) : false,
      local: this.local,
    };
  }

  async switchModel(
    target: { provider: string; id: string },
    context: ExtensionContext,
  ): Promise<FabricMainModelSwitchResult> {
    if (!this.local) {
      return { ok: false, error: `Main agent ${this.id} is owned by another Fabric process` };
    }
    const key = `${target.provider}/${target.id}`;
    const model = context.modelRegistry.find(target.provider, target.id);
    if (!model) return { ok: false, error: `Model is not available: ${key}` };
    const switched = await this.pi.setModel(model);
    if (!switched) return { ok: false, error: `No authentication configured for model: ${key}` };
    return { ok: true };
  }

  deliverUser(message: string, delivery: FabricAgentMessageDelivery): FabricAgentMessageResult {
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    const text = message.trim();
    if (!text) throw new Error("Main agent message must not be empty");
    const messageId = randomUUID();
    this.pi.sendUserMessage(text, { deliverAs: delivery });
    return { queued: true, messageId, routed: "main" };
  }

  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult {
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    const message = request.message.trim();
    if (!message) throw new Error("Main agent message must not be empty");
    const item: HeldAgentMessage = {
      id: randomUUID(),
      from: structuredClone(request.from),
      message,
      sentAt: Date.now(),
      ...(request.data === undefined ? {} : { data: serializableData(request.data) }),
    };
    const triggerTurn = request.triggerTurn ?? true;
    // Pi releases its own followUp queue only when Main has no more work, so a Main that
    // chains turns reads it an hour late (smarty-dev#1495). Fabric holds a triggering
    // followUp for a busy Main instead: turn_end flushes the due ones as one steer, and
    // agent_settled releases the rest as a followUp. A non-triggering one never waited.
    if (request.delivery === "followUp" && triggerTurn && this.#drainActive()) {
      this.#held.push(item);
      if (this.#context!.isIdle()) this.#release(true);
      else this.#arm();
    } else {
      this.#send([item], request.delivery, triggerTurn, false);
    }
    return { queued: true, messageId: item.id, routed: "main", ...this.queueDepth() };
  }

  /** The followUps Fabric still holds for this Main, and the age of the oldest. */
  queueDepth(): FabricFollowUpQueueDepth {
    // ponytail: counts only what Fabric holds. With the drain off, followUps sit in Pi's own
    // queue, which exposes no depth to extensions, and this reports 0.
    const oldest = this.#held[0];
    return {
      pendingFollowUps: this.#held.length,
      oldestAgeS: oldest ? Math.max(0, Math.floor((Date.now() - oldest.sentAt) / 1000)) : 0,
    };
  }

  /**
   * Hold followUps for this local Main while it is busy, and flush those that have waited
   * flushMs at the next boundary between tool calls (turn_end, the hook the shell and
   * completion inboxes use). flushMs 0 keeps Pi's followUp queue, as before.
   */
  attachFollowUpDrain(context: ExtensionContext, flushMs: number): void {
    this.closeFollowUpDrain();
    if (!this.local || !(flushMs > 0) || typeof this.pi.on !== "function") return;
    this.#context = context;
    this.#flushMs = flushMs;
    this.#closed = false;
    const on = (name: string, fn: (event: any, ctx: ExtensionContext) => unknown): void => {
      const off = (this.pi.on as (name: string, fn: (event: any, ctx: ExtensionContext) => unknown) => unknown)(name, fn);
      if (typeof off === "function") this.#unsubscribe.push(off as () => void);
    };
    on("turn_end", (event: { message?: { stopReason?: string } }, ctx) => {
      this.#context = ctx;
      if (ctx.signal?.aborted || ["aborted", "error"].includes(event.message?.stopReason ?? "")) {
        this.#suspended = true;
        return;
      }
      this.#flushDue();
    });
    // Main is about to go idle. Hand the held followUps to Pi's followUp queue here, the last
    // boundary where Pi still continues the run for a queued message and where Pi itself drops
    // that continuation when the user cancels (review/astra F1 on pi-fabric#102).
    on("agent_before_settle", (event: { outcome?: string }, ctx) => {
      this.#context = ctx;
      if (this.#suspended || (event.outcome !== undefined && event.outcome !== "completed")) return;
      this.#release(true);
    });
    // Only followUps that arrived after that boundary are left. Start a run for them only on a
    // settle Pi reports as completed: a cancel after the last turn sets no aborted turn_end, and
    // a Pi that reports no outcome cannot rule one out, so they are appended for the next run.
    on("agent_settled", (event: { outcome?: string }, ctx) => {
      this.#context = ctx;
      const completed = !this.#suspended && event.outcome === "completed";
      this.#suspended = false;
      this.#release(completed);
    });
    on("agent_start", (_event, ctx) => { this.#context = ctx; this.#suspended = false; });
    on("input", (_event, ctx) => { this.#context = ctx; this.#suspended = false; });
  }

  /** Stop holding; any held followUps go to Pi's own followUp queue, as before the drain. */
  closeFollowUpDrain(): void {
    for (const off of this.#unsubscribe.splice(0)) off();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    const held = this.#held.splice(0);
    this.#closed = true;
    this.#context = undefined;
    for (const item of held) {
      try { this.#send([item], "followUp", true, false); } catch {
        // A session that is shutting down has no queue left to take it.
      }
    }
  }

  #drainActive(): boolean {
    return !this.#closed && this.#context !== undefined &&
      (this.#held.length > 0 || this.#context.isIdle() === false);
  }

  #flushDue(): void {
    if (!this.#held.length || this.#suspended) return;
    const now = Date.now();
    let due = 0;
    while (due < this.#held.length && now - this.#held[due]!.sentAt >= this.#flushMs) due++;
    if (!due) return;
    // One message, queued behind any steer already in Pi's queue: it never overtakes one.
    this.#send(this.#held.splice(0, due), "steer", true, true);
  }

  #release(triggerTurn: boolean): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    if (!this.#held.length) return;
    this.#send(this.#held.splice(0), "followUp", triggerTurn, false);
  }

  #arm(): void {
    if (this.#timer || !this.#held.length) return;
    // A fallback for a Main busy without turns (compaction) or a missed settle.
    const wait = Math.max(0, this.#held[0]!.sentAt + this.#flushMs - Date.now());
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#closed) return;
      if (this.#context?.isIdle()) this.#release(!this.#suspended);
      else this.#arm();
    }, wait || this.#flushMs);
    this.#timer.unref?.();
  }

  #send(
    items: HeldAgentMessage[],
    deliverAs: FabricAgentMessageDelivery,
    triggerTurn: boolean,
    flushed: boolean,
  ): void {
    // Each item keeps its own delivery mark: a flushed batch goes in as a steer, but it holds followUps.
    const delivery: FabricAgentMessageDelivery = flushed ? "followUp" : deliverAs;
    const blocks = items.map((item) => [
      `<fabric-agent-message from_name=${JSON.stringify(item.from.name)} from_id=${JSON.stringify(item.from.id)} from_kind=${JSON.stringify(item.from.kind)} delivery="${delivery}" sent_at="${new Date(item.sentAt).toISOString()}">`,
      escapeXmlText(item.message),
      item.data === undefined ? undefined : `<data>${escapeXmlText(JSON.stringify(item.data))}</data>`,
      "</fabric-agent-message>",
    ].filter((line): line is string => Boolean(line)).join("\n"));
    const itemDetails = (item: HeldAgentMessage) => ({
      id: item.id,
      from: item.from,
      delivery,
      sentAt: new Date(item.sentAt).toISOString(),
      ...(item.data === undefined ? {} : { data: item.data }),
    });
    const first = items[0]!;
    this.pi.sendMessage(
      {
        customType: "pi-fabric-agent-message",
        content: [
          ...(flushed
            ? [`${items.length} follow-up message(s) sent while you were busy, delivered at a tool boundary after waiting ${Math.round(this.#flushMs / 1000)} s or more. Oldest first; each is a followUp, not a steer.`]
            : []),
          ...blocks,
        ].join("\n\n"),
        display: true,
        details: {
          ...itemDetails(first),
          ...(items.length > 1 ? { id: randomUUID(), items: items.map(itemDetails) } : {}),
          triggerTurn,
          ...(flushed ? { flushed: true } : {}),
        },
      },
      { deliverAs, triggerTurn },
    );
  }
}
