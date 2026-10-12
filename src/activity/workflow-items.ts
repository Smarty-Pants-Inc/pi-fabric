import {
  FABRIC_WORKFLOW_ITEM_EVENT,
  type FabricWorkflowItemEventV1,
  type FabricWorkflowItemStatusV1,
} from "../protocol.js";

import { isWorkflowItemId, WORKFLOW_ITEM_ID_MAX_CHARS } from "./store.js";

export { isWorkflowItemId, WORKFLOW_ITEM_ID_MAX_CHARS };
export const WORKFLOW_ITEM_META_MAX_BYTES = 2 * 1024;
const MAX_META_DEPTH = 8;
const MAX_LABEL_CHARS = 120;

const statuses = new Set<FabricWorkflowItemStatusV1>([
  "pending",
  "running",
  "completed",
  "failed",
  "blocked",
  "stopped",
]);

const legacyItemId = (value: unknown): string | undefined => {
  if (typeof value !== "string") return undefined;
  const safe = value
    .trim()
    .slice(0, WORKFLOW_ITEM_ID_MAX_CHARS)
    .replace(/[^a-zA-Z0-9._:-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return safe || undefined;
};

const isPlainData = (value: unknown, depth: number): boolean => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "object" || depth >= MAX_META_DEPTH) return false;
  if (Array.isArray(value)) return value.every((entry) => isPlainData(entry, depth + 1));
  const prototype = Object.getPrototypeOf(value) as unknown;
  if (prototype !== Object.prototype && prototype !== null) return false;
  return Object.values(value).every((entry) => isPlainData(entry, depth + 1));
};

export interface WorkflowItemTransitionInput {
  id: string;
  status: FabricWorkflowItemStatusV1;
  meta?: Record<string, unknown>;
}

/**
 * Fail-closed validation for the stable-identity fields of one
 * `workflow.item` call. The guest input is never mutated: the durable trace
 * keeps projecting the original arguments, so `meta` never reaches it.
 */
export const validateWorkflowItemInput = (
  args: Record<string, unknown>,
  defaultId: () => string,
): WorkflowItemTransitionInput => {
  // Ids outside the stable grammar keep the activity store's historical
  // normalization instead of failing programs that predate stable ids.
  const id = isWorkflowItemId(args.id) ? args.id : legacyItemId(args.id) ?? defaultId();
  const status = args.status ?? "running";
  if (!statuses.has(status as FabricWorkflowItemStatusV1)) {
    throw new Error(`workflow.item status must be one of ${[...statuses].join(", ")}`);
  }
  if (args.meta === undefined) return { id, status: status as FabricWorkflowItemStatusV1 };
  const meta = args.meta;
  if (
    typeof meta !== "object" ||
    meta === null ||
    Array.isArray(meta) ||
    !isPlainData(meta, 0)
  ) {
    throw new Error("workflow.item meta must be a plain JSON object");
  }
  const serialized = JSON.stringify(meta);
  if (Buffer.byteLength(serialized, "utf8") > WORKFLOW_ITEM_META_MAX_BYTES) {
    throw new Error(
      `workflow.item meta must serialize to at most ${WORKFLOW_ITEM_META_MAX_BYTES} bytes`,
    );
  }
  return {
    id,
    status: status as FabricWorkflowItemStatusV1,
    meta: JSON.parse(serialized) as Record<string, unknown>,
  };
};

type EmitEvent = (channel: string, data: unknown) => void;

/**
 * Per-invocation item status ledger. Emits one host event per status
 * transition; listeners observe and can never block or fail the program.
 */
export class FabricWorkflowItemTransitions {
  readonly #statuses = new Map<string, FabricWorkflowItemStatusV1>();
  readonly #labels = new Map<string, string>();

  constructor(
    readonly invocationId: string,
    readonly emit: EmitEvent | undefined,
    readonly sessionId?: string,
  ) {}

  /** Matches the activity store's historical `item-<n>` fallback. */
  nextDefaultId(): string {
    return `item-${this.#statuses.size + 1}`;
  }

  record(input: WorkflowItemTransitionInput, label: unknown): void {
    const from = this.#statuses.get(input.id);
    this.#statuses.set(input.id, input.status);
    if (typeof label === "string" && label.trim()) {
      this.#labels.set(input.id, label.trim().slice(0, MAX_LABEL_CHARS));
    }
    if (from === input.status) return;
    this.#send(input.id, from, input.status, input.meta);
  }

  /** Mirrors the activity store: only running items settle with the run. */
  finish(success: boolean): void {
    const to: FabricWorkflowItemStatusV1 = success ? "completed" : "failed";
    for (const [id, status] of this.#statuses) {
      if (status !== "running") continue;
      this.#statuses.set(id, to);
      this.#send(id, status, to);
    }
  }

  #send(
    itemId: string,
    from: FabricWorkflowItemStatusV1 | undefined,
    to: FabricWorkflowItemStatusV1,
    meta?: Record<string, unknown>,
  ): void {
    if (!this.emit) return;
    const label = this.#labels.get(itemId);
    const event: FabricWorkflowItemEventV1 = {
      version: 1,
      invocationId: this.invocationId,
      ...(this.sessionId ? { sessionId: this.sessionId } : {}),
      itemId,
      ...(label ? { label } : {}),
      ...(from ? { from } : {}),
      to,
      at: Date.now(),
      ...(meta ? { meta } : {}),
    };
    try {
      this.emit(FABRIC_WORKFLOW_ITEM_EVENT, event);
    } catch (error) {
      console.warn(
        `[pi-fabric] workflow item event listener failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
}
