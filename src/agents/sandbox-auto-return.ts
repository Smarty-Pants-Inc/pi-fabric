import { copyFabricProvenance } from "../fabric-provenance.js";
import { isPlacementRunId } from "../topology/placement-run-id.js";
import type { AgentSpawner } from "./types.js";

const TASK_PREFIX = "FABRIC-SANDBOX-TASK ";
const RESULT_PREFIX = "FABRIC-RESULT ";

export const sandboxTaskMessage = (runId: string, task: string): string =>
  `${TASK_PREFIX}${runId}\n${task}`;

export const sandboxResultPrefix = (runId: string): string => `${RESULT_PREFIX}${runId}\n`;

const AUTO_RETURNS = Symbol.for("pi-fabric.sandbox-auto-returns.v1");
const autoReturns = (): Map<string, SandboxAutoReturn> => {
  const globals = globalThis as unknown as Record<symbol, Map<string, SandboxAutoReturn> | undefined>;
  return globals[AUTO_RETURNS] ??= new Map();
};

export interface SandboxTaskReceipt {
  message: string;
  provenance: unknown;
}

/** Child-side, run-scoped return gate. A task is armed only by its authenticated bound spawner. */
export class SandboxAutoReturn<Result = unknown> {
  #taskReceived = false;
  #assistantText: string | undefined;
  #sent = false;
  #sentResult: Result | undefined;
  #sending: Promise<Result> | undefined;
  #autoAttempted = false;

  constructor(readonly runId: string, readonly spawnerId: string) {}

  receiveTask(receipt: SandboxTaskReceipt): boolean {
    const provenance = copyFabricProvenance(receipt.provenance);
    if (!provenance || provenance.via !== "followUp" || provenance.sender.id !== this.spawnerId ||
      !receipt.message.startsWith(`${TASK_PREFIX}${this.runId}\n`)) return false;
    this.#taskReceived = true;
    this.#assistantText = undefined;
    return true;
  }

  recordAssistantText(text: string): void {
    if (this.#taskReceived) this.#assistantText = text;
  }

  get hasTaskReceipt(): boolean { return this.#taskReceived; }
  get hasSentResult(): boolean { return this.#sent; }

  /** Explicit result followUps retain their existing route; eligible results share one send. */
  sendExplicitResult(
    targetId: string,
    message: string,
    send: () => Promise<Result>,
  ): Promise<{ handled: false } | { handled: true; result: Result }> {
    const prefix = sandboxResultPrefix(this.runId);
    if (!this.#taskReceived || targetId !== this.spawnerId || !message.startsWith(prefix)) {
      return Promise.resolve({ handled: false });
    }
    return this.#sendOnce(send).then(result => ({ handled: true, result }));
  }

  /** Called only at agent_settled. An explicit successful result always wins. */
  async sendAutomaticResult(
    outcome: string,
    send: (message: string) => Promise<Result>,
  ): Promise<Result | undefined> {
    if (!this.#taskReceived || outcome !== "completed" || this.#autoAttempted) return undefined;
    const text = this.#assistantText;
    if (!text?.trim()) return undefined;

    if (this.#sending) {
      try { await this.#sending; } catch { /* an unsuccessful explicit result leaves auto-return eligible */ }
      if (this.#sent) return this.#sentResult;
    }
    if (this.#sent) return this.#sentResult;

    this.#autoAttempted = true;
    return this.#sendOnce(() => send(`${sandboxResultPrefix(this.runId)}${text}`));
  }

  async #sendOnce(send: () => Promise<Result>): Promise<Result> {
    if (this.#sent) return this.#sentResult as Result;
    if (this.#sending) return this.#sending;
    const sending = Promise.resolve().then(send);
    this.#sending = sending;
    try {
      const result = await sending;
      this.#sent = true;
      this.#sentResult = result;
      return result;
    } finally {
      if (this.#sending === sending) this.#sending = undefined;
    }
  }
}

/** Reuse the run-keyed receipt across extension generations; never rebind a run to another spawner. */
export const sandboxAutoReturnForChildRoot = (input: {
  runId: unknown;
  spawner: AgentSpawner | undefined;
  isRoot: boolean;
}): SandboxAutoReturn | undefined => {
  if (!input.isRoot || !isPlacementRunId(input.runId) || input.spawner?.kind !== "main" ||
    !/^session:[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,247}$/u.test(input.spawner.id)) return undefined;
  const states = autoReturns();
  const existing = states.get(input.runId);
  if (existing) return existing.spawnerId === input.spawner.id ? existing : undefined;
  const state = new SandboxAutoReturn(input.runId, input.spawner.id);
  states.set(input.runId, state);
  return state;
};
