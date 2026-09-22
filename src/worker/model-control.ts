type ModelIdentity = { provider: string; id: string };

const object = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;

const identity = (value: unknown): ModelIdentity | undefined => {
  const record = object(value);
  return typeof record?.provider === "string" && record.provider.trim() &&
    typeof record.id === "string" && record.id.trim()
    ? { provider: record.provider, id: record.id }
    : undefined;
};

const key = (model: ModelIdentity): string => `${model.provider}/${model.id}`;

// CLI selection happens before session_start extensions. Reapply it over RPC
// after startup, then read actual state: set_model's response echoes its input
// model even if a model_select extension switched away again.
export class PiModelControl {
  #awaitingStartup = true;
  #expected: ModelIdentity | undefined;
  #pending: { id: string; command: string } | undefined;
  #sequence = 0;
  #failed = false;
  ready = false;

  private readonly runId: string;
  private readonly requested: string | undefined;
  private readonly thinking: string | undefined;
  private readonly activationWindow: boolean;
  private readonly io: {
    send(frame: Record<string, unknown>): void;
    admitted(model?: string, thinking?: string): void;
    observed(model: string): void;
    fail(error: string): void;
  };

  constructor(
    runId: string,
    requested: string | undefined,
    thinking: string | undefined,
    io: PiModelControl["io"],
    activationWindow = false,
  ) {
    // Keep this module executable through Node's native type stripping too;
    // source workers must not rely on transform-only parameter properties.
    this.runId = runId;
    this.requested = requested;
    this.thinking = thinking;
    this.io = io;
    this.activationWindow = activationWindow;
  }

  start(): void {
    if (!this.requested && !this.activationWindow) {
      this.ready = true;
      this.io.admitted();
      return;
    }
    // Pi opens its RPC input only after extension factories and session_start
    // handlers finish. A correlated response, not spawn or startup UI output,
    // marks readiness; the worker's overall timeout bounds the entire handshake.
    this.#send("get_state");
  }

  #resolveModel(): void {
    const requested = this.requested!;
    const separator = requested.indexOf("/");
    if (separator > 0 && separator < requested.length - 1) {
      this.#expected = {
        provider: requested.slice(0, separator),
        id: requested.slice(separator + 1),
      };
      this.#select();
    } else {
      // Standalone AgentManager callers may supply a bare model ID. Resolve
      // it uniquely from the child catalogue, never from its MRU-selected state.
      this.#send("get_available_models");
    }
  }

  #send(command: string, args: Record<string, unknown> = {}): void {
    const id = `fabric-model:${this.runId}:${++this.#sequence}`;
    this.#pending = { id, command };
    this.io.send({ type: command, id, ...args });
  }

  #select(): void {
    this.#send("set_model", { provider: this.#expected!.provider, modelId: this.#expected!.id });
  }

  fail(reason: string): void {
    if (this.#failed) return;
    this.#failed = true;
    this.#pending = undefined;
    this.io.fail(`Fabric model selection failed for ${this.requested ?? "default"}: ${reason}`);
  }

  observe(event: Record<string, unknown>): boolean {
    if (event.type !== "response" || !this.#pending || event.id !== this.#pending.id) return false;
    const command = this.#pending.command;
    this.#pending = undefined;
    if (event.command !== command || event.success !== true) {
      this.fail(`${command}: ${typeof event.error === "string" ? event.error : "invalid or rejected RPC response"}`);
      return true;
    }
    if (this.activationWindow && command === "get_state") {
      const state = object(event.data);
      if (state?.autoCompactionDisabledForProcess !== true || state?.autoCompactionEnabled !== false ||
          state?.isStreaming !== false || state?.isCompacting !== false) {
        this.fail("activation window requires native --no-auto-compaction support and an idle child; task was not sent");
        return true;
      }
    }
    if (this.#awaitingStartup) {
      this.#awaitingStartup = false;
      if (this.requested) this.#resolveModel();
      else {
        this.ready = true;
        this.io.admitted();
      }
    } else if (command === "get_available_models") {
      const models = object(event.data)?.models;
      const matches = Array.isArray(models)
        ? models.map(identity).filter((model): model is ModelIdentity => model?.id === this.requested)
        : [];
      if (matches.length !== 1) {
        this.fail("model ID is unavailable or ambiguous; specify an exact provider/model");
      } else {
        this.#expected = matches[0];
        this.#select();
      }
    } else if (command === "set_model") {
      if (this.thinking) this.#send("set_thinking_level", { level: this.thinking });
      else this.#send("get_state");
    } else if (command === "set_thinking_level") {
      this.#send("get_state");
    } else {
      const state = object(event.data);
      const actual = identity(state?.model);
      if (actual) this.io.observed(key(actual));
      if (!actual || key(actual) !== key(this.#expected!)) {
        this.fail(`requested ${key(this.#expected!)}, but child reports ${actual ? key(actual) : "no model"} after set_model; task was not sent`);
      } else if (state?.isStreaming === true || state?.isCompacting === true) {
        this.fail("child started work before model admission; task was not sent");
      } else {
        this.ready = true;
        this.io.admitted(key(actual), typeof state?.thinkingLevel === "string" ? state.thinkingLevel : undefined);
      }
    }
    return true;
  }

  // Record real attribution, not just the launch label, and stop on drift.
  // Called on the earliest assistant frame as well as completed messages.
  observeAssistant(message: Record<string, unknown>): void {
    if (message.role !== "assistant") return;
    const actual = identity({ provider: message.provider, id: message.model });
    if (actual) this.io.observed(key(actual));
    if (this.#failed) return;
    if (this.requested && !this.ready) {
      this.fail("child emitted an assistant message before model admission");
    } else if (this.#expected && (!actual || key(actual) !== key(this.#expected))) {
      this.fail(`requested ${key(this.#expected)}, but assistant reports ${actual ? key(actual) : "missing model attribution"}; terminating child`);
    }
  }
}
