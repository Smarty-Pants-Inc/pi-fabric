import { randomUUID } from "node:crypto";
import { throwIfAborted, settleWithin } from "../async-settlement.js";
import { providerTake, providerRevoke, type ProviderTicket, type StateText } from "../verified/generated/provider-kernel.js";
import type { FabricInvocationContext, FabricScopedProviderResult } from "../protocol.js";
import type { FabricProviderBinding, FabricProviderBindings } from "./provider-bindings.js";

const encode = (text: string): StateText => {
  let result: StateText = { $: "Nil" };
  for (let i = text.length - 1; i >= 0; i--) result = { $: "Con", head: BigInt(text.charCodeAt(i)), tail: result };
  return result;
};
const decode = (text: StateText): string => {
  const result: string[] = [];
  for (let rest = text; rest.$ === "Con"; rest = rest.tail) {
    if (rest.head < 0n || rest.head > 65535n) throw new Error("Invalid provider plan text");
    result.push(String.fromCharCode(Number(rest.head)));
  }
  return result.join("");
};

export interface ProviderOperation {
  binding: FabricProviderBinding;
  ref: string;
  action: string;
  mode: "invoke" | "acquire" | "speculate" | "replay";
  descriptor: string;
  args: Record<string, unknown>;
  context: FabricInvocationContext;
  replayValue?: unknown;
  /** Synchronously transfer disposal to a trusted owner inverse stack. */
  adopt?: (dispose: () => Promise<void>) => void;
  observe(context: FabricInvocationContext): Promise<{ ref: string; descriptor: string }>;
}

/** A host effect interpreter, not a proof of arbitrary provider implementations.
 * The pure kernel selects one exact private payload slot and consumes its ticket. */
export class ProviderOperations {
  readonly #scopes = new Set<() => Promise<void>>();
  constructor(private readonly bindings: FabricProviderBindings) {}

  prepare(input: ProviderOperation): (signal?: AbortSignal) => Promise<unknown> {
    const { binding, ref, mode } = input;
    const payload = randomUUID();
    // Large tool arguments stay in a private slot, not millions of Bend cells.
    // Snapshot/slot fidelity remains an explicit host-adapter assumption.
    const args = structuredClone(input.args);
    const replay = mode === "replay" ? structuredClone(input.replayValue) : undefined;
    const identity = (target: string) => JSON.stringify([binding.id, String(binding.generation), mode, target]);
    let ticket: ProviderTicket = {
      $: "Ticket", grant: { $: "Grant", active: true, key: encode(identity(ref)) },
      expected: encode(input.descriptor), payload: encode(payload),
    };
    return signal => {
      const attempt = ticket;
      ticket = providerRevoke(ticket); // Reserve before awaits, even for failed attempts.
      return this.bindings.track(binding.id, async () => {
        const context = {
          ...input.context,
          signal: AbortSignal.any([...(input.context.signal ? [input.context.signal] : []), ...(signal ? [signal] : [])]),
        };
        throwIfAborted(context.signal);
        const observed = await input.observe(context);
        const result = providerTake(attempt, encode(identity(observed.ref)), encode(observed.descriptor),
          context.signal.aborted || !this.bindings.binding(binding.id));
        if (result.plan.$ !== "Write") {
          throw new Error("Fabric provider operation denied: revoked, stale descriptor, wrong target, or spent ticket");
        }
        const [id, generation, operation, target] = JSON.parse(decode(result.plan.key)) as string[];
        if (decode(result.plan.value) !== payload || id !== binding.id || generation !== String(binding.generation) || operation !== mode || target !== ref) {
          throw new Error("Invalid provider execution plan");
        }
        const separator = target.indexOf(".");
        const action = target.slice(separator + 1);
        if (target.slice(0, separator) !== binding.name || action !== input.action) {
          throw new Error("Provider descriptor changed the requested action");
        }
        if (mode === "replay") return replay;
        if (mode !== "acquire") return binding.provider.invoke(action, args, context);
        const acquired = await binding.provider.acquire!(action, args, context);
        let cleanup: () => void | Promise<void>;
        try {
          const callback = acquired?.dispose;
          if (typeof callback !== "function") throw new Error("Scoped acquisition did not return a disposer");
          cleanup = () => callback.call(acquired);
        } catch (error) {
          this.bindings.quarantine(id, error);
          throw error;
        }
        const release = this.bindings.retain([id], true);
        let disposal: Promise<void> | undefined;
        const onAbort = () => { void dispose().catch(() => undefined); };
        const dispose = (): Promise<void> => {
          disposal ??= Promise.resolve().then(async () => {
            const end = this.bindings.beginInvocation(id, true);
            try { await cleanup(); }
            catch (error) { this.bindings.quarantine(id, error); throw error; }
            finally {
              this.#scopes.delete(dispose);
              context.signal.removeEventListener("abort", onAbort);
              await end();
              await release();
            }
          });
          return disposal;
        };
        this.#scopes.add(dispose);
        try {
          if (input.adopt) input.adopt(dispose);
          else context.signal.addEventListener("abort", onAbort, { once: true });
          if (context.signal.aborted || !this.bindings.binding(id)) {
            throwIfAborted(context.signal);
            throw new Error("Fabric provider authority revoked during acquisition");
          }
          return { value: acquired.value, dispose } satisfies FabricScopedProviderResult;
        } catch (error) {
          await dispose();
          throw error;
        }
      });
    };
  }

  async close(): Promise<void> {
    await settleWithin([...this.#scopes].map(dispose => dispose()), 1_000);
  }
}
