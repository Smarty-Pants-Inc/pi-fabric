import type { FabricJevConfig } from "../jev/config.js";
import type { JevClient } from "../jev/client.js";
import type { JevRequest, JevResponse } from "../jev/types.js";

/** Resident host snapshot of the same shadow gates. Absent/old config never grants network. */
export interface ShadowRoutePolicy {
  jev: FabricJevConfig;
  networkAllowed: boolean;
  schemaEnforced: boolean;
}

/** Optional resident client: zero imports/credential work at registration or idle. */
export class ShadowRouteOwner {
  readonly #abort = new AbortController();
  readonly #pending = new Set<Promise<unknown>>();
  #client?: Promise<JevClient>;
  constructor(readonly policy: () => ShadowRoutePolicy | undefined) {}

  evaluate(request: JevRequest, signal: AbortSignal): Promise<JevResponse> {
    const policy = this.policy();
    if (this.#abort.signal.aborted || !policy?.jev.enabled || policy.networkAllowed !== true || policy.schemaEnforced !== false) {
      throw new Error("Jev routing unavailable");
    }
    const routeSignal = AbortSignal.any([signal, this.#abort.signal]);
    const pending = (async () => {
      routeSignal.throwIfAborted();
      const client = await (this.#client ??= (async () => {
        const [{ JevClient }, { resolveJevModelRoute }] = await Promise.all([
          import("../jev/client.js"), import("../jev/routes.js"),
        ]);
        return new JevClient(policy.jev, undefined, undefined, resolveJevModelRoute(policy.jev.model).route);
      })());
      routeSignal.throwIfAborted();
      return client.evaluate(request, routeSignal);
    })();
    this.#pending.add(pending);
    void pending.then(() => this.#pending.delete(pending), () => this.#pending.delete(pending));
    return pending;
  }

  async close(): Promise<void> {
    this.#abort.abort();
    await Promise.allSettled([...this.#pending]);
    const client = await this.#client?.catch(() => undefined);
    await client?.drainCredentials();
  }
}
