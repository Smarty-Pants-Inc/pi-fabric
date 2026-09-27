import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { JevProvider } from "../src/providers/jev-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { JevObservationHost } from "../src/jev/observation.js";
import type { JevLaunch, JevRunInfo } from "../src/jev/types.js";
export const jevContext = (signal?: AbortSignal): FabricInvocationContext => ({
  cwd: process.cwd(), signal, parentToolCallId: "jev-test", nestedToolCallId: "jev-nested",
  extensionContext: { hasUI: false, sessionManager: { getSessionId: () => "jev-test-session", getSessionFile: () => undefined } } as ExtensionContext, update() {},
});
export function setupJev(overrides: Record<string, unknown> = {}, fetcher?: typeof fetch, credentials?: JevCredentials, observationHost?: JevObservationHost) {
  const config = normalizeFabricConfig({
    approvals: { network: "allow", execute: "allow", read: "allow", write: "allow" },
    ...overrides,
  });
  const registry = new ActionRegistry();
  const client = new JevClient(config.jev, fetcher, credentials ?? new JevCredentials([], { TYPESAFE_API_KEY: "test-only-never-a-real-key" }));
  const provider = new JevProvider({ registry, config, observationHost }, client);
  registry.register(provider);
  return { registry, provider, config, client };
}
export const launch = (code: string, change: Partial<JevLaunch["program"]> = {}, input: JevLaunch["input"] = null): JevLaunch => ({
  program: { name: "test-program", code, requires: [], inputSchema: {}, outputSchema: {}, ...change }, input,
});
export const callProgram = async (provider: JevProvider, method: "run" | "spawn", request: JevLaunch, context = jevContext()): Promise<JevRunInfo> =>
  await provider.invoke(method, request as unknown as Record<string, unknown>, context) as JevRunInfo;
