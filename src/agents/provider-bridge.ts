import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Legacy explicit bridge target. Workers no longer set or inject it. */
export const PROVIDER_BRIDGE_MODEL_ENV = "PI_FABRIC_EXTENSION_MODEL";

export interface ResolveExtensionProvidersOptions {
  model: string;
  cwd: string;
  agentDir: string;
}

/** Provider discovery cannot authorize executable extension code. */
export class ProviderNotFoundError extends Error {
  readonly code = "PROVIDER_NOT_FOUND";
  constructor(readonly model: string, readonly provider: string) {
    super(`Model "${model}" needs provider "${provider}", which is not built into Pi or defined in models.json. ` +
      "Disabled extensions cannot run provider registration code. Define the model in models.json, or explicitly run with extensions: true.");
    this.name = "ProviderNotFoundError";
  }
}

type PiSdk = typeof import("@earendil-works/pi-coding-agent");

/** Split a canonical `provider/id` key at its first slash. */
export function parseModelKey(key: string): { provider: string; id: string } | undefined {
  const value = key.trim();
  const slash = value.indexOf("/");
  if (slash <= 0 || slash === value.length - 1) return undefined;
  return { provider: value.slice(0, slash), id: value.slice(slash + 1) };
}

/**
 * An explicitly loaded legacy bridge may only check Pi's declarative model
 * registry. Path names, project trust, and installed/enabled extensions are
 * not permission to execute registration functions in a disabled-extension
 * child. Known models need no bridge; all others fail without loading code.
 */
export async function resolveExtensionProviders(
  options: ResolveExtensionProvidersOptions,
  sdk?: PiSdk,
): Promise<undefined> {
  const key = parseModelKey(options.model);
  if (!key) return undefined;
  const pi = sdk ?? await import("@earendil-works/pi-coding-agent");
  const models = await pi.ModelRuntime.create({
    authPath: path.join(options.agentDir, "auth.json"),
    modelsPath: path.join(options.agentDir, "models.json"),
    refreshOnCreate: false,
  });
  if (models.getModel(key.provider, key.id)) return undefined;
  await models.refresh({ allowNetwork: false, signal: AbortSignal.timeout(15_000) });
  if (models.getModel(key.provider, key.id)) return undefined;

  throw new ProviderNotFoundError(options.model, key.provider);
}

/** Stable legacy -e entry: never executes provider-extension registration. */
export default async function fabricProviderBridge(_pi: ExtensionAPI): Promise<void> {
  const model = process.env[PROVIDER_BRIDGE_MODEL_ENV]?.trim();
  if (!model) return;
  const { getAgentDir } = await import("@earendil-works/pi-coding-agent");
  await resolveExtensionProviders({ model, cwd: process.cwd(), agentDir: getAgentDir() });
}
