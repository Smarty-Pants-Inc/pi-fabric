import {
  createAssistantMessageEventStream,
  envApiKeyAuth,
  type Api,
  type Model,
  type Provider,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Invalid chat calls fail locally; the auth-only provider never generates text. */
const noChat = (model: Model<Api>) => {
  if (!model) throw new Error("Jev supplies typed judgments, not chat generation");
  const stream = createAssistantMessageEventStream();
  const error = {
    role: "assistant" as const,
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "error" as const,
    errorMessage: "Jev (TypeSafe) is a login-only provider for Fabric's safety classifier; it has no chat models.",
    timestamp: Date.now(),
  };
  stream.push({ type: "error", reason: "error", error });
  stream.end(error);
  return stream;
};

/** Native auth-only Provider: /login support without inventing an operation API. */
export const createJevAuthProvider = (): Provider => ({
  id: "jev",
  name: "Jev (TypeSafe System One)",
  baseUrl: "https://api.typesafe.ai/v1",
  auth: { apiKey: envApiKeyAuth("TypeSafe API key", ["TYPESAFE_API_KEY"]) },
  getModels: () => [],
  stream: noChat,
  streamSimple: noChat,
});
export function registerJevAuth(pi: ExtensionAPI): void {
  // Keep lightweight test/managed adapters without provider registration usable.
  if (typeof pi.registerProvider === "function") pi.registerProvider(createJevAuthProvider());
}
