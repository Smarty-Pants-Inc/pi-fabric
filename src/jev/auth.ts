import { createAssistantMessageEventStream, envApiKeyAuth, type Api, type Model, type Provider } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// Retain the fork's error stream for callers that supply a model while using
// Pi 0.99's native auth-only Provider contract (no invented chat API/models).
const noChat = (model?: Model<Api>) => {
  if (!model) throw new Error("Jev supplies typed judgments, not chat generation");
  const stream = createAssistantMessageEventStream();
  const error = {
    role: "assistant" as const, content: [], api: model.api, provider: model.provider, model: model.id,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    stopReason: "error" as const,
    errorMessage: "Jev is a login-only provider for typed judgments, not chat generation.", timestamp: Date.now(),
  };
  stream.push({ type: "error", reason: "error", error });
  stream.end(error);
  return stream;
};

/** Auth-only native provider: /login support without advertising chat models. */
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
