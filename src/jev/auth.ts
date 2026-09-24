import {
  createAssistantMessageEventStream,
  createProvider,
  envApiKeyAuth,
  type Api,
  type Model,
  type ProviderStreams,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const JEV_NOT_A_CHAT_PROVIDER =
  "Jev (TypeSafe) is a login-only provider for Fabric's safety classifier; it has no chat models.";

// Newer Pi requires every provider to carry at least one api/images/classifiers
// implementation. Jev is auth-only and publishes no models, so this stream is
// unreachable in practice; if anything ever calls it, it ends with an error.
const failWithoutModels = (model: Model<Api>) => {
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
    errorMessage: JEV_NOT_A_CHAT_PROVIDER,
    timestamp: Date.now(),
  };
  stream.push({ type: "error", reason: "error", error });
  stream.end(error);
  return stream;
};

const JEV_LOGIN_ONLY_API: ProviderStreams = {
  stream: (model) => failWithoutModels(model),
  streamSimple: (model) => failWithoutModels(model),
};

/** Auth-only provider: available to /login, never advertised as a chat model. */
export const createJevAuthProvider = () => createProvider({
  id: "jev",
  name: "Jev (TypeSafe System One)",
  baseUrl: "https://api.typesafe.ai/v1",
  auth: { apiKey: envApiKeyAuth("TypeSafe API key", ["TYPESAFE_API_KEY"]) },
  models: [],
  api: JEV_LOGIN_ONLY_API,
});
export function registerJevAuth(pi: ExtensionAPI): void {
  // Keep lightweight test/managed adapters without provider registration usable.
  if (typeof pi.registerProvider === "function") pi.registerProvider(createJevAuthProvider());
}
