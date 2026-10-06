import fs from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@earendil-works/pi-ai";

export default async function (pi: ExtensionAPI) {
  // Pi's jiti maps the package root to its compat facade; resolve this test-only
  // native subpath explicitly rather than extending that root alias.
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } = await import(process.env.DURABLE_TEST_FAUX_MODULE!) as typeof import("@earendil-works/pi-ai/providers/faux");
  const faux = fauxProvider({ provider: "durable-offline", models: [{ id: "test", reasoning: true, input: ["text", "image"] }], tokensPerSecond: 100000 });
  pi.registerProvider({ ...faux.provider, auth: { apiKey: { name: "Offline", resolve: async () => ({ auth: { apiKey: "offline" } }) } } });
  faux.setResponses(Array.from({ length: 30 }, () => context => {
    const last = context.messages.filter(message => message.role === "user" || message.role === "toolResult").at(-1);
    const text = typeof last?.content === "string" ? last.content : (last?.content ?? []).filter(part => part.type === "text").map(part => part.text).join("\n");
    if (last?.role === "toolResult") return fauxAssistantMessage(`result:${text}`);
    if (text === "read fixture") return fauxAssistantMessage([fauxToolCall("read", { path: process.env.DURABLE_TEST_FILE! })]);
    if (text === "write fixture") return fauxAssistantMessage([fauxToolCall("write", { path: process.env.DURABLE_TEST_FILE!, content: "overwritten" })]);
    if (text === "fabric fixture") return fauxAssistantMessage([fauxToolCall("fabric_exec", { code: process.env.DURABLE_TEST_FABRIC_CODE! })]);
    if (text === "hold effect") return fauxAssistantMessage([fauxToolCall("hold_effect", {})]);
    return fauxAssistantMessage(JSON.stringify({ reply: text, images: typeof last?.content === "string" ? 0 : last?.content.filter(part => part.type === "image").length ?? 0 }));
  }));
  pi.registerTool({ name: "hold_effect", label: "Hold effect", description: "Offline crash-safety fixture", parameters: Type.Object({}),
    async execute(_id, _args, signal) {
      fs.appendFileSync(process.env.DURABLE_TEST_EFFECT!, "effect\n");
      await new Promise<void>((resolve) => { if (signal?.aborted) resolve(); else signal?.addEventListener("abort", () => resolve(), { once: true }); });
      return { content: [{ type: "text", text: "stopped" }], details: {} };
    },
  });
}
