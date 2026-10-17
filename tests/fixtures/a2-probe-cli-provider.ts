import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import fs from "node:fs";
import path from "node:path";

const model = {
  provider: "a2probe", id: "offline", name: "A2 probe", api: "a2probe-api", baseUrl: "http://invalid.local",
  reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 200_000, maxTokens: 4096,
};
const textOf = (message: any): string => message?.content?.filter((part: any) => part.type === "text").map((part: any) => part.text).join("") ?? "";

// Test-only host adapter: no mesh writes, no private router access, and no fault during refresh.
// Restore BEFORE throwing so a pre-fix router can recover; failing to hit the boundary fails the probe.
export function armLateDirectoryReadFault(root: string, onInject?: () => void) {
  const original = fs.statSync;
  let injections = 0;
  const restore = () => { fs.statSync = original; };
  fs.statSync = ((file: any, ...args: any[]) => {
    const stack = new Error().stack ?? "";
    if (String(file) === path.join(root, "state.json") && stack.includes("#directoryRead") && stack.includes("#readCachedState")) {
      injections++;
      restore();
      onInject?.();
      throw Object.assign(new Error("A2PROBE one-shot late directory read"), { code: "EIO" });
    }
    return (original as any)(file, ...args);
  }) as typeof fs.statSync;
  return { restore, get injections() { return injections; } };
}

export default function register(pi: any): void {
  const stream = (selected: any, context: any) => {
    const events = createAssistantMessageEventStream();
    const prompt = textOf(context.messages.at(-1));
    const spec = context.messages.at(-1)?.role === "user" && prompt.startsWith("A2PROBE:")
      ? JSON.parse(prompt.slice("A2PROBE:".length)) : undefined;
    if (spec?.lateReadFault) {
      const root = process.env.PI_FABRIC_MESH_ROOT;
      if (!root) throw new Error("A2 fault needs the private mesh root");
      armLateDirectoryReadFault(root, () => process.stderr.write("A2FAULT one-shot late directory stat; restored before throw\n"));
    }
    const call = {
      type: "toolCall" as const, id: "a2probe-" + Date.now(), name: "fabric_exec",
      arguments: { code: spec?.code, resultFormat: "json", display: { name: "A2 router probe", description: "Public Fabric A2 post-install acceptance" } },
    };
    const message: any = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: spec ? [{ type: "text", text: "" }, call] : [{ type: "text", text: "A2PROBE complete" }], stopReason: spec ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    events.push({ type: "start", partial: message });
    if (spec) events.push({ type: "toolcall_end", contentIndex: 1, toolCall: call, partial: message });
    events.push({ type: "done", reason: message.stopReason, message });
    events.end();
    return events;
  };
  pi.registerProvider({
    id: "a2probe", name: "A2 probe",
    auth: { apiKey: { name: "Keyless", check: async () => ({ type: "api_key", source: "keyless" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
}
