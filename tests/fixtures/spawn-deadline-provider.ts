import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// Only inference is synthetic: native Pi, tools, worker, transport and teardown are real.
export default function (pi: ExtensionAPI) {
  const root = process.env.SPAWN_DEADLINE_ROOT!;
  const model = { provider: "provider", id: "session", name: "Offline deadline proof", api: "spawn-deadline-api",
    baseUrl: "http://invalid.local", reasoning: false, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096 };
  let calls = 0;
  const stream = (selected: any) => {
    const events = createAssistantMessageEventStream();
    const first = ++calls === 1;
    const message: any = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api,
      timestamp: Date.now(), content: first
        ? [{ type: "text", text: "Completed checkpoint." }, { type: "toolCall", id: "checkpoint", name: "write",
          arguments: { path: path.join(root, "checkpoint.txt"), content: "preserved work\n" } }]
        : [{ type: "text", text: "Partial deadline report." }],
      stopReason: first ? "toolUse" : "stop",
      usage: { input: 3, output: 4, cacheRead: 0, cacheWrite: 0, totalTokens: 7,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    events.push({ type: "start", partial: message });
    if (first) {
      events.push({ type: "done", reason: "toolUse", message }); events.end();
    } else {
      events.push({ type: "text_delta", contentIndex: 0, delta: "Partial deadline report.", partial: message });
      const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
      fs.writeFileSync(path.join(root, "native-pids.json"), JSON.stringify({ pi: process.pid, descendant: child.pid }));
      const watcher = fs.watch(root, () => {
        if (!fs.existsSync(path.join(root, "release"))) return;
        watcher.close(); child.kill();
        events.push({ type: "done", reason: "stop", message }); events.end();
      });
    }
    return events;
  };
  pi.registerProvider({ id: "provider", name: "Offline deadline proof",
    auth: { apiKey: { name: "Offline", check: async () => ({ type: "api_key", source: "fixture" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model as any], stream, streamSimple: stream });
}
