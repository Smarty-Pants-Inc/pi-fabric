import fs from "node:fs";
import readline from "node:readline";

const argv = process.argv.slice(2);
const environmentKeys = [
  "PI_CODING_AGENT_DIR",
  "PI_FABRIC_FULL_CODE_MODE",
  "PI_FABRIC_TOOL_ALLOWLIST",
  "PI_FABRIC_PINNED_EXTENSION",
  "PI_FABRIC_EXTENSION_MODEL",
  "PI_FABRIC_PI_BINARY",
];
const environment = Object.fromEntries(environmentKeys.map(name => [name, process.env[name] ?? null]));
fs.writeFileSync(process.env.FABRIC_EXTENSION_ARGV_CAPTURE, JSON.stringify({ argv, environment }));

const model = argv[argv.indexOf("--model") + 1] ?? "fake/fake-model";
const [provider, modelId] = model.split("/", 2);
const send = event => process.stdout.write(`${JSON.stringify(event)}\n`);
const message = {
  role: "assistant",
  content: [{ type: "text", text: "legacy extension launch snapshot" }],
  provider,
  model: modelId,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  stopReason: "stop",
};
const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", line => {
  if (!line.trim()) return;
  const command = JSON.parse(line);
  if (command.type === "get_state" || command.type === "get_messages") {
    const data = command.type === "get_state"
      ? { model: { provider, id: modelId, contextWindow: 128_000 }, isStreaming: false, isCompacting: false }
      : { messages: [] };
    send({ type: "response", id: command.id, command: command.type, success: true, data });
    return;
  }
  if (command.type === "set_model" || command.type === "set_thinking_level") {
    send({ type: "response", id: command.id, command: command.type, success: true });
    return;
  }
  if (command.type !== "prompt") return;
  send({ type: "response", id: command.id, command: "prompt", success: true });
  send({ type: "agent_start" });
  send({ type: "message_end", message });
  send({ type: "turn_end", message, toolResults: [] });
  send({ type: "agent_end", messages: [message], willRetry: false });
  send({ type: "agent_settled" });
});
input.on("close", () => setTimeout(() => process.exit(0), 5));
process.stdin.resume();
