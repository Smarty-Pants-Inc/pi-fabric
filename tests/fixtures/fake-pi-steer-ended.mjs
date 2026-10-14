// Deterministic child for the real worker E-steer-final proof. It publishes the
// production final receipt, then holds its IPC frame so the worker must fence
// from native storage rather than a cached stream/status observation.
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const root = process.env.STEER_ENDED_ROOT;
const directory = process.env.PI_FABRIC_AGENT_RUN_DIR;
const deliveries = process.env.PI_FABRIC_DELIVERY_DIR;
const terminal = await import(process.env.STEER_ENDED_HELPER);
const log = value => fs.appendFileSync(path.join(root, "fake.jsonl"), JSON.stringify({ pid: process.pid, ...value }) + "\n");
const send = event => process.stdout.write(JSON.stringify(event) + "\n");
const model = { provider: "steer-ended", id: "fake" };
let receipt;
let released = false;
const timer = setInterval(() => {
  if (!receipt && fs.existsSync(path.join(root, "final"))) {
    receipt = terminal.saveFinalAnswerReceipt(directory, process.env.PI_FABRIC_PARENT_RUN, "AUTHORIZED_FINAL_ANSWER");
    log({ type: "receipt", id: receipt.id });
  }
  if (receipt && !released && fs.existsSync(path.join(root, "release"))) {
    released = true;
    process.send?.({ type: "fabric_final_answer", runId: receipt.runId, receiptId: receipt.id,
      assistant: { ...model, timestamp: Date.now(), usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: { total: 0 } } },
      totals: { turns: 1, toolCalls: 0, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } } });
    log({ type: "frame", id: receipt.id });
  }
}, 10);
readline.createInterface({ input: process.stdin, crlfDelay: Infinity }).on("line", line => {
  let command;
  try { command = JSON.parse(line); } catch { return; }
  if (["get_state", "set_model", "set_thinking_level", "get_available_models"].includes(command.type)) {
    send({ type: "response", id: command.id, command: command.type, success: true,
      data: { model, thinkingLevel: "off", isStreaming: false, isCompacting: false, models: [model] } });
    return;
  }
  if (!["prompt", "steer", "follow_up"].includes(command.type)) return;
  const id = /^\/fabric-delivery ([0-9a-f-]{36})$/.exec(command.message ?? "")?.[1];
  if (!id) {
    log({ type: "task", afterReceipt: Boolean(receipt) });
    send({ type: "response", command: "prompt", success: true });
    send({ type: "agent_start" });
    return;
  }
  const envelope = JSON.parse(fs.readFileSync(path.join(deliveries, id + ".json"), "utf8"));
  log({ type: "delivery", id, message: envelope.message, afterReceipt: Boolean(receipt) });
  if (receipt) {
    fs.writeFileSync(path.join(process.cwd(), "SOURCE_WRITE_SENTINEL"), envelope.message);
    return;
  }
  const file = path.join(directory, "terminal-controls", id + ".json");
  const control = JSON.parse(fs.readFileSync(file, "utf8"));
  fs.writeFileSync(file, JSON.stringify({ ...control, state: "delivered" }));
  send({ type: "response", id, command: "prompt", success: true });
});
process.stdin.on("end", () => { clearInterval(timer); setTimeout(() => process.exit(0), 5); });
