// Fake native task entry for tests/worker-steer-ended.test.ts. The test builds a
// throwaway SDK directory whose dist/index.js imports this module, so the real
// worker resolves a "native SDK" and runs the terminal-task path (task-entry,
// tracked controls, IPC final-answer frame) against deterministic input.
//
// It persists final-answer.json on STEER_ENDED_ROOT/final, then withholds the
// fabric_final_answer IPC frame until STEER_ENDED_ROOT/release exists. Any
// delivery it receives after its receipt is a fence failure: it then writes
// the no-source-write sentinel into the workspace (cwd), as a model obeying the
// late request would.
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { randomUUID } from "node:crypto";

const root = process.env.STEER_ENDED_ROOT;
const runDirectory = process.env.PI_FABRIC_AGENT_RUN_DIR;
const runId = process.env.PI_FABRIC_PARENT_RUN;
const controlDirectory = path.resolve(runDirectory) + ".ctl";
const deliveries = process.env.PI_FABRIC_DELIVERY_DIR;
const log = value => fs.appendFileSync(path.join(root, "fake.jsonl"), JSON.stringify({ pid: process.pid, ...value }) + "\n");
const send = event => process.stdout.write(JSON.stringify(event) + "\n");
const model = { provider: "steer-ended", id: "fake" };
// The race proof compiles the production receipt helper into its private root.
const terminal = process.env.STEER_ENDED_HELPER ? await import(process.env.STEER_ENDED_HELPER) : undefined;
let receipt;
let released = false;

const recordReceipt = () => {
  if (terminal) receipt = terminal.saveFinalAnswerReceipt(runDirectory, runId, "AUTHORIZED_FINAL_ANSWER");
  else {
    receipt = { version: 1, id: randomUUID(), runId, recordedAt: Date.now(), text: "AUTHORIZED_FINAL_ANSWER" };
    const temporary = path.join(runDirectory, `.final-answer.${process.pid}.tmp`);
    fs.writeFileSync(temporary, JSON.stringify(receipt));
    fs.renameSync(temporary, path.join(controlDirectory, "final-answer.json"));
  }
  log({ type: "receipt", id: receipt.id });
};
const release = () => {
  released = true;
  const assistant = { usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { total: 0 } },
    model: model.id, provider: model.provider, timestamp: Date.now() };
  process.send?.({ type: "fabric_final_answer", runId, receiptId: receipt.id, assistant,
    totals: { turns: 1, toolCalls: 0, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } } });
  log({ type: "frame", id: receipt.id });
};
const timer = setInterval(() => {
  if (!receipt && fs.existsSync(path.join(root, "final"))) recordReceipt();
  if (receipt && !released && fs.existsSync(path.join(root, "release"))) release();
}, 10);

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", line => {
  let command;
  try { command = JSON.parse(line); } catch { return; }
  if (["get_state", "set_model", "set_thinking_level", "get_available_models"].includes(command.type)) {
    send({ type: "response", id: command.id, command: command.type, success: true,
      data: { model, thinkingLevel: "off", isStreaming: false, isCompacting: false, models: [model] } });
    return;
  }
  if (command.type !== "prompt" && command.type !== "steer" && command.type !== "follow_up") return;
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
    // Counterexample sink: obeying a post-final control is a source write.
    fs.writeFileSync(path.join(process.cwd(), "SOURCE_WRITE_SENTINEL"), envelope.message);
    return;
  }
  // Pre-final continuation: the native context hook marks consumption.
  const file = path.join(controlDirectory, "terminal-controls", id + ".json");
  const control = JSON.parse(fs.readFileSync(file, "utf8"));
  fs.writeFileSync(file, JSON.stringify({ ...control, state: "delivered" }));
  send({ type: "response", id, command: "prompt", success: true });
});
process.stdin.on("end", () => { clearInterval(timer); setTimeout(() => process.exit(0), 5); });
