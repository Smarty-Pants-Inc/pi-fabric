// Real worker pipes; fake only the SDK AgentSession/native queue. Final receipt,
// ingress fence, delivery consumption and terminal discard all use production
// code bundled by the test. A captured stdin frame is NOT native consumption.
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const root = process.env.STEER_ENDED_ROOT;
const directory = process.env.PI_FABRIC_AGENT_RUN_DIR;
const terminal = await import(process.env.STEER_ENDED_HELPER);
const log = value => fs.appendFileSync(path.join(root, "fake.jsonl"), JSON.stringify({ pid: process.pid, ...value }) + "\n");
const send = event => process.stdout.write(JSON.stringify(event) + "\n");
const model = { provider: "steer-ended", id: "fake" };
const usage = { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: { total: 0 } };
const hooks = new Map();
const commands = new Map();
const listeners = [];
const nativeQueue = [];
const frames = [];
const heldResponses = [];
let receipt;
let released = false;
let ending = false;
let pumping = false;
let finalizing = false;
let controlsReleased = false;

const emit = async event => {
  for (const hook of hooks.get(event.type) ?? []) {
    const result = await hook(event, { isIdle: () => false });
    if (result?.message) event = { ...event, message: result.message };
  }
  for (const listener of listeners) await listener(event);
  return event;
};
const agent = {
  state: { messages: [] },
  subscribe: listener => { listeners.push(listener); },
  hasQueuedMessages: () => nativeQueue.length > 0,
  prompt: async message => { nativeQueue.push(message); },
  continue: async () => {},
  steer: message => { nativeQueue.push(message); },
  followUp: message => { nativeQueue.push(message); },
  finishTurn: async () => ({ action: "end" }),
};
const session = {
  agent,
  clearQueue: () => { nativeQueue.length = 0; },
  // Like the SDK, extension commands execute before ordinary input processing.
  prompt: async text => {
    const match = /^\/([^ ]+) (.*)$/.exec(text);
    if (match && commands.has(match[1])) {
      await commands.get(match[1]).handler(match[2], { isIdle: () => false });
      return;
    }
    log({ type: "task", afterReceipt: Boolean(receipt) });
    send({ type: "agent_start" });
  },
  steer: async text => { agent.steer({ role: "user", content: [{ type: "text", text }], timestamp: Date.now() }); },
  followUp: async text => { agent.followUp({ role: "user", content: [{ type: "text", text }], timestamp: Date.now() }); },
};
terminal.principalDelivery({
  on: (name, handler) => { hooks.set(name, [...(hooks.get(name) ?? []), handler]); },
  registerCommand: (name, command) => { commands.set(name, command); },
  sendUserMessage: (content, options) => {
    const message = { role: "user", content: typeof content === "string" ? [{ type: "text", text: content }] : content, timestamp: Date.now() };
    if (options?.deliverAs === "followUp") agent.followUp(message);
    else agent.steer(message);
  },
});
await emit({ type: "session_start" });
terminal.installTerminalAnswerBoundary(session, directory, process.env.PI_FABRIC_PARENT_RUN, value => {
  receipt = value;
  log({ type: "receipt", id: receipt.id });
});

const drainNative = async id => {
  while (nativeQueue.length) {
    const event = await emit({ type: "message_end", message: nativeQueue.shift() });
    agent.state.messages.push(event.message);
    let messages = [event.message];
    for (const hook of hooks.get("context") ?? []) messages = (await hook({ messages }))?.messages ?? messages;
    for (const message of messages) {
      const text = message.content.find(block => block.type === "text")?.text ?? "";
      log({ type: "delivery", id, message: text, afterReceipt: Boolean(receipt) });
      const sentinel = /^WRITE (SOURCE_WRITE_SENTINEL|PREFINAL_WRITE_SENTINEL)$/.exec(text)?.[1];
      if (sentinel) {
        fs.writeFileSync(path.join(process.cwd(), sentinel), text);
        log({ type: "action", id, sentinel, afterReceipt: Boolean(receipt) });
      }
    }
  }
};
const finish = () => {
  if (!ending || pumping || frames.length || heldResponses.length) return;
  watcher.close();
  setImmediate(() => process.exit(0));
};
const pump = async () => {
  if (pumping) return;
  pumping = true;
  try {
    while (frames.length) {
      const { command, id } = frames[0];
      if (id && fs.existsSync(path.join(root, "hold-controls")) && !controlsReleased && !fs.existsSync(path.join(root, "consume-controls"))) break;
      frames.shift();
      log({ type: "consume", id, afterReceipt: Boolean(receipt) });
      try {
        if (command.type === "prompt") await session.prompt(command.message);
        else if (command.type === "steer") await session.steer(command.message);
        else await session.followUp(command.message);
        await drainNative(id);
        const response = { type: "response", id: command.id, command: command.type, success: true };
        if (id && fs.existsSync(path.join(root, "hold-responses")) && !fs.existsSync(path.join(root, "release-responses"))) {
          heldResponses.push(response);
          log({ type: "response-held", id });
        } else send(response);
      } catch (error) {
        log({ type: "command-error", id, code: error.code, message: error.message, afterReceipt: Boolean(receipt) });
        send({ type: "response", id: command.id, command: command.type, success: false, error: error.message });
      }
      // Observation only: the fixture NEVER writes delivered/refused state.
      if (id) log({ type: "ack-observed", id, state: terminal.readTerminalControl(directory, id)?.state, afterReceipt: Boolean(receipt) });
    }
  } finally { pumping = false; finish(); }
};
const signals = async () => {
  if (!receipt && !finalizing && fs.existsSync(path.join(root, "final"))) {
    finalizing = true;
    const message = { role: "assistant", content: [{ type: "text", text: "AUTHORIZED_FINAL_ANSWER" }], stopReason: "stop",
      ...model, timestamp: Date.now(), usage };
    agent.state.messages.push(message);
    await emit({ type: "message_end", message });
    await agent.finishTurn({ toolResults: [] });
    if (fs.existsSync(path.join(root, "consume-on-final"))) {
      controlsReleased = true;
      await pump();
    }
  }
  if (receipt && !released && fs.existsSync(path.join(root, "release"))) {
    released = true;
    process.send?.({ type: "fabric_final_answer", runId: receipt.runId, receiptId: receipt.id,
      assistant: { ...model, timestamp: Date.now(), usage },
      totals: { turns: 1, toolCalls: 0, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } } });
    log({ type: "frame", id: receipt.id });
  }
  if (fs.existsSync(path.join(root, "release-responses"))) {
    for (const response of heldResponses.splice(0)) {
      send(response);
      log({ type: "response-released", id: response.id, afterReceipt: Boolean(receipt) });
    }
  }
  await pump();
};
// Normal progress is driven by named test events. There is no timer race/poll.
const watcher = fs.watch(root, () => { void signals().catch(error => { log({ type: "fixture-error", message: error.message }); process.exit(1); }); });
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
  log({ type: "captured", id, command: command.type, message: command.message, afterReceipt: Boolean(receipt) });
  frames.push({ command, id });
  void pump();
});
process.stdin.on("end", () => { ending = true; log({ type: "stdin-end", pending: frames.length }); finish(); });
