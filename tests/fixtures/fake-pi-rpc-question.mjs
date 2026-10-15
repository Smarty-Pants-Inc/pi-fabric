#!/usr/bin/env node
// RPC fake pi for routed child questions: after the prompt it raises one
// extension_ui_request (FAKE_PI_QUESTION, JSON) and records the worker's
// extension_ui_response to FAKE_PI_QUESTION_LOG before settling.
import fs from "node:fs";
import readline from "node:readline";

const send = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const recordPath = process.env.FAKE_PI_QUESTION_LOG;
const record = (entry) => {
  if (recordPath) fs.appendFileSync(recordPath, JSON.stringify(entry) + "\n");
};
const question = JSON.parse(
  process.env.FAKE_PI_QUESTION ?? '{"method":"select","title":"Pick","options":["A","B"]}',
);

let prompted = false;
const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", (line) => {
  if (!line.trim()) return;
  let command;
  try {
    command = JSON.parse(line);
  } catch {
    return;
  }
  if (command.type === "prompt" && !prompted) {
    prompted = true;
    send({ type: "response", command: "prompt", success: true });
    send({ type: "agent_start" });
    send({ type: "extension_ui_request", id: "ui-1", ...question });
  } else if (command.type === "extension_ui_response") {
    record(command);
    send({
      type: "message_end",
      message: { role: "assistant", content: [{ type: "text", text: `answer ${JSON.stringify(command)}` }] },
    });
    setTimeout(() => send({ type: "agent_settled" }), 50);
  }
});
process.stdin.on("end", () => setTimeout(() => process.exit(0), 5));
process.stdin.resume();
