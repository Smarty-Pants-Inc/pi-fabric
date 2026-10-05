#!/usr/bin/env node
// A real RPC-protocol fake pi for the steering e2e. Unlike fake-pi-rpc.mjs (which
// processes one prompt and exits), this stays alive after the prompt so the
// Fabric worker's steer.jsonl poller can forward steer/follow_up/queue-mode
// commands to it between turns. It records every command it receives to the
// file named by FAKE_PI_STEER_LOG so tests can assert the worker forwarded them,
// and emits queue_update events so the worker surfaces pendingMessages.
import fs from "node:fs";
import readline from "node:readline";

const send = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const recordPath = process.env.FAKE_PI_STEER_LOG;
const record = (entry) => {
  if (recordPath) fs.appendFileSync(recordPath, JSON.stringify(entry) + "\n");
};

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
    if (command.type === "get_state" || command.type === "get_messages") {
      const data = command.type === "get_state"
        ? { model: { provider: "fake", id: "fake-model", contextWindow: 128_000 }, thinkingLevel: "off", isStreaming: false, isCompacting: false }
        : { messages: [] };
      send({ type: "response", id: command.id, command: command.type, success: true, data });
      record({ type: command.type, id: command.id });
    } else if (command.type === "prompt" && !prompted) {
      prompted = true;
      send({ type: "response", command: "prompt", success: true });
      send({ type: "agent_start" });
      send({ type: "queue_update", steering: [], followUp: [] });
      record({ type: "prompt", message: command.message });
      setTimeout(() => send({ type: "agent_settled" }), 700);
    } else if (command.type === "steer") {
      send({ type: "response", command: "steer", success: true });
      send({ type: "queue_update", steering: [command.message], followUp: [] });
      record({ type: "steer", message: command.message });
    } else if (command.type === "follow_up") {
      send({ type: "response", command: "follow_up", success: true });
      send({ type: "queue_update", steering: [], followUp: [command.message] });
      record({ type: "follow_up", message: command.message });
    } else if (command.type === "set_steering_mode") {
      send({ type: "response", command: "set_steering_mode", success: true });
      record({ type: "set_steering_mode", mode: command.mode });
    } else if (command.type === "set_follow_up_mode") {
      send({ type: "response", command: "set_follow_up_mode", success: true });
      record({ type: "set_follow_up_mode", mode: command.mode });
    } else if (command.type === "compact") {
      record({
        type: "compact",
        id: command.id,
        ...(typeof command.customInstructions === "string"
          ? { customInstructions: command.customInstructions }
          : {}),
      });
      send({ type: "compaction_start", reason: "manual" });
      send({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "compacted", firstKeptEntryId: "entry-1", tokensBefore: 100 },
        aborted: false,
        willRetry: false,
      });
      send({ type: "response", id: command.id, command: "compact", success: true });
    }
});
process.stdin.on("end", () => setTimeout(() => process.exit(0), 5));
process.stdin.resume();
