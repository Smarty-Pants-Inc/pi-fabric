#!/usr/bin/env node

import readline from "node:readline";

const send = (event) => {
  process.stdout.write(`${JSON.stringify(event)}\n`);
};

const usage = { input: 3, output: 4, cacheRead: 0, cacheWrite: 0 };

const successMessage = (text) => ({
  role: "assistant",
  content: [{ type: "text", text }],
  provider: "fake",
  model: "fake-model",
  usage,
  stopReason: "stop",
});

const providerFailure = () => ({
  role: "assistant",
  content: [],
  provider: "openai-codex",
  model: "gpt-test",
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  stopReason: "error",
  errorMessage: "fetch failed",
  diagnostics: [
    {
      type: "provider_transport_failure",
      error: { name: "Error", message: "WebSocket error" },
      details: { configuredTransport: "auto", fallbackTransport: "sse" },
    },
  ],
});

const finishAttempt = (message, willRetry) => {
  send({ type: "message_end", message });
  send({ type: "turn_end", message, toolResults: [] });
  send({ type: "agent_end", messages: [message], willRetry });
  send({ type: "agent_settled" });
};

let started = false;
const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", (line) => {
  if (started || !line.trim()) return;
  const command = JSON.parse(line);
  // Actors inspect idle native state/history before dispatching their task.
  // A control frame must never consume this fixture's one business prompt.
  if (command.type === "get_state" || command.type === "get_messages") {
    const data = command.type === "get_state"
      ? { model: { provider: "fake", id: "fake-model", contextWindow: 128_000 }, isStreaming: false, isCompacting: false }
      : { messages: [] };
    send({ type: "response", id: command.id, command: command.type, success: true, data });
    return;
  }
  if (command.type !== "prompt") return;
  started = true;
  const task = typeof command.message === "string" ? command.message : "";

  send({ type: "response", command: "prompt", success: true });
  send({ type: "agent_start" });

  if (task.includes("RETRY_THEN_SUCCEED")) {
    finishAttempt(providerFailure(), true);
    setTimeout(() => {
      send({ type: "agent_start" });
      finishAttempt(successMessage("retry recovered"), false);
    }, 25);
    return;
  }

  if (task.includes("FAIL_PROVIDER")) {
    finishAttempt(providerFailure(), false);
    return;
  }

  if (task.includes("REPORT_FABRIC_IDENTITY")) {
    finishAttempt(
      successMessage(
        JSON.stringify({
          mainAgentId: process.env.PI_FABRIC_MAIN_AGENT_ID,
          parentRun: process.env.PI_FABRIC_PARENT_RUN,
          agentName: process.env.PI_FABRIC_AGENT_NAME,
        }),
      ),
      false,
    );
    return;
  }

  if (task.includes("REPORT_FLEET_ROLE")) {
    finishAttempt(
      successMessage(
        JSON.stringify({
          role: process.env.SMARTY_ROLE ?? null,
          actorName: process.env.PI_FABRIC_ACTOR_NAME ?? null,
          fabricRole: process.env.PI_FABRIC_ROLE ?? null,
        }),
      ),
      false,
    );
    return;
  }

  const value = {
    action: "message",
    message: `validated actor response:${process.env.PI_FABRIC_FULL_CODE_MODE ?? "missing"}`,
  };
  finishAttempt(successMessage(JSON.stringify(value)), false);
});

process.stdin.on("end", () => {
  setTimeout(() => process.exit(0), 5);
});
process.stdin.resume();
