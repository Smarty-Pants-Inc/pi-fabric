#!/usr/bin/env node
// Deterministic fake `pi` for canary-runner subprocess tests: no network,
// no credentials, behavior selected by FAKE_PI_MODE.
import fs from "node:fs";

const mode = process.env.FAKE_PI_MODE ?? "ok";
const telemetry = process.env.PREWALK_CANARY_TELEMETRY;
const write = (text) => process.stdout.write(text);
const record = (data) => {
  if (telemetry) fs.appendFileSync(telemetry, JSON.stringify(data) + "\n");
};

if (mode === "ok") {
  write(JSON.stringify({ type: "session", version: 3, id: "fake-session-1", timestamp: new Date().toISOString(), cwd: process.cwd() }) + "\n");
  write(JSON.stringify({ type: "agent_settled" }) + "\n");
  record({ type: "session_start", at: Date.now(), sessionId: "fake-session-1" });
  record({ type: "session_shutdown", at: Date.now() });
  process.exit(0);
}
if (mode === "partial") {
  write(JSON.stringify({ type: "session", version: 3, id: "fake-session-1" }) + "\n");
  write('{"type":"agent_settled","trunc');
  process.exit(0);
}
if (mode === "fail") {
  record({ type: "session_start", at: Date.now(), sessionId: "fake-session-1" });
  process.exit(3);
}
if (mode === "noshutdown") {
  write(JSON.stringify({ type: "session", version: 3, id: "fake-session-1" }) + "\n");
  record({ type: "session_start", at: Date.now(), sessionId: "fake-session-1" });
  process.exit(0);
}
if (mode === "badtelemetry") {
  write(JSON.stringify({ type: "session", version: 3, id: "fake-session-1" }) + "\n");
  if (telemetry) fs.appendFileSync(telemetry, "{not-json\n");
  process.exit(0);
}
// RPC modes: speak the pi RPC protocol on stdin/stdout so runner tests can
// exercise the persistent recovery path without a network or real provider.
// "rpc-hang" simulates a stuck run to exercise the runner's graceful abort.
if (mode.startsWith("rpc-")) {
  const rpc = (message) => write(JSON.stringify(message) + "\n");
  let started = false;
  let keepAlive = null;
  const startRun = () => {
    rpc({ type: "agent_start" });
    rpc({ type: "agent_settled" });
  };
  const handle = (line) => {
    let command;
    try {
      command = JSON.parse(line);
    } catch {
      rpc({ type: "response", command: "parse", success: false, error: "Failed to parse command" });
      return;
    }
    if (command.type === "prompt") {
      rpc({ type: "response", id: command.id, command: "prompt", success: true });
      if (mode === "rpc-hang") {
        // Simulate a stuck run: accepted, started, never settles, ignores EOF.
        started = true;
        record({ type: "session_start", at: Date.now(), sessionId: "fake-rpc-session-1" });
        rpc({ type: "agent_start" });
        keepAlive = setInterval(() => {}, 1000);
        return;
      }
      if (!started) {
        started = true;
        record({ type: "session_start", at: Date.now(), sessionId: "fake-rpc-session-1" });
        startRun();
        if (mode !== "rpc-early-exit") startRun();
        if (mode === "rpc-early-exit") setTimeout(() => process.exit(4), 100);
        if (mode === "rpc-noise") {
          rpc({ type: "response", id: "stray-1", command: "stray", success: true });
          write("this-is-not-json\n");
        }
      }
      return;
    }
    if (command.type === "get_state") {
      if (mode === "rpc-bad-state") {
        rpc({ type: "response", id: command.id, command: "get_state", success: false, error: "state unavailable" });
        return;
      }
      rpc({
        type: "response",
        id: command.id,
        command: "get_state",
        success: true,
        data: {
          model: null,
          thinkingLevel: "medium",
          isStreaming: false,
          isCompacting: false,
          steeringMode: "one-at-a-time",
          followUpMode: "one-at-a-time",
          sessionFile: null,
          sessionId: "fake-rpc-session-1",
          messageCount: 2,
          pendingMessageCount: mode === "rpc-pending" ? 1 : 0,
        },
      });
      return;
    }
    if (command.type === "get_session_stats") {
      rpc({
        type: "response",
        id: command.id,
        command: "get_session_stats",
        success: true,
        data: {
          sessionId: "fake-rpc-session-1",
          tokens: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, total: 2 },
          cost: 0,
        },
      });
      return;
    }
    if (command.type === "get_entries") {
      rpc({ type: "response", id: command.id, command: "get_entries", success: true, data: { entries: [], leafId: null } });
      return;
    }
    if (command.type === "abort") {
      rpc({ type: "response", id: command.id, command: "abort", success: true });
      if (mode === "rpc-hang") {
        record({ type: "session_shutdown", at: Date.now() });
        if (keepAlive) clearInterval(keepAlive);
        process.exit(0);
      }
      return;
    }
    rpc({ type: "response", id: command.id, command: command.type, success: false, error: "unsupported command" });
  };
  let buffer = "";
  process.stdin.on("data", (chunk) => {
    buffer += chunk;
    let newline = buffer.indexOf("\n");
    while (newline >= 0) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.trim()) handle(line);
      newline = buffer.indexOf("\n");
    }
  });
  process.stdin.on("end", () => {
    if (mode === "rpc-hang") return; // stuck run outlives the client's EOF
    record({ type: "session_shutdown", at: Date.now() });
    process.exit(0);
  });
}

if (mode === "hang") {
  setInterval(() => {}, 1000);
}
