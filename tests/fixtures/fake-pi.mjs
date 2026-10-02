#!/usr/bin/env node
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
// A stub `pi` binary for the real-worker e2e. The Fabric worker spawns this as
// the child agent and talks to it over stdin/stdout JSON lines. Behavior is
// selected with the FAKE_PI_BEHAVIOR env var so the e2e can drive the real
// worker.ts + AgentManager.#monitor across child outcomes.
const behavior = process.env.FAKE_PI_BEHAVIOR || "success";
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");

// Drain the prompt the worker writes so its stdin write does not block.
process.stdin.resume();
process.stdin.on("data", () => {});

const capturePrompt = () => {
  process.stdin.removeAllListeners("data");
  let buffer = "";
  process.stdin.on("data", (chunk) => {
    buffer += chunk.toString("utf8");
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    const frame = JSON.parse(buffer.slice(0, newline).replace(/\r$/, ""));
    if (process.env.FAKE_PI_PROMPT_LOG) {
      fs.writeFileSync(process.env.FAKE_PI_PROMPT_LOG, JSON.stringify(frame));
    }
    emit({ type: "message_end", message: { role: "assistant", content: "captured prompt" } });
    emit({ type: "agent_settled" });
    process.exit(0);
  });
};

const runCompactionLifecycle = (fail) => {
  process.stdin.removeAllListeners("data");
  let buffer = "";
  let settled = false;
  process.stdin.on("data", (chunk) => {
    buffer += chunk.toString("utf8");
    while (true) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) break;
      let raw = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (raw.endsWith("\r")) raw = raw.slice(0, -1);
      if (!raw) continue;
      const frame = JSON.parse(raw);
      if (frame.type === "prompt") {
        emit({ type: "agent_start" });
        emit({ type: "message_end", message: { role: "assistant", content: "ready" } });
        setTimeout(() => {
          settled = true;
          emit({ type: "agent_settled" });
        }, 500);
      } else if (frame.type === "compact") {
        emit({
          type: "fake_compact_received",
          requestId: frame.id,
          customInstructions: frame.customInstructions,
          afterSettled: settled,
        });
        emit({ type: "compaction_start", reason: "manual" });
        emit({
          type: "compaction_end",
          reason: "manual",
          result: fail ? null : { summary: "child compacted", firstKeptEntryId: "entry-1", tokensBefore: 100 },
          aborted: false,
          willRetry: false,
          ...(fail ? { errorMessage: "child summary failed" } : {}),
        });
        emit({ type: "response", id: frame.id, command: "compact", success: true });
      }
    }
  });
};

const terminated = () => {
  emit({ type: "agent_start" });
  emit({ type: "message_end", message: {
    role: "assistant", content: [], stopReason: "error", errorMessage: "Error: Terminated",
  } });
};

switch (behavior) {
  case "terminated-hang":
  case "retry-hang": {
    terminated();
    if (behavior === "retry-hang") {
      emit({ type: "agent_end", willRetry: true });
      emit({ type: "agent_settled" });
    }
    // Ignore both EOF and SIGTERM so the real worker must escalate to SIGKILL.
    process.on("SIGTERM", () => {});
    emit({ type: "fake_child_pid", pid: process.pid });
    setInterval(() => {
      emit({ type: "queue_update", steering: [], followUp: [] });
      if (behavior === "retry-hang") emit({ type: "auto_retry_start", errorMessage: "Error: Terminated" });
    }, 1_000);
    break;
  }
  case "retry-exhausted":
  case "retry-exhausted-stubborn":
    terminated();
    emit({ type: "agent_end", willRetry: true });
    emit({ type: "auto_retry_start", errorMessage: "Error: Terminated" });
    emit({ type: "auto_retry_end", success: false, finalError: "Error: Terminated (retries exhausted)" });
    emit({ type: "agent_settled" });
    if (behavior === "retry-exhausted-stubborn") {
      process.on("SIGTERM", () => {});
      setInterval(() => {}, 60_000);
    }
    break;
  case "terminated-restart-exit":
    terminated();
    emit({ type: "agent_start" });
    process.exit(0);
    break;
  case "terminated-silent-recover": {
    terminated();
    emit({ type: "agent_end", willRetry: true });
    emit({ type: "auto_retry_start", errorMessage: "Error: Terminated" });
    emit({ type: "agent_start" });
    emit({ type: "message_start", message: { role: "assistant", content: [], stopReason: "stop" } });
    setTimeout(() => {
      emit({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "silent reasoning recovered" } });
      emit({ type: "message_end", message: { role: "assistant", content: "silent reasoning recovered", stopReason: "stop" } });
      emit({ type: "agent_end", willRetry: false });
      emit({ type: "auto_retry_end", success: true });
      emit({ type: "agent_settled" });
    }, 70_000);
    break;
  }
  case "terminated-stream-recover": {
    terminated();
    emit({ type: "agent_end", willRetry: true });
    emit({ type: "auto_retry_start", errorMessage: "Error: Terminated" });
    emit({ type: "agent_start" });
    let count = 0;
    const timer = setInterval(() => {
      const type = ["text_delta", "thinking_delta", "toolcall_delta"][count % 3];
      // Native Pi harness uses `event`; legacy Pi uses `assistantMessageEvent`.
      emit({ type: "message_update", event: { type, contentIndex: 0, delta: "working" },
        message: { role: "assistant", content: [{ type: "text", text: "working" }] } });
      if (++count < 9) return;
      clearInterval(timer);
      emit({ type: "message_end", message: { role: "assistant", content: "stream recovered", stopReason: "stop" } });
      emit({ type: "agent_end", willRetry: false });
      emit({ type: "auto_retry_end", success: true });
      emit({ type: "agent_settled" });
    }, 8_000);
    break;
  }
  case "terminated-recover":
    terminated();
    emit({ type: "agent_end", willRetry: true });
    emit({ type: "agent_settled" });
    emit({ type: "auto_retry_start", errorMessage: "Error: Terminated" });
    setTimeout(() => {
      emit({ type: "agent_start" });
      emit({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "recovered" } });
      emit({ type: "message_end", message: { role: "assistant", content: "recovered", stopReason: "stop" } });
      emit({ type: "agent_end", willRetry: false });
      emit({ type: "auto_retry_end", success: true });
      emit({ type: "agent_settled" });
    }, 25);
    break;
  case "capture-prompt":
    capturePrompt();
    break;
  case "compact-success":
    runCompactionLifecycle(false);
    break;
  case "compact-failure":
    runCompactionLifecycle(true);
    break;
  case "exit-clean":
    process.exit(0);
  case "exit-error":
    process.exit(1);
  case "kill-worker":
    // Simulate the worker being hard-killed mid-run (OOM / external kill): it dies
    // before it can write a terminal status.
    try {
      process.kill(process.ppid, "SIGKILL");
    } catch {
      /* worker already gone */
    }
    process.exit(0);
  case "reject":
    emit({ type: "response", command: "prompt", success: false, error: "provider rejected the prompt" });
    process.exit(1);
  case "hang":
    // Never exit and never settle; the worker timeout should fire.
    setInterval(() => {}, 60_000);
    break;
  case "split-utf8": {
    const line = Buffer.from(
      JSON.stringify({ type: "message_end", message: { role: "assistant", content: "界面 🚀" } }) + "\n",
    );
    const split = line.indexOf(Buffer.from("界")) + 1;
    process.stdout.write(line.subarray(0, split));
    setTimeout(() => {
      process.stdout.write(line.subarray(split));
      emit({ type: "agent_settled" });
      process.exit(0);
    }, 10);
    break;
  }
  case "large-lifecycle":
  case "large-lifecycle-retry": {
    // Each message is below the worker cap; aggregate history is well above it.
    // Include both images and ordinary text so this cannot pass via base64 regex
    // redaction. Write fragmented records with backpressure, including metadata
    // after the large field and a following event in the same write.
    const retry = behavior === "large-lifecycle-retry";
    const history = Array.from({ length: 8 }, (_, index) => ({
      role: "toolResult",
      content: [
        { type: "image", data: "a".repeat(512 * 1024), mimeType: "image/png" },
        { type: "text", text: `image ${index}: 界 🚀 \\ \" { }\n` + "ordinary text ".repeat(8192) },
      ],
    }));
    const write = (text) => new Promise((resolve, reject) => {
      process.stdout.write(text, (error) => error ? reject(error) : resolve());
    });
    const fragmented = async (event, following = "") => {
      const line = JSON.stringify(event) + "\r\n" + following;
      for (let i = 0; i < line.length; i += 8191) await write(line.slice(i, i + 8191));
    };
    emit({ type: "agent_start" });
    emit({ type: "tool_execution_start", toolName: "fabric_exec", toolCallId: "image-1" });
    emit({ type: "tool_execution_end", toolName: "fabric_exec", toolCallId: "image-1", result: { content: "saved screenshot" }, isError: false });
    const message = { role: "assistant", content: "progress preserved", usage: { input: 100, output: 50 } };
    emit({ type: "message_end", message });
    await fragmented({ type: "turn_end", message, toolResults: history, turnIndex: 1 });
    await fragmented({ type: "agent_end", messages: history, willRetry: retry }, '{"type":"agent_settled"}\n');
    if (retry) {
      // If willRetry after history was lost, the premature settled event closes
      // stdin and ends the run before the authoritative final response arrives.
      let ended = false;
      process.stdin.on("end", () => { ended = true; });
      await new Promise((resolve) => setTimeout(resolve, 100));
      if (!ended) {
        emit({ type: "agent_start" });
        emit({ type: "message_end", message: { role: "assistant", content: "retry completed", usage: { input: 200, output: 75 } } });
        await fragmented({ messages: history, type: "agent_end", willRetry: false }, '{"type":"agent_settled"}\n');
      }
    }
    process.stdin.pause();
    break;
  }
  case "image-tool-result": {
    // smarty-dev#1907: a child `read` of a PNG returns base64 far above the event-line cap.
    const content = [
      { type: "text", text: "Read image file [image/png]" },
      { type: "image", data: "A".repeat(5 * 1024 * 1024 + 4), mimeType: "image/png" },
    ];
    emit({ type: "agent_start" });
    emit({ type: "tool_execution_start", toolName: "read", toolCallId: "read-1", args: { path: "shot.png" } });
    emit({ type: "tool_execution_end", toolName: "read", toolCallId: "read-1", result: { content }, isError: false });
    emit({ type: "message_end", message: { role: "toolResult", toolCallId: "read-1", toolName: "read", content } });
    emit({ type: "message_end", message: { role: "assistant", content: "image reviewed", usage: { input: 10, output: 5 } } });
    emit({ type: "agent_end", messages: [], willRetry: false });
    process.stdout.write(`${JSON.stringify({ type: "agent_settled" })}\n`, () => process.exit(0));
    break;
  }
  case "oversized-final":
  case "oversized-error": {
    // smarty-dev#1907 review: the dropped event is the run's last assistant result.
    emit({ type: "agent_start" });
    emit({ type: "message_end", message: { role: "assistant", content: "progress only", stopReason: "toolUse" } });
    const message = { role: "assistant", content: "x".repeat(4 * 1024 * 1024 + 1_024) };
    if (behavior === "oversized-error") Object.assign(message, { stopReason: "error", errorMessage: "provider failed" });
    emit({ type: "message_end", message });
    emit({ type: "agent_end", messages: [], willRetry: false });
    process.stdout.write(`${JSON.stringify({ type: "agent_settled" })}\n`, () => process.exit(0));
    break;
  }
  case "oversized-event": {
    const event = {
      type: "message_end",
      message: { role: "assistant", content: "x".repeat(4 * 1024 * 1024 + 1_024) },
    };
    emit(event);
    emit({ type: "message_end", message: { role: "assistant", content: "after oversized" } });
    process.stdout.write(`${JSON.stringify({ type: "agent_settled" })}\n`, () => process.exit(0));
    break;
  }
  case "run-tmpdir": {
    const tmpdir = process.env.TMPDIR;
    const scratch = process.platform === "win32"
      ? fs.mkdtempSync(path.join(os.tmpdir(), "ordinary-temp-"))
      : execFileSync("mktemp", [], { encoding: "utf8" }).trim();
    const report = { tmpdir, osTmpdir: os.tmpdir(), tmp: process.env.TMP, temp: process.env.TEMP,
      mode: fs.statSync(tmpdir).mode & 0o777, scratch };
    // Keep concurrent worker runs overlapped; the runner must own separate roots.
    setTimeout(() => {
      emit({ type: "message_end", message: { role: "assistant", content: JSON.stringify(report) } });
      emit({ type: "agent_settled" });
      process.exit(0);
    }, 100);
    break;
  }
  case "fabric-session-env":
    emit({
      type: "message_end",
      message: { role: "assistant", content: process.env.PI_FABRIC_SESSION_ID || "missing" },
    });
    emit({ type: "agent_settled" });
    process.exit(0);
    break;
  case "stderr-framing":
    process.stderr.write(
      JSON.stringify({ type: "message_end", message: { role: "assistant", content: "spoofed" } }),
    );
    emit({ type: "message_end", message: { role: "assistant", content: "trusted" } });
    emit({ type: "agent_settled" });
    process.exit(0);
    break;
  case "usage-flow":
    emit({ type: "agent_start" });
    emit({ type: "message_end", message: { role: "assistant", content: "first", usage: { input: 100, output: 50, cacheRead: 10, cacheWrite: 5, cost: 0.01 } } });
    setTimeout(() => {
      emit({ type: "message_end", message: { role: "assistant", content: "second", usage: { input: 200, output: 100, cacheRead: 20, cacheWrite: 10, cost: 0.02 } } });
      emit({ type: "agent_settled" });
      process.exit(0);
    }, 50);
    break;
  case "shim-env-inheritance": {
    // Reports only presence booleans for the sentinel names listed in
    // FAKE_PI_SENTINEL_VARS — never values — so the e2e can assert that env
    // seeded in the owner process (what the LocalTerm shim injects into the
    // parent pi) reaches the child pi the worker spawns.
    const names = (process.env.FAKE_PI_SENTINEL_VARS || "").split(",").filter(Boolean);
    const report = names
      .map((name) => `${name}=${process.env[name] ? "present" : "absent"}`)
      .join(" ");
    emit({ type: "message_end", message: { role: "assistant", content: report } });
    emit({ type: "agent_settled" });
    process.exit(0);
  }
  case "reply-tool": {
    // smarty-dev#967: the reply-tool hook is loaded and allowed, and its env is set. FAKE_PI_REPLY
    // picks what the model does: call the tool, or reply in text (JSON only, or with prose).
    const argv = process.argv.slice(2);
    const hook = argv.findIndex((arg, index) => arg === "-e" && /reply-tool\.(js|ts)$/.test(argv[index + 1] ?? ""));
    const tools = (argv[argv.indexOf("--tools") + 1] ?? "").split(",");
    if (hook < 0 || !tools.includes("fabric_reply") || !process.env.PI_FABRIC_REPLY_FILE) process.exit(81);
    const mode = process.env.FAKE_PI_REPLY || "tool";
    if (mode === "tool") fs.writeFileSync(process.env.PI_FABRIC_REPLY_FILE, JSON.stringify({ action: "silent" }));
    const text = mode === "tool" ? "" : mode === "json" ? '{"action":"silent"}' : 'Agent progress comment, nothing to steer.\n{"action":"silent"}';
    emit({ type: "message_end", message: { role: "assistant", content: text } });
    emit({ type: "agent_settled" });
    process.exit(0);
  }
  case "success":
  default:
    emit({ type: "message_end", message: { role: "assistant", content: "hi" } });
    emit({ type: "agent_settled" });
    process.exit(0);
}
