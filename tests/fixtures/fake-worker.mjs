import fs from "node:fs";
import path from "node:path";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  args.set(process.argv[index].slice(2), process.argv[index + 1]);
}
const statusFile = args.get("status-file");
const taskFile = args.get("task-file");
const logFile = args.get("log-file");
const lifecycleFile = args.get("lifecycle-file");
const sessionFile = args.get("session-file");
const schemaFile = args.get("schema-file");
const imagesFile = args.get("images-file");
const schema = schemaFile ? JSON.parse(fs.readFileSync(schemaFile, "utf8")) : undefined;
const images = imagesFile ? JSON.parse(fs.readFileSync(imagesFile, "utf8")) : [];
const task = fs.readFileSync(taskFile, "utf8");

if (task.includes("HANG")) {
  // Write a non-terminal "running" status so the AgentManager monitor keeps
  // waiting, then stay alive until the transport kills this process (abort/stop).
  fs.mkdirSync(path.dirname(statusFile), { recursive: true });
  fs.writeFileSync(
    statusFile,
    JSON.stringify({
      id: args.get("id"),
      name: args.get("name"),
      task,
      status: "running",
      runner: args.get("runner") ?? "pi",
      transport: args.get("transport"),
      cwd: args.get("cwd"),
      startedAt: Date.now(),
      updatedAt: Date.now(),
      turns: 0,
      toolCalls: 0,
      text: "",
      exitCode: null,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }),
  );
  const stay = () => setTimeout(stay, 1_000);
  stay();
  process.on("SIGTERM", () => process.exit(0));
  process.on("SIGINT", () => process.exit(0));
} else if (task.includes("RESUME_AFTER_STOP") || task.includes("RESUME_AFTER_CRASH")) {
  // Simulates a long participant losing its first attempt mid-run: either the
  // worker caught an external signal (a terminal "stopped" record, as SIGTERM
  // produces) or its transport simply died with work already done. Attempts are
  // counted beside the status file, and the cumulative prefix the manager passes
  // back through --carry-over seeds the next attempt's record exactly as a real
  // worker does.
  const marker = path.join(path.dirname(statusFile), "resume-attempts");
  const attempt = fs.existsSync(marker) ? Number(fs.readFileSync(marker, "utf8")) + 1 : 1;
  fs.writeFileSync(marker, String(attempt));
  const carryOver = args.has("carry-over") ? JSON.parse(args.get("carry-over")) : undefined;
  const running = {
    id: args.get("id"),
    name: args.get("name"),
    task,
    status: "running",
    runner: args.get("runner") ?? "pi",
    transport: args.get("transport"),
    cwd: args.get("cwd"),
    startedAt: Date.now(),
    updatedAt: Date.now(),
    turns: carryOver?.turns ?? 0,
    toolCalls: carryOver?.toolCalls ?? 0,
    text: "",
    exitCode: null,
    usage: carryOver?.usage ?? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  const addUsage = (delta) => ({
    input: running.usage.input + delta.input,
    output: running.usage.output + delta.output,
    cacheRead: running.usage.cacheRead + delta.cacheRead,
    cacheWrite: running.usage.cacheWrite + delta.cacheWrite,
    cost: running.usage.cost + delta.cost,
  });
  fs.mkdirSync(path.dirname(statusFile), { recursive: true });
  fs.writeFileSync(statusFile, JSON.stringify(running));
  await new Promise((resolve) => setTimeout(resolve, 100));
  if (attempt === 1 && task.includes("RESUME_AFTER_STOP")) {
    const stoppedAt = Date.now();
    fs.writeFileSync(
      statusFile,
      JSON.stringify({
        ...running,
        status: "stopped",
        error: "Agent stopped",
        updatedAt: stoppedAt,
        finishedAt: stoppedAt,
        turns: 5,
        toolCalls: 3,
        usage: addUsage({ input: 100, output: 50, cacheRead: 0, cacheWrite: 0, cost: 0.01 }),
      }),
    );
    process.exit(1);
  }
  if (attempt === 1) process.exit(3);
  const finishedAt = Date.now();
  fs.writeFileSync(
    statusFile,
    JSON.stringify({
      ...running,
      status: "completed",
      updatedAt: finishedAt,
      finishedAt,
      turns: running.turns + 1,
      toolCalls: running.toolCalls + 1,
      text: `resumed attempt ${attempt}`,
      usage: addUsage({ input: 10, output: 5, cacheRead: 0, cacheWrite: 0, cost: 0.001 }),
    }),
  );
} else if (task.includes("LIVE_WITH_PROGRESS")) {
  // A live attempt that already did work, keeps running, and finishes on its own
  // unless a stop or a kill gets there first. Attempts are counted beside the
  // status file so tests can prove whether a relaunch happened.
  process.on("SIGTERM", () => process.exit(0));
  process.on("SIGINT", () => process.exit(0));
  const marker = path.join(path.dirname(statusFile), "resume-attempts");
  const attempt = fs.existsSync(marker) ? Number(fs.readFileSync(marker, "utf8")) + 1 : 1;
  fs.writeFileSync(marker, String(attempt));
  const startedAt = Date.now();
  const running = {
    id: args.get("id"),
    name: args.get("name"),
    task,
    status: "running",
    runner: args.get("runner") ?? "pi",
    transport: args.get("transport"),
    cwd: args.get("cwd"),
    startedAt,
    updatedAt: startedAt,
    turns: 4,
    toolCalls: 2,
    text: "",
    exitCode: null,
    usage: { input: 40, output: 20, cacheRead: 0, cacheWrite: 0, cost: 0.002 },
  };
  fs.mkdirSync(path.dirname(statusFile), { recursive: true });
  fs.writeFileSync(statusFile, JSON.stringify(running));
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  const finishedAt = Date.now();
  fs.writeFileSync(
    statusFile,
    JSON.stringify({
      ...running,
      status: "completed",
      updatedAt: finishedAt,
      finishedAt,
      turns: 5,
      toolCalls: 3,
      text: `live attempt ${attempt} complete`,
    }),
  );
} else if (task.includes("STREAM_PREVIEW")) {
  const startedAt = Date.now();
  const running = {
    id: args.get("id"),
    name: args.get("name"),
    task,
    status: "running",
    runner: args.get("runner") ?? "pi",
    transport: args.get("transport"),
    cwd: args.get("cwd"),
    startedAt,
    updatedAt: startedAt,
    turns: 0,
    toolCalls: 0,
    text: "",
    exitCode: null,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  fs.mkdirSync(path.dirname(statusFile), { recursive: true });
  fs.mkdirSync(path.dirname(logFile), { recursive: true });
  fs.writeFileSync(statusFile, JSON.stringify(running));
  fs.writeFileSync(
    logFile,
    `${JSON.stringify({ type: "tool_execution_start", toolCallId: "read-1", toolName: "read", args: { path: "first.ts" } })}\n`,
  );
  await new Promise((resolve) => setTimeout(resolve, 1_200));
  fs.appendFileSync(
    logFile,
    [
      { type: "tool_execution_end", toolCallId: "read-1", toolName: "read", result: "first" },
      { type: "tool_execution_start", toolCallId: "bash-1", toolName: "bash", args: { command: "echo second" } },
    ].map((event) => JSON.stringify(event)).join("\n") + "\n",
  );
  await new Promise((resolve) => setTimeout(resolve, 1_200));
  fs.appendFileSync(
    logFile,
    `${JSON.stringify({ type: "tool_execution_end", toolCallId: "bash-1", toolName: "bash", result: "second" })}\n`,
  );
  await new Promise((resolve) => setTimeout(resolve, 300));
  const finishedAt = Date.now();
  fs.writeFileSync(statusFile, JSON.stringify({
    ...running,
    status: "completed",
    updatedAt: finishedAt,
    finishedAt,
    turns: 1,
    toolCalls: 2,
    text: "stream preview complete",
  }));
} else {
  const fail = task.includes("FAIL_DIRECTIVE");
  const stopDirective = task.includes("STOP_DIRECTIVE");
  const emptyMessage = task.includes("EMPTY_MESSAGE_DIRECTIVE");
  const directive = schema?.properties?.action
    ? emptyMessage
      ? { action: "message" }
      : {
          action: stopDirective ? "stop" : "message",
          message: stopDirective ? "fake actor role complete" : "fake actor advice",
          ...(images.length > 0 ? { data: { imageCount: images.length } } : {}),
        }
    : undefined;
  const now = Date.now();
  const largeText = task.includes("LARGE_RESULT") ? "x".repeat(100_000) : undefined;
  const text = largeText ?? (directive && !fail ? JSON.stringify(directive) : "fake worker complete");
  const record = {
    id: args.get("id"),
    name: args.get("name"),
    task,
    status: fail ? "failed" : "completed",
    runner: args.get("runner") ?? "pi",
    transport: args.get("transport"),
    fullCodeMode: args.get("full-code-mode"),
    mainAgentId: args.get("main-agent-id"),
    tools: JSON.parse(args.get("tools") ?? "[]"),
    extensions: args.get("extensions"),
    fabricExtension: args.get("fabric-extension"),
    grantedRisks: JSON.parse(args.get("granted-risks") ?? "[]"),
    imageCount: images.length,
    cwd: args.get("cwd"),
    startedAt: now,
    updatedAt: now,
    finishedAt: now,
    turns: 1,
    toolCalls: 0,
    text,
    ...(largeText
      ? { value: { output: largeText } }
      : directive && !fail
        ? { value: directive }
        : {}),
    ...(fail ? { error: "Structured agent output was invalid: Unexpected token (output: not json)" } : {}),
    exitCode: 0,
    usage: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, cost: 0 },
    ...(args.has("model") ? { model: args.get("model") } : {}),
    ...(args.has("thinking") ? { thinking: args.get("thinking") } : {}),
    ...(args.has("system-prompt") ? { systemPrompt: args.get("system-prompt") } : {}),
  };
  fs.mkdirSync(path.dirname(statusFile), { recursive: true });
  if (lifecycleFile) {
    const lifecycleEvents = [
      { version: 1, event: "pi.agent_start", occurredAt: now },
      { version: 1, event: "pi.turn_end", occurredAt: now, data: { turnIndex: 0 } },
      { version: 1, event: "pi.agent_end", occurredAt: now, data: { willRetry: false } },
      { version: 1, event: "pi.agent_settled", occurredAt: now },
    ];
    fs.writeFileSync(
      lifecycleFile,
      lifecycleEvents.map((event) => JSON.stringify(event)).join("\n") + "\n",
    );
  }
  fs.writeFileSync(statusFile, JSON.stringify(record));

  // Emit a per-run event stream so agents.log / readLog can inspect the run.
  if (logFile) {
    fs.mkdirSync(path.dirname(logFile), { recursive: true });
    const events = [
      { type: "agent_start" },
      { type: "tool_execution_start", toolName: "read" },
      { type: "tool_execution_end", toolName: "read" },
      { type: "turn_end" },
      {
        type: "message_end",
        message: { role: "assistant", content: text, usage: { input: 1, output: 2 } },
      },
      { type: "agent_end", willRetry: false },
      { type: "agent_settled" },
    ];
    fs.writeFileSync(logFile, events.map((event) => JSON.stringify(event)).join("\n") + "\n");
  }

  // Append a lightweight actor transcript only when this is not already a
  // native Pi session. Handoff fixtures pass a real branched JSONL file; raw
  // role records would corrupt its id/parentId tree.
  let nativePiSession = false;
  if (sessionFile && fs.existsSync(sessionFile)) {
    try {
      const first = fs.readFileSync(sessionFile, "utf8").split("\n", 1)[0];
      nativePiSession = JSON.parse(first).type === "session";
    } catch {}
  }
  if (sessionFile && !nativePiSession) {
    fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
    const turns = [
      { role: "user", content: task },
      { role: "assistant", content: text },
    ];
    fs.appendFileSync(sessionFile, turns.map((turn) => JSON.stringify(turn)).join("\n") + "\n");
  }
}
