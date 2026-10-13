// A custom worker runner process: speaks the documented file protocol only.
import fs from "node:fs";

const context = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const { files } = context;
const now = Date.now();
const record = {
  id: context.id,
  name: context.name,
  task: context.task,
  status: "running",
  runner: "acme",
  transport: "process",
  cwd: context.cwd,
  startedAt: now,
  updatedAt: now,
  turns: 0,
  toolCalls: 0,
  text: "",
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
};
const write = (value) => {
  fs.writeFileSync(`${files.statusFile}.tmp`, JSON.stringify(value));
  fs.renameSync(`${files.statusFile}.tmp`, files.statusFile);
};
write(record);
fs.appendFileSync(
  files.logFile,
  `${JSON.stringify({ type: "message_end", message: { role: "assistant", content: "acme says hi" } })}\n`,
);
fs.appendFileSync(
  files.lifecycleFile,
  `${JSON.stringify({
    version: 1,
    event: "tokens.usage",
    occurredAt: Date.now(),
    data: {
      runId: context.id,
      name: context.name,
      runner: "acme",
      depth: context.depth,
      cumulativeTokens: 10,
      input: 10,
      output: 5,
      cacheRead: 0,
      cacheWrite: 0,
      cost: 0.01,
    },
  })}\n`,
);
write({
  ...record,
  status: "completed",
  turns: 1,
  text: `acme finished: ${context.task}`,
  usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, cost: 0.01 },
  updatedAt: Date.now(),
  finishedAt: Date.now(),
});
