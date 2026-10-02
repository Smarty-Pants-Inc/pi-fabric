import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { dedupedUsageTokens } from "../src/worker/session-export.js";

// smarty-dev#3327: an actor's Pi journal (session.jsonl) and its `.fabric`
// usage export record the same model turns, so summing both double-counted.
// Drives the real src/worker.ts with a stub Pi that journals two turns.
const workerPath = path.resolve("src/worker.ts");
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const FAKE_PI = `
import fs from "node:fs";
const journal = process.argv[process.argv.indexOf("--session") + 1];
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\\n");
const turn = (timestamp, input, text) => ({
  role: "assistant", content: [{ type: "text", text }], api: "openai-completions",
  provider: "dedupe-test", model: "offline", stopReason: "stop", timestamp,
  usage: { input, output: 7, cacheRead: 100, cacheWrite: 3, totalTokens: input + 110,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.001 } },
});
let buffer = "";
process.stdin.on("data", (chunk) => {
  buffer += chunk.toString("utf8");
  if (!buffer.includes("\\n")) return;
  const turns = [turn(1790000000001, 40, "first"), turn(1790000000002, 50, "done")];
  fs.writeFileSync(journal, JSON.stringify({ type: "session", version: 3, id: "actor-journal", timestamp: new Date().toISOString(), cwd: process.cwd() }) + "\\n");
  let parentId = null;
  turns.forEach((message, index) => {
    const id = "entry-" + index;
    fs.appendFileSync(journal, JSON.stringify({ type: "message", id, parentId, timestamp: new Date().toISOString(), message }) + "\\n");
    parentId = id;
    emit({ type: "message_end", message });
  });
  emit({ type: "agent_settled" });
  process.exit(0);
});
`;

const readLines = (file: string): unknown[] =>
  fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as unknown);
const exportFiles = (dir: string): string[] =>
  fs.readdirSync(dir, { recursive: true, withFileTypes: true })
    .filter(entry => entry.isFile() && entry.name.endsWith(".jsonl"))
    .map(entry => path.join(entry.parentPath, entry.name));

describe("actor journal vs usage export (smarty-dev#3327)", () => {
  it("marks re-exported turns so a usage sum over both sources equals the journal alone", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-turn-dedupe-"));
    roots.push(root);
    const piBinary = path.join(root, "fake-pi.mjs");
    fs.writeFileSync(piBinary, FAKE_PI);
    const actorDir = path.join(root, "actors", "test-actor");
    fs.mkdirSync(actorDir, { recursive: true });
    const journal = path.join(actorDir, "session.jsonl");
    const exportRoot = path.join(root, "export");
    vi.stubEnv("PI_FABRIC_AGENT_DIR", "");
    const manager = new AgentManager(root, {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, sessionExport: true, sessionExportDir: exportRoot,
    }, { workerPath, piBinary, runRoot: path.join(root, "runs") });
    managers.push(manager);
    const result = await manager.run({
      task: "two turns", cwd: root, transport: "process",
      sessionFile: journal, actorId: "actor-test", actorName: "test-actor",
    });
    expect(result.status).toBe("completed");

    const journalLines = readLines(journal);
    const [exportFile] = exportFiles(exportRoot);
    expect(exportFile).toBeDefined();
    const exportLines = readLines(exportFile!);
    const exported = exportLines.filter((line): line is Record<string, unknown> =>
      (line as { type?: unknown }).type === "message");
    expect(exported).toHaveLength(2);
    // Regression: every re-exported entry carries the journal turn marker.
    expect(exported.map(entry => entry.reexportOf)).toEqual([
      `${journal}#1790000000001`,
      `${journal}#1790000000002`,
    ]);

    const journalOnly = dedupedUsageTokens([{ file: journal, lines: journalLines }]);
    expect(journalOnly).toBe((40 + 7 + 100 + 3) + (50 + 7 + 100 + 3));
    expect(dedupedUsageTokens([
      { file: journal, lines: journalLines },
      { file: exportFile!, lines: exportLines },
    ])).toBe(journalOnly);
    // Export alone still counts every turn (trackers that only see the export).
    expect(dedupedUsageTokens([{ file: exportFile!, lines: exportLines }])).toBe(journalOnly);
  }, 90_000);
});
