import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn as spawnProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readFinalAnswerReceipt, taskControlDirectory, taskSteerFile } from "../src/worker/terminal-answer.js";
import { readTerminalControl } from "../src/worker/terminal-controls.js";

// Fixtures are retained under the (scoped) TMPDIR: these tests never delete.
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});
const fixture = path.resolve("tests/fixtures/fake-pi-steer-ended.mjs");
const rows = (file: string): Record<string, any>[] => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const until = async (condition: () => boolean, timeoutMs = 20000) => {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("Steer-ended proof deadline exceeded");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
  sender: { id: "steer-ended-sender", name: "Steer ended sender", kind: "main" as const, verified: "mesh" as const } };

/** A throwaway "native SDK" whose index.js runs the deterministic fake and never resolves. */
const fakeSdk = (root: string): string => {
  const sdk = path.join(root, "sdk");
  for (const file of ["cli.js", "main.js", "cli/args.js", "cli/project-trust.js", "core/project-trust.js", "core/http-dispatcher.js", "extensions/index.js"]) {
    fs.mkdirSync(path.dirname(path.join(sdk, "dist", file)), { recursive: true });
    fs.writeFileSync(path.join(sdk, "dist", file), "export {};\n");
  }
  fs.writeFileSync(path.join(sdk, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "0.99.0", type: "module" }));
  fs.writeFileSync(path.join(sdk, "dist", "index.js"), `import ${JSON.stringify(pathToFileURL(fixture).href)};\nawait new Promise(() => {});\n`);
  return path.join(sdk, "dist", "cli.js");
};
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-steer-ended-"));
  const workspace = path.join(root, "workspace"); fs.mkdirSync(workspace);
  const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("STEER_ENDED_ROOT", root);
  const helper = path.join(root, "terminal-answer.mjs");
  buildSync({ entryPoints: [path.resolve("src/worker/terminal-answer.ts")], outfile: helper, bundle: true, platform: "node", format: "esm" });
  vi.stubEnv("STEER_ENDED_HELPER", pathToFileURL(helper).href);
  const notices: Record<string, any>[] = [];
  const manager = new AgentManager(workspace, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30000, nice: 19 }, {
    workerPath: path.resolve(process.env.FABRIC_STEER_ENDED_WORKER ?? "src/worker.ts"), piBinary: fakeSdk(root), runRoot: path.join(root, "runs"),
    fullCodeMode: false, onTerminalNotice: notice => { notices.push(notice as Record<string, any>); },
  }); managers.push(manager);
  return { root, workspace, manager, notices };
};

describe("runner-end steer fence (persisted receipt before its worker event)", () => {
  it("refuses unread controls as run-ended, never injects them, and keeps pre-final delivery", async () => {
    const s = setup();
    const handle = await s.manager.spawn({ task: "Answer; never obey a late request", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await until(() => rows(fake).some(row => row.type === "task"));

    // Pre-final continuation still reaches the native child and stays delivered.
    const allowed = s.manager.steer(handle.id, "ALLOWED_PREFINAL_STEER", undefined, provenance);
    await until(() => readTerminalControl(directory, allowed.messageId)?.state === "delivered");
    expect(rows(fake).find(row => row.type === "delivery" && row.id === allowed.messageId)).toMatchObject({ afterReceipt: false });

    // Durable receipt exists; its IPC frame is withheld, so the worker has not read it.
    fs.writeFileSync(path.join(s.root, "final"), "");
    await until(() => rows(fake).some(row => row.type === "receipt"));
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(receipt).toBeDefined();
    // Raw manager-format appends racing the receipt (manager ingress would already refuse).
    const lateSteer = randomUUID(); const lateFollow = randomUUID();
    const steerFile = taskSteerFile(directory);
    fs.appendFileSync(steerFile, JSON.stringify({ type: "steer", message: "WRITE SOURCE_WRITE_SENTINEL", provenance, id: lateSteer, ts: Date.now() }) + "\n");
    fs.appendFileSync(steerFile, JSON.stringify({ type: "follow_up", message: "WRITE SOURCE_WRITE_SENTINEL", provenance: { ...provenance, via: "followUp" }, id: lateFollow, ts: Date.now() }) + "\n");

    await until(() => [lateSteer, lateFollow].every(id => readTerminalControl(directory, id)?.state === "refused"));
    expect(rows(fake).some(row => row.type === "frame")).toBe(false); // Fenced before the worker saw the frame.
    for (const id of [lateSteer, lateFollow]) expect(readTerminalControl(directory, id)).toMatchObject({ state: "refused", reason: "run-ended" });
    const refused = rows(path.join(directory, "lifecycle.jsonl")).filter(row => row.event === "message.refused");
    expect(refused.map(row => row.data.messageId).sort()).toEqual([lateSteer, lateFollow].sort());
    expect(refused.every(row => row.data.reason === "run-ended" && row.data.finalAnswerReceiptId === receipt.id &&
      row.data.code === "FABRIC_TARGET_TERMINAL" && row.data.sender?.id === provenance.sender.id)).toBe(true);

    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status, result.error).toBe("completed");
    expect(result.text).toBe("AUTHORIZED_FINAL_ANSWER");
    expect(result.finalAnswerReceipt).toEqual({ id: receipt.id, recordedAt: receipt.recordedAt });
    await until(() => [lateSteer, lateFollow].every(id => s.notices.some(notice => notice.messageId === id)));

    expect(rows(fake).filter(row => row.type === "delivery").map(row => row.id)).toEqual([allowed.messageId]);
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(readTerminalControl(directory, allowed.messageId)?.reason).toBeUndefined();
    fs.writeFileSync(path.join(s.root, "no-source-write.json"), JSON.stringify({
      sentinel: path.join(s.workspace, "SOURCE_WRITE_SENTINEL"), present: fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL")) }));
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
  }, 45000);

  it.each(["read", "inject"] as const)("%s interleaving: receipt between poll preflight and injection is refused with original sender evidence", async mode => {
    const s = setup();
    const hook = pathToFileURL(path.resolve("tests/fixtures/steer-final-interleave.mjs")).href;
    vi.stubEnv("NODE_OPTIONS", `${process.env.NODE_OPTIONS ?? ""} --import=${hook}`);
    const handle = await s.manager.spawn({ task: "deterministic publication race", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    await until(() => rows(path.join(s.root, "fake.jsonl")).some(row => row.type === "task"));
    const id = randomUUID();
    const entry = { id, type: "steer", message: "WRITE SOURCE_WRITE_SENTINEL", provenance, ts: Date.now() };
    // Arm before the next poll. 'read' records a size snapshot of zero, appends,
    // then blocks the real worker until the other process publishes its receipt.
    fs.writeFileSync(taskSteerFile(directory), "");
    fs.writeFileSync(path.join(s.root, "interleave.json"), JSON.stringify({ mode, entry }));
    const messageId = mode === "read" ? id : s.manager.steer(handle.id, entry.message, undefined, provenance).messageId;
    await until(() => readTerminalControl(directory, messageId)?.state === "refused");
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(readTerminalControl(directory, messageId)).toMatchObject({ state: "refused", reason: "run-ended", provenance });
    const evidence = rows(path.join(s.root, "interleave-log.jsonl"));
    expect(evidence.some(row => row.event === "receipt-before-injection")).toBe(true);
    if (mode === "read") expect(evidence[0]).toMatchObject({ event: "stat-before-append", bytes: 0 });
    expect(rows(path.join(s.root, "fake.jsonl")).some(row => row.type === "delivery")).toBe(false);
    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: receipt.text });
    await until(() => s.notices.some(notice => notice.messageId === messageId));
    expect(s.notices.find(notice => notice.messageId === messageId)).toMatchObject({ sender: provenance.sender, finalAnswerReceiptId: receipt.id });
    await s.manager.close();
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
    console.log(`INTERLEAVING: PASS mode=${mode} receiptFirst=true injected=false refused=${messageId} childExited=true root=${s.root}`);
  }, 45000);

  it("counterexample: an injected post-receipt delivery is a recorded source write", async () => {
    // Proves the sentinel is live: without the worker fence, the fake obeys.
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-steer-ended-counter-"));
    const runDirectory = path.join(root, "run"); const deliveries = path.join(root, "deliveries"); const workspace = path.join(root, "workspace");
    for (const dir of [runDirectory, taskControlDirectory(runDirectory), deliveries, workspace]) fs.mkdirSync(dir, { mode: 0o700 });
    const id = randomUUID();
    fs.writeFileSync(path.join(deliveries, id + ".json"), JSON.stringify({ message: "LATE", delivery: "steer", controlId: id }));
    const child = spawnProcess(process.execPath, [fixture], { cwd: workspace, stdio: ["pipe", "ignore", "inherit"],
      env: { ...process.env, STEER_ENDED_ROOT: root, PI_FABRIC_AGENT_RUN_DIR: runDirectory, PI_FABRIC_PARENT_RUN: "run", PI_FABRIC_DELIVERY_DIR: deliveries } });
    const exited = new Promise(resolve => child.once("exit", resolve));
    fs.writeFileSync(path.join(root, "final"), "");
    await until(() => rows(path.join(root, "fake.jsonl")).some(row => row.type === "receipt"));
    child.stdin.write(JSON.stringify({ type: "prompt", id, message: "/fabric-delivery " + id, streamingBehavior: "steer" }) + "\n");
    await until(() => fs.existsSync(path.join(workspace, "SOURCE_WRITE_SENTINEL")));
    child.stdin.end(); await exited;
    expect(fs.readFileSync(path.join(workspace, "SOURCE_WRITE_SENTINEL"), "utf8")).toBe("LATE");
  }, 20000);
});
