import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { readTerminalControl } from "../src/worker/terminal-controls.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";

// Same process-transport + built real-worker path as worker-e2e. Only the model
// child is deterministic; fixtures are retained (no real deletes in this proof).
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});
const rows = (file: string): Array<Record<string, any>> => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const observed = async (root: string, condition: () => boolean): Promise<void> => {
  if (condition()) return;
  await new Promise<void>((resolve, reject) => {
    const finish = (error?: Error) => { clearTimeout(timer); watcher.close(); error ? reject(error) : resolve(); };
    const watcher = fs.watch(root, { recursive: true }, () => { if (condition()) finish(); });
    const timer = setTimeout(() => finish(new Error("E-steer-final observation timed out")), 20_000);
    if (condition()) finish();
  });
};
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
  sender: { id: "session:steer-ended-sender", name: "Original sender", kind: "main" as const, verified: "mesh" as const } };
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-steer-ended-"));
  const workspace = path.join(root, "workspace"); fs.mkdirSync(workspace);
  const profile = path.join(root, "profile"); fs.mkdirSync(profile);
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ retry: { enabled: false } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("STEER_ENDED_ROOT", root);
  const helper = path.join(root, "terminal-answer.mjs");
  buildSync({ entryPoints: [path.resolve("src/worker/terminal-answer.ts")], outfile: helper, bundle: true, platform: "node", format: "esm" });
  vi.stubEnv("STEER_ENDED_HELPER", pathToFileURL(helper).href);
  const sdk = path.join(root, "sdk");
  for (const file of ["cli.js", "main.js", "cli/args.js", "cli/project-trust.js", "core/project-trust.js", "core/http-dispatcher.js", "extensions/index.js"]) {
    fs.mkdirSync(path.dirname(path.join(sdk, "dist", file)), { recursive: true });
    fs.writeFileSync(path.join(sdk, "dist", file), "export {};\n");
  }
  fs.writeFileSync(path.join(sdk, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "0.99.0", type: "module" }));
  fs.writeFileSync(path.join(sdk, "dist", "index.js"), `import ${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-steer-ended.mjs")).href)};\nawait new Promise(() => {});\n`);
  const notices: AgentTerminalNotice[] = [];
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 30_000, nice: 19, retainRuns: true }, {
    workerPath: path.resolve("dist/worker.js"), piBinary: path.join(sdk, "dist", "cli.js"), runRoot: path.join(root, "runs"),
    fullCodeMode: false, onTerminalNotice: notice => { notices.push(notice); },
  });
  managers.push(manager);
  return { root, workspace, manager, notices };
};

describe("E-steer-final: real worker final-answer fence", () => {
  it("delivers before final, refuses a post-final steer with an undelivered receipt, and starts no new turn", async () => {
    const s = setup();
    const handle = await s.manager.spawn({ task: "Return the authorized answer", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await observed(s.root, () => rows(fake).some(row => row.type === "task"));
    const allowed = s.manager.steer(handle.id, "ALLOWED_PREFINAL_STEER", undefined, provenance);
    await observed(s.root, () => readTerminalControl(directory, allowed.messageId)?.state === "delivered");
    expect(rows(fake).find(row => row.type === "delivery" && row.id === allowed.messageId)).toMatchObject({ afterReceipt: false });

    fs.writeFileSync(path.join(s.root, "final"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "receipt"));
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    const journal = path.join(directory, "steer.jsonl");
    const before = fs.readFileSync(journal, "utf8");
    expect(() => s.manager.steer(handle.id, "WRITE SOURCE_WRITE_SENTINEL", undefined, provenance)).toThrow(expect.objectContaining({
      name: "FabricTargetTerminalError", code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", deliveryStatus: "undelivered",
      runState: "final-answer-pending", finalAnswerReceiptId: receipt.id,
      message: expect.stringContaining("undelivered: task already finished"),
    }));
    expect(fs.readFileSync(journal, "utf8")).toBe(before);
    expect(rows(fake).some(row => row.type === "frame")).toBe(false);

    // An admission racing the pre-check is represented by its original journal
    // row. The worker must refuse it BEFORE seeing the held final IPC frame.
    const late = randomUUID();
    fs.appendFileSync(journal, JSON.stringify({ type: "steer", message: "WRITE SOURCE_WRITE_SENTINEL", provenance, id: late, ts: Date.now() }) + "\n");
    await observed(s.root, () => readTerminalControl(directory, late)?.state === "refused");
    expect(rows(fake).some(row => row.type === "frame")).toBe(false);
    const refusal = rows(path.join(directory, "lifecycle.jsonl")).find(row => row.event === "message.refused" && row.data.messageId === late);
    expect(refusal?.data).toMatchObject({ code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt.id, sender: provenance.sender });

    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30_000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: "AUTHORIZED_FINAL_ANSWER", finalAnswerReceipt: { id: receipt.id } });
    await s.manager.close(); // Join the owned native process tree, not just the answer.
    expect(s.notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, finalAnswerReceiptId: receipt.id,
      messageId: late, delivery: "steer", sender: provenance.sender }]);
    expect(rows(fake).filter(row => row.type === "task")).toHaveLength(1);
    expect(rows(fake).filter(row => row.type === "delivery").map(row => row.id)).toEqual([allowed.messageId]);
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
    expect(() => s.manager.steer(handle.id, "late after native close")).toThrow(expect.objectContaining({ deliveryStatus: "undelivered", runState: "completed" }));
    console.log(`E-steer-final: PASS worker=dist/worker.js root=${s.root} prefinal=delivered postfinal=undelivered turns=1 sourceUnchanged=true`);
  }, 45_000);
});
