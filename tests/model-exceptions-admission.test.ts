import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const model = "anthropic/claude-opus-5-5";
const reason = "Bounded test admission #1";
const future = "2999-01-01T00:00:00.000Z";

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-model-exception-"));
  roots.push(root);
  const cwd = path.join(root, "project");
  const agentDir = path.join(root, "agent");
  const runRoot = path.join(root, "runs");
  const projectDir = path.join(cwd, ".pi");
  fs.mkdirSync(projectDir, { recursive: true });
  fs.mkdirSync(agentDir, { recursive: true });
  const redirectedPath = path.join(root, "attacker-selected.json");
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: {
    modelExceptionsPath: redirectedPath,
    modelExceptions: [{ ref: "smarty-dev#1", models: ["anthropic/claude"], expires: future }],
  } }));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ agents: {
    modelExceptionsPath: redirectedPath,
    modelExceptions: [{ ref: "smarty-dev#1", models: ["anthropic/claude"], expires: future }],
  } }));
  fs.writeFileSync(path.join(projectDir, "model-exceptions.json"), JSON.stringify([{
    ref: "smarty-dev#1", models: ["anthropic/claude"], expires: future,
  }]));
  const config = loadFabricConfig({ cwd, agentDir, projectTrusted: true });
  const exceptionPath = path.resolve(agentDir, "model-exceptions.json");
  expect(config.agents.modelExceptionsPath).toBe(exceptionPath);
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, ...config.agents, budgetUsd: 0 }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
    runRoot,
  });
  managers.push(manager);
  return { cwd, agentDir, exceptionPath, manager, runRoot };
}

const writeGrant = (file: string, record: Record<string, unknown>) =>
  fs.writeFileSync(file, JSON.stringify([record]));

describe("fresh host model-exception admission", () => {
  it("refuses unlisted #1 before launch, then completes the matching host-exception control", async () => {
    const state = fixture();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      await expect(state.manager.spawn({ task: "unlisted issue stays denied", model, modelReason: reason, transport: "process" }))
        .rejects.toMatchObject({ name: "FabricModelExceptionDeniedError", code: "FABRIC_MODEL_EXCEPTION_DENIED" });
      expect(state.manager.list()).toEqual([]);
      expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
      expect(launch).not.toHaveBeenCalled();

      writeGrant(state.exceptionPath, { ref: "smarty-dev#1", models: ["anthropic/claude-opus"], expires: future });
      const handle = await state.manager.spawn({ task: "approved issue control", model, modelReason: reason, transport: "process" });
      expect(launch).toHaveBeenCalledOnce();
      expect(await state.manager.wait(handle.id)).toMatchObject({ status: "completed", model, modelReason: reason });
      const auditFile = path.join(os.homedir(), ".local", "state", "smarty", "route", "opus-exceptions.jsonl");
      const auditRows = fs.readFileSync(auditFile, "utf8").trim().split("\n").filter(Boolean)
        .map(line => JSON.parse(line) as Record<string, unknown>);
      expect(auditRows.find(row => row.reason === reason)).toMatchObject({ ref: "smarty-dev#1", role: "task", model });
    } finally { launch.mockRestore(); }
  });

  it("pins the admitted canonical ref when the host file changes before audit", async () => {
    const state = fixture();
    const reason = "Unlisted decoy #1; approved host grant smarty-dev#12000";
    writeGrant(state.exceptionPath, { ref: "smarty-dev#12000", models: ["anthropic/claude-opus"], expires: future });
    const beforeCommit = vi.fn(() => fs.writeFileSync(state.exceptionPath, "[]"));

    const handle = await state.manager.spawn({ task: "audit attribution race", model, modelReason: reason, transport: "process" },
      undefined, undefined, beforeCommit);
    expect(beforeCommit).toHaveBeenCalledOnce();
    expect(await state.manager.wait(handle.id)).toMatchObject({ status: "completed", model, modelReason: reason });
    const auditFile = path.join(os.homedir(), ".local", "state", "smarty", "route", "opus-exceptions.jsonl");
    const auditRows = fs.readFileSync(auditFile, "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as Record<string, unknown>);
    expect(auditRows.find(row => row.reason === reason)).toMatchObject({ ref: "smarty-dev#12000", role: "task", model });
  });

  it.each([
    ["missing host file", undefined, "file-unavailable"],
    ["malformed host file", "not json", "malformed"],
    ["expired grant", JSON.stringify([{ ref: "smarty-dev#1", models: ["anthropic/claude"], expires: "2000-01-01T00:00:00.000Z" }]), "expired"],
    ["wrong model", JSON.stringify([{ ref: "smarty-dev#1", models: ["provider/gpt"], expires: future }]), "model-mismatch"],
    ["wrong role", JSON.stringify([{ ref: "smarty-dev#1", models: ["anthropic/claude"], roles: ["actor:reviewer"], expires: future }]), "role-mismatch"],
  ] as const)("rejects a fresh-read mismatch (%s) before creating a run", async (_label, contents, denial) => {
    const state = fixture();
    if (contents !== undefined) fs.writeFileSync(state.exceptionPath, contents);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      await expect(state.manager.spawn({ task: "task role", model, modelReason: reason, transport: "process" }))
        .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial });
      expect(state.manager.list()).toEqual([]);
      expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
      expect(launch).not.toHaveBeenCalled();
    } finally { launch.mockRestore(); }
  });
});
