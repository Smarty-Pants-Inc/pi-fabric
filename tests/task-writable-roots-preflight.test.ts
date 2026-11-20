import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager, type AgentTaskPresence } from "../src/agents/manager.js";
import { AgentAdmission } from "../src/agents/lifecycle.js";
import { WorktreeManager } from "../src/agents/worktree-manager.js";
import type { AgentRunRequest } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Only this suite's own fixtures are removed, after their managers have closed.
const fixtures: { root: string; manager: AgentManager }[] = [];
afterEach(async () => {
  for (const { root, manager } of fixtures.splice(0)) {
    await manager.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});

const fixture = (taskPresence?: AgentTaskPresence) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-writable-preflight-")));
  const cwd = path.join(root, "work");
  fs.mkdirSync(cwd);
  const meshRoot = path.join(root, "mesh");
  fs.mkdirSync(meshRoot);
  // Keep the existing-directory control's overlapping custom run-root layout.
  const runRoot = path.join(cwd, "runs");
  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs';
const args = new Map(); for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const policy = JSON.parse(args.get('--write-policy'));
fs.appendFileSync(args.get('--log-file'),JSON.stringify({type:'agent_start'})+'\\n');
const now=Date.now(); fs.writeFileSync(args.get('--status-file'),JSON.stringify({id:args.get('--id'),name:args.get('--name'),task:'probe',status:'completed',runner:args.get('--runner'),transport:'process',sessionId:String(process.pid),cwd:args.get('--cwd'),startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:0,text:JSON.stringify({policy,tmpdir:args.get('--tmpdir')??null}),exitCode:0,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));`);
  const recipient = { rootId: "session:admitted-main", sessionId: "admitted-main", projectRoot: cwd, cwd, name: "Admitted Main", startedAt: Date.now() };
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, retainRuns: true }, {
    workerPath: worker, runRoot, fullCodeMode: false, meshRoot, completionRecipient: recipient,
    ...(taskPresence ? { taskPresence, meshTransport: { transport: "nats" as const, nats: { replicas: 1 } } } : {}),
  });
  fixtures.push({ root, manager });
  return { root, cwd, meshRoot, runRoot, manager };
};

const report = async (f: ReturnType<typeof fixture>, request: Partial<AgentRunRequest>, beforeCommit: (id: string) => void) => {
  const handle = await f.manager.spawn({ task: "probe", transport: "process", ...request }, undefined, undefined, beforeCommit);
  const result = await f.manager.wait(handle.id);
  expect(result.status).toBe("completed");
  return { result, value: JSON.parse(result.text) };
};

describe("writable-root preflight before durable launch commit", () => {
  // Real catch: smarty-dev#18321 committed a durable ID before rejecting a file-valued root.
  it.each(["absolute", "relative", "ancestor", "worktree-absolute"] as const)("refuses a %s non-directory writable root before admission, commit, or publication", async kind => {
    const taskPresence = { publishBeforeLaunch: vi.fn(async () => {}), withdraw: vi.fn(async () => {}) };
    const f = fixture(taskPresence);
    const file = path.join(f.cwd, "live-registry.tsx");
    fs.writeFileSync(file, "must remain unchanged");
    const invalid = kind === "relative" ? "live-registry.tsx" : kind === "ancestor" ? path.join(file, "out", "gen") : file;
    const missing = path.join(f.cwd, "not-created", "nested");
    const beforeCommit = vi.fn();
    const admission = vi.spyOn(AgentAdmission.prototype, "admit");
    // A regression must fail without ever creating a real Git worktree.
    const worktreeCreation = vi.spyOn(WorktreeManager.prototype, "create")
      .mockRejectedValue(new Error("Unexpected worktree creation during writable-root preflight"));
    try {
      expect(fs.existsSync(f.runRoot)).toBe(false);
      await expect(f.manager.spawn({ task: "must not launch", transport: "process", residency: "durable",
        writableRoots: [missing, invalid], ...(kind === "worktree-absolute" ? { worktree: true } : {}),
      }, undefined, undefined, beforeCommit)).rejects.toThrow(`Writable root is not a directory: ${invalid}`);
      expect(admission).not.toHaveBeenCalled();
      expect(beforeCommit).not.toHaveBeenCalled();
      expect(worktreeCreation).not.toHaveBeenCalled();
      expect(taskPresence.publishBeforeLaunch).not.toHaveBeenCalled();
      expect(taskPresence.withdraw).not.toHaveBeenCalled();
      expect(f.manager.listForUi()).toEqual([]);
      expect(fs.existsSync(f.runRoot)).toBe(false);
      expect(fs.readdirSync(f.meshRoot)).toEqual([]);
      expect(fs.existsSync(path.join(f.cwd, "not-created"))).toBe(false);
      expect(fs.readFileSync(file, "utf8")).toBe("must remain unchanged");
    } finally {
      worktreeCreation.mockRestore();
      admission.mockRestore();
    }
  });

  it("admits an existing directory and calls beforeCommit before creating run artifacts", async () => {
    const f = fixture();
    const beforeCommit = vi.fn(() => {
      expect(fs.statSync(f.cwd).isDirectory()).toBe(true);
      expect(fs.existsSync(f.runRoot)).toBe(false);
      expect(fs.readdirSync(f.meshRoot)).toEqual([]);
    });
    const { result, value } = await report(f, { writableRoots: [f.cwd], shell: "unconfined" }, beforeCommit);
    expect(beforeCommit).toHaveBeenCalledExactlyOnceWith(result.id);
    expect(value.policy.writableRoots).toEqual([f.cwd]);
    expect(value.tmpdir).toBeNull();
    expect(fs.statSync(f.manager.runDirectory(result.id)!).isDirectory()).toBe(true);
  });

  it("admits a relative missing nested writable root without creating it during preflight", async () => {
    const f = fixture();
    const nested = path.join(f.cwd, "out", "gen");
    const beforeCommit = vi.fn(() => {
      expect(fs.existsSync(path.join(f.cwd, "out"))).toBe(false);
      expect(fs.existsSync(f.runRoot)).toBe(false);
      expect(fs.readdirSync(f.meshRoot)).toEqual([]);
    });
    const { result, value } = await report(f, { writableRoots: ["out/gen"] }, beforeCommit);
    expect(beforeCommit).toHaveBeenCalledExactlyOnceWith(result.id);
    expect(value.policy.writableRoots).toEqual([nested]);
    expect(fs.statSync(nested).isDirectory()).toBe(true);
    expect(fs.statSync(f.manager.runDirectory(result.id)!).isDirectory()).toBe(true);
  });
});
