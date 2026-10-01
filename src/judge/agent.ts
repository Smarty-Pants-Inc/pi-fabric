import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AgentManager } from "../agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../config.js";
import { hasUnresolvedWorker } from "../storage/retention.js";
import type { AgentRunRequest, AgentRunResult } from "../agents/types.js";

/** An owned, empty cwd avoids workspace instructions and source transfer. */
export async function runJudgmentAgent(request: AgentRunRequest, limits: { timeoutMs: number; maxTokens: number }, signal: AbortSignal,
  options: { piBinary: string; workerPath: string }): Promise<AgentRunResult> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-judge-"));
  const cwd = path.join(root, "cwd"); fs.mkdirSync(cwd, { mode: 0o700 });
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: limits.timeoutMs,
    maxTokensPerChild: limits.maxTokens, maxConcurrent: 1, maxPerExecution: 1, maxDepth: 1,
    extensions: false, defaultTools: [], retainRuns: false, sessionExport: false, notifyOnComplete: false, nice: 10,
  }, { ...options, runRoot: path.join(root, "runs"), fullCodeMode: false });
  try {
    const handle = await manager.spawn(request, signal);
    // Wait cancellation is not stop. The finally always stops and waits for owned processes.
    return await manager.wait(handle.id, { signal });
  } finally {
    await manager.close();
    const runs = path.join(root, "runs");
    const unresolved = fs.existsSync(runs) && fs.readdirSync(runs).some(id => hasUnresolvedWorker(path.join(runs, id)));
    // Never erase files an unresolved worker could still use. Keep its owned receipt for reconciliation.
    if (unresolved) throw new Error("agent_cleanup_unresolved");
    fs.rmSync(root, { recursive: true, force: true });
  }
}
