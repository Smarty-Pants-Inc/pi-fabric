import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AgentManager } from "../agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../config.js";
import type { AgentRunRequest, AgentRunResult } from "../agents/types.js";

/** An owned, empty cwd avoids workspace instructions and source transfer. */
export async function runJudgmentAgent(request: AgentRunRequest, limits: { timeoutMs: number; maxTokens: number }, signal: AbortSignal,
  options: { piBinary: string; workerPath: string }): Promise<AgentRunResult> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-judge-"));
  const cwd = path.join(root, "cwd"); fs.mkdirSync(cwd, { mode: 0o700 });
  const runs = path.join(root, "runs");
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: limits.timeoutMs,
    maxTokensPerChild: limits.maxTokens, maxConcurrent: 1, maxPerExecution: 1, maxDepth: 1,
    extensions: false, defaultTools: [], retainRuns: false, sessionExport: false, notifyOnComplete: false, nice: 10,
  }, { ...options, runRoot: runs, fullCodeMode: false });
  let result: AgentRunResult | undefined;
  try {
    const handle = await manager.spawn(request, signal);
    try {
      result = await manager.wait(handle.id, { signal });
    } catch (error) {
      if (!signal.aborted) throw error;
      // Wait cancellation is not stop. Join the owned worker and return its final
      // receipt so the caller accounts for reported usage before checking abort.
      result = await manager.stop(handle.id);
    }
  } finally {
    await manager.close();
    // Close collects only durably saved, confirmed-exited runs. Any remaining
    // root is a preservation veto, including in-memory lost contact when its
    // marker write failed. Never override that veto with recursive deletion.
    if (fs.existsSync(runs)) {
      const error = `agent_cleanup_unresolved: retained receipt root ${root}`;
      if (!result) throw new Error(error);
      result = { ...result, status: "failed", error };
    } else {
      fs.rmSync(root, { recursive: true, force: true });
    }
  }
  return result;
}
