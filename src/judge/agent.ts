import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AgentManager } from "../agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../config.js";
import type { AgentHandleInfo, AgentRunRequest, AgentRunResult } from "../agents/types.js";

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
  let handle: AgentHandleInfo | undefined;
  let result: AgentRunResult | undefined;
  let failure: unknown;
  try {
    handle = await manager.spawn(request, signal);
    try {
      result = await manager.wait(handle.id, { signal });
    } catch (error) {
      if (!signal.aborted) throw error;
      // Wait cancellation is not stop. Join the owned worker and return its final
      // receipt so the caller accounts for reported usage before checking abort.
      result = await manager.stop(handle.id);
    }
  } catch (error) {
    failure = error;
  }
  // Stop/close can reject while retaining execution custody. That rejection is
  // not permission to skip the preservation veto or lose the child's identity.
  let closed = false;
  try { await manager.close(); closed = true; } catch (error) { failure ??= error; }
  let cleanupResolved = false;
  if (closed) {
    // The explicit run root belongs to this wrapper, not the manager. Remove
    // only its empty allocation: rmdir is atomic and cannot delete receipts.
    try { fs.rmdirSync(runs); cleanupResolved = true; }
    catch (error) { cleanupResolved = (error as NodeJS.ErrnoException).code === "ENOENT"; }
  }
  // Missing files do not prove release when close itself failed.
  if (!cleanupResolved) {
    const error = `agent_cleanup_unresolved: retained receipt root ${root}`;
    if (!result && handle) {
      const receipt = manager.status(handle.id);
      const now = Date.now();
      // This is a diagnostic receipt, NOT manager settlement or exit proof.
      // Unknown launches may have only a queued handle and no usage snapshot.
      result = "usage" in receipt ? { ...receipt, status: "failed" } : {
        ...receipt, status: "failed", task: request.task, startedAt: now, updatedAt: now,
        turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      };
    }
    if (!result) throw new Error(error);
    result = { ...result, status: "failed", error };
  } else {
    // Checked close collected the execution custody and the caller-owned runs
    // allocation was empty or absent. Only then release the private wrapper.
    fs.rmSync(root, { recursive: true, force: true });
    if (failure) throw failure;
  }
  if (!result) throw failure ?? new Error("Judgment agent exited without a receipt");
  return result;
}
