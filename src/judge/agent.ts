import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AgentManager } from "../agents/manager.js";
import { retainCleanupRoot } from "../agents/worktree-manager.js";
import { runTreeExitVeto, runTreeResourceVeto } from "../storage/retention.js";
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
    extensions: false, defaultTools: [], retainRuns: true, sessionExport: false, notifyOnComplete: false, nice: 10,
  }, { ...options, runRoot: runs, fullCodeMode: false });
  let handle: AgentHandleInfo | undefined;
  let result: AgentRunResult | undefined;
  let failure: unknown;
  let closeFailure: unknown;
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
  try { await manager.close(); } catch (error) { closeFailure = error; failure ??= error; }
  let custodyDestination: string | undefined;
  if (fs.existsSync(runs)) {
    // A pending archive or native-exit veto still makes the judgment fail closed,
    // but it does not authorize leaving the complete raw tree in this workspace.
    if (!closeFailure) {
      try {
        for (const name of fs.readdirSync(runs)) {
          const child = path.join(runs, name);
          if (!fs.lstatSync(child).isDirectory()) continue;
          const custodyVeto = runTreeExitVeto(child, 0, undefined, false);
          const descendantVeto = runTreeResourceVeto(child, 0, undefined, true);
          if (custodyVeto || descendantVeto) {
            closeFailure = new Error(`Judgment run exit/custody proof changed before move: ${custodyVeto ?? descendantVeto}`);
            failure ??= closeFailure;
            break;
          }
        }
      } catch (error) { closeFailure = error; failure ??= error; }
    }
    // Custody is a preserving rename, not permission to discard unresolved
    // evidence. Move the complete root even after close fails; a source symlink
    // keeps later manager recovery pointed at the held tree, not duplicate bytes.
    try {
      const judged = fs.lstatSync(runs);
      const moved = retainCleanupRoot(runs, `${root}.trace-archive`, "judgment-agent-runs", "judgment agent run-tree custody", undefined, {
        expected: { dev: judged.dev, ino: judged.ino, mtimeMs: judged.mtimeMs },
      });
      if (!moved || fs.existsSync(runs)) throw new Error("Judgment run root disappeared before whole-tree custody was verified");
      custodyDestination = moved.destination;
      if (closeFailure) fs.symlinkSync(moved.destination, runs, process.platform === "win32" ? "junction" : "dir");
      else fs.rmSync(root, { recursive: true, force: true });
    } catch (error) { closeFailure = error; failure ??= error; }
  }
  if (fs.existsSync(runs)) {
    const cleanup = closeFailure ? `agent_cleanup_unresolved: retained receipt root ${custodyDestination ?? root}` : String(failure ?? "Judgment run custody failed");
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
    if (!result) throw new Error(cleanup);
    result = { ...result, status: "failed", error: cleanup };
  } else if (fs.existsSync(root)) {
    // No run tree remains. The parent is safe to remove; custody failures above
    // keep the original root rather than allowing this cleanup to unlink it.
    fs.rmSync(root, { recursive: true, force: true });
    if (failure) throw failure;
  }
  if (!result) throw failure ?? new Error("Judgment agent exited without a receipt");
  return result;
}
