// Detached temp-root sweep (smarty-dev#2010). AgentManager claims the host's sweep marker and
// starts this file as its own niced process, so no Pi exit and no live event loop waits for a walk
// of every retained run on the host. The walk applies the same retention rules as an in-process
// sweep: expired runs, whitelisted files only, never a live or unresolved run.
import { sweepTempRunRoots, type TempRunSweepRequest } from "./retention.js";

const request = JSON.parse(process.argv[2] ?? "") as TempRunSweepRequest;
sweepTempRunRoots({
  tempRoot: request.tempRoot,
  ...(request.currentRoot ? { currentRoot: request.currentRoot } : {}),
  orphanedTempRunRetentionMs: request.orphanedTempRunRetentionMs,
  oneShotRunRetentionMs: request.oneShotRunRetentionMs,
});
