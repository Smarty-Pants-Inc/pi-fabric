/** Parent-manager/worker CLI, status, lifecycle and steering contract.
 * Bump on an incompatible boundary change; installed releases advertise this
 * value in dist/worker-protocol.json without executing their code in the parent.
 */
// v2 requires the native final-answer receipt, receipt-time manager barrier,
// and attributed per-sender message.refused handling. Mixing v1/v2 can revive
// terminal tasks or silently discard mandatory queued-input refusal notices.
// v3 requires the task's --tmpdir contract. A v2 worker can silently ignore it,
// leaving TMPDIR outside the default writable roots despite accepting the policy.
// v4 requires manager-observed internal Pi restarts. Never select a v3 worker
// for a v4 parent: it can silently restart without the host callback.
export const WORKER_PROTOCOL_VERSION = 4;

export interface PlacementRestartRequest {
  type: "placement.restart";
  launchId: string;
  generation: number;
}
export type PlacementRestartDecision = "BASELINE" | "REFUSE";

const identity = (value: Record<string, unknown>): boolean =>
  typeof value.launchId === "string" && value.launchId.length > 0 && value.launchId.length <= 200 &&
  !/[\u0000-\u001f\u007f]/u.test(value.launchId) && Number.isSafeInteger(value.generation) && Number(value.generation) > 0;
const record = (value: unknown): value is Record<string, unknown> =>
  Boolean(value && typeof value === "object" && !Array.isArray(value));
export const isPlacementRestartRequest = (value: unknown): value is PlacementRestartRequest =>
  record(value) && value.type === "placement.restart" && identity(value) &&
  Object.keys(value).length === 3 && Object.keys(value).every(key => ["type", "launchId", "generation"].includes(key));

/** Private worker IPC only. BASELINE is not PLACE and contains no launch authority.
 * The event path is the normal completion; the one-shot 2s deadline bounds a
 * missing/legacy parent. No late frame survives listener cleanup. */
export const requestPlacementRestart = (
  channel: Pick<NodeJS.Process, "send" | "connected" | "on" | "removeListener">,
  request: PlacementRestartRequest,
  signal?: AbortSignal,
  enforcing = false,
): Promise<boolean> => new Promise(resolve => {
  if (!isPlacementRestartRequest(request) || !channel.connected || !channel.send || signal?.aborted) { resolve(false); return; }
  const { launchId, generation } = request;
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const finish = (allowed: boolean): void => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    channel.removeListener("message", message);
    channel.removeListener("disconnect", refused);
    signal?.removeEventListener("abort", refused);
    resolve(allowed && !signal?.aborted && channel.connected && !enforcing);
  };
  const refused = (): void => finish(false);
  const message = (value: unknown): void => {
    if (!record(value) || value.type !== "placement.restart.decision") return;
    // Another generation's response cannot complete this request. A malformed
    // correlated response refuses it; neither can be upgraded to a PLACE.
    if (value.launchId !== launchId || value.generation !== generation) return;
    finish(Object.keys(value).length === 4 && Object.keys(value).every(key =>
      ["type", "launchId", "generation", "decision"].includes(key)) && value.decision === "BASELINE");
  };
  channel.on("message", message);
  channel.on("disconnect", refused);
  signal?.addEventListener("abort", refused, { once: true });
  timer = setTimeout(refused, 2_000);
  try { channel.send({ type: "placement.restart", launchId, generation }, error => { if (error) refused(); }); }
  catch { refused(); }
  if (signal?.aborted || !channel.connected) refused();
});

/** Shared by scoped and legacy/Windows process transports, independent of the
 * POSIX execution-custody protocol. Captures only the host callback and fences. */
export const attachPlacementRestartHandler = (
  child: import("node:child_process").ChildProcess,
  callback: ((request: PlacementRestartRequest) => Promise<PlacementRestartDecision>) | undefined,
  signal?: AbortSignal,
  authorize?: () => boolean,
): (() => void) => {
  // Ordinary workers must not acquire extra lifecycle listeners when there is
  // no local restart callback (including remote one-shot launches).
  if (!callback) return () => undefined;
  let active = true;
  const revoke = (): void => { active = false; };
  child.once("exit", revoke);
  child.once("disconnect", revoke);
  signal?.addEventListener("abort", revoke, { once: true });
  child.once("close", () => { revoke(); signal?.removeEventListener("abort", revoke); });
  const allowed = (): boolean => {
    try { return active && child.connected && !signal?.aborted && authorize?.() !== false; }
    catch { return false; }
  };
  child.on("message", (value: unknown) => {
    if (!record(value) || value.type !== "placement.restart" || !identity(value)) return;
    const { launchId, generation } = value as unknown as PlacementRestartRequest;
    const request: PlacementRestartRequest = { type: "placement.restart", launchId, generation };
    const valid = isPlacementRestartRequest(value);
    void (async () => {
      let decision: PlacementRestartDecision = "REFUSE";
      if (valid && allowed() && callback) {
        try { if (await callback(request) === "BASELINE" && allowed()) decision = "BASELINE"; }
        catch { /* no host answer is permission */ }
      }
      if (!active || !child.connected) return;
      try { child.send({ type: "placement.restart.decision", launchId, generation, decision }, () => undefined); }
      catch { /* disconnected workers cannot consume authority */ }
    })();
  });
  return revoke;
};
