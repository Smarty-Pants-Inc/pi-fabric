/**
 * fs.watch failure handling (smarty-dev#5247).
 *
 * On Linux, fs.watch is inotify. When the per-user inotify watch budget
 * (fs.inotify.max_user_watches) is exhausted, inotify_add_watch(2) fails with ENOSPC and Node
 * reports `ENOSPC: System limit for number of file watchers reached, watch '<path>'`. That is
 * the same errno as a full disk, so the symptom reads as "no space left" on a host with
 * terabytes free. No byte of storage is involved; the fix is to poll, and to say so precisely.
 */

/** True for an fs.watch error caused by the inotify watch limit rather than storage. */
export function isWatchLimitError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const { code, syscall, message } = error as NodeJS.ErrnoException;
  if (code !== "ENOSPC") return typeof message === "string" && /file watchers/i.test(message);
  // A watch error is the only ENOSPC fs.watch can raise; accept a missing syscall (Bun, mocks)
  // but never relabel a write/open ENOSPC, which is genuinely out of space.
  return syscall === undefined || syscall === "watch" || syscall === "inotify_add_watch";
}

/** One line naming the real cause and the degraded mode. */
export function watchFallbackMessage(label: string, target: string, error: unknown, pollMs: number): string {
  const interval = `${Math.max(1, Math.round(pollMs))} ms`;
  if (isWatchLimitError(error)) {
    return `[pi-fabric] ${label}: fs.watch(${target}) failed with ENOSPC: inotify watch limit, not disk space ` +
      `(fs.inotify.max_user_watches is exhausted for this user). Falling back to polling every ${interval}; ` +
      `no events or writes are lost. To restore instant wakeups, raise the limit, e.g. ` +
      `\`sudo sysctl fs.inotify.max_user_watches=524288\` (persist it under /etc/sysctl.d).`;
  }
  const detail = error instanceof Error ? error.message : String(error);
  return `[pi-fabric] ${label}: fs.watch(${target}) failed (${detail}); falling back to polling every ${interval}.`;
}

const warned = new Set<string>();

/**
 * Warn once per process per label and cause; every monitor restart would otherwise repeat it.
 * Never throws: a diagnostic must not turn a degraded watcher into a crash.
 */
export function warnWatchFallback(label: string, target: string, error: unknown, pollMs: number): boolean {
  const key = `${label}\0${isWatchLimitError(error) ? "limit" : "other"}`;
  if (warned.has(key)) return false;
  warned.add(key);
  try { console.warn(watchFallbackMessage(label, target, error, pollMs)); } catch {}
  return true;
}

/** Test seam: forget which warnings were printed. */
export function resetWatchFallbackWarnings(): void { warned.clear(); }
