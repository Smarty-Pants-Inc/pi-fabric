import fs from "node:fs";
import path from "node:path";

/** Subscribe BEFORE the first marker read so atomic creation/rename cannot be
 * missed. Watch failure is non-admission, never a timer-driven polling fallback.
 * Callers must join the failed native launcher before considering direct spawn. */
export const waitForScopeAdmission = (
  marker: string, closed: Promise<void>, signal?: AbortSignal, timeoutMs = 5_000,
): Promise<boolean> => new Promise(resolve => {
  let watcher: fs.FSWatcher | undefined, timer: ReturnType<typeof setTimeout> | undefined;
  let settled = false;
  const finish = (admitted: boolean): void => {
    if (settled) return;
    settled = true; clearTimeout(timer); watcher?.close();
    signal?.removeEventListener("abort", abort); resolve(admitted);
  };
  const abort = (): void => finish(false);
  const inspect = (): void => {
    if (settled) return;
    try { if (fs.existsSync(marker)) finish(true); }
    catch { finish(false); }
  };
  if (signal?.aborted) { finish(false); return; }
  try {
    watcher = fs.watch(path.dirname(marker), { persistent: false }, (_event, name) => {
      if (name === null || String(name) === path.basename(marker)) inspect();
    });
    watcher.on("error", () => finish(false));
  } catch { finish(false); return; }
  signal?.addEventListener("abort", abort, { once: true });
  timer = setTimeout(() => { inspect(); finish(false); }, timeoutMs);
  void closed.then(() => { inspect(); finish(false); }, () => finish(false));
  inspect();
});
