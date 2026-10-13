import { execFile } from "node:child_process";
import { PARTICIPANT_NAME_PATTERN } from "./participant-name.js";

// Herdr pane ids look like "w3Q:p1". Anything else is not published.
const HERDR_PANE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9:_.-]{0,63}$/;

/** The Herdr pane this process runs in, when the launcher exported a well-formed one. */
export const herdrPaneId = (environment: NodeJS.ProcessEnv = process.env): string | undefined => {
  const pane = environment.HERDR_PANE_ID?.trim();
  return pane && HERDR_PANE_PATTERN.test(pane) ? pane : undefined;
};

/** One-shot retry offsets from start (smarty-dev#11172); the last pre-minute try is at 55 s. */
export const HERDR_NAME_RETRY_MS: readonly number[] = [5_000, 10_000, 20_000, 35_000, 55_000, 300_000];

/**
 * Bounded, single-flight Herdr name lookup. `start()` performs the initial read and arms the
 * fixed one-shot retry schedule. Each retry checks that the Main is still unnamed and live;
 * finding a name, retirement, or an explicit name clears every remaining timer.
 */
export const herdrNameLookup = (options: {
  read: () => Promise<string | undefined>;
  live: () => boolean;
  eligible: () => boolean;
  onName: (name: string) => void;
}): { start(): void; stop(): void; readonly pending: boolean } => {
  let busy = false;
  let done = false;
  let started = false;
  const timers = new Set<ReturnType<typeof setTimeout>>();
  const stop = (): void => {
    if (done) return;
    done = true;
    for (const timer of timers) clearTimeout(timer);
    timers.clear();
  };
  const run = (): void => {
    if (done || busy) return;
    if (!options.live()) { stop(); return; }
    busy = true;
    void options.read().catch(() => undefined).then((name) => {
      busy = false;
      if (done) return;
      if (!options.live() || !options.eligible()) { stop(); return; }
      if (name) {
        stop();
        options.onName(name);
      } else if (timers.size === 0) stop();
    });
  };
  const arm = (): void => {
    for (const delay of HERDR_NAME_RETRY_MS) {
      let timer: ReturnType<typeof setTimeout>;
      timer = setTimeout(() => {
        timers.delete(timer);
        if (done) return;
        if (!options.live() || !options.eligible()) { stop(); return; }
        run();
      }, delay);
      timer.unref?.();
      timers.add(timer);
    }
  };
  return {
    start: () => {
      if (started || done) return;
      started = true;
      if (!options.live()) { stop(); return; }
      arm();
      run();
    },
    stop,
    get pending() { return !done; },
  };
};

/**
 * The Herdr agent name of this pane (smarty-dev#6758), read once with `herdr agent get <pane>`.
 * No shell, a hard timeout, and any failure (no pane, missing binary, slow, bad JSON, invalid
 * name) yields undefined so the caller keeps "main". Names are unauthenticated display
 * metadata, never identity, routing custody, or authority.
 */
export const readHerdrAgentName = (
  environment: NodeJS.ProcessEnv = process.env,
  timeoutMs = 2_000,
): Promise<string | undefined> => {
  const pane = herdrPaneId(environment);
  if (!pane) return Promise.resolve(undefined);
  const binary = environment.HERDR_BIN_PATH?.trim() || "herdr";
  return new Promise((resolve) => {
    try {
      execFile(binary, ["agent", "get", pane], {
        timeout: timeoutMs,
        killSignal: "SIGKILL",
        maxBuffer: 256 * 1024,
        windowsHide: true,
        env: environment,
      }, (error, stdout) => {
        if (error) return resolve(undefined);
        try {
          const name = (JSON.parse(stdout) as { result?: { agent?: { name?: unknown } } })?.result?.agent?.name;
          const trimmed = typeof name === "string" ? name.trim() : "";
          resolve(PARTICIPANT_NAME_PATTERN.test(trimmed) ? trimmed : undefined);
        } catch { resolve(undefined); }
      }).on("error", () => resolve(undefined));
    } catch { resolve(undefined); }
  });
};
