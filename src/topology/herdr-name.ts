import { execFile } from "node:child_process";
import { PARTICIPANT_NAME_PATTERN } from "./participant-name.js";

// Herdr pane ids look like "w3Q:p1". Anything else is not published.
const HERDR_PANE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9:_.-]{0,63}$/;

/** The Herdr pane this process runs in, when the launcher exported a well-formed one. */
export const herdrPaneId = (environment: NodeJS.ProcessEnv = process.env): string | undefined => {
  const pane = environment.HERDR_PANE_ID?.trim();
  return pane && HERDR_PANE_PATTERN.test(pane) ? pane : undefined;
};

/** Backoffs after a failed Herdr lookup (smarty-dev#11077); after the last one the Main stays "main". */
export const HERDR_NAME_RETRY_MS: readonly number[] = [5_000, 15_000, 60_000, 300_000];

/**
 * Bounded, single-flight Herdr name lookup. `start()` runs the initial read; `poke()` is called
 * from an existing participant refresh (no own timer or poll) and re-reads only once the next
 * backoff is due and `eligible()` (the Main is still unnamed) holds. `live()` is checked before
 * and after every read; a retired lifecycle or a found name stops all further reads.
 */
export const herdrNameLookup = (options: {
  read: () => Promise<string | undefined>;
  live: () => boolean;
  eligible: () => boolean;
  onName: (name: string) => void;
}): { start(): void; poke(): void; readonly pending: boolean } => {
  let failures = 0;
  let nextAt: number | undefined;
  let busy = false;
  let done = false;
  const run = (): void => {
    if (done || busy) return;
    if (!options.live()) { done = true; return; }
    busy = true;
    nextAt = undefined;
    void options.read().catch(() => undefined).then((name) => {
      busy = false;
      if (!options.live()) { done = true; return; }
      if (name) { done = true; options.onName(name); return; }
      const delay = HERDR_NAME_RETRY_MS[failures++];
      if (delay === undefined) done = true;
      else nextAt = Date.now() + delay;
    });
  };
  return {
    start: run,
    poke: () => {
      if (done || busy || nextAt === undefined || Date.now() < nextAt) return;
      if (!options.live()) { done = true; return; }
      if (!options.eligible()) return;
      run();
    },
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
