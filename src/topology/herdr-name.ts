import { execFile } from "node:child_process";
import { PARTICIPANT_NAME_PATTERN } from "./participant-name.js";

// Herdr pane ids look like "w3Q:p1". Anything else is not published.
const HERDR_PANE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9:_.-]{0,63}$/;

/** The Herdr pane this process runs in, when the launcher exported a well-formed one. */
export const herdrPaneId = (environment: NodeJS.ProcessEnv = process.env): string | undefined => {
  const pane = environment.HERDR_PANE_ID?.trim();
  return pane && HERDR_PANE_PATTERN.test(pane) ? pane : undefined;
};

/**
 * The Herdr agent name of this pane (smarty-dev#6758), read once with `herdr agent get <pane>`.
 * No shell, a hard timeout, and any failure (no pane, missing binary, slow, bad JSON, invalid
 * name) yields undefined so the caller keeps "main". Names are selectors, never authority.
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
