import { execFile } from "node:child_process";
import fs from "node:fs";

export const SYSTEMD_NOTIFY_PATH = "/usr/bin/systemd-notify";
const NOTIFY_TIMEOUT_MS = 5_000;
let failureLogged = false;

export interface SystemdWatchdogNotifyOptions {
  env?: NodeJS.ProcessEnv;
  platform?: NodeJS.Platform;
  execFile?: typeof execFile;
  log?: (message: string) => void;
}

/** Best-effort systemd watchdog ping. The bridge calls this only after its peer round trip succeeds. */
export const systemdWatchdogNotify = (options: SystemdWatchdogNotifyOptions = {}): void => {
  const env = options.env ?? process.env;
  if ((options.platform ?? process.platform) !== "linux" || !env.NOTIFY_SOCKET) return;
  try {
    fs.accessSync(SYSTEMD_NOTIFY_PATH, fs.constants.X_OK);
  } catch {
    return;
  }

  const reportFailure = (): void => {
    if (failureLogged) return;
    failureLogged = true;
    const log = options.log ?? (message => process.stderr.write(`${message}\n`));
    log("mesh-bridge: systemd-notify failed; watchdog notification skipped");
  };
  try {
    (options.execFile ?? execFile)(SYSTEMD_NOTIFY_PATH, ["WATCHDOG=1"], {
      env,
      timeout: NOTIFY_TIMEOUT_MS,
      windowsHide: true,
    }, error => { if (error) reportFailure(); });
  } catch {
    reportFailure();
  }
};
