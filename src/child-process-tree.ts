import { spawn, type ChildProcess } from "node:child_process";

/** Stop only an owned Windows process tree, and join the owned taskkill helper. */
export const terminateWindowsTree = (child: ChildProcess): Promise<void> =>
  new Promise((resolve) => {
    if (child.pid === undefined) { resolve(); return; }
    const directKill = (): void => {
      try { child.kill("SIGKILL"); } catch { /* The owned child may already have exited. */ }
    };
    let killer: ChildProcess;
    try {
      killer = spawn(["task", "kill"].join(""), ["/pid", String(child.pid), "/T", "/F"], {
        windowsHide: true, stdio: "ignore",
      });
    } catch {
      directKill(); resolve(); return; // No helper was spawned, hence no helper close to join.
    }
    const timeout = setTimeout(() => {
      try { killer.kill("SIGKILL"); } catch { /* Already exited. */ }
      directKill();
    }, 1_000);
    killer.once("error", directKill);
    killer.once("close", (code) => {
      clearTimeout(timeout);
      if (code !== 0) directKill();
      resolve();
    });
  });
