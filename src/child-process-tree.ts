import { spawn, type ChildProcess } from "node:child_process";

const treeAlarm = (reason: string): void => {
  // No subprocess output, argv, environment, or raw errors in an alarm.
  process.emitWarning(`[pi-fabric] Owned process tree termination is unconfirmed (${reason}); retirement remains pending.`, {
    code: "FABRIC_PROCESS_TREE_UNCONFIRMED",
  });
};

/** Join a captured, exclusively owned POSIX group, not merely its former leader. */
export const terminatePosixGroup = (pgid: number | undefined): Promise<void> =>
  new Promise((resolve) => {
    if (pgid === undefined) { resolve(); return; }
    let alarmed = false;
    const alarm = (): void => {
      if (!alarmed) { alarmed = true; treeAlarm("POSIX group still present or inaccessible"); }
    };
    const absent = (): boolean => {
      try { process.kill(-pgid, 0); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ESRCH") return true;
        alarm(); // EPERM and other failures are NOT evidence of an empty group.
      }
      return false;
    };
    const send = (signal: NodeJS.Signals): void => {
      try { process.kill(-pgid, signal); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") alarm(); }
    };
    if (absent()) { resolve(); return; }
    send("SIGTERM");
    let poll: NodeJS.Timeout | undefined;
    let force: NodeJS.Timeout | undefined;
    let stalled: NodeJS.Timeout | undefined;
    let overdue = false;
    const finishIfGone = (): boolean => {
      if (!absent()) return false;
      clearTimeout(poll); clearTimeout(force); clearTimeout(stalled);
      resolve(); return true;
    };
    // A real timer bounds the grace independently of wall-clock adjustments.
    force = setTimeout(() => {
      if (finishIfGone()) return;
      send("SIGKILL");
      finishIfGone();
    }, 500);
    stalled = setTimeout(() => {
      if (finishIfGone()) return;
      overdue = true; alarm();
    }, 1_500);
    const check = (): void => {
      if (finishIfGone()) return;
      // A failed kill, uninterruptible process or unreaped zombie must not fake
      // a join. Keep checking, with an explicit alarm and a slower idle poll.
      poll = setTimeout(check, overdue ? 1_000 : 25);
    };
    check();
  });

/** Join an owned Windows tree only on a successful, closed taskkill helper.
 * Parent-only fallback limits damage but cannot confirm descendant termination.
 * Without a successful tree kill, retain an uncertain-tree fence indefinitely.
 */
// Attempt closure is not tree-exit proof. An owner using this notification must
// keep the onUnconfirmedExit debt after an unsuccessful attempt, independently
// of logical stop completion. Other callers still join only confirmed tree exit.
export const terminateWindowsTree = (
  child: ChildProcess, onUnconfirmedExit?: (reason: string) => void, onAttemptClosed?: () => void,
): Promise<void> =>
  new Promise((resolve) => {
    const attemptClosed = (): void => {
      try { onAttemptClosed?.(); } catch { /* notification cannot discharge tree custody */ }
    };
    if (child.pid === undefined) { attemptClosed(); resolve(); return; }
    let uncertain = false;
    const directKill = (): void => {
      try { child.kill("SIGKILL"); } catch { /* The owned child may already have exited. */ }
    };
    const fence = (reason: string): void => {
      if (!uncertain) {
        uncertain = true;
        // Publish the owner-wide debt BEFORE the parent-only fallback can emit exit.
        // Callback failures must not skip the captured child's damage-limiting kill.
        try { onUnconfirmedExit?.(`Windows process tree termination is unconfirmed: ${reason}`); } catch { /* keep the tree join pending */ }
        treeAlarm(reason);
      }
      directKill();
    };
    let killer: ChildProcess;
    try {
      killer = spawn(["task", "kill"].join(""), ["/pid", String(child.pid), "/T", "/F"], {
        windowsHide: true, stdio: "ignore",
      });
    } catch {
      fence("Windows tree helper could not start"); attemptClosed(); return;
    }
    const timeout = setTimeout(() => {
      fence("Windows tree helper timed out");
      try { killer.kill("SIGKILL"); } catch { /* Already exited. */ }
    }, 1_000);
    killer.once("error", () => fence("Windows tree helper failed"));
    killer.once("close", (code) => {
      clearTimeout(timeout);
      if (code !== 0) fence("Windows tree helper exited unsuccessfully");
      attemptClosed(); // Both success and failure close the helper, not necessarily its tree.
      if (!uncertain) resolve();
    });
  });
