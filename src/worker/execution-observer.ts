import type { ChildProcess } from "node:child_process";

type Group = {
  exited(): boolean;
  inspectIdle?(): "empty" | "leader-only" | "active";
};

/** Sample active custody, not positively idle native Pi. Native exit is only a
 * membership wake; inherited pipe close remains a separate cleanup receipt. */
export const executionObserver = (child: ChildProcess, group: Group) => {
  let empty = false;
  let nativeIdle = false;
  let timer: ReturnType<typeof setInterval> | undefined;
  const pause = (): void => { clearInterval(timer); timer = undefined; };
  const latchEmpty = (): void => {
    empty = true;
    pause();
    child.removeListener("exit", nativeExit);
    child.removeListener("close", observe);
  };
  const exited = (): boolean => {
    if (!empty && group.exited()) latchEmpty();
    return empty;
  };
  const observe = (): void => {
    if (empty) return;
    try {
      if (nativeIdle && group.inspectIdle) {
        const state = group.inspectIdle();
        if (state === "empty") latchEmpty();
        else if (state === "leader-only") pause();
      } else exited();
    } catch { /* Unknown membership retains sampling; cleanup fails closed. */ }
  };
  const arm = (): void => {
    nativeIdle = false;
    if (!empty) timer ??= setInterval(observe, 100);
  };
  const nativeExit = (): void => { arm(); observe(); };
  arm(); // Unknown runner/handshake is active custody, never positive idle.
  child.once("exit", nativeExit);
  child.once("close", observe);
  return {
    exited,
    arm,
    idle(): void {
      if (empty) return;
      timer ??= setInterval(observe, 100);
      nativeIdle = true;
      observe();
    },
  };
};
