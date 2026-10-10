import type { ChildProcess } from "node:child_process";

type Group = {
  exited(): boolean;
};

/** Sample active custody until the execution group is confirmed empty. Native
 * exit is only a membership wake; inherited pipe close remains a separate
 * cleanup receipt. */
export const executionObserver = (child: ChildProcess, group: Group) => {
  let empty = false;
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
    try { exited(); }
    catch { /* Unknown membership retains sampling; cleanup fails closed. */ }
  };
  const arm = (): void => {
    if (!empty) timer ??= setInterval(observe, 100);
  };
  const nativeExit = (): void => { arm(); observe(); };
  arm(); // Unknown runner/handshake is active custody, never positive idle.
  child.once("exit", nativeExit);
  child.once("close", observe);
  return {
    exited,
    arm,
  };
};
