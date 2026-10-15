import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import fs from "node:fs";
import path from "node:path";

import { applyRunBashDefaults } from "./actor-bash-timeout.js";

// smarty-dev#8257: a capped task (the launcher exports FABRIC_TASK_MEMORY_MAX only after it admits
// the capped cgroup receipt) must not start a nested systemd unit: a new transient unit leaves the
// task's memory-capped leaf. The launcher puts a task-local `systemd-run` shim first on PATH
// (FABRIC_TASK_SYSTEMD_RUN_WRAPPER); it runs `--user --scope` commands in place inside the leaf.
// Plain PATH `systemd-run` is allowed only while that shim is truthful (PATH really resolves to it)
// and nothing in the command rewrites PATH lookup; an absolute or path-qualified systemd-run that
// bypasses the shim is refused. This is a best-effort guard on the native bash tool's command
// words for supported tools, not a sandbox: scripts or same-UID native code can still reach systemd.
// Uncapped fallback runs and Mains never carry the variable and keep the same commands allowed.
const WRAPPERS = new Set([
  "sudo", "doas", "env", "exec", "command", "builtin", "nohup", "nice", "ionice", "timeout", "setsid",
  "stdbuf", "xargs", "time", "chrt", "taskset", "eval", "sh", "bash", "dash", "zsh", "ksh", "busybox",
]);

const isSystemdRun = (word: string): boolean => word === "systemd-run" || word.endsWith("/systemd-run");

// Text that can change which `systemd-run` a plain name resolves to: PATH edits, hash/alias,
// privilege wrappers with their own secure PATH, `command -p`. `env -i/-u/-S` or `env -` are
// checked per segment (only env's own options, not the target command's).
const PATH_REWRITE = /\bPATH\s*=|\b(?:unset|read)\s+(?:-[A-Za-z]+\s+)*PATH\b|\b(?:hash|alias|sudo|doas|busybox)\b|\bcommand\s+-[A-Za-z]*p/;
const ENV_RESET = /^(?:-|--ignore-environment|--unset(?:=.*)?|--split-string(?:=.*)?|-[A-Za-z]*[iuS].*)$/;
const envResetsPath = (words: readonly string[]): boolean => {
  for (const word of words) {
    if (ENV_RESET.test(word)) return true;
    if (!word.startsWith("-") && !/^[A-Za-z_][A-Za-z0-9_]*=/.test(word)) return false;
  }
  return false;
};

/**
 * True when a shell command runs `systemd-run` in command position (plain, absolute or quoted)
 * other than through `allowedWrapper`, the truthful task-local shim (plain name or its exact path).
 */
export const nestedSystemdRun = (command: string, allowedWrapper?: string): boolean => {
  // Quote/escape removal makes "systemd-run", '/usr/bin/systemd-run' and sys\temd-run plain words.
  const plain = command.replace(/\\\n/g, "").replace(/["'\\]/g, "");
  const shimmed = allowedWrapper !== undefined && !PATH_REWRITE.test(plain);
  for (const segment of plain.split(/\$\(|[;&|()`{}\n!]/)) {
    const words = segment.trim().split(/\s+/).filter(Boolean);
    let index = 0;
    while (index < words.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[index]!)) index += 1;
    const head = words[index];
    if (head === undefined) continue;
    const viaShim = shimmed && !(path.basename(head) === "env" && envResetsPath(words.slice(index + 1)));
    const escapes = (word: string): boolean =>
      isSystemdRun(word) && !(viaShim && (word === "systemd-run" || word === allowedWrapper));
    if (escapes(head)) return true;
    // Through a wrapper (env, sudo, timeout, bash -c ...), any later systemd-run word counts.
    if (WRAPPERS.has(head.slice(head.lastIndexOf("/") + 1)) && words.slice(index + 1).some(escapes)) return true;
  }
  return false;
};

/** The task-local shim path when it is what PATH resolves `systemd-run` to; otherwise undefined. */
export const truthfulSystemdRunWrapper = (env: Readonly<Record<string, string | undefined>>): string | undefined => {
  const wrapper = env.FABRIC_TASK_SYSTEMD_RUN_WRAPPER;
  if (!wrapper || !path.isAbsolute(wrapper) || path.basename(wrapper) !== "systemd-run") return undefined;
  for (const directory of (env.PATH ?? "").split(path.delimiter)) {
    if (!path.isAbsolute(directory)) continue; // relative entries depend on cwd: never the shim
    const candidate = path.join(directory, "systemd-run");
    try { fs.accessSync(candidate, fs.constants.X_OK); } catch { continue; }
    try { return fs.realpathSync(candidate) === fs.realpathSync(wrapper) ? wrapper : undefined; } catch { return undefined; }
  }
  return undefined;
};

/** The capped task's block reason, or undefined when this run is uncapped or the command is allowed. */
export const cappedTaskBashBlockReason = (
  env: Readonly<Record<string, string | undefined>>,
  command: unknown,
): string | undefined => {
  const cap = env.FABRIC_TASK_MEMORY_MAX;
  if (!cap || !/^[1-9][0-9]*$/.test(cap) || typeof command !== "string" || !nestedSystemdRun(command)) return undefined;
  const wrapper = truthfulSystemdRunWrapper(env);
  if (!nestedSystemdRun(command, wrapper)) return undefined;
  const cgroup = env.FABRIC_TASK_CGROUP ? ` in ${env.FABRIC_TASK_CGROUP}` : "";
  return `Blocked nested systemd-run: this task already runs under its own memory cap (memory.max=${cap} bytes, swap 0${cgroup}). `
    + "A new systemd unit would leave that cap. "
    + (wrapper
      ? "Use plain `systemd-run --user --scope [-p MemoryMax=...] -- CMD` without changing PATH; the task-local shim runs CMD in place under the cap."
      : "Run the command directly; it inherits the task's cap.");
};

// smarty-dev#2184 (review finding 4): the Fabric extension's tool_call hook is the only other place
// that sets a run's bash defaults, and a native-tool run (extensions: false) never loads it. The
// worker loads this defaults-only hook with `-e` for every Pi run it launches (#6137: task agents
// too); `-e` still loads under --no-extensions. It adds no tool and no authority, and it must not
// import the Fabric graph. When Fabric is loaded too, both hooks run; each acts only when no timeout
// is set and the input lacks the hook-set private wrapping metadata, so the second does nothing.
// A marker in caller-supplied command text is never trusted. The worker loads
// this one after Fabric's `-e`, so Fabric's foreground-wait guard still judges the caller's command.
export default function actorBashHook(pi: ExtensionAPI): void {
  pi.on("tool_call", (event) => {
    if (event.toolName !== "bash") return undefined;
    const reason = cappedTaskBashBlockReason(process.env, (event.input as { command?: unknown }).command);
    if (reason) return { block: true, reason };
    applyRunBashDefaults(process.env, event.input as Parameters<typeof applyRunBashDefaults>[1]);
    return undefined;
  });
}
