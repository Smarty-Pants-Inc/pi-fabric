// smarty-dev#854: agents blocked their own sessions in foreground waits (a 70-minute
// `for … seq 1 14; sleep 300` loop). A blocked session takes no steer or ask and reads as
// working, so the stall is invisible. This refuses a foreground bash command whose sleeps add up
// to more than FOREGROUND_WAIT_LIMIT_S.
// ponytail: a textual estimate, not a shell parser. It covers the waits seen live: a long sleep,
// a sleep in a counted `for` loop, and a sleep in an unbounded `while`/`until` loop. A wait
// hidden in a script or a program passes. Heredoc bodies and quoted strings count as data,
// except a string run by `bash -c`/`sh -c`. Revisit if agents route around it.

export const FOREGROUND_WAIT_LIMIT_S = 300;

const UNIT_SECONDS: Record<string, number> = { "": 1, s: 1, m: 60, h: 3_600, d: 86_400 };

const duration = (value: string, unit: string | undefined): number =>
  Number(value) * (UNIT_SECONDS[unit ?? ""] ?? 1);

// The command text that runs: heredoc bodies and quoted data removed, `-c` scripts kept.
const executableText = (command: string): string => {
  let text = command.replace(/<<-?\s*(['"]?)(\w+)\1[^\n]*\n[\s\S]*?\n\s*\2\s*(?:\n|$)/g, "\n");
  text = text.replace(/(-c\s+)?(['"])((?:\\.|(?!\2)[^\\])*)\2/g, (_match, script: string | undefined, _quote, body: string) =>
    script ? ` ${body} ` : " \"\" ");
  return text;
};

// A command that detaches its work does not block the session.
const detached = (text: string): boolean =>
  /\b(?:nohup|setsid|disown)\b/.test(text) || /(?:^|[^&|>])&\s*(?:$|\n|;|\))/.test(text);

/**
 * The foreground wait a bash command is expected to make, in seconds: the sum of its sleeps,
 * times the count of an enclosing counted loop; Infinity for a sleep in an unbounded loop. A
 * `timeout N` in the command, or the tool call's own timeout, bounds it.
 */
export const foregroundWaitSeconds = (command: string, toolTimeoutS?: number): number => {
  const text = executableText(command);
  if (detached(text)) return 0;
  const sleeps = [...text.matchAll(/(?:^|[\s;&|(`{])sleep\s+(\d+(?:\.\d+)?)([smhd])?(?=[\s;&|)`}]|$)/g)]
    .map((match) => duration(match[1]!, match[2]));
  if (sleeps.length === 0) return 0;
  let total = sleeps.reduce((sum, seconds) => sum + seconds, 0);
  let count = 1;
  for (const match of text.matchAll(/\bfor\b[^;\n]*?\bin\s+(?:\$\(\s*seq\s+(\d+)(?:\s+(\d+))?(?:\s+(\d+))?\s*\)|\{(\d+)\.\.(\d+)\})/g)) {
    if (match[4] !== undefined) count *= Math.abs(Number(match[5]) - Number(match[4])) + 1;
    else if (match[3] !== undefined) count *= Math.floor((Number(match[3]) - Number(match[1])) / Math.max(1, Number(match[2]))) + 1;
    else if (match[2] !== undefined) count *= Number(match[2]) - Number(match[1]) + 1;
    else count *= Number(match[1]);
  }
  for (const match of text.matchAll(/\bfor\s*\(\(\s*\w+\s*=\s*(\d+)\s*;\s*\w+\s*(<=?)\s*(\d+)\s*;/g)) {
    count *= Number(match[3]) - Number(match[1]) + (match[2] === "<=" ? 1 : 0);
  }
  total *= Math.max(1, count);
  if (/\b(?:while|until)\b/.test(text)) total = Number.POSITIVE_INFINITY;
  const bounds = [...text.matchAll(/\btimeout\s+(?:-\S+\s+)*(\d+(?:\.\d+)?)([smhd])?\b/g)]
    .map((match) => duration(match[1]!, match[2]));
  if (toolTimeoutS !== undefined && toolTimeoutS > 0) bounds.push(toolTimeoutS);
  return bounds.length > 0 ? Math.min(total, ...bounds) : total;
};

/** A refusal for a foreground wait over the limit, or undefined to let the command run. */
export const foregroundWaitRefusal = (command: string, toolTimeoutS?: number): string | undefined => {
  const seconds = foregroundWaitSeconds(command, toolTimeoutS);
  if (seconds <= FOREGROUND_WAIT_LIMIT_S) return undefined;
  const size = Number.isFinite(seconds) ? `about ${Math.round(seconds / 6) / 10} min` : "an unbounded wait";
  return `Fabric refused this command: its foreground wait is ${size} (limit ${FOREGROUND_WAIT_LIMIT_S / 60} min, ` +
    "smarty-dev#854). A session blocked in a wait takes no steer or ask and shows as working. Instead, start the " +
    "poll detached (setsid or nohup … &) and end your turn, wait for a mesh github.* event or an agent's " +
    "completion message, or bound a short wait with `timeout 300`.";
};
