/**
 * smarty-dev#9729's journal-alarm reader contract (setup/factory/fabric_alarm.py at 1c5a493d), restated for tests:
 * only `fabric.alarm.mesh-outage {json}` lines; label a string; state outage|recovered; sinceMs a non-negative int up
 * to 2100-01-01; optional durationMs an int up to 10 years; optional reason a string; extra fields allowed.
 */
export const parse9729 = (line: string): Record<string, unknown> | undefined => {
  const match = /^fabric\.alarm\.mesh-outage (\{.*\})$/.exec(line);
  if (!match) return undefined;
  let value: Record<string, unknown>;
  try { value = JSON.parse(match[1]!) as Record<string, unknown>; } catch { return undefined; }
  const int = (n: unknown, max: number) => typeof n === "number" && Number.isInteger(n) && n >= 0 && n <= max;
  if (typeof value.label !== "string" || (value.state !== "outage" && value.state !== "recovered")) return undefined;
  if (!int(value.sinceMs, Date.UTC(2100, 0, 1))) return undefined;
  if (value.durationMs !== undefined && !int(value.durationMs, 10 * 366 * 24 * 60 * 60 * 1_000)) return undefined;
  if (value.reason !== undefined && typeof value.reason !== "string") return undefined;
  return value;
};
