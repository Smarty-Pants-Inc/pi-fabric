/** Capped exponential full jitter. Callers retain their own absolute deadlines. */
export const retryDelayMs = (
  attempt: number,
  baseMs: number,
  capMs: number,
  remainingMs = Number.POSITIVE_INFINITY,
  random: () => number = Math.random,
): number => {
  const ceiling = Math.min(capMs, baseMs * 2 ** Math.min(30, Math.max(0, attempt)));
  return Math.min(Math.max(0, remainingMs), Math.floor(random() * ceiling));
};
