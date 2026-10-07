const BUILT_IN_RUNNERS = ["pi", "pi-durable", "claude", "veda"];

const editDistance = (left: string, right: string): number => {
  const row = Array.from({ length: right.length + 1 }, (_, index) => index);
  for (let i = 1; i <= left.length; i += 1) {
    let diagonal = row[0]!;
    row[0] = i;
    for (let j = 1; j <= right.length; j += 1) {
      const above = row[j]!;
      row[j] = Math.min(above + 1, row[j - 1]! + 1, diagonal + (left[i - 1] === right[j - 1] ? 0 : 1));
      diagonal = above;
    }
  }
  return row[right.length]!;
};

/**
 * Warning text when the configured default runner is not registered, or
 * undefined when it is. A configured custom id is kept before its extension
 * registers it (load order), so launches fail closed until it does.
 */
export const unregisteredRunnerNotice = (
  runner: string,
  registered: Iterable<string>,
): string | undefined => {
  const known = [...new Set([...BUILT_IN_RUNNERS, ...registered])];
  if (known.includes(runner)) return undefined;
  let suggestion: { id: string; distance: number } | undefined;
  for (const id of known) {
    const distance = editDistance(runner, id);
    if (distance <= 2 && (!suggestion || distance < suggestion.distance)) suggestion = { id, distance };
  }
  return `Fabric agents.runner "${runner}" is not registered, so agent launches fail until an extension registers it.${
    suggestion ? ` Did you mean "${suggestion.id}"?` : ""
  } Registered runners: ${known.join(", ")}.`;
};
