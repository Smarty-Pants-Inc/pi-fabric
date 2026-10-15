import type { FabricActorSlotClaim } from "../actors/types.js";

export const SLOT_IMPORT_USAGE = "Usage: /fabric import <global-actor-name-or-id> [as <new-name>] [--slot-claim KEY --slot-version N --idempotency-key KEY]";

/** Per-materialization inputs only; reusable templates never carry claim authority. */
export function parseSlotImport(argumentsList: readonly string[]): {
  key?: string; as?: string; slotClaim?: FabricActorSlotClaim; idempotencyKey?: string;
} {
  const positional: string[] = [];
  let key: string | undefined, version: number | undefined, requestKey: string | undefined;
  const stableKey = (flag: string, value: string): string => {
    if (!value || value.length > 256 || /[\s\x00-\x1f\x7f]/.test(value)) {
      throw new Error(`${flag} must be a nonempty stable key of 1 to 256 non-whitespace characters`);
    }
    return value;
  };
  const seen = new Set<string>();
  for (let index = 0; index < argumentsList.length; index++) {
    const argument = argumentsList[index]!;
    if (argument === "--slot-claim" || argument === "--slot-version" || argument === "--idempotency-key") {
      if (seen.has(argument)) throw new Error(`${argument} must be supplied only once`);
      seen.add(argument);
      const value = argumentsList[++index];
      if (value === undefined || value.startsWith("--")) throw new Error(`${argument} requires a value`);
      if (argument === "--slot-claim") key = stableKey(argument, value);
      else if (argument === "--idempotency-key") requestKey = stableKey(argument, value);
      else {
        if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
          throw new Error("--slot-version must be a positive safe integer");
        }
        version = Number(value);
      }
    } else if (argument.startsWith("--")) throw new Error(`Unknown import option: ${argument}`);
    else positional.push(argument);
  }
  if (!positional[0]) return {};
  if ((key !== undefined) !== (version !== undefined)) throw new Error("--slot-claim and --slot-version must be supplied together");
  if (key !== undefined && requestKey === undefined) throw new Error("Protected import requires --idempotency-key");
  if (positional.length !== 1 && !(positional.length === 3 && positional[1] === "as")) throw new Error(SLOT_IMPORT_USAGE);
  return { key: positional[0], ...(positional[1] === "as" ? { as: positional[2] } : {}),
    ...(key !== undefined && version !== undefined ? { slotClaim: { key, ifVersion: version } } : {}),
    ...(requestKey !== undefined ? { idempotencyKey: requestKey } : {}) };
}
