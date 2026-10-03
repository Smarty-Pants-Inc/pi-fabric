import type { SessionEntry } from "@earendil-works/pi-coding-agent";
import { canonicalizeText } from "./bounds.js";
import {
  compactionRequestBoundsError,
  MAX_PRESERVE_ITEM_CHARS,
  MAX_PRESERVE_ITEMS,
} from "./instructions.js";

// Carry-forward focus: a small, bounded list of facts the deterministic
// compactor renders into EVERY summary until it is cleared. Unlike the
// one-shot `preserve` of a compaction request, the list lives in the session
// log as a custom entry, so the latest entry on the active branch is the
// current value. Reload, restart and tree navigation replay it by reading the
// branch; there is no in-memory copy to lose or to go stale.

export const COMPACTION_CARRY_ENTRY_TYPE = "pi-fabric-compact-carry";
export const MAX_CARRY_ITEMS = MAX_PRESERVE_ITEMS;
export const MAX_CARRY_ITEM_CHARS = MAX_PRESERVE_ITEM_CHARS;

export interface CompactionCarryEntryData {
  version: 1;
  items: string[];
}

export interface CompactionCarryUpdate {
  items?: string[];
  add?: string[];
  remove?: string[];
  clear?: boolean;
}

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === "string");

const carryBoundsError = (items: readonly string[]): string | undefined => {
  if (items.length > MAX_CARRY_ITEMS) return `compact carry exceeds ${MAX_CARRY_ITEMS} items`;
  if (items.some((item) => item.trim() === "")) return "compact carry items must be non-empty";
  return compactionRequestBoundsError({ preserve: [...items] })?.message.replace(
    "typed compaction preserve",
    "compact carry",
  );
};

/** Validates a persisted entry; a malformed one fails closed to no carry. */
export const decodeCarryEntryData = (data: unknown): string[] | undefined => {
  if (data === null || typeof data !== "object" || Array.isArray(data)) return undefined;
  const record = data as Record<string, unknown>;
  if (record.version !== 1 || !isStringArray(record.items)) return undefined;
  return carryBoundsError(record.items) === undefined ? [...record.items] : undefined;
};

// The latest carry entry on the branch is authoritative, including an empty
// (cleared) list. A malformed latest entry yields an empty list rather than
// resurrecting an older value the user may have replaced.
export const latestCarryItems = (entries: readonly SessionEntry[]): string[] => {
  for (let index = entries.length - 1; index >= 0; index--) {
    const entry = entries[index]!;
    if (entry.type !== "custom" || entry.customType !== COMPACTION_CARRY_ENTRY_TYPE) continue;
    return decodeCarryEntryData(entry.data) ?? [];
  }
  return [];
};

// Order: clear, replace, remove, then add. Exact-string removal; additions
// skip items already present so repeated adds are idempotent.
export const applyCarryUpdate = (
  current: readonly string[],
  update: CompactionCarryUpdate,
): string[] => {
  let next = update.clear === true ? [] : [...current];
  if (update.items !== undefined) next = [...update.items];
  if (update.remove !== undefined) {
    const removed = new Set(update.remove);
    next = next.filter((item) => !removed.has(item));
  }
  for (const item of update.add ?? []) {
    if (!next.includes(item)) next.push(item);
  }
  next = next.filter((item, index) => next.indexOf(item) === index);
  const error = carryBoundsError(next);
  if (error) throw new Error(error);
  return next;
};

export const isCarryUpdate = (update: CompactionCarryUpdate): boolean =>
  update.clear === true
  || update.items !== undefined
  || update.add !== undefined
  || update.remove !== undefined;

export const sameCarryItems = (left: readonly string[], right: readonly string[]): boolean =>
  left.length === right.length && left.every((item, index) => item === right[index]);

/** Summary lines for the protected `[Carry Forward]` block. */
export const carryRequestLines = (items: readonly string[]): string[] =>
  items.flatMap((item, index) => {
    const text = canonicalizeText(item, MAX_CARRY_ITEM_CHARS).text;
    return text ? [`- ${text} [carry:${index}]`] : [];
  });
