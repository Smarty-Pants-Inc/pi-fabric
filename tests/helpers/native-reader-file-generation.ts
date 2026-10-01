import fs from "node:fs";
import { expect } from "vitest";

// Numeric Windows file IDs can exceed MAX_SAFE_INTEGER. Keep identity probes
// exact, including descriptor probes inside readSync fault-injection spies.
export const fileIdentity = (file: string) => {
  const { dev, ino } = fs.statSync(file, { bigint: true });
  return `${dev}:${ino}`;
};
export const descriptorIdentity = (descriptor: number) => {
  const { dev, ino } = fs.fstatSync(descriptor, { bigint: true });
  return `${dev}:${ino}`;
};

let generation = 0;
const retainGeneration = (file: string): string => {
  const retained = `${file}.retained-${generation++}`;
  fs.renameSync(file, retained);
  return retained;
};

export const replaceGeneration = (replacement: string, file: string): void => {
  const before = fileIdentity(file);
  const retained = retainGeneration(file);
  fs.renameSync(replacement, file);
  expect(fileIdentity(retained)).toEqual(before);
  expect(fileIdentity(file)).not.toEqual(before);
};

// Keep the reader's actual original inode alive without an open handle across
// the production compactor's destination replacement (Windows-safe). Linking
// back preserves that identity; compactTerminalRunLog still performs the real
// rewrite/atomic rename. Retained paths live in each suite's cleaned workspace.
export const retainCompactionGeneration = (file: string): void => {
  const before = fileIdentity(file);
  const retained = retainGeneration(file);
  fs.linkSync(retained, file);
  expect(fileIdentity(file)).toEqual(before);
};
