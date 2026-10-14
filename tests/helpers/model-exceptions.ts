import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { vi } from "vitest";
import type { ModelExceptionEntry } from "../../src/agents/model-policy-log.js";

const realFstatSync = fs.fstatSync;
const realLstatSync = fs.lstatSync;

/** Test-only root metadata for one real regular grant and its directory ancestors. */
export function rootMetadataFixture(file: string, groupWritableParent?: string) {
  const endpoint = realLstatSync(file, { throwIfNoEntry: false });
  const parents = new Set<string>();
  const addParent = (directory: string): void => {
    parents.add(directory);
    const parent = path.dirname(directory);
    if (parent !== directory) addParent(parent);
  };
  addParent(path.dirname(path.resolve(file)));
  const fstat = vi.spyOn(fs, "fstatSync").mockImplementation(((fd, options) => {
    const stat = realFstatSync(fd, options);
    if (!endpoint?.isFile() || !stat.isFile() || stat.dev !== endpoint.dev || stat.ino !== endpoint.ino) return stat;
    return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: 0, mode: Number(stat.mode) & ~0o022 });
  }) as typeof fs.fstatSync);
  const lstat = vi.spyOn(fs, "lstatSync").mockImplementation(((at, options) => {
    const stat = realLstatSync(at, options);
    const isGrant = String(at) === file && endpoint?.isFile() && stat?.isFile() &&
      stat.dev === endpoint.dev && stat.ino === endpoint.ino;
    if (!isGrant && (!stat?.isDirectory() || !parents.has(String(at)))) return stat;
    const mode = Number(stat!.mode) & ~0o022;
    return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, {
      uid: 0, mode: String(at) === groupWritableParent ? mode | 0o020 : mode,
    });
  }) as typeof fs.lstatSync);
  return { fstat, lstat };
}

/** Scope the shared metadata seam to synchronous admission; real policy/open/read stay intact. */
export function withRootExceptionMetadata<T>(file: string, admit: () => T): T {
  const { fstat, lstat } = rootMetadataFixture(file);
  try { return admit(); }
  finally { fstat.mockRestore(); lstat.mockRestore(); }
}

/** Pure v1 row creator: no filesystem writes, admission, or host configuration changes. */
export function modelExceptionGrant(
  input: Pick<ModelExceptionEntry, "model" | "lane"> & Partial<Omit<ModelExceptionEntry, "model" | "lane">>,
): ModelExceptionEntry {
  const grantedAt = input.granted_at ?? new Date().toISOString();
  const issue = input.issue ?? "Smarty-Pants-Inc/smarty-dev#11864";
  const issuePath = issue.replace("#", "/issues/");
  return {
    id: `fixture-${randomUUID()}`,
    issue,
    reason_url: `https://github.com/${issuePath}#issuecomment-118640001`,
    role: "task",
    host: os.hostname().split(".")[0]!,
    thinking: "max",
    granted_by: "fixture-host",
    granted_at: grantedAt,
    expires_at: new Date(Date.parse(grantedAt) + 60 * 60 * 1_000).toISOString(),
    status: "active",
    ...input,
  };
}

/** Provision only a caller-owned private fixture directory with finite exact task rows. */
export const writeModelExceptions = (agentDir: string, models: string[], rootCwd: string = agentDir): string => {
  const filePath = path.join(agentDir, "model-exceptions.json");
  fs.writeFileSync(filePath, JSON.stringify({
    version: 1,
    grants: models.map(model => modelExceptionGrant({ model, lane: path.basename(rootCwd) })),
  }), { mode: 0o600 });
  return filePath;
};
