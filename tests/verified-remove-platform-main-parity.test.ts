import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { platformRemovalSkips, setWindowsVerifiedRemoval, startRemovalTally, stopRemovalTally, verifiedRemove } from "../src/storage/verified-remove.js";

// Owner ruling (fabric-store): off Linux main's branch is unchanged: the caller's `fallback` removes, the tally counts
// main's bytes (a size walk only under a report), a dry run never calls it, `proceed` is not consulted, and `freed`
// (a compaction) is the byte count. A failed or throwing fallback is counted and logged. On Linux the fallback is never
// used and missing getuid still refuses. One case per platform branch. Every destructive fs call is a recorder
// installed before product code runs; the fallback itself is a recorder too. Native Windows helper tests live elsewhere.
const DAY = 24 * 60 * 60 * 1_000;
const OLD = (Date.now() - 30 * DAY) / 1000;
const roots: string[] = [];
const restores: Array<() => void> = [];
afterEach(() => {
  for (const restore of restores.splice(0).reverse()) restore();
  vi.restoreAllMocks();
  vi.useRealTimers();
  stopRemovalTally();
  setWindowsVerifiedRemoval(false);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const override = (key: "platform" | "getuid" | "geteuid", value: unknown) => {
  const descriptor = Object.getOwnPropertyDescriptor(process, key);
  Object.defineProperty(process, key, { value, configurable: true, writable: true });
  restores.push(() => descriptor ? Object.defineProperty(process, key, descriptor) : delete (process as unknown as Record<string, unknown>)[key]);
};
const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};
/** parent/{target/{a.txt, sub/inner.txt}, file.log}, aged 30 days: tree 9 bytes, file 9 bytes. */
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-vrmp-"));
  roots.push(root);
  const parent = path.join(root, "parent");
  const target = path.join(parent, "target");
  fs.mkdirSync(path.join(target, "sub"), { recursive: true });
  fs.writeFileSync(path.join(target, "a.txt"), "aaaa");
  fs.writeFileSync(path.join(target, "sub", "inner.txt"), "inner");
  const file = path.join(parent, "file.log");
  fs.writeFileSync(file, "log-bytes");
  for (const entry of [path.join(target, "a.txt"), path.join(target, "sub", "inner.txt"), path.join(target, "sub"), target, file]) fs.utimesSync(entry, OLD, OLD);
  return { parent, target, file, before: shape(parent) };
};
/** Records (never executes) unlink/rmdir/rm, and the coalesced skip warning (its 1 s timer faked). */
const record = () => {
  const calls: string[] = [];
  const capture = (op: string) => ((file: fs.PathLike) => { calls.push(`${op}:${path.basename(String(file))}`); }) as never;
  vi.spyOn(fs, "unlinkSync").mockImplementation(capture("unlink"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(capture("rmdir"));
  vi.spyOn(fs, "rmSync").mockImplementation(capture("rm"));
  vi.useFakeTimers({ toFake: ["setTimeout"] });
  const warned: string[] = [];
  vi.spyOn(console, "warn").mockImplementation((message: unknown) => { warned.push(String(message)); });
  return { calls, warnings: () => { vi.runOnlyPendingTimers(); return warned.join("\n"); } };
};
const FAILED = "removal failed (main's path on this platform)";

/** Main's ordinary off-Linux dispatch, identical with getuid present or absent; the opt-in native helper stays off. */
const mainBranch = (platform: "win32" | "darwin", uid: "present" | "absent") => {
  const { parent, target, file, before } = fixture();
  const { calls, warnings } = record();
  const judgedTree = fs.lstatSync(target), judgedFile = fs.lstatSync(file);
  if (uid === "absent") { override("getuid", undefined); override("geteuid", undefined); }
  else { if (typeof process.getuid !== "function") override("getuid", () => 0); if (typeof process.geteuid !== "function") override("geteuid", () => 0); }
  override("platform", platform);
  setWindowsVerifiedRemoval(false); // The ordinary caller branch on native Windows too; native-helper tests stay separate.
  const fallbackRuns: string[] = [];
  let proceedRuns = 0;
  const proceed = () => { proceedRuns++; return false; };
  const fallback = (name: string, result: boolean | "throw") => () => {
    fallbackRuns.push(name);
    if (result === "throw") throw new Error("main's best effort");
    return result;
  };

  // No tally (in-process sweep): main's filesystem cost only, no size walk; a failed fallback is logged.
  const lstat = vi.spyOn(fs, "lstatSync"), readdir = vi.spyOn(fs, "readdirSync");
  expect(verifiedRemove(target, judgedTree, { deleter: "parity", fallback: fallback("apply", true), proceed })).toEqual({ removed: true, bytes: 0 });
  expect(lstat).not.toHaveBeenCalled();
  expect(readdir).not.toHaveBeenCalled();
  const key = `parity-log: ${FAILED}`;
  const was = platformRemovalSkips()[key] ?? 0;
  expect(verifiedRemove(file, judgedFile, { deleter: "parity-log", fallback: fallback("false", false) })).toEqual({ removed: false, reason: FAILED });
  expect(platformRemovalSkips()[key]).toBe(was + 1);
  expect(warnings()).toContain(key);
  lstat.mockRestore(); readdir.mockRestore();

  // Under a tally (the operator's report): main's byte walk, dry run never calls the fallback, compaction `freed`.
  const tally = startRemovalTally();
  expect(verifiedRemove(target, judgedTree, { deleter: "parity", dryRun: true, fallback: fallback("dry", true), proceed })).toEqual({ removed: true, bytes: 9 });
  expect(verifiedRemove(target, judgedTree, { deleter: "parity", fallback: fallback("tree", true), proceed })).toEqual({ removed: true, bytes: 9 });
  // A compaction: the fallback publishes the new tail (delegated unchanged); `freed` is the count.
  expect(verifiedRemove(file, judgedFile, { deleter: "parity", freed: 4, fallback: fallback("compact", true), proceed })).toEqual({ removed: true, bytes: 4 });
  expect(verifiedRemove(file, judgedFile, { deleter: "parity", fallback: fallback("throw", "throw") })).toEqual({ removed: false, reason: FAILED });
  expect(verifiedRemove(file, undefined, { deleter: "parity", fallback: fallback("unjudged", true) })).toEqual({ removed: true, bytes: 9 });
  expect(tally.parity).toMatchObject({ removed: 4, bytes: 31, refused: { [FAILED]: 1 } });

  expect(fallbackRuns).toEqual(["apply", "false", "tree", "compact", "throw", "unjudged"]);
  expect(proceedRuns).toBe(0);
  expect(calls).toEqual([]);
  expect(shape(parent)).toEqual(before);
};

describe("off Linux, main's dispatch is unchanged; on Linux the fallback is never used", () => {
  it("win32 (forced; getuid absent, as on Windows): main's fallback branch, tally, dry run, compaction", () => mainBranch("win32", "absent"));

  it("darwin (forced; getuid present): main's fallback branch, tally, dry run, compaction", () => mainBranch("darwin", "present"));

  it.skipIf(process.platform !== "linux")("linux: never the fallback; missing getuid refuses, present runs the verified path", () => {
    const { parent, file, before } = fixture();
    const { calls } = record();
    const judged = fs.lstatSync(file);
    let fallbackRuns = 0;
    const fallback = () => { fallbackRuns++; return true; };
    const tally = startRemovalTally();
    override("geteuid", undefined);
    expect(verifiedRemove(file, judged, { deleter: "parity", fallback })).toEqual({ removed: false, reason: expect.stringContaining("no getuid") });
    expect(verifiedRemove(file, judged, { deleter: "parity", fallback, dryRun: true })).toMatchObject({ removed: false });
    expect(calls).toEqual([]);
    for (const restore of restores.splice(0).reverse()) restore();
    // getuid present: the verified dirfd path; its unlink/rmdir are recorded, never executed (the file stays moved).
    expect(verifiedRemove(file, judged, { deleter: "parity", fallback, dryRun: true })).toEqual({ removed: true, bytes: 9 });
    expect(verifiedRemove(file, judged, { deleter: "parity", fallback, proceed: () => true })).toEqual({ removed: true, bytes: 9 });
    expect(calls.map(call => call.replace(/\.[0-9a-f-]{36}$/, ""))).toEqual(["unlink:file.log", `rmdir:.retention-graveyard-${process.pid}`]);
    expect(fallbackRuns).toBe(0);
    expect(tally.parity).toMatchObject({ removed: 2, bytes: 18 });
    expect(fs.existsSync(file)).toBe(false);
    expect(before).toMatchObject({ "file.log": "log-bytes" });
  });
});
