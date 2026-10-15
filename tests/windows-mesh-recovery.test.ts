import type fs from "node:fs";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  encodeMeshDebrisRecoveryRequest, encodeMeshOwnerRecoveryRequest, interpretMeshRecoveryResult,
  removeWindowsMeshDebris, retireWindowsMeshOwner, windowsMeshRecoveryFence,
  type WindowsMeshRecoveryEntry,
} from "../src/storage/windows-mesh-recovery.js";
import { windowsApiPathUnits, windowsLogicalPathUnits, type MeshRetireChildResult } from "../src/storage/windows-mesh-release.js";

// Keep reason: the C/D native authority boundary and interrupted destructive result ambiguity (smarty-dev#9569).
// Pure recorded framing/outcomes ONLY. No filesystem, subprocess, monkeypatched platform or product deletion.
// These tests are NOT NTFS/Windows evidence; hosted schedules are mesh-lock-recovery-windows.test.ts.
const lock = "\\\\?\\C:\\m\\.lock";
const uuid = "11111111-2222-3333-4444-555555555555";
const record = `${uuid}\n42\n123\n`;
const stat = (directory: boolean, ino = 2n, size = directory ? 0n : BigInt(Buffer.byteLength(record))): fs.BigIntStats => ({
  dev: 0xabcden, ino, size, mtimeNs: 123_456_700n,
  isDirectory: () => directory, isFile: () => !directory, isSymbolicLink: () => false,
} as fs.BigIntStats);
const directory = stat(true), owner = stat(false, 3n);
const entry = (file: string, isDirectory: boolean, text = record, ino = 3n): WindowsMeshRecoveryEntry => ({
  path: file, stat: stat(isDirectory, ino, isDirectory ? 0n : BigInt(Buffer.byteLength(text))), ...(isDirectory ? {} : { record: text }),
});
const inventory = (): WindowsMeshRecoveryEntry[] => [
  entry(`${lock}\\r.first\\owner`, false),
  entry(`${lock}\\owner.released.${uuid}`, false, record, 4n),
  entry(`${lock}\\r.first`, true, "", 5n),
  entry(lock, true, "", 2n),
];
const decode = (message: Buffer, magic: string): string[] | undefined => {
  if (message.subarray(0, 5).toString("ascii") !== magic) return undefined;
  let at = 5;
  const fields: string[] = [];
  while (at < message.length) {
    const length = message.subarray(at, at + 8).toString("ascii");
    if (!/^[0-9a-f]{8}$/.test(length)) return undefined;
    at += 8;
    const size = Number.parseInt(length, 16);
    if (size > message.length - at) return undefined;
    fields.push(message.subarray(at, at + size).toString("utf8"));
    at += size;
  }
  return fields;
};

describe("C uses original R + provided S, existing permanently retained F ABI", () => {
  it("frames the exact supplied bigint values/full bytes, never a pathname resnapshot", () => {
    const message = encodeMeshOwnerRecoveryRequest(lock, record, directory, owner, 1234);
    const fence = `${lock}.dead.${createHash("sha256").update(`${directory.dev}:${directory.ino}:${record}`).digest("hex")}`;
    expect(windowsMeshRecoveryFence(lock, directory, record)).toBe(fence);
    expect(decode(message, "FMRC1")).toEqual([
      lock, fence, "abcde", "2", "abcde", "3", (123_456_700n / 100n + 116_444_736_000_000_000n).toString(16),
      owner.size.toString(16), record, "1234",
    ]);
    expect(decode(encodeMeshOwnerRecoveryRequest(lock, record, stat(true, 9_007_199_254_740_993n), stat(false, 0xffff_ffff_ffff_ffffn)), "FMRC1")?.slice(2, 6))
      .toEqual(["abcde", "20000000000001", "abcde", "ffffffffffffffff"]);
    expect(decode(message, "FMRQ1")).toBeUndefined();
    expect(decode(message.subarray(0, message.length - 1), "FMRC1")).toBeUndefined();
    expect(decode(Buffer.concat([message, Buffer.from("x")]), "FMRC1")).toBeUndefined();
    // Retained F hashes use decimal ids (not hex) and EVERY byte, including record terminators.
    expect(fence).not.toBe(`${lock}.dead.${createHash("sha256").update(`${directory.dev.toString(16)}:${directory.ino.toString(16)}:${record}`).digest("hex")}`);
    expect(windowsMeshRecoveryFence(lock, directory, `${record} `)).not.toBe(fence);
  });

  it("rejects invalid provided S/fullR/deadline without weakening it to a new snapshot", () => {
    expect(() => encodeMeshOwnerRecoveryRequest(lock, `${record} `, directory, owner)).toThrow(/provided S/);
    expect(() => encodeMeshOwnerRecoveryRequest(lock, record, owner, owner)).toThrow(/provided S/);
    expect(() => encodeMeshOwnerRecoveryRequest(lock, record, directory, stat(false, 1n << 64n))).toThrow(/id/);
    for (const ms of [0, 30_001, 1.2]) expect(() => encodeMeshOwnerRecoveryRequest(lock, record, directory, owner, ms)).toThrow(/deadline/);
    for (const text of ["", "a\0b", "a".repeat(4097), "\ud800"]) expect(() => encodeMeshOwnerRecoveryRequest(lock, text, directory, owner)).toThrow();
    for (const file of ["C:\\relative\\..\\m\\.lock", "C:.lock", "\\\\.\\C:\\m\\.lock", `${lock}\\..\\.lock`, `${lock}\0`, `${lock} `, `${lock}:ads`]) {
      expect(() => encodeMeshOwnerRecoveryRequest(file, record, directory, owner)).toThrow();
    }
    expect(() => encodeMeshOwnerRecoveryRequest(`${lock}.dead.${"a".repeat(64)}`, record, directory, owner)).toThrow(/canonical/);
  });
});

describe("D ALL-before-ANY independently judged inventory framing", () => {
  it("frames ALL paths/ids/times/sizes/records plus top L, with an owner-first order preserved", () => {
    const items = inventory();
    const fields = decode(encodeMeshDebrisRecoveryRequest(lock, items), "FMRD1")!;
    expect(fields.slice(0, 3)).toEqual([lock, "4000", "4"]);
    expect(fields).toHaveLength(3 + 7 * items.length);
    for (let i = 0; i < items.length; i++) {
      const e = items[i]!;
      expect(fields.slice(3 + 7 * i, 10 + 7 * i)).toEqual([
        e.path, e.stat.isDirectory() ? "d" : "f", "abcde", e.stat.ino.toString(16),
        (e.stat.mtimeNs / 100n + 116_444_736_000_000_000n).toString(16), e.stat.size.toString(16), e.record ?? "",
      ]);
    }
    expect(decode(encodeMeshDebrisRecoveryRequest(lock, [entry(lock, true)]), "FMRD1")).toHaveLength(10);
  });

  it("requires a record for EVERY file, never a vanished neighbor's proof or filename UUID", () => {
    const different = `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n99\n456\n`;
    const items = [entry(`${lock}\\owner.released.${uuid}`, false, different), entry(lock, true)];
    expect(decode(encodeMeshDebrisRecoveryRequest(lock, items), "FMRD1")?.[9]).toBe(different); // filename is shape only
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [{ path: items[0]!.path, stat: items[0]!.stat }, items[1]!])).toThrow(/full original record/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [{ ...items[0]!, record: `${record}extra` }, items[1]!])).toThrow(/match/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\raw`, false), entry(lock, true)])).toThrow(/grammar/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\owner`, false), entry(lock, true)])).toThrow(/grammar/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\r.first`, false), entry(lock, true)])).toThrow(/grammar/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\owner.released.short`, false), entry(lock, true)])).toThrow(/grammar/);
  });

  it("requires exact top L, no F/siblings, aliases, reparse entries, implicit parents or parent-first order", () => {
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [])).toThrow(/1..64/);
    const items = inventory();
    expect(() => encodeMeshDebrisRecoveryRequest(lock, items.slice(0, -1))).toThrow(/final directory/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [items[2]!, items[0]!, items[1]!, items[3]!])).toThrow(/follow/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [items[0]!, items[3]!])).toThrow(/follow/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [items[1]!, { ...items[1]!, path: items[1]!.path.toUpperCase() }, items[3]!])).toThrow(/aliased/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [{ ...items[1]!, stat: { ...items[1]!.stat, isSymbolicLink: () => true } as fs.BigIntStats }, items[3]!])).toThrow(/real directories/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`\\\\?\\C:\\m\\r.other\\owner`, false), items[3]!])).toThrow(/outside/);
    const fence = `${lock}.dead.${"a".repeat(64)}`;
    expect(() => encodeMeshDebrisRecoveryRequest(fence, [entry(fence, true)])).toThrow(/never retained F/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\r.bad.`, true), items[3]!])).toThrow(/canonical|component/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\r.good\\..\\owner`, false), items[3]!])).toThrow(/canonical/);
  });

  it("caps depth, entries, full owner bytes, aggregate message and every logical path", () => {
    const deep = ["r.1", "r.2", "r.3", "r.4", "r.5"];
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\${deep.join("\\")}`, true), entry(lock, true)])).toThrow(/depth/);
    const tooMany = Array.from({ length: 64 }, (_, i) => entry(`${lock}\\r.${i}`, true));
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [...tooMany, entry(lock, true)])).toThrow(/1..64/);
    const oversize = Array.from({ length: 20 }, (_, i) => entry(`${lock}\\owner.released.${String(i).padStart(8, "0")}-2222-3333-4444-555555555555`, false, "a".repeat(4096)));
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [...oversize, entry(lock, true)])).toThrow(/64 KiB/);
    expect(() => encodeMeshDebrisRecoveryRequest(lock, [entry(`${lock}\\owner.released.${uuid}`, false, "a".repeat(4097)), entry(lock, true)])).toThrow(/4096/);
    const longTop = `\\\\?\\C:\\${"d".repeat(147)}\\.lock`;
    const prefix = `${longTop}\\r.`;
    const at239 = `${prefix}${"a".repeat(239 - windowsLogicalPathUnits(prefix))}`;
    expect(windowsLogicalPathUnits(at239)).toBe(239);
    expect(() => encodeMeshDebrisRecoveryRequest(longTop, [entry(at239, true), entry(longTop, true)])).not.toThrow();
    expect(() => encodeMeshDebrisRecoveryRequest(longTop, [entry(`${at239}a`, true), entry(longTop, true)])).toThrow(/240/);
  });
});

describe("C/D closed terminal tuples and accurate unknown", () => {
  const joined = (stdout: string, status: number | null, mode: "owner" | "debris" = "debris", extra: Partial<MeshRetireChildResult> = {}) =>
    interpretMeshRecoveryResult({ stdout, status, pid: 17, signal: null, ...extra }, mode);
  it("C accepts retained completion, not a normal release cleanup or D completion", () => {
    expect(joined("FMREC1 recovered commit retained\n", 0, "owner")).toEqual({ outcome: "recovered" });
    expect(joined("FMR1 refused owner unexpected-entries\n", 3, "owner")).toEqual({ outcome: "refused", reason: "owner: unexpected-entries" });
    expect(joined("FMR1 not-owner guard directory-replaced\n", 5, "owner").outcome).toBe("refused");
    for (const text of ["FMR1 released cleanup clean\n", "FMREC1 recovered cleanup clean\n", "FMR1 refused owner made-up\n", "FMR1 refused cleanup unexpected-entries\n"]) {
      expect(joined(text, text.startsWith("FMR1 refused") ? 3 : 0, "owner").outcome).toBe("recovery-outcome-unknown");
    }
  });
  it("D preserves the exact native terminal inventory and refuses every wrong status/phase/mode", () => {
    const native = [
      [0, "recovered", "cleanup", ["clean"]],
      [3, "refused", "parse", ["watchdog-unavailable", "stdin-unreadable", "message-too-large", "bad-magic", "bad-field-length", "truncated-field", "bad-fields", "bad-path", "mesh-path-too-long", "bad-kind", "bad-id", "bad-record", "bad-placement"]],
      [3, "refused", "guard", ["ancestor-not-held"]],
      [3, "refused", "verify", ["entry-unverified", "entry-location", "directory-modified", "record-unverified", "unexpected-entries", "deadline"]],
      [2, "partial", "delete", ["deadline", "entry-delete"]],
    ] as const;
    for (const [status, verdict, phase, details] of native) for (const detail of details) {
      const line = `FMREC1 ${verdict} ${phase} ${detail}\n`;
      expect(joined(line, status).outcome, line).toBe(verdict === "partial" ? "recovery-outcome-unknown" : verdict);
      if (verdict === "partial") expect(joined(line, status)).toMatchObject({ reason: expect.stringContaining("selected entries may already be gone") });
      for (const wrongStatus of [0, 2, 3, 5, 124].filter(code => code !== status)) expect(joined(line, wrongStatus).outcome).toBe("recovery-outcome-unknown");
      expect(joined(line, status, "owner").outcome).toBe("recovery-outcome-unknown");
      for (const wrongPhase of ["parse", "guard", "verify", "cleanup", "delete"].filter(value => value !== phase))
        expect(joined(`FMREC1 ${verdict} ${wrongPhase} ${detail}\n`, status).outcome).toBe("recovery-outcome-unknown");
    }
  });
  it("does not infer a pre-mutation phase from kills, timeouts, truncation, mixed output or exit alone", () => {
    for (const text of ["", "FMREC1 recovered cleanup clean", "FMREC1 recovered cleanup clean\r\n", "FMREC1 recovered cleanup clean\n\n", "FMREC1 recovered cleanup clean\nextra", "FMREC1 refused verify made-up\n", "FMR1 not-owner owner absent\n"]) {
      for (const status of [0, 2, 3, 124, null]) expect(joined(text, status).outcome).toBe("recovery-outcome-unknown");
    }
    const timeout = Object.assign(new Error("timeout"), { code: "ETIMEDOUT" });
    for (const mode of ["owner", "debris"] as const) {
      expect(joined("", null, mode, { error: timeout, pid: 0 }).outcome).toBe("recovery-outcome-unknown");
      expect(joined("FMREC1 recovered cleanup clean\n", 0, mode, { error: timeout }).outcome).toBe("recovery-outcome-unknown");
      expect(joined("", null, mode, { signal: "SIGTERM" }).outcome).toBe("recovery-outcome-unknown");
      const missing = Object.assign(new Error("missing"), { code: "ENOENT" });
      expect(joined("", null, mode, { error: missing, pid: 0 }).outcome).toBe("helper-unavailable/not-started");
      expect(joined("", null, mode, { error: missing }).outcome).toBe("recovery-outcome-unknown");
    }
  });
});

describe("C/D T9 two distinct path metrics", () => {
  for (const root of [`C:\\${"界".repeat(145)}😀`, `\\\\s\\h\\${"u".repeat(144)}`]) {
    it(`keeps a 150-unit ${root.startsWith("C:") ? "drive" : "UNC"} F/owner at 239 logical units, API 243/245`, () => {
      expect(root.length).toBe(150);
      const source = `${root}\\custody.lock`;
      const fields = decode(encodeMeshOwnerRecoveryRequest(source, record, directory, owner), "FMRC1")!;
      expect(windowsLogicalPathUnits(`${fields[1]}\\owner`)).toBe(239);
      expect(windowsApiPathUnits(`${fields[1]}\\owner`)).toBe(root.startsWith("C:") ? 243 : 245);
      expect(() => encodeMeshOwnerRecoveryRequest(source.replace(root, `${root}x`), record, directory, owner)).toThrow(/240/);
    });
  }
});

describe.skipIf(process.platform === "win32")("off Windows recovery is inert", () => {
  it("refuses without using any supplied paths, snapshots or capability", () => {
    const cap = { absolutePath: "absent", digest: "0".repeat(64), meshAbi: 1 as const };
    expect(retireWindowsMeshOwner("absent", record, directory, owner, cap).outcome).toBe("helper-unavailable/not-started");
    expect(removeWindowsMeshDebris("absent", [], cap).outcome).toBe("helper-unavailable/not-started");
  });
});
