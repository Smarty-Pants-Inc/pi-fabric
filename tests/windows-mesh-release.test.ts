import { describe, expect, it } from "vitest";
import {
  encodeMeshRetireRequest, interpretMeshRetireResult, meshRetirementName, prepareWindowsMeshCapability, releaseWindowsMeshLock,
  windowsApiPathUnits, windowsLogicalPathUnits, type MeshRetireRequest,
} from "../src/storage/windows-mesh-release.js";

// smarty-dev#9569 Commit A: the platform-independent part of the Windows mesh release primitive. Keep reason: the
// framed request and the strict terminal-outcome interpretation are the native trust boundary's TypeScript half; an
// unknown child outcome misread as a pre-commit refusal (or a malformed record accepted) is a data-safety bug that
// no normal user flow reaches cheaply. Pure functions only: no filesystem, no child process. The real NTFS/native
// schedules (T2, T3, T5, T9 on disk) are in tests/mesh-lock-release-windows.test.ts, hosted Windows only.

const request = (overrides: Partial<MeshRetireRequest> = {}): MeshRetireRequest => ({
  lockPath: "\\\\?\\C:\\m\\.lock", retiredPath: `\\\\?\\C:\\m\\r.${"a".repeat(64)}`,
  dirVolume: 0xabcn, dirIndex: 0xffff_ffff_ffff_ffffn, ownerVolume: 0xabcn, ownerIndex: 1n, ownerFileTime: 0x1d9n, ownerSize: 3n,
  record: Buffer.from("t\n1"), deadlineMs: 4000, ...overrides,
});

/** A reference reader of the framing rules the native parser enforces (exact field count, no trailing bytes). */
const decode = (message: Buffer): string[] | undefined => {
  if (message.subarray(0, 5).toString("ascii") !== "FMRQ1") return undefined;
  const fields: string[] = [];
  let at = 5;
  for (let i = 0; i < 10; i++) {
    const length = message.subarray(at, at + 8).toString("ascii");
    if (!/^[0-9a-f]{8}$/.test(length)) return undefined;
    at += 8;
    const size = Number.parseInt(length, 16);
    if (size > message.length - at) return undefined;
    fields.push(message.subarray(at, at + size).toString("utf8"));
    at += size;
  }
  return at === message.length ? fields : undefined;
};

describe("Windows mesh retire request framing (smarty-dev#9569)", () => {
  it("encodes exactly ten length-prefixed fields with exact hex ids and the original record bytes", () => {
    const message = encodeMeshRetireRequest(request());
    expect(decode(message)).toEqual(["\\\\?\\C:\\m\\.lock", `\\\\?\\C:\\m\\r.${"a".repeat(64)}`, "abc", "ffffffffffffffff", "abc", "1", "1d9", "3", "t\n1", "4000"]);
    // A trailing byte or a cut field is not a valid frame.
    expect(decode(Buffer.concat([message, Buffer.from("x")]))).toBeUndefined();
    expect(decode(message.subarray(0, message.length - 1))).toBeUndefined();
  });

  it("refuses out-of-range ids, deadlines and oversized requests before any child", () => {
    expect(() => encodeMeshRetireRequest(request({ dirIndex: 1n << 64n }))).toThrow(RangeError);
    expect(() => encodeMeshRetireRequest(request({ ownerVolume: -1n }))).toThrow(RangeError);
    for (const deadlineMs of [0, 30_001, 1.5]) expect(() => encodeMeshRetireRequest(request({ deadlineMs }))).toThrow(RangeError);
    expect(() => encodeMeshRetireRequest(request({ lockPath: "x".repeat(70_000) }))).toThrow(RangeError);
  });

  it("enforces native record bounds, exact size and NUL refusal without truncating full UUID records", () => {
    for (const record of [Buffer.alloc(0), Buffer.alloc(4097, 65), Buffer.from("a\0b")]) {
      expect(() => encodeMeshRetireRequest(request({ record, ownerSize: BigInt(record.length) }))).toThrow(RangeError);
    }
    expect(() => encodeMeshRetireRequest(request({ ownerSize: 2n }))).toThrow(/match owner size/);
    for (const text of ["11111111-2222-3333-4444-555555555555\n42\n1\n", "a".repeat(4096)]) {
      const record = Buffer.from(text);
      expect(decode(encodeMeshRetireRequest(request({ record, ownerSize: BigInt(record.length) })))?.[8]).toBe(text);
    }
  });

  it("rejects invalid UTF-8 paths, NUL, placement and caps before spawning", () => {
    for (const lockPath of ["C:\\m\\.lock", "\\\\?\\C:\\m\\bad\0path", "\\\\?\\C:\\m\\\ud800", `\\\\?\\C:\\${"界".repeat(11_000)}\\.lock`]) {
      expect(() => encodeMeshRetireRequest(request({ lockPath }))).toThrow(RangeError);
    }
    for (const retiredPath of [`\\\\?\\C:\\other\\r.${"a".repeat(64)}`, "\\\\?\\C:\\m\\r.short", `\\\\?\\C:\\m\\r.${"A".repeat(64)}`]) {
      expect(() => encodeMeshRetireRequest(request({ retiredPath }))).toThrow(/sibling/);
    }
  });

  it("names the retirement destination by lock kind, exact directory id and the FULL record (no truncation)", () => {
    const record = Buffer.from("token\n42\n1\n");
    const name = meshRetirementName(".lock", 1n, 2n, record);
    expect(name).toMatch(/^r\.[0-9a-f]{64}$/);
    expect(meshRetirementName(".lock", 1n, 2n, Buffer.from("token\n42\n1\n"))).toBe(name);
    expect(meshRetirementName("custody.lock", 1n, 2n, record)).not.toBe(name);
    expect(meshRetirementName(".lock", 1n, 3n, record)).not.toBe(name);
    expect(meshRetirementName(".lock", 1n, 2n, Buffer.from("token\n42\n1\n "))).not.toBe(name);
  });
});

describe("Windows mesh retire terminal outcomes (smarty-dev#9569)", () => {
  const retired = "C:\\m\\r.x";
  const joined = (stdout: string, status: number | null, extra: Partial<Parameters<typeof interpretMeshRetireResult>[0]> = {}) =>
    interpretMeshRetireResult({ status, signal: null, stdout: Buffer.from(stdout), pid: 7, ...extra }, retired);

  it("accepts only a single terminal line paired with its exit status", () => {
    expect(joined("FMR1 released cleanup clean\n", 0)).toEqual({ outcome: "released", cleanup: "clean", retiredPath: retired });
    expect(joined("FMR1 released cleanup pending-extra-entries\n", 0)).toMatchObject({ outcome: "released", cleanup: "pending", retiredPath: retired });
    expect(joined("FMR1 not-owner owner record-differs\n", 5)).toEqual({ outcome: "not-owner" });
    expect(joined("FMR1 not-owner guard absent\n", 5)).toEqual({ outcome: "not-owner" });
    for (const [phase, detail] of [["parse", "stdin-unreadable"], ["guard", "directory-not-held"],
      ["owner", "record-unreadable"], ["commit", "rename-denied"]]) {
      expect(joined(`FMR1 refused ${phase} ${detail}\n`, 3)).toEqual({ outcome: "refused-before-retirement", reason: `${phase}: ${detail}` });
    }
    expect(joined("FMR1 refused owner unexpected-entries\n", 3)).toEqual({ outcome: "refused-before-retirement", reason: "owner: unexpected-entries" });
  });

  it("never turns a missing, malformed or unpaired record into a pre-commit refusal", () => {
    for (const [stdout, status] of [
      ["", 0], ["", 2], ["", 3], ["FMR1 released cleanup clean", 0], ["FMR1 released cleanup clean\r\n", 0],
      ["FMR1 released cleanup clean\nextra\n", 0], ["FMR1 released cleanup clean\n\n", 0],
      ["FMR1 released cleanup clean\n", 3], ["FMR1 refused owner x\n", 0],
      ["FMR1 not-owner owner absent\n", 0], ["FMR1 released commit clean\n", 0], ["FMR1 released cleanup bogus\n", 0],
      ["ok 1 removed\n", 0], ["FMR1 refused owner x\n", 124],
      ["FMR1 not-owner parse absent\n", 5], ["FMR1 not-owner commit absent\n", 5],
      ["FMR1 not-owner cleanup absent\n", 5], ["FMR1 refused cleanup owner-delete\n", 3],
    ] as const) expect(joined(stdout, status).outcome, `${JSON.stringify(stdout)} exit ${status}`).toBe("release-outcome-unknown");
    expect(joined("FMR1 released cleanup clean\n", null, { signal: "SIGTERM" }).outcome).toBe("release-outcome-unknown");
    const timeout = Object.assign(new Error("spawnSync ETIMEDOUT"), { code: "ETIMEDOUT" });
    expect(joined("FMR1 refused owner x\n", 3, { error: timeout }).outcome).toBe("release-outcome-unknown");
  });

  it("CR-A-01: preserves every exact native meshOut/refusedAt tuple, with no phase or exit substitutions", () => {
    // Independent inventory of meshRetire's literal emissions, including both arms of conditional details.
    const native = [
      [0, "released", "cleanup", ["clean", "pending-location", "pending-entries-unreadable", "pending-extra-entries", "pending-deadline",
        "pending-owner-unverified", "pending-record-unreadable", "pending-owner-delete", "pending-directory-not-empty"]],
      [5, "not-owner", "guard", ["absent", "directory-replaced"]],
      [5, "not-owner", "owner", ["absent", "record-differs"]],
      [3, "refused", "parse", ["watchdog-unavailable", "stdin-unreadable", "message-too-large", "bad-magic", "bad-field-length",
        "truncated-field", "trailing-bytes", "bad-path", "bad-id", "bad-record", "bad-deadline", "bad-placement", "mesh-path-too-long"]],
      [3, "refused", "guard", ["ancestor-not-held", "directory-not-held", "directory-unverified", "directory-location"]],
      [3, "refused", "owner", ["path-too-long", "owner-not-held", "owner-modified", "owner-unverified", "record-unreadable",
        "owner-outside-directory", "entries-unreadable", "unexpected-entries"]],
      [3, "refused", "commit", ["deadline", "no-memory", "destination-exists", "rename-denied", "rename-failed"]],
    ] as const;
    for (const [status, verdict, phase, details] of native) for (const detail of details) {
      const line = `FMR1 ${verdict} ${phase} ${detail}\n`;
      const result = joined(line, status);
      expect(result.outcome, line).toBe(verdict === "refused" ? "refused-before-retirement" : verdict);
      if (result.outcome === "released") expect(result.cleanup).toBe(detail === "clean" ? "clean" : "pending");
      if (result.outcome === "refused-before-retirement") expect(result.reason).toBe(`${phase}: ${detail}`);
      for (const wrongStatus of [0, 3, 5, 124].filter(code => code !== status)) {
        expect(joined(line, wrongStatus).outcome, `${line} exit ${wrongStatus}`).toBe("release-outcome-unknown");
      }
      // Even a known detail is unknown under a different phase (not-owner/absent has two valid phases).
      for (const wrongPhase of ["parse", "guard", "owner", "commit", "cleanup"]) {
        if (native.some(([code, kind, stage, allowed]) => code === status && kind === verdict && stage === wrongPhase &&
          (allowed as readonly string[]).includes(detail))) continue;
        expect(joined(`FMR1 ${verdict} ${wrongPhase} ${detail}\n`, status).outcome).toBe("release-outcome-unknown");
      }
    }
  });

  it.each([
    ["released", "cleanup", "pending-unrecognized", 0],
    ["not-owner", "guard", "unrecognized", 5],
    ["not-owner", "owner", "unrecognized", 5],
    ["refused", "parse", "unreadable", 3],
    ["refused", "guard", "unreadable", 3],
    ["refused", "owner", "unreadable", 3],
    ["refused", "commit", "unreadable", 3],
  ] as const)("CR-A-01: rejects an unknown native tuple: %s %s %s exit %s", (verdict, phase, detail, status) => {
    expect(joined(`FMR1 ${verdict} ${phase} ${detail}\n`, status).outcome).toBe("release-outcome-unknown");
  });

  it("reports a child that never started as not-started (known pre-commit)", () => {
    const missing = Object.assign(new Error("spawnSync ENOENT"), { code: "ENOENT" });
    expect(interpretMeshRetireResult({ status: null, signal: null, stdout: null, error: missing, pid: 0 }, retired))
      .toEqual({ outcome: "helper-unavailable/not-started", reason: "the helper did not start (ENOENT)" });
  });
});

describe("Windows mesh path budget, E3/T9 arithmetic (smarty-dev#9569)", () => {
  // Logical: the absolute on-disk path in UTF-16 units, transport syntax (\\?\ or \\?\UNC\) excluded. API: the string
  // handed to Win32. A 150-unit root has no trailing separator.
  const driveRoot = `C:\\${"d".repeat(147)}`;
  const uncRoot = `\\\\s\\h\\${"u".repeat(144)}`;
  const retiredOwner = (root: string) => `${root}\\r.${"0".repeat(64)}\\owner`;
  const namespaced = (file: string) => file.startsWith("\\\\") ? `\\\\?\\UNC\\${file.slice(2)}` : `\\\\?\\${file}`;

  it("keeps the retired owner at 223 logical units under a 150-unit root, with the API string measured separately", () => {
    expect(driveRoot.length).toBe(150);
    expect(uncRoot.length).toBe(150);
    expect(windowsLogicalPathUnits(namespaced(retiredOwner(driveRoot)))).toBe(223);
    expect(windowsApiPathUnits(namespaced(retiredOwner(driveRoot)))).toBe(227);
    expect(windowsLogicalPathUnits(namespaced(retiredOwner(uncRoot)))).toBe(223);
    expect(windowsApiPathUnits(namespaced(retiredOwner(uncRoot)))).toBe(229);
    expect(windowsLogicalPathUnits(retiredOwner(driveRoot))).toBe(223);
    // The destination name carries the whole 64-hex digest: no units are regained by truncation.
    expect(meshRetirementName(".lock", 1n, 1n, Buffer.from("r")).length).toBe(66);
  });

  it("preflights Unicode 150-unit roots and allows a >240 API string when its logical owner is 239", () => {
    const root = `C:\\${"界".repeat(145)}\u{1F600}`;
    expect(root.length).toBe(150);
    const retiredPath = namespaced(`${root}\\r.${"a".repeat(64)}`);
    const lockPath = namespaced(`${root}\\custody.lock.dead.${"b".repeat(64)}`);
    expect(windowsApiPathUnits(`${lockPath}\\owner`)).toBe(243);
    expect(windowsLogicalPathUnits(`${lockPath}\\owner`)).toBe(239);
    expect(() => encodeMeshRetireRequest(request({ lockPath, retiredPath }))).not.toThrow();
    // One more logical unit crosses the strict <240 boundary, even though namespace accounting is unchanged.
    expect(() => encodeMeshRetireRequest(request({ lockPath: `${lockPath}x`, retiredPath }))).toThrow(/below 240/);
    const tooLongRoot = `${root}${"界".repeat(17)}`; // retired owner 223 + 17 = 240
    expect(() => encodeMeshRetireRequest(request({ lockPath: namespaced(`${tooLongRoot}\\.lock`),
      retiredPath: namespaced(`${tooLongRoot}\\r.${"a".repeat(64)}`) }))).toThrow(/below 240/);
    const unc = namespaced(`${uncRoot}\\custody.lock.dead.${"b".repeat(64)}`);
    expect(windowsApiPathUnits(`${unc}\\owner`)).toBe(245);
    expect(() => encodeMeshRetireRequest(request({ lockPath: unc, retiredPath: namespaced(`${uncRoot}\\r.${"a".repeat(64)}`) }))).not.toThrow();
  });

  it("counts UTF-16 code units, not code points (a non-BMP character is two units)", () => {
    const root = `C:\\${"d".repeat(145)}\u{1F600}`;
    expect([...root].length).toBe(149);
    expect(root.length).toBe(150);
    expect(windowsLogicalPathUnits(namespaced(retiredOwner(root)))).toBe(223);
  });
});

describe.skipIf(process.platform === "win32")("off Windows the primitive is inert (smarty-dev#9569)", () => {
  it("refuses to prepare a capability and never starts a release child", () => {
    expect(() => prepareWindowsMeshCapability()).toThrow(/real Windows only/);
    expect(releaseWindowsMeshLock("/nonexistent/.lock", "t\n", { absolutePath: "/nonexistent/helper", digest: "0".repeat(64), meshAbi: 1 }))
      .toEqual({ outcome: "helper-unavailable/not-started", reason: "the Windows mesh release runs on real Windows only" });
  });
});
