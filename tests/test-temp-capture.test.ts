import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { beginDriverCapture, captureAllocation, captureAfterShell, captureBoundary, captureCleanup, captureEnabled, captureRecord } from "../scripts/test-temp-capture.mjs";

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});
const windowsCapture = () => {
  Object.defineProperty(process, "platform", { value: "win32" });
  vi.stubEnv("PI_TEST_TEMP_CAPTURE", "1");
  const original = fs.writeSync;
  const calls: [number, string][] = [];
  vi.spyOn(fs, "writeSync").mockImplementation((fd, ...args) => {
    if (fd === process.stdout.fd) {
      calls.push([fd, String(args[0])]);
      return Buffer.byteLength(String(args[0]));
    }
    return Reflect.apply(original, fs, [fd, ...args]);
  });
  return {mock:{calls}};
};

describe("bounded temp capture", () => {
  it("requires both Windows and explicit opt-in", () => {
    vi.stubEnv("PI_TEST_TEMP_CAPTURE", "1");
    Object.defineProperty(process, "platform", {value:"linux"});
    expect(captureEnabled()).toBe(false);
    const output = vi.spyOn(fs, "writeSync").mockReturnValue(1);
    captureAllocation("not-a-real-root");
    expect(output).not.toHaveBeenCalled();
    Object.defineProperty(process, "platform", {value:"win32"});
    vi.stubEnv("PI_TEST_TEMP_CAPTURE", "0");
    expect(captureEnabled()).toBe(false);
  });

  it("observes cleanup before the next child and restores only its owned ledger", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "capture-unit-"));
    const output = windowsCapture();
    vi.stubEnv("PI_TEST_TEMP_LEDGER", undefined);
    vi.stubEnv("UNRELATED_SECRET", "must-not-be-logged");
    const end = beginDriverCapture();
    const ledger = process.env.PI_TEST_TEMP_LEDGER!;
    try {
      captureAllocation(root);
      captureCleanup("cleanup-before", root);
      fs.rmSync(root, {recursive:true});
      captureCleanup("cleanup-after", root);
      captureBoundary("driver-after-child");
      captureBoundary("driver-before-child");
      const records = output.mock.calls.map(call => JSON.parse(String(call[1]).replace(/^FABRIC_TEMP_CAPTURE /, "")));
      expect(records.find(row => row.phase === "cleanup-before").state.exists).toBe(true);
      expect(records.find(row => row.phase === "cleanup-after").state.exists).toBe(false);
      expect(records.find(row => row.phase === "driver-before-child").roots[0].state.exists).toBe(false);
      expect(JSON.stringify(records)).not.toContain("must-not-be-logged");
    } finally { end(); fs.rmSync(root, {recursive:true,force:true}); }
    expect(fs.existsSync(ledger)).toBe(false);
    expect(process.env.PI_TEST_TEMP_LEDGER).toBeUndefined();
  });

  it.skipIf(process.platform === "win32")("parses one labelled post-shell fixture, without treating it as native evidence", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "capture-probe-unit-"));
    fs.mkdirSync(path.join(root, "bin"));
    fs.mkdirSync(path.join(root, "etc", "fstab.d"), {recursive:true});
    fs.writeFileSync(path.join(root, "etc", "fstab"), "none /tmp usertemp binary 0 0\n");
    fs.writeFileSync(path.join(root, "etc", "fstab.d", "fixture"), "none /tmp usertemp binary 0 0\n");
    const shell = path.join(root, "bin", "bash.exe");
    fs.writeFileSync(shell, `#!/bin/sh\nprintf '%s\\n' 'BASH_VERSION=fixture' 'EXE=fixture' 'TMP_NATIVE=${root}' 'RUNTIME=fixture' 'USER=fixture' 'TMP_IS_DIR_STATUS=0'\n`, {mode:0o700});
    const dll = path.join(root, "bin", "msys-2.0.dll");
    const dllFd = fs.openSync(dll, "w");
    fs.ftruncateSync(dllFd, 32 * 1024 * 1024 + 1);
    fs.closeSync(dllFd);
    const overrideFd = fs.openSync(path.join(root, "etc", "fstab.d", "fixture"), "w");
    fs.ftruncateSync(overrideFd, 64 * 1024 + 1);
    fs.closeSync(overrideFd);
    const output = windowsCapture();
    vi.stubEnv("PI_TEST_TEMP_LEDGER", undefined);
    try {
      captureAfterShell(shell, "original warning must remain");
      const count = output.mock.calls.length;
      captureAfterShell(shell, "not another probe");
      expect(output.mock.calls.length).toBe(count);
      const records = output.mock.calls.map(call => JSON.parse(String(call[1]).replace(/^FABRIC_TEMP_CAPTURE /, "")));
      expect(records[0].phase).toBe("post-shell-before-probe");
      expect(records[0].outputPrefix).toBe("original warning must remain");
      expect(records[1].status).toBe(0);
      expect(records[1].mapping.exists).toBe(true);
      expect(records[1].binaries[0].sha256).toMatch(/^[a-f0-9]{64}$/);
      expect(records[1].mounts[0].entries).toEqual(["none /tmp usertemp binary 0 0"]);
      expect(records[1].mounts[1].error.message).toContain("65536-byte bound");
      expect(records[1].binaries.find((entry: {file:string}) => entry.file === dll).error.message).toContain("33554432-byte bound");
    } finally { fs.rmSync(root, {recursive:true,force:true}); }
  });

  it("enforces UTF-8 field and framed record bounds for non-ASCII roots", () => {
    const output = windowsCapture();
    captureRecord("unicode", {roots:Array.from({length:64}, () => ({root:"界".repeat(180), state:{root:"界".repeat(180)}}))});
    const framed = String(output.mock.calls.at(-1)?.[1]);
    expect(Buffer.byteLength(framed)).toBeLessThanOrEqual(65536);
    expect(JSON.parse(framed.replace(/^FABRIC_TEMP_CAPTURE /, "")).incomplete).toBe(true);
    captureRecord("field", {root:"😀".repeat(1000)});
    const field = JSON.parse(String(output.mock.calls.at(-1)?.[1]).replace(/^FABRIC_TEMP_CAPTURE /, ""));
    expect(Buffer.byteLength(field.root)).toBeLessThanOrEqual(2048);
    expect(field.root).not.toContain("�");
    expect(field.truncatedStrings).toBe(1);
  });

  it.each(["bytes", "rows", "contention"])("refuses ledger %s overflow with explicit incomplete evidence", (capacity) => {
    const output = windowsCapture();
    vi.stubEnv("PI_TEST_TEMP_LEDGER", undefined);
    const end = beginDriverCapture();
    const ledger = process.env.PI_TEST_TEMP_LEDGER!;
    try {
      if (capacity === "bytes") {
        const empty = JSON.stringify({pid:1, selection:"", root:""}) + "\n";
        fs.writeFileSync(ledger, JSON.stringify({pid:1, selection:"", root:"x".repeat(65536 - Buffer.byteLength(empty) - 4)}) + "\n");
      } else if (capacity === "rows") {
        for (let i=0; i<64; i++) captureAllocation("r");
      } else fs.writeFileSync(ledger + ".lock", "", {flag:"wx"});
      const before = fs.readFileSync(ledger);
      captureAllocation("omitted");
      captureAllocation("also-omitted");
      expect(fs.readFileSync(ledger).equals(before)).toBe(true);
      expect(before.length).toBeLessThanOrEqual(65536);
      expect(before.toString().split("\n").filter(Boolean).length).toBeLessThanOrEqual(64);
      captureBoundary("after-overflow");
      const record = JSON.parse(String(output.mock.calls.at(-1)?.[1]).replace(/^FABRIC_TEMP_CAPTURE /, ""));
      expect(record.incomplete).toBe(true);
      expect(record.omission.omitted).toContain("at least one");
    } finally { end(); }
    for (const suffix of ["", ".lock", ".incomplete"]) expect(fs.existsSync(ledger + suffix)).toBe(false);
  });

  it.each(["overflow", "changed"])("refuses %s after fstat with at most limit+1 bytes read", (mode) => {
    const output = windowsCapture();
    vi.stubEnv("PI_TEST_TEMP_LEDGER", undefined);
    const end = beginDriverCapture();
    const ledger = process.env.PI_TEST_TEMP_LEDGER!;
    const originalStat = fs.fstatSync.bind(fs);
    let grew = false;
    vi.spyOn(fs, "fstatSync").mockImplementation((fd, options) => {
      const result = originalStat(fd, options);
      if (!grew) { grew = true; fs.appendFileSync(ledger, "x".repeat(mode === "overflow" ? 65538 : 1)); }
      return result;
    });
    const reads = vi.spyOn(fs, "readSync");
    try {
      captureBoundary("growth");
      const record = JSON.parse(String(output.mock.calls.at(-1)?.[1]).replace(/^FABRIC_TEMP_CAPTURE /, ""));
      expect(record.incomplete).toBe(true);
      expect(record.error.message).toContain(mode === "overflow" ? "grew beyond 65536-byte bound" : "changed during diagnostic read");
      expect(reads.mock.results.reduce((sum, result) => sum + (result.type === "return" ? Number(result.value) : 0), 0)).toBeLessThanOrEqual(65537);
    } finally { end(); }
  });

  it("reports a bounded ledger failure without throwing or changing the root", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "capture-unit-"));
    const output = windowsCapture();
    vi.stubEnv("PI_TEST_TEMP_LEDGER", path.join(root,"absent"));
    try {
      expect(() => captureBoundary("driver-after-child")).not.toThrow();
      expect(String(output.mock.calls[0]?.[1])).toContain("ENOENT");
      captureCleanup("cleanup-error", root, Object.assign(new Error("held handle"), {code:"EBUSY"}));
      expect(String(output.mock.calls.at(-1)?.[1])).toContain("EBUSY");
      expect(fs.existsSync(root)).toBe(true);
      captureRecord("large", {data:"x".repeat(70_000)});
      const record = String(output.mock.calls.at(-1)?.[1]);
      expect(Buffer.byteLength(record)).toBeLessThanOrEqual(65536);
      expect(JSON.parse(record.replace(/^FABRIC_TEMP_CAPTURE /, "")).incomplete).toBe(true);
    } finally { fs.rmSync(root, {recursive:true,force:true}); }
  });
});
