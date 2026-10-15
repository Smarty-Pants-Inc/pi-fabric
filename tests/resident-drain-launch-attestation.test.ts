import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ContainmentUnavailableError, type AttemptLaunchReceipt } from "../src/residency/attempt-containment-contract.js";
import { readResidentLaunchAttestation } from "../src/residency/launch-attestation.js";

// Keep reason: launcher/owner trust boundary and bounded fd-relative native reads.
// All fixtures are retained; no cleanup, child launch or native containment claims.
function fixture() {
  const parent = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-launch-attestation-")));
  const root = path.join(parent, "resident");
  fs.mkdirSync(root, { mode: 0o700 });
  const receipt: AttemptLaunchReceipt = { attemptId: randomUUID(), scope: "fabric-attempt.scope",
    cgroupPath: "/user.slice/exact-launch-cgroup", launchedInScope: true, at: Date.now() - 10 };
  const file = path.join(root, `attempt-launch-${receipt.attemptId}.json`);
  const record: Record<string, unknown> = { ...receipt };
  const save = () => fs.writeFileSync(file, JSON.stringify(record), { mode: 0o600 });
  save();
  return { parent, root, receipt, file, record, save };
}
function unavailable(root: string, receipt: AttemptLaunchReceipt) {
  expect(() => readResidentLaunchAttestation(root, receipt)).toThrow(ContainmentUnavailableError);
  try { readResidentLaunchAttestation(root, receipt); }
  catch (error) { expect(error).toMatchObject({ code: "CONTAINMENT_UNAVAILABLE" }); return; }
  throw new Error("Reader unexpectedly accepted untrusted attestation");
}

afterEach(() => vi.restoreAllMocks());

describe.skipIf(process.platform !== "linux")("readResidentLaunchAttestation native record boundary", () => {
  it("returns only the exact authentic bare receipt, without reconstructing paths/time", () => {
    const f = fixture();
    const received = readResidentLaunchAttestation(f.root, f.receipt);
    expect(received).toEqual(f.receipt);
    expect(received).not.toBe(f.receipt);
    expect(Object.keys(received)).toEqual(["attemptId", "scope", "cgroupPath", "launchedInScope", "at"]);
    expect(received.cgroupPath).toBe(f.receipt.cgroupPath);
    expect(received.at).toBe(f.receipt.at);
  });

  it.each([
    ["attemptId", "another-attempt"], ["scope", "other.scope"],
    ["cgroupPath", "/different/cgroup"], ["launchedInScope", false], ["at", 1],
  ])("refuses a record mismatch in %s", (key, value) => {
    const f = fixture(); f.record[key as string] = value; f.save();
    unavailable(f.root, f.receipt);
  });

  it.each([
    ["attemptId", 3], ["scope", null], ["scope", ""], ["cgroupPath", []], ["cgroupPath", ""],
    ["launchedInScope", "true"], ["at", "1"], ["at", -1], ["at", 1.5], ["at", Number.MAX_SAFE_INTEGER + 1],
  ])("refuses invalid record field %s=%j", (key, value) => {
    const f = fixture(); f.record[key as string] = value; f.save();
    unavailable(f.root, f.receipt);
  });

  it("requires exactly five fields in record and expected receipt, never accepting wrappers or tokens", () => {
    for (const key of ["attemptId", "scope", "cgroupPath", "launchedInScope", "at"]) {
      const f = fixture(); delete f.record[key]; f.save(); unavailable(f.root, f.receipt);
    }
    for (const extra of [{ token: "private-launcher-token" }, { format: 1 }, { receipt: {} }]) {
      const f = fixture(); Object.assign(f.record, extra); f.save(); unavailable(f.root, f.receipt);
    }
    const authentic = fixture();
    unavailable(authentic.root, { ...authentic.receipt, token: "private-token" } as AttemptLaunchReceipt);
    for (const key of ["attemptId", "scope", "cgroupPath", "launchedInScope", "at"]) {
      const expected = { ...authentic.receipt }; delete expected[key as keyof AttemptLaunchReceipt];
      unavailable(authentic.root, expected);
    }
  });

  it("rejects missing roots/records, malformed JSON and non-object JSON without writing or inventing a receipt", () => {
    const f = fixture(); fs.renameSync(f.file, `${f.file}.retained`);
    const before = fs.readdirSync(f.root);
    unavailable(f.root, f.receipt); expect(fs.readdirSync(f.root)).toEqual(before);
    unavailable(path.join(f.parent, "absent"), f.receipt);
    for (const text of ["{", "null", "[]", "true", "42", "\"text\""]) {
      const other = fixture(); fs.writeFileSync(other.file, text); unavailable(other.root, other.receipt);
    }
  });

  it("rejects symlink files and symlink roots, including roots with a trailing separator", () => {
    const f = fixture(); fs.renameSync(f.file, `${f.file}.retained`); fs.symlinkSync(`${f.file}.retained`, f.file);
    unavailable(f.root, f.receipt);
    const authentic = fixture(); const alias = path.join(authentic.parent, "alias"); fs.symlinkSync(authentic.root, alias);
    unavailable(alias, authentic.receipt); unavailable(`${alias}/`, authentic.receipt);
  });

  it("refuses unsafe file permissions and writable roots rather than repairing them", () => {
    for (const mode of [0o644, 0o660, 0o666, 0o400, 0o1600, 0o2600, 0o4600]) {
      const f = fixture(); fs.chmodSync(f.file, mode); unavailable(f.root, f.receipt);
      expect(fs.statSync(f.file).mode & 0o7777).toBe(mode);
    }
    for (const mode of [0o720, 0o702]) {
      const f = fixture(); fs.chmodSync(f.root, mode); unavailable(f.root, f.receipt);
      expect(fs.statSync(f.root).mode & 0o777).toBe(mode);
    }
  });

  it("refuses a foreign uid on either opened descriptor (uid-only stat substitution)", () => {
    for (const kind of ["file", "root"] as const) {
      const f = fixture(); const native = fs.fstatSync.bind(fs);
      vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number) => {
        const stat = native(fd);
        if (kind === "file" ? stat.isFile() : stat.isDirectory()) stat.uid++;
        return stat;
      }) as typeof fs.fstatSync);
      unavailable(f.root, f.receipt); vi.restoreAllMocks();
    }
  });

  it("rejects nonregular, empty and oversized files before attempting any content read", () => {
    for (const kind of ["directory", "empty", "oversized"] as const) {
      const f = fixture();
      if (kind === "directory") { fs.renameSync(f.file, `${f.file}.retained`); fs.mkdirSync(f.file, { mode: 0o600 }); }
      if (kind === "empty") fs.writeFileSync(f.file, "");
      if (kind === "oversized") fs.writeFileSync(f.file, " ".repeat(64 * 1024 + 1));
      const read = vi.spyOn(fs, "readSync"); unavailable(f.root, f.receipt);
      expect(read).not.toHaveBeenCalled(); vi.restoreAllMocks();
    }
  });

  it("bounds a file that grows after fstat and rejects a concurrent rewrite", () => {
    for (const kind of ["grow", "rewrite"] as const) {
      const f = fixture(); const native = fs.readSync.bind(fs); let altered = false;
      vi.spyOn(fs, "readSync").mockImplementation(((...args: Parameters<typeof fs.readSync>) => {
        if (!altered) {
          altered = true;
          if (kind === "grow") fs.appendFileSync(f.file, " ".repeat(80 * 1024));
          else fs.writeFileSync(f.file, JSON.stringify({ ...f.record, scope: "replaced.scope" }));
        }
        return native(...args);
      }) as typeof fs.readSync);
      unavailable(f.root, f.receipt); vi.restoreAllMocks();
    }
  });

  it("uses the pinned root inode even when the path is renamed/replaced immediately after its open", () => {
    const f = fixture(); const native = fs.openSync.bind(fs); let swapped = false;
    const opened: string[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: number | string, mode?: fs.Mode) => {
      const fd = native(file, flags, mode);
      // The injected counterfeit write also opens a file; track only reader opens.
      if (typeof flags === "number" && (flags & fs.constants.O_NOFOLLOW) !== 0) opened.push(String(file));
      if (String(file) === f.root && !swapped) {
        swapped = true; fs.renameSync(f.root, `${f.root}.retained`); fs.mkdirSync(f.root, { mode: 0o700 });
        fs.writeFileSync(f.file, JSON.stringify({ ...f.record, at: 1 }), { mode: 0o600 });
      }
      return fd;
    }) as typeof fs.openSync);
    expect(readResidentLaunchAttestation(f.root, f.receipt)).toEqual(f.receipt);
    expect(opened).toHaveLength(2);
    expect(opened[1]).toMatch(/^\/proc\/self\/fd\/\d+\/attempt-launch-/);
    expect(opened[1]).not.toBe(f.file);
  });

  it("rejects unsafe attempt filenames and invalid expected values before opening any path", () => {
    const f = fixture(); const open = vi.spyOn(fs, "openSync");
    for (const attemptId of ["../other", "a/b", "", ".", "..", "a".repeat(201), "a\u0000b"]) {
      unavailable(f.root, { ...f.receipt, attemptId });
    }
    for (const patch of [{ scope: "" }, { cgroupPath: "" }, { at: Date.now() + 60_000 }, { launchedInScope: false }]) {
      unavailable(f.root, { ...f.receipt, ...patch } as AttemptLaunchReceipt);
    }
    expect(open).not.toHaveBeenCalled();
  });

  it("closes both descriptors on authentic and malformed records", () => {
    const f = fixture(); const close = vi.spyOn(fs, "closeSync");
    readResidentLaunchAttestation(f.root, f.receipt); expect(close).toHaveBeenCalledTimes(2);
    close.mockClear(); fs.writeFileSync(f.file, "{"); close.mockClear();
    expect(() => readResidentLaunchAttestation(f.root, f.receipt)).toThrow(ContainmentUnavailableError);
    expect(close).toHaveBeenCalledTimes(2);
  });
});
