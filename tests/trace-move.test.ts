import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import childProcess from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { moveTraceNoReplace, nativeTraceMoveHost, traceMoveHelper } from "../src/storage/trace-move.js";

const roots: string[] = [];
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-trace-move-test-")); roots.push(root);
  return { source: path.join(root, "raw-λ.jsonl"), destination: path.join(root, "held-雪.jsonl") };
};
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

// KEEP: native trust boundary + trace data safety. Successful cases execute the real packaged syscall subprocess.
describe.runIf(nativeTraceMoveHost())("native non-replacing trace move", () => {
  it("moves unicode argv with exactly moved LF, preserving bytes and inode", () => {
    const { source, destination } = fixture(); fs.writeFileSync(source, "raw\n");
    const before = fs.statSync(source);
    expect(traceMoveHelper()).toBeDefined();
    const result = childProcess.spawnSync(traceMoveHelper()!, [source, destination]);
    expect(result.status).toBe(0); expect(result.stdout).toEqual(Buffer.from([0x6d, 0x6f, 0x76, 0x65, 0x64, 0x0a]));
    expect(fs.existsSync(source)).toBe(false); expect(fs.readFileSync(destination, "utf8")).toBe("raw\n");
    expect(fs.statSync(destination).ino).toBe(before.ino);
  });

  it.each(["file", "directory", "same path"])("never replaces an occupied %s and reports the platform error", kind => {
    const { source, destination } = fixture(); fs.writeFileSync(source, "raw A\n");
    if (kind === "file") fs.writeFileSync(destination, "raw C\n");
    if (kind === "directory") fs.mkdirSync(destination);
    const result = moveTraceNoReplace(source, kind === "same path" ? source : destination);
    expect(result.status).toBe("error");
    if (result.status === "error") expect(result.code).toBeGreaterThan(0);
    expect(fs.readFileSync(source, "utf8")).toBe("raw A\n");
    if (kind === "file") expect(fs.readFileSync(destination, "utf8")).toBe("raw C\n");
    if (kind === "directory") expect(fs.statSync(destination).isDirectory()).toBe(true);
  });

  it("wrapper executes the actual move, and missing sources fail without destination creation", () => {
    const { source, destination } = fixture();
    expect(moveTraceNoReplace(source, destination).status).toBe("error"); expect(fs.existsSync(destination)).toBe(false);
    fs.writeFileSync(source, "raw B\n");
    expect(moveTraceNoReplace(source, destination)).toEqual({ status: "moved" });
    expect(fs.readFileSync(destination, "utf8")).toBe("raw B\n");
  });

  it.each([{ args: [] }, { args: ["only-one"] }, { args: ["one", "two", "three"] }])("usage returns exact numeric error LF for %j", ({ args }) => {
    const result = childProcess.spawnSync(traceMoveHelper()!, args);
    expect(result.status).toBe(2);
    expect(result.stdout.toString("ascii")).toBe(`error ${os.type() === "Linux" ? 22 : 87}\n`);
  });

  it("selects the actual OS helper when process.platform is forced to Windows on Linux", () => {
    const actual = os.type(), descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...descriptor, value: "win32" });
    try {
      expect(path.basename(traceMoveHelper()!)).toBe(actual === "Linux" ? "fabric-trace-move" : "fabric-win-trace-move.exe");
      const { source, destination } = fixture(); fs.writeFileSync(source, "raw\n");
      expect(moveTraceNoReplace(source, destination)).toEqual({ status: "moved" });
    } finally { Object.defineProperty(process, "platform", descriptor); }
  });

  // Protocol parser negative controls only: not evidence of successful native movement.
  it.each([
    { status: 0, stdout: "moved\\n" }, { status: 0, stdout: "moved\nextra\n" },
    { status: 2, stdout: "error 0\n" }, { status: 2, stdout: "error 17\r\n" },
    { status: 0, stdout: "error 17\n" }, { status: null, stdout: "moved\n", signal: "SIGTERM" },
    { status: 0, stdout: "moved\n", error: new Error("timeout") },
  ])("unknown execution/protocol never retries or falls back: %j", result => {
    const { source, destination } = fixture(); fs.writeFileSync(source, "raw\n");
    const spawn = vi.spyOn(childProcess, "spawnSync").mockReturnValue(result as unknown as ReturnType<typeof childProcess.spawnSync>);
    expect(moveTraceNoReplace(source, destination)).toEqual({ status: "unknown" });
    expect(spawn).toHaveBeenCalledTimes(1); expect(fs.readFileSync(source, "utf8")).toBe("raw\n");
    expect(fs.existsSync(destination)).toBe(false);
  });
});
